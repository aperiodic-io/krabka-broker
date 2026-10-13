// Tests for the bounds that segmenting the index gives: the largest move when
// a segment grows or shrinks, the heap against the live entries, and the work
// of one call. Every assertion is on counts and estimates, never on time, so
// the tests are deterministic. The one timed test is ignored.

use {
    super::{
        test_support::{OpRun, OpSpace, VISITS_PER_CALL, VISITS_PER_TICK, check_invariants, nth},
        *,
    },
    assert2::assert,
    proptest::prelude::*,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const SECOND: i64 = 1_000;
const HOUR: i64 = 3_600 * SECOND;
const SEGMENTS: usize = DedupWindowFilter::SEGMENTS;

// An upper bound of the heap of one live entry in a segment that is at least a
// quarter full: the dense entry, and up to 16/7 hash buckets, from the 7/8 load
// limit and the rounding of the bucket count to a power of two.
const BYTES_PER_ENTRY: usize = ENTRY_BYTES + 3 * INDEX_BUCKET_BYTES;

// The heap a filter is allowed for `entries` live entries: `factor` times
// their bytes, every segment at the shrink floor, and the fixed segment table
// and histogram.
fn heap_bound(factor: usize, entries: usize) -> usize {
    let fixed = DedupWindowFilter::new(SECOND, 1, None).heap_bytes();
    factor * entries * BYTES_PER_ENTRY + SEGMENTS * segment_heap_bytes(SHRINK_FLOOR) + fixed
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Path {
    Leader,
    Follower,
}

impl Path {
    // Indexes one new identity the way a leader or a follower does, at a
    // broker time equal to the record's.
    fn apply(self, filter: &mut DedupWindowFilter, identity: IdentityDigest, timestamp_ms: i64) {
        match self {
            Path::Leader => {
                filter.classify(
                    &[DedupRecord {
                        identity: Some(identity),
                        timestamp_ms,
                    }],
                    timestamp_ms,
                );
            }
            Path::Follower => filter.populate(identity, timestamp_ms, timestamp_ms),
        }
    }
}

const PATHS: [Path; 2] = [Path::Leader, Path::Follower];

// A small deterministic generator, so traffic is varied and repeatable.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
}

#[test]
fn segment_constants_agree() {
    assert!(SEGMENTS == 1 << SEGMENT_BITS);
    assert!(SEGMENTS <= usize::try_from(u64::BITS).unwrap());
    assert!(SHRINK_FLOOR >= SHRINK_DIVISOR);
}

// The top bits of `hi` pick the segment, and `lo`, which the in-segment
// hasher takes, plays no part.
#[test]
fn identities_route_by_the_top_bits_of_hi() {
    let cases = [
        (0, 0, 0),
        (0, u64::MAX, 0),
        (u64::MAX >> SEGMENT_BITS, 7, 0),
        (1 << (u64::BITS - SEGMENT_BITS), 0, 1),
        (u64::MAX, 0, SEGMENTS - 1),
        (u64::MAX, u64::MAX, SEGMENTS - 1),
    ];
    for (hi, lo, segment) in cases {
        assert!(
            segment_of(IdentityDigest { hi, lo }) == segment,
            "hi {hi:#x}"
        );
    }
}

#[test]
fn well_spread_identities_load_every_segment_evenly() {
    const IDENTITIES: u64 = 64_000;
    let mut loads = [0_usize; SEGMENTS];
    for index in 0..IDENTITIES {
        loads[segment_of(nth(index))] += 1;
    }
    let even = usize::try_from(IDENTITIES).unwrap() / SEGMENTS;
    // About six standard deviations of a binomial share.
    for (segment, load) in loads.iter().enumerate() {
        assert!(load.abs_diff(even) < 200, "segment {segment}");
    }
}

#[test]
fn segment_limit_is_twice_the_share_and_at_least_the_floor() {
    let cases = [
        (0, SEGMENT_LIMIT_FLOOR),
        (8, SEGMENT_LIMIT_FLOOR),
        (100_000, SEGMENT_LIMIT_FLOOR),
        (DedupWindowFilter::DEFAULT_MAX_ENTRIES, 31_250),
        (DedupWindowFilter::MAX_MAX_ENTRIES, 312_500),
        (usize::MAX, usize::MAX.div_ceil(SEGMENTS) * 2),
    ];
    for (max_entries, limit) in cases {
        assert!(segment_limit_of(max_entries) == limit, "max {max_entries}");
    }
}

#[test]
fn segment_heap_estimate_follows_the_table_layout() {
    let cases = [
        (0, 0),
        // Four buckets is the smallest table.
        (
            2,
            2 * ENTRY_BYTES + 4 * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES,
        ),
        (
            3,
            3 * ENTRY_BYTES + 4 * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES,
        ),
        (
            7,
            7 * ENTRY_BYTES + 8 * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES,
        ),
        (
            896,
            896 * ENTRY_BYTES + 1024 * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES,
        ),
        // After a shrink to twice the length the bucket count rounds up.
        (
            600,
            600 * ENTRY_BYTES + 1024 * INDEX_BUCKET_BYTES + CONTROL_TAIL_BYTES,
        ),
    ];
    for (capacity, bytes) in cases {
        assert!(segment_heap_bytes(capacity) == bytes, "capacity {capacity}");
    }
}

// One table for the partition moves every entry when it doubles. Segments
// move one segment's entries, about twice the even share at most.
#[test]
fn growth_never_relocates_more_than_one_segment() {
    const IDENTITIES: u64 = 200_000;
    let identities = usize::try_from(IDENTITIES).unwrap();
    let bound = 2 * identities.div_ceil(SEGMENTS);
    let mut relocations = Vec::new();
    for path in PATHS {
        let mut filter = DedupWindowFilter::new(HOUR, DedupWindowFilter::DEFAULT_MAX_ENTRIES, None);
        for index in 0..IDENTITIES {
            path.apply(&mut filter, nth(index), 1000);
        }
        assert!(filter.len() == identities, "{path:?}");
        check_invariants(&filter);
        assert!(filter.largest_relocation() > 0, "{path:?}");
        assert!(filter.largest_relocation() <= bound, "{path:?}");
        assert!(filter.largest_relocation() * 32 < identities, "{path:?}");
        relocations.push(filter.largest_relocation());
    }
    assert!(relocations[0] == relocations[1]);
}

struct SaturationCase {
    name: &'static str,
    // The timestamp step between new identities. Zero puts the whole index
    // in one histogram bucket, so no slice can be cut and the index fails
    // open. A step spreads it, so the cursor trims the oldest quarter.
    step_ms: i64,
    expect_evictions: bool,
    expect_unindexed: bool,
}

const SATURATION_CASES: [SaturationCase; 2] = [
    SaturationCase {
        name: "spread timestamps are trimmed",
        step_ms: 1,
        expect_evictions: true,
        expect_unindexed: false,
    },
    SaturationCase {
        name: "one timestamp fails open",
        step_ms: 0,
        expect_evictions: false,
        expect_unindexed: true,
    },
];

// Filling to the entry limit and far past it keeps the count at the limit and
// the heap within a constant factor of it.
#[test]
fn heap_stays_within_a_constant_factor_of_the_limit() {
    const MAX: usize = 30_000;
    const IDENTITIES: u64 = 3 * MAX as u64;
    let bound = heap_bound(4, MAX);
    for case in SATURATION_CASES {
        for path in PATHS {
            let mut filter = DedupWindowFilter::new(HOUR, MAX, None);
            let mut timestamp = 1000;
            for index in 0..IDENTITIES {
                path.apply(&mut filter, nth(index), timestamp);
                timestamp += case.step_ms;
                assert!(filter.len() <= MAX, "{} {path:?} {index}", case.name);
                assert!(
                    filter.heap_bytes() <= bound,
                    "{} {path:?} {index}",
                    case.name
                );
                if index % 1009 == 0 {
                    check_invariants(&filter);
                }
            }
            let counters = filter.counters();
            assert!(
                (counters.capacity_evicted_entries > 0) == case.expect_evictions,
                "{} {path:?}",
                case.name
            );
            assert!(
                (counters.unindexed_records > 0) == case.expect_unindexed,
                "{} {path:?}",
                case.name
            );
        }
    }
}

// After a burst expires, the segments shrink as the cursor empties them, so
// the heap follows the live entries down instead of staying at its peak.
#[test]
fn heap_shrinks_after_a_burst_expires() {
    const BURST: u64 = 200_000;
    const LATER: u64 = 40_000;
    for path in PATHS {
        let mut filter =
            DedupWindowFilter::new(SECOND, DedupWindowFilter::DEFAULT_MAX_ENTRIES, None);
        for index in 0..BURST {
            path.apply(&mut filter, nth(index), 1000);
        }
        let peak = filter.heap_bytes();
        assert!(
            peak >= usize::try_from(BURST).unwrap() * ENTRY_BYTES,
            "{path:?}"
        );

        // Each later identity is past the window of every earlier one, so
        // the cursor drops whatever it visits, and the index drains.
        let mut timestamp = 10 * SECOND;
        for index in BURST..BURST + LATER {
            path.apply(&mut filter, nth(index), timestamp);
            timestamp += 2 * SECOND;
        }
        check_invariants(&filter);
        assert!(filter.len() < 1000, "{path:?}");
        assert!(
            filter.heap_bytes() <= heap_bound(4, filter.len()),
            "{path:?}"
        );
        assert!(filter.heap_bytes() * 4 < peak, "{path:?}");
        for segment in &filter.segments {
            let retained = SHRINK_FLOOR.max(SHRINK_DIVISOR * segment.table.len());
            assert!(segment.capacity <= retained, "{path:?}");
        }
    }
}

// Every call visits a bounded number of entries and moves a bounded number,
// while the index grows from empty and while it is saturated.
#[test]
fn per_call_work_is_bounded_at_every_size() {
    const MAX: usize = 30_000;
    const IDENTITIES: u64 = 3 * MAX as u64;
    for case in SATURATION_CASES {
        for path in PATHS {
            let mut filter = DedupWindowFilter::new(HOUR, MAX, None);
            let mut timestamp = 1000;
            let mut peak = 0;
            for index in 0..IDENTITIES {
                let before = filter.sweep_visits();
                path.apply(&mut filter, nth(index), timestamp);
                timestamp += case.step_ms;
                assert!(
                    filter.sweep_visits() - before <= VISITS_PER_CALL,
                    "{} {path:?} {index}",
                    case.name
                );
                peak = peak.max(filter.len());
            }
            // The later calls ran against an index at its soft limit.
            assert!(peak >= soft_limit_of(MAX), "{} {path:?}", case.name);
            assert!(
                filter.largest_relocation() <= 2 * MAX.div_ceil(SEGMENTS),
                "{} {path:?}",
                case.name
            );
        }
    }
}

// Many partitions on one broker, each allowed more than the broker budget,
// with traffic that repeats identities and moves time back and forth.
#[test]
fn budget_limit_is_respected_across_many_filters() {
    const FILTERS: u64 = 16;
    const LIMIT: usize = 5_000;
    const OPS: usize = 100_000;
    let shared = Arc::new(DedupBudget::new(LIMIT));
    let mut filters: Vec<DedupWindowFilter> = (0..FILTERS)
        .map(|_| DedupWindowFilter::new(10 * SECOND, 4 * LIMIT, Some(shared.clone())))
        .collect();
    let mut random = Lcg(7);
    let mut timestamp = 1_000_000;
    for op in 0..OPS {
        let filter = &mut filters[usize::try_from(random.below(FILTERS)).unwrap()];
        // About ten thousand distinct identities per window, twice the budget.
        let identity = nth(random.below(200_000));
        timestamp += i64::try_from(random.below(3)).unwrap();
        // Occasional records from behind, rare enough that time still moves
        // forward on average.
        if random.below(997) == 0 {
            timestamp -= SECOND / 2;
        }
        let path = if random.below(2) == 0 {
            Path::Leader
        } else {
            Path::Follower
        };
        path.apply(filter, identity, timestamp);
        let total: usize = filters.iter().map(DedupWindowFilter::len).sum();
        assert!(shared.entries() <= LIMIT, "op {op}");
        assert!(shared.entries() == total, "op {op}");
    }
    let trimmed: u64 = filters
        .iter()
        .map(|filter| {
            filter.counters().capacity_evicted_entries + filter.counters().unindexed_records
        })
        .sum();
    assert!(trimmed > 0);
    drop(filters);
    assert!(shared.entries() == 0);
}

// Identities chosen to share one segment fill that segment to its limit and
// no further. The largest move stays within the limit, and the other segments
// keep indexing.
#[test]
fn crowded_segment_is_bounded_by_its_limit() {
    const MAX: usize = 100_000;
    const CROWD: usize = 6_000;
    let crowd: Vec<IdentityDigest> = (0..u64::MAX)
        .map(nth)
        .filter(|identity| segment_of(*identity) == 0)
        .take(CROWD)
        .collect();
    let mut filter = DedupWindowFilter::new(HOUR, MAX, None);
    let limit = filter.segment_limit;
    assert!(limit == SEGMENT_LIMIT_FLOOR);
    for identity in &crowd {
        filter.populate(*identity, 1000, 1000);
    }
    check_invariants(&filter);
    assert!(filter.len() == limit);
    assert!(filter.largest_relocation() <= limit);
    let refused = u64::try_from(CROWD - limit).unwrap();
    assert!(
        filter.counters()
            == DedupCounters {
                unindexed_records: refused,
                capacity_sweeps: refused,
                ..DedupCounters::default()
            }
    );

    let spread = (0..u64::MAX)
        .map(nth)
        .find(|identity| segment_of(*identity) != 0)
        .unwrap();
    let record = DedupRecord {
        identity: Some(spread),
        timestamp_ms: 1000,
    };
    assert!(filter.classify(&[record], 1000).all_kept());
    assert!(filter.classify(&[record], 1000).none_kept());
}

// The state that a leader and a follower must agree on after the same calls:
// the entries in cursor order, the watermark, the work and the moves. The
// follower never sees a dropped record, so only that count differs.
fn assert_converged(leader: &DedupWindowFilter, follower: &DedupWindowFilter) {
    check_invariants(leader);
    check_invariants(follower);
    assert!(leader.iter().collect::<Vec<_>>() == follower.iter().collect::<Vec<_>>());
    assert!(leader.watermark_ms() == follower.watermark_ms());
    assert!(leader.largest_relocation() == follower.largest_relocation());
    assert!(leader.heap_bytes() == follower.heap_bytes());
    assert!(leader.sweep_visits() == follower.sweep_visits());
    assert!(
        DedupCounters {
            dropped_records: 0,
            ..leader.counters()
        } == follower.counters()
    );
}

// Feeds one record to the leader, and to the follower when the leader keeps
// it, as the follower reads it from the log. Returns whether it was kept.
fn feed(
    leader: &mut DedupWindowFilter,
    follower: &mut DedupWindowFilter,
    identity: IdentityDigest,
    timestamp_ms: i64,
) -> bool {
    let record = DedupRecord {
        identity: Some(identity),
        timestamp_ms,
    };
    let kept = leader.classify(&[record], timestamp_ms).all_kept();
    if kept {
        follower.populate(identity, timestamp_ms, timestamp_ms);
    }
    kept
}

// The follower applies only what the leader kept, as it does from the log,
// and both tick at the same times. Through capacity pressure, expiry waves,
// and the shrinks they cause, both hold the same entries in the same order
// and made the same moves.
#[test]
fn leader_and_follower_agree_through_growth_and_shrink() {
    const MAX: usize = 3_000;
    let mut leader = DedupWindowFilter::new(10 * SECOND, MAX, None);
    let mut follower = DedupWindowFilter::new(10 * SECOND, MAX, None);
    for filter in [&mut leader, &mut follower] {
        filter.shrink_floor = 4;
    }
    // Rounds of dense traffic that repeats identities and fills the index,
    // each followed by a quiet spell past the window, where a trickle of new
    // identities lets the cursor expire the round.
    let mut random = Lcg(11);
    let mut timestamp = 1000;
    let mut traffic = Vec::new();
    for round in 0..6 {
        for _ in 0..8_000 {
            timestamp += i64::try_from(random.below(3)).unwrap();
            traffic.push((nth(random.below(12_000)), timestamp));
        }
        timestamp += 20 * SECOND;
        for index in 0..1_000 {
            traffic.push((nth(1_000_000 + round * 1_000 + index), timestamp));
        }
    }

    let mut shrinks = 0;
    for (index, (identity, timestamp_ms)) in traffic.into_iter().enumerate() {
        let heap = leader.heap_bytes();
        feed(&mut leader, &mut follower, identity, timestamp_ms);
        // A tick now and then, at the time of the last record.
        if index % 97 == 0 {
            leader.tick(timestamp_ms);
            follower.tick(timestamp_ms);
        }
        if leader.heap_bytes() < heap {
            shrinks += 1;
        }
    }
    assert!(shrinks > 0);
    assert!(leader.counters().capacity_evicted_entries > 0);
    assert!(leader.counters().dropped_records > 0);
    assert_converged(&leader, &follower);

    // With no more traffic, ticks alone empty both.
    for _ in 0..=MAX.div_ceil(DedupWindowFilter::TICK_VISITS) {
        leader.tick(timestamp + 20 * SECOND);
        follower.tick(timestamp + 20 * SECOND);
    }
    assert!(leader.is_empty());
    assert_converged(&leader, &follower);
}

// The problem that ticks solve: a partition with no traffic. Its entries, and
// their share of the broker budget, go back once they expire, through ticks
// alone, and its heap shrinks back to the floor. No tick visits more than
// `TICK_VISITS` entries.
#[test]
fn ticks_release_an_idle_filter() {
    const IDENTITIES: u64 = 100_000;
    const START: i64 = 1_000_000;
    const WINDOW: i64 = 10 * SECOND;
    let identities = usize::try_from(IDENTITIES).unwrap();
    let floor = heap_bound(1, 0);
    for path in PATHS {
        let shared = Arc::new(DedupBudget::new(4 * identities));
        let mut filter = DedupWindowFilter::new(
            WINDOW,
            DedupWindowFilter::DEFAULT_MAX_ENTRIES,
            Some(shared.clone()),
        );
        for index in 0..IDENTITIES {
            path.apply(&mut filter, nth(index), START);
        }
        assert!(filter.len() == identities, "{path:?}");
        let peak = filter.heap_bytes();

        // At the edge of the window every entry is still live, so a tick
        // drops nothing.
        filter.tick(START + WINDOW);
        assert!(filter.len() == identities, "{path:?}");

        let mut ticks = 0;
        let mut now = START + WINDOW + 1;
        while !filter.is_empty() {
            assert!(
                ticks <= identities.div_ceil(DedupWindowFilter::TICK_VISITS),
                "{path:?}"
            );
            let before = filter.sweep_visits();
            filter.tick(now);
            assert!(
                filter.sweep_visits() - before <= VISITS_PER_TICK,
                "{path:?}"
            );
            assert!(shared.entries() == filter.len(), "{path:?}");
            ticks += 1;
            now += 1;
        }
        check_invariants(&filter);
        assert!(shared.entries() == 0, "{path:?}");
        assert!(filter.heap_bytes() <= floor, "{path:?}");
        assert!(filter.heap_bytes() * 4 < peak, "{path:?}");
        assert!(filter.largest_relocation() <= 2 * identities.div_ceil(SEGMENTS));

        // A tick on an empty filter visits nothing.
        let before = filter.sweep_visits();
        filter.tick(now);
        assert!(filter.sweep_visits() == before, "{path:?}");
    }
}

// The time of the slowest of `calls` calls of `call`, and the index of that call.
fn slowest_of(calls: u64, mut call: impl FnMut(u64)) -> (Duration, u64) {
    let mut slowest = (Duration::ZERO, 0);
    for index in 0..calls {
        let started = Instant::now();
        call(index);
        slowest = slowest.max((started.elapsed(), index));
    }
    slowest
}

// Prints the slowest single classify while one partition grows to two million
// identities. Wall-clock time depends on the machine, so this asserts nothing
// and runs only on request:
// `cargo test -p krabka-dedup --release -- --ignored --nocapture`.
#[test]
#[ignore = "timing report; run with --ignored --release --nocapture"]
fn report_slowest_classify_while_growing() {
    const IDENTITIES: u64 = 2_000_000;
    let mut filter = DedupWindowFilter::new(HOUR, DedupWindowFilter::MAX_MAX_ENTRIES, None);
    let started = Instant::now();
    let (slowest, slowest_at) = slowest_of(IDENTITIES, |index| {
        let record = DedupRecord {
            identity: Some(nth(index)),
            timestamp_ms: 1000,
        };
        filter.classify(&[record], 1000);
    });
    println!(
        "segments {SEGMENTS}: {} identities in {:?}; slowest classify {:?} at identity {slowest_at}; largest relocation {} entries; heap estimate {} MiB",
        filter.len(),
        started.elapsed(),
        slowest,
        filter.largest_relocation(),
        filter.heap_bytes() >> 20,
    );
}

// Prints the slowest single tick of an idle partition of two million
// identities: while they are all live, so that each tick only surveys, and
// while they all expire, so that each tick removes entries and shrinks
// segments. Like the report above, it asserts nothing and runs only on
// request.
#[test]
#[ignore = "timing report; run with --ignored --release --nocapture"]
fn report_slowest_tick_while_idle() {
    const IDENTITIES: u64 = 2_000_000;
    const START: i64 = 1000;
    let mut filter = DedupWindowFilter::new(HOUR, DedupWindowFilter::MAX_MAX_ENTRIES, None);
    for index in 0..IDENTITIES {
        filter.populate(nth(index), START, START);
    }
    let peak = filter.heap_bytes();
    let surveys = IDENTITIES / u64::try_from(DedupWindowFilter::TICK_VISITS).unwrap();
    let (slowest_live, _) = slowest_of(surveys, |_| filter.tick(START + HOUR));
    let started = Instant::now();
    let mut ticks = 0;
    let (slowest_expired, slowest_at) = slowest_of(2 * surveys, |index| {
        if !filter.is_empty() {
            ticks += 1;
            filter.tick(START + HOUR + 1 + i64::try_from(index).unwrap());
        }
    });
    println!(
        "tick of an idle index of {IDENTITIES} identities, {} visits each: slowest while live {slowest_live:?}; {ticks} ticks to drain in {:?}, slowest {slowest_expired:?} at tick {slowest_at}; heap estimate {} MiB to {} KiB; largest relocation {} entries",
        DedupWindowFilter::TICK_VISITS,
        started.elapsed(),
        peak >> 20,
        filter.heap_bytes() >> 10,
        filter.largest_relocation(),
    );
}

// Small segments that shrink and fill, so that random sequences reach the
// shrink and the segment limit with the cursor inside the segment.
fn with_small_segments(mut filter: DedupWindowFilter) -> DedupWindowFilter {
    filter.shrink_floor = 4;
    filter.segment_limit = 3;
    filter
}

fn small_filter(shared: &Arc<DedupBudget>) -> DedupWindowFilter {
    with_small_segments(DedupWindowFilter::new(
        5 * SECOND,
        150,
        Some(shared.clone()),
    ))
}

// One step of a leader and its follower: a record for a key after a step of
// the clock, or a tick of both after a step of the clock.
#[derive(Clone, Copy, Debug)]
enum Step {
    Record { key: u16, advance: i64 },
    Tick { advance: i64 },
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        6 => (0..400_u16, 0..300_i64).prop_map(|(key, advance)| Step::Record { key, advance }),
        1 => (0..3000_i64).prop_map(|advance| Step::Tick { advance }),
    ]
}

// A leader or a follower with small segments that shrink and fill.
fn small_replica() -> DedupWindowFilter {
    with_small_segments(DedupWindowFilter::new(5 * SECOND, 150, None))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    // A leader and a follower that make the same calls at the same times,
    // ticks among them, end in the same state.
    #[test]
    fn leader_and_follower_agree_with_ticks(steps in prop::collection::vec(step(), 1..1500)) {
        let mut leader = small_replica();
        let mut follower = small_replica();
        let mut clock = 100_000;
        for step in steps {
            match step {
                Step::Record { key, advance } => {
                    clock += advance;
                    feed(&mut leader, &mut follower, nth(u64::from(key)), clock);
                }
                Step::Tick { advance } => {
                    clock += advance;
                    leader.tick(clock);
                    follower.tick(clock);
                }
            }
        }
        // A dropped record moves only the leader's watermark, and nothing
        // else. The follower's next call catches up, so one more tick each
        // makes the watermarks comparable.
        leader.tick(clock);
        follower.tick(clock);
        assert_converged(&leader, &follower);
    }

    // Whatever mix of operations runs, with time moving both ways, no limit
    // is crossed, the budget stays exact, and the cursor never leaves the
    // segments.
    #[test]
    fn random_ops_keep_every_bound(ops in prop::collection::vec(
        OpSpace {
            keys: 2000,
            advance: -3000..3000,
            jump: Some(-20_000..40_000),
            revert_weight: 2,
        }
        .strategy(),
        1..1500,
    )) {
        OpRun {
            budget: 300,
            start_ms: 100_000,
            filter: small_filter,
            identity: |key| nth(u64::from(key)),
        }
        .run(ops);
    }
}
