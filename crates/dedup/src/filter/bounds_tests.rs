// Tests for the bounds that segmenting the index gives: the largest move when
// a segment grows or shrinks, the heap against the live entries, and the work
// of one call. Every assertion is on counts and estimates, never on time, so
// the tests are deterministic. The one timed test is ignored.

use {
    super::*,
    assert2::assert,
    proptest::prelude::*,
    std::{sync::Arc, time::Instant},
};

const NOW: i64 = 1_000_000_000_000;
const SECOND: i64 = 1_000;
const HOUR: i64 = 3_600 * SECOND;
const SEGMENTS: usize = DedupWindowFilter::SEGMENTS;

// The most entries the cursor visits in one call for one record.
const VISITS_PER_CALL: u64 = (SWEEP_BUDGET + CAPACITY_EMERGENCY_BUDGET) as u64;

// An upper bound of the heap of one live entry in a segment that is at least a
// quarter full: the dense entry, and up to 16/7 hash buckets, from the 7/8 load
// limit and the rounding of the bucket count to a power of two.
const BYTES_PER_ENTRY: usize = ENTRY_BYTES + 3 * INDEX_BUCKET_BYTES;

fn nth(index: u64) -> IdentityDigest { IdentityDigest::of(&index.to_le_bytes()) }

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
    // Indexes one new identity the way a leader or a follower does.
    fn apply(self, filter: &mut DedupWindowFilter, identity: IdentityDigest, timestamp_ms: i64) {
        match self {
            Path::Leader => {
                filter.classify(
                    &[DedupRecord {
                        identity: Some(identity),
                        timestamp_ms,
                    }],
                    NOW,
                );
            }
            Path::Follower => filter.populate(identity, timestamp_ms, NOW),
        }
    }
}

const PATHS: [Path; 2] = [Path::Leader, Path::Follower];

// The state the segments must agree on after every call.
fn check_invariants(filter: &DedupWindowFilter) {
    let mut total = 0;
    let mut occupied = 0_u64;
    for (index, segment) in filter.segments.iter().enumerate() {
        total += segment.table.len();
        if !segment.table.is_empty() {
            occupied |= 1 << index;
        }
        assert!(
            segment.table.len() <= filter.segment_limit,
            "segment {index}"
        );
        // The usable slots never exceed the allocation the segment records.
        assert!(
            segment.table.capacity() <= segment.capacity,
            "segment {index}"
        );
    }
    assert!(total == filter.len());
    assert!(occupied == filter.occupied);
    assert!(filter.len() <= filter.max_entries());
    assert!(filter.largest_relocation() <= filter.segment_limit);
    assert!(filter.sweep_segment < SEGMENTS);
}

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
        filter.populate(*identity, 1000, NOW);
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
    assert!(filter.classify(&[record], NOW).all_kept());
    assert!(filter.classify(&[record], NOW).none_kept());
}

// The follower applies only what the leader kept, as it does from the log.
// Through capacity pressure, expiry waves, and the shrinks they cause, both
// hold the same entries in the same order and made the same moves.
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
    for (identity, timestamp_ms) in traffic {
        let record = DedupRecord {
            identity: Some(identity),
            timestamp_ms,
        };
        let heap = leader.heap_bytes();
        if leader.classify(&[record], NOW).all_kept() {
            follower.populate(identity, timestamp_ms, NOW);
        }
        if leader.heap_bytes() < heap {
            shrinks += 1;
        }
    }
    check_invariants(&leader);
    check_invariants(&follower);
    assert!(shrinks > 0);
    assert!(leader.counters().capacity_evicted_entries > 0);
    assert!(leader.counters().dropped_records > 0);
    assert!(leader.iter().collect::<Vec<_>>() == follower.iter().collect::<Vec<_>>());
    assert!(leader.largest_relocation() == follower.largest_relocation());
    assert!(leader.heap_bytes() == follower.heap_bytes());
    assert!(leader.sweep_visits() == follower.sweep_visits());
    // The follower never sees a dropped record, so only that count differs.
    assert!(
        DedupCounters {
            dropped_records: 0,
            ..leader.counters()
        } == follower.counters()
    );
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
    let mut slowest = std::time::Duration::ZERO;
    let mut slowest_at = 0;
    let started = Instant::now();
    for index in 0..IDENTITIES {
        let record = DedupRecord {
            identity: Some(nth(index)),
            timestamp_ms: 1000,
        };
        let call = Instant::now();
        filter.classify(&[record], NOW);
        let elapsed = call.elapsed();
        if elapsed > slowest {
            slowest = elapsed;
            slowest_at = index;
        }
    }
    println!(
        "segments {SEGMENTS}: {} identities in {:?}; slowest classify {:?} at identity {slowest_at}; largest relocation {} entries; heap estimate {} MiB",
        filter.len(),
        started.elapsed(),
        slowest,
        filter.largest_relocation(),
        filter.heap_bytes() >> 20,
    );
}

#[derive(Clone, Debug)]
enum Op {
    Classify {
        filter: usize,
        key: u16,
        advance: i64,
        keep_undo: bool,
    },
    Populate {
        filter: usize,
        key: u16,
        advance: i64,
    },
    // A jump of the clock with no record: the next record then expires a
    // wave of entries, or arrives behind the stored ones.
    Jump {
        advance: i64,
    },
    Revert,
    Clear {
        filter: usize,
    },
    Replace {
        filter: usize,
    },
}

const FILTERS: usize = 3;

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        8 => (0..FILTERS, 0..2000_u16, -3000..3000_i64, any::<bool>())
            .prop_map(|(filter, key, advance, keep_undo)| Op::Classify { filter, key, advance, keep_undo }),
        8 => (0..FILTERS, 0..2000_u16, -3000..3000_i64)
            .prop_map(|(filter, key, advance)| Op::Populate { filter, key, advance }),
        1 => (-20_000..40_000_i64).prop_map(|advance| Op::Jump { advance }),
        2 => Just(Op::Revert),
        1 => (0..FILTERS).prop_map(|filter| Op::Clear { filter }),
        1 => (0..FILTERS).prop_map(|filter| Op::Replace { filter }),
    ]
}

// Small segments that shrink and fill, so that random sequences reach the
// shrink and the segment limit with the cursor inside the segment.
fn small_filter(shared: &Arc<DedupBudget>) -> DedupWindowFilter {
    let mut filter = DedupWindowFilter::new(5 * SECOND, 150, Some(shared.clone()));
    filter.shrink_floor = 4;
    filter.segment_limit = 3;
    filter
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    // Whatever mix of operations runs, with time moving both ways, no limit
    // is crossed, the budget stays exact, and the cursor never leaves the
    // segments.
    #[test]
    fn random_ops_keep_every_bound(ops in prop::collection::vec(op(), 1..1500)) {
        let shared = Arc::new(DedupBudget::new(300));
        let mut filters: Vec<DedupWindowFilter> = (0..FILTERS).map(|_| small_filter(&shared)).collect();
        let mut clock = 100_000;
        let mut pending: Vec<(usize, Undo)> = Vec::new();
        for op in ops {
            match op {
                Op::Classify { filter, key, advance, keep_undo } => {
                    clock += advance;
                    let record = DedupRecord { identity: Some(nth(u64::from(key))), timestamp_ms: clock };
                    let before = filters[filter].sweep_visits();
                    let result = filters[filter].classify(&[record], NOW);
                    assert!(filters[filter].sweep_visits() - before <= VISITS_PER_CALL);
                    if keep_undo {
                        pending.push((filter, result.undo));
                    }
                }
                Op::Populate { filter, key, advance } => {
                    clock += advance;
                    let before = filters[filter].sweep_visits();
                    filters[filter].populate(nth(u64::from(key)), clock, NOW);
                    assert!(filters[filter].sweep_visits() - before <= VISITS_PER_CALL);
                }
                Op::Jump { advance } => clock += advance,
                Op::Revert => {
                    if let Some((filter, undo)) = pending.pop() {
                        filters[filter].revert(&undo);
                    }
                }
                Op::Clear { filter } => filters[filter].clear(),
                Op::Replace { filter } => filters[filter] = small_filter(&shared),
            }
            let total: usize = filters.iter().map(DedupWindowFilter::len).sum();
            assert!(shared.entries() == total);
            assert!(shared.entries() <= shared.max_entries());
            for filter in &filters {
                check_invariants(filter);
            }
        }
        drop(filters);
        assert!(shared.entries() == 0);
    }
}
