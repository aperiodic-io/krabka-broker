use {
    super::{
        test_support::{OpRun, OpSpace},
        *,
    },
    assert2::assert,
    proptest::prelude::*,
    std::{ptr, sync::Arc},
};

const SECOND: i64 = 1_000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;
const DEFAULT: usize = DedupWindowFilter::DEFAULT_MAX_ENTRIES;
// A broker time far ahead of the fixed timestamps of the other tests, for the
// tests about clock skew.
const NOW: i64 = 1_000_000_000_000;

fn id(name: &str) -> IdentityDigest { IdentityDigest::of(name.as_bytes()) }

fn rec(name: &str, timestamp_ms: i64) -> DedupRecord {
    DedupRecord {
        identity: Some(id(name)),
        timestamp_ms,
    }
}

fn no_identity(timestamp_ms: i64) -> DedupRecord {
    DedupRecord {
        identity: None,
        timestamp_ms,
    }
}

fn filter(window_ms: i64) -> DedupWindowFilter { DedupWindowFilter::new(window_ms, DEFAULT, None) }

fn budget(max_entries: usize) -> Arc<DedupBudget> { Arc::new(DedupBudget::new(max_entries)) }

// Most tests run the broker's clock at the newest record time, so that the
// watermark follows the records and no timestamp is clamped. The tests about
// the clock give it explicitly.

// Classifies `records` at the broker time of the newest one.
fn classify(filter: &mut DedupWindowFilter, records: &[DedupRecord]) -> Classification {
    let now_ms = records
        .iter()
        .map(|record| record.timestamp_ms)
        .max()
        .unwrap_or(i64::MIN);
    filter.classify(records, now_ms)
}

// Whether a single-record request for `name` is kept at broker time `now_ms`.
fn kept_at(filter: &mut DedupWindowFilter, name: &str, timestamp_ms: i64, now_ms: i64) -> bool {
    filter
        .classify(&[rec(name, timestamp_ms)], now_ms)
        .all_kept()
}

// Whether a single-record request for `name` is kept at its own time.
fn kept(filter: &mut DedupWindowFilter, name: &str, timestamp_ms: i64) -> bool {
    kept_at(filter, name, timestamp_ms, timestamp_ms)
}

fn populate(filter: &mut DedupWindowFilter, name: &str, timestamp_ms: i64) {
    filter.populate(id(name), timestamp_ms, timestamp_ms);
}

fn populate_n(
    filter: &mut DedupWindowFilter,
    prefix: &str,
    count: usize,
    first_ms: i64,
    step_ms: i64,
) {
    let mut timestamp = first_ms;
    for index in 0..count {
        populate(filter, &format!("{prefix}-{index}"), timestamp);
        timestamp += step_ms;
    }
}

fn capacity(filter: &DedupWindowFilter) -> usize {
    filter.segments.iter().map(|segment| segment.capacity).sum()
}

fn entries(filter: &DedupWindowFilter) -> Vec<(IdentityDigest, i64)> { filter.iter().collect() }

fn inserted(name: &str, applied_timestamp: i64) -> UndoEntry {
    UndoEntry {
        identity: id(name),
        applied_timestamp,
        previous_timestamp: None,
    }
}

fn readmitted(name: &str, applied_timestamp: i64, previous: i64) -> UndoEntry {
    UndoEntry {
        identity: id(name),
        applied_timestamp,
        previous_timestamp: Some(previous),
    }
}

fn undo(entries: Vec<UndoEntry>) -> Undo { Undo { entries } }

#[test]
fn bucket_constants_agree() {
    assert!(i64::try_from(CAPACITY_EVICT_BUCKETS - 1) == Ok(LAST_BUCKET));
}

#[test]
fn soft_limit_is_a_tenth_and_at_least_one_below_the_limit() {
    for (limit, soft) in [
        (0, 0),
        (1, 0),
        (2, 1),
        (9, 8),
        (10, 9),
        (100, 90),
        (1_000_000, 900_000),
    ] {
        assert!(soft_limit_of(limit) == soft, "limit {limit}");
    }
}

#[test]
fn classification_helpers() {
    let cases = [
        (vec![], 0, true, true),
        (vec![false, false], 2, true, false),
        (vec![true, false], 1, false, false),
        (vec![true, true], 0, false, true),
    ];
    for (dropped, kept, all_kept, none_kept) in cases {
        let classification = Classification {
            dropped: dropped.clone(),
            kept,
            undo: Undo::default(),
        };
        assert!(classification.all_kept() == all_kept, "{dropped:?}");
        assert!(classification.none_kept() == none_kept, "{dropped:?}");
    }
}

#[test]
fn filter_is_send_and_sync() {
    fn check<T: Send + Sync>() {}
    check::<DedupWindowFilter>();
}

// The first record for an identity is kept, a duplicate within the window is
// dropped, and the identity is kept again after the window.
#[test]
fn first_wins_within_window() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "k", 1000));
    assert!(!kept(&mut f, "k", 1500));
    assert!(kept(&mut f, "k", 2100));
}

// The entry is at 5000 and the window is a second. A record is a duplicate
// while the entry is live at the broker's time and the two timestamps are a
// window apart or less. A record that is itself older than the window at the
// broker's time is kept.
#[test]
fn window_edge_is_inclusive_on_both_sides() {
    let cases = [
        ("a later record at the edge", 6000, 6000, false),
        ("an earlier record at the edge", 4000, 5000, false),
        ("an earlier record inside the window", 4500, 5500, false),
        ("a later record past the edge", 6001, 6001, true),
        ("an earlier record past the edge", 3999, 5000, true),
        (
            "an earlier record behind the broker's window",
            4999,
            6000,
            true,
        ),
    ];
    for (name, timestamp, now, expected) in cases {
        let mut f = filter(SECOND);
        populate(&mut f, "k", 5000);
        assert!(kept_at(&mut f, "k", timestamp, now) == expected, "{name}");
    }
}

#[test]
fn different_identities_do_not_interfere() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "a", 1000));
    assert!(kept(&mut f, "b", 1000));
    assert!(!kept(&mut f, "a", 1500));
    assert!(!kept(&mut f, "b", 1500));
}

// A record far behind the stored one arrives outside the broker's window. It
// is kept, and it does not touch the stored entry.
#[test]
fn record_older_than_stored_by_more_than_window_is_not_duplicate() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "k", 1_000_000));
    assert!(kept_at(&mut f, "k", 2000, 1_000_000));
    assert!(f.timestamp_of(id("k")) == Some(1_000_000));
    assert!(!kept(&mut f, "k", 1_000_500));
}

// One call that moves the watermark.
#[derive(Clone, Copy)]
enum Call {
    // A one-record request, for an identity or for a record without one.
    Classify(Option<&'static str>),
    EmptyClassify,
    Populate,
    Tick,
}

impl Call {
    fn apply(self, f: &mut DedupWindowFilter, now_ms: i64) {
        match self {
            Call::Classify(Some(name)) => drop(f.classify(&[rec(name, 1000)], now_ms)),
            Call::Classify(None) => drop(f.classify(&[no_identity(1000)], now_ms)),
            Call::EmptyClassify => drop(f.classify(&[], now_ms)),
            Call::Populate => f.populate(id("p"), 1000, now_ms),
            Call::Tick => f.tick(now_ms),
        }
    }
}

// The watermark is the broker's time, whatever the records carry, and every
// call moves it: a duplicate, a record without identity, a populate and a
// tick. A call with an earlier time does not move it back.
#[test]
fn watermark_follows_the_broker_clock_and_never_moves_back() {
    let mut f = filter(SECOND);
    assert!(f.watermark_ms() == i64::MIN);
    let steps = [
        ("a new identity", Call::Classify(Some("k")), 2000, 2000),
        ("a duplicate", Call::Classify(Some("k")), 2000, 2000),
        ("an earlier time", Call::Classify(Some("k")), 1500, 2000),
        ("no identity", Call::Classify(None), 2500, 2500),
        ("an empty request", Call::EmptyClassify, 3000, 3000),
        ("a populate", Call::Populate, 3500, 3500),
        ("a tick", Call::Tick, 4000, 4000),
        ("an earlier tick", Call::Tick, 1, 4000),
    ];
    for (name, call, now, watermark) in steps {
        call.apply(&mut f, now);
        assert!(f.watermark_ms() == watermark, "{name}");
    }
    f.clear();
    assert!(f.watermark_ms() == i64::MIN);
}

#[test]
fn classify_returns_undo_for_new_and_readmitted_identities() {
    let mut f = filter(SECOND);
    let first = classify(&mut f, &[rec("a", 1000), rec("b", 1000), rec("a", 1000)]);
    assert!(
        first
            == Classification {
                dropped: vec![false, false, true],
                kept: 2,
                undo: undo(vec![inserted("a", 1000), inserted("b", 1000)]),
            }
    );

    let again = classify(&mut f, &[rec("a", 2500)]);
    assert!(
        again
            == Classification {
                dropped: vec![false],
                kept: 1,
                undo: undo(vec![readmitted("a", 2500, 1000)]),
            }
    );
}

// A record older than the window at the broker's time, such as a backfill,
// is admitted and not indexed on both paths, and counted the same way, so a
// leader, a follower and a rebuild hold the same index. It neither replaces
// nor matches the stored entry.
#[test]
fn record_older_than_the_window_on_arrival_is_admitted_unindexed() {
    const STORED: i64 = 10_000;
    const NOW_MS: i64 = STORED + 5 * SECOND;
    let names = ["stored", "new"];
    let expected_counters = DedupCounters {
        expired_on_arrival_records: 2,
        ..DedupCounters::default()
    };
    let mut leader = filter(10 * SECOND);
    let mut follower = filter(10 * SECOND);
    for f in [&mut leader, &mut follower] {
        populate(f, "stored", STORED);
    }
    let old = NOW_MS - 10 * SECOND - 1;
    let result = leader.classify(&[rec(names[0], old), rec(names[1], old)], NOW_MS);
    for name in names {
        follower.populate(id(name), old, NOW_MS);
    }
    assert!(
        result
            == Classification {
                dropped: vec![false, false],
                kept: 2,
                undo: Undo::default(),
            }
    );
    for f in [&leader, &follower] {
        assert!(entries(f) == vec![(id("stored"), STORED)]);
        assert!(f.counters() == expected_counters);
    }
    // One millisecond later it is inside the window, so it is a duplicate.
    assert!(!kept_at(&mut leader, "stored", old + 1, NOW_MS));
}

// An entry that is no longer live counts as absent before a sweep removes it.
// The record replaces it, and a revert puts the expired entry back exactly.
#[test]
fn expired_entry_is_absent_before_it_is_swept() {
    let mut f = filter(SECOND);
    populate(&mut f, "k", 1000);
    // The entry and the record are 500 ms apart, but the entry is 1500 ms
    // behind the broker's time.
    let result = f.classify(&[rec("k", 1500)], 2500);
    assert!(
        result
            == Classification {
                dropped: vec![false],
                kept: 1,
                undo: undo(vec![readmitted("k", 1500, 1000)]),
            }
    );
    assert!(entries(&f) == vec![(id("k"), 1500)]);

    f.revert(&result.undo);
    assert!(entries(&f) == vec![(id("k"), 1000)]);
    // The restored entry is still not live, so it still drops nothing.
    assert!(kept_at(&mut f, "k", 1500, 2500));
}

#[test]
fn revert_restores_pre_request_state() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "a", 1000));

    let result = classify(&mut f, &[rec("a", 2500), rec("b", 2500)]);
    assert!(result.all_kept());
    assert!(f.len() == 2);

    f.revert(&result.undo);
    assert!(entries(&f) == vec![(id("a"), 1000)]);
    // "b" is new again. The watermark is not reverted, so "a" is back at its
    // old timestamp, which is no longer live.
    assert!(kept(&mut f, "b", 2500));
    assert!(kept(&mut f, "a", 2500));
}

#[test]
fn revert_leaves_newer_overwrites_untouched() {
    let mut f = filter(SECOND);
    let first = classify(&mut f, &[rec("k", 1000)]);
    // A later request readmits the identity before the first one's
    // replication outcome is known.
    let second = classify(&mut f, &[rec("k", 2500)]);
    assert!(second.all_kept());

    f.revert(&first.undo);
    assert!(entries(&f) == vec![(id("k"), 2500)]);
    assert!(!kept(&mut f, "k", 3000));
}

// One request has one broker time, so two of its records for one identity are
// either a duplicate or not live on arrival: the undo holds one change for
// the identity.
#[test]
fn one_request_changes_an_identity_once() {
    let mut f = filter(SECOND);
    let result = f.classify(&[rec("k", 1000), rec("k", 2000), rec("k", 2500)], 2500);
    assert!(
        result
            == Classification {
                dropped: vec![false, false, true],
                kept: 2,
                undo: undo(vec![inserted("k", 2000)]),
            }
    );
    f.revert(&result.undo);
    assert!(f.is_empty());
}

// Revert unwinds several changes to one identity in reverse order. Classify no
// longer makes such an undo, so this one is written out.
#[test]
fn revert_unwinds_same_identity_mutations_in_reverse() {
    let mut f = filter(SECOND);
    populate(&mut f, "k", 2500);
    f.revert(&undo(vec![
        inserted("k", 1000),
        readmitted("k", 2500, 1000),
    ]));
    assert!(f.is_empty());
}

// A revert restores exactly what the request did: its own admissions go, and
// the entry that made it drop a record stays.
#[test]
fn revert_after_partial_drop_restores_exactly() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "dup", 1000));

    let result = classify(&mut f, &[rec("a", 1000), rec("dup", 1000), rec("b", 1000)]);
    assert!(result.dropped == vec![false, true, false]);
    assert!(f.len() == 3);

    f.revert(&result.undo);
    assert!(entries(&f) == vec![(id("dup"), 1000)]);
    assert!(kept(&mut f, "a", 1000));
    assert!(kept(&mut f, "b", 1000));
    assert!(!kept(&mut f, "dup", 1000));
}

#[test]
fn revert_of_an_all_duplicate_request_is_a_noop() {
    let mut f = filter(SECOND);
    assert!(classify(&mut f, &[rec("a", 1000), rec("b", 1000)]).all_kept());
    let before = entries(&f);

    let result = classify(&mut f, &[rec("a", 1200), rec("b", 1200)]);
    assert!(result.none_kept());
    assert!(result.undo.is_empty());

    f.revert(&result.undo);
    assert!(entries(&f) == before);
    assert!(!kept(&mut f, "a", 1200));
}

#[test]
fn records_without_identity_are_always_kept_and_never_indexed() {
    let mut f = filter(SECOND);
    assert!(kept(&mut f, "dup", 1000));

    let result = classify(&mut f, &[
        no_identity(1000),
        rec("dup", 1000),
        no_identity(1000),
        rec("a", 1000),
    ]);
    assert!(
        result
            == Classification {
                dropped: vec![false, true, false, false],
                kept: 3,
                undo: undo(vec![inserted("a", 1000)]),
            }
    );
    assert!(f.len() == 2);
    assert!(classify(&mut f, &[no_identity(1000), no_identity(1000)]).all_kept());
    assert!(f.len() == 2);
}

// Classification runs in record order, so the drop pattern within one request
// matches what the same records would get across requests.
struct DropCase {
    name: &'static str,
    seeds: &'static [&'static str],
    request: &'static [&'static str],
    expected: &'static [bool],
}

#[test]
fn drops_land_on_the_right_records() {
    let cases = [
        DropCase {
            name: "same identity twice keeps the first",
            seeds: &[],
            request: &["x", "x"],
            expected: &[false, true],
        },
        DropCase {
            name: "same identity many times keeps the first",
            seeds: &[],
            request: &["x", "x", "y", "x", "x"],
            expected: &[false, true, false, true, true],
        },
        DropCase {
            name: "duplicate at the start",
            seeds: &["dup"],
            request: &["dup", "a", "b"],
            expected: &[true, false, false],
        },
        DropCase {
            name: "duplicate at the end",
            seeds: &["dup"],
            request: &["a", "b", "dup"],
            expected: &[false, false, true],
        },
        DropCase {
            name: "alternating duplicates",
            seeds: &["d1", "d2"],
            request: &["d1", "a", "d2", "b", "d1"],
            expected: &[true, false, true, false, true],
        },
        DropCase {
            name: "all duplicates",
            seeds: &["a", "b"],
            request: &["a", "b"],
            expected: &[true, true],
        },
    ];
    for DropCase {
        name,
        seeds,
        request,
        expected,
    } in cases
    {
        let mut f = filter(SECOND);
        for seed in seeds {
            assert!(kept(&mut f, seed, 1000), "{name}");
        }
        let records: Vec<DedupRecord> = request.iter().map(|key| rec(key, 1000)).collect();
        let result = classify(&mut f, &records);
        assert!(result.dropped == expected, "{name}");
        assert!(
            result.kept == expected.iter().filter(|dropped| !**dropped).count(),
            "{name}"
        );
    }
}

#[test]
fn large_request_keeps_the_right_records() {
    let mut f = filter(SECOND);
    let mut records = Vec::new();
    let mut expected = Vec::new();
    for index in 0..3000 {
        let key = format!("k-{index}");
        let seeded = index % 3 == 0;
        if seeded {
            assert!(kept(&mut f, &key, 1000));
        }
        records.push(rec(&key, 1000));
        expected.push(seeded);
    }
    let result = classify(&mut f, &records);
    assert!(result.dropped == expected);
    assert!(result.kept == 2000);
}

// Only a partial drop makes the caller rebuild and recompress the batch, so
// the counter must tell it apart from both pass-through cases.
#[test]
fn rebuilt_batches_counts_only_partial_drops() {
    let mut f = filter(SECOND);
    assert!(classify(&mut f, &[rec("a", 1000), rec("b", 1000)]).all_kept());
    assert!(f.counters().rebuilt_batches == 0);

    assert!(classify(&mut f, &[rec("a", 1500), rec("b", 1500)]).none_kept());
    assert!(f.counters().rebuilt_batches == 0);

    let partial = classify(&mut f, &[rec("a", 1500), rec("c", 1500)]);
    assert!(partial.kept == 1);
    assert!(f.counters().rebuilt_batches == 1);

    // A revert does not take back the rebuild the request paid for.
    f.revert(&partial.undo);
    assert!(
        f.counters()
            == DedupCounters {
                dropped_records: 3,
                rebuilt_batches: 1,
                ..DedupCounters::default()
            }
    );
}

#[test]
fn populate_seeds_the_index() {
    let mut f = filter(SECOND);
    populate(&mut f, "k", 1000);
    assert!(!kept(&mut f, "k", 1500));
}

// Max-wins makes log replay idempotent: the same prefix applied again gives
// the same index.
#[test]
fn populate_is_idempotent_and_max_wins() {
    let mut f = filter(SECOND);
    populate(&mut f, "k", 2000);
    populate(&mut f, "k", 1000);
    populate(&mut f, "k", 2000);
    assert!(entries(&f) == vec![(id("k"), 2000)]);
    assert!(!kept(&mut f, "k", 2900));
}

// Each new identity moves the cursor, so an insert that moves the watermark
// past an entry's window drops it at once.
#[test]
fn populate_triggers_an_eviction_sweep() {
    let mut f = filter(SECOND);
    populate(&mut f, "stale", 0);
    populate(&mut f, "fresh", 5000);
    assert!(entries(&f) == vec![(id("fresh"), 5000)]);
}

// Expiry is spread over inserts. A backlog of expired entries must still drain
// under ordinary traffic.
#[test]
fn expired_entries_drain_incrementally() {
    const STALE: usize = 500;
    let mut f = filter(SECOND);
    populate_n(&mut f, "stale", STALE, 0, 0);
    assert!(f.len() == STALE);

    populate_n(&mut f, "fresh", STALE, 5000, 0);
    assert!(f.len() == STALE);
    for index in 0..STALE {
        assert!(!kept(&mut f, &format!("fresh-{index}"), 5000));
    }
    assert!(kept(&mut f, "stale-0", 5000));
}

// A filter whose two-entry limit `a` and `b` fill.
fn filter_full_of_a_and_b() -> DedupWindowFilter {
    let mut f = DedupWindowFilter::new(HOUR, 2, None);
    assert!(kept(&mut f, "a", 1000));
    assert!(kept(&mut f, "b", 1000));
    f
}

#[test]
fn entry_limit_fails_open_without_growing_the_index() {
    let mut f = filter_full_of_a_and_b();

    // A new identity is admitted with the index full, and is not remembered.
    assert!(kept(&mut f, "c", 1000));
    assert!(kept(&mut f, "c", 1000));
    assert!(f.len() == 2);

    // Indexed identities keep their coverage.
    assert!(!kept(&mut f, "a", 1000));
    assert!(f.len() == 2);
}

#[test]
fn drops_and_saturation_are_counted() {
    let mut f = filter_full_of_a_and_b();
    assert!(f.counters() == DedupCounters::default());

    assert!(!kept(&mut f, "a", 1000));
    assert!(kept(&mut f, "c", 1000));
    assert!(kept(&mut f, "c", 1000));
    // The apply path counts saturation the same way.
    populate(&mut f, "d", 1000);

    assert!(
        f.counters()
            == DedupCounters {
                dropped_records: 1,
                unindexed_records: 3,
                capacity_sweeps: 3,
                ..DedupCounters::default()
            }
    );
    assert!(f.len() == 2);
}

// The cursor runs before the capacity check, so a full index whose entries
// expire with this insert indexes the new identity.
#[test]
fn entry_limit_reopens_after_an_eviction_sweep() {
    let mut f = DedupWindowFilter::new(SECOND, 2, None);
    populate(&mut f, "stale-a", 0);
    populate(&mut f, "stale-b", 0);

    populate(&mut f, "fresh", 5000);
    assert!(entries(&f) == vec![(id("fresh"), 5000)]);
    assert!(f.counters().unindexed_records == 0);
    assert!(!kept(&mut f, "fresh", 5000));
}

// The limit is hard: populate never exceeds it, and an indexed identity still
// takes a higher timestamp while the index is full.
#[test]
fn populate_respects_the_entry_limit() {
    let mut f = DedupWindowFilter::new(HOUR, 2, None);
    populate(&mut f, "a", 1000);
    populate(&mut f, "b", 1000);
    for index in 0..8 {
        populate(&mut f, &format!("new-{index}"), 1000);
        assert!(f.len() <= f.max_entries());
    }
    assert!(f.len() == 2);
    assert!(f.counters().unindexed_records == 8);

    populate(&mut f, "a", 2500);
    assert!(f.timestamp_of(id("a")) == Some(2500));
    assert!(f.counters().unindexed_records == 8);
    assert!(!kept(&mut f, "a", 2500));
}

// At the limit the index drops its oldest slice to make room. Dedup then keeps
// working with an effective window of about the limit divided by the rate of
// new identities, and does not stop for every new identity.
#[test]
fn capacity_eviction_drops_the_oldest_not_the_newest() {
    const CAP: usize = 8;
    let mut f = DedupWindowFilter::new(100 * SECOND, CAP, None);
    // Twice what the index holds, all inside the window, so room can only
    // come from live entries.
    for index in 0..16_i64 {
        assert!(kept(&mut f, &format!("id-{index}"), (index + 1) * 5000));
        assert!(f.len() <= CAP);
    }

    assert!(f.counters().capacity_evicted_entries > 0);
    assert!(f.counters().unindexed_records == 0);
    assert!(!kept(&mut f, "id-15", 80_000));
    assert!(kept(&mut f, "id-0", 5000));
}

#[test]
fn capacity_eviction_keeps_dedup_working_when_saturated() {
    const CAP: usize = 8;
    let mut f = DedupWindowFilter::new(100 * SECOND, CAP, None);
    // Five times what the index holds, none old enough to expire.
    for index in 1..=40_i64 {
        populate(&mut f, &format!("id-{index}"), index * 2000);
        assert!(f.len() <= CAP);
    }
    assert!(f.counters().unindexed_records == 0);
    assert!(f.counters().capacity_evicted_entries > 0);
    assert!(!kept(&mut f, "id-40", 80_000));
}

// A timestamp cutoff cannot drop a bounded slice when more than that slice
// shares one narrow range. Dropping more would lose coverage nobody asked to
// lose, so the record is admitted unindexed.
#[test]
fn capacity_eviction_fails_open_when_no_slice_can_be_freed() {
    const CAP: usize = 4;
    let mut f = DedupWindowFilter::new(HOUR, CAP, None);
    for index in 0..CAP {
        assert!(kept(&mut f, &format!("id-{index}"), 1000));
    }
    assert!(kept(&mut f, "extra", 1000));
    assert!(f.len() == CAP);
    assert!(f.counters().capacity_evicted_entries == 0);
    assert!(f.counters().unindexed_records == 1);
}

// A full index with nothing to free is the case that would rescan the whole
// index per record. The cursor budget per call is the only defence, so this
// measures it directly.
#[test]
fn saturated_index_does_bounded_work_per_record() {
    const CAP: usize = 64;
    const ADMITTED: u64 = 500;
    let visit_bound = u64::try_from(SWEEP_BUDGET + CAPACITY_EMERGENCY_BUDGET).unwrap();
    let mut f = DedupWindowFilter::new(HOUR, CAP, None);
    // One timestamp for the whole index: the oldest bucket holds everything,
    // so no bounded slice can be cut.
    for index in 0..CAP {
        assert!(kept(&mut f, &format!("id-{index}"), 1000));
    }

    // Distinct, advancing timestamps, so that a gate on the watermark alone
    // would not hold. Nothing expires inside an hour.
    for index in 0..ADMITTED {
        let before = f.sweep_visits();
        let timestamp = 1000 + i64::try_from(index).unwrap();
        assert!(kept(&mut f, &format!("new-{index}"), timestamp));
        assert!(f.sweep_visits() - before <= visit_bound, "record {index}");
    }
    assert!(f.counters().unindexed_records == ADMITTED);
    assert!(f.len() == CAP);

    assert!(!kept(&mut f, "id-0", 1500));
    assert!(kept(&mut f, "new-0", 1500));

    // Saturation ends: once the expiry cutoff reaches the indexed entries, the
    // cursor frees them.
    assert!(kept(&mut f, "later", 1000 + 2 * HOUR));
    assert!(f.len() < CAP);
}

// The same bound against index size: the cost of one call must not grow with
// the index.
#[test]
fn sweep_work_does_not_grow_with_index_size() {
    const SEEDED: usize = 200_000;
    let budget = u64::try_from(SWEEP_BUDGET).unwrap();
    let mut f = DedupWindowFilter::new(HOUR, DEFAULT, None);
    // One timestamp in an hour-long window, far below the soft limit: the
    // cursor only surveys.
    populate_n(&mut f, "seed", SEEDED, 1000, 0);
    assert!(f.len() == SEEDED);

    for index in 0..2000 {
        let before = f.sweep_visits();
        populate(&mut f, &format!("extra-{index}"), 1000);
        assert!(f.sweep_visits() - before <= budget, "insert {index}");
    }

    // An indexed identity does not reach the cursor at all.
    let before = f.sweep_visits();
    assert!(!kept(&mut f, "seed-0", 1000));
    assert!(f.sweep_visits() == before);
}

// Sends `count` new identities, `step_ms` apart, through the leader's classify
// path and the follower's populate path, as the follower applies them from the
// log.
fn feed_leader_and_follower(
    leader: &mut DedupWindowFilter,
    follower: &mut DedupWindowFilter,
    count: i64,
    step_ms: i64,
) {
    for index in 0..count {
        let key = format!("id-{index}");
        let timestamp = 1000 + index * step_ms;
        assert!(kept(leader, &key, timestamp));
        populate(follower, &key, timestamp);
    }
}

// The index is a function of the log, so the leader's classify path and the
// follower's populate path must agree entry for entry, including which live
// entries capacity trimming drops. The cursor position is part of the state,
// so this also compares the dense order the cursor walks.
#[test]
fn leader_and_follower_converge_across_the_entry_limit() {
    const CAP: usize = 8;
    let mut leader = DedupWindowFilter::new(100 * SECOND, CAP, None);
    let mut follower = DedupWindowFilter::new(100 * SECOND, CAP, None);

    feed_leader_and_follower(&mut leader, &mut follower, 60, 1500);
    assert!(leader.counters().capacity_evicted_entries > 0);
    assert!(entries(&leader) == entries(&follower));
    assert!(leader.watermark_ms() == follower.watermark_ms());
    assert!(leader.counters() == follower.counters());
    assert!(leader.sweep_visits() == follower.sweep_visits());
}

#[test]
fn clear_resets_the_index_and_releases_memory() {
    let mut f = filter(SECOND);
    populate_n(&mut f, "s", 300, 0, 0);
    let grown = capacity(&f);
    assert!(grown >= 300);
    let grown_heap = f.heap_bytes();

    f.clear();
    assert!(f.is_empty());
    assert!(capacity(&f) == 0);
    assert!(f.heap_bytes() < grown_heap);
    assert!(f.watermark_ms() == i64::MIN);

    populate(&mut f, "after-clear", 1000);
    assert!(f.len() == 1);
    assert!(!kept(&mut f, "after-clear", 1000));
}

// A cursor position and a part-built histogram describe one array. After a
// clear, the filter behaves like a new one: same entries, same order, same
// capacity decisions.
#[test]
fn clear_resets_the_cursor_and_the_histogram() {
    const CAP: usize = 8;
    let mut cleared = DedupWindowFilter::new(100 * SECOND, CAP, None);
    // Leave the cursor in the middle of the array and a cutoff in place.
    populate_n(&mut cleared, "before", 37, 1000, 700);
    assert!(cleared.counters().capacity_evicted_entries > 0);
    cleared.clear();

    let mut fresh = DedupWindowFilter::new(100 * SECOND, CAP, None);
    let before_visits = cleared.sweep_visits();
    for target in [&mut cleared, &mut fresh] {
        populate_n(target, "after", 50, 500, 1500);
    }
    assert!(entries(&cleared) == entries(&fresh));
    assert!(cleared.sweep_visits() - before_visits == fresh.sweep_visits());
}

// A future-dated record does not move the watermark: only the broker's time
// does. Entries that a later, correctly timed duplicate has to match stay.
#[test]
fn future_timestamp_does_not_move_the_watermark() {
    let mut f = filter(5 * MINUTE);
    assert!(kept_at(&mut f, "a", NOW, NOW));
    assert!(kept_at(&mut f, "skewed", NOW + HOUR, NOW));
    assert!(f.watermark_ms() == NOW);
    // A new identity gives the cursor its budget.
    assert!(kept_at(&mut f, "filler", NOW, NOW));
    assert!(!kept_at(&mut f, "a", NOW, NOW));
}

// Without the clamp, the skewed entry would stay live for an hour past its
// window and drop correctly timed records all that time.
#[test]
fn future_timestamp_is_clamped_and_counted() {
    let mut f = filter(5 * MINUTE);
    assert!(kept_at(&mut f, "skewed", NOW + HOUR, NOW));
    assert!(entries(&f) == vec![(id("skewed"), NOW)]);
    assert!(kept_at(
        &mut f,
        "skewed",
        NOW + 5 * MINUTE + 1,
        NOW + 5 * MINUTE + 1
    ));
    assert!(
        f.counters()
            == DedupCounters {
                skew_clamped_records: 1,
                ..DedupCounters::default()
            }
    );
}

// The clamp applies to the stored entry as well as the watermark. An entry
// stored ahead of the broker's time would stay above the cutoff and never
// expire.
#[test]
fn future_timestamp_is_clamped_in_the_stored_entry() {
    let mut f = filter(5 * MINUTE);
    f.populate(id("skewed"), NOW + HOUR, NOW);
    assert!(entries(&f) == vec![(id("skewed"), NOW)]);
    assert!(f.counters().skew_clamped_records == 1);
}

#[test]
fn records_without_identity_are_clamped_too() {
    let mut f = filter(5 * MINUTE);
    assert!(f.classify(&[no_identity(NOW + 1)], NOW).all_kept());
    assert!(f.counters().skew_clamped_records == 1);
    assert!(f.is_empty());
}

// Client timestamps and the window are arbitrary i64 values. No combination
// may overflow, in the comparison, the sweep, or the capacity histogram.
#[test]
fn extreme_windows_and_timestamps_do_not_overflow() {
    let windows = [i64::MIN, -1, 0, 1, HOUR, i64::MAX / 2, i64::MAX];
    let timestamps = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for window in windows {
        let mut leader = DedupWindowFilter::new(window, 8, None);
        let mut follower = DedupWindowFilter::new(window, 8, None);
        for round in 0..6 {
            for (index, timestamp) in timestamps.iter().enumerate() {
                let key = format!("id-{round}-{index}");
                leader.classify(&[rec(&key, *timestamp), rec(&key, *timestamp)], i64::MAX);
                leader.classify(&[rec("shared", *timestamp)], i64::MAX);
                follower.populate(id(&key), *timestamp, i64::MAX);
                follower.populate(id("shared"), *timestamp, i64::MAX);
                assert!(leader.len() <= 8, "window {window}");
            }
        }
    }
}

#[test]
fn window_comparison_is_exact_at_the_extremes() {
    let cases = [
        ("the widest window spans zero", i64::MAX, -1, NOW, true),
        (
            "the widest window does not span the whole range",
            i64::MAX,
            i64::MIN,
            i64::MAX,
            false,
        ),
        (
            "the widest window reaches the far end",
            i64::MAX,
            i64::MIN,
            -1,
            true,
        ),
        (
            "a zero window matches the same timestamp",
            0,
            i64::MIN,
            i64::MIN,
            true,
        ),
        (
            "a zero window does not match the next one",
            0,
            i64::MIN,
            i64::MIN + 1,
            false,
        ),
        ("a negative window matches nothing", -1, 1000, 1000, false),
        (
            "the most negative window matches nothing",
            i64::MIN,
            1000,
            1000,
            false,
        ),
    ];
    for (name, window, first, second, duplicate) in cases {
        let mut f = DedupWindowFilter::new(window, DEFAULT, None);
        populate(&mut f, "k", first);
        let result = f.classify(&[rec("k", second)], first.max(second));
        assert!(result.none_kept() == duplicate, "{name}");
    }
}

#[test]
fn set_window_applies_to_later_comparisons() {
    let mut f = filter(SECOND);
    populate(&mut f, "k", 1000);
    f.set_window(10 * SECOND);
    assert!(f.window_ms() == 10 * SECOND);
    assert!(!kept(&mut f, "k", 9000));
}

#[test]
fn debug_shows_the_summary() {
    let f = DedupWindowFilter::new(SECOND, 5, None);
    let text = format!("{f:?}");
    assert!(text.starts_with("DedupWindowFilter { window_ms: 1000, max_entries: 5, len: 0"));
}

// Every filter on a broker reports its size into one budget, and new
// identities are trimmed or refused against the broker total as well as the
// partition limit. The budget must equal the sum of the indices after every
// operation that changes a size.

#[test]
fn without_a_budget_nothing_is_shared() {
    let mut f = DedupWindowFilter::new(100 * SECOND, 50, None);
    assert!(f.budget().is_none());
    populate_n(&mut f, "k", 40, 1000, 1);
    assert!(f.len() == 40);
    assert!(f.counters().unindexed_records == 0);
}

#[test]
fn budget_counts_every_filter() {
    let shared = budget(1000);
    let mut a = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    let mut b = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    populate_n(&mut a, "a", 10, 1000, 1);
    populate_n(&mut b, "b", 5, 1000, 1);
    assert!(shared.entries() == 15);
    assert!(
        a.budget()
            .is_some_and(|held| ptr::eq(held, shared.as_ref()))
    );

    // A repeated identity updates in place and costs nothing more.
    populate_n(&mut a, "a", 10, 2000, 1);
    assert!(shared.entries() == 15);
}

#[test]
fn budget_counts_produce_path_inserts() {
    let shared = budget(1000);
    let mut f = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    assert!(classify(&mut f, &[rec("a", 1000), rec("b", 1000), rec("c", 1000)]).all_kept());
    assert!(shared.entries() == 3);
    assert!(!kept(&mut f, "a", 1100));
    assert!(shared.entries() == 3);
}

#[test]
fn reverted_request_gives_its_entries_back() {
    let shared = budget(1000);
    let mut f = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    populate_n(&mut f, "kept", 4, 1000, 1);
    let result = classify(&mut f, &[
        rec("x", 5000),
        rec("y", 5000),
        rec("kept-0", 5000),
    ]);
    assert!(shared.entries() == 6);

    f.revert(&result.undo);
    assert!(f.len() == 4);
    assert!(shared.entries() == 4);
}

#[test]
fn expiry_sweeps_give_entries_back() {
    let shared = budget(1000);
    let mut f = DedupWindowFilter::new(SECOND, 1000, Some(shared.clone()));
    populate_n(&mut f, "old", 64, 1000, 0);
    assert!(shared.entries() == 64);
    // Each new identity far past the window sweeps eight expired entries.
    populate_n(&mut f, "new", 4, 1_000_000, 0);
    assert!(f.len() < 64);
    assert!(shared.entries() == f.len());
}

#[test]
fn clear_and_drop_keep_the_total_exact() {
    let shared = budget(1000);
    let mut a = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    populate_n(&mut a, "a", 20, 1000, 1);
    {
        let mut b = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
        populate_n(&mut b, "b", 30, 1000, 1);
        assert!(shared.entries() == 50);
        b.clear();
        assert!(shared.entries() == 20);
        populate_n(&mut b, "b", 30, 1000, 1);
        assert!(shared.entries() == 50);
    }
    assert!(shared.entries() == 20);
    a.clear();
    assert!(shared.entries() == 0);
}

// Four full partitions on one broker, each within its own limit, together far
// over what the broker allows.
#[test]
fn combined_size_stays_within_the_budget() {
    const LIMIT: usize = 200;
    let shared = budget(LIMIT);
    let mut filters: Vec<DedupWindowFilter> = (0..4)
        .map(|_| DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone())))
        .collect();
    for index in 0..2000_usize {
        let count = filters.len();
        let target = &mut filters[index % count];
        populate(
            target,
            &format!("id-{index}"),
            1000 + i64::try_from(index).unwrap(),
        );
        assert!(shared.entries() <= LIMIT, "after {index}");
    }
    let total: usize = filters.iter().map(DedupWindowFilter::len).sum();
    let trimmed: u64 = filters
        .iter()
        .map(|f| f.counters().capacity_evicted_entries + f.counters().unindexed_records)
        .sum();
    assert!(total == shared.entries());
    assert!(trimmed > 0);
    for f in &filters {
        // Far below its own limit: the budget is what bounded it.
        assert!(f.len() < f.max_entries());
    }
}

// Broker pressure trims a partition that is far from its own soft limit,
// through the same oldest-quarter cursor, so the partition keeps its newest
// identities.
#[test]
fn pressure_trims_the_oldest_entries_of_the_inserting_filter() {
    const LIMIT: usize = 100;
    const IDENTITIES: i64 = 2000;
    const STEP: i64 = 100;
    let shared = budget(LIMIT);
    // Timestamps spread over many histogram buckets: an index packed into one
    // narrow range is never trimmed for capacity.
    let mut f = DedupWindowFilter::new(100 * SECOND, 10_000, Some(shared.clone()));
    populate_n(
        &mut f,
        "id",
        usize::try_from(IDENTITIES).unwrap(),
        1000,
        STEP,
    );

    assert!(f.len() <= LIMIT);
    assert!(f.counters().capacity_evicted_entries > 0);
    assert!(shared.entries() == f.len());
    let newest = format!("id-{}", IDENTITIES - 1);
    assert!(!kept(&mut f, &newest, 1000 + (IDENTITIES - 1) * STEP));
}

// At the broker limit, a partition with nothing of its own to free admits new
// identities unindexed. When the space is released elsewhere, it indexes
// again.
#[test]
fn at_the_limit_a_filter_with_nothing_freeable_admits_unindexed() {
    const LIMIT: usize = 50;
    let shared = budget(LIMIT);
    let mut idle = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
    {
        let mut full = DedupWindowFilter::new(100 * SECOND, 1000, Some(shared.clone()));
        // One timestamp, so that no histogram frame can cut a slice and the
        // full partition holds the whole budget.
        populate_n(&mut full, "full", LIMIT, 1000, 0);
        assert!(shared.entries() == LIMIT);

        assert!(kept(&mut idle, "new", 1000));
        assert!(idle.is_empty());
        assert!(
            idle.counters()
                == DedupCounters {
                    unindexed_records: 1,
                    capacity_sweeps: 1,
                    ..DedupCounters::default()
                }
        );
        assert!(shared.entries() == LIMIT);
    }
    // The full partition went away and took its entries with it.
    assert!(shared.entries() == 0);
    assert!(kept(&mut idle, "new", 2000));
    assert!(idle.len() == 1);
    assert!(!kept(&mut idle, "new", 2100));
}

// The leader and follower paths agree when the budget does the trimming,
// given the same budget state.
#[test]
fn leader_and_follower_agree_under_budget_pressure() {
    const LIMIT: usize = 40;
    let leader_budget = budget(LIMIT);
    let follower_budget = budget(LIMIT);
    let mut leader = DedupWindowFilter::new(100 * SECOND, 1000, Some(leader_budget.clone()));
    let mut follower = DedupWindowFilter::new(100 * SECOND, 1000, Some(follower_budget.clone()));

    feed_leader_and_follower(&mut leader, &mut follower, 500, 150);
    assert!(leader.counters().capacity_evicted_entries > 0);
    assert!(entries(&leader) == entries(&follower));
    assert!(leader_budget.entries() == follower_budget.entries());
    assert!(leader.counters() == follower.counters());
}

fn budget_filter(shared: &Arc<DedupBudget>) -> DedupWindowFilter {
    DedupWindowFilter::new(5 * SECOND, 150, Some(shared.clone()))
}

proptest! {
    // Whatever mix of operations runs against several filters, the budget
    // equals the sum of their sizes after each one, and drops to zero when
    // they all go.
    #[test]
    fn budget_total_matches_the_indices(ops in prop::collection::vec(
        OpSpace {
            keys: 700,
            advance: 0..50,
            jump: None,
            revert_weight: 1,
        }
        .strategy(),
        1..800,
    )) {
        OpRun {
            budget: 300,
            start_ms: 1000,
            filter: budget_filter,
            identity: |key| id(&format!("k-{key}")),
        }
        .run(ops);
    }
}
