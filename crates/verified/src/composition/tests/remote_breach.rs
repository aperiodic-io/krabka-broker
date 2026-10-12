use assert2::assert;

use super::*;
use crate::retention::RemoteRetentionSegment;

fn check_plan(
    facts: DeleteRecordsTrimFacts,
    stores: (i64, i64),
    previous: Option<i64>,
    trace: &[bool],
    published: bool,
    allowed: bool,
    finished: &[(i64, i64, u64)],
) {
    let admitted = trim_witnesses::logical_oracle(facts, stores.0, stores.1, &[]);
    let expected = admitted.map(|(floor, _, _)| {
        let steps = trace.iter().filter(|done| **done).count();
        let wal_steps = usize::from(stores.0 < floor);
        let observed = (
            if steps >= wal_steps { floor } else { stores.0 },
            if steps > wal_steps { floor } else { stores.1 },
        );
        let checkpoint = if published && observed == (floor, floor) {
            Some(floor)
        } else {
            previous
        };
        let rows: Vec<_> = finished
            .iter()
            .map(|&(_, end, size)| RemoteRetentionSegment {
                blocked: false,
                log_start_breached: checkpoint.is_some_and(|floor| end < floor),
                time_expired: false,
                size,
            })
            .collect();
        let count = if allowed {
            finished
                .iter()
                .position(|&(_, end, _)| checkpoint.is_none_or(|floor| end >= floor))
                .unwrap_or(finished.len())
        } else {
            0
        };
        (floor, observed, checkpoint, rows, count)
    });
    let proof_rows: Vec<_> = finished
        .iter()
        .map(|&(start, end, size)| (start, end, size, false))
        .collect();
    assert!(
        published_trim_bounds_remote_breach_cleanup(
            facts,
            stores,
            previous,
            trace,
            published,
            allowed,
            &proof_rows,
        ) == expected
    );
}

proptest! {
    #[test]
    fn published_cleanup_matches_stage_count_and_range_oracles(
        requested in -2i64..105, current in 0i64..101,
        wal in 0i64..101, local in 0i64..101,
        old in prop::option::of(0i64..101), trace in prop::collection::vec(any::<bool>(), 0..16),
        published in any::<bool>(), allowed in any::<bool>(), delivery in any::<bool>(),
        inputs in prop::collection::vec((0i64..110, 0i64..110, any::<u64>()), 0..24),
    ) {
        let facts = DeleteRecordsTrimFacts { requested, current_start: current, high_watermark: 100, log_end: 100,
            has_delivery_watermark: delivery, delivery_watermark: current.max(wal).max(local) };
        let previous = old.map(|floor| floor.min(wal).min(local));
        let rows: Vec<_> = inputs.iter().map(|&(a, b, size)| (a.min(b), a.max(b), size)).collect();
        check_plan(facts, (wal, local), previous, &trace, published, allowed, &rows);
    }
}

#[test]
fn cleanup_requires_publication_and_stops_at_the_first_retained_range() {
    let facts = DeleteRecordsTrimFacts {
        requested: 10,
        current_start: 0,
        high_watermark: 20,
        log_end: 20,
        has_delivery_watermark: false,
        delivery_watermark: 0,
    };
    let rows = [(0, 1, u64::MAX), (2, 9, 1), (9, 10, 1), (0, 1, 0)];
    for stores in [(0, 0), (10, 0), (0, 10), (10, 10)] {
        for previous in [None, Some(0)] {
            for trace in [&[][..], &[true], &[false, true, false, true]] {
                for published in [false, true] {
                    for allowed in [false, true] {
                        check_plan(facts, stores, previous, trace, published, allowed, &rows);
                    }
                }
            }
        }
    }
    for requested in [-2, 21] {
        check_plan(
            DeleteRecordsTrimFacts { requested, ..facts },
            (0, 0),
            None,
            &[true, true],
            true,
            true,
            &rows,
        );
    }
    let extreme = DeleteRecordsTrimFacts {
        requested: -1,
        current_start: i64::MAX,
        high_watermark: i64::MAX,
        log_end: i64::MAX,
        has_delivery_watermark: true,
        delivery_watermark: i64::MAX,
    };
    check_plan(
        extreme,
        (i64::MAX, i64::MAX),
        None,
        &[],
        true,
        true,
        &[(0, i64::MAX - 1, u64::MAX), (0, i64::MAX, 0)],
    );
}
