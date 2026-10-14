use {
    super::{FetchWatermarks, scheduled_prefix_bounds_fetch, scheduled_stable_prefix_bounds_fetch},
    crate::broker::FetchVisibility,
    assert2::assert,
    proptest::prelude::*,
};

fn delivery_oracle(
    batches: &[(i64, i32)],
    times: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) -> Option<i64> {
    let mut cursor = i128::from(w.log_start);
    for &(base, delta) in batches {
        let end = i128::from(base) + i128::from(delta) + 1;
        if delta < 0 || i128::from(base) < cursor || end > i128::from(w.log_end) {
            return None;
        }
        cursor = end;
    }
    if cursor != i128::from(w.log_end) {
        return None;
    }
    Some(
        batches
            .iter()
            .zip(times)
            .filter_map(|(&(base, _), &time)| {
                let deadline = i128::from(time) + i128::from(uncertainty);
                (uncertainty < 0 || deadline > i128::from(i64::MAX) || deadline > i128::from(now))
                    .then_some(base)
            })
            .min()
            .unwrap_or(w.log_end),
    )
}

fn visibility_oracle(
    w: FetchWatermarks,
    delivery: i64,
    lso: i64,
    follower: bool,
) -> FetchVisibility {
    let stable = w.hw.min(lso);
    FetchVisibility {
        limit_offset: if follower {
            w.log_end
        } else {
            stable.min(delivery)
        },
        response_hw: w.hw,
        response_lso: stable,
        effective_lso: if follower { lso } else { stable },
        read_committed_aborts: !follower,
        out_of_range: false,
        empty: if follower {
            w.log_start == w.log_end
        } else {
            w.log_start >= w.hw.min(delivery)
        },
    }
}

fn check_witness(
    batches: &[(i64, i32)],
    times: &[i64],
    starts: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) {
    let delivery = delivery_oracle(batches, times, uncertainty, now, w);
    let expected = delivery.map(|frontier| {
        (
            frontier,
            visibility_oracle(w, frontier, w.lso, false),
            visibility_oracle(w, frontier, w.lso, true),
        )
    });
    assert!(scheduled_prefix_bounds_fetch(batches, times, uncertainty, now, w) == expected);
    let lso = if starts.iter().any(|start| *start > w.log_end) {
        None
    } else {
        Some(starts.iter().copied().min().unwrap_or(w.log_end))
    };
    let combined = delivery.zip(lso).map(|(frontier, stable)| {
        (
            frontier,
            stable,
            visibility_oracle(w, frontier, stable, false),
            visibility_oracle(w, frontier, stable, true),
        )
    });
    assert!(
        scheduled_stable_prefix_bounds_fetch(batches, times, starts, uncertainty, now, w)
            == combined
    );
}

proptest! {
    #[test]
    fn raw_batch_walk_and_stability_match_wide_oracles(
        rows in prop::collection::vec((any::<i64>(), any::<i32>(), any::<i64>()), 0..8),
        starts in prop::collection::vec(any::<i64>(), 0..8),
        start in 0_i64..100, extent in 0_i64..1000,
        uncertainty in any::<i64>(), now in any::<i64>(), hw in any::<i64>(),
        inherited_lso in any::<i64>(), inherited_delivery in any::<i64>(),
    ) {
        let batches: Vec<_> = rows.iter().map(|row| (row.0, row.1)).collect();
        let times: Vec<_> = rows.iter().map(|row| row.2).collect();
        let w = FetchWatermarks { log_start: start, log_end: start + extent, hw,
            lso: inherited_lso, deliverable: inherited_delivery };
        check_witness(&batches, &times, &starts, uncertainty, now, w);
    }

    #[test]
    fn complete_gapped_walks_compose_both_visibility_gates(
        rows in prop::collection::vec((0_i64..5, 0_i32..5, any::<i64>()), 0..8),
        starts in prop::collection::vec(-10_i64..150, 0..8),
        start in 0_i64..10, uncertainty in any::<i64>(), now in any::<i64>(),
        hw in -10_i64..150, inherited_lso in any::<i64>(), inherited_delivery in any::<i64>(),
    ) {
        let mut cursor = start;
        let mut batches = Vec::new();
        let mut times = Vec::new();
        for (gap, delta, time) in rows {
            let base = cursor + gap;
            cursor = base + i64::from(delta) + 1;
            batches.push((base, delta)); times.push(time);
        }
        let w = FetchWatermarks { log_start: start, log_end: cursor, hw,
            lso: inherited_lso, deliverable: inherited_delivery };
        check_witness(&batches, &times, &starts, uncertainty, now, w);
    }
}

#[test]
fn schedules_reject_corrupt_tails_and_ignore_stale_gate_fields() {
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 6,
        hw: 6,
        lso: i64::MIN,
        deliverable: i64::MIN,
    };
    for (batches, times) in super::VALID_SCHEDULES.into_iter().chain([
        (&[(0, 1), (1, 1)][..], &[0, 0][..]),
        (&[(0, 1), (4, -1)][..], &[0, 0][..]),
        (&[(0, 1)][..], &[0][..]),
        (&[][..], &[][..]),
        (&[(0, 5)][..], &[i64::MAX][..]),
    ]) {
        for now in [i64::MIN, 0, 100, i64::MAX] {
            for uncertainty in [-1, 0, 1, i64::MAX] {
                for starts in [&[][..], &[1, 5][..], &[3, 1, 3][..], &[7][..]] {
                    check_witness(batches, times, starts, uncertainty, now, w);
                }
            }
        }
    }
}

#[yare::parameterized(
    empty_log = { 0, 0, &[][..], &[][..] },
    maximum_empty_batch = { i64::MAX - 1, i64::MAX, &[(i64::MAX - 1, 0)][..], &[i64::MIN][..] },
    maximum_end_mismatch = { 0, i64::MAX, &[(i64::MAX, 0)][..], &[0][..] },
)]
fn schedules_handle_empty_and_signed_extreme_extents(
    start: i64,
    end: i64,
    batches: &[(i64, i32)],
    times: &[i64],
) {
    check_witness(batches, times, &[], 0, i64::MAX, FetchWatermarks {
        log_start: start,
        log_end: end,
        hw: end,
        lso: i64::MIN,
        deliverable: i64::MIN,
    });
}
