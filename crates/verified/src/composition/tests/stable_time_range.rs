use {super::*, assert2::assert};

fn maximum_offset_watermarks() -> FetchWatermarks {
    FetchWatermarks {
        log_start: 0,
        log_end: i64::MAX,
        hw: i64::MAX,
        lso: i64::MIN,
        deliverable: i64::MAX,
    }
}

fn signed_extreme_window() -> SparseTimestampWindow<'static> {
    (&[0, 1], &[i64::MIN, i64::MAX], &[(0, 0), (1, 1)])
}

fn check(
    window: SparseTimestampWindow<'_>,
    starts: &[i64],
    span: (i64, i64),
    w: FetchWatermarks,
    targets: (i64, i64),
) {
    let (offsets, times, _) = window;
    let valid = !starts.iter().any(|start| *start > w.log_end)
        && span
            .1
            .checked_sub(span.0)
            .is_some_and(|width| (0..=i64::from(u32::MAX)).contains(&width))
        && offsets
            .iter()
            .all(|&offset| span.0 + i64::from(offset) <= span.1);
    let expected = if valid {
        let lso = starts.iter().copied().min().unwrap_or(w.log_end);
        let limit = [w.log_end, w.hw, w.deliverable]
            .into_iter()
            .chain(starts.iter().copied())
            .min()
            .unwrap();
        let first = offsets.iter().zip(times).position(|(&offset, &time)| {
            let absolute = span.0 + i64::from(offset);
            w.log_start <= absolute && absolute < limit && targets.0 <= time && time <= targets.1
        });
        Ok((lso, limit, first))
    } else {
        Err(())
    };
    assert!(stable_time_range_preserves_first(window, starts, span, w, targets) == expected);
}

proptest! {
    #[test]
    fn stable_interval_matches_full_record_and_independent_frontier_oracles(
        records in prop::collection::btree_map(0u32..100, any::<i64>(), 0..24),
        starts in prop::collection::vec(0i64..201, 0..12), base in 0i64..101,
        width in 0u32..101, floor in 0i64..201, hw in 0i64..201, delivery in 0i64..201,
        inherited_lso in any::<i64>(), a in any::<i64>(), b in any::<i64>(), step in 1usize..8,
    ) {
        let (offsets, times, rows) = timestamp_records(&records, step, 0);
        let w = FetchWatermarks { log_start: floor, log_end: base + 100, hw, lso: inherited_lso, deliverable: delivery };
        let window = (&offsets[..], &times[..], &rows[..]);
        check(window, &starts, (base, base + i64::from(width)), w, (a.min(b), a.max(b)));
        let valid_starts: Vec<_> = starts.iter().copied().filter(|start| *start <= w.log_end).collect();
        check(window, &valid_starts, (base, base + 100), w, (a.min(b), a.max(b)));
        let visible = FetchWatermarks { log_start: base + floor % 100, hw: w.log_end, deliverable: w.log_end, ..w };
        check(window, &[], (base, base + 100), visible, (a.min(b), a.max(b)));
    }

    #[test]
    fn stable_interval_preserves_signed_frontier_and_timestamp_extremes(
        records in prop::collection::btree_map(any::<u32>(), any::<i64>(), 0..16),
        starts in prop::collection::vec(any::<i64>(), 0..8), end in any::<i64>(),
        floor in any::<u32>(), hw in any::<i64>(), delivery in any::<i64>(), inherited_lso in any::<i64>(),
        a in any::<i64>(), b in any::<i64>(),
    ) {
        let (offsets, times, rows) = timestamp_records(&records, 1, 0);
        let w = FetchWatermarks { log_start: i64::from(floor), log_end: end, hw, lso: inherited_lso, deliverable: delivery };
        check((&offsets, &times, &rows), &starts, (0, i64::from(u32::MAX)), w, (a.min(b), a.max(b)));
    }
}

#[test]
fn derived_offset_caps_preserve_regressions_and_override_stale_lso() {
    let window = (
        &[0, 3, 6][..],
        &[100, 300, 200][..],
        &[(0, 0), (1, 1), (2, 2)][..],
    );
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 7,
        hw: 7,
        lso: 0,
        deliverable: 7,
    };
    assert!(
        stable_time_range_preserves_first(window, &[], (0, 6), w, (200, 250))
            == Ok((7, 7, Some(2)))
    );
    for starts in [&[][..], &[3], &[6], &[7], &[8], &[-1], &[i64::MIN]] {
        for floor in [0, 1, 3, 4, 6, 7] {
            for limit in [0, 3, 6, 7] {
                check(
                    window,
                    starts,
                    (0, 6),
                    FetchWatermarks {
                        log_start: floor,
                        hw: limit,
                        lso: i64::MAX,
                        ..w
                    },
                    (200, 250),
                );
                check(
                    window,
                    starts,
                    (0, 6),
                    FetchWatermarks {
                        log_start: floor,
                        deliverable: limit,
                        lso: i64::MAX,
                        ..w
                    },
                    (200, 250),
                );
            }
        }
    }
    assert!(
        stable_time_range_preserves_first(window, &[6], (0, 6), w, (200, 250)) == Ok((6, 6, None))
    );
    // Minimum equivalence preserves visibility, not validation of omitted rows.
    assert!(stable_time_range_preserves_first(window, &[3, 8], (0, 6), w, (200, 250)) == Err(()));
    assert!(
        stable_time_range_preserves_first(window, &[3], (0, 6), w, (200, 250)) == Ok((3, 3, None))
    );
    check(window, &[], (0, 5), w, (200, 250));
    check(window, &[0], (0, 5), w, (200, 250));
    let repeated = (
        &[0, 3, 6][..],
        &[100, 100, 300][..],
        &[(0, 0), (1, 1), (2, 2)][..],
    );
    check(repeated, &[], (0, 6), w, (100, 100));
    check(
        repeated,
        &[],
        (0, 6),
        FetchWatermarks { log_start: 1, ..w },
        (100, 100),
    );
}

#[yare::parameterized(
    negative_width = { (0, -1) },
    excessive_width = { (0, i64::MAX) },
    maximum_zero_width = { (i64::MAX, i64::MAX) },
)]
fn stable_interval_checks_complete_admission_and_exclusive_maximum_offset(span: (i64, i64)) {
    let w = maximum_offset_watermarks();
    check((&[], &[], &[]), &[], span, w, (i64::MIN, i64::MAX));
}

#[yare::parameterized(
    no_start = { &[][..] },
    minimum_start = { &[i64::MIN][..] },
    maximum_start = { &[i64::MAX][..] },
)]
fn stable_interval_handles_signed_start_frontiers(starts: &[i64]) {
    let w = maximum_offset_watermarks();
    let window = signed_extreme_window();
    check(
        window,
        starts,
        (i64::MAX - 1, i64::MAX),
        FetchWatermarks {
            log_start: i64::MAX - 1,
            ..w
        },
        (i64::MIN, i64::MAX),
    );
    check(
        window,
        starts,
        (i64::MAX - 1, i64::MAX),
        w,
        (i64::MAX, i64::MAX),
    );
}

#[test]
fn stable_interval_keeps_exclusive_maximum_offset() {
    let w = maximum_offset_watermarks();
    let window = signed_extreme_window();
    assert!(
        stable_time_range_preserves_first(
            window,
            &[],
            (i64::MAX - 1, i64::MAX),
            w,
            (i64::MAX, i64::MAX)
        ) == Ok((i64::MAX, i64::MAX, None))
    );
    check(
        (&[0], &[0], &[(0, 0)]),
        &[1],
        (0, 0),
        FetchWatermarks { log_end: 0, ..w },
        (0, 0),
    );
}
