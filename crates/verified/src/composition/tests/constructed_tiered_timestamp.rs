use {super::*, crate::list_offsets::ListOffsetsSelectionDecision, assert2::assert};

fn check_window(window: SparseTimestampWindow<'_>, base: i64, floor: i64, target: i64) {
    let (offsets, times, rows) = window;
    let expected_rows: Vec<_> = prefix_maxima(window);
    let untrimmed = times.iter().position(|time| *time >= target);
    assert!(
        constructed_time_index_preserves_first(offsets, times, rows, target)
            == ConstructedTimeIndex {
                entries: expected_rows,
                selected: untrimmed
            }
    );
    let retained = offsets
        .iter()
        .zip(times)
        .position(|(&offset, &time)| base + i64::from(offset) >= floor && time >= target);
    assert!(constructed_index_retained_candidate(window, base, floor, target) == retained);
}

fn check_tiers(
    remote: SparseTimestampWindow<'_>,
    local: SparseTimestampWindow<'_>,
    bases: (i64, i64),
    request: (i64, i64, i64),
    epoch: i32,
) {
    let (target, floor, bound) = request;
    check_window(remote, bases.0, floor, target);
    check_window(local, bases.1, floor, target);
    let records = remote
        .0
        .iter()
        .zip(remote.1)
        .map(|(&offset, &time)| (bases.0 + i64::from(offset), time))
        .chain(
            local
                .0
                .iter()
                .zip(local.1)
                .map(|(&offset, &time)| (bases.1 + i64::from(offset), time)),
        );
    let expected = records
        .filter(|&(offset, time)| offset >= floor && offset < bound && time >= target)
        .min_by_key(|&(offset, _)| offset)
        .map_or(
            ListOffsetsSelectionDecision::Unknown,
            |(offset, timestamp)| ListOffsetsSelectionDecision::Resolved {
                offset,
                timestamp,
                leader_epoch: epoch,
            },
        );
    assert!(
        constructed_tiered_timestamp_preserves_first(remote, local, bases, request, epoch)
            == expected
    );
}

proptest! {
    #[test]
    fn constructed_rows_and_retained_candidates_match_independent_oracles(
        records in proptest::collection::btree_map(any::<u32>(), any::<i64>(), 0..24),
        target in any::<i64>(), floor in 0i64..=i64::MAX,
        base in 0i64..=i64::MAX - i64::from(u32::MAX),
        step in 1usize..8, span in 0usize..8,
    ) {
        let (offsets, times, rows) = timestamp_records(&records, step, span);
        check_window((&offsets, &times, &rows), base, floor, target);
    }

    #[test]
    fn constructed_tiers_match_the_complete_retained_visible_union(
        remote in proptest::collection::btree_map(0u32..1000, any::<i64>(), 0..24),
        local in proptest::collection::btree_map(0u32..1000, any::<i64>(), 0..24),
        bases in (0i64..100, 0i64..100), target in 0i64..=i64::MAX,
        floor in 0i64..1200, bound in 0i64..1200, epoch in -1i32..=i32::MAX,
        step in 1usize..8, span in 0usize..8,
    ) {
        let (ro, rt, rr) = timestamp_records(&remote, step, span);
        let (lo, lt, lr) = timestamp_records(&local, step, span);
        check_tiers((&ro, &rt, &rr), (&lo, &lt, &lr), bases, (target, floor, bound), epoch);
    }
}

#[test]
fn constructed_tiers_preserve_retained_matches_across_regressions_and_overlap() {
    let remote = (
        &[0, 20, 30, 40][..],
        &[100, 1, 80, 200][..],
        &[(1, 2), (3, 3)][..],
    );
    let local = (
        &[5, 15, 30, 50][..],
        &[100, 70, 300, 70][..],
        &[(0, 1), (2, 3)][..],
    );
    for target in [0, 1, 70, 80, 100, 200, 300, i64::MAX] {
        for floor in [0, 1, 10, 15, 30, 40, 51, i64::MAX] {
            for bound in [0, 15, 30, 40, 50, 51, i64::MAX] {
                check_tiers(remote, local, (0, 0), (target, floor, bound), 7);
            }
        }
    }
    check_tiers(remote, local, (100, 0), (70, 10, 1000), -1);
    check_tiers((&[], &[], &[]), local, (0, 0), (70, 10, 51), 1);
    check_tiers(remote, (&[], &[], &[]), (0, 0), (70, 10, 51), 1);
    // A retained first raw match at offset 30 lies exactly on the exclusive bound.
    assert!(
        constructed_tiered_timestamp_preserves_first(
            remote,
            (&[], &[], &[]),
            (0, 0),
            (70, 10, 30),
            1
        ) == ListOffsetsSelectionDecision::Unknown
    );
}

#[test]
fn constructed_tiers_cover_empty_indexes_tied_maxima_and_extreme_coordinates() {
    let offsets = [0, 1, u32::MAX];
    let times = [i64::MAX, i64::MIN, i64::MAX];
    for rows in [&[][..], &[(0, 0), (1, 2), (2, 2)][..]] {
        let window = (&offsets[..], &times[..], rows);
        let base = i64::MAX - i64::from(u32::MAX);
        for floor in [0, base, base + 1, i64::MAX] {
            for target in [i64::MIN, 0, i64::MAX] {
                check_window(window, base, floor, target);
            }
            check_tiers(
                window,
                window,
                (base, base),
                (i64::MAX, floor, i64::MAX),
                i32::MAX,
            );
        }
    }
    check_tiers(
        (&[], &[], &[]),
        (&[], &[], &[]),
        (i64::MAX, 0),
        (0, 0, 0),
        -1,
    );
}
