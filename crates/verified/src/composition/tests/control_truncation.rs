use {super::whole_batch_truncation_bounds_controls, assert2::assert, proptest::prelude::*};

fn check(ends: &[i64], start: i64, cut: i64, previous: i64, history: &[i64], need: i64) {
    let retained_batches: Vec<_> = ends.iter().copied().filter(|&end| end <= cut).collect();
    let end = retained_batches.last().copied().unwrap_or(start);
    let hwm = previous.min(end);
    let retained_history: Vec<_> = history
        .iter()
        .copied()
        .filter(|&offset| offset < end)
        .collect();
    let committed_history: Vec<_> = history
        .iter()
        .copied()
        .filter(|&offset| offset < hwm)
        .collect();
    assert!(
        whole_batch_truncation_bounds_controls(ends, start, cut, previous, history, need)
            == (
                retained_batches.len(),
                end,
                hwm,
                retained_history.len(),
                committed_history.len(),
                need <= end,
                need <= hwm,
            )
    );
}

proptest! {
    #[test]
    fn physical_prefix_controls_every_logical_frontier(
        sizes in prop::collection::vec(1_i64..20, 0..32),
        start in 0_i64..100,
        raw_cut in 0_i64..800,
        raw_hwm in 0_i64..800,
        raw_history in prop::collection::vec(-1_i64..800, 0..80),
        need in 0_i64..800,
    ) {
        let mut end = start;
        let ends: Vec<_> = sizes.iter().map(|size| {end += size; end}).collect();
        let history: Vec<_> = raw_history.into_iter().collect::<std::collections::BTreeSet<_>>().into_iter().collect();
        check(&ends, start, raw_cut.max(start), raw_hwm.min(end), &history, need);
    }
}

#[test]
fn control_rows_below_an_interior_cut_still_belong_to_the_discarded_batch() {
    let history = [-1, 0, 1, 2, 3];
    for cut in [0, 1, 2, 3, 4, i64::MAX] {
        for previous in 0..=4 {
            for need in 0..=5 {
                check(&[2, 4], 0, cut, previous, &history, need);
            }
        }
    }
    // Offset 2 is below the requested cut, but its whole batch was discarded.
    assert!(
        whole_batch_truncation_bounds_controls(&[2, 4], 0, 3, 4, &history, 3)
            == (1, 2, 2, 3, 3, false, false)
    );
}

#[test]
fn empty_shifted_and_maximum_offset_boundaries_keep_only_the_physical_prefix() {
    for (ends, start, history) in [
        (&[][..], 7, &[-1, 6, 7][..]),
        (&[9, 12][..], 7, &[-1, 6, 7, 8, 9, 11, 12][..]),
        (
            &[i64::MAX - 1, i64::MAX][..],
            i64::MAX - 3,
            &[-1, i64::MAX - 3, i64::MAX - 2, i64::MAX - 1][..],
        ),
    ] {
        let old_end = ends.last().copied().unwrap_or(start);
        for cut in [start, old_end - 1, old_end, i64::MAX]
            .into_iter()
            .filter(|cut| *cut >= start)
        {
            for previous in [0, start, old_end] {
                for need in [0, start, old_end, i64::MAX] {
                    check(ends, start, cut, previous, history, need);
                }
            }
        }
    }
}
