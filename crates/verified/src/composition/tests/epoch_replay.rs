use {
    super::{
        EpochEntry, FetchWatermarks, LeaderEpoch, Offset, resolved_epoch_bounds_retained_replay,
        validated_epochs_bound_truncated_fetch,
    },
    crate::broker::FetchVisibility,
    assert2::assert,
    proptest::prelude::*,
    std::{
        collections::BTreeMap,
        ops::Bound::{Excluded, Unbounded},
    },
};

type EpochOracle = Result<Option<(i32, FetchWatermarks, FetchVisibility)>, ()>;

fn epoch_oracle(
    entries: &[EpochEntry],
    requested: i32,
    base: i64,
    w: FetchWatermarks,
) -> EpochOracle {
    if base < 0
        || base > w.log_end
        || w.log_start < 0
        || w.log_start > w.log_end
        || entries
            .iter()
            .any(|e| e.epoch.0 < 0 || e.start_offset.0 < base || e.start_offset.0 > w.log_end)
        || entries
            .windows(2)
            .any(|e| e[0].epoch.0 >= e[1].epoch.0 || e[0].start_offset.0 >= e[1].start_offset.0)
    {
        return Err(());
    }
    let history: BTreeMap<_, _> = entries
        .iter()
        .map(|e| (e.epoch.0, e.start_offset.0))
        .collect();
    if requested == -1 {
        return Ok(None);
    }
    let (found, cut) = if history
        .last_key_value()
        .is_some_and(|(epoch, _)| *epoch == requested)
    {
        (requested, w.log_end)
    } else {
        let Some((_, &cut)) = history.range((Excluded(requested), Unbounded)).next() else {
            return Ok(None);
        };
        let found = history
            .range(..=requested)
            .next_back()
            .map_or(requested, |(epoch, _)| *epoch);
        (found, cut)
    };
    let bounded = FetchWatermarks {
        log_end: cut,
        hw: w.hw.min(cut),
        lso: w.lso.min(cut),
        deliverable: w.deliverable.min(cut),
        ..w
    };
    let visibility = FetchVisibility {
        out_of_range: w.log_start > cut,
        empty: w.log_start <= cut && w.log_start >= bounded.hw.min(bounded.deliverable),
        limit_offset: w.hw.min(w.lso).min(w.deliverable).min(cut),
        effective_lso: bounded.hw.min(bounded.lso),
        read_committed_aborts: true,
        response_hw: bounded.hw,
        response_lso: bounded.hw.min(bounded.lso),
    };
    Ok(Some((found, bounded, visibility)))
}

fn entries(rows: &[(i32, i64)]) -> Vec<EpochEntry> {
    rows.iter()
        .map(|&(epoch, start)| EpochEntry {
            epoch: LeaderEpoch(epoch),
            start_offset: Offset(start),
        })
        .collect()
}

fn check_replay(
    rows: &[(i32, i64)],
    requested: i32,
    base: i64,
    w: FetchWatermarks,
    snapshots: &[i64],
    local: i64,
) {
    let history = entries(rows);
    let expected = epoch_oracle(&history, requested, base, w);
    assert!(validated_epochs_bound_truncated_fetch(&history, requested, base, w) == expected);
    let actual =
        resolved_epoch_bounds_retained_replay(&history, requested, base, w, snapshots, local);
    if local < 0 || local > w.log_end {
        assert!(actual == Err(()));
        return;
    }
    match expected {
        Err(()) => assert!(actual == Err(())),
        Ok(None) => assert!(actual == Ok(None)),
        Ok(Some((_, bounded, _))) if bounded.log_end < w.log_start || bounded.log_end < local => {
            assert!(actual == Err(()));
        }
        Ok(Some((found, bounded, visibility))) => {
            let Ok(Some((epoch, clamped, view, selected, cursor))) = actual else {
                panic!("valid resolved replay rejected: {actual:?}");
            };
            assert!((epoch, clamped, view) == (found, bounded, visibility));
            let newest = snapshots
                .iter()
                .copied()
                .filter(|o| *o > w.log_start && *o <= bounded.log_end)
                .max();
            match (newest, selected) {
                (None, None) => assert!(cursor == w.log_start.max(local)),
                (Some(offset), Some(index)) => {
                    assert!(index < snapshots.len());
                    assert!(snapshots[index] == offset);
                    assert!(cursor == local.max(offset));
                }
                _ => panic!(
                    "snapshot selection {selected:?} does not match newest offset {newest:?}"
                ),
            }
            assert!(cursor <= bounded.log_end);
        }
    }
}

proptest! {
    #[test]
    fn epoch_replay_matches_arbitrary_history_and_window_oracles(
        rows in proptest::collection::vec((any::<i32>(), any::<i64>()), 0..12),
        requested in any::<i32>(), base in any::<i64>(),
        fields in any::<[i64; 5]>(), local in any::<i64>(),
        snapshots in proptest::collection::vec(any::<i64>(), 0..16),
    ) {
        let w = FetchWatermarks { log_start: fields[0], log_end: fields[1], hw: fields[2], lso: fields[3], deliverable: fields[4] };
        check_replay(&rows, requested, base, w, &snapshots, local);
    }

    #[test]
    fn ordered_epochs_drive_selection_against_the_resolved_cut(
        epochs in proptest::collection::btree_set(0i32..40, 0..12),
        requested in -3i32..44, floor in 0i64..70, local in 0i64..75,
        gates in any::<[i64; 3]>(),
        snapshots in proptest::collection::vec(-2i64..110, 0..24),
    ) {
        let rows: Vec<_> = epochs.into_iter().enumerate().map(|(i, epoch)| (epoch, i64::try_from(i).expect("epoch set has at most twelve entries") * 5 + 10)).collect();
        let w = FetchWatermarks { log_start: floor, log_end: 100, hw: gates[0], lso: gates[1], deliverable: gates[2] };
        check_replay(&rows, requested, 0, w, &snapshots, local);
    }
}

#[test]
fn resolved_epoch_replay_covers_gaps_pruned_floors_and_signed_extremes() {
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 100,
        hw: 99,
        lso: 98,
        deliverable: 97,
    };
    let rows = [(2, 10), (5, 50), (i32::MAX, 90)];
    for requested in [i32::MIN, -3, -1, 0, 2, 3, 5, 6, i32::MAX] {
        for local in [0, 10, 50, 70, 100, 101, -1, i64::MIN, i64::MAX] {
            check_replay(
                &rows,
                requested,
                0,
                w,
                &[100, 51, 50, 50, 10, 0, i64::MIN, i64::MAX],
                local,
            );
            check_replay(&[], requested, 0, w, &[], local);
        }
    }
    let cut_below_logical_floor = FetchWatermarks { log_start: 60, ..w };
    check_replay(&rows, 3, 0, cut_below_logical_floor, &[50, 80], 10);
    for malformed in [
        std::vec![(5, 10), (2, 20)],
        std::vec![(2, 10), (2, 20)],
        std::vec![(2, 10), (5, 10)],
        std::vec![(-1, 10)],
        std::vec![(2, -1)],
        std::vec![(2, 101)],
    ] {
        check_replay(&malformed, 2, 0, w, &[50], 0);
    }
    let maximum = FetchWatermarks {
        log_start: i64::MAX - 1,
        log_end: i64::MAX,
        hw: i64::MAX,
        lso: i64::MIN,
        deliverable: i64::MAX,
    };
    check_replay(
        &[(i32::MAX, i64::MAX - 1)],
        i32::MAX,
        0,
        maximum,
        &[i64::MAX, i64::MAX],
        i64::MAX,
    );
}
