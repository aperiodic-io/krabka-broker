use {
    super::{
        ProducerDecision, ProducerSnapshotEntryFacts, oracle_next_producer_decision,
        oracle_retry_matches, oracle_sequence as sequence, recovered_window_row as row,
        truncated_replay_bounds_first_retry,
    },
    assert2::assert,
    proptest::prelude::*,
};

fn check(
    ends: &[i64],
    start: i64,
    cut: i64,
    hwm: i64,
    rows: &[ProducerSnapshotEntryFacts],
    request: (i16, i32, i32, bool),
) {
    let end = ends
        .iter()
        .copied()
        .filter(|end| *end <= cut)
        .max()
        .unwrap_or(start);
    let kept: Vec<_> = rows.iter().filter(|row| row.last_offset < cut).collect();
    let window: Vec<_> = kept
        .iter()
        .rev()
        .take_while(|row| row.producer_epoch == kept.last().unwrap().producer_epoch)
        .take(5)
        .copied()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    let first = kept.len() - window.len();
    let match_index = window
        .iter()
        .position(|row| oracle_retry_matches(row, request));
    let decision = if let Some(index) = match_index {
        ProducerDecision::Duplicate {
            retained: if index + 1 == window.len() { 4 } else { index },
        }
    } else {
        oracle_next_producer_decision(kept.last().copied(), request, end)
    };
    let witness = match_index.map(|index| {
        let row = window[index];
        (
            first + index,
            row.last_offset - i64::from(row.offset_delta),
            row.last_offset + 1,
            hwm > row.last_offset,
        )
    });
    assert!(
        truncated_replay_bounds_first_retry(ends, start, cut, hwm, rows, request)
            == (end, kept.len(), first, decision, witness)
    );
}

proptest! {
    #[test]
    fn physical_truncation_bounds_rebuilt_first_alias_and_acknowledgement(
        widths in prop::collection::vec(1_i32..17, 1..33),
        start in 0_i64..100, cut in 0_i64..400, hwm in 0_i64..400,
        mask in any::<u64>(), same_epoch in any::<bool>(), epoch in any::<i16>(), base_sequence in 0_i32..=i32::MAX,
        delta in 0_i32..20, trunk in any::<bool>(),
    ) {
        let mut end = start;
        let mut rows = Vec::new();
        let mut ends = Vec::new();
        for (index, width) in widths.iter().copied().enumerate() {
            if index == 0 || (mask & (1 << index) != 0) {
                let mut entry = row(end, width - 1, sequence(end + i64::from(width) - 1));
                if !same_epoch { entry.producer_epoch += i16::try_from(rows.len() / 4).unwrap(); }
                rows.push(entry);
            }
            end += i64::from(width);
            ends.push(end);
        }
        let cut = cut.max(start);
        let hwm = hwm.min(end);
        check(&ends, start, cut, hwm, &rows, (epoch, base_sequence, delta, trunk));
        for row in &rows {
            check(&ends, start, cut, hwm, &rows,
                (row.producer_epoch, sequence(i64::from(row.last_sequence) - i64::from(row.offset_delta)), row.offset_delta, trunk));
        }
    }
}

#[test]
fn interior_cuts_delete_whole_retry_spans_and_repack_surviving_slots() {
    let rows = [row(0, 1, 1), row(3, 2, 4), row(6, 0, 5)];
    let ends = [2, 3, 6, 7]; // A different producer occupies the middle gap.
    for cut in 0..=8 {
        for hwm in 0..=7 {
            for trunk in [false, true] {
                for request in [
                    (7, 0, 1, trunk),
                    (7, 2, 2, trunk),
                    (7, 5, 0, trunk),
                    (6, 0, 1, trunk),
                    (8, 0, 1, trunk),
                ] {
                    check(&ends, 0, cut, hwm, &rows, request);
                }
            }
        }
    }
    assert!(
        truncated_replay_bounds_first_retry(&ends, 0, 5, 7, &rows, (7, 0, 1, false))
            == (
                3,
                1,
                0,
                ProducerDecision::Duplicate { retained: 4 },
                Some((0, 0, 2, true))
            )
    );
}

#[test]
fn wrap_aliases_and_maximum_offsets_keep_the_original_first_coordinates() {
    let rows = [
        row(0, 0, 0),
        row(1, i32::MAX - 1, i32::MAX),
        row(i64::from(i32::MAX) + 1, 0, 0),
    ];
    let ends: Vec<_> = rows.iter().map(|row| row.last_offset + 1).collect();
    for cut in [0, 1, 2, ends[1], ends[2]] {
        for hwm in [0, 1, ends[1], ends[2]] {
            check(&ends, 0, cut, hwm, &rows, (7, 0, 0, false));
            check(&ends, 0, cut, hwm, &rows, (7, 1, i32::MAX - 1, false));
        }
    }
    let rows = [row(i64::MAX - 2, 1, 0)];
    for cut in [i64::MAX - 2, i64::MAX - 1, i64::MAX] {
        check(
            &[i64::MAX],
            i64::MAX - 2,
            cut,
            i64::MAX,
            &rows,
            (7, i32::MAX, 1, false),
        );
    }
}

#[test]
fn tail_deletion_can_reintroduce_an_older_alias_or_producer_epoch() {
    let wrap = i64::from(i32::MAX) + 1;
    let rows = [
        row(0, 0, 0),
        row(1, i32::MAX - 1, i32::MAX),
        row(wrap, 0, 0),
        row(wrap + 1, 0, 1),
        row(wrap + 2, 0, 2),
        row(wrap + 3, 0, 3),
    ];
    let ends: Vec<_> = rows.iter().map(|row| row.last_offset + 1).collect();
    assert!(
        truncated_replay_bounds_first_retry(&ends, 0, ends[5], ends[5], &rows, (7, 0, 0, false)).4
            == Some((2, wrap, wrap + 1, true))
    );
    assert!(
        truncated_replay_bounds_first_retry(&ends, 0, ends[4], ends[5], &rows, (7, 0, 0, false)).4
            == Some((0, 0, 1, true))
    );
    for cut in ends.iter().copied() {
        check(&ends, 0, cut, ends[5], &rows, (7, 0, 0, false));
    }
    let mut older = row(0, 1, 1);
    older.producer_epoch = 6;
    let rows = [older, row(2, 1, 1)];
    assert!(
        truncated_replay_bounds_first_retry(&[2, 4], 0, 4, 4, &rows, (6, 0, 1, false)).3
            == ProducerDecision::Fenced
    );
    assert!(
        truncated_replay_bounds_first_retry(&[2, 4], 0, 3, 4, &rows, (6, 0, 1, false)).4
            == Some((0, 0, 2, true))
    );
    check(&[2, 4], 0, 3, 4, &rows, (6, 0, 1, false));
}
