use {
    super::{
        ProducerDecision, ProducerReloadRange, ProducerSnapshotEntryFacts,
        loaded_snapshot_bounds_truncated_retry, oracle_next_producer_decision,
        oracle_retry_matches, oracle_sequence as sequence, recovered_window_row as row,
    },
    assert2::assert,
    proptest::prelude::*,
};

type Row = ProducerSnapshotEntryFacts;

fn check(
    ends: &[i64],
    start: i64,
    range: ProducerReloadRange,
    bounds: (i64, i64),
    loaded: Option<(i64, Option<Row>)>,
    tail: &[Row],
    request: (i16, i32, i32, bool),
) {
    let end = ends
        .iter()
        .copied()
        .filter(|end| *end <= bounds.0)
        .max()
        .unwrap_or(start);
    let admissible = range.log_start <= end
        && range.local_start <= end
        && loaded.is_none_or(|(offset, seed)| {
            offset > range.log_start
                && offset <= end
                && seed.is_none_or(|seed| {
                    seed.last_offset >= 0
                        && seed.last_offset < offset
                        && seed.last_sequence >= 0
                        && seed.offset_delta >= 0
                        && i64::from(seed.offset_delta) <= seed.last_offset
                })
        });
    let actual =
        loaded_snapshot_bounds_truncated_retry(ends, start, range, bounds, loaded, tail, request);
    if !admissible {
        assert!(actual == (end, None));
        return;
    }
    let cursor = loaded
        .map_or(range.log_start, |(offset, _)| offset)
        .max(range.local_start);
    let seed = loaded.and_then(|(_, seed)| seed);
    let mut rows: Vec<_> = seed.into_iter().collect();
    let mut origins = if seed.is_some() {
        vec![None]
    } else {
        Vec::new()
    };
    for (index, row) in tail
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, row)| row.last_offset >= cursor && row.last_offset < end)
    {
        rows.push(row);
        origins.push(Some(index));
    }
    let width = rows
        .iter()
        .rev()
        .take_while(|row| row.producer_epoch == rows.last().unwrap().producer_epoch)
        .take(5)
        .count();
    let first = rows.len() - width;
    let found = rows
        .iter()
        .enumerate()
        .skip(first)
        .find(|(_, row)| oracle_retry_matches(row, request));
    let decision = if let Some((index, _)) = found {
        ProducerDecision::Duplicate {
            retained: if index + 1 == rows.len() {
                4
            } else {
                index - first
            },
        }
    } else {
        oracle_next_producer_decision(rows.last(), request, end)
    };
    let witness = found.map(|(index, row)| {
        (
            origins[index],
            row.last_offset - i64::from(row.offset_delta),
            row.last_offset + 1,
            bounds.1 > row.last_offset,
        )
    });
    assert!(
        actual
            == (
                end,
                Some((cursor, seed.is_some(), origins, first, decision, witness))
            )
    );
}

proptest! {
    #[test]
    fn snapshot_seed_and_exact_tail_match_an_independent_window_oracle(
        widths in prop::collection::vec(1_i32..10, 0..25), start in 0_i64..50,
        logical in 0_i64..250, local in 0_i64..250, cut in 0_i64..250, hwm in 0_i64..300,
        mask in any::<u32>(), mode in 0_u8..4, snapshot in -1_i64..300,
        epoch in any::<i16>(), base in 0_i32..=i32::MAX, delta in 0_i32..10, trunk in any::<bool>(),
    ) {
        let mut end = start;
        let mut ends = Vec::new();
        let mut tail = Vec::new();
        for (index, width) in widths.into_iter().enumerate() {
            if mask & (1 << index) != 0 {
                let mut entry = row(end, width - 1, sequence(end + i64::from(width) - 1));
                entry.producer_epoch += i16::try_from(index / 6).unwrap();
                tail.push(entry);
            }
            end += i64::from(width); ends.push(end);
        }
        let range = ProducerReloadRange { log_start: logical.min(end), local_start: local.max(start).min(end), log_end: end };
        let mut seed = row(0, 0, 0);
        if mode == 3 { seed.last_offset = -1; seed.last_sequence = -1; }
        let loaded = match mode { 0 => None, 1 => Some((snapshot, None)), _ => Some((snapshot, Some(seed))) };
        let bounds = (cut.max(start), hwm.min(end));
        check(&ends, start, range, bounds, loaded, &tail, (epoch, base, delta, trunk));
        for entry in tail.iter().chain(loaded.and_then(|(_, entry)| entry).iter()) {
            if entry.last_sequence >= 0 {
                check(&ends, start, range, bounds, loaded, &tail,
                    (entry.producer_epoch, sequence(i64::from(entry.last_sequence) - i64::from(entry.offset_delta)), entry.offset_delta, trunk));
            }
        }
    }
}

#[test]
fn snapshots_carry_retries_below_floors_and_tail_capacity_can_evict_the_seed() {
    let seed = row(0, 1, 1);
    let range = ProducerReloadRange {
        log_start: 11,
        local_start: 10,
        log_end: 12,
    };
    let loaded = Some((12, Some(seed)));
    let actual = loaded_snapshot_bounds_truncated_retry(
        &[12],
        10,
        range,
        (12, 2),
        loaded,
        &[],
        (7, 0, 1, false),
    );
    assert!(actual.1.unwrap().5 == Some((None, 0, 2, true)));
    check(&[12], 10, range, (12, 2), loaded, &[], (7, 0, 1, false));
    let tail: Vec<_> = (0..6).map(|index| row(10 + index, 0, 0)).collect();
    let ends: Vec<_> = tail.iter().map(|row| row.last_offset + 1).collect();
    let range = ProducerReloadRange {
        log_start: 0,
        local_start: 10,
        log_end: 16,
    };
    let loaded = Some((2, Some(row(0, 0, 0))));
    check(&ends, 10, range, (16, 16), loaded, &tail, (7, 0, 0, false));
    assert!(
        loaded_snapshot_bounds_truncated_retry(
            &ends,
            10,
            range,
            (16, 16),
            loaded,
            &tail,
            (7, 0, 0, false)
        )
        .1
        .unwrap()
        .5 == Some((Some(1), 11, 12, true))
    );
}

#[test]
fn interior_replay_cursors_keep_whole_spans_and_stale_or_marker_seeds_are_rejected() {
    let tail = [row(0, 3, 3), row(4, 0, 4)];
    let range = ProducerReloadRange {
        log_start: 1,
        local_start: 2,
        log_end: 5,
    };
    for cut in 0..=6 {
        for hwm in 0..=5 {
            for loaded in [None, Some((4, None)), Some((5, Some(row(0, 0, 0))))] {
                check(
                    &[4, 5],
                    0,
                    range,
                    (cut, hwm),
                    loaded,
                    &tail,
                    (7, 0, 3, false),
                );
            }
        }
    }
    assert!(
        loaded_snapshot_bounds_truncated_retry(
            &[4, 5],
            0,
            range,
            (4, 4),
            None,
            &tail,
            (7, 0, 3, false)
        )
        .1
        .unwrap()
        .5 == Some((Some(0), 0, 4, true))
    );
    let mut marker = row(0, 0, 0);
    marker.last_offset = -1;
    marker.last_sequence = -1;
    check(
        &[4, 5],
        0,
        range,
        (5, 5),
        Some((4, Some(marker))),
        &tail,
        (7, 0, 0, false),
    );
    let tail = [row(i64::MAX - 2, 1, 0)];
    let range = ProducerReloadRange {
        log_start: i64::MAX - 2,
        local_start: i64::MAX - 1,
        log_end: i64::MAX,
    };
    check(
        &[i64::MAX],
        i64::MAX - 2,
        range,
        (i64::MAX, i64::MAX),
        None,
        &tail,
        (7, i32::MAX, 1, false),
    );
}
