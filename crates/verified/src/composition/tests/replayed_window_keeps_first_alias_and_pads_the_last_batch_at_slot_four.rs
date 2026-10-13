use {super::*, assert2::assert};

#[test]
fn replayed_window_keeps_first_alias_and_pads_the_last_batch_at_slot_four() {
    // A complete sequence-space wrap aliases the first and last batches.
    // Even the successor sequence can match an earlier retained batch.
    let rows = [
        recovered_window_row(0, 0, 0),
        recovered_window_row(1, i32::MAX - 1, i32::MAX),
        recovered_window_row(i64::from(i32::MAX) + 1, 0, 0),
    ];
    let end = rows[2].last_offset + 1;
    assert!(
        replayed_window_preserves_first_retry_coordinates(end, &rows, (7, 0, 0))
            == (ProducerDecision::Duplicate { retained: 0 }, Some((0, 0, 1)))
    );
    assert!(
        replayed_window_preserves_first_retry_coordinates(end, &rows, (7, 1, i32::MAX - 1))
            == (
                ProducerDecision::Duplicate { retained: 1 },
                Some((1, 1, i64::from(i32::MAX) + 1))
            )
    );
    let rows = [recovered_window_row(i64::MAX - 2, 1, 0)];
    assert!(
        replayed_window_preserves_first_retry_coordinates(i64::MAX, &rows, (7, i32::MAX, 1))
            == (
                ProducerDecision::Duplicate { retained: 4 },
                Some((0, i64::MAX - 2, i64::MAX))
            )
    );
}

proptest! {
    #[test]
    fn corrupt_snapshot_fallback_matches_latest_noncorrupt_and_io_oracle(
        snapshots in proptest::collection::vec((any::<i64>(), 0u8..=2), 0..24),
        (range, cut) in replay_range_cases(),
    ) {
        let start = range.log_start;
        let offsets: Vec<_> = snapshots.iter().map(|row| row.0).collect();
        let outcomes: Vec<_> = snapshots.iter().map(|row| row.1).collect();
        let expected = snapshots.iter().filter(|(offset, outcome)|
            start < *offset && *offset <= cut && *outcome != 1)
            .map(|row| row.0).max();
        match corrupt_snapshot_fallback_preserves_replay(&offsets, &outcomes, range, cut) {
            Ok((None, cursor)) => { assert!(expected == None && cursor == start.max(range.local_start)); },
            Ok((Some(index), cursor)) => {
                assert!(outcomes[index] == 0 && expected == Some(offsets[index]));
                assert!(cursor == range.local_start.max(offsets[index]));
            }
            Err(index) => { assert!(outcomes[index] == 2 && expected == Some(offsets[index])); },
        }
    }
}

#[test]
fn corrupt_snapshot_fallback_retries_only_corruption_and_keeps_original_identity() {
    let range = ProducerReloadRange {
        log_start: 3,
        local_start: 7,
        log_end: 20,
    };
    let offsets = [20, 5, 10, 3, 8, 9];
    for (outcomes, expected) in [
        ([0, 0, 1, 0, 0, 1], Ok((Some(4), 8))),
        ([0, 0, 1, 0, 1, 1], Ok((Some(1), 7))),
        ([0, 0, 1, 0, 2, 1], Err(4)),
        ([0, 1, 1, 0, 1, 1], Ok((None, 7))),
    ] {
        assert!(
            corrupt_snapshot_fallback_preserves_replay(&offsets, &outcomes, range, 10) == expected
        );
    }
    assert!(
        corrupt_snapshot_fallback_preserves_replay(&[9, 9, 9], &[1, 1, 0], range, 10)
            == Ok((Some(2), 9))
    );
    assert!(corrupt_snapshot_fallback_preserves_replay(&[], &[], range, 10) == Ok((None, 7)));
    let range = ProducerReloadRange {
        log_start: i64::MAX - 2,
        local_start: i64::MAX - 1,
        log_end: i64::MAX,
    };
    assert!(
        corrupt_snapshot_fallback_preserves_replay(
            &[i64::MAX, i64::MAX - 1],
            &[1, 0],
            range,
            i64::MAX
        ) == Ok((Some(1), i64::MAX - 1))
    );
}

proptest! {
    #[test]
    fn truncated_snapshot_matches_latest_survivor_and_exact_replay_oracles(
        offsets in proptest::collection::vec(any::<i64>(), 0..24),
        (range, cut) in replay_range_cases(),
    ) {
        let start = range.log_start;
        let local = range.local_start;
        let expected = offsets.iter().copied()
            .filter(|offset| start < *offset && *offset <= cut).max();
        let (selected, cursor) = truncated_snapshot_selection_bounds_replay(&offsets, range, cut).unwrap();
        assert!(selected.map(|index| offsets[index]) == expected);
        assert!(cursor == expected.unwrap_or(start).max(local));
    }
}

#[test]
fn truncated_snapshot_replay_preserves_exact_boundaries_and_local_floor() {
    let range = ProducerReloadRange {
        log_start: 3,
        local_start: 7,
        log_end: 20,
    };
    for (offsets, expected) in [
        (&[][..], Some((None, 7))),
        (&[3, 21, -1][..], Some((None, 7))),
        (&[20, 5, 0, 11, -1, 4][..], Some((Some(1), 7))),
        (&[i64::MAX, i64::MIN, 9][..], Some((Some(2), 9))),
    ] {
        assert!(truncated_snapshot_selection_bounds_replay(offsets, range, 10) == expected);
    }
    let tied = [20, 10, 10, 3];
    let (selected, cursor) = truncated_snapshot_selection_bounds_replay(&tied, range, 10).unwrap();
    assert!(selected.map(|index| tied[index]) == Some(10) && cursor == 10);
    let range = ProducerReloadRange {
        log_start: i64::MAX - 2,
        local_start: i64::MAX - 1,
        log_end: i64::MAX,
    };
    assert!(
        truncated_snapshot_selection_bounds_replay(&[i64::MAX], range, i64::MAX)
            == Some((Some(0), i64::MAX))
    );
}

proptest! {
    #[test]
    fn snapshot_retry_matches_independent_sequence_and_offset_oracles(
        base in 0i64..i64::MAX - 32, delta in 0i32..32,
        last_sequence in 0i32..=i32::MAX, epoch in 0i16..=i16::MAX,
        request_epoch in any::<i16>(), transaction in any::<bool>(),
    ) {
        let last = base + i64::from(delta);
        let entry = ProducerSnapshotEntryFacts {
            producer_id: 42, producer_epoch: epoch, last_sequence,
            last_offset: last, offset_delta: delta, coordinator_epoch: -1,
            current_txn_first_offset: if transaction { base } else { -1 },
        };
        let sequence = i32::try_from((i64::from(last_sequence) - i64::from(delta))
            .rem_euclid(1i64 << 31)).unwrap();
        let expected = if request_epoch < epoch { ProducerDecision::Fenced }
            else if request_epoch == epoch { ProducerDecision::Duplicate { retained: 4 } }
            else if sequence == 0 { ProducerDecision::Append }
            else { ProducerDecision::OutOfOrder };
        assert!(reloaded_snapshot_preserves_last_batch_retry(last + 1, entry, request_epoch)
            == Some((base, sequence, last + 1, expected, ProducerDecision::Append)));
    }
}
