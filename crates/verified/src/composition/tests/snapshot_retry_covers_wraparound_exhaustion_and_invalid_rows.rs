use {super::*, assert2::assert};

#[test]
fn snapshot_retry_covers_wraparound_exhaustion_and_invalid_rows() {
    let entry = ProducerSnapshotEntryFacts {
        producer_id: 42,
        producer_epoch: 7,
        last_sequence: 0,
        last_offset: i64::MAX - 1,
        offset_delta: 2,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    };
    assert!(
        reloaded_snapshot_preserves_last_batch_retry(i64::MAX, entry, 7)
            == Some((
                i64::MAX - 3,
                i32::MAX - 1,
                i64::MAX,
                ProducerDecision::Duplicate { retained: 4 },
                ProducerDecision::Append
            ))
    );
    for (snapshot, row) in [
        (entry.last_offset, entry),
        (i64::MAX, ProducerSnapshotEntryFacts {
            offset_delta: -1,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            last_offset: 1,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            producer_epoch: -1,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            producer_id: -1,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            coordinator_epoch: -2,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            current_txn_first_offset: i64::MAX,
            ..entry
        }),
        (i64::MAX, ProducerSnapshotEntryFacts {
            last_offset: -1,
            last_sequence: -1,
            offset_delta: 0,
            ..entry
        }),
    ] {
        assert!(reloaded_snapshot_preserves_last_batch_retry(snapshot, row, 7) == None);
    }
}

proptest! {
    #[test]
    fn published_trim_matches_checkpoint_prefix_and_fetch_oracles(
        ends in proptest::collection::btree_set(1i64..=64, 0..16),
        prior_slot in any::<u8>(),
        requested in 1i64..128,
        stage in 0u8..=3,
        partial_floor in any::<u8>(),
        hw in any::<i64>(), lso in any::<i64>(), deliverable in any::<i64>(), probe in any::<i64>(),
    ) {
        let ends: Vec<_> = ends.into_iter().collect();
        let end = ends.last().copied().unwrap_or(0);
        let prior_end = ends.get(usize::from(prior_slot) % (ends.len() + 1)).copied().unwrap_or(0);
        let desired = requested.min(end);
        let observed_floor = match stage { 0 | 1 => 0, 2 => i64::from(partial_floor) % (desired + 1), _ => desired };
        let checkpoint_floor = if stage < 2 { 0 } else { desired };
        let checkpoint_end = if stage < 2 { prior_end } else { end };
        let kept = if checkpoint_floor == checkpoint_end { 0 }
            else { ends.iter().filter(|end| **end <= checkpoint_end).count() };
        let limit = [checkpoint_end, hw, lso, deliverable].into_iter().min().unwrap();
        let w = FetchWatermarks { log_start: 0, log_end: end, hw, lso, deliverable };
        assert!(published_trim_bounds_recovery(&ends, 0, w, prior_end, requested,
            (stage, observed_floor), probe) == Some((checkpoint_floor, checkpoint_end, kept,
            limit, checkpoint_floor <= probe && probe < limit)));
    }
}

#[test]
fn published_trim_recovers_interior_floors_deleted_prefixes_and_empty_ranges() {
    let w = FetchWatermarks {
        log_start: 1,
        log_end: 9,
        hw: 9,
        lso: 9,
        deliverable: 9,
    };
    for stage in 0..=3 {
        let observed = if stage == 3 { 4 } else { 1 };
        let (floor, end, kept) = if stage < 2 { (1, 5, 2) } else { (4, 9, 3) };
        for probe in 0..=10 {
            assert!(
                published_trim_bounds_recovery(&[2, 5, 9], 0, w, 5, 4, (stage, observed), probe)
                    == Some((floor, end, kept, end, floor <= probe && probe < end))
            );
        }
    }
    assert!(
        published_trim_bounds_recovery(&[5, 9], 2, w, 5, 4, (2, 3), 4) == Some((4, 9, 2, 9, true))
    );
    assert!(
        published_trim_bounds_recovery(&[], 9, w, 5, 10, (2, 9), 9) == Some((9, 9, 0, 9, false))
    );
    assert!(
        published_trim_bounds_recovery(&[2, 5, 9], 0, w, 1, 4, (1, 1), 1)
            == Some((1, 1, 0, 1, false))
    );
    let w = FetchWatermarks {
        log_start: i64::MAX - 5,
        log_end: i64::MAX,
        hw: i64::MAX,
        lso: i64::MAX,
        deliverable: i64::MAX,
    };
    assert!(
        published_trim_bounds_recovery(
            &[i64::MAX - 3, i64::MAX],
            i64::MAX - 5,
            w,
            i64::MAX - 3,
            i64::MAX - 1,
            (2, i64::MAX - 2),
            i64::MAX - 1
        ) == Some((i64::MAX - 1, i64::MAX, 2, i64::MAX, true))
    );
}

proptest! {
    #[test]
    fn marker_visibility_matches_wire_and_remaining_transaction_oracle(
        marker_type in prop_oneof![Just(0i16), Just(1), Just(1000), any::<i16>()],
        version in any::<i16>(),
        key_len in 0usize..=7,
        is_control in any::<bool>(),
        matches_pid in any::<bool>(),
        other_starts in proptest::collection::vec(0i64..=10, 0..16),
        hw in prop_oneof![Just(12i64), Just(13), any::<i64>()],
        deliverable in any::<i64>(),
    ) {
        let mut key = Vec::from(version.to_be_bytes());
        key.extend_from_slice(&marker_type.to_be_bytes());
        key.resize(key_len, 255);
        let decoded = key.get(2..4).map(|bytes| i16::from_be_bytes([bytes[0], bytes[1]]));
        let closes = is_control && matches_pid && matches!(decoded, Some(0 | 1));
        let released = closes && hw > 12;
        let expected_limit = other_starts.iter().copied()
            .chain((!released).then_some(2)).chain([13, hw, deliverable]).min().unwrap();
        let expected_abort = (closes && decoded == Some(0)).then_some((2, 12));
        assert!(control_marker_bounds_committed_fetch(&key, is_control,
            (7, if matches_pid { 7 } else { 8 }, 2), (10, 2), &other_starts,
            (hw, deliverable)) == (expected_limit, expected_abort));
    }
}

#[test]
fn marker_release_is_strict_and_cannot_release_another_producer() {
    for marker_type in [0i16, 1, 1000, -1, 23] {
        let mut key = Vec::from(0i16.to_be_bytes());
        key.extend_from_slice(&marker_type.to_be_bytes());
        for hw in [12, 13] {
            let release = matches!(marker_type, 0 | 1) && hw == 13;
            let aborted = (marker_type == 0).then_some((2, 12));
            assert!(
                control_marker_bounds_committed_fetch(
                    &key,
                    true,
                    (7, 7, 2),
                    (10, 2),
                    &[],
                    (hw, 13)
                ) == (if release { 13 } else { 2 }, aborted)
            );
            assert!(
                control_marker_bounds_committed_fetch(
                    &key,
                    true,
                    (7, 8, 2),
                    (10, 2),
                    &[],
                    (hw, 13)
                ) == (2, None)
            );
            assert!(
                control_marker_bounds_committed_fetch(
                    &key,
                    true,
                    (7, 7, 2),
                    (10, 2),
                    &[1],
                    (hw, 13)
                ) == (1, aborted)
            );
        }
    }
    assert!(
        control_marker_bounds_committed_fetch(
            &[0, 0, 0, 1],
            true,
            (7, 7, i64::MAX - 2),
            (i64::MAX - 1, 0),
            &[],
            (i64::MAX, i64::MAX)
        ) == (i64::MAX, None)
    );
}
