use {super::*, assert2::assert};

#[test]
fn restored_state_composition_boundaries() {
    let time_entries = [
        IndexEntry::new(10, 1),
        IndexEntry::new(10, 3),
        IndexEntry::new(20, 7),
    ];
    // Equal timestamps select the final equal entry, not the first one.
    assert!(time_index_lookup(&time_entries, 10) == 3);
    for entries in [
        &[][..],
        &time_entries,
        &[IndexEntry::new(20, 1), IndexEntry::new(10, 3)],
        &[IndexEntry::new(10, 3), IndexEntry::new(20, 3)],
        &[IndexEntry::new(10, 11)],
    ] {
        for (lower, upper) in [(i64::MIN, 9), (10, 10), (10, 20), (20, i64::MAX)] {
            time_range::check_cursors(entries, 100, 110, lower, upper);
        }
    }
    time_range::check_cursors(
        &[IndexEntry::new(10, u32::MAX)],
        0,
        i64::from(u32::MAX),
        0,
        10,
    );
    time_range::check_cursors(&[IndexEntry::new(10, 1)], i64::MAX - 1, i64::MAX, 0, 10);
    let epochs = [
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(100),
        },
        EpochEntry {
            epoch: LeaderEpoch(5),
            start_offset: Offset(105),
        },
    ];
    let w = FetchWatermarks {
        log_start: 100,
        log_end: 110,
        hw: 109,
        lso: 108,
        deliverable: 107,
    };
    assert!(
        epoch_and_offset_for_entries(&epochs, LeaderEpoch(3), Offset(110))
            == (LeaderEpoch(2), Offset(105))
    );
    for requested in [-1, 0, 2, 3, 5, 6] {
        assert!(validated_epochs_bound_truncated_fetch(&epochs, requested, 100, w).is_ok());
        assert!(validated_epochs_bound_truncated_fetch(&[], requested, 100, w).is_ok());
    }
    let malformed_epochs = [
        EpochEntry {
            epoch: LeaderEpoch(5),
            start_offset: Offset(100),
        },
        EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(105),
        },
    ];
    assert!(validated_epochs_bound_truncated_fetch(&malformed_epochs, 3, 100, w).is_err());
    let range = ProducerReloadRange {
        log_start: 0,
        local_start: 2,
        log_end: 20,
    };
    let snapshots = [20, 5, 0, 11, -1, 4];
    let shortened = ProducerReloadRange {
        log_end: 10,
        ..range
    };
    assert!(producer_snapshot_latest_index(&snapshots, shortened) == Some(1));
    assert!(producer_snapshot_replay_start(shortened, Some(5)) == Some(5));
    for offsets in [&[][..], &snapshots, &[20, 11], &[2, 3, 10]] {
        let expected = offsets
            .iter()
            .copied()
            .filter(|offset| 0 < *offset && *offset <= 10)
            .max();
        let (selected, cursor) =
            truncated_snapshot_selection_bounds_replay(offsets, range, 10).unwrap();
        assert!(selected.map(|index| offsets[index]) == expected);
        assert!(cursor == expected.unwrap_or(0).max(2));
    }
    let segment = RestoreSegmentExtent {
        base_offset: 100,
        last_offset: 110,
    };
    // Starts can decrease and precede this segment; abort markers must advance.
    let aborts = [
        RestoreAbortedTxn {
            producer_id: 2,
            start_offset: 105,
            last_offset: 108,
        },
        RestoreAbortedTxn {
            producer_id: 1,
            start_offset: 90,
            last_offset: 110,
        },
    ];
    assert!(aborted_transaction_overlaps(90, 110, 100, 107));
    assert!(!aborted_transaction_overlaps(105, 108, 100, 105));
    for entries in [&[][..], &aborts, &[aborts[1], aborts[0]]] {
        for cut in [i64::MIN, 100, 105, 107, 110, i64::MAX] {
            let limit = w.hw.min(w.lso).min(w.deliverable).min(cut);
            let valid = entries
                .windows(2)
                .all(|pair| pair[0].last_offset < pair[1].last_offset);
            let expected = valid.then(|| {
                entries
                    .iter()
                    .enumerate()
                    .filter(|(_, entry)| {
                        100 < limit && entry.start_offset < limit && entry.last_offset >= 100
                    })
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>()
            });
            assert!(
                restored_aborts_remain_bounded_when_fetch_shrinks(entries, segment, w, 100, cut)
                    == expected
            );
        }
    }
}

#[test]
fn composition_boundary_witnesses() {
    for (followers, epoch_start, leader_counts) in [
        (&[10, 5][..], 0, true),
        (&[3, 5][..], 0, true),
        (&[3, 5][..], 6, true),
        (&[10, 5][..], 0, false),
    ] {
        let (hw, limit) = quorum_commit_bounds_fetch(
            followers,
            2,
            epoch_start,
            0,
            leader_counts,
            FetchWatermarks {
                log_start: 0,
                log_end: 10,
                hw: 0,
                lso: 8,
                deliverable: 9,
            },
        );
        assert!(limit <= hw && hw <= 10);
        if hw > 0 {
            let supporters = usize::from(leader_counts)
                + followers.iter().filter(|offset| **offset >= limit).count();
            assert!(supporters >= 2);
        }
    }
    for (entries, valid) in [
        (&[][..], true),
        (
            &[
                IndexEntry::new(1, 2),
                IndexEntry::new(3, 5),
                IndexEntry::new(8, 9),
            ][..],
            true,
        ),
        (&[IndexEntry::new(1, 2), IndexEntry::new(1, 3)][..], false),
        (&[IndexEntry::new(1, 10)][..], false),
    ] {
        for target in [0, 1, 2, 3, 8, u32::MAX] {
            assert!(validated_index_bounds_lookup(entries, target, 8, 10).is_ok() == valid);
        }
    }
    for (generation, count, marker_generation, marker_count) in [
        (1, 5, 1, 2),
        (1, 2, 1, 2),
        (1, 2, 1, 3),
        (1, 5, 2, 2),
        (u64::MAX - 1, 5, u64::MAX - 1, 2),
    ] {
        let (settled, replayed) =
            loss_settlement_is_idempotent(AuditLosses { generation, count }, AuditLosses {
                generation: marker_generation,
                count: marker_count,
            });
        assert!(settled == replayed);
    }
    // Witness the theorem's generation-exhaustion boundary explicitly.
    let saturated = AuditLosses {
        generation: u64::MAX,
        count: 5,
    };
    let marker = AuditLosses {
        generation: u64::MAX,
        count: 2,
    };
    let settled = settle_loss_batch(saturated, marker);
    let replayed = settle_loss_batch(settled, marker);
    assert!(settled.count == 3 && replayed.count == 1);
}
