//! Unit tests for truncation of the log tail and trimming of the log
//! head, including the sidecar state each one has to carry with it.

use {
    super::*,
    crate::{
        config::LogConfig,
        leader_epoch_checkpoint::EpochEntry,
        log::test_support::{
            append_samples, check_stamps, rolling_test_log, sample_batch, sample_batch_with_epoch,
            stamped_rolling_log, stamped_test_log, test_batch_at, test_log, tiny_segments,
        },
        producer_snapshot::ProducerSnapshotEntry,
        stamp_index::{StampEntry, StampIndex},
        test_support::open_log,
        txn_index::AbortedTxn,
    },
    assert2::check,
    krabka_ids::{LeaderEpoch, ProducerId},
    krabka_units::prelude::{bytes, gibibytes},
    tempfile::tempdir,
};

/// Kafka's `UnifiedLog.truncateTo` never refuses a target below the log start.
/// When a local segment reaches down to the target, the log is cut there and
/// the log start moves down onto it (`logStartOffset = min(target,
/// logStartOffset)`); the checkpoint follows, so a reopen agrees.
///
/// A follower whose start sits above the leader's divergence point, after
/// retention or an unclean election, would otherwise fail the same truncation
/// on every fetch and never rejoin the ISR.
#[test]
fn a_truncation_below_the_log_start_lowers_the_start_to_the_target() {
    let (dir, mut log) = test_log();
    append_samples(&mut log, 4, 2);
    log.set_log_start_offset(Offset(4)).expect("set log start");

    log.truncate_to(Offset(4)).unwrap();
    check!(
        log.log_start_offset() == Offset(4),
        "a cut at the log start leaves it"
    );
    log.truncate_to(Offset(2)).unwrap();
    check!(log.log_end_offset() == Offset(2));
    check!(log.log_start_offset() == Offset(2));

    let reopened = open_log(dir.path());
    check!(reopened.log_start_offset() == Offset(2));
    check!(reopened.log_end_offset() == Offset(2));
}

/// Kafka's `UnifiedLog.truncateTo` resets the log at the target when no local
/// segment reaches down to it (`truncateFullyAndStartAt`): the log is empty,
/// starts and ends at the target, and appends continue from there.
#[test]
fn a_truncation_below_every_local_segment_resets_the_log_at_the_target() {
    let (_dir, mut log) = test_log();
    log.reset_to(Offset(20)).unwrap();
    append_samples(&mut log, 3, 2);
    check!(log.log_start_offset() == Offset(20));
    check!(log.log_end_offset() == Offset(26));

    log.truncate_to(Offset(10)).unwrap();

    check!(log.log_start_offset() == Offset(10));
    check!(log.log_end_offset() == Offset(10));
    let mut batch = sample_batch(2);
    let (first_offset, _) = log.append(&mut batch).unwrap();
    check!(first_offset == Offset(10));
}

#[test]
fn truncate_to_drops_later_records() {
    let (_dir, mut log) = test_log();
    let mut b1 = sample_batch(3);
    let mut b2 = sample_batch(2);
    log.append(&mut b1).unwrap();
    log.append(&mut b2).unwrap();
    check!(log.log_end_offset() == Offset(5));
    log.truncate_to(Offset(3)).unwrap();
    // First batch (offsets 0..=2) survives; last_offset == 2, end == 3.
    check!(log.log_end_offset() == Offset(3));
}

#[test]
fn truncation_clamps_tail_state_to_the_actual_retained_batch_prefix() {
    let (_dir, mut log) = test_log();
    log.append(&mut sample_batch(3)).unwrap();
    log.append(&mut sample_batch(3)).unwrap();
    log.active_txn_index
        .append(AbortedTxn {
            start_offset: Offset(3),
            last_offset: Offset(5),
            producer_id: ProducerId(9),
            last_stable_offset: Offset(6),
        })
        .unwrap();
    log.delivery_watermark = Offset(6);
    log.lso = Offset(6);

    // The cut lands inside the second batch, so the exact retained prefix
    // ends at 3 rather than the requested 4.
    log.truncate_to(Offset(4)).unwrap();
    check!(log.log_end_offset() == Offset(3));
    check!(log.lso() == Offset(3));
    check!(log.delivery_watermark == Offset(3));
    check!(log.active_txn_index.entries().is_empty());
    check!(log.active.is_some(), "one active segment survives");
    check!(
        log.segments
            .iter()
            .all(|segment| segment.base_offset() < log.active.as_ref().unwrap().base_offset()),
        "every sealed segment precedes the sole active segment"
    );

    log.append(&mut sample_batch(1)).unwrap();
    check!(
        log.delivery_watermark == Offset(3),
        "append must not unmask the discarded tail watermark"
    );
}

#[test]
fn truncate_to_rebuilds_producer_state_from_surviving_batches() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    let mut first = sample_batch(2);
    first.producer_id = 42;
    first.producer_epoch = 3;
    first.base_sequence = 7;
    first.max_timestamp = 100;
    log.append(&mut first).unwrap();
    let mut discarded = sample_batch(2);
    discarded.producer_id = 42;
    discarded.producer_epoch = 3;
    discarded.base_sequence = 9;
    discarded.max_timestamp = 200;
    log.append(&mut discarded).unwrap();
    let mut later = sample_batch(2);
    later.producer_id = 42;
    later.producer_epoch = 3;
    later.base_sequence = 11;
    later.max_timestamp = 300;
    log.append(&mut later).unwrap();

    log.truncate_to(Offset(2)).unwrap();

    assert2::assert!(
        log.producer_state_snapshot()
            == vec![ProducerSnapshotEntry {
                producer_id: ProducerId(42),
                producer_epoch: 3,
                last_sequence: 8,
                last_offset: Offset(1),
                offset_delta: 1,
                timestamp: 100,
                coordinator_epoch: -1,
                current_txn_first_offset: None,
            }]
    );
    assert2::assert!(crate::log::test_support::snapshot_offsets(dir.path()) == vec![Offset(2)]);
}

#[test]
fn truncate_to_removes_future_empty_producer_snapshots() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    append_samples(&mut log, 3, 2);
    assert2::assert!(log.producer_state.is_empty());

    log.truncate_to(Offset(2)).unwrap();

    assert2::assert!(crate::log::test_support::snapshot_offsets(dir.path()) == vec![Offset(2)]);
}

/// Kafka takes a producer-state snapshot at a roll, on `close`, after
/// `truncateTo` and after `truncateFullyAndStartAt`, and a truncation drops
/// every snapshot at or below the log start. Each row starts from three
/// two-record batches from one idempotent producer, one per segment (rolls at
/// 2 and 4, log end 6), then
/// applies an action and lists the snapshot offsets left on disk. A reopen
/// then restores the same producer state it had.
#[test]
fn producer_snapshots_follow_kafkas_lifecycle() {
    #[derive(Debug, Clone, Copy)]
    enum Action {
        Nothing,
        Close,
        TruncateToFirstRoll,
        TrimThenTruncate,
        ResetTo,
    }
    let cases = [
        (Action::Nothing, vec![Offset(2), Offset(4)]),
        (Action::Close, vec![Offset(2), Offset(4), Offset(6)]),
        (Action::TruncateToFirstRoll, vec![Offset(2)]),
        (Action::TrimThenTruncate, vec![Offset(4)]),
        (Action::ResetTo, vec![Offset(10)]),
    ];
    for (action, expected) in cases {
        let dir = tempdir().unwrap();
        let config = tiny_segments();
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for sequence in [0, 2, 4] {
            let mut batch = sample_batch(2);
            batch.producer_id = 42;
            batch.base_sequence = sequence;
            log.append(&mut batch).unwrap();
        }
        match action {
            Action::Nothing | Action::Close => {}
            Action::TruncateToFirstRoll => log.truncate_to(Offset(2)).unwrap(),
            Action::TrimThenTruncate => {
                log.trim_to_offset(Offset(2)).unwrap();
                log.truncate_to(Offset(4)).unwrap();
            }
            Action::ResetTo => log.reset_to(Offset(10)).unwrap(),
        }
        let before = log.producer_state_snapshot();
        if matches!(action, Action::Close) {
            log.close();
        } else {
            drop(log);
        }
        let offsets: Vec<Offset> = producer_snapshot::list(dir.path())
            .unwrap()
            .into_iter()
            .map(|(offset, _)| offset)
            .collect();
        check!(offsets == expected, "{action:?}");
        let reopened = Log::open(dir.path(), config).unwrap();
        check!(reopened.producer_state_snapshot() == before, "{action:?}");
    }
}

#[test]
fn truncate_to_log_end_is_noop() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch(2);
    log.append(&mut b).unwrap();
    let before = log.log_end_offset();
    log.truncate_to(before + 100).unwrap();
    assert2::assert!(log.log_end_offset() == before);
}

// `truncate_to` promoting a **sealed** segment with base_offset > 0 must
// compute the relative cut as `offset - base` (line: `offset.0 -
// seg.base_offset().0`). We build sealed segment base 1 holding three
// single-record batches (offsets 1,2,3), drop the active segment, and
// truncate to offset 3. Correct `rel = 3 - 1 = 2` drops only the offset-3
// batch → log_end 3. Both the `+` mutant (`rel = 4`) and the `/` mutant
// (`rel = 3`) leave every batch in place → log_end 4.
#[test]
fn truncate_to_promoted_sealed_uses_relative_offset() {
    let (_dir, mut log) = active_segment_at_one();
    // Roll: seal base 1 (offsets 1,2,3), fresh active base 4, batch E.
    log.set_config(tiny_segments());
    log.append(&mut test_batch_at(4)).unwrap();
    assert2::assert!(log.log_end_offset() == 5);

    // Truncate to 3: active base 4 (>=3) is dropped, then sealed base 1 is
    // promoted and truncated. rel = 3 - 1 = 2 keeps offsets 1,2, drops 3.
    log.truncate_to(Offset(3)).unwrap();
    assert2::assert!(log.log_end_offset() == 3);
}

// `truncate_to` truncating a **surviving active** segment with
// base_offset > 0 must compute the cut as `offset - base` (line:
// `offset.0 - active.base_offset().0`). Active segment base 1 holds three
// single-record batches (offsets 1,2,3); truncate to offset 3. Correct
// `rel = 3 - 1 = 2` drops only the offset-3 batch → log_end 3. The `+`
// mutant (`rel = 4`) drops nothing → log_end 4.
#[test]
fn truncate_to_active_segment_uses_relative_offset() {
    let (_dir, mut log) = active_segment_at_one();
    assert2::assert!(log.log_end_offset() == 4);

    // Active base 1 survives (1 < 3); rel = 3 - 1 = 2 drops the offset-3
    // batch, keeps offsets 1,2 → log_end 3.
    log.truncate_to(Offset(3)).unwrap();
    assert2::assert!(log.log_end_offset() == 3);
}

#[test]
fn truncate_removes_stamps_for_discarded_tail() {
    let (dir, mut log) = stamped_test_log(10, 1);
    append_samples(&mut log, 2, 1);
    check!(log.stamp_for_offset(Offset(1)) == Some(11));

    log.truncate_to(Offset(1)).unwrap();
    check!(log.stamp_for_offset(Offset(0)) == Some(10));
    check!(log.stamp_for_offset(Offset(1)) == None);
    assert2::assert!(
        StampIndex::open(dir.path().join("00000000000000000000.stampindex"))
            .unwrap()
            .entries()
            == [StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(0),
                stamp: 10,
            }]
    );
}

#[test]
fn truncate_preserves_stamp_indexes_before_promoted_segment() {
    let (_dir, mut log) = stamped_rolling_log(10, 1, 3);

    log.truncate_to(Offset(2)).unwrap();

    check_stamps(&log, &[(0, Some(10)), (1, Some(11)), (2, None)]);
}

#[test]
fn trim_removes_only_evicted_segment_stamp_indexes() {
    let (_dir, mut log) = stamped_rolling_log(10, 1, 3);

    log.trim_to_offset(Offset(1)).unwrap();

    check_stamps(&log, &[(0, None), (1, Some(11)), (2, Some(12))]);
}

#[test]
fn truncate_to_drops_stale_epoch_checkpoint_entries() {
    let (_dir, mut log) = test_log();
    // Epoch 1 at offsets 0..3, then epoch 7 starting at offset 3.
    let mut b1 = sample_batch_with_epoch(3, 1);
    log.append(&mut b1).unwrap();
    let epoch7_start = log.log_end_offset();
    let mut b2 = sample_batch_with_epoch(2, 7);
    log.append(&mut b2).unwrap();
    assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(7)));

    // Truncate away the entire epoch-7 tail.
    log.truncate_to(epoch7_start).unwrap();

    let leo = log.log_end_offset();
    assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
    assert2::assert!(
        log.epoch_checkpoint()
            .end_offset_for_epoch(LeaderEpoch(7), leo)
            == Offset(-1)
    );
    assert2::assert!(
        log.epoch_checkpoint()
            .end_offset_for_epoch(LeaderEpoch(1), leo)
            == leo
    );
}

/// Kafka's `UnifiedLog.truncateTo` no-effect branch: a replica that took a
/// leader epoch at its log end and lost leadership before writing drops that
/// unbacked entry on any truncation at or past its log end, so the next
/// leader's epoch can be recorded at the same offset.
#[test]
fn a_no_op_truncation_still_drops_an_epoch_no_record_backs() {
    let entry = |epoch, start_offset| EpochEntry {
        epoch: LeaderEpoch(epoch),
        start_offset: Offset(start_offset),
    };
    for (name, target) in [("at the log end", 3), ("past the log end", 9)] {
        let (_dir, mut log) = test_log();
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap();
        log.assign_epoch_start_offset(LeaderEpoch(5), Offset(3))
            .unwrap();

        log.truncate_to(Offset(target)).unwrap();

        check!(log.log_end_offset() == Offset(3), "{name}");
        check!(
            log.epoch_checkpoint().entries() == &[entry(1, 0)][..],
            "{name}"
        );
    }
}

#[test]
fn trim_to_offset_drops_old_segments() {
    let dir = tempdir().expect("tempdir");
    let mut log = Log::open(dir.path(), LogConfig {
        segment_size: bytes(200), // small so we roll fast
        ..LogConfig::default()
    })
    .expect("open");
    // Append 30 records to force multiple sealed segments.
    append_samples(&mut log, 30, 1);
    let leo = log.log_end_offset();
    let sealed_before = log.segments.len();
    check!(sealed_before > 0);
    check!(log.sealed_txn_indexes.contains_key(&Offset(0)));
    let new_start = log.trim_to_offset(Offset(15)).expect("trim");
    check!(log.segments.len() < sealed_before);
    check!(!log.sealed_txn_indexes.contains_key(&Offset(0)));
    // Trim clamps to next segment boundary <= target; new_start may
    // be less than 15 if 15 falls inside a sealed segment that we
    // can't drop without losing in-range records. LEO is unaffected.
    check!(new_start <= 15);
    check!(log.log_end_offset() == leo);
    // If target landed inside the active segment, log_start advanced
    // exactly to target. Otherwise it advanced to a sealed boundary.
    check!(log.log_start_offset() >= 0);
}

#[test]
fn trim_to_offset_clamps_to_leo() {
    let (_dir, mut log) = test_log();
    append_samples(&mut log, 3, 1);
    let leo = log.log_end_offset();
    let new_start = log.trim_to_offset(Offset(999)).expect("trim");
    // Asking to trim past LEO means trim to LEO.
    assert2::assert!(new_start == leo);
}

#[test]
fn trim_to_offset_rejects_negative() {
    let (_dir, mut log) = test_log();
    assert2::assert!(log.trim_to_offset(Offset(-5)).is_err());
}

#[test]
fn trim_to_offset_idempotent_at_or_below_log_start() {
    let (_dir, mut log) = test_log();
    append_samples(&mut log, 3, 1);
    // Trim to 0 on a fresh log → no change.
    let r = log.trim_to_offset(Offset(0)).expect("trim");
    assert2::assert!(r == log.log_start_offset());
}

/// Kafka's `truncateAndReload(logStartOffset, cut, _)` on a truncation:
/// every snapshot outside `(log start, cut]` is deleted -- one above the cut
/// and one at or below the log start alike -- and the newest one left loads.
/// A producer whose batches are all below the log start survives because
/// the loaded snapshot carries it.
#[test]
fn truncation_reloads_from_the_newest_snapshot_between_log_start_and_cut() {
    for (log_start, cut, snapshots, producers) in [
        // Truncation at a snapshot: it is kept and loaded.
        (0, 3, vec![1, 2, 3], vec![1, 2, 3]),
        // Truncation below a snapshot: it is deleted.
        (0, 2, vec![1, 2], vec![1, 2]),
        // The snapshot at the log start is deleted; the one at 2 still
        // carries producer 1, whose only record is below the log start.
        (1, 2, vec![2], vec![1, 2]),
        // Truncation to the log start leaves no snapshot and no producer.
        (2, 2, vec![], vec![]),
    ] {
        let dir = tempdir().unwrap();
        let mut log = rolling_test_log(dir.path());
        // One record per producer and per segment: segments at 0..=3 and a
        // snapshot at every roll, 1..=3.
        for producer_id in 1..=4 {
            let mut batch = sample_batch(1);
            batch.producer_id = producer_id;
            batch.producer_epoch = 0;
            batch.base_sequence = 0;
            log.append(&mut batch).unwrap();
        }
        log.set_log_start_offset(Offset(log_start)).unwrap();

        log.truncate_to(Offset(cut)).unwrap();

        let mut reloaded: Vec<i64> = log
            .producer_state_snapshot()
            .into_iter()
            .map(|entry| entry.producer_id.get())
            .collect();
        reloaded.sort_unstable();
        check!(reloaded == producers, "log start {log_start}, cut {cut}");
        check!(
            producer_snapshot::list(dir.path())
                .unwrap()
                .into_iter()
                .map(|(offset, _)| offset.0)
                .collect::<Vec<_>>()
                == snapshots,
            "log start {log_start}, cut {cut}"
        );
    }
}

fn active_segment_at_one() -> (tempfile::TempDir, Log) {
    let (dir, mut log) = test_log();
    // Batch A → active base 0.
    log.append(&mut test_batch_at(0)).unwrap();
    // Roll: seal base 0, fresh active base 1, batch B.
    log.set_config(tiny_segments());
    log.append(&mut test_batch_at(1)).unwrap();
    // No roll: batches C, D accumulate in active base 1 (offsets 2, 3).
    log.set_config(LogConfig {
        segment_size: gibibytes(1),
        ..LogConfig::default()
    });
    log.append(&mut test_batch_at(2)).unwrap();
    log.append(&mut test_batch_at(3)).unwrap();
    (dir, log)
}
