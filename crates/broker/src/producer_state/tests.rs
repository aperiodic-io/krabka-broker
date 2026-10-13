//! Unit tests for the topic and partition map plumbing and for the
//! produce-path dedup, commit, truncate, and snapshot behaviour of
//! `ProducerState`.
use {
    super::*,
    crate::{
        producer_state::test_support::{commit_fixture, opened_log, producer_batch},
        test_support::default_records_batch,
    },
    assert2::{assert, check},
};

async fn mirror_marker(state: &ProducerState) {
    state
        .mirror_log_entries("t", PartitionIndex(0), vec![
            krabka_log::ProducerSnapshotEntry {
                producer_id: krabka_log::ProducerId(1000),
                producer_epoch: 4,
                last_sequence: -1,
                last_offset: krabka_log::Offset(-1),
                offset_delta: 0,
                timestamp: -1,
                coordinator_epoch: 0,
                current_txn_first_offset: None,
            },
        ])
        .await;
}

fn remove_snapshots(dir: &std::path::Path, offsets: impl IntoIterator<Item = i64>) {
    for offset in offsets {
        let path = krabka_log::name::producer_snapshot_path(dir, offset);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
}

#[tokio::test]
async fn first_batch_appends() {
    let s = ProducerState::new();
    let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
    assert!(d == Decision::Append);
}

#[tokio::test]
async fn next_sequence_appends() {
    let s = ProducerState::new();
    commit_fixture().state(&s).call().await;
    let d = s.check("t", PartitionIndex(0), 1000, 0, 5, 2).await;
    assert!(d == Decision::Append);
}

#[tokio::test]
async fn duplicate_returns_cached_offset() {
    let s = ProducerState::new();
    commit_fixture().state(&s).call().await;
    let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
    assert!(d == Decision::Duplicate { base_offset: 0 });
}

/// A deferred `acks=all` commit for a pre-marker batch must not undo a
/// marker's epoch bump.
///
/// `AppendCommit::record` waits on the high-watermark gate for its own
/// append, so it can resolve long after a later transaction marker mirrored
/// a bumped epoch into the tracker. `commit` must skip a write that would
/// regress the tracked epoch, or the tracker would fence the wrong epoch, or
/// accept a first sequence at the new epoch that is not 0, until the next
/// restart.
#[tokio::test]
async fn a_late_commit_at_an_older_epoch_does_not_undo_a_marker() {
    let s = ProducerState::new();
    // The marker's mirror lands first, well before the older batch's deferred
    // acks=all commit resolves.
    mirror_marker(&s).await;

    // The stale, pre-marker commit for epoch 3 resolves after the marker.
    commit_fixture()
        .state(&s)
        .epoch(3)
        .delta(2)
        .offset(10)
        .call()
        .await;

    assert!(
        s.check("t", PartitionIndex(0), 1000, 4, 0, 0).await == Decision::Append,
        "the marker's epoch must still be the tracked one"
    );
    assert!(
        s.check("t", PartitionIndex(0), 1000, 3, 0, 2).await == Decision::Fenced,
        "the stale commit must not have reopened the pre-marker epoch"
    );
}

#[tokio::test]
async fn only_an_exact_retry_of_the_last_batch_is_duplicate() {
    let s = ProducerState::new();
    commit_fixture()
        .state(&s)
        .sequence(3)
        .delta(1)
        .offset(10)
        .call()
        .await;

    check!(
        s.check("t", PartitionIndex(0), 1000, 0, 3, 1).await
            == Decision::Duplicate { base_offset: 10 }
    );
    check!(s.check("t", PartitionIndex(0), 1000, 0, 3, 0).await == Decision::OutOfOrder);
    check!(s.check("t", PartitionIndex(0), 1000, 0, 2, 1).await == Decision::OutOfOrder);
}

#[tokio::test]
async fn sequence_rollover_appends_and_commits_without_overflow() {
    let s = ProducerState::new();
    commit_fixture()
        .state(&s)
        .sequence(i32::MAX - 1)
        .delta(1)
        .offset(10)
        .call()
        .await;

    check!(s.check("t", PartitionIndex(0), 1000, 0, 0, 0).await == Decision::Append);
    check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::OutOfOrder);

    commit_fixture()
        .state(&s)
        .delta(2)
        .offset(12)
        .timestamp(2)
        .call()
        .await;
    let entry = s.snapshot("t", PartitionIndex(0)).await[0].1;
    check!(entry.last_sequence == 2);
    check!(entry.last_offset == 14);
}

#[tokio::test]
async fn batch_can_cross_sequence_rollover() {
    let s = ProducerState::new();
    commit_fixture()
        .state(&s)
        .sequence(i32::MAX - 1)
        .delta(2)
        .offset(20)
        .call()
        .await;

    check!(
        s.check("t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 2)
            .await
            == Decision::Duplicate { base_offset: 20 }
    );
    check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::Append);
}

#[tokio::test]
async fn truncate_drops_dedup_entry_above_offset_so_retry_reappends() {
    // The failover-stall regression: a batch was appended at base_offset
    // 1471686 (last_offset 1471699), then the divergent tail was truncated
    // back to 1471686 on rejoin. A retry must NOT be deduplicated against
    // the now-truncated offset — otherwise the acks=all HW gate
    // (await_hw_at_least(1471700)) waits forever for a high watermark the
    // log can never reach, stalling the producer.
    let s = ProducerState::new();
    commit_fixture()
        .state(&s)
        .delta(13)
        .offset(1_471_686)
        .call()
        .await;
    assert!(
        s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await
            == Decision::Duplicate {
                base_offset: 1_471_686
            }
    );
    s.truncate("t", PartitionIndex(0), 1_471_686).await;
    assert!(
        s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await == Decision::Append,
        "after truncation the retried batch must re-append, not dedup against the truncated offset"
    );
}

#[yare::parameterized(
    survives = { 200, Decision::Duplicate { base_offset: 100 } },
    exact_boundary = { 104, Decision::Append },
)]
#[test_macro(tokio::test)]
async fn truncate_dedup_entry(truncate_offset: i64, expected: Decision) {
    // The retained batch ends at offset 104, so it stays deduplicated only
    // when truncation keeps that complete entry below the cut.
    let s = ProducerState::new();
    commit_fixture().state(&s).offset(100).call().await; // last_offset 104
    s.truncate("t", PartitionIndex(0), truncate_offset).await;
    assert2::assert!(s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await == expected);
}

#[tokio::test]
async fn truncate_unknown_partition_is_noop() {
    let s = ProducerState::new();
    s.truncate("never-seen", PartitionIndex(7), 0).await; // must not panic or create state
    assert!(s.snapshot("never-seen", PartitionIndex(7)).await.is_empty());
}

/// A marker-only entry (a transaction-version-2 marker cleared the retained
/// batch, so `last_offset` is the Kafka `-1` sentinel) carries no offset of
/// its own, so it cannot be placed relative to a truncation cut. `truncate`
/// must drop it regardless of `offset`, or a KIP-320 divergent-tail
/// truncation that removes the marker itself would leave the tracker fencing
/// or accepting sequences at an epoch the log no longer has. See
/// `ProducerState::truncate`'s doc comment.
#[tokio::test]
async fn truncate_drops_every_marker_only_entry_regardless_of_offset() {
    for offset in [0, 1, i64::MAX] {
        let s = ProducerState::new();
        mirror_marker(&s).await;
        s.truncate("t", PartitionIndex(0), offset).await;
        assert!(
            s.snapshot("t", PartitionIndex(0)).await.is_empty(),
            "offset={offset}"
        );
    }
}

#[tokio::test]
async fn out_of_order_when_gap() {
    let s = ProducerState::new();
    commit_fixture().state(&s).call().await;
    // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
    let d = s.check("t", PartitionIndex(0), 1000, 0, 10, 2).await;
    assert!(d == Decision::OutOfOrder);
}

async fn commit_epoch_five_prefix(state: &ProducerState) {
    commit_fixture().state(state).epoch(5).delta(2).call().await;
}

#[tokio::test]
async fn lower_epoch_is_fenced() {
    let s = ProducerState::new();
    commit_fixture().state(&s).epoch(5).call().await;
    let d = s.check("t", PartitionIndex(0), 1000, 4, 5, 2).await;
    assert!(d == Decision::Fenced);
}

/// A bumped producer epoch (same `producer_id`, higher epoch) establishes a
/// FRESH sequence baseline: `base_sequence == 0` at the new epoch must be a
/// fresh `Append`, NOT a `Duplicate` against the prior epoch's high-water.
/// This is the EOS-restart path. The client resets its sequence to 0.
///
/// This is the regression test for the cross-restart EOS data-loss bug.
/// Before the fix, the broker silently deduped a restarted EOS producer's
/// first record on each partition and echoed the old `base_offset`. The
/// txn's offset commit still landed. The source offset advanced, but the
/// output record vanished.
#[tokio::test]
async fn higher_epoch_at_seq_zero_appends() {
    let s = ProducerState::new();
    // Epoch 5 committed sequences 0..=2 (last_sequence = 2).
    commit_epoch_five_prefix(&s).await;
    // Same pid, epoch 6, base_sequence 0 — a fresh write, NOT a duplicate.
    let d = s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await;
    assert!(d == Decision::Append);
}

/// A bumped epoch that continues the sequence (`base_sequence > 0`) is out
/// of order. Kafka's `ProducerAppendInfo.checkSequence` requires sequence 0
/// for the first batch at a new epoch. That includes the KIP-890
/// (transaction version 2) epoch bump on every commit or abort, after which
/// the Java client calls `resetSequenceNumbers()`. The rejected batch changes
/// nothing, so the batch at sequence 0 still appends, and same-epoch dedup
/// resumes after it commits.
#[tokio::test]
async fn higher_epoch_continuing_sequence_is_out_of_order() {
    let s = ProducerState::new();
    commit_epoch_five_prefix(&s).await;
    check!(s.check("t", PartitionIndex(0), 1000, 6, 3, 0).await == Decision::OutOfOrder);
    check!(s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await == Decision::Append);
    commit_fixture()
        .state(&s)
        .epoch(6)
        .delta(0)
        .offset(10)
        .timestamp(2)
        .call()
        .await;
    let dup = s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await;
    assert!(dup == Decision::Duplicate { base_offset: 10 });
}

#[tokio::test]
async fn snapshot_reports_committed_entries() {
    let s = ProducerState::new();
    commit_fixture()
        .state(&s)
        .partition(3)
        .offset(7)
        .call()
        .await;
    let snap = s.snapshot("t", PartitionIndex(3)).await;
    let expected = vec![(1000, ProducerEntry {
        epoch: 0,
        last_sequence: 4,
        last_offset: 11,
        base_offset: 7,
        last_timestamp: 1,
        entry_timestamp: 1,
        current_txn_first_offset: None,
        earlier: crate::producer_state::NO_EARLIER_BATCHES,
    })];
    assert!(snap == expected);
    // Untouched partition / topic report empty without panicking.
    for (topic, partition) in [("t", PartitionIndex(0)), ("other", PartitionIndex(3))] {
        assert!(
            s.snapshot(topic, partition).await == vec![],
            "case: {topic}/{partition}"
        );
    }
}

/// The log keeps one batch per producer, so the mirror of a marker entry
/// keeps the tracker's earlier batches only when the marker leaves the epoch
/// and the last batch as they are (transaction version 1). A marker at a new
/// epoch clears them, as Kafka's `ProducerStateEntry.maybeUpdateProducerEpoch`
/// does.
#[tokio::test]
async fn a_marker_mirror_keeps_the_earlier_batches_only_at_the_same_epoch() {
    let marker_entry =
        |epoch: i16, last_sequence: i32, last_offset: i64| krabka_log::ProducerSnapshotEntry {
            producer_id: krabka_log::ProducerId(1000),
            producer_epoch: epoch,
            last_sequence,
            last_offset: krabka_log::Offset(last_offset),
            offset_delta: 0,
            // The marker's own timestamp, which differs from the data batch.
            timestamp: 9,
            coordinator_epoch: 0,
            current_txn_first_offset: None,
        };
    let cases = [
        (
            "transaction version 1 marker",
            marker_entry(0, 1, 1),
            Decision::Duplicate { base_offset: 0 },
            2,
        ),
        (
            "transaction version 2 marker",
            marker_entry(1, -1, -1),
            Decision::Fenced,
            9,
        ),
    ];
    for (name, entry, want, last_timestamp) in cases {
        let s = ProducerState::new();
        commit_fixture().state(&s).delta(0).call().await;
        commit_fixture()
            .state(&s)
            .sequence(1)
            .delta(0)
            .offset(1)
            .timestamp(2)
            .call()
            .await;
        s.mirror_log_entries("t", PartitionIndex(0), vec![entry])
            .await;
        check!(
            s.check("t", PartitionIndex(0), 1000, 0, 0, 0).await == want,
            "{name}"
        );
        check!(
            s.snapshot("t", PartitionIndex(0)).await[0].1.last_timestamp == last_timestamp,
            "{name}"
        );
    }
}

/// #981: after a reopen, the tracker dedups a retry of any batch that Kafka's
/// `UnifiedLog.rebuildProducerState` retains: the snapshot's batch and the
/// replayed tail, up to five. Six batches; the stop leaves only
/// the snapshot a segment roll wrote before the last four. Batches 1 to 5
/// answer as duplicates at their own offsets, and batch 0, which left the
/// five, is out of order. Multi-record batches, sequence wrap, and anonymous
/// offset gaps must preserve each duplicate's own acknowledgement frontier.
#[tokio::test]
async fn a_rebuild_retains_the_replayed_tail_for_duplicates() {
    use krabka_protocol::records::{Record, RecordBatch};

    for (initial_sequence, widths) in [(0, [1; 6]), (i32::MAX - 1, [1, 3, 2, 1, 4, 2])] {
        let dir = tempfile::tempdir().unwrap();
        let RollingLog { mut log, config } = snapshot_rolling_log(dir.path());
        let mut sequence = initial_sequence;
        let mut expected = Vec::new();
        for (index, width) in widths.into_iter().enumerate() {
            let base = log.log_end_offset().0;
            let timestamp = 100 + i64::try_from(index).unwrap();
            log.append(&mut RecordBatch {
                producer_id: 42,
                producer_epoch: 0,
                base_sequence: sequence,
                last_offset_delta: width - 1,
                base_timestamp: timestamp,
                max_timestamp: timestamp,
                records: (0..width)
                    .map(|offset_delta| Record {
                        offset_delta,
                        value: Some(bytes::Bytes::from_static(b"v")),
                        ..Record::default()
                    })
                    .collect(),
                ..RecordBatch::default()
            })
            .unwrap();
            expected.push((
                sequence,
                width - 1,
                base,
                base + i64::from(width),
                timestamp,
            ));
            sequence =
                i32::try_from((i64::from(sequence) + i64::from(width)) % (1i64 << 31)).unwrap();
            // Anonymous data separates physical offsets from producer sequences
            // and writes a roll snapshot at the producer batch's end.
            log.append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .unwrap();
        }
        let end = log.log_end_offset().0;
        let seed = expected[1].3;
        log.sync().unwrap();
        assert!(krabka_log::name::producer_snapshot_path(dir.path(), seed).exists());
        drop(log);
        remove_snapshots(dir.path(), (seed + 1)..=end);
        let log = krabka_log::Log::open(dir.path(), config).unwrap();
        assert!(log.recovered_producers()[0].earlier.len() == 4);
        let s = ProducerState::new();
        s.rebuild_from_log("t", PartitionIndex(0), &log)
            .await
            .unwrap();
        for (index, &(sequence, delta, base, frontier, timestamp)) in expected.iter().enumerate() {
            let checked = s
                .check_batch(
                    "t",
                    PartitionIndex(0),
                    SequenceContext::RELEASED,
                    (42, 0),
                    (sequence, delta),
                )
                .await;
            if index == 0 {
                assert!(checked.decision == Decision::OutOfOrder && checked.duplicate == None);
            } else {
                assert!(checked.decision == Decision::Duplicate { base_offset: base });
                let duplicate = checked.duplicate.unwrap();
                assert!(
                    duplicate.base_offset == base
                        && duplicate.last_offset == frontier - 1
                        && duplicate.timestamp == timestamp
                        && duplicate.base_sequence == sequence
                );
                assert!(
                    krabka_verified::produce_durability_frontier(duplicate.base_offset, delta)
                        == Some(frontier)
                        && frontier < end
                );
            }
        }
        assert!(s.check("t", PartitionIndex(0), 42, 0, sequence, 0).await == Decision::Append);
    }
}

/// Sparse batches can span an entire sequence wrap without allocating that
/// many records. A retry names the first matching retained batch, including
/// when its sequence key also matches the last batch after replay.
#[tokio::test]
async fn replay_chooses_first_retained_alias_after_sequence_wrap() {
    use krabka_protocol::records::{Record, RecordBatch};

    let dir = tempfile::tempdir().unwrap();
    let RollingLog { mut log, config } = snapshot_rolling_log(dir.path());
    for (index, (sequence, delta)) in [(0, 0), (1, i32::MAX - 1), (0, 0)].into_iter().enumerate() {
        let timestamp = 100 + i64::try_from(index).unwrap();
        log.append(&mut RecordBatch {
            producer_id: 42,
            producer_epoch: 7,
            base_sequence: sequence,
            last_offset_delta: delta,
            base_timestamp: timestamp,
            max_timestamp: timestamp,
            records: vec![Record {
                offset_delta: delta,
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .unwrap();
    }
    log.sync().unwrap();
    assert!(krabka_log::name::producer_snapshot_path(dir.path(), 1).exists());
    drop(log);
    remove_snapshots(dir.path(), [
        i64::from(i32::MAX) + 1,
        i64::from(i32::MAX) + 2,
    ]);
    let log = krabka_log::Log::open(dir.path(), config).unwrap();
    assert!(log.recovered_producers()[0].earlier.len() == 2);
    let state = ProducerState::new();
    state
        .rebuild_from_log("t", PartitionIndex(0), &log)
        .await
        .unwrap();
    for (sequence, delta, base, frontier, timestamp) in [
        (0, 0, 0, 1, 100),
        (1, i32::MAX - 1, 1, i64::from(i32::MAX) + 1, 101),
    ] {
        let checked = state
            .check_batch(
                "t",
                PartitionIndex(0),
                SequenceContext::RELEASED,
                (42, 7),
                (sequence, delta),
            )
            .await;
        assert!(checked.decision == Decision::Duplicate { base_offset: base });
        let duplicate = checked.duplicate.unwrap();
        assert!(
            duplicate.base_offset == base
                && duplicate.last_offset + 1 == frontier
                && duplicate.timestamp == timestamp
        );
        assert!(
            krabka_verified::produce_durability_frontier(duplicate.base_offset, delta)
                == Some(frontier)
        );
    }
}

/// A snapshot-only reopen reconstructs a multi-record batch's original span,
/// including sequence wraparound, before the producer tracker accepts requests.
#[tokio::test]
async fn snapshot_reload_preserves_retry_frontiers_and_epoch_fencing() {
    for sequence in [0, 1, i32::MAX - 1, i32::MAX] {
        let (dir, config, mut log) = opened_log();
        for _ in 0..3 {
            log.append(&mut default_records_batch().call()).unwrap();
        }
        log.append(
            &mut producer_batch()
                .sequence(sequence)
                .delta(2)
                .timestamp(44)
                .call(),
        )
        .unwrap();
        log.sync().unwrap();
        log.take_producer_snapshot().unwrap();
        assert!(krabka_log::name::producer_snapshot_path(dir.path(), 6).exists());
        drop(log);
        let log = krabka_log::Log::open(dir.path(), config).unwrap();
        let recovered = log.recovered_producers();
        assert!(recovered.len() == 1 && recovered[0].earlier.is_empty());
        let state = ProducerState::new();
        state
            .rebuild_from_log("t", PartitionIndex(0), &log)
            .await
            .unwrap();
        let checked = state
            .check_batch(
                "t",
                PartitionIndex(0),
                SequenceContext::RELEASED,
                (42, 7),
                (sequence, 2),
            )
            .await;
        assert!(checked.decision == Decision::Duplicate { base_offset: 3 });
        let retained = checked.duplicate.unwrap();
        assert!(
            retained.base_sequence == sequence
                && retained.base_offset == 3
                && retained.last_offset == 5
                && retained.timestamp == 44
        );
        assert!(
            krabka_verified::produce_durability_frontier(retained.base_offset, 2)
                == Some(log.log_end_offset().0)
        );
        assert!(
            state
                .check("t", PartitionIndex(0), 42, 6, sequence, 2)
                .await
                == Decision::Fenced
        );
        assert!(!matches!(
            state
                .check("t", PartitionIndex(0), 42, 8, sequence, 2)
                .await,
            Decision::Duplicate { .. }
        ));
        let next = i32::try_from((i64::from(sequence) + 3) % (1i64 << 31)).unwrap();
        assert!(state.check("t", PartitionIndex(0), 42, 7, next, 0).await == Decision::Append);
    }
}

/// A log that rolls on every append, and the config that reopens it.
struct RollingLog {
    log: krabka_log::Log,
    config: krabka_log::LogConfig,
}

/// Every append rolls, exposing the snapshot plus uncovered replay tail.
fn snapshot_rolling_log(path: &std::path::Path) -> RollingLog {
    let config = krabka_log::LogConfig {
        segment_size: krabka_units::prelude::bytes(1),
        ..Default::default()
    };
    let log = krabka_log::Log::open(path, config.clone()).unwrap();
    RollingLog { log, config }
}
