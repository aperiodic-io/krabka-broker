//! Unit tests for the owned append paths: offset assignment, the
//! caller-supplied `append_at` offset, the leader-epoch transitions all
//! four append paths record, and the size-driven segment roll.

use assert2::{assert, check};
use krabka_units::prelude::bytes;
use tempfile::tempdir;

use super::*;
use crate::{
    config::{DeliveryPolicy, LogConfig, ScheduleOrder},
    io::{FileIo, LogIo},
    leader_epoch_checkpoint::EpochEntry,
    log::test_support::{
        abort_marker, append_transaction, assert_empty_append_state, assert_only_active,
        check_append_time_lookup, configured_test_log, gzip_value, install_stamps,
        log_append_time_log, rolling_test_log, sample_batch, sample_batch_with_epoch, sample_log,
        stamped_test_log, test_batch_at, test_log, tiny_segments, transactional_batch,
        verbatim_from, write_with_budget,
    },
    name::log_path,
    retention::now_ms,
    stamp_index::{StampEntry, StampIndex},
    test_support::{log_file_count, open_log},
};

#[derive(Debug)]
struct FailAfterBytes(std::sync::Mutex<usize>);

impl LogIo for FailAfterBytes {
    fn write(&self, file: &std::fs::File, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = self.0.lock().unwrap();
        write_with_budget(file, buf, &mut remaining)
    }
}

#[derive(Debug)]
struct FailFirstSync(std::sync::atomic::AtomicUsize);

impl LogIo for FailFirstSync {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            Err(std::io::Error::other("injected sync_data failure"))
        } else {
            file.sync_data()
        }
    }
}

#[derive(Debug)]
struct CountSync(std::sync::atomic::AtomicUsize);

impl LogIo for CountSync {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        file.sync_data()
    }
}

#[derive(Debug)]
struct FailSyncAt {
    calls: std::sync::atomic::AtomicUsize,
    fail_at: usize,
}

impl LogIo for FailSyncAt {
    fn sync_data(&self, file: &std::fs::File) -> std::io::Result<()> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if call == self.fail_at {
            Err(std::io::Error::other("injected ordered sync failure"))
        } else {
            file.sync_data()
        }
    }
}

fn fail_after_bytes(log: &mut Log, budget: usize) {
    log.test_set_io(std::sync::Arc::new(FailAfterBytes(std::sync::Mutex::new(
        budget,
    ))));
}

fn assert_storage_full(error: &LogError) {
    assert!(
        matches!(error, LogError::Io(error) if error.kind() == std::io::ErrorKind::StorageFull)
    );
}

#[test]
fn partial_write_rolls_back_cursor_and_recovers_pre_append_state() {
    let (dir, mut log) = test_log();
    fail_after_bytes(&mut log, 16);

    let error = log.append(&mut sample_batch(2)).unwrap_err();

    assert_storage_full(&error);
    assert!(log.log_end_offset() == Offset(0));
    assert!(log.producer_state_snapshot().is_empty());
    drop(log);
    let recovered = open_log(dir.path());
    assert!(recovered.log_end_offset() == Offset(0));
}

#[test]
fn shorter_append_after_partial_write_leaves_no_physical_tail() {
    let (dir, mut log) = test_log();
    let mut first = sample_batch(1);
    log.append(&mut first).unwrap();
    let log_path = log_path(dir.path(), 0);
    let position = std::fs::metadata(&log_path).unwrap().len();

    let mut next = sample_batch(1);
    let mut failed = sample_batch(10);
    let partial_len = next.encoded_len() + 1;
    assert!(partial_len < failed.encoded_len());
    fail_after_bytes(&mut log, partial_len);

    let error = log.append(&mut failed).unwrap_err();

    assert_storage_full(&error);
    assert!(std::fs::metadata(&log_path).unwrap().len() == position);

    log.test_set_io(std::sync::Arc::new(FileIo));
    let next_len = next.encoded_len() as u64;
    log.append(&mut next).unwrap();

    assert!(std::fs::metadata(log_path).unwrap().len() == position + next_len);
    assert!(
        log.read(Offset(0), crate::log::test_support::NO_LIMIT)
            .unwrap()
            .batches
            == vec![first, next]
    );
}

#[test]
fn sync_failure_rolls_back_leo_producer_and_transaction_state() {
    let dir = tempdir().unwrap();
    let config = LogConfig {
        flush_on_append: true,
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config.clone()).unwrap();
    let io = std::sync::Arc::new(FailFirstSync(std::sync::atomic::AtomicUsize::new(0)));
    log.test_set_io(io.clone());
    let producer = ProducerId(41);

    let error = log
        .append(&mut transactional_batch(producer.get(), 0, &["value"]))
        .unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert!(io.0.load(std::sync::atomic::Ordering::Relaxed) == 2);
    assert_empty_append_state(&log);
    assert!(log.pending_transaction_start(producer) == None);
    drop(log);
    let recovered = Log::open(dir.path(), config).unwrap();
    assert!(recovered.log_end_offset() == Offset(0));
    assert!(recovered.producer_state_snapshot().is_empty());
    assert!(recovered.pending_transaction_start(producer) == None);
}

/// Every append path hands every appended batch's leader epoch to Kafka's
/// `LeaderEpochFileCache.assign`, as `UnifiedLog.append` does, and the whole
/// resulting epoch history is compared.
///
/// Batch `i` of a row lands at offset `i`. The `-1` "no epoch" sentinel
/// records nothing. The latest epoch again is a no-op. A batch stamped with
/// an older epoch than the latest is still assigned, and drops every trailing
/// entry it does not strictly follow: a gate that only records advances would
/// leave the newer epoch in place.
/// One way of appending a batch to a log: the append path's name and the
/// call that appends at `(log, position, leader_epoch)`.
type AppendPath<'a> = Box<dyn Fn(&mut Log, usize, i32) + 'a>;

#[test]
fn every_appended_batch_is_assigned_its_leader_epoch() {
    let entry = |epoch, start_offset| EpochEntry {
        epoch: LeaderEpoch(epoch),
        start_offset: Offset(start_offset),
    };
    let rows: [(&str, &[i32], Vec<EpochEntry>); 3] = [
        (
            "unknown epochs record nothing and a repeat is a no-op",
            &[-1, 5, 5, 6, -1],
            vec![entry(5, 1), entry(6, 3)],
        ),
        (
            "an older epoch truncates the entries it does not follow",
            &[1, 3, 2],
            vec![entry(1, 0), entry(2, 2)],
        ),
        (
            "an epoch older than every entry replaces them all",
            &[4, 6, 3],
            vec![entry(3, 2)],
        ),
    ];

    for (name, offered, expected) in rows {
        let at = |i: usize| Offset(i64::try_from(i).unwrap());
        let paths: [(&str, AppendPath<'_>); 4] = [
            (
                "append",
                Box::new(|log, _, epoch| {
                    let mut batch = sample_batch(1);
                    batch.partition_leader_epoch = epoch;
                    log.append(&mut batch).expect("append");
                }),
            ),
            (
                "append_at",
                Box::new(|log, i, epoch| {
                    let mut batch = sample_batch(1);
                    batch.partition_leader_epoch = epoch;
                    log.append_at(&mut batch, at(i)).expect("append_at");
                }),
            ),
            (
                "append_verbatim",
                Box::new(|log, _, epoch| {
                    let (_wire, vb) = verbatim_from(&test_batch_at(0), LeaderEpoch(epoch));
                    log.append_verbatim(&vb).expect("append_verbatim");
                }),
            ),
            (
                "append_verbatim_at",
                Box::new(|log, i, epoch| {
                    let (_wire, vb) = verbatim_from(&test_batch_at(0), LeaderEpoch(epoch));
                    log.append_verbatim_at(&vb, at(i))
                        .expect("append_verbatim_at");
                }),
            ),
        ];
        for (path, append) in &paths {
            let (_dir, mut log) = test_log();
            for (i, epoch) in offered.iter().enumerate() {
                append(&mut log, i, *epoch);
            }
            check!(
                log.epoch_checkpoint().entries() == &expected[..],
                "{path}: {name}"
            );
        }
    }
}

#[test]
fn append_at_uses_reconciled_frontier_floor() {
    let (dir, mut log) = test_log();
    let mut prefix = test_batch_at(0);
    log.append(&mut prefix).unwrap();

    log.reconcile_next_offset(Offset(3));
    let mut gap_batch = test_batch_at(0);
    let mut rejected = gap_batch.clone();
    let err = log.append_at(&mut rejected, Offset(1)).unwrap_err();
    assert!(matches!(
        err,
        LogError::OffsetMismatch {
            expected: Offset(3),
            actual: Offset(1)
        }
    ));

    log.append_at(&mut gap_batch, Offset(3)).unwrap();
    assert!(log.log_end_offset() == Offset(4));
    drop(dir);
}

/// Kafka trunk's `UnifiedLog.appendAsFollower` skips `LogValidator`, the one
/// place an append reads `max.decompressed.message.bytes`, so a replicated
/// record that got past the leader's limit lands as it is. The limit governs the
/// log's later reads, not the write.
#[test]
fn a_replicated_batch_is_not_held_to_the_decompressed_record_limit() {
    let (_dir, mut log) = configured_test_log(LogConfig {
        max_decompressed_record: Some(bytes(100)),
        ..LogConfig::default()
    });
    let mut batch = test_batch_at(0);
    gzip_value(&mut batch, 1_000);

    log.append_at(&mut batch, Offset(0)).unwrap();

    assert!(log.log_end_offset() == Offset(1));
}

#[test]
fn assigned_append_uses_reconciled_frontier_floor() {
    let (_dir, mut log) = test_log();
    log.append(&mut test_batch_at(0)).unwrap();
    log.reconcile_next_offset(Offset(3));

    let mut batch = test_batch_at(0);
    let (assigned, _) = log.append(&mut batch).unwrap();

    assert!(assigned == Offset(3));
    assert!(batch.base_offset == 3);
    assert!(log.log_end_offset() == Offset(4));
}

#[test]
fn invalid_interval_is_write_free_and_a_corrected_retry_succeeds() {
    let (_dir, mut log) = test_log();
    let mut invalid = sample_batch(1);
    invalid.last_offset_delta = -1;

    let error = log.append(&mut invalid).unwrap_err();

    assert!(matches!(error, LogError::InvalidArgument(_)));
    assert_empty_append_state(&log);

    let mut retry = sample_batch(1);
    assert!(log.append(&mut retry).unwrap().0 == Offset(0));
    assert!(log.log_end_offset() == Offset(1));
}

#[test]
fn successor_overflow_is_rejected_before_log_mutation() {
    let (_dir, mut log) = test_log();
    log.reconcile_next_offset(Offset(i64::MAX));
    let mut batch = sample_batch(1);

    let error = log.append(&mut batch).unwrap_err();

    assert!(matches!(error, LogError::InvalidArgument(_)));
    assert_empty_append_state(&log);
}

#[test]
fn sidecar_failure_rolls_back_bytes_frontiers_and_allows_retry() {
    let (_dir, mut log) = stamped_test_log(100, 1);
    log.stamp_indexes.clear();

    let error = log.append(&mut sample_batch(1)).unwrap_err();

    assert!(matches!(error, LogError::Corrupt(_)));
    assert_empty_append_state(&log);
    assert!(log.active_txn_index.entries().is_empty());
    assert!(log.stamp_for_offset(Offset(0)).is_none());

    assert!(log.append(&mut sample_batch(1)).unwrap().0 == Offset(0));
    assert!(log.log_end_offset() == Offset(1));
    assert!(log.lso() == Offset(1));
    assert!(log.stamp_for_offset(Offset(0)).is_some());
}

#[test]
fn segment_bytes_are_synced_before_durable_sidecars() {
    let (_dir, mut stamped_log) = stamped_test_log(100, 1);
    let stamp_syncs = std::sync::Arc::new(CountSync(std::sync::atomic::AtomicUsize::new(0)));
    stamped_log.test_set_io(stamp_syncs.clone());

    stamped_log.append(&mut sample_batch(1)).unwrap();
    assert!(stamp_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 1);

    let (_dir, mut transaction_log) = test_log();
    let txn_syncs = std::sync::Arc::new(CountSync(std::sync::atomic::AtomicUsize::new(0)));
    transaction_log.test_set_io(txn_syncs.clone());
    append_transaction(&mut transaction_log, (7, 0), &["value"]);
    assert!(txn_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 0);

    transaction_log.append(&mut abort_marker(7, 0)).unwrap();
    assert!(txn_syncs.0.load(std::sync::atomic::Ordering::Relaxed) == 1);
}

#[test]
fn post_roll_failure_restores_the_previous_active_segment() {
    let (_dir, mut log) = configured_test_log(LogConfig {
        flush_on_append: true,
        ..tiny_segments()
    });
    log.append(&mut sample_batch(1)).unwrap();
    install_stamps(&mut log, 100, 1);
    let io = std::sync::Arc::new(FailSyncAt {
        calls: std::sync::atomic::AtomicUsize::new(0),
        fail_at: 2,
    });
    log.test_set_io(io.clone());

    let error = log.append(&mut sample_batch(1)).unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert!(io.calls.load(std::sync::atomic::Ordering::Relaxed) == 4);
    assert_only_active(&log, Offset(1), Offset(0));
    assert!(log.lso() == Offset(1));
    assert!(log.stamp_for_offset(Offset(1)).is_none());

    assert!(log.append(&mut sample_batch(1)).unwrap().0 == Offset(1));
    assert!(log.log_end_offset() == Offset(2));
}

#[test]
fn post_roll_partial_write_restores_the_previous_active_segment() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    log.append(&mut sample_batch(1)).unwrap();
    fail_after_bytes(&mut log, 16);

    let error = log.append(&mut sample_batch(2)).unwrap_err();

    assert!(matches!(error, LogError::Io(_)));
    assert_only_active(&log, Offset(1), Offset(0));

    log.test_set_io(std::sync::Arc::new(FileIo));
    assert!(log.append(&mut sample_batch(1)).unwrap().0 == Offset(1));
    assert!(log.log_end_offset() == Offset(2));
}

#[test]
fn append_assigns_monotonic_offsets() {
    let (_dir, mut log) = test_log();
    let mut b1 = sample_batch(3);
    let mut b2 = sample_batch(2);
    let (first_offset, _) = log.append(&mut b1).unwrap();
    let (second_offset, _) = log.append(&mut b2).unwrap();
    assert2::assert!(first_offset == Offset(0));
    assert2::assert!(second_offset == Offset(3));
    assert2::assert!(log.log_end_offset() == Offset(5));
}

#[test]
fn append_at_matching_offset_preserves_caller_offset() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch(3);
    // Pretend the caller (a replicator) already knows the leader's
    // assigned offset for this batch is 0.
    log.append_at(&mut b, Offset(0)).unwrap();
    assert2::assert!(b.base_offset == 0);
    assert2::assert!(log.log_end_offset() == Offset(3));

    let mut b2 = sample_batch(2);
    log.append_at(&mut b2, Offset(3)).unwrap();
    assert2::assert!(b2.base_offset == 3);
    assert2::assert!(log.log_end_offset() == Offset(5));
}

/// Kafka's `appendAsFollower` refuses a first offset below the log end
/// offset, the duplicate or divergence a replicator has to notice.
#[test]
fn append_at_below_the_log_end_offset_errors() {
    let (_dir, mut log) = test_log();
    log.append(&mut sample_batch(3)).unwrap();
    let mut b = sample_batch(2);
    let err = log.append_at(&mut b, Offset(2)).unwrap_err();
    assert2::assert!(matches!(
        err,
        LogError::OffsetMismatch {
            expected: Offset(3),
            actual: Offset(2)
        }
    ));
    // Failure must not advance the log.
    assert2::assert!(log.log_end_offset() == 3);
}

/// ...and it takes a first offset above the log end offset, the hole a
/// compacted leader's log has between two batches.
#[test]
fn append_at_past_the_log_end_offset_leaves_a_hole() {
    let (_dir, mut log) = test_log();
    log.append_at(&mut sample_batch(2), Offset(0)).unwrap();
    let mut b = sample_batch(2);

    log.append_at(&mut b, Offset(7)).unwrap();

    assert2::assert!(b.base_offset == 7);
    assert2::assert!(log.log_end_offset() == Offset(9));
    // The floor moves with the append.
    assert2::assert!(matches!(
        log.append_at(&mut sample_batch(1), Offset(8)),
        Err(LogError::OffsetMismatch {
            expected: Offset(9),
            actual: Offset(8)
        })
    ));
}

#[test]
fn segment_rolls_when_bytes_exceeded() {
    // tiny so we roll fast
    let (dir, _log) = sample_log(
        LogConfig {
            segment_size: bytes(200),
            ..LogConfig::default()
        },
        5,
        2,
    );
    // Multiple .log files should exist now.
    let log_files = log_file_count(dir.path());
    assert2::assert!(log_files >= 2);
}

/// Kafka's `LogSegment.shouldRoll` measures the wait to roll against the
/// gap between the incoming batch's own timestamp and the segment's first
/// record's timestamp, never the wall clock. Table over
/// (`segment.ms`, first batch timestamp, incoming batch timestamp) to
/// rolled or not, all with a `segment.bytes` too large to roll on size.
#[test]
fn segment_ms_rolls_against_the_incoming_batch_timestamp() {
    use krabka_units::prelude::{gibibytes, millis};

    // `(segment_ms, first_ts, incoming_ts, expect_roll, label)`.
    let cases: [(u32, i64, i64, bool, &str); 4] = [
        (10_000, 0, 10_001, true, "gap exceeds segment.ms"),
        (
            10_000,
            0,
            10_000,
            false,
            "gap equal to segment.ms does not roll",
        ),
        (10_000, 0, 5_000, false, "within segment.ms"),
        // A backfill batch with an old timestamp: the gap is measured from
        // the batch's own stamp, not from when it arrived, so a producer
        // backfilling old records rolls on krabka exactly when it would on
        // Kafka and not otherwise.
        (
            10_000,
            100_000,
            80_000,
            false,
            "backfill batch narrows the gap",
        ),
    ];
    for (segment_ms, first_ts, incoming_ts, expect_roll, label) in cases {
        let (_dir, mut log) = configured_test_log(LogConfig {
            segment_size: gibibytes(1),
            segment_roll_interval: millis(segment_ms),
            ..LogConfig::default()
        });
        let mut first = test_batch_at(0);
        first.base_timestamp = first_ts;
        first.max_timestamp = first_ts;
        log.append(&mut first).unwrap();

        let mut second = test_batch_at(0);
        second.base_timestamp = incoming_ts;
        second.max_timestamp = incoming_ts;
        log.append(&mut second).unwrap();

        let rolled = !log.segments.is_empty();
        check!(rolled == expect_roll, "{label}");
    }
}

/// An idle partition never rolls: nothing (not even a later `tick`) rolls a
/// segment that never sees another append, because the roll test only ever
/// runs inside the append itself.
#[test]
fn an_idle_partition_never_rolls() {
    use krabka_units::prelude::millis;

    let (_dir, mut log) = configured_test_log(LogConfig {
        segment_roll_interval: millis(1),
        ..LogConfig::default()
    });
    log.append(&mut sample_batch(1)).unwrap();

    assert!(log.segments.is_empty());
    assert!(log.active.as_ref().unwrap().base_offset() == Offset(0));
}

// Kafka's `LogSegment.shouldRoll` rolls once `offsetIndex().isFull()` or
// `timeIndex().isFull()`. `segment.index.bytes` caps both: the offset index
// holds `size / 8` entries, and the time index is full one entry short of
// `size / 12`. With `index.interval.bytes=1`, every batch but a segment's
// first adds an offset-index entry, and a time-index entry when it carries the
// segment's newest timestamp. Each case appends ten one-record batches and
// compares the base offset of every segment.
#[test]
fn a_full_offset_or_time_index_rolls_the_segment() {
    use krabka_units::prelude::mebibytes;

    // `(label, segment.index.bytes, rising timestamps, segment bases)`.
    let cases: [(&str, ByteSize, bool, Vec<i64>); 5] = [
        (
            "a time index too small for an entry rolls every batch",
            bytes(12),
            true,
            (0..10).collect(),
        ),
        (
            "two time-index slots fill after one entry",
            bytes(24),
            true,
            vec![0, 2, 4, 6, 8],
        ),
        (
            "three time-index slots fill after two entries",
            bytes(36),
            true,
            vec![0, 3, 6, 9],
        ),
        (
            "a flat clock leaves the four offset-index slots to fill",
            bytes(36),
            false,
            vec![0, 5],
        ),
        (
            "Kafka's default holds every entry",
            mebibytes(10),
            true,
            vec![0],
        ),
    ];
    for (label, segment_index_size, rising, expected) in cases {
        let (_dir, mut log) = configured_test_log(LogConfig {
            index_interval: bytes(1),
            segment_index_size,
            ..LogConfig::default()
        });
        for timestamp in 0..10 {
            let mut batch = sample_batch(1);
            batch.max_timestamp = if rising { 1_000 + timestamp } else { 1_000 };
            log.append(&mut batch).unwrap();
        }

        let bases: Vec<i64> = log
            .segments
            .iter()
            .chain(log.active.as_ref())
            .map(|segment| segment.base_offset().0)
            .collect();
        check!(bases == expected, "{label}");
    }
}

/// A roll to a new segment reopens the active `.stampindex` at the
/// sidecar of the new segment. The entry for the post-roll batch lands in
/// the new segment's file and does not leak back into the sealed
/// segment's file. This test guards the reopen. A reopen that did nothing
/// would keep the stamps in the sealed segment's index.
#[test]
fn roll_reopens_stampindex_for_new_segment() {
    let dir = tempdir().unwrap();
    let mut log = rolling_test_log(dir.path());
    install_stamps(&mut log, 100, 5);

    log.append(&mut sample_batch(1)).unwrap(); // offset 0, segment 0, stamp 100
    log.append(&mut sample_batch(1)).unwrap(); // rolls: segment @ base 1, stamp 105

    // The new segment's own sidecar holds exactly its post-roll entry.
    let seg1 = StampIndex::open(dir.path().join("00000000000000000001.stampindex")).unwrap();
    assert2::assert!(
        seg1.entries()
            == [StampEntry {
                base_offset: Offset(1),
                last_offset: Offset(1),
                stamp: 105,
            }]
    );
    // The sealed segment kept only its pre-roll entry — nothing leaked back.
    let seg0 = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
    assert2::assert!(
        seg0.entries()
            == [StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(0),
                stamp: 100,
            }]
    );
}

#[test]
fn append_records_epoch_transition() {
    let (_dir, mut log) = test_log();
    let mut b = sample_batch_with_epoch(3, 0);
    log.append(&mut b).unwrap();
    let mut b2 = sample_batch_with_epoch(2, 1); // 2 records at epoch 1
    log.append(&mut b2).unwrap();
    assert2::assert!(
        log.epoch_checkpoint().entries()
            == &[
                EpochEntry {
                    epoch: LeaderEpoch(0),
                    start_offset: Offset(0)
                },
                EpochEntry {
                    epoch: LeaderEpoch(1),
                    start_offset: Offset(3)
                }
            ]
    );
}

/// The first record of the only batch the log holds, read back off disk.
fn only_batch(log: &Log) -> RecordBatch {
    let end = log.log_end_offset();
    let raw = log
        .read_raw(Offset(0), end, krabka_units::prelude::mebibytes(10))
        .unwrap();
    let mut cursor: &[u8] = &raw.bytes;
    RecordBatch::decode(&mut cursor).expect("the stored batch decodes, so its CRC is valid")
}

/// Kafka's `LogValidator` rewrites exactly three header fields on a
/// `LogAppendTime` topic: the timestamp-type attribute bit, `max_timestamp`,
/// and the CRC over them. `base_timestamp` and the per-record deltas are the
/// producer's own, because a reader substitutes `max_timestamp` for every
/// record while the bit is set.
#[test]
fn log_append_time_stamps_the_batch_and_leaves_the_producer_timestamps_alone() {
    let (dir, mut log) = log_append_time_log();
    let mut produced = test_batch_at(0);
    produced.base_timestamp = 1_000;
    produced.max_timestamp = 1_000;
    let before = now_ms(std::time::SystemTime::now());

    let (base_offset, stamp) = log.append(&mut produced).unwrap();

    let after = now_ms(std::time::SystemTime::now());
    let stamp = stamp.expect("a LogAppendTime log reports the stamp it wrote");
    check!(base_offset == Offset(0));
    check!((before..=after).contains(&stamp));
    // The whole stored batch, not a field at a time: everything but the three
    // rewritten fields is what the producer sent.
    let expected = RecordBatch {
        base_offset: 0,
        partition_leader_epoch: produced.partition_leader_epoch,
        attributes: produced
            .attributes
            .with_timestamp_type(krabka_protocol::records::TimestampType::LogAppendTime),
        last_offset_delta: produced.last_offset_delta,
        base_timestamp: 1_000,
        max_timestamp: stamp,
        producer_id: produced.producer_id,
        producer_epoch: produced.producer_epoch,
        base_sequence: produced.base_sequence,
        records: produced.records.clone(),
    };
    check!(only_batch(&log) == expected);
    drop(dir);
}

/// The default topic stamps nothing, so the produce response has no
/// `logAppendTimeMs` to report and the records keep the producer's clock.
#[test]
fn create_time_leaves_the_batch_untouched_and_reports_no_stamp() {
    let (dir, mut log) = test_log();
    let mut produced = test_batch_at(0);
    let expected = RecordBatch {
        base_offset: 0,
        ..produced.clone()
    };

    let (base_offset, stamp) = log.append(&mut produced).unwrap();

    check!(base_offset == Offset(0));
    check!(stamp == None);
    check!(only_batch(&log) == expected);
    drop(dir);
}

/// Kafka builds the time index from the rewritten `max_timestamp`, so a
/// `ListOffsets` by time on a `LogAppendTime` topic answers in append time: the
/// producer's own timestamp finds nothing, and the stamp finds the batch.
#[test]
fn offset_for_timestamp_answers_in_append_time() {
    let (dir, mut log) = log_append_time_log();
    let mut produced = test_batch_at(0);
    // Far enough in the past that no clock reading can collide with it.
    produced.base_timestamp = 1_000;
    produced.max_timestamp = 1_000;

    let (_base_offset, stamp) = log.append(&mut produced).unwrap();

    let stamp = check_append_time_lookup(&log, stamp);
    check!(log.offset_for_timestamp(stamp) == Some((Offset(0), stamp)));
    check!(log.offset_for_timestamp(stamp + 1) == None);
    drop(dir);
}

/// A log on a topic with `delivery.mode=scheduled` and, when `monotonic`,
/// `delivery.schedule.monotonic=true`.
fn scheduled_log(monotonic: ScheduleOrder) -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let log = Log::open(
        dir.path(),
        LogConfig {
            delivery_policy: DeliveryPolicy::Scheduled,
            schedule_order: monotonic,
            ..LogConfig::default()
        },
    )
    .unwrap();
    (dir, log)
}

/// A one-record batch whose delivery time — its `max_timestamp` on a
/// scheduled partition — is `delivery_ms`.
fn batch_delivered_at(delivery_ms: i64) -> RecordBatch {
    let mut batch = test_batch_at(0);
    batch.base_timestamp = delivery_ms;
    batch.max_timestamp = delivery_ms;
    batch
}

/// KFC-1 `delivery.schedule.monotonic`. The delivery times the partition holds
/// run out to `2_000`, and a batch is refused exactly when its own delivery time
/// is strictly below that. Both offset-assigning leader appends enforce it, and
/// a refusal appends nothing.
#[test]
fn a_monotonic_scheduled_log_refuses_a_batch_that_precedes_its_schedule() {
    /// `(delivery_ms, refused, label)`.
    const CASES: [(i64, bool, &str); 5] = [
        (2_001, false, "after the largest delivery time held"),
        (2_000, false, "equal to it, which does not run backwards"),
        (1_999, true, "one millisecond before it"),
        (1_500, true, "between the two batches already scheduled"),
        (0, true, "behind the whole schedule"),
    ];

    for (delivery_ms, refused, label) in CASES {
        for verbatim in [false, true] {
            let (dir, mut log) = scheduled_log(ScheduleOrder::Monotonic);
            for seeded in [1_000, 2_000] {
                log.append(&mut batch_delivered_at(seeded))
                    .expect("seed the partition schedule");
            }
            let end = log.log_end_offset();

            let result = if verbatim {
                let (_wire, batch) =
                    verbatim_from(&batch_delivered_at(delivery_ms), LeaderEpoch(1));
                log.append_verbatim(&batch).map(|(base, _stamp)| base)
            } else {
                log.append(&mut batch_delivered_at(delivery_ms))
                    .map(|(base, _stamp)| base)
            };

            check!(
                matches!(
                    &result,
                    Err(LogError::ScheduleRunsBackwards { delivery_ms: refused_ms })
                        if *refused_ms == delivery_ms
                ) == refused,
                "case: {label}, verbatim: {verbatim}"
            );
            // A refused batch leaves the log exactly where it found it; an
            // admitted one extends it by its single record.
            let want_end = if refused { end } else { Offset(end.0 + 1) };
            check!(log.log_end_offset() == want_end, "case: {label}");
            drop(dir);
        }
    }
}

/// The same schedule without the setting, and the same schedule on a topic
/// that delivers immediately. Neither reads the schedule at all, so both
/// admit the batch that the case above refuses.
#[test]
fn only_a_monotonic_scheduled_log_reads_the_schedule() {
    let (dir, mut log) = scheduled_log(ScheduleOrder::Unordered);
    log.append(&mut batch_delivered_at(2_000)).unwrap();
    check!(log.append(&mut batch_delivered_at(0)).is_ok());
    drop(dir);

    let (dir, mut log) = test_log();
    log.append(&mut batch_delivered_at(2_000)).unwrap();
    check!(log.append(&mut batch_delivered_at(0)).is_ok());
    drop(dir);
}
