//! End-to-end compaction over a log that holds transactions: what
//! [`Log::compact`] keeps of a compacted topic that an exactly-once producer
//! wrote to, and what its `.txnindex` says afterwards.

use bytes::Bytes;
use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, RecordBatch};
use krabka_units::prelude::mebibytes;
use tempfile::tempdir;

use super::*;
use crate::{
    config::LogConfig,
    log::test_support::{
        abort_marker, commit_marker, compacting_segments, compaction_ctx, keyed_batch,
        set_segment_size,
    },
    txn_index::AbortedTxn,
};

#[derive(Clone, Copy, Default)]
struct ProducerSequence(i32);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct KeyedTransactionSetup<'a> {
    #[default(ProducerId(1000))]
    producer: ProducerId,
    sequence: ProducerSequence,
    #[default((b"k", b"v"))]
    payload: (&'a [u8], &'a [u8]),
}

/// One keyed transactional record; `Log::append` assigns its offset.
fn transactional_keyed(setup: KeyedTransactionSetup<'_>) -> RecordBatch {
    let (key, value) = setup.payload;
    let mut batch = keyed_batch(0, &[(0, key, value)]);
    batch.producer_id = setup.producer.0;
    batch.producer_epoch = 0;
    batch.base_sequence = setup.sequence.0;
    batch.attributes = Attributes::default().with_transactional(true);
    batch
}

/// A compacted log with one batch per sealed segment, and a final append that
/// stays in the active segment.
fn log_with_a_batch_per_segment(dir: &std::path::Path, batches: Vec<RecordBatch>) -> Log {
    let cfg = compacting_segments();
    let mut log = Log::open(dir, cfg).unwrap();
    for mut batch in batches {
        log.append(&mut batch).unwrap();
    }
    let mut tail = keyed_batch(0, &[(0, b"tail", b"t")]);
    log.append(&mut tail).unwrap();
    // The pass groups by `segment.bytes`; widen it so this pass merges them.
    set_segment_size(&mut log, mebibytes(1));
    log
}

fn values_of(log: &Log, key: &[u8]) -> Vec<Bytes> {
    log.read(Offset(0), mebibytes(1))
        .unwrap()
        .batches
        .iter()
        .filter(|batch| !batch.attributes.is_control_batch())
        .flat_map(|batch| batch.records.iter())
        .filter(|record| record.key.as_deref() == Some(key))
        .filter_map(|record| record.value.clone())
        .collect()
}

fn marker_offsets(log: &Log) -> Vec<i64> {
    log.read(Offset(0), mebibytes(1))
        .unwrap()
        .batches
        .iter()
        .filter(|batch| batch.attributes.is_control_batch())
        .map(|batch| batch.base_offset)
        .collect()
}

fn aborted_txns(log: &Log) -> Vec<AbortedTxn> {
    log.aborted_in_range(Offset(0), Offset(10))
}

/// The one aborted transaction the fixtures write: producer 2000's data at
/// offset 2 and its abort marker at offset 3.
fn index_of_the_fixture(log: &Log) -> Vec<AbortedTxn> {
    let index = aborted_txns(log);
    assert2::assert!(index.len() == 1);
    assert2::assert!(index[0].producer_id == ProducerId(2000));
    assert2::assert!(index[0].start_offset == Offset(2));
    assert2::assert!(index[0].last_offset == Offset(3));
    index
}

fn committed_and_aborted_batches(tail: [RecordBatch; 2]) -> Vec<RecordBatch> {
    let mut batches = vec![
        transactional_keyed(KeyedTransactionSetup {
            payload: (b"k", b"committed"),
            ..Default::default()
        }), // 0
        commit_marker(1000, 0), // 1
        transactional_keyed(KeyedTransactionSetup {
            producer: ProducerId(2000),
            payload: (b"k", b"aborted"),
            ..Default::default()
        }), // 2
    ];
    batches.extend(tail);
    batches
}

/// A committed value followed by a newer aborted write under the same key.
fn committed_then_aborted(dir: &std::path::Path) -> Log {
    log_with_a_batch_per_segment(
        dir,
        committed_and_aborted_batches([
            abort_marker(2000, 0),                  // 3
            keyed_batch(0, &[(0, b"after", b"a")]), // 4
        ]),
    )
}

/// A read-committed consumer of a compacted, transactional topic must keep the
/// last committed value of a key whose newest write was aborted. Key `k` is
/// committed at offset 0 and written again inside a transaction that aborts.
/// Kafka's cleaner drops the aborted record and never lets it shadow the
/// committed one; the abort marker and its `.txnindex` entry stay, because the
/// pass still met the aborted batch.
#[test]
fn a_committed_value_survives_a_newer_aborted_write() {
    let dir = tempdir().unwrap();
    let mut log = committed_then_aborted(dir.path());
    let index_before = index_of_the_fixture(&log);

    log.compact(&compaction_ctx()).unwrap();

    assert2::assert!(values_of(&log, b"k") == vec![Bytes::from_static(b"committed")]);
    assert2::assert!(marker_offsets(&log) == vec![1, 3]);
    assert2::assert!(aborted_txns(&log) == index_before);
}

/// The pass after the one that dropped the aborted data finds no batch of the
/// transaction left, so it stamps the abort marker and drops the transaction's
/// `.txnindex` entry, and the pass after the delete horizon removes the marker.
///
/// That holds once the producers have expired. While producer 2000 is active,
/// Kafka's cleaner keeps its last data batch as an empty batch
/// (`isBatchLastRecordOfProducer` in `Cleaner.cleanInto`). Every pass then
/// meets a batch of the aborted transaction, so the marker and the index entry
/// stay.
#[test]
fn an_abort_marker_and_its_index_entry_age_out_once_the_aborted_data_is_gone() {
    /// Kafka's default `producer.id.expiration.ms`.
    const EXPIRATION_MS: i64 = 86_400_000;
    // (case, whether the producers expire before the first pass, then for
    // each pass the marker offsets and whether the index entry stays)
    let cases = [
        (
            "expired producers",
            true,
            [(vec![1, 3], true), (vec![1, 3], false), (vec![1], false)],
        ),
        (
            "active producers",
            false,
            [(vec![1, 3], true), (vec![1, 3], true), (vec![1, 3], true)],
        ),
    ];
    // The third pass runs after the delete horizon of the first two.
    let delete_retention_ms = LogConfig::default().delete_retention.millis_i64_trunc();
    let horizon_passed = CompactionContext {
        now: std::time::SystemTime::UNIX_EPOCH
            + std::time::Duration::from_millis(u64::try_from(delete_retention_ms).unwrap() + 1_000),
        ..compaction_ctx()
    };
    let passes = [compaction_ctx(), compaction_ctx(), horizon_passed];
    for (name, expired, want) in cases {
        let dir = tempdir().unwrap();
        let mut log = committed_then_aborted(dir.path());
        let index_before = index_of_the_fixture(&log);
        if expired {
            log.remove_expired_producers(EXPIRATION_MS, EXPIRATION_MS);
        }

        let mut seen = Vec::new();
        for ctx in &passes {
            log.compact(ctx).unwrap();
            seen.push((marker_offsets(&log), aborted_txns(&log) == index_before));
        }

        assert2::assert!(seen == want.to_vec(), "{name}");
        assert2::assert!(
            values_of(&log, b"k") == vec![Bytes::from_static(b"committed")],
            "{name}"
        );
    }
}

/// The pass can stop before a transaction's abort marker, when the last stable
/// offset or the min compaction lag ends its range there. The transaction's
/// `.txnindex` entry then sits in a segment the pass does not consume, and the
/// pass must still find it: it drops the aborted batch inside the range and
/// keeps the committed value.
#[test]
fn an_aborted_batch_is_dropped_when_its_marker_lies_beyond_the_consumed_range() {
    let dir = tempdir().unwrap();
    let mut log = log_with_a_batch_per_segment(
        dir.path(),
        committed_and_aborted_batches([
            keyed_batch(0, &[(0, b"other", b"o")]), // 3
            abort_marker(2000, 0),                  // 4
        ]),
    );
    // The pass consumes the sealed segments below offset 4 only.
    let ctx = CompactionContext {
        last_stable_offset: Offset(4),
        ..compaction_ctx()
    };

    log.compact(&ctx).unwrap();

    assert2::assert!(values_of(&log, b"k") == vec![Bytes::from_static(b"committed")]);
    assert2::assert!(values_of(&log, b"other") == vec![Bytes::from_static(b"o")]);
    assert2::assert!(marker_offsets(&log) == vec![1, 4]);
}
