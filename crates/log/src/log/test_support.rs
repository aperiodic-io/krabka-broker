//! Fixtures shared by the unit tests of the `log` submodules: batch
//! builders, marker builders, and the small log harnesses they open.
//!
//! The builders live here rather than beside one test module because the
//! same transactional batch and the same control marker are what several
//! submodules assert on.

use std::{fs::File, io::Write as _, time::SystemTime};

use bytes::Bytes;
use krabka_ids::{LeaderEpoch, Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{ByteSize, bytes, gibibytes};
use tempfile::tempdir;

use super::{BARRIER_CONTROL_TYPE, CompactionContext, Log, VerbatimBatch};
use crate::{
    CleanupPolicy, config::LogConfig, producer_snapshot::ProducerSnapshotEntry,
    txn_index::AbortedTxn,
};

/// A read budget larger than anything these tests write, so the byte
/// budget never clips the result.
pub const NO_LIMIT: ByteSize = gibibytes(4);

pub fn sample_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset: 0, // overwritten by Log::append
        max_timestamp: 0,
        last_offset_delta: n - 1,
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(crate::test_support::numbered_record(i, 0));
    }
    b
}

/// Append numbered sample records, retaining the caller's batch count and size.
pub fn append_samples(log: &mut Log, batches: usize, records_per_batch: i32) {
    for _ in 0..batches {
        let mut batch = sample_batch(records_per_batch);
        log.append(&mut batch).expect("append");
    }
}

pub fn test_log() -> (tempfile::TempDir, Log) {
    configured_test_log(LogConfig::default())
}

pub fn configured_test_log(config: LogConfig) -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let log = Log::open(dir.path(), config).unwrap();
    (dir, log)
}

pub fn sample_log(config: LogConfig, batches: usize, records: i32) -> (tempfile::TempDir, Log) {
    let (dir, mut log) = configured_test_log(config);
    append_samples(&mut log, batches, records);
    (dir, log)
}

/// A refused append must not create offsets, transactions, or producer state.
pub fn assert_empty_append_state(log: &Log) {
    assert2::assert!(log.log_end_offset() == Offset(0));
    assert2::assert!(log.lso() == Offset(0));
    assert2::assert!(log.producer_state_snapshot().is_empty());
}

/// Preserve the original active segment when a roll fails.
pub fn assert_only_active(log: &Log, end: Offset, base: Offset) {
    assert2::assert!(log.log_end_offset() == end);
    assert2::assert!(log.segments.is_empty());
    assert2::assert!(log.active.as_ref().unwrap().base_offset() == base);
}

pub fn check_append_time_lookup(log: &Log, stamp: Option<i64>) -> i64 {
    let stamp = stamp.expect("a LogAppendTime log reports the stamp it wrote");
    assert2::check!(log.offset_for_timestamp(1_000) == Some((Offset(0), stamp)));
    stamp
}

/// Compress the existing batch and replace its first value with an explicit length.
pub fn gzip_value(batch: &mut RecordBatch, length: usize) {
    batch.attributes = batch
        .attributes
        .with_compression(krabka_compression::CompressionType::Gzip);
    batch.records[0].value = Some(Bytes::from(vec![7_u8; length]));
}

pub fn synced_sample_log(
    dir: &std::path::Path,
    config: LogConfig,
    batches: usize,
    records: i32,
) -> Log {
    let mut log = Log::open(dir, config).unwrap();
    append_samples(&mut log, batches, records);
    log.sync().unwrap();
    log
}

pub fn stamped_rolling_log(first: u64, step: u64, batches: usize) -> (tempfile::TempDir, Log) {
    let (dir, mut log) = configured_test_log(tiny_segments());
    install_stamps(&mut log, first, step);
    append_samples(&mut log, batches, 1);
    (dir, log)
}

/// Install an explicit monotonic stamp sequence on an existing log.
pub fn install_stamps(log: &mut Log, first: u64, step: u64) {
    log.set_stamp_source(std::sync::Arc::new(
        crate::stamp_source::MonotonicStampSource::new(first, step),
    ))
    .unwrap();
}

/// A default log with the caller's explicit stamp sequence installed.
pub fn stamped_test_log(first: u64, step: u64) -> (tempfile::TempDir, Log) {
    let (dir, mut log) = test_log();
    install_stamps(&mut log, first, step);
    (dir, log)
}

/// Check each explicitly expected offset-to-stamp mapping.
pub fn check_stamps(log: &Log, expected: &[(i64, Option<u64>)]) {
    for &(offset, stamp) in expected {
        assert2::check!(log.stamp_for_offset(Offset(offset)) == stamp);
    }
}

/// Read every aborted interval from the caller's explicit sidecar filename.
#[cfg(test)]
pub fn transaction_entries(dir: &std::path::Path, filename: &str) -> Vec<AbortedTxn> {
    crate::txn_index::TxnIndex::open(dir.join(filename))
        .unwrap()
        .entries()
        .to_vec()
}

/// Read the entire durable stamp sidecar of one segment.
pub fn stamp_entries(dir: &std::path::Path, base: i64) -> Vec<crate::stamp_index::StampEntry> {
    crate::stamp_index::StampIndex::open(dir.join(format!("{base:020}.stampindex")))
        .unwrap()
        .entries()
        .to_vec()
}

/// A config whose one-byte `segment_size` rolls the active segment on every
/// append after the first, so each batch lands in a segment of its own.
pub fn tiny_segments() -> LogConfig {
    LogConfig {
        segment_size: bytes(1),
        ..LogConfig::default()
    }
}

/// A compacted log with one batch per segment.
pub fn compacting_segments() -> LogConfig {
    LogConfig {
        cleanup_policy: CleanupPolicy::Compact,
        ..tiny_segments()
    }
}

pub fn compact_test_log() -> (tempfile::TempDir, Log) {
    configured_test_log(compacting_segments())
}

/// Change the roll and compaction grouping cap without changing other options.
pub fn set_segment_size(log: &mut Log, size: ByteSize) {
    let mut config = log.config_snapshot();
    config.segment_size = size;
    log.set_config(config);
}

/// Append one record per batch, with caller-selected keys and value `v`.
pub fn append_keyed_samples(log: &mut Log, batches: i64, key: impl Fn(i64) -> String) {
    for i in 0..batches {
        let key = key(i);
        log.append(&mut keyed_batch(i, &[(0, key.as_bytes(), b"v")]))
            .unwrap();
    }
}

/// Write the allowed prefix, then report a full disk when no budget remains.
pub fn write_with_budget(file: &File, buf: &[u8], remaining: &mut usize) -> std::io::Result<usize> {
    if *remaining == 0 {
        return Err(std::io::ErrorKind::StorageFull.into());
    }
    let written = (&*file).write(&buf[..buf.len().min(*remaining)])?;
    *remaining -= written;
    Ok(written)
}

/// A log under `dir` opened with [`tiny_segments`].
pub fn rolling_test_log(dir: &std::path::Path) -> Log {
    Log::open(dir, tiny_segments()).unwrap()
}

/// A log configured the way Kafka's `message.timestamp.type=LogAppendTime`
/// configures one, with everything else at its default.
pub fn log_append_time_log() -> (tempfile::TempDir, Log) {
    let dir = tempdir().unwrap();
    let log = Log::open(
        dir.path(),
        LogConfig {
            message_timestamp_type: krabka_protocol::records::TimestampType::LogAppendTime,
            ..LogConfig::default()
        },
    )
    .unwrap();
    (dir, log)
}

pub fn test_batch_at(_off: i64) -> RecordBatch {
    // `Log::append` overwrites `base_offset`; one record per batch.
    crate::test_support::single_record_batch(0, 1_000, Bytes::from("v"))
}

/// Encode a "producer" batch with a producer-chosen `base_offset` and
/// leader epoch. Return both the wire bytes and a `VerbatimBatch`.
pub fn verbatim_from(producer: &RecordBatch, leader_epoch: LeaderEpoch) -> (Bytes, VerbatimBatch) {
    let mut wire = bytes::BytesMut::new();
    producer.encode(&mut wire).unwrap();
    let wire = wire.freeze();
    let vb = VerbatimBatch {
        bytes: wire.clone(),
        last_offset_delta: producer.last_offset_delta,
        max_timestamp: producer.max_timestamp,
        leader_epoch,
        producer_id: ProducerId(producer.producer_id),
        producer_epoch: producer.producer_epoch,
        base_sequence: producer.base_sequence,
        is_transactional: producer.attributes.is_transactional(),
    };
    (wire, vb)
}

// ---- helpers for transactional tests ----

/// A transactional (non-control) batch for the given pid/epoch containing `values`.
pub fn append_transaction(log: &mut Log, producer: (i64, i16), values: &[&str]) {
    log.append(&mut transactional_batch(producer.0, producer.1, values))
        .unwrap();
}

pub fn transactional_batch(pid: i64, epoch: i16, values: &[&str]) -> RecordBatch {
    let last_offset_delta = i32::try_from(values.len()).unwrap() - 1;
    let mut records = Vec::new();
    for (i, v) in values.iter().enumerate() {
        records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    RecordBatch {
        base_offset: 0, // overwritten by Log::append
        last_offset_delta,
        producer_id: pid,
        producer_epoch: epoch,
        attributes: Attributes::default().with_transactional(true),
        records,
        ..RecordBatch::default()
    }
}

krabka_macros::control_marker_fixture!(control_marker);

/// A commit control batch (`marker_type=1`) for the given pid and epoch.
/// `Log::append` rewrites the offsets.
pub fn commit_marker(pid: i64, epoch: i16) -> RecordBatch {
    transaction_marker(pid, epoch, 1 /* COMMIT */)
}

/// An abort control batch (`marker_type=0`) for the given pid and epoch.
/// `Log::append` rewrites the offsets.
pub fn abort_marker(pid: i64, epoch: i16) -> RecordBatch {
    transaction_marker(pid, epoch, 0 /* ABORT */)
}

fn transaction_marker(pid: i64, epoch: i16, marker_type: i16) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: pid,
        producer_epoch: epoch,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(control_key(marker_type)),
            value: Some(control_value(17)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

#[derive(Clone, Copy)]
pub struct BarrierEpoch(pub i64);

#[derive(Clone, Copy)]
pub struct ProducerEpoch(pub i16);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct BarrierMarkerSetup<'a> {
    #[default("nightly")]
    pub group: &'a str,
    #[default(BarrierEpoch(1))]
    pub epoch: BarrierEpoch,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct HostileBarrierSetup<'a> {
    pub marker: BarrierMarkerSetup<'a>,
    #[default(ProducerId(1000))]
    pub producer: ProducerId,
    #[default(ProducerEpoch(2))]
    pub producer_epoch: ProducerEpoch,
}

/// Build a barrier-marker value: `(version=0: i16, group: string,
/// epoch: i64, triggered_at: i64)` big-endian, where a string is an `i16`
/// byte length and then UTF-8 bytes.
pub fn barrier_value(group: &str, epoch: BarrierEpoch) -> Bytes {
    let mut buf = Vec::new();
    buf.extend_from_slice(&0i16.to_be_bytes());
    buf.extend_from_slice(&i16::try_from(group.len()).unwrap().to_be_bytes());
    buf.extend_from_slice(group.as_bytes());
    buf.extend_from_slice(&epoch.0.to_be_bytes());
    buf.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
    Bytes::from(buf)
}

/// A barrier control batch, control type [`BARRIER_CONTROL_TYPE`].
///
/// `RecordBatch::default` already carries the marker's identity: a
/// `producer_id` of -1, a `producer_epoch` of -1, and a `base_sequence`
/// of -1. The attributes set the control bit and leave the transactional
/// bit clear. `Log::append` rewrites the offsets.
pub fn barrier_marker(setup: BarrierMarkerSetup<'_>) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        attributes: Attributes::default().with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(control_key(BARRIER_CONTROL_TYPE)),
            value: Some(barrier_value(setup.group, setup.epoch)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// A barrier control batch that carries `producer_id` and
/// `producer_epoch`. The wire format sets both to -1, so this shape is
/// hostile input. It exists to state that the log decides by
/// control-record type alone.
pub fn barrier_marker_from_producer(setup: HostileBarrierSetup<'_>) -> RecordBatch {
    RecordBatch {
        producer_id: setup.producer.0,
        producer_epoch: setup.producer_epoch.0,
        ..barrier_marker(setup.marker)
    }
}

/// Every piece of per-partition state that a barrier marker leaves alone,
/// in one comparable value.
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionState {
    pub lso: Offset,
    pub transactions: Vec<(i64, (i32, Option<Offset>))>,
    pub aborted: Vec<AbortedTxn>,
    pub producers: Vec<ProducerSnapshotEntry>,
}

/// The transaction state the log keeps for `producer_id` beside its producer
/// entry: the coordinator epoch of its last durable end marker, `-1` before
/// the first one, and the first offset of its open transaction.
pub fn transaction_fields(log: &Log, producer_id: ProducerId) -> (i32, Option<Offset>) {
    (
        log.transaction_marker_state(producer_id).1,
        log.pending_transaction_start(producer_id),
    )
}

/// Collect the [`PartitionState`] of `log`, with one transaction entry per
/// id in `producer_ids` and the producer entries in id order.
pub fn partition_state(log: &Log, producer_ids: &[i64]) -> PartitionState {
    let mut producers = log.producer_state_snapshot();
    producers.sort_by_key(|entry| entry.producer_id.get());
    PartitionState {
        lso: log.lso(),
        transactions: producer_ids
            .iter()
            .map(|pid| (*pid, transaction_fields(log, ProducerId(*pid))))
            .collect(),
        aborted: log.aborted_in_range(Offset(0), Offset(i64::MAX)),
        producers,
    }
}

pub fn sample_batch_with_epoch(n: i32, epoch: i32) -> RecordBatch {
    let mut b = sample_batch(n);
    b.partition_leader_epoch = epoch;
    b
}

pub fn keyed_batch(base: i64, items: &[(i32, &[u8], &[u8])]) -> RecordBatch {
    let records: Vec<Record> = items
        .iter()
        .map(|(d, k, v)| Record {
            offset_delta: *d,
            key: Some(Bytes::copy_from_slice(k)),
            value: Some(Bytes::copy_from_slice(v)),
            ..Default::default()
        })
        .collect();
    let last_delta = items.iter().map(|(d, _, _)| *d).max().unwrap_or(0);
    RecordBatch {
        base_offset: base,
        last_offset_delta: last_delta,
        max_timestamp: 0,
        records,
        ..RecordBatch::default()
    }
}

/// A `CompactionContext` with a fixed, deterministic epoch and a last stable
/// offset past every offset a log can hold. The in-crate compaction tests use
/// it where tombstone age, marker age and the watermark bound are not under
/// test.
pub fn compaction_ctx() -> CompactionContext {
    CompactionContext {
        now: SystemTime::UNIX_EPOCH,
        last_stable_offset: krabka_ids::Offset(i64::MAX),
    }
}

/// Build a log rolled into several sealed segments under `dir`. This
/// mirrors the `remote_log_manager` test helper and stays local to this
/// module.
pub fn rolled_log(dir: &std::path::Path, extra: &LogConfig) -> Log {
    let mut log = Log::open(
        dir,
        LogConfig {
            segment_size: bytes(200),
            ..extra.clone()
        },
    )
    .unwrap();
    for _ in 0..16 {
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
    }
    log.sync().unwrap();
    log
}

pub fn ts_batch(ts: i64) -> RecordBatch {
    let mut b = RecordBatch {
        base_offset: 0, // overwritten by Log::append
        base_timestamp: ts,
        max_timestamp: ts,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    b.records.push(Record {
        offset_delta: 0,
        timestamp_delta: 0,
        value: Some(Bytes::from("v")),
        ..Default::default()
    });
    b
}

/// Append through the same leader/follower paths as the broker.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AppendPath {
    Leader,
    Follower,
    Verbatim,
}

pub(crate) fn append_path(log: &mut Log, path: AppendPath, mut batch: RecordBatch) {
    let log_end = log.log_end_offset();
    match path {
        AppendPath::Leader => {
            log.append(&mut batch).unwrap();
        }
        AppendPath::Verbatim if !batch.attributes.is_control_batch() => {
            batch.base_offset = log_end.0;
            let (_, verbatim) = verbatim_from(&batch, LeaderEpoch(0));
            log.append_verbatim_at(&verbatim, log_end).unwrap();
        }
        AppendPath::Follower | AppendPath::Verbatim => log.append_at(&mut batch, log_end).unwrap(),
    }
}

pub fn snapshot_offsets(dir: &std::path::Path) -> Vec<Offset> {
    crate::producer_snapshot::list(dir)
        .unwrap()
        .into_iter()
        .map(|(offset, _)| offset)
        .collect()
}

pub fn check_contiguous_exports(exports: &[super::SegmentExport]) {
    for pair in exports.windows(2) {
        // Each sealed segment ends exactly one offset before its successor.
        assert2::assert!(pair[0].last_offset + 1 == pair[1].base_offset);
    }
}

/// Several sealed segments and an active segment, with identical batch timestamps.
pub(crate) fn rolled_sample_log(dir: &std::path::Path) -> Log {
    let mut log = crate::test_support::segmented_log(dir, krabka_units::kibibytes(1));
    append_samples(&mut log, 40, 4);
    assert2::check!(
        !log.segments.is_empty(),
        "the appends should have rolled a segment"
    );
    log
}
