//! Fixtures the compaction unit tests share: the record and sealed-segment
//! builders, and the two configuration constants every rewrite test passes
//! through.

use std::{collections::HashMap, path::Path};

use bytes::Bytes;
use krabka_ids::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use krabka_units::prelude::{ByteSize, Time};

use super::{
    CleanedTransactionMetadata, CleaningRound, ProducerLastRecord, RewriteOutput, RewriteRetention,
    build_offset_map, rewrite_segments,
};
use crate::segment::Segment;

/// Kafka's default `index.interval.bytes`. The compaction tests do not
/// exercise sparse-index density, so they all pass the default value
/// through.
pub(super) const INDEX_INTERVAL: ByteSize = krabka_units::kibibytes(4);

/// The `delete.retention.ms` the rewrite tests share.
pub(super) const RETENTION: Time = krabka_units::secs(1);

pub(super) fn make_record(offset_delta: i32, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
    Record {
        offset_delta,
        key: key.map(Bytes::copy_from_slice),
        value: value.map(Bytes::copy_from_slice),
        ..Default::default()
    }
}

pub(super) fn write_sealed_segment(dir: &Path, base_offset: i64, records: Vec<Record>) -> Segment {
    let mut seg = Segment::create(dir, Offset(base_offset)).unwrap();
    let n = i32::try_from(records.len()).expect("record count fits i32");
    let max_ts = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
    let batch = RecordBatch {
        base_offset,
        last_offset_delta: n - 1,
        max_timestamp: max_ts,
        records,
        attributes: Attributes::default(),
        ..RecordBatch::default()
    };
    seg.append(&batch, INDEX_INTERVAL).unwrap();
    seg.seal().unwrap();
    seg
}

/// Write a sealed segment that holds the given batches verbatim, with
/// `base_offset`, attributes, and `producer_id` preserved. Tests use it to
/// build control batches and mixed data and control layouts.
pub(super) fn write_sealed_batches(dir: &Path, batches: &[RecordBatch]) -> Segment {
    let base = batches.first().map_or(0, |b| b.base_offset);
    let mut seg = Segment::create(dir, Offset(base)).unwrap();
    for batch in batches {
        seg.append(batch, INDEX_INTERVAL).unwrap();
    }
    seg.seal().unwrap();
    seg
}

/// A control batch that carries a single commit or abort marker record.
/// The marker key is `(version: i16, marker_type: i16)` big-endian.
pub(super) fn control_batch(base_offset: i64, producer_id: i64, marker_type: i16) -> RecordBatch {
    let mut key = [0u8; 4];
    key[2..4].copy_from_slice(&marker_type.to_be_bytes());
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        producer_id,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            offset_delta: 0,
            key: Some(Bytes::copy_from_slice(&key)),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// The cleaning round a test rewrites `segments` in, over `active_producers`.
/// The round ends where the last segment does, the way a pass that rewrites
/// every sealed segment ends at the active segment's base.
pub(super) fn round_over<'a>(
    segments: &[&Segment],
    active_producers: &'a HashMap<ProducerId, ProducerLastRecord>,
) -> CleaningRound<'a> {
    CleaningRound {
        active_producers,
        upper_bound: segments
            .last()
            .expect("a round has a segment")
            .last_offset()
            + 1,
        max_decompressed_record: None,
    }
}

/// Input records in which the final k1 value supersedes its first value.
pub(super) fn superseded_records() -> Vec<Record> {
    vec![
        make_record(0, Some(b"k1"), Some(b"v1")),
        make_record(1, Some(b"k2"), Some(b"v2")),
        make_record(2, Some(b"k1"), Some(b"v3")),
    ]
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TransactionalRecordSetup<'a> {
    pub offset: Offset,
    #[default(ProducerId(1000))]
    pub producer: ProducerId,
    #[default((b"k", b"v"))]
    pub payload: (&'a [u8], &'a [u8]),
}

/// One transactional keyed record, retaining the protocol's epoch/sequence defaults.
pub(super) fn transactional_record(setup: TransactionalRecordSetup<'_>) -> RecordBatch {
    let TransactionalRecordSetup {
        offset,
        producer,
        payload: (key, value),
    } = setup;
    RecordBatch {
        base_offset: offset.0,
        last_offset_delta: 0,
        producer_id: producer.0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![make_record(0, Some(key), Some(value))],
        ..RecordBatch::default()
    }
}

/// Rewrite a fixture using its explicit map and cleaning-round state.
pub(super) fn rewrite_with_map(
    dir: &Path,
    segments: &[&Segment],
    map: &HashMap<Bytes, Offset>,
    txn: &mut CleanedTransactionMetadata,
    retention: RewriteRetention,
    round: CleaningRound<'_>,
) -> RewriteOutput {
    rewrite_segments(
        &crate::io::FileIo,
        dir,
        segments,
        map,
        txn,
        retention,
        round,
    )
    .unwrap()
}

/// Build the map and rewrite every supplied segment as one complete round.
pub(super) fn rewrite_all(
    dir: &Path,
    segments: &[&Segment],
    txn: &mut CleanedTransactionMetadata,
    retention: RewriteRetention,
    active: &HashMap<ProducerId, ProducerLastRecord>,
) -> RewriteOutput {
    let map = build_offset_map(segments, vec![], None).unwrap();
    rewrite_with_map(
        dir,
        segments,
        &map,
        txn,
        retention,
        round_over(segments, active),
    )
}
