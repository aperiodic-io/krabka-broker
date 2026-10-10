//! The records and batches the scenarios archive.
//!
//! Every builder here exists because some bound flag selects on the field it
//! sets: a key, a header, a timestamp, a producer id, or the transactional and
//! control attribute bits. They are written once here so each scenario names
//! only the shape it is about.

use bytes::Bytes;
use krabka_protocol::records::{Attributes, Record, RecordBatch, RecordHeader};

/// Base timestamp every fixture batch's `base_timestamp` starts from, so a
/// record's absolute timestamp is `BASE_TIMESTAMP + timestamp_delta`.
pub(crate) const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

pub(crate) fn value_record(offset_delta: i32, value: &str) -> Record {
    Record {
        offset_delta,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        ..Record::default()
    }
}

pub(crate) fn keyed_record(offset_delta: i32, key: &str, value: &str) -> Record {
    Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        ..value_record(offset_delta, value)
    }
}

pub(crate) fn timestamped_record(offset_delta: i32, timestamp_delta: i64, value: &str) -> Record {
    Record {
        timestamp_delta,
        ..value_record(offset_delta, value)
    }
}

pub(crate) fn headered_record(
    offset_delta: i32,
    value: &str,
    header_name: &str,
    header_value: &str,
) -> Record {
    Record {
        headers: vec![RecordHeader {
            key: header_name.to_owned(),
            value: Some(Bytes::copy_from_slice(header_value.as_bytes())),
        }],
        ..value_record(offset_delta, value)
    }
}

/// A batch from `producer_id`, with `base_offset` overwritten by
/// `Log::append`. `last_offset_delta` and `max_timestamp` are derived from
/// `records`, matching the convention `bound.rs`'s and `materialize.rs`'s
/// own unit tests already use.
pub(crate) fn producer_batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    let last_offset_delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
    let max_delta = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
    let (producer_epoch, base_sequence) = if producer_id >= 0 { (0, 0) } else { (-1, -1) };
    RecordBatch {
        last_offset_delta,
        base_timestamp: BASE_TIMESTAMP,
        max_timestamp: BASE_TIMESTAMP + max_delta,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
        ..RecordBatch::default()
    }
}

/// A batch with no producer id (the ordinary, non-idempotent case).
pub(crate) fn plain_batch(records: Vec<Record>) -> RecordBatch {
    producer_batch(-1, records)
}

/// A transactional (non-control) batch for `producer_id`.
pub(crate) fn transactional_batch(producer_id: i64, records: Vec<Record>) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..producer_batch(producer_id, records)
    }
}

krabka_macros::control_marker_fixture!(control_marker);

/// A COMMIT control batch (`marker_type=1`) for `producer_id`, matching the
/// shape `crates/log/src/log.rs`'s own `commit_marker` test helper builds.
pub(crate) fn commit_marker(producer_id: i64) -> RecordBatch {
    RecordBatch {
        producer_epoch: 0,
        base_sequence: -1,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            key: Some(control_key(1 /* COMMIT */)),
            value: Some(control_value(0)),
            ..Record::default()
        }],
        ..producer_batch(producer_id, vec![])
    }
}
