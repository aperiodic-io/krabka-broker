//! The `LogConfig` and the record batches every fixture partition appends.
//!
//! One appended batch per sealed segment is what makes the archive
//! multi-segment, so the tiny `segment_size` that forces the roll sits here
//! beside the batch builders whose size it depends on.

use bytes::Bytes;
use krabka_log::LogConfig;
use krabka_protocol::records::{Attributes, Record, RecordBatch};

/// A `LogConfig` whose `segment_size` is shrunk to a little over one byte, so
/// a second `append` always rolls the batch that was active before it into
/// its own sealed segment.
///
/// `krabka-restore` depends on `krabka-log` but not on `krabka-units`
/// directly, so this crate has no path to name `krabka_units::ByteSize` or
/// call `krabka_units::prelude::bytes`. Scaling the existing 1 GiB default
/// down through `Div<f64>` -- implemented on every `uom` quantity
/// `krabka-units` wraps, and reachable here purely through operator syntax
/// without naming the type -- gets the same tiny segment size the sibling
/// `jvm_tiered_storage.rs` fixture builds with `bytes(1)`.
pub(crate) fn tiny_segment_config() -> LogConfig {
    let default = LogConfig::default();
    LogConfig {
        segment_size: default.segment_size / 1_073_741_824.0,
        ..default
    }
}

/// One record with a distinguishable value, at `offset_delta` within its
/// batch.
fn record(offset_delta: i32, value: &str) -> Record {
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta),
        offset_delta,
        key: None,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Vec::new(),
    }
}

/// A batch holding one record per `values` entry, in order. `base_offset` is
/// a placeholder: `Log::append` overwrites it with the log's real end offset
/// when the batch is appended.
pub(crate) fn text_batch(values: &[&str]) -> RecordBatch {
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            record(
                i32::try_from(index).expect("fixture batches stay far below i32::MAX"),
                value,
            )
        })
        .collect();
    let last_offset_delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes: Attributes::default(),
        last_offset_delta,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000 + i64::from(last_offset_delta),
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

/// Append and sync the fixture groups, retaining their assigned offsets and sealed exports.
pub(crate) fn append_groups(
    log: &mut krabka_log::Log,
    groups: &[&[&str]],
) -> (Vec<RecordBatch>, Vec<krabka_log::SegmentExport>) {
    let mut appended = Vec::with_capacity(groups.len());
    for values in groups {
        let mut batch = text_batch(values);
        log.append(&mut batch).expect("append batch");
        appended.push(batch);
    }
    log.sync().unwrap();
    (appended, log.tierable_segments())
}
