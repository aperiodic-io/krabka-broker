//! Shared log, record and directory fixtures for storage tests.

use std::{collections::BTreeMap, ops::RangeInclusive, path::Path};

use bytes::Bytes;
use krabka_protocol::records::{Record, RecordBatch};

use crate::{Log, LogConfig, Offset, ProducerId};

pub(crate) fn open_log(dir: &Path) -> Log {
    Log::open(dir, LogConfig::default()).unwrap()
}

pub(crate) fn segmented_log(dir: &Path, segment_size: krabka_units::prelude::ByteSize) -> Log {
    Log::open(
        dir,
        LogConfig {
            segment_size,
            ..LogConfig::default()
        },
    )
    .unwrap()
}

/// Every file of a directory, by name, with its contents.
pub(crate) type Files = BTreeMap<String, Vec<u8>>;

pub(crate) fn directory_files(dir: &Path) -> Files {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().into_string().unwrap(),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

pub(crate) fn log_file_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().and_then(|ext| ext.to_str()) == Some("log"))
        .count()
}

pub(crate) fn single_record_batch(base_offset: i64, timestamp: i64, value: Bytes) -> RecordBatch {
    RecordBatch {
        base_offset,
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        last_offset_delta: 0,
        records: vec![Record {
            value: Some(value),
            ..Default::default()
        }],
        ..RecordBatch::default()
    }
}

/// One explicit inclusive stamp range, shared by the index and log scenarios.
pub(crate) fn stamp_entry(base: i64, last: i64, stamp: u64) -> crate::stamp_index::StampEntry {
    crate::stamp_index::StampEntry {
        base_offset: crate::Offset(base),
        last_offset: crate::Offset(last),
        stamp,
    }
}

/// The ordinary four-record aborted transaction; fields remain overrideable for malformed fixtures.
#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct AbortedTxnSetup {
    #[default(ProducerId(1000))]
    pub producer: ProducerId,
    #[default(Offset(0)..=Offset(3))]
    pub bounds: RangeInclusive<Offset>,
    #[default(Offset(4))]
    pub stable: Offset,
}

pub(crate) fn aborted_txn(setup: AbortedTxnSetup) -> crate::txn_index::AbortedTxn {
    let AbortedTxnSetup {
        producer,
        bounds,
        stable,
    } = setup;
    let (start_offset, last_offset) = bounds.into_inner();
    crate::txn_index::AbortedTxn {
        start_offset,
        last_offset,
        producer_id: producer,
        last_stable_offset: stable,
    }
}

pub(crate) fn numbered_record(offset_delta: i32, timestamp_delta: i64) -> Record {
    Record {
        offset_delta,
        timestamp_delta,
        key: Some(Bytes::from(format!("k{offset_delta}"))),
        value: Some(Bytes::from(format!("v{offset_delta}"))),
        ..Default::default()
    }
}

/// A [`crate::io::LogIo`] that records every readahead hint a read gives,
/// as `(offset, len)`, and does real I/O for everything else.
#[derive(Debug, Default)]
pub(crate) struct RecordedAdvice(std::sync::Mutex<Vec<(u64, u64)>>);

impl RecordedAdvice {
    /// The hints given since the last call, oldest first.
    pub(crate) fn take(&self) -> Vec<(u64, u64)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl crate::io::LogIo for RecordedAdvice {
    fn advise_will_need(&self, _file: &std::fs::File, offset: u64, len: u64) {
        self.0.lock().unwrap().push((offset, len));
    }
}
