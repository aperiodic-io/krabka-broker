//! Opening a sealed segment through the public API.

use std::{fs, io::Write};

use assert2::assert;
use bytes::Bytes;
use krabka_log::{LogError, Offset, Segment, name};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::prelude::kibibytes;
use tempfile::tempdir;

fn batch(offset: i64, timestamp: i64) -> RecordBatch {
    RecordBatch {
        base_offset: offset,
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        records: vec![Record {
            value: Some(Bytes::from_static(b"value")),
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}

#[test]
fn opened_segments_restore_frontiers_and_refuse_appends() {
    let dir = tempdir().unwrap();
    let expected = vec![batch(10, 500), batch(11, 100)];
    let mut writer = Segment::create(dir.path(), Offset(10)).unwrap();
    for batch in &expected {
        writer.append(batch, kibibytes(4)).unwrap();
    }
    writer.seal().unwrap();
    drop(writer);

    let mut reader = Segment::open(dir.path(), Offset(10)).unwrap();
    assert!(reader.is_sealed());
    assert!(reader.last_offset() == Offset(11));
    assert!(reader.max_timestamp() == 500);
    assert!(reader.read(Offset(10), kibibytes(4)).unwrap() == expected);
    let raw = reader
        .read_raw(Offset(10), Offset(12), kibibytes(4))
        .unwrap();
    assert!(!raw.is_empty());
    assert!((raw.start_offset, raw.last_offset) == (Offset(10), Offset(11)));

    let before = fs::read(name::log_path(dir.path(), 10)).unwrap();
    assert!(matches!(
        reader.append(&batch(12, 600), kibibytes(4)),
        Err(LogError::Io(_))
    ));
    assert!(fs::read(name::log_path(dir.path(), 10)).unwrap() == before);
}

#[test]
fn a_sealed_open_refuses_a_torn_tail_without_truncating_it() {
    let dir = tempdir().unwrap();
    let mut writer = Segment::create(dir.path(), Offset(0)).unwrap();
    writer.append(&batch(0, 500), kibibytes(4)).unwrap();
    writer.seal().unwrap();
    drop(writer);
    let path = name::log_path(dir.path(), 0);
    fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let before = fs::read(&path).unwrap();

    assert!(matches!(
        Segment::open(dir.path(), Offset(0)),
        Err(LogError::Corrupt(_))
    ));
    assert!(fs::read(&path).unwrap() == before);
}

#[test]
fn an_empty_segment_opens_sealed_at_its_base() {
    let dir = tempdir().unwrap();
    drop(Segment::create(dir.path(), Offset(10)).unwrap());
    let reader = Segment::open(dir.path(), Offset(10)).unwrap();
    assert!(reader.is_sealed());
    assert!(reader.last_offset() == Offset(9));
    assert!(reader.read(Offset(10), kibibytes(4)).unwrap().is_empty());
}
