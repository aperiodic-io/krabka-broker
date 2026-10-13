//! Tests for the records a `__consumer_offsets` replay skips and the records
//! that fail it, which follow Kafka's `CoordinatorLoaderImpl`.
//!
//! The loader skips a record whose type `GroupCoordinatorRecordSerde` does not
//! know, value or tombstone, and fails on every other record that does not
//! deserialize: a missing or short key, a known type with an unsupported value
//! version, and a value that does not decode. It deserializes a transactional
//! record when it reads it, before any marker, so a bad value fails the load
//! even inside a transaction that aborts.

use {
    super::{
        replay::{PendingTxnKeys, replay_records},
        test_support::bare_coordinator,
    },
    crate::{
        coordinator::{persistence::OffsetCommitValue, unified::classic_state::OffsetEntry},
        txn::marker::{MarkerType, build_marker_batch},
    },
    assert2::check,
    bytes::Bytes,
    krabka_log::{Offset, ProducerId},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    std::collections::{HashMap, HashSet},
    tempfile::tempdir,
};

/// What a replay leaves behind, in the parts these cases can change.
#[derive(Debug, PartialEq, Eq)]
struct ReplayView {
    committed: HashMap<String, HashMap<(String, i32), OffsetEntry>>,
    classic: HashSet<String>,
    pending_txn: HashMap<String, HashMap<i64, PendingTxnKeys>>,
}

/// How the odd record in the middle of the log is written.
#[derive(Debug, Clone, Copy)]
enum Framing {
    /// In a plain batch of its own.
    Plain,
    /// In a transactional batch of producer 7 that a marker of `MarkerType`
    /// then ends.
    Transaction(MarkerType),
}

const PRODUCER: i64 = 7;

fn offset_value(offset: i64) -> Bytes {
    OffsetCommitValue {
        offset: Offset(offset),
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
        topic_id: None,
    }
    .encode_value()
}

fn commit(partition: i32, offset: i64) -> Record {
    Record {
        key: Some(OffsetCommitValue::encode_key("g", "t", partition).unwrap()),
        value: Some(offset_value(offset)),
        ..Default::default()
    }
}

/// A key of record type `record_type` followed by a well-formed group id, so
/// only the type can make it unknown.
fn typed_key(record_type: i16) -> Bytes {
    let mut key = record_type.to_be_bytes().to_vec();
    key.extend_from_slice(&1_i16.to_be_bytes());
    key.push(b'g');
    Bytes::from(key)
}

fn entry(offset: i64) -> OffsetEntry {
    OffsetEntry::from(OffsetCommitValue::decode_value(&offset_value(offset)).unwrap())
}

/// The state of a replay that skipped the odd record: the commits on either
/// side of it, and nothing else.
fn both_commits() -> ReplayView {
    ReplayView {
        committed: HashMap::from([(
            "g".to_owned(),
            HashMap::from([
                (("t".to_owned(), 0), entry(10)),
                (("t".to_owned(), 1), entry(11)),
            ]),
        )]),
        classic: HashSet::new(),
        pending_txn: HashMap::new(),
    }
}

/// Replays a log of a commit, then `odd` framed as `framing`, then another
/// commit, and returns what the replay left, or `None` when it failed.
fn replay_around(odd: Record, framing: Framing) -> Option<ReplayView> {
    let coordinator = bare_coordinator();
    let dir = tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    log.append(&mut RecordBatch {
        records: vec![commit(0, 10)],
        ..RecordBatch::default()
    })
    .unwrap();
    match framing {
        Framing::Plain => {
            log.append(&mut RecordBatch {
                records: vec![odd],
                ..RecordBatch::default()
            })
            .unwrap();
        }
        Framing::Transaction(marker) => {
            log.append(&mut RecordBatch {
                producer_id: PRODUCER,
                producer_epoch: 0,
                attributes: Attributes::default().with_transactional(true),
                records: vec![odd],
                ..RecordBatch::default()
            })
            .unwrap();
            log.append(&mut build_marker_batch(
                ProducerId(PRODUCER),
                0,
                Offset(0),
                marker,
                0,
            ))
            .unwrap();
        }
    }
    log.append(&mut RecordBatch {
        records: vec![commit(1, 11)],
        ..RecordBatch::default()
    })
    .unwrap();
    replay_records(&log, &coordinator)
        .ok()
        .map(|replayed| ReplayView {
            committed: replayed.committed,
            classic: replayed.classic.into_keys().collect(),
            pending_txn: replayed.pending_txn,
        })
}

#[test]
fn replay_skips_unknown_record_types_and_fails_on_bad_records() {
    let record = |key: Option<Bytes>, value: Option<Bytes>| Record {
        key,
        value,
        ..Default::default()
    };
    let known_key = || Some(OffsetCommitValue::encode_key("g", "t", 2).unwrap());
    let mut unsupported_version = offset_value(12).to_vec();
    unsupported_version[..2].copy_from_slice(&99_i16.to_be_bytes());
    let unsupported_version = Bytes::from(unsupported_version);
    let truncated = offset_value(12).slice(..6);

    let cases: Vec<(&str, Record, Framing, Option<ReplayView>)> = vec![
        (
            "unknown type 9 with a value",
            record(Some(typed_key(9)), Some(offset_value(12))),
            Framing::Plain,
            Some(both_commits()),
        ),
        (
            "type 18, which Kafka 4.3.1 no longer defines",
            record(Some(typed_key(18)), Some(offset_value(12))),
            Framing::Plain,
            Some(both_commits()),
        ),
        (
            "unknown type 24 with an undecodable value",
            record(Some(typed_key(24)), Some(Bytes::from_static(b"\xff"))),
            Framing::Plain,
            Some(both_commits()),
        ),
        (
            "unknown type -1 with a key body that does not decode",
            record(Some(Bytes::from_static(b"\xff\xff\x7f")), None),
            Framing::Plain,
            Some(both_commits()),
        ),
        (
            "unknown type 9 as a tombstone",
            record(Some(typed_key(9)), None),
            Framing::Plain,
            Some(both_commits()),
        ),
        (
            "unknown type in a committed transaction",
            record(Some(typed_key(9)), Some(offset_value(12))),
            Framing::Transaction(MarkerType::Commit),
            Some(both_commits()),
        ),
        (
            "known type with an unsupported value version",
            record(known_key(), Some(unsupported_version.clone())),
            Framing::Plain,
            None,
        ),
        (
            "known type with a corrupt value",
            record(known_key(), Some(truncated.clone())),
            Framing::Plain,
            None,
        ),
        (
            "unsupported value version in an aborted transaction",
            record(known_key(), Some(unsupported_version)),
            Framing::Transaction(MarkerType::Abort),
            None,
        ),
        (
            "corrupt value in an aborted transaction",
            record(known_key(), Some(truncated)),
            Framing::Transaction(MarkerType::Abort),
            None,
        ),
        (
            "known type whose key does not decode",
            record(Some(Bytes::from_static(b"\x00\x01\x00\x05g")), None),
            Framing::Plain,
            None,
        ),
        (
            "key shorter than its record type",
            record(Some(Bytes::from_static(b"\x00")), Some(offset_value(12))),
            Framing::Plain,
            None,
        ),
        (
            "record without a key",
            record(None, Some(offset_value(12))),
            Framing::Plain,
            None,
        ),
    ];

    for (name, odd, framing, expected) in cases {
        check!(replay_around(odd, framing) == expected, "{name}");
    }
}
