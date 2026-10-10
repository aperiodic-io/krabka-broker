//! Unit tests for segment verification, driven end to end through
//! [`verify_segment`] against a local archive: a clean segment and the facts it
//! yields, every torn copy in the order the artifacts are written, and one
//! corruption per artifact -- a flipped CRC-covered byte, a truncated log, an
//! index entry past the log end, a non-monotonic time index, a bad snapshot
//! checksum, and a malformed leader-epoch checkpoint.

use assert2::check;
use bytes::{BufMut, BytesMut};
use clap::Parser as _;
use crc32c::crc32c;
use krabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;

use super::*;

fn test_partition() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(0xA5CD), "orders", 0)
}

fn archive_at(dir: &std::path::Path) -> ArchiveStore {
    let cli = crate::Cli::parse_from([
        "krabka-restore",
        "--log-dir",
        "/target",
        "--archive-local",
        &dir.display().to_string(),
    ]);
    crate::open_archive(&cli.args).expect("archive store")
}

/// A local archive and the partition identity shared by verification cases.
fn verification_context() -> (TempDir, ArchiveStore, TopicIdPartition) {
    let dir = TempDir::new().expect("tempdir");
    let store = archive_at(dir.path());
    let partition = test_partition();
    (dir, store, partition)
}

fn write_object(root: &std::path::Path, relative: &str, bytes: &[u8]) -> ArchiveObject {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("mkdir");
    }
    std::fs::write(&path, bytes).expect("write object");
    ArchiveObject {
        key: Path::from(relative),
        size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
    }
}

#[tokio::test]
async fn authenticated_size_mismatch_is_rejected_from_head_metadata() {
    let dir = TempDir::new().expect("tempdir");
    let store = archive_at(dir.path());
    let object = write_object(dir.path(), "wal.ckwl", b"oversized");
    let authenticated = BTreeMap::from([(
        object.key.to_string(),
        ObjectEntry {
            suffix: ".ckwl".into(),
            key: object.key.to_string(),
            size_bytes: 1,
            sha256: Sha256Digest::of(b"x"),
            e_tag: None,
            version_id: None,
            create_precondition: false,
        },
    )]);

    let error = fetch_capped(
        store.ops(),
        &object.key,
        MAX_LOG_BYTES,
        Some(&authenticated),
    )
    .await
    .expect_err("signed size mismatch");
    check!(matches!(error, RestoreError::Authenticity { .. }));
}

fn record(offset_delta: i32, timestamp_delta: i64) -> Record {
    Record {
        offset_delta,
        timestamp_delta,
        value: Some(Bytes::from_static(b"value")),
        ..Default::default()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::From, derive_more::Into,
)]
struct RecordTimestamp(i64);

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::From, derive_more::Into,
)]
struct RecordCount(i32);

#[derive(Clone, Copy)]
struct BatchSetup {
    base_offset: krabka_ids::Offset,
    base_timestamp: RecordTimestamp,
    max_timestamp: RecordTimestamp,
    record_count: RecordCount,
}

impl Default for BatchSetup {
    fn default() -> Self {
        Self {
            base_offset: krabka_ids::Offset(100),
            base_timestamp: RecordTimestamp(1_000),
            max_timestamp: RecordTimestamp(1_020),
            record_count: RecordCount(3),
        }
    }
}

fn batch(setup: BatchSetup) -> RecordBatch {
    let BatchSetup {
        base_offset,
        base_timestamp,
        max_timestamp,
        record_count,
    } = setup;
    RecordBatch {
        base_offset: base_offset.0,
        last_offset_delta: record_count.0 - 1,
        base_timestamp: base_timestamp.0,
        max_timestamp: max_timestamp.0,
        records: (0..record_count.0)
            .map(|i| record(i, i64::from(i) * 10))
            .collect(),
        ..RecordBatch::default()
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct SingleRecordSetup {
    #[default(krabka_ids::Offset(100))]
    base_offset: krabka_ids::Offset,
    #[default(RecordTimestamp(1_000))]
    timestamp: RecordTimestamp,
}

/// A framed one-record batch for segment-boundary verification cases.
fn single_record_bytes(setup: SingleRecordSetup) -> Bytes {
    encode_all(&[batch(BatchSetup {
        base_offset: setup.base_offset,
        base_timestamp: setup.timestamp,
        max_timestamp: setup.timestamp,
        record_count: RecordCount(1),
    })])
}

fn encode_all(batches: &[RecordBatch]) -> Bytes {
    let mut buf = BytesMut::new();
    for b in batches {
        b.encode(&mut buf).expect("encode batch");
    }
    buf.freeze()
}

fn offset_index_bytes(entries: &[(u32, u32)]) -> Bytes {
    let mut buf = BytesMut::new();
    for &(rel, pos) in entries {
        buf.put_u32(rel);
        buf.put_u32(pos);
    }
    buf.freeze()
}

fn time_index_bytes(entries: &[(i64, u32)]) -> Bytes {
    let mut buf = BytesMut::new();
    for &(ts, rel) in entries {
        buf.put_i64(ts);
        buf.put_u32(rel);
    }
    buf.freeze()
}

/// `(start_offset, last_offset, producer_id)` of one `.txnindex` entry.
type TxnEntry = (i64, i64, i64);

fn txn_index_bytes(entries: &[TxnEntry]) -> Bytes {
    let mut buf = BytesMut::new();
    for &(start, last, producer_id) in entries {
        buf.put_i16(0); // version
        buf.put_i64(producer_id);
        buf.put_i64(start);
        buf.put_i64(last);
        buf.put_i64(last + 1); // last_stable_offset
    }
    buf.freeze()
}

type SnapshotEntry = (i64, i16, i32, i64, i32, i64, i32, i64);

/// Kafka's v1 producer-state `.snapshot` layout, written independently of
/// `krabka_log`'s encoder: version, CRC32C over everything after the CRC
/// field, entry count, then 46-byte entries.
fn snapshot_bytes(entries: &[SnapshotEntry]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    buf.put_u32(0); // CRC placeholder, patched below.
    buf.put_i32(i32::try_from(entries.len()).unwrap());
    for &(producer_id, epoch, sequence, last, delta, timestamp, coordinator, txn) in entries {
        buf.put_i64(producer_id);
        buf.put_i16(epoch);
        buf.put_i32(sequence);
        buf.put_i64(last);
        buf.put_i32(delta);
        buf.put_i64(timestamp);
        buf.put_i32(coordinator);
        buf.put_i64(txn);
    }
    check!(buf.len() == 10 + entries.len() * 46);
    let crc = crc32c(&buf[6..]);
    buf[2..6].copy_from_slice(&crc.to_be_bytes());
    buf.freeze()
}

fn build_checkpoint(entries: &[(i32, i64)]) -> Bytes {
    use std::fmt::Write as _;

    let mut s = format!("0\n{}\n", entries.len());
    for (epoch, offset) in entries {
        let _ = writeln!(s, "{epoch} {offset}");
    }
    Bytes::from(s.into_bytes())
}

/// The byte blobs of one internally-consistent two-batch segment: offsets
/// 100..=102 in the first batch, 103..=104 in the second, plus the facts
/// `verify_segment` should derive from them.
struct SegmentBytes {
    log: Bytes,
    offset_index: Bytes,
    time_index: Bytes,
    transaction_index: Option<Bytes>,
    snapshot: Bytes,
    checkpoint: Bytes,
    base_offset: Offset,
    end_offset: Offset,
    max_timestamp_ms: i64,
    batches: u64,
    records: u64,
    leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

fn segment_bytes(batch1_max_ts: i64, batch2_max_ts: i64) -> SegmentBytes {
    let batch1 = batch(BatchSetup {
        base_timestamp: RecordTimestamp(1000),
        max_timestamp: RecordTimestamp(batch1_max_ts),
        ..Default::default()
    });
    let batch2 = batch(BatchSetup {
        base_offset: krabka_ids::Offset(103),
        base_timestamp: RecordTimestamp(1030),
        max_timestamp: RecordTimestamp(batch2_max_ts),
        record_count: RecordCount(2),
    });
    let batch1_len = batch1.encoded_len();
    let log = encode_all(&[batch1, batch2]);
    let max_timestamp_ms = [batch1_max_ts, batch2_max_ts]
        .into_iter()
        .filter(|&ts| ts != -1)
        .max()
        .unwrap_or(-1);

    SegmentBytes {
        log,
        offset_index: offset_index_bytes(&[(0, 0), (3, u32::try_from(batch1_len).unwrap())]),
        time_index: time_index_bytes(&[(0, 0)]),
        transaction_index: None,
        snapshot: snapshot_bytes(&[]),
        checkpoint: build_checkpoint(&[(0, 100), (1, 103)]),
        base_offset: Offset(100),
        end_offset: Offset(104),
        max_timestamp_ms,
        batches: 2,
        records: 5,
        leader_epochs: vec![(LeaderEpoch(0), Offset(100)), (LeaderEpoch(1), Offset(103))],
    }
}

fn valid_segment_bytes() -> SegmentBytes {
    segment_bytes(1020, 1040)
}

fn write_segment(dir: &std::path::Path, bytes: &SegmentBytes, omit: &[&str]) -> SegmentInventory {
    let present = |name: &str| !omit.contains(&name);
    SegmentInventory {
        segment_id: Uuid::from_u128(0xBEEF),
        base_offset: bytes.base_offset,
        log: present(".log").then(|| write_object(dir, "orders-0/seg.log", &bytes.log)),
        offset_index: present(".index")
            .then(|| write_object(dir, "orders-0/seg.index", &bytes.offset_index)),
        time_index: present(".timeindex")
            .then(|| write_object(dir, "orders-0/seg.timeindex", &bytes.time_index)),
        producer_snapshot: present(".snapshot")
            .then(|| write_object(dir, "orders-0/seg.snapshot", &bytes.snapshot)),
        leader_epoch: present(".leader_epoch_checkpoint").then(|| {
            write_object(
                dir,
                "orders-0/seg.leader_epoch_checkpoint",
                &bytes.checkpoint,
            )
        }),
        transaction_index: bytes
            .transaction_index
            .as_ref()
            .map(|bytes| write_object(dir, "orders-0/seg.txnindex", bytes)),
    }
}

#[tokio::test]
async fn a_clean_segment_verifies_and_reports_its_facts() {
    let (dir, store, partition) = verification_context();
    let fixture = valid_segment_bytes();
    let segment = write_segment(dir.path(), &fixture, &[]);

    let verified = verify_segment(&store, &partition, &segment)
        .await
        .expect("verify");

    check!(
        verified.facts
            == SegmentFacts {
                segment_id: segment.segment_id,
                base_offset: fixture.base_offset,
                end_offset: fixture.end_offset,
                max_timestamp_ms: fixture.max_timestamp_ms,
                batches: fixture.batches,
                records: fixture.records,
                log_bytes: u64::try_from(fixture.log.len()).unwrap(),
                leader_epochs: fixture.leader_epochs.clone(),
            }
    );
    check!(verified.log == fixture.log);
}

#[tokio::test]
async fn authenticated_fetch_rejects_object_replacement() {
    let (dir, store, partition) = verification_context();
    let fixture = valid_segment_bytes();
    let segment = write_segment(dir.path(), &fixture, &[]);
    let objects = segment
        .artifacts()
        .map(|artifact| {
            let key = artifact.key.to_string();
            let bytes = std::fs::read(dir.path().join(&key)).expect("read object");
            (
                key.clone(),
                krabka_remote_storage::ObjectEntry {
                    suffix: artifact
                        .key
                        .extension()
                        .map_or_else(String::new, |suffix| format!(".{suffix}")),
                    key,
                    size_bytes: u64::try_from(bytes.len()).unwrap(),
                    sha256: krabka_remote_storage::Sha256Digest::of(&bytes),
                    e_tag: None,
                    version_id: None,
                    create_precondition: true,
                },
            )
        })
        .collect();

    let replacement = vec![0x5a; fixture.log.len()];
    std::fs::write(dir.path().join("orders-0/seg.log"), replacement).expect("replace log");

    let error = verify_segment_authenticated(&store, &partition, &segment, &objects)
        .await
        .expect_err("replacement must fail authentication");
    check!(matches!(error, RestoreError::Authenticity { .. }));
}

#[tokio::test]
async fn missing_mandatory_artifacts_are_torn_copies_in_written_order() {
    let cases: [(&[&str], &str); 7] = [
        (&[".log"], ".log"),
        (&[".index"], ".index"),
        (&[".timeindex"], ".timeindex"),
        (&[".snapshot"], ".snapshot"),
        (&[".leader_epoch_checkpoint"], ".leader_epoch_checkpoint"),
        // When more than one artifact is absent, the first in the
        // documented order is the one reported.
        (&[".log", ".timeindex"], ".log"),
        (
            &[".index", ".snapshot", ".leader_epoch_checkpoint"],
            ".index",
        ),
    ];

    for (omit, expected) in cases {
        let (dir, store, partition) = verification_context();
        let fixture = valid_segment_bytes();
        let segment = write_segment(dir.path(), &fixture, omit);

        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("torn copy");
        let RestoreError::TornCopy { artifact, .. } = error else {
            panic!("omit {omit:?}: expected TornCopy, got {error:?}");
        };
        check!(artifact.as_str() == expected, "omit {omit:?}");
    }
}

#[tokio::test]
async fn a_flipped_crc_covered_byte_is_a_checksum_mismatch() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    // Byte 62 sits inside the first batch's body, well past its 61-byte
    // header and the CRC field it carries.
    let mut corrupted = fixture.log.to_vec();
    corrupted[62] ^= 0xFF;
    fixture.log = Bytes::from(corrupted);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("checksum mismatch");
    let RestoreError::ChecksumMismatch { key, position, .. } = error else {
        panic!("expected ChecksumMismatch, got {error:?}");
    };
    check!(key == "orders-0/seg.log");
    check!(position == 0);
}

#[tokio::test]
async fn a_log_truncated_mid_batch_is_a_truncated_segment() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    let full_len = fixture.log.len();
    fixture.log = fixture.log.slice(0..full_len - 10);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("truncated segment");
    check!(matches!(error, RestoreError::TruncatedSegment { .. }));
}

#[tokio::test]
async fn a_gap_between_crc_valid_batches_is_accepted() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    // The first batch ends at 102, but the second starts at 104. Both batches
    // are independently well-framed and carry valid CRCs.
    fixture.log = encode_all(&[
        batch(BatchSetup {
            base_timestamp: RecordTimestamp(1000),
            max_timestamp: RecordTimestamp(1020),
            ..Default::default()
        }),
        batch(BatchSetup {
            base_offset: krabka_ids::Offset(104),
            base_timestamp: RecordTimestamp(1040),
            max_timestamp: RecordTimestamp(1040),
            record_count: RecordCount(1),
        }),
    ]);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let verified = verify_segment(&store, &partition, &segment)
        .await
        .expect("compacted offset gap");
    check!(verified.facts.end_offset == Offset(104));
}

#[tokio::test]
async fn first_batch_must_match_the_segment_base_offset() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    fixture.log = single_record_bytes(SingleRecordSetup {
        base_offset: krabka_ids::Offset(101),
        ..Default::default()
    });

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("mis-keyed segment");
    check!(matches!(error, RestoreError::TruncatedSegment { .. }));
}

#[tokio::test]
async fn a_batch_whose_exclusive_end_overflows_is_rejected() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    fixture.base_offset = Offset(i64::MAX);
    fixture.log = single_record_bytes(SingleRecordSetup {
        base_offset: krabka_ids::Offset(i64::MAX),
        ..Default::default()
    });

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("exclusive offset overflow");
    check!(matches!(error, RestoreError::TruncatedSegment { .. }));
}

#[tokio::test]
async fn an_offset_index_entry_past_the_log_end_is_rejected() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    let log_len = u32::try_from(fixture.log.len()).unwrap();
    fixture.offset_index = offset_index_bytes(&[(0, 0), (3, log_len + 1_000)]);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("index entry past the log end");
    check!(matches!(error, RestoreError::TruncatedSegment { .. }));
}

#[tokio::test]
async fn a_non_monotonic_time_index_is_rejected() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    fixture.time_index = time_index_bytes(&[(1_030, 3), (1_000, 0)]);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("non-monotonic time index");
    check!(matches!(error, RestoreError::TruncatedSegment { .. }));
}

#[tokio::test]
async fn sparse_indexes_require_complete_strict_bounded_entries() {
    enum Sidecar {
        Offset(Bytes),
        Time(Bytes),
        Transaction(Bytes),
    }

    let cases = [
        Sidecar::Offset(offset_index_bytes(&[(0, 0), (0, 10)])),
        Sidecar::Offset(offset_index_bytes(&[(0, 0), (3, 0)])),
        Sidecar::Offset(Bytes::from_static(&[0; 7])),
        Sidecar::Time(time_index_bytes(&[(1_000, 0), (1_010, 0)])),
        Sidecar::Time(Bytes::from_static(&[0; 11])),
        // Abort markers out of order, then repeated.
        Sidecar::Transaction(txn_index_bytes(&[(100, 104, 7), (101, 103, 8)])),
        Sidecar::Transaction(txn_index_bytes(&[(100, 103, 7), (101, 103, 8)])),
        // A marker before or past the segment that indexes it.
        Sidecar::Transaction(txn_index_bytes(&[(90, 99, 7)])),
        Sidecar::Transaction(txn_index_bytes(&[(100, 105, 7)])),
        // A transaction without a producer. (One that starts after its
        // marker is already rejected by `parse_txn_index`.)
        Sidecar::Transaction(txn_index_bytes(&[(100, 102, -1)])),
        Sidecar::Transaction(Bytes::from_static(&[0; 33])),
    ];

    for sidecar in cases {
        let (dir, store, partition) = verification_context();
        let mut fixture = valid_segment_bytes();
        match sidecar {
            Sidecar::Offset(bytes) => fixture.offset_index = bytes,
            Sidecar::Time(bytes) => fixture.time_index = bytes,
            Sidecar::Transaction(bytes) => fixture.transaction_index = Some(bytes),
        }

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("invalid sparse index");
        check!(
            matches!(error, RestoreError::TruncatedSegment { .. }),
            "{error:?}"
        );
    }
}

/// Kafka indexes an aborted transaction in the segment holding its abort
/// marker, in marker order (`TransactionIndex.append` requires `lastOffset` to
/// strictly increase). The transaction's first offset is not constrained by
/// the segment base or by the previous entry.
#[tokio::test]
async fn transaction_index_accepts_kafka_marker_order() {
    let cases: [(&str, &[TxnEntry]); 3] = [
        ("sequential aborts", &[(100, 101, 7), (102, 103, 8)]),
        // Producer 2 opens at 102 and aborts at 103; producer 1 opened
        // earlier at 100 and aborts later at 104.
        ("interleaved aborts", &[(102, 103, 2), (100, 104, 1)]),
        // The transaction's data sits in an earlier segment; its abort marker
        // at 100 is the first offset of this one.
        ("transaction spanning a segment roll", &[(40, 100, 1_000)]),
    ];

    for (name, entries) in cases {
        let (dir, store, partition) = verification_context();
        let mut fixture = valid_segment_bytes();
        fixture.transaction_index = Some(txn_index_bytes(entries));

        let segment = write_segment(dir.path(), &fixture, &[]);
        let verified = verify_segment(&store, &partition, &segment).await;
        check!(verified.is_ok(), "{name}: {:?}", verified.as_ref().err());
    }
}

#[tokio::test]
async fn a_corrupt_snapshot_crc_is_rejected() {
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();

    let mut corrupted = fixture.snapshot.to_vec();
    corrupted[2] ^= 0xFF;
    fixture.snapshot = Bytes::from(corrupted);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let error = verify_segment(&store, &partition, &segment)
        .await
        .expect_err("corrupt snapshot CRC");
    check!(matches!(error, RestoreError::ChecksumMismatch { .. }));
}

#[tokio::test]
async fn producer_snapshots_require_legal_unique_states_in_any_order() {
    let valid = [
        (9, 2, 4, 104, 1, 1_040, 1, 103),
        (7, 1, 3, 102, 2, 1_020, 0, -1),
    ];

    // The same verified bytes are safe to retry.
    let (dir, store, partition) = verification_context();
    let mut fixture = valid_segment_bytes();
    fixture.snapshot = snapshot_bytes(&valid);
    let segment = write_segment(dir.path(), &fixture, &[]);
    verify_segment(&store, &partition, &segment)
        .await
        .expect("first verification");
    verify_segment(&store, &partition, &segment)
        .await
        .expect("retry verification");

    let invalid = [
        vec![valid[0], valid[0]],               // duplicate producer ID
        vec![(7, 1, 3, 105, 2, 1_050, 0, -1)],  // state at the frontier
        vec![(-1, 1, 3, 102, 2, 1_020, 0, -1)], // invalid producer ID
    ];
    for entries in invalid {
        let (dir, store, partition) = verification_context();
        let mut fixture = valid_segment_bytes();
        fixture.snapshot = snapshot_bytes(&entries);
        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("invalid producer snapshot");
        check!(matches!(error, RestoreError::TruncatedSegment { .. }));
    }
}

#[tokio::test]
async fn malformed_leader_epoch_checkpoints_are_rejected() {
    let cases = [
        "1\n0\n",               // wrong version header
        "0\nnot-a-number\n",    // non-numeric row count
        "0\n2\n0 100\n",        // declares 2 rows, only 1 present
        "0\n1\nzero hundred\n", // non-numeric row
        "0\n2\n0 100\n0 103\n", // epoch does not strictly increase
        "0\n2\n0 103\n1 102\n", // start offset does not strictly increase
        "0\n1\n-1 100\n",       // negative epoch
        "0\n1\n0 99\n",         // before the segment
        "0\n1\n0 105\n",        // after the segment
    ];

    for bad in cases {
        let (dir, store, partition) = verification_context();
        let mut fixture = valid_segment_bytes();
        fixture.checkpoint = Bytes::from_static(bad.as_bytes());

        let segment = write_segment(dir.path(), &fixture, &[]);
        let error = verify_segment(&store, &partition, &segment)
            .await
            .expect_err("malformed checkpoint");
        check!(
            matches!(error, RestoreError::TruncatedSegment { .. }),
            "case {bad:?}"
        );
    }
}

#[tokio::test]
async fn every_batch_reporting_unknown_timestamp_keeps_the_sentinel() {
    let (dir, store, partition) = verification_context();
    let fixture = segment_bytes(-1, -1);

    let segment = write_segment(dir.path(), &fixture, &[]);
    let verified = verify_segment(&store, &partition, &segment)
        .await
        .expect("verify");
    check!(verified.facts.max_timestamp_ms == -1);
}
