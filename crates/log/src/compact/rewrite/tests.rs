//! Unit tests for the compaction rewrite pass: superseded-record removal,
//! tombstone and marker delete-horizon stamping, and `RETAIN_EMPTY`.

use std::fs;

use krabka_compression::CompressionType;
use krabka_ids::Offset;
use krabka_protocol::records::{Attributes, Record, TimestampType};
use krabka_units::prelude::millis;

use super::*;
use crate::compact::test_support::{
    RETENTION, control_batch, make_record, rewrite_all, rewrite_with_map, superseded_records,
    transactional_record, write_sealed_batches, write_sealed_segment,
};

/// A far-future `now` so nothing in the simple tests ages out, plus an
/// empty active-producer set and no surviving transactions.
const NEVER_AGE_NOW_MS: i64 = 0;

fn keyed_value_batch(base_offset: i64, key: &[u8], value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        last_offset_delta: 0,
        producer_id: -1,
        records: vec![make_record(0, Some(key), Some(value))],
        ..RecordBatch::default()
    }
}

fn rewrite_simple(dir: &Path, segment_refs: &[&Segment]) -> RewriteOutput {
    rewrite_all(
        dir,
        segment_refs,
        &mut CleanedTransactionMetadata::default(),
        RewriteRetention {
            now_ms: NEVER_AGE_NOW_MS,
            delete_retention: RETENTION,
        },
        &HashMap::new(),
    )
}

pub(super) fn decode_all(bytes: &[u8]) -> Vec<RecordBatch> {
    let mut cursor = bytes;
    let mut out = Vec::new();
    while !cursor.is_empty() {
        let Ok(b) = RecordBatch::decode(&mut cursor) else {
            break;
        };
        out.push(b);
    }
    out
}

/// Decode the written bytes before checking both independent output frontiers.
fn read_rewrite(out: &RewriteOutput, expected: (i64, i64)) -> Vec<RecordBatch> {
    let batches = decode_all(&fs::read(&out.log_swap).unwrap());
    assert2::assert!(out.new_base_offset == Offset(expected.0));
    assert2::assert!(out.new_last_offset == Offset(expected.1));
    batches
}

/// Independent expected records for the two supersession cases.
fn surviving_key_records() -> Vec<Record> {
    vec![
        make_record(1, Some(b"k2"), Some(b"v2")),
        make_record(2, Some(b"k1"), Some(b"v3")),
    ]
}

#[test]
fn rewrite_drops_superseded_records() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(dir.path(), 0, superseded_records());
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    let batches = read_rewrite(&out, (0, 2));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 0,
                last_offset_delta: 2,
                records: surviving_key_records(),
                ..RecordBatch::default()
            }]
    );
}

#[test]
fn rewrite_keeps_tombstone_as_latest() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        0,
        vec![
            make_record(0, Some(b"k1"), Some(b"v1")),
            make_record(1, Some(b"k1"), None), // tombstone
        ],
    );
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    let bytes = fs::read(&out.log_swap).unwrap();
    let mut cursor = &bytes[..];
    let batch = RecordBatch::decode(&mut cursor).unwrap();
    let mut record = make_record(1, Some(b"k1"), None);
    record.timestamp_delta = -1_000;
    assert2::assert!(out.new_base_offset == Offset(0));
    assert2::assert!(out.new_last_offset == Offset(1));
    assert2::assert!(
        batch
            == RecordBatch {
                base_offset: 0,
                last_offset_delta: 1,
                base_timestamp: RETENTION.millis_i64(),
                attributes: Attributes::default().with_delete_horizon(true),
                records: vec![record],
                ..RecordBatch::default()
            }
    );
}

#[test]
fn rewrite_preserves_absolute_offsets() {
    let dir = tempfile::tempdir().unwrap();
    // Absolute offsets 100..=102 retain k2 and the newest k1; 103 is unkeyed.
    let mut records = superseded_records();
    records.push(make_record(3, None, Some(b"unkeyed"))); // abs 103 — dropped
    let first_segment = write_sealed_segment(dir.path(), 100, records);
    let segment_refs = vec![&first_segment];
    let out = rewrite_simple(dir.path(), &segment_refs);
    // The batch keeps its original last offset, 103, although the record at
    // 103 is gone: Kafka's `overrideLastOffset(originalBatch.lastOffset())`.
    let batches = read_rewrite(&out, (100, 103));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 100,
                last_offset_delta: 3,
                records: surviving_key_records(),
                ..RecordBatch::default()
            }]
    );
}

/// A record `delta` milliseconds after its batch's base timestamp.
fn record_at(offset_delta: i32, key: Option<&[u8]>, delta: i64) -> Record {
    Record {
        timestamp_delta: delta,
        ..make_record(offset_delta, key, Some(b"v"))
    }
}

/// A batch that loses the record carrying its maximum timestamp gets the
/// retained records' maximum under `CreateTime`, as Kafka's
/// `MemoryRecordsBuilder.recordWritten` computes it, and keeps its own under
/// `LogAppendTime`, where `writeDefaultBatchHeader` writes the append time.
#[test]
fn a_batch_that_loses_its_newest_record_recomputes_its_max_timestamp() {
    let cases = [
        (
            "CreateTime",
            Attributes::default(),
            1_002, // the newest of the kept records
        ),
        (
            "LogAppendTime",
            Attributes::default().with_timestamp_type(TimestampType::LogAppendTime),
            1_050, // the append time
        ),
    ];
    for (label, attributes, expected_max_timestamp) in cases {
        let dir = tempfile::tempdir().unwrap();
        let batch = RecordBatch {
            base_offset: 0,
            last_offset_delta: 3,
            base_timestamp: 1_000,
            max_timestamp: 1_050,
            attributes,
            records: vec![
                record_at(0, Some(b"k1"), 0),
                record_at(1, Some(b"k2"), 1),
                record_at(2, Some(b"k1"), 2),
                record_at(3, None, 50), // dropped: it carries the max timestamp
            ],
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[batch]);
        let segment_refs = vec![&seg];
        let out = rewrite_simple(dir.path(), &segment_refs);

        assert2::assert!(
            decode_all(&fs::read(&out.log_swap).unwrap())
                == vec![RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 3,
                    base_timestamp: 1_000,
                    max_timestamp: expected_max_timestamp,
                    attributes,
                    records: vec![record_at(1, Some(b"k2"), 1), record_at(2, Some(b"k1"), 2)],
                    ..RecordBatch::default()
                }],
            "{label}"
        );
    }
}

/// (a) End-to-end control-batch bug fix. Two commit markers at different
/// offsets BOTH survive when the data of their transactions survives.
#[test]
fn rewrite_both_commit_markers_survive_when_data_survives() {
    let dir = tempfile::tempdir().unwrap();
    // pid 1000: data batch at offset 0 (key k1), commit marker at offset 1.
    // pid 2000: data batch at offset 2 (key k2), commit marker at offset 3.
    let data1 = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        payload: (b"k1", b"v1"),
        ..Default::default()
    });
    let marker1 = control_batch(1, 1000, 1 /* COMMIT */);
    let data2 = transactional_record(crate::compact::test_support::TransactionalRecordSetup {
        offset: crate::Offset(2),
        producer: crate::ProducerId(2000),
        payload: (b"k2", b"v2"),
    });
    let marker2 = control_batch(3, 2000, 1 /* COMMIT */);
    let expected = vec![
        data1.clone(),
        marker1.clone(),
        data2.clone(),
        marker2.clone(),
    ];
    let seg = write_sealed_batches(dir.path(), &[data1, marker1, data2, marker2]);
    let segment_refs = vec![&seg];
    let out = rewrite_simple(dir.path(), &segment_refs);

    let batches = read_rewrite(&out, (0, 3));
    assert2::assert!(batches == expected);
}

/// (b) A newest-for-key tombstone with no existing horizon gets bit 6 set
/// and `base_timestamp == now + delete_retention_ms`.
#[test]
fn rewrite_tombstone_gets_horizon_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let first_segment = write_sealed_segment(
        dir.path(),
        0,
        vec![make_record(0, Some(b"k1"), None)], // tombstone, newest for k1
    );
    let segment_refs = vec![&first_segment];
    let now = 5_000i64;
    let ret = 50i64;
    let retention = Time::from_millis(ret);
    let out = rewrite_all(
        dir.path(),
        &segment_refs,
        &mut CleanedTransactionMetadata::default(),
        RewriteRetention {
            now_ms: now,
            delete_retention: retention,
        },
        &HashMap::new(),
    );
    let mut record = make_record(0, Some(b"k1"), None);
    record.timestamp_delta = -(now + ret);
    let batches = read_rewrite(&out, (0, 0));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 0,
                last_offset_delta: 0,
                base_timestamp: now + ret,
                attributes: Attributes::default().with_delete_horizon(true),
                records: vec![record],
                ..RecordBatch::default()
            }]
    );
}

/// (c) The rewrite drops a commit marker when the data of its transaction
/// is fully gone and its existing horizon has elapsed.
#[test]
fn rewrite_marker_dropped_when_data_gone_and_horizon_elapsed() {
    let dir = tempfile::tempdir().unwrap();
    // A standalone commit marker for pid 1000 with NO surviving data, and
    // an already-stamped delete horizon at base_timestamp = 100.
    let mut marker = control_batch(0, 1000, 1 /* COMMIT */);
    marker.base_timestamp = 100;
    marker.attributes = marker.attributes.with_delete_horizon(true);
    // A second data batch (pid -1) so the marker is not the last batch
    // (otherwise RETAIN_EMPTY would keep a bare header).
    let data = RecordBatch {
        base_offset: 1,
        last_offset_delta: 0,
        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[marker, data]);
    let segment_refs = vec![&seg];
    // now=200 >= horizon 100 → marker deleted.
    let out = rewrite_all(
        dir.path(),
        &segment_refs,
        &mut CleanedTransactionMetadata::default(),
        RewriteRetention {
            now_ms: 200,
            delete_retention: millis(50),
        },
        &HashMap::new(),
    );
    let batches = read_rewrite(&out, (0, 1));
    assert2::assert!(
        batches
            == vec![RecordBatch {
                base_offset: 1,
                last_offset_delta: 0,
                records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
                ..RecordBatch::default()
            }]
    );
}

/// (d) `RETAIN_EMPTY`: the rewrite writes the fully-emptied batch of an
/// active producer again as a bare header with no records. `producer_id`,
/// `epoch`, and `sequence` survive.
#[test]
fn rewrite_retain_empty_for_active_producer() {
    let dir = tempfile::tempdir().unwrap();
    // pid 1000 data batch under k1 at offset 0, then a NEWER data batch
    // (pid -1) under k1 at offset 1 that supersedes it — so pid 1000's
    // only record is dropped, emptying its batch. pid 1000 is active.
    let data1 = RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        producer_id: 1000,
        producer_epoch: 7,
        base_sequence: 3,
        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        ..RecordBatch::default()
    };
    let data2 = keyed_value_batch(1, b"k1", b"v2");
    let seg = write_sealed_batches(dir.path(), &[data1, data2]);
    let segment_refs = vec![&seg];
    let mut active = HashMap::new();
    // pid 1000 is active, and its last data batch ends at offset 0.
    active.insert(
        ProducerId(1000),
        ProducerLastRecord {
            last_data_offset: Some(Offset(0)),
            producer_epoch: 7,
        },
    );
    let out = rewrite_all(
        dir.path(),
        &segment_refs,
        &mut CleanedTransactionMetadata::default(),
        RewriteRetention {
            now_ms: 0,
            delete_retention: RETENTION,
        },
        &active,
    );
    let batches = read_rewrite(&out, (0, 1));
    assert2::assert!(
        batches
            == vec![
                RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 0,
                    producer_id: 1000,
                    producer_epoch: 7,
                    base_sequence: 3,
                    base_timestamp: -1,
                    ..RecordBatch::default()
                },
                keyed_value_batch(1, b"k1", b"v2"),
            ]
    );
}

// `RETAIN_EMPTY` last-offset arithmetic: an emptied output-last batch is
// re-emitted as a bare header, and its `base_offset + last_offset_delta`
// must extend `new_last_offset`. The emptied batch sits at base_offset 100
// with `last_offset_delta` 5, so its last absolute offset is `100 + 5 =
// 105`. This pins the `+` in `Offset(base_offset + last_offset_delta)`:
// mutating it to `-` would report `new_last_offset == 95`.
#[test]
fn rewrite_retain_empty_extends_last_offset() {
    let dir = tempfile::tempdir().unwrap();
    // Batch 0 (base 0): one surviving keyed record (abs offset 0).
    let data0 = keyed_value_batch(0, b"k1", b"v1");
    // Batch 1 (base 100, last_offset_delta 5): only NULL-key records, all
    // dropped, so the batch is emptied. As the output-last batch it is
    // re-emitted as a bare header spanning abs offsets 100..=105.
    let data1 = RecordBatch {
        base_offset: 100,
        last_offset_delta: 5,
        producer_id: -1,
        records: vec![
            make_record(0, None, Some(b"n1")),
            make_record(5, None, Some(b"n2")),
        ],
        ..RecordBatch::default()
    };
    let seg = write_sealed_batches(dir.path(), &[data0, data1]);
    let segment_refs = vec![&seg];
    let out = rewrite_simple(dir.path(), &segment_refs);

    // The emptied batch is re-emitted as a bare header at base_offset 100.
    let batches = read_rewrite(&out, (0, 105));
    assert2::assert!(
        batches
            == vec![
                keyed_value_batch(0, b"k1", b"v1"),
                RecordBatch {
                    base_offset: 100,
                    last_offset_delta: 5,
                    base_timestamp: -1,
                    producer_id: -1,
                    ..RecordBatch::default()
                },
            ]
    );
}

/// A one-record batch at `base` from `producer_id` at `producer_epoch`. A
/// `None` key makes the record one the cleaner always drops.
fn one_record_batch(
    base: i64,
    (producer_id, producer_epoch): (i64, i16),
    key: Option<&[u8]>,
) -> RecordBatch {
    RecordBatch {
        base_offset: base,
        last_offset_delta: 0,
        producer_id,
        producer_epoch,
        records: vec![make_record(0, key, Some(b"v"))],
        ..RecordBatch::default()
    }
}

/// Which bare headers `Cleaner.cleanInto` keeps (#1198): a batch emptied by
/// the pass is written again only when it is its producer's last data batch
/// (or, for a producer that wrote only transaction markers, a marker of its
/// current epoch), or when it is the last batch of the whole round. Each case
/// is `(label, batches, the active producer, the base offsets that survive)`.
#[test]
fn retain_empty_follows_kafkas_cleaner_rules() {
    // A commit marker whose delete horizon has elapsed, so the pass drops its
    // record, from producer 1000 at epoch 7.
    let mut marker = control_batch(0, 1000, 1 /* COMMIT */);
    marker.producer_epoch = 7;
    marker.base_timestamp = 100;
    marker.attributes = marker.attributes.with_delete_horizon(true);
    let kept = |base| one_record_batch(base, (-1, -1), Some(b"k"));
    let dropped = |base, producer| one_record_batch(base, producer, None);
    let last = |last_data_offset, producer_epoch| ProducerLastRecord {
        last_data_offset,
        producer_epoch,
    };
    let cases = [
        (
            "the producer's last data batch keeps its header",
            vec![dropped(0, (1000, 7)), dropped(1, (1000, 7)), kept(2)],
            Some(last(Some(Offset(1)), 7)),
            vec![1, 2],
        ),
        (
            "a producer that is not active keeps nothing",
            vec![dropped(0, (1000, 7)), dropped(1, (1000, 7)), kept(2)],
            None,
            vec![2],
        ),
        (
            "a marker of the current epoch keeps a marker-only producer alive",
            vec![marker.clone(), kept(1)],
            Some(last(None, 7)),
            vec![0, 1],
        ),
        (
            "a marker of another epoch does not",
            vec![marker.clone(), kept(1)],
            Some(last(None, 8)),
            vec![1],
        ),
        (
            "a data batch of a marker-only producer does not",
            vec![dropped(0, (1000, 7)), kept(1)],
            Some(last(None, 7)),
            vec![1],
        ),
        (
            "only the last batch of the round is kept empty",
            vec![kept(0), dropped(1, (-1, -1)), dropped(2, (-1, -1))],
            None,
            vec![0, 2],
        ),
    ];
    for (label, batches, active, expected) in cases {
        let dir = tempfile::tempdir().unwrap();
        let seg = write_sealed_batches(dir.path(), &batches);
        let segment_refs = vec![&seg];
        let active: HashMap<_, _> = active
            .into_iter()
            .map(|last| (ProducerId(1000), last))
            .collect();

        let out = rewrite_all(
            dir.path(),
            &segment_refs,
            &mut CleanedTransactionMetadata::default(),
            RewriteRetention {
                now_ms: 200,
                delete_retention: millis(50),
            },
            &active,
        );

        let survivors: Vec<i64> = decode_all(&fs::read(&out.log_swap).unwrap())
            .iter()
            .map(|batch| batch.base_offset)
            .collect();
        assert2::assert!(survivors == expected, "{label}");
    }
}

/// The bare header a `RETAIN_EMPTY` batch leaves is Kafka's
/// `DefaultRecordBatch.writeEmptyHeader`: no compression, no delete horizon
/// and no base timestamp, whatever the emptied batch had. The transactional
/// and timestamp-type bits, the max timestamp and the producer state carry
/// over.
#[test]
fn a_retained_empty_batch_has_no_compression_horizon_or_base_timestamp() {
    let transactional_append_time = Attributes::default()
        .with_transactional(true)
        .with_timestamp_type(TimestampType::LogAppendTime);
    let cases = [
        (
            "compressed",
            Attributes::default().with_compression(CompressionType::Gzip),
            Attributes::default(),
        ),
        (
            "delete horizon",
            Attributes::default().with_delete_horizon(true),
            Attributes::default(),
        ),
        (
            "transactional LogAppendTime, compressed, with a horizon",
            transactional_append_time
                .with_compression(CompressionType::Gzip)
                .with_delete_horizon(true),
            transactional_append_time,
        ),
    ];
    for (label, attributes, expected_attributes) in cases {
        let emptied = RecordBatch {
            base_offset: 10,
            partition_leader_epoch: 4,
            attributes,
            last_offset_delta: 1,
            base_timestamp: 500,
            max_timestamp: 700,
            producer_id: 1000,
            producer_epoch: 7,
            base_sequence: 3,
            records: vec![make_record(0, None, Some(b"n1"))],
        };
        assert2::assert!(
            bare_header(&emptied)
                == RecordBatch {
                    base_offset: 10,
                    partition_leader_epoch: 4,
                    attributes: expected_attributes,
                    last_offset_delta: 1,
                    base_timestamp: -1,
                    max_timestamp: 700,
                    producer_id: 1000,
                    producer_epoch: 7,
                    base_sequence: 3,
                    records: vec![],
                },
            "{label}"
        );
    }
}

/// A round that rewrites several size-bounded groups keeps only the last
/// batch of the round when it is empty, not the last batch of every group:
/// Kafka's `batch.nextOffset() == upperBoundOffsetOfCleaningRound`.
#[test]
fn only_the_last_group_of_a_round_keeps_an_emptied_last_batch() {
    let dir = tempfile::tempdir().unwrap();
    let first = write_sealed_batches(dir.path(), &[one_record_batch(0, (-1, -1), None)]);
    let second = write_sealed_batches(dir.path(), &[one_record_batch(1, (-1, -1), None)]);
    let map = crate::compact::build_offset_map(&[&first, &second], vec![], None).unwrap();
    let active = HashMap::new();
    let round = CleaningRound {
        active_producers: &active,
        upper_bound: Offset(2),
        max_decompressed_record: None,
    };
    let mut txn = CleanedTransactionMetadata::default();
    let outputs = [&first, &second].map(|segment| {
        let out = rewrite_with_map(
            dir.path(),
            &[segment],
            &map,
            &mut txn,
            RewriteRetention {
                now_ms: NEVER_AGE_NOW_MS,
                delete_retention: RETENTION,
            },
            round,
        );
        decode_all(&fs::read(&out.log_swap).unwrap())
    });

    assert2::assert!(
        outputs
            == [
                vec![],
                vec![RecordBatch {
                    base_offset: 1,
                    base_timestamp: -1,
                    last_offset_delta: 0,
                    ..RecordBatch::default()
                }]
            ]
    );
}
