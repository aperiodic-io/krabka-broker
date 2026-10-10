//! Tests that drive the zero-copy dispatch end to end, from the verbatim
//! passthrough predicate to the writer's `ProduceData`.

use std::sync::Arc;

use assert2::{assert, check};
use bytes::{Bytes, BytesMut};
use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_protocol::records::{
    Attributes, CRC_COVERAGE_START, HEADER_LEN, Record, RecordBatch, RecordsPayload, TimestampType,
};

use super::super::{DecodeEnv, PartitionPayload, PreparedSource, prepare_batch};
use crate::{
    handlers::produce::{append::build_produce_data, topic_settings::TimestampPolicy},
    partition::ProduceData,
};

/// Prepare against the fixture topic with the caller's compression, metrics
/// and version; the macro retains temporary borrows through the full expression.
macro_rules! prepare_for_topic {
    ($payload:expr, $compression:expr, $metrics:expr, $version:expr) => {
        prepare_batch(
            $payload,
            $compression,
            TimestampPolicy::default(),
            false,
            DecodeEnv {
                topic_name: &topic(),
                metrics: $metrics,
                policy: RecordDecompressionPolicy::default(),
            },
            $version,
        )
    };
}

/// The topic name these cases record under, as the shared handle the metric
/// label sets clone.
fn topic() -> Arc<str> {
    Arc::from("t")
}

fn encode(b: &RecordBatch) -> Bytes {
    let mut buf = BytesMut::new();
    b.encode(&mut buf).unwrap();
    buf.freeze()
}

fn refresh_batch_crc(encoded: &mut [u8]) {
    let crc = crc32c::crc32c(&encoded[CRC_COVERAGE_START..]);
    encoded[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
}

fn plain_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: -1,
        last_offset_delta: 0,
        max_timestamp: 42,
        producer_id: -1,
        records: vec![Record {
            value: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn rejected_batch(batch: &RecordBatch) -> i16 {
    prepare_for_topic!(
        PartitionPayload::Slice(encode(batch)),
        None,
        &crate::metrics::BrokerMetrics::new(),
        13
    )
    .unwrap_err()
}

#[test]
fn message_count_reports_v2_record_total() {
    // Multi-record batch so the count can't be mistaken for a constant.
    let batch = RecordBatch {
        last_offset_delta: 2,
        records: vec![
            Record {
                value: Some(Bytes::from_static(b"a")),
                ..Default::default()
            },
            Record {
                value: Some(Bytes::from_static(b"b")),
                ..Default::default()
            },
            Record {
                value: Some(Bytes::from_static(b"c")),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let wire = encode(&batch);
    // A null field and a non-v2 (zeroed) slice both contribute zero.
    let cases = [
        (PartitionPayload::Slice(wire), 3, "v2 slice with 3 records"),
        (PartitionPayload::Null, 0, "null records field"),
        (
            PartitionPayload::Slice(Bytes::from_static(&[0u8; 64])),
            0,
            "non-v2 zeroed slice",
        ),
    ];
    for (payload, want, label) in cases {
        assert!(payload.message_count() == want, "case: {label}");
    }
}

/// Run the full dispatch over a v≥3 records slice: first
/// `prepare_batch`, then `build_produce_data` with the given leader
/// epoch.
fn dispatch_slice(
    slice: Bytes,
    topic_compression: Option<CompressionType>,
    leader_epoch: i32,
) -> ProduceData {
    let m = crate::metrics::BrokerMetrics::new();
    let prepared =
        prepare_for_topic!(PartitionPayload::Slice(slice), topic_compression, &m, 13).unwrap();
    build_produce_data(prepared, leader_epoch)
}

#[test]
fn passthrough_when_all_conditions_hold() {
    let b = plain_batch();
    let wire = encode(&b);
    let data = dispatch_slice(wire.clone(), None, 7);
    match data {
        ProduceData::Verbatim(v) => {
            check!(&v.bytes[..] == &wire[..]);
            check!(v.leader_epoch == 7);
            check!(v.max_timestamp == 42);
            check!(v.last_offset_delta == 0);
        }
        ProduceData::Owned(_) => panic!("expected Verbatim"),
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

#[test]
fn passthrough_when_target_codec_equals_current() {
    // Topic forces lz4; batch is already lz4 → no recompression needed.
    let mut b = plain_batch();
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    let wire = encode(&b);
    let data = dispatch_slice(wire, Some(CompressionType::Lz4), 1);
    assert!(matches!(data, ProduceData::Verbatim(_)));
}

#[test]
fn fallback_when_null_field() {
    // A wire-null records field is rejected as INVALID_REQUEST.
    let m = crate::metrics::BrokerMetrics::new();
    let err = prepare_for_topic!(PartitionPayload::Null, None, &m, 13).unwrap_err();
    assert!(err == crate::codes::INVALID_REQUEST);
}

#[test]
fn fallback_on_recompression_to_different_codec() {
    // Batch uncompressed, topic forces zstd → must recompress (owned).
    let b = plain_batch();
    let wire = encode(&b);
    let data = dispatch_slice(wire, Some(CompressionType::Zstd), 0);
    assert!(matches!(data, ProduceData::Owned(_)));
}

#[test]
fn rejects_client_log_append_time() {
    let mut b = plain_batch();
    b.attributes = b
        .attributes
        .with_timestamp_type(TimestampType::LogAppendTime);
    let err = rejected_batch(&b);
    assert!(err == crate::codes::INVALID_TIMESTAMP);
}

#[test]
fn rejects_client_control_batch() {
    let mut b = plain_batch();
    b.attributes = Attributes::default().with_control(true);
    let err = rejected_batch(&b);
    assert!(err == crate::codes::INVALID_RECORD);
}

/// `ProduceRequest.validateRecords` refuses a zstd batch below `Produce` v7
/// with `UNSUPPORTED_COMPRESSION_TYPE`, on both append shapes and whatever the
/// topic's own `compression.type` asks for, because the refusal is about
/// whether the REQUESTING client's version can decode zstd, not about what
/// the broker stores it as.
#[test]
fn rejects_zstd_below_v7_and_admits_it_from_v7_on_both_paths() {
    let mut b = plain_batch();
    b.attributes = b.attributes.with_compression(CompressionType::Zstd);
    let wire = encode(&b);

    for (version, admitted) in [(0, false), (6, false), (7, true), (13, true)] {
        let payloads = [
            PartitionPayload::Slice(wire.clone()),
            PartitionPayload::Owned(RecordsPayload::V2(vec![b.clone()])),
        ];
        for payload in payloads {
            let result = prepare_for_topic!(
                payload,
                None,
                &crate::metrics::BrokerMetrics::new(),
                version
            );
            if admitted {
                assert!(result.is_ok(), "version {version}: {result:?}");
            } else {
                assert!(
                    result.as_ref().err() == Some(&crate::codes::UNSUPPORTED_COMPRESSION_TYPE),
                    "version {version}: {result:?}"
                );
            }
        }
    }
}

/// Kafka does not inspect `baseOffset` at all: `LogValidator.validateBatch`
/// checks only that the record count is positive and agrees with the offset
/// range. A nonzero `base_offset` is therefore admitted, on both append
/// shapes, exactly like every other batch this file drives through
/// `prepare_batch`.
#[test]
fn admits_a_nonzero_base_offset_on_both_paths() {
    let mut b = plain_batch();
    b.base_offset = 1;
    let payloads = [
        PartitionPayload::Slice(encode(&b)),
        PartitionPayload::Owned(RecordsPayload::V2(vec![b])),
    ];
    for payload in payloads {
        let prepared = prepare_for_topic!(payload, None, &crate::metrics::BrokerMetrics::new(), 13);
        assert!(prepared.is_ok());
    }
}

#[test]
fn rejects_invalid_client_batch_metadata_on_header_and_owned_paths() {
    let mut invalid_offset_range = plain_batch();
    invalid_offset_range.last_offset_delta = -1;

    let mut inconsistent_count = plain_batch();
    inconsistent_count.last_offset_delta = 1;

    let mut overflowing_offset_count = plain_batch();
    overflowing_offset_count.last_offset_delta = i32::MAX;
    overflowing_offset_count.records = vec![Record::default(); 1];

    let mut empty = plain_batch();
    empty.records.clear();

    let mut invalid_sequence = plain_batch();
    invalid_sequence.producer_id = 7;
    invalid_sequence.producer_epoch = 0;
    invalid_sequence.base_sequence = -1;

    for (name, batch) in [
        ("invalid offset range", invalid_offset_range),
        ("inconsistent count", inconsistent_count),
        ("overflowing offset count", overflowing_offset_count),
        ("empty batch", empty),
        ("invalid producer sequence", invalid_sequence),
    ] {
        let payloads = [
            PartitionPayload::Slice(encode(&batch)),
            PartitionPayload::Owned(RecordsPayload::V2(vec![batch])),
        ];
        for payload in payloads {
            let err = prepare_for_topic!(payload, None, &crate::metrics::BrokerMetrics::new(), 13)
                .unwrap_err();
            assert!(err == crate::codes::INVALID_RECORD, "case: {name}");
        }
    }
}

/// Kafka answers a records field it cannot frame, or whose batch CRC does not
/// match, with `CORRUPT_MESSAGE` (`CorruptRecordException`, which the JVM
/// producer retries), and every other refusal with `INVALID_RECORD`.
/// `ProduceRequest.validateRecords` frames the field before the log checks a
/// CRC, so a second batch is `INVALID_RECORD` whatever the first one's CRC.
#[test]
fn an_undecodable_slice_gets_the_code_kafka_gives_it() {
    use crate::codes::{CORRUPT_MESSAGE, INVALID_RECORD};

    struct Case {
        name: &'static str,
        wire: Vec<u8>,
        error_code: i16,
    }
    let clean = encode(&plain_batch()).to_vec();
    let with_size = |size: i32| {
        let mut wire = clean.clone();
        wire[8..12].copy_from_slice(&size.to_be_bytes());
        wire
    };
    let with_magic = |magic: u8| {
        let mut wire = clean.clone();
        wire[16] = magic;
        wire
    };
    let mut corrupt_crc = clean.clone();
    corrupt_crc[HEADER_LEN] ^= 0xFF;
    let followed_by_a_batch = |first: &[u8]| [first, &clean].concat();
    let cases = [
        Case {
            name: "a CRC that does not match",
            wire: corrupt_crc.clone(),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a CRC that does not match, then a second batch",
            wire: followed_by_a_batch(&corrupt_crc),
            error_code: INVALID_RECORD,
        },
        Case {
            name: "two batches that check out",
            wire: followed_by_a_batch(&clean),
            error_code: INVALID_RECORD,
        },
        Case {
            name: "a size field below any batch",
            wire: with_size(13),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a negative size field",
            wire: with_size(-1),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a batch shorter than a v2 header",
            wire: with_size(20)[..32].to_vec(),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a magic above the current one",
            wire: with_magic(3),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a magic with the sign bit set",
            wire: with_magic(0xFF),
            error_code: CORRUPT_MESSAGE,
        },
        Case {
            name: "a body that stops short of its size field",
            wire: clean[..clean.len() - 1].to_vec(),
            error_code: INVALID_RECORD,
        },
        Case {
            name: "less than a size field",
            wire: clean[..11].to_vec(),
            error_code: INVALID_RECORD,
        },
    ];
    for case in cases {
        let error_code = prepare_for_topic!(
            PartitionPayload::Slice(Bytes::from(case.wire)),
            None,
            &crate::metrics::BrokerMetrics::new(),
            13
        )
        .unwrap_err();
        assert!(error_code == case.error_code, "{}", case.name);
    }
}

#[test]
fn rejects_crc_valid_malformed_record_body() {
    let mut wire = encode(&plain_batch()).to_vec();
    wire[HEADER_LEN] = 0; // zero-length first record body
    refresh_batch_crc(&mut wire);

    let error = prepare_for_topic!(
        PartitionPayload::Slice(Bytes::from(wire)),
        None,
        &crate::metrics::BrokerMetrics::new(),
        13
    )
    .unwrap_err();
    assert!(error == crate::codes::INVALID_RECORD);
}

#[test]
fn fallback_on_multiple_batches_in_slice() {
    // Kafka v2 records fields contain exactly one batch. A second
    // batch is invalid and must never be silently discarded.
    let b = plain_batch();
    let mut two = BytesMut::new();
    b.encode(&mut two).unwrap();
    b.encode(&mut two).unwrap();
    let err = prepare_for_topic!(
        PartitionPayload::Slice(two.freeze()),
        None,
        &crate::metrics::BrokerMetrics::new(),
        13
    )
    .unwrap_err();
    assert!(err == crate::codes::INVALID_RECORD);
}

#[test]
fn transactional_batch_can_pass_through() {
    let mut b = plain_batch();
    b.producer_id = 100;
    b.producer_epoch = 0;
    b.base_sequence = 0;
    b.attributes = b.attributes.with_transactional(true);
    let wire = encode(&b);
    let data = dispatch_slice(wire, None, 0);
    match data {
        ProduceData::Verbatim(v) => {
            assert!(v.is_transactional);
            assert!(v.producer_id == krabka_log::ProducerId(100));
        }
        ProduceData::Owned(_) => panic!("transactional data batch should pass through"),
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

/// A producer-LZ4-compressed batch stays verbatim after structural
/// validation, even when its decompressed form is 100 KiB and its
/// compressed wire bytes are tiny.
///
/// The stored `Verbatim.bytes` equal the compressed wire bytes, which
/// are much smaller than the decompressed payload. The header fields
/// `last_offset_delta` and `max_timestamp` come straight from the v2
/// header. This test pins the no-reencoding guarantee.
#[test]
fn lz4_batch_passes_through_without_reencoding() {
    // 100 KiB of highly-compressible payload across many records.
    let big = vec![b'A'; 100 * 1024];
    let mut b = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 7_777,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    b.records.push(Record {
        value: Some(Bytes::from(big.clone())),
        ..Default::default()
    });
    let wire = encode(&b);
    // The compressed wire bytes must be far smaller than the raw payload,
    // so an accidental re-encode to an uncompressed batch is obvious.
    assert!(
        wire.len() < big.len() / 4,
        "lz4 wire ({} B) should be much smaller than raw ({} B)",
        wire.len(),
        big.len()
    );

    let data = dispatch_slice(wire.clone(), None, 3);
    match data {
        ProduceData::Verbatim(v) => {
            // Stored bytes are the COMPRESSED wire bytes — verbatim, not
            // re-encoded from decompressed records ("must stay compressed").
            // Header fields came from the v2 header, no record decode.
            check!(&v.bytes[..] == &wire[..]);
            check!(v.bytes.len() == wire.len());
            check!(v.bytes.len() < big.len());
            check!(v.max_timestamp == 7_777);
            check!(v.last_offset_delta == 0);
            check!(v.leader_epoch == 3);
        }
        ProduceData::Owned(_) => {
            panic!("lz4 producer batch must pass through verbatim")
        }
        ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
            panic!("expected producer data")
        }
    }
}

/// HEADER fields drive the idempotent dedup over the verbatim path.
///
/// `prepare_batch` exposes `producer_id`, `producer_epoch`,
/// `base_sequence`, and `last_offset_delta`. It reads them from the v2
/// header without materializing owned records. The values match what
/// an owned decode of the same bytes would give.
#[test]
fn header_fields_drive_dedup_on_verbatim_path() {
    let mut b = plain_batch();
    b.producer_id = 4242;
    b.producer_epoch = 9;
    b.base_sequence = 17;
    b.last_offset_delta = 2;
    b.max_timestamp = 555;
    b.records.extend([
        Record {
            value: Some(Bytes::from_static(b"second")),
            ..Default::default()
        },
        Record {
            value: Some(Bytes::from_static(b"third")),
            ..Default::default()
        },
    ]);
    // Force lz4 so validation must decompress while the append still
    // retains the producer's exact compressed bytes.
    b.attributes = b.attributes.with_compression(CompressionType::Lz4);
    let wire = encode(&b);

    let m = crate::metrics::BrokerMetrics::new();
    let prepared = prepare_for_topic!(PartitionPayload::Slice(wire.clone()), None, &m, 13).unwrap();
    assert!(matches!(prepared.source, PreparedSource::Verbatim(_)));
    check!(prepared.producer_id == 4242);
    check!(prepared.producer_epoch == 9);
    check!(prepared.base_sequence == 17);
    check!(prepared.last_offset_delta == 2);
    check!(prepared.max_timestamp == 555);

    // Cross-check: an owned decode of the same compressed bytes yields
    // the same header identity (proving the header read is correct).
    let mut cur: &[u8] = &wire;
    let owned = RecordBatch::decode(&mut cur).unwrap();
    check!(owned.producer_id == prepared.producer_id);
    check!(owned.producer_epoch == prepared.producer_epoch);
    check!(owned.base_sequence == prepared.base_sequence);
    check!(owned.last_offset_delta == prepared.last_offset_delta);
}
