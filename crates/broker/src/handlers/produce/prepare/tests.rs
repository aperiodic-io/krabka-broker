//! Tests for the verbatim-versus-owned prepare decision.

use {
    super::*,
    crate::{
        dedup::DedupSettings, handlers::produce::test_support::encode_batch, metrics::BrokerMetrics,
    },
    assert2::assert,
    krabka_compression::CompressionType,
    krabka_protocol::records::Record,
    krabka_units::{bytes, fraction},
    std::sync::Arc,
};

krabka_macros::legacy_policy_fixture!(legacy_policy_wire, ::krabka_records_legacy);

/// The topic name these cases record under, as the shared handle the metric
/// label sets clone.
fn topic() -> Arc<str> { Arc::from("t") }

#[test]
fn record_decompression_policy_limits_owned_and_verbatim_produce() {
    let policy = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
    let metrics = BrokerMetrics::new();

    let v2 = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Lz4),
        records: vec![Record {
            value: Some(Bytes::from(vec![b'x'; 4096])),
            ..Default::default()
        }],
        ..Default::default()
    };
    let wire = encode_batch(&v2);
    let run_prepare = |compression, pol| {
        prepare_batch(
            PartitionPayload::Slice(wire.clone()),
            compression,
            TimestampPolicy::default(),
            false,
            DecodeEnv {
                topic_name: &topic(),
                metrics: &metrics,
                policy: pol,
                dedup: None,
            },
            13,
        )
    };
    assert!(run_prepare(None, policy).unwrap_err() == crate::codes::INVALID_RECORD);
    assert!(
        run_prepare(Some(CompressionType::Zstd), policy).unwrap_err()
            == crate::codes::INVALID_RECORD
    );
    assert!(
        run_prepare(
            Some(CompressionType::Zstd),
            RecordDecompressionPolicy::default()
        )
        .is_ok()
    );

    let legacy = legacy_policy_wire();
    let error = decode_owned_batch(
        RecordsPayload::Legacy(legacy.freeze()),
        &topic(),
        &metrics,
        policy,
    )
    .unwrap_err();
    assert!(error == crate::codes::INVALID_RECORD);
}

// ── verbatim passthrough predicate (prepare_batch + build_produce_data) ──
//
// These drive the zero-copy dispatch end to end: `prepare_batch` validates
// the v2 batch and decides verbatim-vs-owned; `build_produce_data` maps the
// result to the writer's `ProduceData`, stamping the leader epoch.
mod verbatim;

// ── the message.timestamp.{before,after}.max.ms window ───────────────────
//
// Kafka's `LogValidator.validateTimestamp` walks every record of a batch and
// fails the whole batch with `INVALID_TIMESTAMP` as soon as one record's
// timestamp falls outside the window around the broker's clock. These cases
// drive that through `prepare_batch` on both append shapes: the verbatim
// passthrough, which reads the timestamps out of the producer's own bytes, and
// the owned fallback, which reads them off the decoded batch.
mod timestamp_window;

// ── compacted topic: the key check ─────────────────────────────────────
//
// Kafka's `LogValidator.validateKey` runs before the timestamp check of the
// same record, on both append shapes.
mod keyless_records;

/// A dedup topic reads every record's identity during validation, and the
/// collector reserves room before any record is parsed. The record count it
/// would size that reservation from is the producer's claim: a 61-byte batch
/// with a valid CRC can declare `i32::MAX` records. The batch is refused for
/// the records it lacks, and nothing is reserved for the ones it claimed.
#[test]
fn a_dedup_topic_reserves_nothing_for_a_record_count_the_batch_does_not_hold() {
    let mut wire = encode_batch(&RecordBatch::default()).to_vec();
    wire[57..61].copy_from_slice(&i32::MAX.to_be_bytes());
    let crc = crc32c::crc32c(&wire[21..]);
    wire[17..21].copy_from_slice(&crc.to_be_bytes());
    let settings = DedupSettings {
        window_ms: 60_000,
        key_header: None,
    };

    let refused = prepare_batch(
        PartitionPayload::Slice(Bytes::from(wire)),
        None,
        TimestampPolicy::default(),
        false,
        DecodeEnv {
            topic_name: &topic(),
            metrics: &BrokerMetrics::new(),
            policy: RecordDecompressionPolicy::default(),
            dedup: Some(&settings),
        },
        13,
    );

    assert!(refused.unwrap_err() == crate::codes::INVALID_RECORD);
}
