//! The keyless-record walk of a compacted topic, on both the verbatim and the
//! owned path, and Kafka's rule that the key check comes before the timestamp
//! check of the same record.

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_compression::{CompressionType, RecordDecompressionPolicy};
use krabka_protocol::records::{Record, RecordBatch};

use crate::{
    handlers::produce::{
        framing::PartitionPayload,
        prepare::{DecodeEnv, prepare_batch},
        test_support::{encode_batch, image_with_topic},
        topic_settings::{kafka_stock_broker_default, resolve_timestamp_policy},
    },
    metrics::BrokerMetrics,
    time_util::now_ms,
};

/// A batch with one record per `(key, timestamp_ms)` pair.
fn batch(records: &[(Option<&'static [u8]>, i64)]) -> RecordBatch {
    let base_timestamp = records.first().map_or(0, |(_, ts)| *ts);
    RecordBatch {
        last_offset_delta: i32::try_from(records.len()).expect("small batch") - 1,
        base_timestamp,
        max_timestamp: records.iter().map(|(_, ts)| *ts).max().unwrap_or(0),
        records: records
            .iter()
            .zip(0..)
            .map(|((key, ts), offset_delta)| Record {
                offset_delta,
                timestamp_delta: ts - base_timestamp,
                key: key.map(Bytes::from_static),
                value: Some(Bytes::from_static(b"v")),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

#[test]
fn a_compacted_topic_names_every_keyless_record_on_both_paths() {
    // A stock topic bounds the future window at one hour.
    let timestamps = resolve_timestamp_policy(
        &image_with_topic("t", &[1]),
        "t",
        kafka_stock_broker_default(),
    );
    let now = now_ms();
    let far_future = now + 7_200_000;
    let mixed = batch(&[(None, now), (Some(b"k"), now), (None, now)]);
    // The only record with a bad timestamp has no key, so Kafka reports the
    // key error and never checks that timestamp.
    let keyless_future = batch(&[(None, far_future), (Some(b"k"), now)]);
    let keyed_future = batch(&[(None, now), (Some(b"k"), far_future)]);

    for topic_compression in [None, Some(CompressionType::Zstd)] {
        for (label, input, compacted, expected_keyless, expected_invalid_timestamp) in [
            ("mixed, compacted", &mixed, true, vec![0, 2], vec![]),
            ("mixed, not compacted", &mixed, false, vec![], vec![]),
            (
                "keyless record in the future",
                &keyless_future,
                true,
                vec![0],
                vec![],
            ),
            (
                // Record 0 is still keyless on this compacted topic, exactly
                // as in `keyless_future`; only its position changed. The
                // record with a bad timestamp (index 1) has a key, so Kafka
                // reports the timestamp error rather than a key error for
                // it, and the batch is no longer refused outright:
                // `prepare_batch` defers to the pipeline.
                "keyed record in the future",
                &keyed_future,
                true,
                vec![0],
                vec![1],
            ),
        ] {
            let metrics = BrokerMetrics::new();
            let prepared = prepare_batch(
                PartitionPayload::Slice(encode_batch(input)),
                topic_compression,
                timestamps,
                compacted,
                DecodeEnv {
                    topic_name: &Arc::from("t"),
                    metrics: &metrics,
                    policy: RecordDecompressionPolicy::default(),
                },
                13,
            )
            .unwrap_or_else(|code| panic!("{label} {topic_compression:?}: {code}"));
            check!(
                prepared.keyless_records == expected_keyless,
                "{label} {topic_compression:?}"
            );
            let invalid_timestamp_indices: Vec<i32> = prepared
                .invalid_timestamp_records
                .iter()
                .map(|error| error.batch_index)
                .collect();
            check!(
                invalid_timestamp_indices == expected_invalid_timestamp,
                "{label} {topic_compression:?}"
            );
        }
    }
}
