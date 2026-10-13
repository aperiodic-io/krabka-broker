//! `message.timestamp.before.max.ms` and `message.timestamp.after.max.ms`
//! applied to a produced batch, on both the verbatim and the owned path.
//!
//! Every case resolves its policy through
//! [`resolve_timestamp_policy`](crate::handlers::produce::topic_settings), the
//! function the produce handler resolves it with, so a case fails if the
//! config key stops reaching the gate as much as if the gate stops applying.

use {
    crate::{
        codes,
        config_keys::{
            DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE,
        },
        handlers::produce::{
            framing::PartitionPayload,
            prepare::{DecodeEnv, prepare_batch},
            test_support::{ConfigOverride, encode_batch, image_with_topic},
            topic_settings::{
                TimestampPolicy, kafka_stock_broker_default, resolve_timestamp_policy,
            },
        },
        metrics::BrokerMetrics,
    },
    assert2::check,
    bytes::Bytes,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_metadata::{MetadataRecord, TopicConfigRecord},
    krabka_protocol::records::{Record, RecordBatch},
    std::{collections::BTreeMap, sync::Arc},
};

/// An hour, the window every case configures.
const WINDOW_MS: i64 = 3_600_000;

// This broker's wall clock, which is the one the gate reads.
krabka_macros::unix_millis_fixture!(
    now_ms,
    "the clock is at or after the epoch",
    "a millisecond clock reading fits in i64"
);

/// The policy a topic carrying `overrides` resolves to.
fn policy(overrides: &[ConfigOverride<'_>]) -> TimestampPolicy {
    let mut img = image_with_topic("t", &[1]);
    let mut map = BTreeMap::new();
    for ConfigOverride { key, value } in overrides {
        map.insert((*key).to_string(), (*value).to_string());
    }
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "t".into(),
        overrides: map,
    }));
    resolve_timestamp_policy(&img, "t", kafka_stock_broker_default())
}

/// A two-record batch whose records carry `first_ms` and `second_ms`, stored
/// the way the v2 format stores them: one `base_timestamp` plus a delta each.
fn batch(first_ms: i64, second_ms: i64, compression: CompressionType) -> RecordBatch {
    RecordBatch {
        attributes: krabka_protocol::records::Attributes::default().with_compression(compression),
        last_offset_delta: 1,
        base_timestamp: first_ms,
        max_timestamp: first_ms.max(second_ms),
        records: vec![
            Record {
                offset_delta: 0,
                timestamp_delta: 0,
                value: Some(Bytes::from_static(b"a")),
                ..Record::default()
            },
            Record {
                offset_delta: 1,
                timestamp_delta: second_ms - first_ms,
                value: Some(Bytes::from_static(b"b")),
                ..Record::default()
            },
        ],
        ..RecordBatch::default()
    }
}

/// Run one batch through the real prepare decision under `timestamps`.
///
/// `topic_compression` is what decides the append shape: `None` is producer
/// pass-through, which keeps the batch verbatim, and a codec that differs from
/// the batch's own forces the owned fallback.
fn prepare(
    batch: &RecordBatch,
    timestamps: TimestampPolicy,
    topic_compression: Option<CompressionType>,
) -> Result<(), i16> {
    let metrics = BrokerMetrics::new();
    let prepared = prepare_batch(
        PartitionPayload::Slice(encode_batch(batch)),
        topic_compression,
        timestamps,
        false,
        DecodeEnv {
            topic_name: &Arc::from("t"),
            metrics: &metrics,
            policy: RecordDecompressionPolicy::default(),
        },
        13,
    )?;
    // `prepare_batch` no longer refuses a batch outright over a timestamp
    // violation -- it defers to the pipeline, the same as a keyless record on
    // a compacted topic -- so these cases fold that back into the refusal
    // they test for.
    if prepared.invalid_timestamp_records.is_empty() {
        Ok(())
    } else {
        Err(codes::INVALID_TIMESTAMP)
    }
}

/// The window refuses a record outside it and admits one inside it, and it
/// does so on both append shapes: the same batch, the same policy, and the
/// same verdict whether the bytes pass through or are decoded.
#[test]
fn the_window_refuses_the_same_batch_on_both_append_paths() {
    let now = now_ms();
    let cases = [
        (
            "both records inside the window",
            [now - 1_000, now + 1_000],
            Ok(()),
        ),
        (
            "the first record is older than before.max.ms",
            [now - 2 * WINDOW_MS, now],
            Err(codes::INVALID_TIMESTAMP),
        ),
        (
            "the second record is older than before.max.ms",
            [now, now - 2 * WINDOW_MS],
            Err(codes::INVALID_TIMESTAMP),
        ),
        (
            "the second record is further ahead than after.max.ms",
            [now, now + 2 * WINDOW_MS],
            Err(codes::INVALID_TIMESTAMP),
        ),
    ];
    let bounded = policy(&[
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
            value: &WINDOW_MS.to_string(),
        },
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            value: &WINDOW_MS.to_string(),
        },
    ]);
    for (label, [first, second], want) in cases {
        let produced = batch(first, second, CompressionType::None);
        check!(
            prepare(&produced, bounded, None) == want,
            "verbatim: {label}"
        );
        check!(
            prepare(&produced, bounded, Some(CompressionType::Zstd)) == want,
            "owned: {label}"
        );
    }
}

/// The policy that bounds nothing admits a timestamp from any era. It is what
/// a KFC-1 scheduled topic and the benchmark seam resolve to, and it is not
/// what a stock topic resolves to -- see
/// [`a_stock_topic_carries_kafkas_one_hour_future_window`].
#[test]
fn the_unbounded_policy_admits_every_timestamp() {
    let unbounded = TimestampPolicy::default();
    let ancient = batch(1_000, 2_000, CompressionType::None);
    let distant = batch(i64::MAX / 2, i64::MAX / 2, CompressionType::None);

    check!(prepare(&ancient, unbounded, None) == Ok(()));
    check!(prepare(&distant, unbounded, None) == Ok(()));
    check!(prepare(&ancient, unbounded, Some(CompressionType::Zstd)) == Ok(()));
}

/// Kafka 4.3.1 defaults `message.timestamp.after.max.ms` to one hour and
/// `message.timestamp.before.max.ms` to `Long.MAX_VALUE`, so a topic that
/// configured nothing refuses a record stamped more than an hour ahead of the
/// broker's clock and accepts one from any point in the past. A topic that did
/// not opt into KFC-1 has to behave exactly that way.
#[test]
fn a_stock_topic_carries_kafkas_one_hour_future_window() {
    let now = now_ms();
    let stock = policy(&[]);
    let cases = [
        (
            "a record from the distant past is admitted",
            [1_000, 2_000],
            Ok(()),
        ),
        (
            "a record at the future bound is admitted",
            [now, now + WINDOW_MS],
            Ok(()),
        ),
        (
            "a record past the future bound is refused",
            [now, now + WINDOW_MS + 60_000],
            Err(codes::INVALID_TIMESTAMP),
        ),
    ];
    for (label, [first, second], want) in cases {
        let produced = batch(first, second, CompressionType::None);
        check!(prepare(&produced, stock, None) == want, "verbatim: {label}");
        check!(
            prepare(&produced, stock, Some(CompressionType::Zstd)) == want,
            "owned: {label}"
        );
    }
}

/// KFC-1: a scheduled topic's timestamps are delivery times, and the default
/// `delivery.max.delay.ms` is seven days. Kafka's one-hour future window would
/// refuse such a topic's own writes, so a scheduled topic carries no window at
/// all and `delivery.max.delay.ms` is the bound on how far ahead a batch may
/// be scheduled.
#[test]
fn a_scheduled_topic_admits_a_delivery_time_days_ahead() {
    let now = now_ms();
    let scheduled = policy(&[ConfigOverride {
        key: DELIVERY_MODE,
        value: DELIVERY_MODE_SCHEDULED,
    }]);
    let due_in_three_days = batch(
        now + 3 * 24 * 3_600_000,
        now + 3 * 24 * 3_600_000,
        CompressionType::None,
    );

    check!(prepare(&due_in_three_days, scheduled, None) == Ok(()));
    check!(prepare(&due_in_three_days, scheduled, Some(CompressionType::Zstd)) == Ok(()));

    // The same batch on a topic that did not opt in is Kafka's refusal.
    check!(prepare(&due_in_three_days, policy(&[]), None) == Err(codes::INVALID_TIMESTAMP));
}

/// A `LogAppendTime` topic ignores the window, because the append overwrites
/// every producer timestamp anyway. Kafka's `validateTimestamp` tests a
/// record's timestamp only under `CreateTime`, for the same reason.
#[test]
fn log_append_time_ignores_the_window() {
    let stamping = policy(&[
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_TYPE,
            value: "LogAppendTime",
        },
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
            value: &WINDOW_MS.to_string(),
        },
        ConfigOverride {
            key: MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            value: &WINDOW_MS.to_string(),
        },
    ]);
    let ancient = batch(1_000, 2_000, CompressionType::None);

    check!(prepare(&ancient, stamping, None) == Ok(()));
    check!(prepare(&ancient, stamping, Some(CompressionType::Zstd)) == Ok(()));
}

/// A compressed batch keeps its verbatim bytes, and the window still reads the
/// timestamps inside the compressed body rather than admitting it unchecked.
#[test]
fn the_window_reads_a_compressed_body_on_the_verbatim_path() {
    let now = now_ms();
    let bounded = policy(&[ConfigOverride {
        key: MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
        value: &WINDOW_MS.to_string(),
    }]);
    let refused = batch(now, now - 2 * WINDOW_MS, CompressionType::Lz4);
    let admitted = batch(now, now - 1_000, CompressionType::Lz4);

    check!(prepare(&refused, bounded, None) == Err(codes::INVALID_TIMESTAMP));
    check!(prepare(&admitted, bounded, None) == Ok(()));
}
