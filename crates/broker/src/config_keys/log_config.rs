//! The applier that merges a topic's validated overrides onto a `LogConfig`
//! for `Log::set_config`.

use {
    super::{
        CLEANUP_POLICY, COMPRESSION_GZIP_LEVEL, COMPRESSION_LZ4_LEVEL, COMPRESSION_TYPE,
        COMPRESSION_ZSTD_LEVEL, DELETE_RETENTION_MS, FILE_DELETE_DELAY_MS, FLUSH_MESSAGES,
        FLUSH_MS, INDEX_INTERVAL_BYTES, INTERNAL_SEGMENT_BYTES, LOCAL_RETENTION_BYTES,
        LOCAL_RETENTION_MS, MAX_COMPACTION_LAG_MS, MAX_MESSAGE_BYTES, MESSAGE_TIMESTAMP_TYPE,
        MESSAGE_TIMESTAMP_TYPE_LOG_APPEND, MIN_CLEANABLE_DIRTY_RATIO, MIN_COMPACTION_LAG_MS,
        PREALLOCATE, REMOTE_LOG_COPY_DISABLE, REMOTE_LOG_DELETE_ON_DISABLE, REMOTE_STORAGE_ENABLE,
        RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES, SEGMENT_INDEX_BYTES, SEGMENT_JITTER_MS,
        SEGMENT_MS,
        delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, DELIVERY_SCHEDULE_MONOTONIC},
        parse::{self, bool_value, int_value, long_value},
        validation::{parse_cleanup_policy, parse_compression_type},
    },
    krabka_log::LogConfig,
    krabka_units::{
        ByteSize, Time,
        convert::{
            ByteSizeExt as _, TimeExt as _,
            wire::{opt_size_from_bytes_i64, opt_time_from_millis_i64},
        },
    },
    std::collections::BTreeMap,
};

/// Merge `overrides` over `base` and return a fresh `LogConfig` to push
/// into `Log::set_config`. This function drops unknown keys silently.
/// `validate_topic_config` should have rejected them at `AlterConfigs` time,
/// before the record reached the metadata image. This function is the
/// applier and treats the input as already-validated.
#[must_use]
pub(crate) fn apply_to_log_config(
    overrides: &BTreeMap<String, String>,
    base: &LogConfig,
) -> LogConfig {
    let mut out = base.clone();
    for (k, v) in overrides {
        match k.as_str() {
            RETENTION_MS => {
                if let Some(ms) = long_value(v) {
                    out.retention = opt_time_from_millis_i64(ms);
                }
            }
            RETENTION_BYTES => {
                if let Some(b) = long_value(v) {
                    out.retention_size = opt_size_from_bytes_i64(b);
                }
            }
            LOCAL_RETENTION_MS => {
                if let Some(ms) = long_value(v) {
                    // -2 (inherit) and -1 (unlimited)
                    // both collapse to `None` — the greenfield simplification noted
                    // in the spec. >=0 maps to `Some(Time)`.
                    out.local_retention = opt_time_from_millis_i64(ms);
                }
            }
            LOCAL_RETENTION_BYTES => {
                if let Some(b) = long_value(v) {
                    out.local_retention_size = opt_size_from_bytes_i64(b);
                }
            }
            SEGMENT_BYTES => {
                if let Some(b) = long_value(v).and_then(|b| u64::try_from(b).ok()) {
                    out.segment_size = ByteSize::from_bytes(b);
                }
            }
            MAX_MESSAGE_BYTES => {
                if let Some(b) = int_value(v)
                    && let Ok(b) = u64::try_from(b)
                {
                    out.max_message_size = ByteSize::from_bytes(b);
                }
            }
            CLEANUP_POLICY => {
                if let Ok(policy) = parse_cleanup_policy(v) {
                    out.cleanup_policy = policy;
                }
            }
            SEGMENT_MS => {
                if let Some(ms) = long_value(v)
                    && ms >= 1
                {
                    out.segment_roll_interval = Time::from_millis(ms);
                }
            }
            INDEX_INTERVAL_BYTES => {
                if let Some(b) = int_value(v)
                    && let Ok(b) = u64::try_from(b)
                {
                    out.index_interval = ByteSize::from_bytes(b);
                }
            }
            SEGMENT_INDEX_BYTES => {
                if let Some(b) = int_value(v)
                    && let Ok(b) = u64::try_from(b)
                {
                    out.segment_index_size = ByteSize::from_bytes(b);
                }
            }
            MIN_COMPACTION_LAG_MS => {
                if let Some(ms) = long_value(v)
                    && ms >= 0
                {
                    out.min_compaction_lag = Time::from_millis(ms);
                }
            }
            MAX_COMPACTION_LAG_MS => {
                if let Some(ms) = long_value(v)
                    && ms >= 1
                {
                    // Kafka's default is `Long.MAX_VALUE`, which is how an
                    // operator spells "no bound"; the log carries that as
                    // `None` rather than as a Time no clock can reach.
                    out.max_compaction_lag = (ms != i64::MAX).then(|| Time::from_millis(ms));
                }
            }
            MIN_CLEANABLE_DIRTY_RATIO => {
                if let Ok(ratio) = super::validation::parse_dirty_ratio(v) {
                    out.min_cleanable_dirty_ratio = ratio;
                }
            }
            MESSAGE_TIMESTAMP_TYPE => {
                out.message_timestamp_type =
                    if parse::java_trim(v) == MESSAGE_TIMESTAMP_TYPE_LOG_APPEND {
                        krabka_protocol::records::TimestampType::LogAppendTime
                    } else {
                        krabka_protocol::records::TimestampType::CreateTime
                    };
            }
            SEGMENT_JITTER_MS | FILE_DELETE_DELAY_MS | FLUSH_MS => {
                if let Some(ms) = long_value(v).filter(|ms| *ms >= 0) {
                    match k.as_str() {
                        SEGMENT_JITTER_MS => out.segment_jitter = Time::from_millis(ms),
                        FILE_DELETE_DELAY_MS => out.file_delete_delay = Time::from_millis(ms),
                        _ => out.flush_interval = (ms != i64::MAX).then(|| Time::from_millis(ms)),
                    }
                }
            }
            FLUSH_MESSAGES => {
                if let Some(messages) = long_value(v).filter(|messages| *messages >= 1) {
                    out.flush_messages =
                        (messages != i64::MAX).then(|| u64::try_from(messages).unwrap());
                }
            }
            COMPRESSION_GZIP_LEVEL | COMPRESSION_LZ4_LEVEL | COMPRESSION_ZSTD_LEVEL => {
                if let Some(level) = int_value(v) {
                    match k.as_str() {
                        COMPRESSION_GZIP_LEVEL => out.compression_gzip_level = level,
                        COMPRESSION_LZ4_LEVEL => out.compression_lz4_level = level,
                        _ => out.compression_zstd_level = level,
                    }
                }
            }
            COMPRESSION_TYPE => {
                if let Ok(target) = parse_compression_type(v) {
                    out.compression_type = target;
                }
            }
            REMOTE_STORAGE_ENABLE => {
                out.remote_storage_enable = bool_value(v) == Some(true);
            }
            REMOTE_LOG_COPY_DISABLE => {
                out.remote_tier.copy_disable = bool_value(v) == Some(true);
            }
            REMOTE_LOG_DELETE_ON_DISABLE => {
                out.remote_tier.delete_on_disable = bool_value(v) == Some(true);
            }
            PREALLOCATE => {
                if let Some(preallocate) = bool_value(v) {
                    out.segment_allocation = if preallocate {
                        krabka_log::SegmentAllocation::Preallocate
                    } else {
                        krabka_log::SegmentAllocation::OnWrite
                    };
                }
            }
            DELIVERY_MODE => {
                out.delivery_policy = if parse::java_trim(v) == DELIVERY_MODE_SCHEDULED {
                    krabka_log::DeliveryPolicy::Scheduled
                } else {
                    krabka_log::DeliveryPolicy::Immediate
                };
            }
            DELIVERY_SCHEDULE_MONOTONIC => {
                // The log enforces this one, under the same lock acquisition
                // that writes the batch, so it travels with `delivery.mode`
                // into `Log.config` rather than being resolved per produce.
                out.schedule_order = if bool_value(v) == Some(true) {
                    krabka_log::ScheduleOrder::Monotonic
                } else {
                    krabka_log::ScheduleOrder::Unordered
                };
            }
            DELETE_RETENTION_MS => {
                if let Some(ms) = long_value(v)
                    && ms >= 0
                {
                    out.delete_retention = Time::from_millis(ms);
                }
            }
            // The remaining keys are recognized but no broker behavior is
            // wired to them yet (see module docs).
            _ => {}
        }
    }
    // Kafka's `LogConfig.segmentSize()`: `internal.segment.bytes ?? segment.bytes`.
    // The internal key carries no floor, which is how Kafka's own coordinators
    // get a sub-mebibyte segment on the topics they create, so it is resolved
    // last and wins over whatever `segment.bytes` left behind.
    if let Some(b) = overrides
        .get(INTERNAL_SEGMENT_BYTES)
        .and_then(|v| int_value(v))
        .and_then(|b| u64::try_from(b).ok())
    {
        out.segment_size = ByteSize::from_bytes(b);
    }
    out
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{bytes, mebibytes, millis, minutes},
    };

    #[test]
    fn apply_compression_type_zstd_propagates() {
        use krabka_compression::CompressionType;
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "zstd".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.compression_type == Some(CompressionType::Zstd));
    }

    #[test]
    fn apply_compression_type_producer_resets_to_none() {
        use krabka_compression::CompressionType;
        let base = LogConfig {
            compression_type: Some(CompressionType::Lz4),
            ..LogConfig::default()
        };
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "producer".into());
        let out = apply_to_log_config(&o, &base);
        assert!(out.compression_type == None);
    }

    #[test]
    fn apply_remote_storage_enable_propagates() {
        let mut o = BTreeMap::new();
        o.insert(REMOTE_STORAGE_ENABLE.into(), "true".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.remote_storage_enable);

        let mut off = BTreeMap::new();
        off.insert(REMOTE_STORAGE_ENABLE.into(), "false".into());
        let base = LogConfig {
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&off, &base);
        assert!(!out.remote_storage_enable);
    }

    #[test]
    fn apply_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "60000".into());
        let base = LogConfig::default();
        let out = apply_to_log_config(&o, &base);
        assert!(out.retention == Some(minutes(1)));
    }

    #[test]
    fn apply_retention_ms_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == None);
    }

    #[test]
    fn apply_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == Some(millis(0)));
    }

    #[test]
    fn apply_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_retention_bytes_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == None);
    }

    #[test]
    fn apply_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(bytes(0)));
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == mebibytes(1));
    }

    #[test]
    fn apply_preallocate_propagates() {
        // A corrupt value (`yes`) leaves the base alone, as its neighbours do.
        use krabka_log::SegmentAllocation::{OnWrite, Preallocate};
        for (value, base_allocation, expected) in [
            ("true", OnWrite, Preallocate),
            ("TRUE", OnWrite, Preallocate),
            ("false", Preallocate, OnWrite),
            ("yes", Preallocate, Preallocate),
            ("yes", OnWrite, OnWrite),
        ] {
            let mut o = BTreeMap::new();
            o.insert(PREALLOCATE.into(), value.into());
            let base = LogConfig {
                segment_allocation: base_allocation,
                ..LogConfig::default()
            };
            let out = apply_to_log_config(&o, &base);
            assert!(
                out == LogConfig {
                    segment_allocation: expected,
                    ..LogConfig::default()
                },
                "preallocate={value} over base {base_allocation:?}"
            );
        }
    }

    #[test]
    fn apply_max_message_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(MAX_MESSAGE_BYTES.into(), "2048".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.max_message_size == bytes(2048));
    }

    #[derive(Clone, Copy)]
    enum SizeField {
        Message,
        Segment,
    }

    #[yare::parameterized(
        max_message_bytes = { MAX_MESSAGE_BYTES, SizeField::Message },
        internal_segment_bytes = { INTERNAL_SEGMENT_BYTES, SizeField::Segment },
    )]
    fn corrupt_size_values_leave_the_base_alone(key: &str, field: SizeField) {
        use assert2::assert;

        let base = LogConfig {
            max_message_size: bytes(4096),
            segment_size: bytes(8192),
            ..LogConfig::default()
        };
        for corrupt in ["-1", "not-a-number", "2147483648"] {
            let mut o = BTreeMap::new();
            o.insert(key.into(), corrupt.into());
            let out = apply_to_log_config(&o, &base);
            let retained = match field {
                SizeField::Message => out.max_message_size == base.max_message_size,
                SizeField::Segment => out.segment_size == base.segment_size,
            };
            assert!(retained, "{key}={corrupt}");
        }
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention: Some(millis(12_345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert!(out.retention == base.retention);
    }

    #[test]
    fn apply_cleanup_policy_compact_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "compact".to_string());
        let out = apply_to_log_config(&overrides, &krabka_log::LogConfig::default());
        assert!(out.cleanup_policy == krabka_log::CleanupPolicy::Compact);
    }

    #[test]
    fn apply_cleanup_policy_compact_and_delete_propagates() {
        for value in ["compact,delete", "delete,compact"] {
            let mut overrides = BTreeMap::new();
            overrides.insert(CLEANUP_POLICY.to_string(), value.to_string());
            let out = apply_to_log_config(&overrides, &LogConfig::default());
            assert!(
                out.cleanup_policy == krabka_log::CleanupPolicy::CompactAndDelete,
                "cleanup.policy={value}"
            );
        }
    }

    /// The keys Kafka's `TopicConfig` carries that krabka now maps onto the
    /// log a partition runs with.
    #[test]
    fn apply_maps_the_remaining_kafka_keys_onto_the_log_config() {
        let overrides = maplit::btreemap! {
        SEGMENT_MS.to_string() => "60000".to_string(),
        INDEX_INTERVAL_BYTES.to_string() => "8192".to_string(),
        SEGMENT_INDEX_BYTES.to_string() => "1048576".to_string(),
        MIN_COMPACTION_LAG_MS.to_string() => "30000".to_string(),
        MAX_COMPACTION_LAG_MS.to_string() => "120000".to_string(),
        MIN_CLEANABLE_DIRTY_RATIO.to_string() => "0.25".to_string(),
        MESSAGE_TIMESTAMP_TYPE.to_string() => "LogAppendTime".to_string()};

        let out = apply_to_log_config(&overrides, &LogConfig::default());

        assert!(
            out == LogConfig {
                segment_roll_interval: minutes(1),
                index_interval: bytes(8192),
                segment_index_size: bytes(1_048_576),
                min_compaction_lag: millis(30_000),
                max_compaction_lag: Some(millis(120_000)),
                min_cleanable_dirty_ratio: krabka_units::fraction(0.25),
                message_timestamp_type: krabka_protocol::records::TimestampType::LogAppendTime,
                ..LogConfig::default()
            }
        );
    }

    /// Kafka's `max.compaction.lag.ms` default is `Long.MAX_VALUE`, which is
    /// how an operator spells "no bound".
    #[test]
    fn apply_max_compaction_lag_long_max_means_unbounded() {
        let base = LogConfig {
            max_compaction_lag: Some(millis(1000)),
            ..LogConfig::default()
        };
        let mut o = BTreeMap::new();
        o.insert(MAX_COMPACTION_LAG_MS.into(), "9223372036854775807".into());
        let out = apply_to_log_config(&o, &base);
        assert!(out.max_compaction_lag == None);
    }

    #[test]
    fn apply_cleanup_policy_delete_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "delete".to_string());
        let base = krabka_log::LogConfig {
            cleanup_policy: krabka_log::CleanupPolicy::Compact,
            ..krabka_log::LogConfig::default()
        };
        let out = apply_to_log_config(&overrides, &base);
        assert!(out.cleanup_policy == krabka_log::CleanupPolicy::Delete);
    }

    #[test]
    fn apply_local_retention_ms_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == None);

        let mut unlimited = BTreeMap::new();
        unlimited.insert(LOCAL_RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&unlimited, &LogConfig::default());
        assert!(out.local_retention == None);
    }

    #[test]
    fn apply_local_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(millis(0)));
    }

    #[test]
    fn apply_local_retention_ms_positive_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "60000".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(minutes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == None);
    }

    #[test]
    fn apply_local_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(bytes(0)));
    }

    #[test]
    fn apply_delete_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(DELETE_RETENTION_MS.into(), "12345".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.delete_retention == millis(12_345));
    }

    /// Kafka's `LogConfig.segmentSize()` returns `internal.segment.bytes` when
    /// it is set, whatever `segment.bytes` says.
    #[test]
    fn apply_internal_segment_bytes_wins_over_segment_bytes() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "2097152".into());
        o.insert(INTERNAL_SEGMENT_BYTES.into(), "4096".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == bytes(4096));
    }

    /// With the internal key unset, the public key is the segment size.
    #[test]
    fn apply_segment_bytes_wins_when_internal_is_unset() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "2097152".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == mebibytes(2));
    }

    /// The one-mebibyte floor belongs to `segment.bytes` alone;
    /// `defineInternal` gives the internal key no validator, and a coordinator
    /// setting it below the floor gets the small segment it asked for.
    #[test]
    fn apply_internal_segment_bytes_below_the_public_floor_is_effective() {
        let mut o = BTreeMap::new();
        o.insert(INTERNAL_SEGMENT_BYTES.into(), "1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == bytes(1));
    }
}

#[cfg(test)]
mod active_options_tests {
    use super::*;

    #[test]
    fn options_override_the_log_and_deleting_overrides_restores_the_base() {
        let overrides = maplit::btreemap! {
            COMPRESSION_GZIP_LEVEL.to_owned() => "1".to_owned(),
            COMPRESSION_LZ4_LEVEL.to_owned() => "17".to_owned(),
            COMPRESSION_ZSTD_LEVEL.to_owned() => "-5".to_owned(),
            SEGMENT_JITTER_MS.to_owned() => "5000".to_owned(),
            FILE_DELETE_DELAY_MS.to_owned() => "1000".to_owned(),
            FLUSH_MESSAGES.to_owned() => "10".to_owned(),
            FLUSH_MS.to_owned() => "50".to_owned(),
        };
        let base = LogConfig::default();
        let applied = apply_to_log_config(&overrides, &base);
        assert2::assert!(
            applied
                == LogConfig {
                    compression_gzip_level: 1,
                    compression_lz4_level: 17,
                    compression_zstd_level: -5,
                    segment_jitter: Time::from_millis(5000),
                    file_delete_delay: Time::from_millis(1000),
                    flush_messages: Some(10),
                    flush_interval: Some(Time::from_millis(50)),
                    ..base.clone()
                }
        );
        assert2::assert!(apply_to_log_config(&BTreeMap::new(), &base) == base);
        let disabled = maplit::btreemap! {
            FLUSH_MESSAGES.to_owned() => i64::MAX.to_string(),
            FLUSH_MS.to_owned() => i64::MAX.to_string(),
        };
        let applied = apply_to_log_config(&disabled, &applied);
        assert2::assert!(applied.flush_messages == None);
        assert2::assert!(applied.flush_interval == None);
    }
}
