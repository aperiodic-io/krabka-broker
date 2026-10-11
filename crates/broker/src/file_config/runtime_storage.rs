//! The `[runtime]` appliers for storage recovery, queue depths, and the
//! network and decompression limits.
//!
//! `apply_recovery_and_queues` covers the diskless WAL, unclean recovery,
//! share-session, and log sizing knobs; `apply_network_limits` covers the
//! socket buffers, ACL string ceilings, and the telemetry and record
//! decompression bounds that guard the broker against a hostile client.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        kafka_int_bytes, kafka_long_bytes, nonnegative_time, positive_i32, positive_i64,
        positive_ratio, positive_time, positive_u32, positive_usize, whole_bytes_u32,
        whole_bytes_u64, whole_bytes_usize,
    },
};
use crate::config::BrokerConfig;

impl RuntimeFileConfig {
    pub(super) fn apply_recovery_and_queues(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! {
            runtime => cfg;
            positive_time: unclean_recovery_aggressive_deadline,
                unclean_recovery_balanced_deadline, operator_recovery_deadline, quota_throttle_max,
                quota_window, controller_mutation_quota_window;
            positive_u32: self_registration_max_attempts;
            whole_bytes_u32: observer_fetch_max;
            positive_usize: audit_event_queue_capacity;
            positive_i64: audit_tail_window_offsets;
            whole_bytes_usize: audit_tail_read_max;
            positive_u32: client_metrics_stale_push_intervals;
            positive_usize: client_metrics_otlp_queue_capacity,
                coordinator_actor_mailbox_capacity, diskless_wal_local_replica_count;
            positive_time: diskless_wal_flush_interval;
            whole_bytes_usize: diskless_wal_flush_max_size, diskless_wal_hot_tail_max_size;
        }
        if let Some(value) = runtime.diskless_wal_trim_safety_lag {
            if value.is_negative() {
                return Err(FileConfigError::InvalidConfig(
                    "diskless_wal_trim_safety_lag must be nonnegative".into(),
                ));
            }
            cfg.diskless_wal_trim_safety_lag = value;
        }
        set_runtime! {
            runtime => cfg;
            positive_time: diskless_wal_index_projection_timeout;
            positive_usize: unclean_recovery_queue_capacity;
            whole_bytes_usize: share_coordinator_load_buffer_size;
            positive_usize: share_session_cache_max_when_unlimited;
            whole_bytes_usize: log_read_buffer_cap => log_config.read_buffer_cap,
                log_timestamp_scan_window => log_config.timestamp_scan_window;
            kafka_long_bytes: log_read_ahead_max => log_config.read_ahead_max,
                log_tail_cache_size => log_config.tail_cache_size;
        }
        // A topic reports these two at `STATIC_BROKER_CONFIG` when the
        // operator named them, so the loader records the provenance.
        cfg.static_config_origins.log.log_segment_bytes |= runtime.log_segment_bytes.is_some();
        cfg.static_config_origins.log.message_max_bytes |= runtime.message_max_bytes.is_some();
        set_runtime! {
            runtime => cfg;
            whole_bytes_u64: log_segment_bytes => log_config.segment_size;
            kafka_int_bytes: message_max_bytes => log_config.max_message_size;
            positive_time: log_delivery_clock_uncertainty => log_config.delivery_clock_uncertainty;
        }
        Ok(())
    }

    pub(super) fn apply_network_limits(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! { runtime => cfg; whole_bytes_u32: socket_request_max; }
        // Neither authentication limit is dynamic, so a named broker reports
        // one at `STATIC_BROKER_CONFIG` when the operator named it.
        let authentication = &mut cfg.static_config_origins.authentication;
        authentication.sasl_server_max_receive |= runtime.sasl_server_max_receive.is_some();
        authentication.connection_failed_authentication_delay |=
            runtime.connection_failed_authentication_delay.is_some();
        set_runtime! {
            runtime => cfg;
            whole_bytes_u32: sasl_server_max_receive;
            nonnegative_time: connection_failed_authentication_delay;
            positive_usize: queued_max_requests;
        }
        if let Some(bytes) = runtime.queued_max_request_bytes.take() {
            cfg.queued_max_request_bytes = Some(bytes);
        }
        set_runtime! {
            runtime => cfg;
            whole_bytes_usize: sendfile_min, socket_send_buffer, socket_receive_buffer;
            positive_i32: max_request_partition_size_limit;
            positive_ratio: record_decompression_max_ratio;
            whole_bytes_u64: record_decompression_output_floor,
                record_decompression_output_ceiling;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{
        bytes,
        convert::{ByteSizeExt as _, TimeExt as _},
        mebibytes, millis,
    };

    use crate::{
        config::BrokerConfig,
        file_config::{
            FileConfig,
            test_support::{configured, optional_configured_feature},
        },
    };

    #[test]
    fn runtime_file_config_rejects_negative_diskless_wal_trim_lag() {
        let file: FileConfig = toml::from_str("[runtime]\ndiskless_wal_trim_safety_lag = -1\n")
            .expect("parse runtime config");
        let error = file
            .apply_to(&mut BrokerConfig::default())
            .expect_err("reject negative trim lag");

        assert!(error.to_string().contains("diskless_wal_trim_safety_lag"));
    }
    #[test]
    fn runtime_file_config_accepts_positive_diskless_wal_trim_lag() {
        let config = configured(
            "[runtime]\ndiskless_wal_trim_safety_lag = 7\n",
            "parse runtime config",
        )
        .expect("accept positive trim lag");

        assert!(config.diskless_wal_trim_safety_lag == 7);
    }
    #[test]
    fn log_delivery_clock_uncertainty_round_trips_into_the_log_config() {
        // KFC-1's clock bound reaches every partition through
        // `BrokerConfig::log_config`, and it is a TOML-only key.
        let cfg = configured(
            "[runtime]\nlog_delivery_clock_uncertainty = \"750ms\"\n",
            "parse runtime config",
        )
        .expect("apply runtime config");

        assert!(cfg.log_config.delivery_clock_uncertainty == millis(750));
        assert!(cfg.log_config.delivery_clock_uncertainty.millis_i64() == 750);
    }
    #[test]
    fn message_max_bytes_round_trips_into_the_log_config() {
        // Kafka's broker-wide `message.max.bytes` is the default behind every
        // topic's `max.message.bytes`, and in krabka that default is the base
        // `LogConfig` the produce gate reads when a topic sets none.
        let cfg = configured(
            "[runtime]\nmessage_max_bytes = \"2KiB\"\n",
            "parse runtime config",
        )
        .expect("apply runtime config");

        assert!(cfg.log_config.max_message_size.bytes_u64() == 2048);
    }

    /// Zero is a value here, not a disabled key: it limits the read-ahead
    /// hint to the fetch's own range. A fractional byte count is refused.
    #[test]
    fn log_read_ahead_max_round_trips_into_the_log_config() {
        for (value, expected) in [
            ("8MiB", Some(8 * 1024 * 1024)),
            ("0B", Some(0)),
            ("1.5B", None),
        ] {
            let applied = optional_configured_feature(
                &format!("[runtime]\nlog_read_ahead_max = \"{value}\"\n"),
                |config| config.log_config.read_ahead_max.bytes_u64(),
            );
            assert!(applied == expected, "log_read_ahead_max={value}");
        }
    }

    /// Zero is a value here too: a partition keeps only its newest batch in
    /// memory. A fractional byte count is refused, and an omitted key keeps
    /// the log's default.
    #[test]
    fn log_tail_cache_size_round_trips_into_the_log_config() {
        for (line, expected) in [
            ("log_tail_cache_size = \"4MiB\"\n", Some(mebibytes(4))),
            ("log_tail_cache_size = \"0B\"\n", Some(bytes(0))),
            ("log_tail_cache_size = \"1.5B\"\n", None),
            ("", Some(krabka_log::DEFAULT_TAIL_CACHE_SIZE)),
        ] {
            let applied = toml::from_str::<FileConfig>(&format!("[runtime]\n{line}"))
                .ok()
                .and_then(|file| {
                    let mut cfg = BrokerConfig::default();
                    file.apply_to(&mut cfg)
                        .ok()
                        .map(|()| cfg.log_config.tail_cache_size)
                });
            assert!(applied == expected, "{line}");
        }
    }

    #[test]
    fn omitted_log_read_ahead_max_keeps_the_log_default() {
        let cfg = configured("[runtime]\n", "parse runtime config").expect("apply runtime config");

        assert!(cfg.log_config.read_ahead_max == krabka_log::DEFAULT_READ_AHEAD_MAX);
    }

    /// The TOML surface takes exactly the values Kafka's `INT` with
    /// `atLeast(0)` takes.
    ///
    /// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
    /// "Value must be at least 0", and refuses `2147483648` with "Not a number
    /// of type INT". The zero and the 2 GiB cases are the ones that separate
    /// this domain from a plain positive-whole-bytes one: a broker that took
    /// 2 GiB here would hand a topic an effective cap `kafka-configs` could
    /// not represent, and one that refused 0 would refuse a value Kafka boots
    /// on.
    #[test]
    fn message_max_bytes_takes_kafkas_int_at_least_zero() {
        for (value, expected) in [
            ("0B", Some(0)),
            ("2KiB", Some(2048)),
            ("2147483647B", Some(2_147_483_647)),
            ("-1B", None),
            ("2147483648B", None),
            ("2GiB", None),
            ("1.5B", None),
        ] {
            let applied = optional_configured_feature(
                &format!("[runtime]\nmessage_max_bytes = \"{value}\"\n"),
                |config| config.log_config.max_message_size.bytes_u64(),
            );
            assert!(applied == expected, "message_max_bytes={value}");
        }
    }

    #[test]
    fn omitted_message_max_bytes_keeps_kafkas_1048588() {
        let cfg = configured("[runtime]\n", "parse runtime config").expect("apply runtime config");

        assert!(cfg.log_config.max_message_size.bytes_u64() == 1_048_588);
    }

    #[test]
    fn omitted_log_delivery_clock_uncertainty_keeps_the_quarter_second_default() {
        let cfg = configured("[runtime]\n", "parse runtime config").expect("apply runtime config");

        assert!(cfg.log_config.delivery_clock_uncertainty == millis(250));
    }
    #[test]
    fn log_delivery_clock_uncertainty_rejects_a_nonpositive_bound() {
        let file: FileConfig =
            toml::from_str("[runtime]\nlog_delivery_clock_uncertainty = \"0ms\"\n")
                .expect("parse runtime config");

        let error = file
            .apply_to(&mut BrokerConfig::default())
            .expect_err("reject a zero clock bound");

        assert!(
            error.to_string().contains("log_delivery_clock_uncertainty"),
            "got: {error}"
        );
    }
    #[test]
    fn runtime_file_config_applies_record_decompression_policy() {
        let source = r#"
[runtime]
record_decompression_max_ratio = "50"
record_decompression_output_floor = "8MiB"
record_decompression_output_ceiling = "512MiB"
"#;
        let cfg = configured(source, "parse runtime config").expect("apply runtime config");

        let policy = cfg
            .record_decompression_policy()
            .expect("validated decompression policy");
        assert!(policy.max_ratio() == krabka_units::fraction(50.0));
        assert!(policy.output_floor() == krabka_units::mebibytes(8));
        assert!(policy.output_ceiling() == krabka_units::mebibytes(512));
    }
    #[test]
    fn runtime_file_config_rejects_invalid_record_decompression_relations() {
        for body in [
            "record_decompression_max_ratio = \"101\"\n",
            concat!(
                "record_decompression_output_floor = \"1GiB\"\n",
                "record_decompression_output_ceiling = \"16MiB\"\n",
            ),
        ] {
            let source = format!("[runtime]\n{body}");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let error = file
                .apply_to(&mut BrokerConfig::default())
                .expect_err("invalid record decompression policy must fail");
            assert!(error.to_string().contains("record_decompression"));
        }
    }
}
