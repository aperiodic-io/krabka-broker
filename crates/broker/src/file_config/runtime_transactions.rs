//! The `[runtime]` appliers for the transaction, offsets, and barrier
//! coordinator state topics.
//!
//! `apply_transactions` covers the internal state topics' partition counts,
//! replication factors, and recovery reads, together with the transaction
//! timeout bounds. `apply_barrier` covers the KFC-4 barrier group knobs.

use {
    super::{
        FileConfigError, RuntimeFileConfig,
        validate::{
            invalid_runtime_value, kafka_int_bytes, positive_i16, positive_i32, positive_time,
            positive_usize, whole_bytes_usize, whole_millis_i32_time, whole_millis_i64_time,
        },
    },
    crate::config::BrokerConfig,
    krabka_units::{ByteSize, Time, convert::TimeExt, kibibytes},
};

impl RuntimeFileConfig {
    pub(super) fn apply_transactions(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_millis_i64_time: producer_id_expiration;
            positive_time: producer_id_expiration_scan_interval;
            positive_usize: max_produce_group, partition_writer_queue_depth;
        }
        // A topic reports a static `min.insync.replicas` at
        // `STATIC_BROKER_CONFIG` when the operator named it, whatever the value.
        cfg.static_config_origins.log.min_insync_replicas |=
            runtime.default_min_insync_replicas.is_some();
        krabka_macros::set_runtime! { runtime => cfg; positive_i32: default_min_insync_replicas; }
        // `DescribeConfigs` reports these two as `STATIC_BROKER_CONFIG` when
        // the operator named them, so the loader records the provenance.
        if let Some(value) = runtime.num_partitions {
            cfg.num_partitions = positive_i32("num_partitions", value)?;
            cfg.static_config_origins.topic_creation.num_partitions = true;
        }
        if let Some(value) = runtime.default_replication_factor {
            cfg.default_replication_factor = positive_i16("default_replication_factor", value)?;
            cfg.static_config_origins
                .topic_creation
                .default_replication_factor = true;
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_bytes_usize: future_log_move_read_chunk;
            positive_i32: share_state_num_partitions => share_coordinator.state_topic_num_partitions;
            positive_i16: share_state_replication_factor
                => share_coordinator.state_topic_replication_factor;
            kafka_int_bytes: share_state_segment_bytes
                => share_coordinator.state_topic_segment_bytes;
            positive_i32: share_state_min_isr => share_coordinator.state_topic_min_isr;
        }
        // Kafka's `between(0, 500)`.
        if let Some(value) = runtime.share_snapshot_update_records_per_snapshot {
            if value > 500 {
                return Err(invalid_runtime_value(
                    "share_snapshot_update_records_per_snapshot",
                    "must be within 0..=500",
                ));
            }
            cfg.share_coordinator.snapshot_update_records_per_snapshot = value;
        }
        // Kafka's `atLeast(1)` milliseconds, as an `INT`.
        krabka_macros::set_runtime! {
            runtime => cfg;
            whole_millis_i32_duration: share_coordinator_write_timeout
                    => share_coordinator.write_timeout,
                share_state_prune_interval => share_coordinator.state_topic_prune_interval,
                share_cold_partition_snapshot_interval
                    => share_coordinator.cold_partition_snapshot_interval;
        }
        // `CompressionType.forId` over Kafka's `INT` codec id.
        if let Some(value) = runtime.share_state_compression_codec {
            cfg.share_coordinator.state_topic_compression_codec = u8::try_from(value)
                .ok()
                .filter(|id| *id <= 4)
                .and_then(krabka_compression::CompressionType::from_attribute_bits)
                .ok_or_else(|| {
                    invalid_runtime_value(
                        "share_state_compression_codec",
                        format!("Unknown compression type id: {value}"),
                    )
                })?;
        }
        // Kafka's `atLeast(1)`.
        if let Some(value) = runtime.share_coordinator_threads {
            let value = positive_i32("share_coordinator_threads", value)?;
            cfg.share_coordinator.threads = value.unsigned_abs();
        }
        // Kafka's `atLeast(-1)`, with -1 its adaptive linger.
        if let Some(value) = runtime.share_coordinator_append_linger_ms {
            cfg.share_coordinator.append_linger = match value {
                -1 => None,
                0.. => Some(std::time::Duration::from_millis(u64::from(
                    value.unsigned_abs(),
                ))),
                _ => {
                    return Err(invalid_runtime_value(
                        "share_coordinator_append_linger_ms",
                        "must be at least -1",
                    ));
                }
            };
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            // Kafka's `atLeast(512 * 1024)` over an `INT`.
            cached_buffer_max_bytes: share_coordinator_cached_buffer_max_bytes
                => share_coordinator.cached_buffer_max_bytes;
            positive_i32: offsets_topic_num_partitions;
            positive_i16: offsets_topic_replication_factor;
            kafka_int_bytes: offsets_topic_segment_bytes;
        }
        // These two carry the operator's intent, not just a value: `Some`
        // means the key was named, which is what `DescribeConfigs` reports as
        // `STATIC_BROKER_CONFIG`.
        if let Some(value) = runtime.offsets_retention {
            cfg.offsets_retention_override = Some(positive_time("offsets_retention", value)?);
        }
        if let Some(value) = runtime.offsets_retention_check_interval {
            cfg.offsets_retention_check_interval_override =
                Some(positive_time("offsets_retention_check_interval", value)?);
        }
        krabka_macros::set_runtime! {
            runtime => cfg;
            positive_i32: transaction_state_num_partitions;
            whole_bytes_usize: transaction_recovery_read_max;
            positive_i16: transaction_state_replication_factor;
            kafka_int_bytes: transaction_state_segment_bytes;
            positive_i32: transaction_state_min_isr;
            whole_millis_i32_time: transaction_max_timeout;
            plain: transaction_partition_verification_enable;
        }
        Ok(())
    }

    /// Applies the `barrier.*` runtime keys.
    ///
    /// `barrier_min_injection_interval` is a floor. A group asks for its own
    /// periodic interval through `AlterBarrierGroups`, and the coordinator
    /// refuses one below this value.
    pub(super) fn apply_barrier(&mut self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;
        krabka_macros::set_runtime! {
            runtime => cfg;
            positive_i32: barrier_state_num_partitions;
            positive_i16: barrier_state_replication_factor;
            whole_millis_i64_time: barrier_min_injection_interval, barrier_injection_timeout;
            whole_bytes_usize: barrier_recovery_read_max;
            positive_i32: barrier_retained_cuts;
            positive_usize: barrier_max_groups, barrier_max_topics_per_group;
        }
        Ok(())
    }
}

/// Kafka's `share.coordinator.cached.buffer.max.bytes` range: an `INT` of at
/// least 512 KiB.
fn cached_buffer_max_bytes(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = kafka_int_bytes(name, value)?;
    if value >= kibibytes(512) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be at least 524288 bytes"))
    }
}

/// [`whole_millis_i32_time`] as the [`std::time::Duration`] the share
/// coordinator's timers hold.
fn whole_millis_i32_duration(
    name: &str,
    value: Time,
) -> Result<std::time::Duration, FileConfigError> {
    whole_millis_i32_time(name, value).map(TimeExt::to_std)
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::BrokerConfig,
            file_config::{FileConfig, test_support::configured},
        },
        assert2::assert,
        krabka_units::{mebibytes, secs},
    };

    #[test]
    fn barrier_runtime_keys_land_in_the_broker_config() {
        let cfg = configured(
            r#"
[runtime]
barrier_state_num_partitions = 12
barrier_state_replication_factor = 2
barrier_min_injection_interval = "5s"
barrier_injection_timeout = "45s"
barrier_recovery_read_max = "4MiB"
barrier_retained_cuts = 25
barrier_max_groups = 8
barrier_max_topics_per_group = 16
"#,
            "parse barrier runtime config",
        )
        .expect("apply barrier runtime config");

        let actual = (
            cfg.barrier_state_num_partitions,
            cfg.barrier_state_replication_factor,
            cfg.barrier_min_injection_interval,
            cfg.barrier_injection_timeout,
            cfg.barrier_recovery_read_max,
            cfg.barrier_retained_cuts,
            cfg.barrier_max_groups,
            cfg.barrier_max_topics_per_group,
        );

        assert!(actual == (12, 2, secs(5), secs(45), mebibytes(4), 25, 8, 16));
    }
    #[test]
    fn barrier_runtime_keys_reject_nonpositive_values() {
        let cases = [
            "barrier_state_num_partitions = 0",
            "barrier_state_replication_factor = 0",
            "barrier_min_injection_interval = \"0s\"",
            "barrier_injection_timeout = \"0s\"",
            "barrier_recovery_read_max = \"0B\"",
            "barrier_retained_cuts = 0",
            "barrier_max_groups = 0",
            "barrier_max_topics_per_group = 0",
        ];

        for case in cases {
            let file: FileConfig = toml::from_str(&format!("[runtime]\n{case}\n"))
                .unwrap_or_else(|error| panic!("parse {case}: {error}"));
            let mut cfg = BrokerConfig::default();

            assert!(file.apply_to(&mut cfg).is_err(), "{case}");
        }
    }

    /// Kafka's `ShareCoordinatorConfig` defaults and bounds for the share
    /// coordinator keys: `Some` is the applied value, `None` a refusal.
    #[test]
    fn share_coordinator_keys_follow_kafka_defaults_and_bounds() {
        struct Row {
            label: &'static str,
            /// The `[runtime]` body.
            body: &'static str,
            /// The expected share coordinator config.
            expected: Option<ShareCoordinatorConfig>,
        }

        use {
            crate::share_coordinator::config::ShareCoordinatorConfig,
            krabka_compression::CompressionType, std::time::Duration,
        };

        let with = |f: fn(&mut ShareCoordinatorConfig)| {
            let mut config = ShareCoordinatorConfig::default();
            f(&mut config);
            Some(config)
        };
        let rows: [Row; 20] = [
            Row {
                label: "defaults",
                body: "",
                expected: Some(ShareCoordinatorConfig::default()),
            },
            Row {
                label: "snapshot cadence at its ceiling",
                body: "share_snapshot_update_records_per_snapshot = 500",
                expected: with(|c| c.snapshot_update_records_per_snapshot = 500),
            },
            Row {
                label: "snapshot cadence of zero",
                body: "share_snapshot_update_records_per_snapshot = 0",
                expected: with(|c| c.snapshot_update_records_per_snapshot = 0),
            },
            Row {
                label: "snapshot cadence above 500",
                body: "share_snapshot_update_records_per_snapshot = 501",
                expected: None,
            },
            Row {
                label: "write timeout",
                body: "share_coordinator_write_timeout = \"250ms\"",
                expected: with(|c| c.write_timeout = Duration::from_millis(250)),
            },
            Row {
                label: "zero write timeout",
                body: "share_coordinator_write_timeout = \"0ms\"",
                expected: None,
            },
            Row {
                label: "prune interval",
                body: "share_state_prune_interval = \"1s\"",
                expected: with(|c| c.state_topic_prune_interval = Duration::from_secs(1)),
            },
            Row {
                label: "cold snapshot interval above the INT ceiling",
                body: "share_cold_partition_snapshot_interval = \"2147483648ms\"",
                expected: None,
            },
            Row {
                label: "load buffer size",
                body: "share_coordinator_load_buffer_size = \"2MiB\"",
                expected: Some(ShareCoordinatorConfig::default()),
            },
            Row {
                label: "zstd state topic codec",
                body: "share_state_compression_codec = 4",
                expected: with(|c| c.state_topic_compression_codec = CompressionType::Zstd),
            },
            Row {
                label: "unknown state topic codec",
                body: "share_state_compression_codec = 5",
                expected: None,
            },
            Row {
                label: "negative state topic codec",
                body: "share_state_compression_codec = -1",
                expected: None,
            },
            Row {
                label: "four coordinator threads",
                body: "share_coordinator_threads = 4",
                expected: with(|c| c.threads = 4),
            },
            Row {
                label: "zero coordinator threads",
                body: "share_coordinator_threads = 0",
                expected: None,
            },
            Row {
                label: "adaptive append linger",
                body: "share_coordinator_append_linger_ms = -1",
                expected: Some(ShareCoordinatorConfig::default()),
            },
            Row {
                label: "zero append linger",
                body: "share_coordinator_append_linger_ms = 0",
                expected: with(|c| c.append_linger = Some(Duration::ZERO)),
            },
            Row {
                label: "append linger below -1",
                body: "share_coordinator_append_linger_ms = -2",
                expected: None,
            },
            Row {
                label: "cached buffer at its floor",
                body: "share_coordinator_cached_buffer_max_bytes = \"512KiB\"",
                expected: with(|c| c.cached_buffer_max_bytes = krabka_units::kibibytes(512)),
            },
            Row {
                label: "cached buffer below 512KiB",
                body: "share_coordinator_cached_buffer_max_bytes = \"524287B\"",
                expected: None,
            },
            Row {
                label: "cached buffer above the INT ceiling",
                body: "share_coordinator_cached_buffer_max_bytes = \"2GiB\"",
                expected: None,
            },
        ];
        for Row {
            label,
            body,
            expected,
        } in rows
        {
            let file: FileConfig =
                toml::from_str(&format!("[runtime]\n{body}\n")).expect("parse runtime config");
            let mut cfg = BrokerConfig::default();
            let applied = file
                .apply_to(&mut cfg)
                .ok()
                .map(|()| *cfg.share_coordinator);
            assert!(applied == expected, "{label}");
        }
        let file: FileConfig =
            toml::from_str("[runtime]\nshare_coordinator_load_buffer_size = \"2MiB\"\n")
                .expect("parse runtime config");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).expect("apply");
        assert!(cfg.share_coordinator_load_buffer_size == krabka_units::mebibytes(2));
        assert!(
            crate::config::BrokerConfig::default().share_coordinator_load_buffer_size
                == krabka_units::mebibytes(5)
        );
    }
}
