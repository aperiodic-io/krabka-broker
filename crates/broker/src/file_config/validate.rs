//! Scalar range, unit, and whole-number validators for file-config values.
//!
//! Each function takes the TOML key it guards so a rejection names the field
//! the operator wrote, and returns [`FileConfigError::InvalidConfig`] with that
//! name when the value is out of range. They live in one module because the
//! `[runtime]` appliers, the `set_runtime_*` macros, and the section appliers
//! all draw on the same set.

use {
    super::FileConfigError,
    crate::config_value::{PositiveCount, PositiveI32, PositiveI64},
    krabka_units::{
        ByteSize, Ratio, Time,
        convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
    },
};

pub(super) fn invalid_runtime_value(name: &str, error: impl std::fmt::Display) -> FileConfigError {
    FileConfigError::InvalidConfig(format!("{name}: {error}"))
}

pub(super) fn positive_u64(name: &str, value: u64) -> Result<u64, FileConfigError> {
    refined_type::rule::GreaterU64::<0>::new(value)
        .map(refined_type::Refined::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_i32(name: &str, value: i32) -> Result<i32, FileConfigError> {
    PositiveI32::new(value)
        .map(PositiveI32::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_i64(name: &str, value: i64) -> Result<i64, FileConfigError> {
    PositiveI64::new(value)
        .map(PositiveI64::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_usize(name: &str, value: usize) -> Result<usize, FileConfigError> {
    PositiveCount::new(value)
        .map(PositiveCount::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_u32(name: &str, value: u32) -> Result<u32, FileConfigError> {
    let count = usize::try_from(value).map_err(|error| invalid_runtime_value(name, error))?;
    positive_usize(name, count)?;
    Ok(value)
}

pub(super) fn whole_bytes_u64(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let bytes = value.bytes_u64();
    if value.bytes_f64().is_finite()
        && value > ByteSize::from_bytes(0)
        && value.bytes_f64() < 18_446_744_073_709_551_616.0
        && ByteSize::from_bytes(bytes) == value
    {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a positive whole number of bytes within the u64 range",
        ))
    }
}

pub(super) fn metadata_snapshot_fetch_max(
    name: &str,
    value: ByteSize,
) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(value)
        .map(|_| value)
        .map_err(|error| invalid_runtime_value(name, error))
}

fn whole_nonnegative_bytes(value: ByteSize) -> bool {
    value.bytes_f64().is_finite()
        && value >= ByteSize::from_bytes(0)
        && ByteSize::from_bytes(value.bytes_u64()) == value
}

/// A byte count in the domain Kafka gives an `INT` config with `atLeast(0)`.
///
/// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
/// "Invalid value -1 for configuration message.max.bytes: Value must be at
/// least 0", and refuses `2147483648` with "Not a number of type INT". The
/// topic-level `max.message.bytes` is the same `INT` and the broker-wide key
/// is the default behind it, so one domain covers the TOML file, the command
/// line, and `kafka-configs --alter` alike.
///
/// This is [`whole_bytes_i32`] with zero allowed, which is why it does not go
/// through `whole_bytes_u64`: that helper rejects zero.
pub(super) fn kafka_int_bytes(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let bytes = value.bytes_u64();
    if whole_nonnegative_bytes(value)
        && bytes <= u64::try_from(i32::MAX).expect("i32::MAX fits u64")
    {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of bytes from 0 to 2147483647",
        ))
    }
}

/// A byte count in the domain of Kafka's `metadata.log.segment.bytes`: an
/// `INT` with `atLeast(8388608)`.
///
/// Kafka's internal `internal.metadata.log.segment.bytes` goes below the
/// 8 MiB floor for tests only, and krabka has no form of it.
pub(super) fn metadata_log_segment_bytes(
    name: &str,
    value: ByteSize,
) -> Result<ByteSize, FileConfigError> {
    let bytes = value.bytes_u64();
    let floor = krabka_raft::MIN_METADATA_LOG_SEGMENT_SIZE.bytes_u64();
    let ceiling = u64::try_from(i32::MAX).expect("i32::MAX fits u64");
    if whole_nonnegative_bytes(value) && (floor..=ceiling).contains(&bytes) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of bytes from 8388608 to 2147483647",
        ))
    }
}

/// A byte count that Kafka's `ConfigDef.Type::LONG` can hold, where zero is a
/// value.
///
/// Kafka gives a key of this kind a negative value to set no limit. A TOML
/// byte size has no negative form, so the operator omits the key instead,
/// and the built-in limit stays.
pub(super) fn kafka_long_bytes(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    if whole_nonnegative_bytes(value) && value.bytes_f64() < 9_223_372_036_854_775_808.0 {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of bytes from 0 to 9223372036854775807",
        ))
    }
}

pub(super) fn whole_bytes_i32(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    if value.bytes_u64() <= u64::try_from(i32::MAX).expect("i32::MAX fits u64") {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be at most 2147483647 bytes",
        ))
    }
}

pub(super) fn whole_bytes_u32(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    if u32::try_from(value.bytes_u64()).is_ok() {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be at most 4294967295 bytes",
        ))
    }
}

pub(super) fn whole_bytes_usize(name: &str, value: ByteSize) -> Result<ByteSize, FileConfigError> {
    let value = whole_bytes_u64(name, value)?;
    usize::try_from(value.bytes_u64())
        .map(|_| value)
        .map_err(|error| invalid_runtime_value(name, error))
}

pub(super) fn positive_ratio(name: &str, value: Ratio) -> Result<Ratio, FileConfigError> {
    if value.as_f64().is_finite() && value > krabka_units::fraction(0.0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be finite and positive"))
    }
}

pub(super) fn positive_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    if value.secs_f64().is_finite() && value > Time::from_secs(0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(name, "must be finite and positive"))
    }
}

pub(super) fn nonnegative_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    if value.secs_f64().is_finite() && value >= Time::from_secs(0) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be finite and nonnegative",
        ))
    }
}

pub(super) fn voter_request_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    whole_millis_i32_time(name, value)
}

pub(super) fn whole_millis_i32_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    let value = whole_millis_i64_time(name, value)?;
    let millis = value.millis_i64();
    if (1..=i64::from(i32::MAX)).contains(&millis) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be within 1ms..=2147483647ms",
        ))
    }
}

/// A whole number of milliseconds Kafka's `ConfigDef.Type::INT` can hold,
/// where zero is a meaningful value rather than a floor violation.
///
/// [`whole_millis_i32_time`] floors at 1ms, which is what Kafka's `atLeast(1)`
/// states for a knob that must always run. A krabka cadence that zero disables
/// needs the same 32-bit ceiling with 0 accepted, so that the value the broker
/// runs on is one `DescribeConfigs` can report as an `INT`.
pub(super) fn disableable_millis_i32_time(
    name: &str,
    value: Time,
) -> Result<Time, FileConfigError> {
    let value = whole_millis(name, nonnegative_time(name, value)?)?;
    let millis = value.millis_i64();
    if (0..=i64::from(i32::MAX)).contains(&millis) {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be within 0ms..=2147483647ms",
        ))
    }
}

pub(super) fn whole_millis_i64_time(name: &str, value: Time) -> Result<Time, FileConfigError> {
    let value = positive_time(name, value)?;
    whole_millis(name, value)
}

/// A whole number of milliseconds Kafka's `ConfigDef.Type::LONG` can hold,
/// where zero is a value.
///
/// [`whole_millis_i64_time`] refuses zero. A Kafka `LONG` of milliseconds with
/// no validator, such as `metadata.max.retention.ms`, accepts it, and Kafka
/// gives the key a negative value to set no limit. A TOML duration has no
/// negative form here, so the operator omits the key instead.
pub(super) fn nonnegative_millis_i64_time(
    name: &str,
    value: Time,
) -> Result<Time, FileConfigError> {
    let value = nonnegative_time(name, value)?;
    whole_millis(name, value)
}

/// Verify representability after the caller applies its own sign or zero policy.
fn whole_millis(name: &str, value: Time) -> Result<Time, FileConfigError> {
    let millis = value.millis_i64();
    if Time::from_millis(millis) == value {
        Ok(value)
    } else {
        Err(invalid_runtime_value(
            name,
            "must be a whole number of milliseconds",
        ))
    }
}

pub(super) fn positive_i16(name: &str, value: i16) -> Result<i16, FileConfigError> {
    positive_i32(name, i32::from(value))?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::{
                AuthenticationOrigins, BrokerConfig, LogOrigins, StaticConfigOrigins,
                TopicAdminOrigins, TopicCreationOrigins,
            },
            file_config::{FileConfig, test_support::configured},
        },
        assert2::{assert, check},
    };

    /// Independent default provenance with the two explicitly varied expiry flags.
    fn expected_origins(expiration: bool, cleanup: bool) -> StaticConfigOrigins {
        StaticConfigOrigins {
            txn_id_expiration: expiration,
            txn_id_expiration_cleanup_interval: cleanup,
            topic_creation: TopicCreationOrigins::default(),
            log: LogOrigins::default(),
            topic_admin: TopicAdminOrigins::default(),
            authentication: AuthenticationOrigins::default(),
            ..Default::default()
        }
    }

    #[test]
    fn runtime_file_config_rejects_zero_and_names_field() {
        let error = configured(
            "[runtime]\ncleaner_interval = \"0ms\"\n",
            "parse runtime config",
        )
        .expect_err("zero cleaner interval must fail");

        assert!(error.to_string().contains("cleaner_interval"));
    }

    #[test]
    fn runtime_file_config_rejects_voter_timeout_above_wire_limit() {
        let error = configured(
            "[runtime]\nauto_join_voter_request_timeout = \"2147483648ms\"\n",
            "parse runtime config",
        )
        .expect_err("timeout above i32 wire limit must fail");

        assert!(
            error
                .to_string()
                .contains("auto_join_voter_request_timeout")
        );
    }

    #[yare::parameterized(
        client_metrics_default_interval = {
            "client_metrics_default_interval",
            "[runtime]\nclient_metrics_default_interval = \"1.5ms\"\n",
        },
        producer_id_expiration = {
            "producer_id_expiration",
            "[runtime]\nproducer_id_expiration = \"1.5ms\"\n",
        },
    )]
    fn runtime_file_config_rejects_fractional_protocol_milliseconds(field: &str, source: &str) {
        let error = configured(source, "parse runtime config")
            .expect_err("fractional protocol milliseconds must fail");
        assert2::assert!(error.to_string().contains(field));
    }
    /// Both KIP-98 expiry knobs are Kafka `ConfigDef.Type::INT`, so a value
    /// wider than an `i32` of milliseconds, or a fractional one, is refused
    /// with the field named. The expiry itself must run, matching Kafka's
    /// `atLeast(1)`; zero on the sweep cadence disables the sweep and is
    /// accepted.
    #[yare::parameterized(
        expiry_above_i32_limit = {
            "an expiry above the i32 millisecond ceiling",
            "[runtime]\ntxn_id_expiration = \"2147483648ms\"\n",
            Some("txn_id_expiration"),
        },
        zero_expiry = {
            "a zero expiry, which Kafka's atLeast(1) refuses",
            "[runtime]\ntxn_id_expiration = \"0ms\"\n",
            Some("txn_id_expiration"),
        },
        fractional_expiry = {
            "a fractional expiry",
            "[runtime]\ntxn_id_expiration = \"1.5ms\"\n",
            Some("txn_id_expiration"),
        },
        sweep_above_i32_limit = {
            "a sweep cadence above the i32 millisecond ceiling",
            "[runtime]\ntxn_id_expiration_cleanup_interval = \"2147483648ms\"\n",
            Some("txn_id_expiration_cleanup_interval"),
        },
        zero_sweep_is_accepted = {
            "a zero sweep cadence, which disables the sweep",
            "[runtime]\ntxn_id_expiration_cleanup_interval = \"0ms\"\n",
            None,
        },
    )]
    fn runtime_file_config_bounds_the_transactional_id_expiry_knobs_to_an_i32(
        label: &str,
        source: &str,
        field: Option<&str>,
    ) {
        let result = configured(source, "parse runtime config");

        if let Some(field) = field {
            let error = result.expect_err(label);
            check!(error.to_string().contains(field), "{label}");
        } else {
            check!(result.is_ok(), "{label}");
            let cfg = result.unwrap();
            check!(
                cfg.txn_id_expiration_cleanup_interval
                    == <krabka_units::Time as krabka_units::convert::TimeExt>::ZERO,
                "{label}"
            );
        }
    }

    /// The loader records *that* the operator supplied each expiry knob, not
    /// only the value, because `DescribeConfigs` reports a config source and a
    /// source is provenance. Supplying Kafka's own default is the case that
    /// separates the two: `apache/kafka:4.3.1` still reports
    /// `STATIC_BROKER_CONFIG` for it, so krabka has to know the key was named.
    #[yare::parameterized(
        nothing_supplied = {
            "nothing supplied",
            "[runtime]\n",
            expected_origins(false, false),
        },
        expiry_at_kafka_default = {
            "the expiry supplied, at Kafka's own default value",
            "[runtime]\ntxn_id_expiration = \"604800000ms\"\n",
            expected_origins(true, false),
        },
        both_expiry_knobs_supplied = {
            "both supplied",
            "[runtime]\ntxn_id_expiration = \"120000ms\"\n\
             txn_id_expiration_cleanup_interval = \"60000ms\"\n",
            expected_origins(true, true),
        },
        topic_creation_defaults_supplied = {
            "the KIP-464 topic-creation defaults supplied, at Kafka's own default value",
            "[runtime]\nnum_partitions = 1\ndefault_replication_factor = 1\n",
            StaticConfigOrigins {
                topic_creation: TopicCreationOrigins {
                    num_partitions: true,
                    default_replication_factor: true,
                },
                ..expected_origins(false, false)
            },
        },
        log_defaults_supplied = {
            "the log defaults that back a topic key supplied, at Kafka's own default value",
            "[runtime]\nmessage_max_bytes = \"1048588B\"\nlog_segment_bytes = \"1GiB\"\n\
                     default_min_insync_replicas = 1\n",
            StaticConfigOrigins {
                log: LogOrigins {
                    message_max_bytes: true,
                    log_segment_bytes: true,
                    min_insync_replicas: true,
                },
                ..expected_origins(false, false)
            },
        },
        authentication_limits_supplied = {
            "the authentication limits supplied, at Kafka's own default value",
            "[runtime]\nsasl_server_max_receive = \"524288B\"\n\
                     connection_failed_authentication_delay = \"100ms\"\n",
            StaticConfigOrigins {
                authentication: AuthenticationOrigins {
                    sasl_server_max_receive: true,
                    connection_failed_authentication_delay: true,
                },
                ..expected_origins(false, false)
            },
        },
    )]
    fn runtime_file_config_records_which_expiry_knobs_the_operator_supplied(
        label: &str,
        source: &str,
        expected: StaticConfigOrigins,
    ) {
        let cfg = configured(source, "parse runtime config").expect("apply runtime config");

        check!(cfg.static_config_origins == expected, "{label}");
    }

    #[yare::parameterized(
        client_metrics_telemetry_max = { "client_metrics_telemetry_max", "0B" },
        replication_fetch_max = { "replication_fetch_max", "0B" },
        replication_fetch_min = { "replication_fetch_min", "0B" },
        observer_fetch_max = { "observer_fetch_max", "0B" },
        audit_tail_read_max = { "audit_tail_read_max", "0B" },
        share_coordinator_load_buffer_size = { "share_coordinator_load_buffer_size", "0B" },
        socket_request_max = { "socket_request_max", "0B" },
        sasl_server_max_receive = { "sasl_server_max_receive", "0B" },
        sendfile_min = { "sendfile_min", "0B" },
        socket_send_buffer = { "socket_send_buffer", "0B" },
        socket_receive_buffer = { "socket_receive_buffer", "0B" },
        record_decompression_output_floor = { "record_decompression_output_floor", "0B" },
        record_decompression_output_ceiling = { "record_decompression_output_ceiling", "0B" },
        future_log_move_read_chunk = { "future_log_move_read_chunk", "0B" },
        metadata_max_bytes_between_snapshots = { "metadata_max_bytes_between_snapshots", "0B" },
        metadata_snapshot_fetch_max = { "metadata_snapshot_fetch_max", "0B" },
        record_decompression_max_ratio = { "record_decompression_max_ratio", "0" },
    )]
    fn runtime_file_config_rejects_invalid_dimensioned_sizes_and_ratios(field: &str, value: &str) {
        // `message_max_bytes` is deliberately absent: Kafka declares it an
        // `INT` with `atLeast(0)` and `apache/kafka:4.3.1` boots on
        // `message.max.bytes=0`, so zero is a value and not an error.
        // `message_max_bytes_takes_kafkas_int_at_least_zero` in
        // `runtime_storage` covers its whole domain.
        let source = format!("[runtime]\n{field} = \"{value}\"\n");
        let error =
            configured(&source, "parse runtime config").expect_err("invalid value must fail");
        assert2::assert!(error.to_string().contains(field), "{error}");
    }

    #[yare::parameterized(
        client_metrics_fractional = { "client_metrics_telemetry_max", "1.5B" },
        replication_fetch_overflow = { "replication_fetch_max", "2147483648B" },
        fetch_max_overflow = { "fetch_max_bytes", "2147483648B" },
        observer_fetch_overflow = { "observer_fetch_max", "4294967296B" },
        socket_request_overflow = { "socket_request_max", "4294967296B" },
        sasl_server_overflow = { "sasl_server_max_receive", "4294967296B" },
        audit_tail_fractional = { "audit_tail_read_max", "1.5B" },
        decompression_floor_fractional = { "record_decompression_output_floor", "1.5B" },
        decompression_ceiling_overflow = {
            "record_decompression_output_ceiling",
            "1073741825B",
        },
        metadata_snapshot_bytes_overflow = {
            "metadata_max_bytes_between_snapshots",
            "18446744073709551616B",
        },
        snapshot_fetch_fractional = { "metadata_snapshot_fetch_max", "1.5B" },
        snapshot_fetch_overflow = { "metadata_snapshot_fetch_max", "1073741825B" },
        message_max_fractional = { "message_max_bytes", "1.5B" },
        message_max_overflow = { "message_max_bytes", "2147483648B" },
    )]
    fn runtime_file_config_rejects_fractional_and_overflowing_sizes(field: &str, value: &str) {
        let source = format!("[runtime]\n{field} = \"{value}\"\n");
        let error = configured(&source, "parse runtime config")
            .expect_err("fractional or overflowing byte size must fail");
        let expected = if field == "record_decompression_output_ceiling" {
            "record_decompression"
        } else {
            field
        };
        assert2::assert!(error.to_string().contains(expected), "{error}");
    }

    #[yare::parameterized(
        zero = { "[runtime]\nmetadata_max_snapshot_interval = \"0ms\"\n", true },
        one_millisecond = { "[runtime]\nmetadata_max_snapshot_interval = \"1ms\"\n", true },
        one_hour = { "[runtime]\nmetadata_max_snapshot_interval = \"1h\"\n", true },
        // A positive sub-millisecond value truncates to 0ms under
        // `millis_i64()`, which the engine reads as "disabled" — so it
        // must be rejected here rather than silently doing nothing.
        positive_submillisecond = {
            "[runtime]\nmetadata_max_snapshot_interval = \"250us\"\n",
            false,
        },
    )]
    fn metadata_max_snapshot_interval_accepts_zero_and_whole_millis_only(
        source: &str,
        expect_ok: bool,
    ) {
        let result = configured(source, "parse runtime config");
        assert2::assert!(result.is_ok() == expect_ok, "{source}: {result:?}");
    }

    /// Each of the five `MetadataLogConfig` keys takes the domain Kafka's
    /// `ConfigDef` gives it, in whole bytes or whole milliseconds. A value
    /// that is accepted lands in its own field and leaves the other four at
    /// Kafka's defaults. A value that is refused names its field.
    #[test]
    fn metadata_log_keys_take_kafkas_domains() {
        struct Case {
            line: &'static str,
            expected: Result<krabka_raft::MetadataLogConfig, &'static str>,
        }

        use krabka_units::{bytes, days, mebibytes, millis};

        let base = krabka_raft::MetadataLogConfig::default();
        let segment_size = |size| {
            Ok(krabka_raft::MetadataLogConfig {
                segment_size: size,
                ..base
            })
        };
        let segment_roll_interval = |time| {
            Ok(krabka_raft::MetadataLogConfig {
                segment_roll_interval: time,
                ..base
            })
        };
        let max_retention_size = |size| {
            Ok(krabka_raft::MetadataLogConfig {
                max_retention_size: Some(size),
                ..base
            })
        };
        let max_retention = |time| {
            Ok(krabka_raft::MetadataLogConfig {
                max_retention: Some(time),
                ..base
            })
        };
        let max_idle_interval = |time| {
            Ok(krabka_raft::MetadataLogConfig {
                max_idle_interval: time,
                ..base
            })
        };
        let cases: [Case; 26] = [
            // `INT` with `atLeast(8388608)`.
            Case {
                line: "metadata_log_segment_bytes = \"8MiB\"",
                expected: segment_size(mebibytes(8)),
            },
            Case {
                line: "metadata_log_segment_bytes = \"2147483647B\"",
                expected: segment_size(bytes(2_147_483_647)),
            },
            Case {
                line: "metadata_log_segment_bytes = \"8388607B\"",
                expected: Err("metadata_log_segment_bytes"),
            },
            Case {
                line: "metadata_log_segment_bytes = \"0B\"",
                expected: Err("metadata_log_segment_bytes"),
            },
            Case {
                line: "metadata_log_segment_bytes = \"2147483648B\"",
                expected: Err("metadata_log_segment_bytes"),
            },
            Case {
                line: "metadata_log_segment_bytes = \"8388608.5B\"",
                expected: Err("metadata_log_segment_bytes"),
            },
            // A `LONG` that krabka holds positive.
            Case {
                line: "metadata_log_segment_roll_interval = \"1ms\"",
                expected: segment_roll_interval(millis(1)),
            },
            Case {
                line: "metadata_log_segment_roll_interval = \"7d\"",
                expected: segment_roll_interval(days(7)),
            },
            Case {
                line: "metadata_log_segment_roll_interval = \"0ms\"",
                expected: Err("metadata_log_segment_roll_interval"),
            },
            Case {
                line: "metadata_log_segment_roll_interval = \"250us\"",
                expected: Err("metadata_log_segment_roll_interval"),
            },
            // A `LONG` where zero is a value.
            Case {
                line: "metadata_max_retention_bytes = \"0B\"",
                expected: max_retention_size(bytes(0)),
            },
            Case {
                line: "metadata_max_retention_bytes = \"1GiB\"",
                expected: max_retention_size(mebibytes(1024)),
            },
            Case {
                line: "metadata_max_retention_bytes = \"1.5B\"",
                expected: Err("metadata_max_retention_bytes"),
            },
            Case {
                line: "metadata_max_retention_bytes = \"9223372036854775808B\"",
                expected: Err("metadata_max_retention_bytes"),
            },
            Case {
                line: "metadata_max_retention = \"0ms\"",
                expected: max_retention(millis(0)),
            },
            Case {
                line: "metadata_max_retention = \"30d\"",
                expected: max_retention(days(30)),
            },
            Case {
                line: "metadata_max_retention = \"250us\"",
                expected: Err("metadata_max_retention"),
            },
            Case {
                line: "metadata_max_retention = \"1.5ms\"",
                expected: Err("metadata_max_retention"),
            },
            // `INT` with `atLeast(0)`, where zero stops the KIP-835 no-op
            // records.
            Case {
                line: "metadata_max_idle_interval = \"0ms\"",
                expected: max_idle_interval(millis(0)),
            },
            Case {
                line: "metadata_max_idle_interval = \"500ms\"",
                expected: max_idle_interval(millis(500)),
            },
            Case {
                line: "metadata_max_idle_interval = \"2147483647ms\"",
                expected: max_idle_interval(millis(2_147_483_647)),
            },
            Case {
                line: "metadata_max_idle_interval = \"2147483648ms\"",
                expected: Err("metadata_max_idle_interval"),
            },
            Case {
                line: "metadata_max_idle_interval = \"250us\"",
                expected: Err("metadata_max_idle_interval"),
            },
            Case {
                line: "metadata_max_idle_interval = \"-1ms\"",
                expected: Err("metadata_max_idle_interval"),
            },
            // A negative value does not stand for "no limit" in the file.
            Case {
                line: "metadata_max_retention = \"-1ms\"",
                expected: Err("metadata_max_retention"),
            },
            Case {
                line: "metadata_log_segment_roll_interval = \"-1ms\"",
                expected: Err("metadata_log_segment_roll_interval"),
            },
        ];

        for Case { line, expected } in cases {
            let source = format!("[runtime]\n{line}\n");
            let file: FileConfig = toml::from_str(&source).expect("parse runtime config");
            let mut cfg = BrokerConfig::default();
            let actual = file
                .apply_to(&mut cfg)
                .map(|()| cfg.metadata_log)
                .map_err(|error| error.to_string());

            match expected {
                Ok(want) => {
                    check!(actual == Ok(want), "{line}");
                }
                Err(field) => {
                    check!(
                        actual
                            .as_ref()
                            .is_err_and(|message| message.contains(&format!("{field}: must"))),
                        "{line}: {actual:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn existing_file_inputs_reject_invalid_refined_values() {
        struct Case {
            source: &'static str,
            field: &'static str,
        }
        let cases = [
            Case {
                source: "heartbeat_interval = \"0ms\"\n",
                field: "heartbeat_interval",
            },
            Case {
                source: "[runtime]\noffsets_topic_replication_factor = 0\n",
                field: "offsets_topic_replication_factor",
            },
            Case {
                source: "[delegation_token]\nmax_lifetime_ms = 0\n",
                field: "max_lifetime_ms",
            },
        ];

        for Case { source, field } in cases {
            let error = configured(source, "parse config").expect_err("zero must fail");
            assert!(error.to_string().contains(field));
        }
    }
}
