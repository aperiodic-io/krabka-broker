//! Behaviour tests for the runtime-scalar checks: which value each rejection
//! names, and the message it reports.

use assert2::assert;
use krabka_units::{gibibytes, millis, nanos};

use super::*;
use crate::config::test_support::{
    RuntimeInvalidator, assert_invalid_runtime, assert_invalid_runtime_cases, base,
};

#[test]
fn rejects_non_positive_runtime_scalars() {
    let cases: [RuntimeInvalidator; 23] = [
        ("startup_leader_wait_timeout", |c| {
            c.startup_leader_wait_timeout = <Time as TimeExt>::ZERO;
        }),
        ("cleaner_interval", |c| {
            c.cleaner_interval = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_flush_interval", |c| {
            c.diskless_wal_flush_interval = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_index_projection_timeout", |c| {
            c.diskless_wal_index_projection_timeout = <Time as TimeExt>::ZERO;
        }),
        ("diskless_wal_flush_max_size", |c| {
            c.diskless_wal_flush_max_size = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("diskless_wal_hot_tail_max_size", |c| {
            c.diskless_wal_hot_tail_max_size = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("client_metrics_default_interval", |c| {
            c.client_metrics_default_interval = <Time as TimeExt>::ZERO;
        }),
        ("client_metrics_telemetry_max", |c| {
            c.client_metrics_telemetry_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("client_metrics_otlp_queue_capacity", |c| {
            c.client_metrics_otlp_queue_capacity = 0;
        }),
        ("replication.fetch_max", |c| {
            c.replication.fetch_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("replication.fetch_max_wait", |c| {
            c.replication.fetch_max_wait = <Time as TimeExt>::ZERO;
        }),
        ("replication.fetch_min", |c| {
            c.replication.fetch_min = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("replication.send_error_backoff", |c| {
            c.replication.send_error_backoff = <Time as TimeExt>::ZERO;
        }),
        ("heartbeat_interval", |c| {
            c.heartbeat_interval = <Time as TimeExt>::ZERO;
        }),
        ("replica_lag_time_max", |c| {
            c.replica_lag_time_max = <Time as TimeExt>::ZERO;
        }),
        ("controller_heartbeat_interval", |c| {
            c.controller_heartbeat_interval = <Time as TimeExt>::ZERO;
        }),
        ("metadata_max_bytes_between_snapshots", |c| {
            c.metadata_max_bytes_between_snapshots = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("metadata_snapshot_interval_records", |c| {
            c.metadata_snapshot_interval_records = 0;
        }),
        ("delegation_token_max_lifetime", |c| {
            c.delegation_token_max_lifetime = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_expiry_check_interval", |c| {
            c.delegation_token_expiry_check_interval = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_default_renew_period", |c| {
            c.delegation_token_default_renew_period = -millis(1);
        }),
        ("remote_log_manager_interval", |c| {
            c.remote_log_manager_interval = <Time as TimeExt>::ZERO;
        }),
        ("delegation_token_max_lifetime", |c| {
            c.delegation_token_max_lifetime = -millis(1);
        }),
    ];

    for (name, invalidate) in cases {
        let mut config = BrokerConfig::default();
        invalidate(&mut config);
        assert_invalid_runtime(&config, &format!("{name} must be positive"));
    }
}

#[test]
fn rejects_invalid_additional_runtime_scalars() {
    let cases: &[RuntimeInvalidator] = &[
        ("self_registration_max_attempts must be positive", |c| {
            c.self_registration_max_attempts = 0;
        }),
        ("observer_fetch_max must be positive", |c| {
            c.observer_fetch_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("audit_event_queue_capacity must be positive", |c| {
            c.audit_event_queue_capacity = 0;
        }),
        ("audit_tail_window_offsets must be positive", |c| {
            c.audit_tail_window_offsets = 0;
        }),
        ("audit_tail_read_max must be positive", |c| {
            c.audit_tail_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        (
            "client_metrics_stale_push_intervals must be positive",
            |c| c.client_metrics_stale_push_intervals = 0,
        ),
        ("coordinator_actor_mailbox_capacity must be positive", |c| {
            c.coordinator_actor_mailbox_capacity = 0;
        }),
        ("diskless_wal_local_replica_count must be positive", |c| {
            c.diskless_wal_local_replica_count = 0;
        }),
        ("diskless_wal_local_replica_count must be odd", |c| {
            c.diskless_wal_local_replica_count = 2;
        }),
        ("diskless_wal_trim_safety_lag must be nonnegative", |c| {
            c.diskless_wal_trim_safety_lag = -1;
        }),
        ("unclean_recovery_queue_capacity must be positive", |c| {
            c.unclean_recovery_queue_capacity = 0;
        }),
        ("share_coordinator_load_buffer_size must be positive", |c| {
            c.share_coordinator_load_buffer_size = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("transaction_recovery_read_max must be positive", |c| {
            c.transaction_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        (
            "share_session_cache_max_when_unlimited must be positive",
            |c| c.share_session_cache_max_when_unlimited = 0,
        ),
        ("socket_request_max must be positive", |c| {
            c.socket_request_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("sasl_server_max_receive must be positive", |c| {
            c.sasl_server_max_receive = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("sendfile_min must be positive", |c| {
            c.sendfile_min = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("socket_send_buffer must be positive", |c| {
            c.socket_send_buffer = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("socket_receive_buffer must be positive", |c| {
            c.socket_receive_buffer = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("producer_id_expiration must be positive", |c| {
            c.producer_id_expiration = <Time as TimeExt>::ZERO;
        }),
        (
            "producer_id_expiration_scan_interval must be positive",
            |c| c.producer_id_expiration_scan_interval = <Time as TimeExt>::ZERO,
        ),
        (
            "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            |c| {
                c.auto_join_voter_request_timeout = <Time as TimeExt>::ZERO;
            },
        ),
        (
            "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            |c| {
                c.auto_join_voter_request_timeout = nanos(1);
            },
        ),
        ("max_produce_group must be positive", |c| {
            c.max_produce_group = 0;
        }),
        ("partition_writer_queue_depth must be positive", |c| {
            c.partition_writer_queue_depth = 0;
        }),
        ("default_min_insync_replicas must be positive", |c| {
            c.default_min_insync_replicas = 0;
        }),
        ("max_request_partition_size_limit must be positive", |c| {
            c.max_request_partition_size_limit = 0;
        }),
        ("future_log_move_read_chunk must be positive", |c| {
            c.future_log_move_read_chunk = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("share_state_num_partitions must be positive", |c| {
            c.share_coordinator.state_topic_num_partitions = 0;
        }),
        ("share_state_replication_factor must be positive", |c| {
            c.share_coordinator.state_topic_replication_factor = 0;
        }),
        ("offsets_topic_num_partitions must be positive", |c| {
            c.offsets_topic_num_partitions = 0;
        }),
        ("offsets_topic_replication_factor must be positive", |c| {
            c.offsets_topic_replication_factor = 0;
        }),
        ("transaction_state_num_partitions must be positive", |c| {
            c.transaction_state_num_partitions = 0;
        }),
        (
            "transaction_state_replication_factor must be positive",
            |c| c.transaction_state_replication_factor = 0,
        ),
        ("transaction_max_timeout must be positive", |c| {
            c.transaction_max_timeout = <Time as TimeExt>::ZERO;
        }),
        ("barrier_state_num_partitions must be positive", |c| {
            c.barrier_state_num_partitions = 0;
        }),
        ("barrier_state_replication_factor must be positive", |c| {
            c.barrier_state_replication_factor = 0;
        }),
        ("barrier_retained_cuts must be positive", |c| {
            c.barrier_retained_cuts = 0;
        }),
        ("barrier_max_groups must be positive", |c| {
            c.barrier_max_groups = 0;
        }),
        ("barrier_max_topics_per_group must be positive", |c| {
            c.barrier_max_topics_per_group = 0;
        }),
        ("barrier_recovery_read_max must be positive", |c| {
            c.barrier_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
        }),
        ("barrier_min_injection_interval must be positive", |c| {
            c.barrier_min_injection_interval = <Time as TimeExt>::ZERO;
        }),
        ("barrier_injection_timeout must be positive", |c| {
            c.barrier_injection_timeout = <Time as TimeExt>::ZERO;
        }),
        ("transaction_state_min_isr must be positive", |c| {
            c.transaction_state_min_isr = 0;
        }),
        ("share_state_min_isr must be positive", |c| {
            c.share_coordinator.state_topic_min_isr = 0;
        }),
        (
            "offsets_topic_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            |c| {
                c.offsets_topic_segment_bytes = krabka_units::kibibytes(1);
            },
        ),
        (
            "transaction_state_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            |c| {
                c.transaction_state_segment_bytes = krabka_units::kibibytes(1);
            },
        ),
        (
            "share_state_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            |c| {
                c.share_coordinator.state_topic_segment_bytes = krabka_units::kibibytes(1);
            },
        ),
    ];

    assert_invalid_runtime_cases(cases.iter().copied());
}

#[test]
fn validate_rejects_a_non_positive_schema_registry_http_timeout() {
    let c = BrokerConfig {
        schema_registry_http_timeout: Time::ZERO,
        ..base()
    };
    assert!(c.validate().is_err());
}

#[test]
fn metadata_snapshot_fetch_max_cannot_raise_the_core_security_ceiling() {
    let cfg = BrokerConfig {
        metadata_snapshot_fetch_max: gibibytes(2),
        ..BrokerConfig::default()
    };

    let error = cfg.validate().expect_err("over-ceiling limit must fail");
    assert!(error.to_string().contains("metadata_snapshot_fetch_max"));
}

/// The whole-config check `Broker::start` runs states the same bounds on the
/// two KIP-98 expiry knobs that the file-config validator does, because an
/// embedding caller that builds a `BrokerConfig` in process never passes
/// through that validator.
///
/// Both keys are `ConfigDef.Type::INT` in Kafka, so the value the sweep runs
/// on has to be a whole number of milliseconds an `i32` holds -- otherwise
/// `DescribeConfigs` saturates and reports an expiry the broker is not using.
/// The expiry itself must run (Kafka's `atLeast(1)`); zero on the cadence
/// disables the sweep and is accepted.
#[test]
fn rejects_expiry_settings_outside_the_kafka_int_millisecond_range() {
    let cases: [RuntimeInvalidator; 6] = [
        ("txn_id_expiration must be within 1ms..=2147483647ms", |c| {
            c.txn_id_expiration = <Time as TimeExt>::ZERO;
        }),
        ("txn_id_expiration must be within 1ms..=2147483647ms", |c| {
            c.txn_id_expiration = <Time as TimeExt>::from_millis(i64::from(i32::MAX) + 1);
        }),
        (
            "txn_id_expiration must be a whole number of milliseconds",
            |c| {
                c.txn_id_expiration = nanos(1_500_000);
            },
        ),
        (
            "txn_id_expiration_cleanup_interval must be within 0ms..=2147483647ms",
            |c| {
                c.txn_id_expiration_cleanup_interval =
                    <Time as TimeExt>::from_millis(i64::from(i32::MAX) + 1);
            },
        ),
        (
            "txn_id_expiration_cleanup_interval must be a whole number of milliseconds",
            |c| {
                c.txn_id_expiration_cleanup_interval = nanos(1_500_000);
            },
        ),
        (
            "txn_id_expiration_cleanup_interval must be within 0ms..=2147483647ms",
            |c| {
                c.txn_id_expiration_cleanup_interval = <Time as TimeExt>::from_millis(-1);
            },
        ),
    ];

    assert_invalid_runtime_cases(cases.iter().copied());
}

/// A zero cadence disables the sweep, so it is a value the whole-config check
/// must accept, and the default expiry has to pass its own bound.
#[test]
fn a_disabled_expiry_sweep_and_the_kafka_defaults_validate() {
    let config = BrokerConfig {
        txn_id_expiration_cleanup_interval: <Time as TimeExt>::ZERO,
        ..base()
    };
    assert!(config.validate().is_ok());
}

/// Kafka's `group.initial.rebalance.delay.ms` is `atLeast(0)`, and its sample
/// `server.properties` sets zero for development.
#[test]
fn a_zero_classic_group_initial_rebalance_delay_validates() {
    let config = BrokerConfig {
        classic_group_initial_rebalance_delay: <Time as TimeExt>::ZERO,
        ..base()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn a_negative_classic_group_initial_rebalance_delay_is_rejected() {
    let config = BrokerConfig {
        classic_group_initial_rebalance_delay: <Time as TimeExt>::from_millis(-1),
        ..base()
    };
    assert_invalid_runtime(
        &config,
        "classic_group_initial_rebalance_delay must be finite and nonnegative",
    );
}
