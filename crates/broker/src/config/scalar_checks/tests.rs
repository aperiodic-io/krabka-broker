//! Behaviour tests for the runtime-scalar checks: which value each rejection
//! names, and the message it reports.

use {
    super::*,
    crate::config::test_support::{
        RuntimeInvalidator, assert_invalid_runtime, assert_invalid_runtime_cases, base,
    },
    assert2::assert,
    krabka_units::{gibibytes, millis, nanos},
};

#[test]
fn rejects_non_positive_runtime_scalars() {
    let cases: [RuntimeInvalidator; 23] = [
        RuntimeInvalidator {
            expected: "startup_leader_wait_timeout",
            invalidate: |c| {
                c.startup_leader_wait_timeout = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "cleaner_interval",
            invalidate: |c| {
                c.cleaner_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_flush_interval",
            invalidate: |c| {
                c.diskless_wal_flush_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_index_projection_timeout",
            invalidate: |c| {
                c.diskless_wal_index_projection_timeout = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_flush_max_size",
            invalidate: |c| {
                c.diskless_wal_flush_max_size = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_hot_tail_max_size",
            invalidate: |c| {
                c.diskless_wal_hot_tail_max_size = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "client_metrics_default_interval",
            invalidate: |c| {
                c.client_metrics_default_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "client_metrics_telemetry_max",
            invalidate: |c| {
                c.client_metrics_telemetry_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "client_metrics_otlp_queue_capacity",
            invalidate: |c| {
                c.client_metrics_otlp_queue_capacity = 0;
            },
        },
        RuntimeInvalidator {
            expected: "replication.fetch_max",
            invalidate: |c| {
                c.replication.fetch_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "replication.fetch_max_wait",
            invalidate: |c| {
                c.replication.fetch_max_wait = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "replication.fetch_min",
            invalidate: |c| {
                c.replication.fetch_min = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "replication.send_error_backoff",
            invalidate: |c| {
                c.replication.send_error_backoff = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "heartbeat_interval",
            invalidate: |c| {
                c.heartbeat_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "replica_lag_time_max",
            invalidate: |c| {
                c.replica_lag_time_max = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "controller_heartbeat_interval",
            invalidate: |c| {
                c.controller_heartbeat_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "metadata_max_bytes_between_snapshots",
            invalidate: |c| {
                c.metadata_max_bytes_between_snapshots = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "metadata_snapshot_interval_records",
            invalidate: |c| {
                c.metadata_snapshot_interval_records = 0;
            },
        },
        RuntimeInvalidator {
            expected: "delegation_token_max_lifetime",
            invalidate: |c| {
                c.delegation_token_max_lifetime = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "delegation_token_expiry_check_interval",
            invalidate: |c| {
                c.delegation_token_expiry_check_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "delegation_token_default_renew_period",
            invalidate: |c| {
                c.delegation_token_default_renew_period = -millis(1);
            },
        },
        RuntimeInvalidator {
            expected: "remote_log_manager_interval",
            invalidate: |c| {
                c.remote_log_manager_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "delegation_token_max_lifetime",
            invalidate: |c| {
                c.delegation_token_max_lifetime = -millis(1);
            },
        },
    ];

    for RuntimeInvalidator {
        expected,
        invalidate,
    } in cases
    {
        let mut config = BrokerConfig::default();
        invalidate(&mut config);
        assert_invalid_runtime(&config, &format!("{expected} must be positive"));
    }
}

/// The additional runtime scalars, each with the change that `validate` refuses.
fn additional_scalar_cases_a() -> Vec<RuntimeInvalidator> {
    vec![
        RuntimeInvalidator {
            expected: "self_registration_max_attempts must be positive",
            invalidate: |c| {
                c.self_registration_max_attempts = 0;
            },
        },
        RuntimeInvalidator {
            expected: "observer_fetch_max must be positive",
            invalidate: |c| {
                c.observer_fetch_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "audit_event_queue_capacity must be positive",
            invalidate: |c| {
                c.audit_event_queue_capacity = 0;
            },
        },
        RuntimeInvalidator {
            expected: "audit_tail_window_offsets must be positive",
            invalidate: |c| {
                c.audit_tail_window_offsets = 0;
            },
        },
        RuntimeInvalidator {
            expected: "audit_tail_read_max must be positive",
            invalidate: |c| {
                c.audit_tail_read_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "client_metrics_stale_push_intervals must be positive",
            invalidate: |c| c.client_metrics_stale_push_intervals = 0,
        },
        RuntimeInvalidator {
            expected: "coordinator_actor_mailbox_capacity must be positive",
            invalidate: |c| {
                c.coordinator_actor_mailbox_capacity = 0;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_local_replica_count must be positive",
            invalidate: |c| {
                c.diskless_wal_local_replica_count = 0;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_local_replica_count must be odd",
            invalidate: |c| {
                c.diskless_wal_local_replica_count = 2;
            },
        },
        RuntimeInvalidator {
            expected: "diskless_wal_trim_safety_lag must be nonnegative",
            invalidate: |c| {
                c.diskless_wal_trim_safety_lag = -1;
            },
        },
        RuntimeInvalidator {
            expected: "unclean_recovery_queue_capacity must be positive",
            invalidate: |c| {
                c.unclean_recovery_queue_capacity = 0;
            },
        },
        RuntimeInvalidator {
            expected: "share_coordinator_load_buffer_size must be positive",
            invalidate: |c| {
                c.share_coordinator_load_buffer_size = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "transaction_recovery_read_max must be positive",
            invalidate: |c| {
                c.transaction_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "share_session_cache_max_when_unlimited must be positive",
            invalidate: |c| c.share_session_cache_max_when_unlimited = 0,
        },
        RuntimeInvalidator {
            expected: "socket_request_max must be positive",
            invalidate: |c| {
                c.socket_request_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "sasl_server_max_receive must be positive",
            invalidate: |c| {
                c.sasl_server_max_receive = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "sendfile_min must be positive",
            invalidate: |c| {
                c.sendfile_min = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "socket_send_buffer must be positive",
            invalidate: |c| {
                c.socket_send_buffer = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "socket_receive_buffer must be positive",
            invalidate: |c| {
                c.socket_receive_buffer = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "producer_id_expiration must be positive",
            invalidate: |c| {
                c.producer_id_expiration = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "producer_id_expiration_scan_interval must be positive",
            invalidate: |c| c.producer_id_expiration_scan_interval = <Time as TimeExt>::ZERO,
        },
        RuntimeInvalidator {
            expected: "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            invalidate: |c| {
                c.auto_join_voter_request_timeout = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds",
            invalidate: |c| {
                c.auto_join_voter_request_timeout = nanos(1);
            },
        },
        RuntimeInvalidator {
            expected: "max_produce_group must be positive",
            invalidate: |c| {
                c.max_produce_group = 0;
            },
        },
        RuntimeInvalidator {
            expected: "partition_writer_queue_depth must be positive",
            invalidate: |c| {
                c.partition_writer_queue_depth = 0;
            },
        },
        RuntimeInvalidator {
            expected: "dedup_max_entries_per_partition must be within 1..=10000000",
            invalidate: |c| {
                c.dedup_max_entries_per_partition = 0;
            },
        },
        RuntimeInvalidator {
            expected: "dedup_max_entries_per_partition must be within 1..=10000000",
            invalidate: |c| {
                c.dedup_max_entries_per_partition = 10_000_001;
            },
        },
        RuntimeInvalidator {
            expected: "dedup_max_entries must be positive",
            invalidate: |c| {
                c.dedup_max_entries = 0;
            },
        },
    ]
}

/// The additional runtime scalars, each with the change that `validate` refuses.
fn additional_scalar_cases_b() -> Vec<RuntimeInvalidator> {
    vec![
        RuntimeInvalidator {
            expected: "default_min_insync_replicas must be positive",
            invalidate: |c| {
                c.default_min_insync_replicas = 0;
            },
        },
        RuntimeInvalidator {
            expected: "max_request_partition_size_limit must be positive",
            invalidate: |c| {
                c.max_request_partition_size_limit = 0;
            },
        },
        RuntimeInvalidator {
            expected: "future_log_move_read_chunk must be positive",
            invalidate: |c| {
                c.future_log_move_read_chunk = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "share_state_num_partitions must be positive",
            invalidate: |c| {
                c.share_coordinator.state_topic_num_partitions = 0;
            },
        },
        RuntimeInvalidator {
            expected: "share_state_replication_factor must be positive",
            invalidate: |c| {
                c.share_coordinator.state_topic_replication_factor = 0;
            },
        },
        RuntimeInvalidator {
            expected: "offsets_topic_num_partitions must be positive",
            invalidate: |c| {
                c.offsets_topic_num_partitions = 0;
            },
        },
        RuntimeInvalidator {
            expected: "offsets_topic_replication_factor must be positive",
            invalidate: |c| {
                c.offsets_topic_replication_factor = 0;
            },
        },
        RuntimeInvalidator {
            expected: "transaction_state_num_partitions must be positive",
            invalidate: |c| {
                c.transaction_state_num_partitions = 0;
            },
        },
        RuntimeInvalidator {
            expected: "transaction_state_replication_factor must be positive",
            invalidate: |c| c.transaction_state_replication_factor = 0,
        },
        RuntimeInvalidator {
            expected: "transaction_max_timeout must be positive",
            invalidate: |c| {
                c.transaction_max_timeout = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_state_num_partitions must be positive",
            invalidate: |c| {
                c.barrier_state_num_partitions = 0;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_state_replication_factor must be positive",
            invalidate: |c| {
                c.barrier_state_replication_factor = 0;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_retained_cuts must be positive",
            invalidate: |c| {
                c.barrier_retained_cuts = 0;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_max_groups must be positive",
            invalidate: |c| {
                c.barrier_max_groups = 0;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_max_topics_per_group must be positive",
            invalidate: |c| {
                c.barrier_max_topics_per_group = 0;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_recovery_read_max must be positive",
            invalidate: |c| {
                c.barrier_recovery_read_max = <ByteSize as ByteSizeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_min_injection_interval must be positive",
            invalidate: |c| {
                c.barrier_min_injection_interval = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "barrier_injection_timeout must be positive",
            invalidate: |c| {
                c.barrier_injection_timeout = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "transaction_state_min_isr must be positive",
            invalidate: |c| {
                c.transaction_state_min_isr = 0;
            },
        },
        RuntimeInvalidator {
            expected: "share_state_min_isr must be positive",
            invalidate: |c| {
                c.share_coordinator.state_topic_min_isr = 0;
            },
        },
        RuntimeInvalidator {
            expected: "offsets_topic_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            invalidate: |c| {
                c.offsets_topic_segment_bytes = krabka_units::kibibytes(1);
            },
        },
        RuntimeInvalidator {
            expected: "transaction_state_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            invalidate: |c| {
                c.transaction_state_segment_bytes = krabka_units::kibibytes(1);
            },
        },
        RuntimeInvalidator {
            expected: "share_state_segment_bytes: Invalid value 1024 for configuration segment.bytes: Value must be at least 1048576",
            invalidate: |c| {
                c.share_coordinator.state_topic_segment_bytes = krabka_units::kibibytes(1);
            },
        },
    ]
}

/// The additional runtime scalars, each with the change that `validate` refuses.
fn additional_scalar_cases() -> Vec<RuntimeInvalidator> {
    let mut cases = additional_scalar_cases_a();
    cases.extend(additional_scalar_cases_b());
    cases
}

#[test]
fn rejects_invalid_additional_runtime_scalars() {
    let cases = additional_scalar_cases();

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
        RuntimeInvalidator {
            expected: "txn_id_expiration must be within 1ms..=2147483647ms",
            invalidate: |c| {
                c.txn_id_expiration = <Time as TimeExt>::ZERO;
            },
        },
        RuntimeInvalidator {
            expected: "txn_id_expiration must be within 1ms..=2147483647ms",
            invalidate: |c| {
                c.txn_id_expiration = <Time as TimeExt>::from_millis(i64::from(i32::MAX) + 1);
            },
        },
        RuntimeInvalidator {
            expected: "txn_id_expiration must be a whole number of milliseconds",
            invalidate: |c| {
                c.txn_id_expiration = nanos(1_500_000);
            },
        },
        RuntimeInvalidator {
            expected: "txn_id_expiration_cleanup_interval must be within 0ms..=2147483647ms",
            invalidate: |c| {
                c.txn_id_expiration_cleanup_interval =
                    <Time as TimeExt>::from_millis(i64::from(i32::MAX) + 1);
            },
        },
        RuntimeInvalidator {
            expected: "txn_id_expiration_cleanup_interval must be a whole number of milliseconds",
            invalidate: |c| {
                c.txn_id_expiration_cleanup_interval = nanos(1_500_000);
            },
        },
        RuntimeInvalidator {
            expected: "txn_id_expiration_cleanup_interval must be within 0ms..=2147483647ms",
            invalidate: |c| {
                c.txn_id_expiration_cleanup_interval = <Time as TimeExt>::from_millis(-1);
            },
        },
    ];

    assert_invalid_runtime_cases(cases.iter().copied());
}

#[derive(Clone, Copy)]
enum AcceptedZeroScalar {
    DisabledExpirySweep,
    ClassicGroupInitialRebalanceDelay,
}

#[yare::parameterized(
    disabled_expiry_sweep = { AcceptedZeroScalar::DisabledExpirySweep },
    classic_group_initial_rebalance_delay = {
        AcceptedZeroScalar::ClassicGroupInitialRebalanceDelay
    },
)]
fn accepted_zero_valued_runtime_scalars_validate(case: AcceptedZeroScalar) {
    let config = match case {
        // A zero cadence disables the sweep, and the default expiry must pass
        // its own bound.
        AcceptedZeroScalar::DisabledExpirySweep => BrokerConfig {
            txn_id_expiration_cleanup_interval: <Time as TimeExt>::ZERO,
            ..base()
        },
        // Kafka's `group.initial.rebalance.delay.ms` is `atLeast(0)`, and its
        // sample `server.properties` sets zero for development.
        AcceptedZeroScalar::ClassicGroupInitialRebalanceDelay => BrokerConfig {
            classic_group_initial_rebalance_delay: <Time as TimeExt>::ZERO,
            ..base()
        },
    };
    assert2::assert!(config.validate().is_ok());
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

/// The KFC-10 dedup bounds accept both ends of the per-partition range and a
/// broker-wide budget of one.
#[test]
fn dedup_bounds_accept_the_ends_of_their_ranges() {
    for (per_partition, total) in [(1, 1), (10_000_000, 1), (1_000_000, 16_000_000)] {
        let config = BrokerConfig {
            dedup_max_entries_per_partition: per_partition,
            dedup_max_entries: total,
            ..base()
        };
        assert!(
            config.validate().is_ok(),
            "per_partition={per_partition} total={total}"
        );
    }
}
