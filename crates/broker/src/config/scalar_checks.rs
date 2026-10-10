//! The positivity and range checks over the broker's runtime scalars: every
//! interval, budget, capacity, count and topic geometry value that must be
//! inside its bounds before the broker starts.

use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};

use crate::{BrokerError, config::BrokerConfig};

#[cfg(test)]
mod tests;

impl BrokerConfig {
    /// The broker-wide time knobs. They sit in their own pass, apart from the
    /// replication timings and the size and count limits, because one function
    /// holding all three lists outgrows the length limit.
    fn validate_positive_broker_times(&self) -> Result<(), BrokerError> {
        for (name, value) in [
            (
                "startup_leader_wait_timeout",
                self.startup_leader_wait_timeout,
            ),
            (
                "self_registration_backoff_min",
                self.self_registration_backoff_min,
            ),
            (
                "self_registration_backoff_max",
                self.self_registration_backoff_max,
            ),
            ("observer_poll_interval", self.observer_poll_interval),
            (
                "audit_spool_replay_interval",
                self.audit_spool_replay_interval,
            ),
            ("audit_stats_poll_interval", self.audit_stats_poll_interval),
            (
                "audit_partition_wait_timeout",
                self.audit_partition_wait_timeout,
            ),
            ("liveness_tick_interval", self.liveness_tick_interval),
            ("gauge_poll_interval", self.gauge_poll_interval),
            ("isr_scan_interval", self.isr_scan_interval),
            ("cleaner_interval", self.cleaner_interval),
            (
                "log_retention_check_interval",
                self.log_retention_check_interval,
            ),
            (
                "diskless_wal_flush_interval",
                self.diskless_wal_flush_interval,
            ),
            (
                "diskless_wal_index_projection_timeout",
                self.diskless_wal_index_projection_timeout,
            ),
            (
                "future_log_move_retry_backoff",
                self.future_log_move_retry_backoff,
            ),
            (
                "client_metrics_eviction_tick",
                self.client_metrics_eviction_tick,
            ),
            (
                "client_metrics_stale_floor",
                self.client_metrics_stale_floor,
            ),
            (
                "client_metrics_prom_snapshot_ttl",
                self.client_metrics_prom_snapshot_ttl,
            ),
            ("rlmm_reconcile_tick", self.rlmm_reconcile_tick),
            (
                "rlmm_bootstrap_backoff_initial",
                self.rlmm_bootstrap_backoff_initial,
            ),
            (
                "rlmm_bootstrap_backoff_max",
                self.rlmm_bootstrap_backoff_max,
            ),
            (
                "connection_creation_throttle_max",
                self.connection_creation_throttle_max,
            ),
            ("opa_http_timeout", self.opa_http_timeout),
            (
                "schema_registry_http_timeout",
                self.schema_registry_http_timeout,
            ),
            ("oauth_jwks_http_timeout", self.oauth_jwks_http_timeout),
            ("auto_join_retry_backoff", self.auto_join_retry_backoff),
            (
                "coordinator_session_expiry_tick",
                self.coordinator_session_expiry_tick,
            ),
            (
                "coordinator_shutdown_ack_timeout",
                self.coordinator_shutdown_ack_timeout,
            ),
            ("sync_group_follower_wait", self.sync_group_follower_wait),
            (
                "unclean_recovery_aggressive_deadline",
                self.unclean_recovery_aggressive_deadline,
            ),
            (
                "unclean_recovery_balanced_deadline",
                self.unclean_recovery_balanced_deadline,
            ),
            (
                "operator_recovery_deadline",
                self.operator_recovery_deadline,
            ),
            ("quota_throttle_max", self.quota_throttle_max),
            (
                "controller_mutation_quota_window",
                self.controller_mutation_quota_window,
            ),
            (
                "controller_heartbeat_interval",
                self.controller_heartbeat_interval,
            ),
            (
                "remote_log_manager_interval",
                self.remote_log_manager_interval,
            ),
            ("remote_copy_timeout", self.remote_copy_timeout),
            (
                "producer_id_expiration_scan_interval",
                self.producer_id_expiration_scan_interval,
            ),
            (
                "client_metrics_default_interval",
                self.client_metrics_default_interval,
            ),
            ("heartbeat_interval", self.heartbeat_interval),
            ("replica_lag_time_max", self.replica_lag_time_max),
            (
                "delegation_token_max_lifetime",
                self.delegation_token_max_lifetime,
            ),
            (
                "delegation_token_expiry_check_interval",
                self.delegation_token_expiry_check_interval,
            ),
            (
                "delegation_token_default_renew_period",
                self.delegation_token_default_renew_period,
            ),
            ("quota_throttle_max", self.quota_throttle_max),
            ("quota_window", self.quota_window),
            (
                "controller_mutation_quota_window",
                self.controller_mutation_quota_window,
            ),
        ] {
            require_positive_time(name, value)?;
        }
        self.validate_offset_retention()?;
        Ok(())
    }

    /// The two KIP-211 knobs, which only have a value when the operator set
    /// one.
    ///
    /// `offsets.retention.minutes` is a whole number of minutes on Kafka —
    /// `GroupCoordinatorConfig` declares it `INT` and multiplies by 60000 — so
    /// a duration krabka cannot state in whole minutes is refused rather than
    /// silently truncated. Truncating would make `DescribeConfigs` advertise a
    /// retention the sweep does not enforce.
    fn validate_offset_retention(&self) -> Result<(), BrokerError> {
        if let Some(retention) = self.offsets_retention_override {
            require_positive_time("offsets_retention", retention)?;
            if retention.millis_i64() % 60_000 != 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "offsets_retention must be a whole number of minutes, got {}ms",
                    retention.millis_i64()
                )));
            }
        }
        if let Some(interval) = self.offsets_retention_check_interval_override {
            require_positive_time("offsets_retention_check_interval", interval)?;
        }
        Ok(())
    }

    pub(super) fn validate_positive_runtime_scalars(&self) -> Result<(), BrokerError> {
        self.validate_positive_broker_times()?;
        for (name, value) in [
            (
                "replication.fetch_max_wait",
                self.replication.fetch_max_wait,
            ),
            (
                "replication.throttle_exhausted_backoff",
                self.replication.throttle_exhausted_backoff,
            ),
            (
                "replication.send_error_backoff",
                self.replication.send_error_backoff,
            ),
            (
                "replication.unknown_topic_retry_delay",
                self.replication.unknown_topic_retry_delay,
            ),
            (
                "replication.epoch_fence_backoff",
                self.replication.epoch_fence_backoff,
            ),
            (
                "replication.unexpected_error_backoff",
                self.replication.unexpected_error_backoff,
            ),
            (
                "replication.reconnect_initial_delay",
                self.replication.reconnect_initial_delay,
            ),
            (
                "replication.reconnect_delay_cap",
                self.replication.reconnect_delay_cap,
            ),
        ] {
            require_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "client_metrics_telemetry_max",
                self.client_metrics_telemetry_max,
            ),
            (
                "metadata_max_bytes_between_snapshots",
                self.metadata_max_bytes_between_snapshots,
            ),
            ("replication.fetch_max", self.replication.fetch_max),
            ("replication.fetch_min", self.replication.fetch_min),
        ] {
            require_positive_size(name, value)?;
        }
        if self.metadata_snapshot_interval_records == 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "metadata_snapshot_interval_records must be positive".into(),
            ));
        }
        krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(
            self.metadata_snapshot_fetch_max,
        )
        .map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!(
                "metadata_snapshot_fetch_max is invalid: {error}"
            ))
        })?;
        Ok(())
    }

    /// The two KIP-98 transactional-id expiry knobs.
    ///
    /// Both are `ConfigDef.Type::INT` in Kafka, so `DescribeConfigs` reports
    /// each as a whole number of milliseconds an `i32` holds. A caller that
    /// builds a [`BrokerConfig`] in process never passes through the
    /// file-config validator, so the same bounds are restated here: without
    /// them the value reported and the value the sweep runs on could differ,
    /// and a zero expiry would reap every eligible id at once.
    fn validate_txn_id_expiry_scalars(&self) -> Result<(), BrokerError> {
        require_whole_millis_i32(
            "txn_id_expiration",
            self.txn_id_expiration,
            MillisFloor::One,
        )?;
        require_whole_millis_i32(
            "txn_id_expiration_cleanup_interval",
            self.txn_id_expiration_cleanup_interval,
            MillisFloor::Zero,
        )
    }

    pub(super) fn validate_additional_runtime_scalars(&self) -> Result<(), BrokerError> {
        // The timeout is carried verbatim in `AddRaftVoter.timeout_ms`, an
        // `int32` millisecond wire field, so it has to survive that narrowing.
        let voter_request_timeout_ms = self.auto_join_voter_request_timeout.millis_i64();
        if !(1..=i64::from(i32::MAX)).contains(&voter_request_timeout_ms) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "auto_join_voter_request_timeout must be within 1..=i32::MAX milliseconds".into(),
            ));
        }
        self.validate_txn_id_expiry_scalars()?;
        // Kafka's `group.initial.rebalance.delay.ms` is `atLeast(0)`: zero
        // completes a new group's first rebalance as soon as it opens.
        require_nonnegative_time(
            "classic_group_initial_rebalance_delay",
            self.classic_group_initial_rebalance_delay,
        )?;
        require_positive_size("observer_fetch_max", self.observer_fetch_max)?;
        let fetch_max_bytes = self.fetch_max_bytes.bytes_u64();
        if !self.fetch_max_bytes.bytes_f64().is_finite()
            || ByteSize::from_bytes(fetch_max_bytes) != self.fetch_max_bytes
            || fetch_max_bytes > u64::try_from(i32::MAX).expect("i32::MAX fits u64")
        {
            return Err(BrokerError::InvalidRuntimeConfig(
                "fetch_max_bytes must be a whole number of bytes from 0 to 2147483647".into(),
            ));
        }
        if let Some(bytes) = self.queued_max_request_bytes {
            require_positive_size("queued_max_request_bytes", bytes)?;
        }
        if self.queued_max_requests == 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "queued_max_requests must be positive".into(),
            ));
        }
        for (name, value) in [
            (
                "self_registration_max_attempts",
                self.self_registration_max_attempts,
            ),
            (
                "client_metrics_stale_push_intervals",
                self.client_metrics_stale_push_intervals,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "client_metrics_otlp_queue_capacity",
                self.client_metrics_otlp_queue_capacity,
            ),
            (
                "audit_event_queue_capacity",
                self.audit_event_queue_capacity,
            ),
            (
                "coordinator_actor_mailbox_capacity",
                self.coordinator_actor_mailbox_capacity,
            ),
            (
                "diskless_wal_local_replica_count",
                self.diskless_wal_local_replica_count,
            ),
            (
                "unclean_recovery_queue_capacity",
                self.unclean_recovery_queue_capacity,
            ),
            (
                "share_session_cache_max_when_unlimited",
                self.share_session_cache_max_when_unlimited,
            ),
            ("barrier_max_groups", self.barrier_max_groups),
            (
                "barrier_max_topics_per_group",
                self.barrier_max_topics_per_group,
            ),
            ("max_produce_group", self.max_produce_group),
            (
                "partition_writer_queue_depth",
                self.partition_writer_queue_depth,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        if self.diskless_wal_local_replica_count.is_multiple_of(2) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "diskless_wal_local_replica_count must be odd".into(),
            ));
        }
        if self.diskless_wal_trim_safety_lag < 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "diskless_wal_trim_safety_lag must be nonnegative".into(),
            ));
        }
        for (name, value) in [
            ("audit_tail_read_max", self.audit_tail_read_max),
            (
                "diskless_wal_flush_max_size",
                self.diskless_wal_flush_max_size,
            ),
            (
                "diskless_wal_hot_tail_max_size",
                self.diskless_wal_hot_tail_max_size,
            ),
            (
                "share_coordinator_load_buffer_size",
                self.share_coordinator_load_buffer_size,
            ),
            ("barrier_recovery_read_max", self.barrier_recovery_read_max),
            ("socket_request_max", self.socket_request_max),
            ("sasl_server_max_receive", self.sasl_server_max_receive),
            ("sendfile_min", self.sendfile_min),
            ("socket_send_buffer", self.socket_send_buffer),
            ("socket_receive_buffer", self.socket_receive_buffer),
            (
                "future_log_move_read_chunk",
                self.future_log_move_read_chunk,
            ),
            (
                "transaction_recovery_read_max",
                self.transaction_recovery_read_max,
            ),
        ] {
            require_positive_size(name, value)?;
        }
        require_positive_time("producer_id_expiration", self.producer_id_expiration)?;
        if self.audit_tail_window_offsets <= 0 {
            return Err(BrokerError::InvalidRuntimeConfig(
                "audit_tail_window_offsets must be positive".into(),
            ));
        }
        self.validate_internal_topic_scalars()
    }

    /// The internal topics' partition counts, replication factors and the
    /// transaction and barrier timeouts.
    ///
    /// Split out of [`Self::validate_additional_runtime_scalars`] because that
    /// one function checks every remaining runtime scalar and had outgrown the
    /// length a reader can hold; these three tables are the part that stands
    /// on its own.
    fn validate_internal_topic_scalars(&self) -> Result<(), BrokerError> {
        for (name, value) in [
            (
                "default_min_insync_replicas",
                self.default_min_insync_replicas,
            ),
            ("num_partitions", self.num_partitions),
            (
                "max_request_partition_size_limit",
                self.max_request_partition_size_limit,
            ),
            (
                "share_state_num_partitions",
                self.share_coordinator.state_topic_num_partitions,
            ),
            (
                "offsets_topic_num_partitions",
                self.offsets_topic_num_partitions,
            ),
            (
                "transaction_state_num_partitions",
                self.transaction_state_num_partitions,
            ),
            (
                "barrier_state_num_partitions",
                self.barrier_state_num_partitions,
            ),
            ("barrier_retained_cuts", self.barrier_retained_cuts),
            ("transaction_state_min_isr", self.transaction_state_min_isr),
            (
                "share_state_min_isr",
                self.share_coordinator.state_topic_min_isr,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            ("transaction_max_timeout", self.transaction_max_timeout),
            (
                "barrier_min_injection_interval",
                self.barrier_min_injection_interval,
            ),
            ("barrier_injection_timeout", self.barrier_injection_timeout),
        ] {
            require_positive_time(name, value)?;
        }
        for (name, value) in [
            (
                "share_state_replication_factor",
                self.share_coordinator.state_topic_replication_factor,
            ),
            (
                "offsets_topic_replication_factor",
                self.offsets_topic_replication_factor,
            ),
            (
                "default_replication_factor",
                self.default_replication_factor,
            ),
            (
                "transaction_state_replication_factor",
                self.transaction_state_replication_factor,
            ),
            (
                "barrier_state_replication_factor",
                self.barrier_state_replication_factor,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        // Each value becomes the `segment.bytes` topic config of its internal
        // topic, so it must pass the topic-config check.
        for (name, value) in [
            (
                "offsets_topic_segment_bytes",
                self.offsets_topic_segment_bytes,
            ),
            (
                "transaction_state_segment_bytes",
                self.transaction_state_segment_bytes,
            ),
            (
                "share_state_segment_bytes",
                self.share_coordinator.state_topic_segment_bytes,
            ),
        ] {
            crate::config_keys::validate_topic_config(
                crate::config_keys::SEGMENT_BYTES,
                &value.bytes_u64().to_string(),
            )
            .map_err(|reason| BrokerError::InvalidRuntimeConfig(format!("{name}: {reason}")))?;
        }
        Ok(())
    }
}

/// Rejects a non-positive time extent. The error names the config field.
fn require_positive_time(name: &str, value: Time) -> Result<(), BrokerError> {
    if value <= <Time as TimeExt>::ZERO {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be positive"
        )));
    }
    Ok(())
}

/// Rejects a negative or non-finite time extent. The error names the config
/// field.
fn require_nonnegative_time(name: &str, value: Time) -> Result<(), BrokerError> {
    if !value.secs_f64().is_finite() || value < <Time as TimeExt>::ZERO {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be finite and nonnegative"
        )));
    }
    Ok(())
}

/// Rejects a non-positive byte count. The error names the config field.
fn require_positive_size(name: &str, value: ByteSize) -> Result<(), BrokerError> {
    if value <= <ByteSize as ByteSizeExt>::ZERO {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be positive"
        )));
    }
    Ok(())
}

/// The smallest millisecond value a [`require_whole_millis_i32`] field may
/// hold: `One` for a knob that must always run, which is Kafka's `atLeast(1)`,
/// and `Zero` for a cadence that zero disables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MillisFloor {
    Zero,
    One,
}

impl MillisFloor {
    fn lower_bound(self) -> i64 {
        match self {
            MillisFloor::Zero => 0,
            MillisFloor::One => 1,
        }
    }
}

/// Rejects a duration that is not a whole number of milliseconds inside the
/// range a Kafka `ConfigDef.Type::INT` can carry. The error names the config
/// field.
fn require_whole_millis_i32(
    name: &str,
    value: Time,
    floor: MillisFloor,
) -> Result<(), BrokerError> {
    if !value.secs_f64().is_finite() {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be finite"
        )));
    }
    let millis = value.millis_i64();
    if <Time as TimeExt>::from_millis(millis) != value {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be a whole number of milliseconds"
        )));
    }
    let lower = floor.lower_bound();
    if !(lower..=i64::from(i32::MAX)).contains(&millis) {
        return Err(BrokerError::InvalidRuntimeConfig(format!(
            "{name} must be within {lower}ms..=2147483647ms"
        )));
    }
    Ok(())
}
