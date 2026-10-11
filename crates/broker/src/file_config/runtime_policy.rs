//! The `[runtime]` applier for broker-wide operational policy.
//!
//! `apply_broker_policy` covers the metadata snapshot cadence, the metadata
//! log's segments and retention, the controlled shutdown and TLS reload
//! timers, the leader-imbalance thresholds, the connection ceilings, and the
//! delegation-token lifetimes — the knobs that govern the broker as a whole
//! rather than one subsystem.

use super::{
    FileConfigError, RuntimeFileConfig,
    validate::{
        disableable_millis_i32_time, kafka_long_bytes, metadata_log_segment_bytes,
        metadata_snapshot_fetch_max, nonnegative_millis_i64_time, nonnegative_time, positive_time,
        positive_u64, whole_bytes_u64, whole_millis_i32_time, whole_millis_i64_time,
    },
};
use crate::config::BrokerConfig;

impl RuntimeFileConfig {
    pub(super) fn apply_broker_policy(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! {
            runtime => cfg;
            // Zero is a valid `partition_disk_scan_interval`: it disables the
            // scanner, so this one is not held to `positive_time`.
            nonnegative_time: partition_disk_scan_interval;
            plain: observer_lag_bound;
            positive_time: heartbeat_interval, heartbeat_timeout, replica_lag_time_max,
                controller_election_timeout, controller_heartbeat_interval;
        }
        if runtime.controller_heartbeat_interval.is_some() {
            cfg.controller_heartbeat_interval_explicit = true;
        }
        if let Some(value) = runtime.controller_fetch_miss_limit {
            cfg.controller_fetch_miss_limit = krabka_raft::ControllerFetchMissLimit::new(value)
                .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.metadata_raft_command_queue_capacity {
            cfg.metadata_raft_command_queue_capacity =
                krabka_raft::MetadataRaftCommandQueueCapacity::new(value)
                    .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.metadata_raft_fetch_max {
            cfg.metadata_raft_fetch_max = krabka_raft::MetadataRaftFetchMax::try_from(value)
                .map_err(FileConfigError::InvalidConfig)?;
        }
        if let Some(value) = runtime.controlled_shutdown_drain_timeout {
            positive_time("controlled_shutdown_drain_timeout", value)?;
        }
        set_runtime! {
            runtime => cfg;
            whole_bytes_u64: metadata_max_bytes_between_snapshots;
            // Zero disables the time-based snapshot cap, so it is not held to
            // `positive_time`. The engine reads this cap in whole
            // milliseconds (Kafka's own `metadata.log.max.snapshot.interval.ms`
            // is an INT of milliseconds), so a sub-millisecond nonzero value
            // must be rejected here rather than silently truncating to 0 and
            // reading as disabled.
            disableable_millis_i32_time: metadata_max_snapshot_interval;
            positive_u64: metadata_snapshot_interval_records;
            metadata_snapshot_fetch_max: metadata_snapshot_fetch_max;
        }
        runtime.apply_metadata_log(&mut cfg.metadata_log)?;
        // Zero disables the reaper, so it is not held to `positive_time`.
        set_runtime! { runtime => cfg; nonnegative_time: txn_abort_cleanup_interval; }
        // Both KIP-98 expiry knobs are `ConfigDef.Type::INT` in Kafka and
        // `DescribeConfigs` reports them as such, so neither may hold a value
        // wider than an `i32` of milliseconds. The expiry itself must run, as
        // Kafka's `atLeast(1)` says; zero on the sweep cadence disables it.
        // Both are static broker configs `DescribeConfigs` reports, so the
        // fact that the operator named them is recorded beside the value: a
        // supplied setting heads the synonym chain at `STATIC_BROKER_CONFIG`
        // even when it happens to equal the built-in default.
        if let Some(value) = runtime.txn_id_expiration {
            cfg.txn_id_expiration = whole_millis_i32_time("txn_id_expiration", value)?;
            cfg.static_config_origins.txn_id_expiration = true;
        }
        if let Some(value) = runtime.txn_id_expiration_cleanup_interval {
            cfg.txn_id_expiration_cleanup_interval =
                disableable_millis_i32_time("txn_id_expiration_cleanup_interval", value)?;
            cfg.static_config_origins.txn_id_expiration_cleanup_interval = true;
        }
        set_runtime! {
            runtime => cfg;
            positive_time: leader_imbalance_check_interval;
            // Zero disables the periodic TLS watcher, so it is not held to
            // `positive_time`.
            nonnegative_time: tls_reload_interval;
            plain: max_incremental_fetch_session_cache_slots, max_connections,
                max_connections_per_ip;
            whole_millis_i64_time: delegation_token_max_lifetime,
                delegation_token_expiry_check_interval, delegation_token_default_renew_period;
            positive_time: remote_log_manager_interval;
        }
        Ok(())
    }

    /// Applies the five keys of Kafka's `MetadataLogConfig` to `metadata_log`.
    ///
    /// Each domain is the one Kafka's `ConfigDef` gives the key.
    /// `metadata.log.segment.bytes` is an `INT` with `atLeast(8388608)`, and
    /// `metadata.max.idle.interval.ms` is an `INT` with `atLeast(0)`, where
    /// zero stops the KIP-835 `NoOpRecord` appends. The other three are
    /// `LONG` values. Krabka reads all five in whole milliseconds or whole
    /// bytes, so a fractional value is refused here and does not round.
    ///
    /// Kafka gives the two retention keys a negative value to keep every
    /// snapshot. A TOML quantity has no negative form, so an omitted key
    /// keeps the built-in limit, and a supplied key always sets a limit.
    fn apply_metadata_log(
        &self,
        metadata_log: &mut krabka_raft::MetadataLogConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime! {
            runtime => metadata_log;
            metadata_log_segment_bytes: metadata_log_segment_bytes => segment_size;
            whole_millis_i64_time: metadata_log_segment_roll_interval => segment_roll_interval;
        }
        if let Some(value) = runtime.metadata_max_retention_bytes {
            metadata_log.max_retention_size =
                Some(kafka_long_bytes("metadata_max_retention_bytes", value)?);
        }
        if let Some(value) = runtime.metadata_max_retention {
            metadata_log.max_retention = Some(nonnegative_millis_i64_time(
                "metadata_max_retention",
                value,
            )?);
        }
        if let Some(value) = runtime.metadata_max_idle_interval {
            metadata_log.max_idle_interval =
                disableable_millis_i32_time("metadata_max_idle_interval", value)?;
        }
        Ok(())
    }
}
