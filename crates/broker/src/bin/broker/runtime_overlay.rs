//! The overlay of the command line onto a `BrokerConfig`.
//!
//! The flags first become a `RuntimeFileConfig`, which carries the same
//! precedence rules as the operator's TOML file, and that config then applies
//! to the `BrokerConfig` the broker starts from.

use {
    crate::{cli::Args, runtime_args::RuntimeArgs},
    krabka_broker::{BrokerConfig, config::DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT},
    krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity},
    krabka_units::Time,
};

impl RuntimeArgs {
    fn as_file_runtime(&self) -> krabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = krabka_broker::file_config::RuntimeFileConfig::default();
        self.copy_into(&mut runtime);
        runtime.streams_group_assignor = self.streams_group_assignor.map(|value| {
            use krabka_broker::coordinator::unified::streams::config::StreamsAssignorKind;
            match value {
                StreamsAssignorKind::Auto => "auto",
                StreamsAssignorKind::Sticky => "sticky",
                StreamsAssignorKind::HighlyAvailable => "highly-available",
            }
            .to_owned()
        });
        runtime
    }
}

impl Args {
    fn runtime_overlay(&self) -> krabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = self.runtime.as_file_runtime();
        runtime.partition_disk_scan_interval = self.partition_disk_scan_interval;
        runtime.observer_lag_bound = self.observer_lag_bound;
        runtime.metadata_max_bytes_between_snapshots = self.metadata_max_bytes_between_snapshots;
        runtime.metadata_max_snapshot_interval = self.metadata_max_snapshot_interval;
        runtime.metadata_snapshot_interval_records = self.metadata_snapshot_interval_records;
        runtime.metadata_snapshot_fetch_max = self.metadata_snapshot_fetch_max;
        runtime.metadata_log_segment_bytes = self.metadata_log_segment_bytes;
        runtime.metadata_log_segment_roll_interval = self.metadata_log_segment_roll_interval;
        runtime.metadata_max_retention_bytes = self.metadata_max_retention_bytes;
        runtime.metadata_max_retention = self.metadata_max_retention;
        runtime.metadata_max_idle_interval = self.metadata_max_idle_interval;
        runtime.txn_abort_cleanup_interval = self.txn_abort_cleanup_interval;
        runtime.txn_id_expiration = self.txn_id_expiration;
        runtime.txn_id_expiration_cleanup_interval = self.txn_id_expiration_cleanup_interval;
        runtime.leader_imbalance_check_interval = self.leader_imbalance_check_interval;
        runtime.tls_reload_interval = self.tls_reload_interval;
        runtime.heartbeat_interval = self.heartbeat_interval;
        runtime.heartbeat_timeout = self.heartbeat_timeout;
        runtime.replica_lag_time_max = self.replica_lag_time_max;
        runtime.controller_election_timeout = self.controller_election_timeout;
        runtime.controller_heartbeat_interval = self.controller_heartbeat_interval;
        runtime.controller_fetch_miss_limit = self.controller_fetch_miss_limit;
        runtime.metadata_raft_command_queue_capacity = self.metadata_raft_command_queue_capacity;
        runtime.metadata_raft_fetch_max = self.metadata_raft_fetch_max;
        runtime.controlled_shutdown_drain_timeout = self.controlled_shutdown_drain_timeout;
        runtime.delegation_token_max_lifetime = self.delegation_token_max_lifetime;
        runtime.delegation_token_expiry_check_interval =
            self.delegation_token_expiry_check_interval;
        runtime.delegation_token_default_renew_period = self.delegation_token_default_renew_period;
        runtime.remote_log_manager_interval = self.remote_log_manager_interval;
        runtime.max_incremental_fetch_session_cache_slots =
            self.max_incremental_fetch_session_cache_slots;
        runtime.max_connections = self.max_connections;
        runtime.max_connections_per_ip = self.max_connections_per_ip;
        runtime
    }

    pub fn apply_runtime_to(
        &self,
        cfg: &mut BrokerConfig,
        file_shutdown: Option<Time>,
    ) -> Result<Time, String> {
        let runtime = self.runtime_overlay();
        let cli_shutdown = runtime.controlled_shutdown_drain_timeout;
        runtime.apply_to(cfg).map_err(|error| error.to_string())?;
        cfg.client_dispatch_queue_capacity =
            ConnectionDispatchQueueCapacity::new(self.runtime.client_dispatch_queue_capacity)
                .expect("validated by clap");
        cfg.client_frame_max =
            ClientFrameMax::try_from(self.runtime.client_frame_max).expect("validated by clap");
        cfg.validate().map_err(|error| error.to_string())?;
        Ok(cli_shutdown
            .or(file_shutdown)
            .unwrap_or(DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT))
    }
}

// A `#[path]`-loaded module resolves its own children as siblings of itself, so
// the test module names its file.
#[cfg(test)]
#[path = "runtime_overlay/tests.rs"]
mod tests;
