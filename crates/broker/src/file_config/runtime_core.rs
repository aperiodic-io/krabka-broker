//! The `[runtime]` appliers for the core broker timers and for replication.
//!
//! `apply_core` covers the startup, registration, audit, metrics, and RLMM
//! cadences; `apply_replication` covers the follower fetch loop's sizes and
//! backoffs. They share a module because both write the timings the broker
//! needs before it serves a single request.

use {
    super::{
        FileConfigError, RuntimeFileConfig,
        validate::{
            invalid_runtime_value, kafka_int_bytes, positive_time, voter_request_time,
            whole_bytes_i32, whole_millis_i32_time,
        },
    },
    crate::config::BrokerConfig,
};

impl RuntimeFileConfig {
    pub(super) fn apply_core(&mut self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;

        set_runtime! {
            runtime => cfg;
            positive_time: startup_leader_wait_timeout, self_registration_backoff_min,
                self_registration_backoff_max, observer_poll_interval,
                audit_spool_replay_interval, audit_stats_poll_interval,
                audit_partition_wait_timeout, liveness_tick_interval, gauge_poll_interval,
                isr_scan_interval, cleaner_interval, log_retention_check_interval,
                future_log_move_retry_backoff;
            plain: client_metrics_enable;
        }
        if let Some(enabled) = runtime.legacy_request_versions_enable {
            cfg.features.legacy_request_versions = enabled.into();
        }
        set_runtime! {
            runtime => cfg;
            kafka_int_bytes: fetch_max_bytes;
            positive_time: client_metrics_eviction_tick, client_metrics_stale_floor;
            whole_millis_i32_time: client_metrics_default_interval;
            whole_bytes_i32: client_metrics_telemetry_max;
            positive_time: client_metrics_prom_snapshot_ttl, rlmm_reconcile_tick,
                rlmm_bootstrap_backoff_initial, rlmm_bootstrap_backoff_max,
                connection_creation_throttle_max, opa_http_timeout,
                schema_registry_http_timeout, oauth_jwks_http_timeout, auto_join_retry_backoff;
            voter_request_time: auto_join_voter_request_timeout;
        }
        Ok(())
    }

    pub(super) fn apply_replication(
        &mut self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        if let Some(fetchers) = runtime.replica_fetchers {
            if fetchers == 0 {
                return Err(invalid_runtime_value(
                    "replica_fetchers",
                    "must be at least 1: a leader with no fetcher is never followed",
                ));
            }
            cfg.replication.fetchers = fetchers;
        }
        set_runtime! {
            runtime => cfg;
            whole_bytes_i32: replication_fetch_max => replication.fetch_max;
            whole_bytes_i32: replication_fetch_response_max => replication.fetch_response_max;
            whole_millis_i32_time: replication_fetch_max_wait => replication.fetch_max_wait;
            whole_bytes_i32: replication_fetch_min => replication.fetch_min;
            positive_time: replication_throttle_exhausted_backoff
                    => replication.throttle_exhausted_backoff,
                replication_send_error_backoff => replication.send_error_backoff,
                replication_unknown_topic_retry_delay => replication.unknown_topic_retry_delay,
                replication_epoch_fence_backoff => replication.epoch_fence_backoff,
                replication_unexpected_error_backoff => replication.unexpected_error_backoff,
                replication_reconnect_initial_delay => replication.reconnect_initial_delay,
                replication_reconnect_delay_cap => replication.reconnect_delay_cap;
        }
        Ok(())
    }
}
