//! The [`BrokerConfig::for_tests`] constructor: the same knobs as the
//! production defaults, retimed so an in-process fixture starts, fails over
//! and shuts down quickly.

use std::{net::SocketAddr, path::PathBuf};

use krabka_raft::NodeId;
use krabka_units::{Time, convert::TimeExt, millis, secs};

use crate::{
    config::{BrokerConfig, RlmmKind, feature_flags::test_feature_flags},
    coordinator::unified::{
        config::NextGenConfig, share::config::ShareGroupConfig, streams::config::StreamsGroupConfig,
    },
    share_coordinator::config::ShareCoordinatorConfig,
};

impl BrokerConfig {
    /// Builds a test config that listens on an OS-assigned port under a
    /// tempdir.
    #[must_use]
    /// # Panics
    /// Panics if the synchronized log state is poisoned.
    ///
    /// Panics if a segment that validated as nonempty is unexpectedly missing
    /// its required batch or index entry.
    pub fn for_tests(log_dir: PathBuf) -> Self {
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let controller_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        Self {
            quota_window: secs(1),
            controller_mutation_quota_window: secs(1),
            // A single-node fixture holds each coordinator topic on its one
            // broker. A multi-broker fixture sizes them with
            // `with_internal_topics_for`.
            offsets_topic_replication_factor: 1,
            transaction_state_replication_factor: 1,
            transaction_state_min_isr: 1,
            barrier_state_replication_factor: 1,
            listen_addr,
            advertised_listener: "127.0.0.1:0".into(),
            log_dir,
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
            incarnation_id: uuid::Uuid::new_v4(),
            heartbeat_interval: millis(200),
            heartbeat_timeout: secs(2),
            replica_lag_time_max: secs(2),
            // Short timings: single-node tests don't need quorum so split-vote
            // isn't a risk; multi-broker tests use these (via the shared
            // `support::start_n_node_with_retry` helper) so failover from a
            // dead controller leader completes well under the producer's
            // 10s timeout. The factor of ~10× vs. production defaults
            // is what makes `acks_all_completes_via_isr_shrink_when_follower_dead`
            // pass within its 5s assertion window.
            controller_election_timeout: millis(500),
            controller_heartbeat_interval: millis(100),
            // A test counts the metadata offsets its own writes land at, so the
            // controller appends no KIP-835 `NoOpRecord` between them.
            metadata_log: krabka_raft::MetadataLogConfig {
                max_idle_interval: secs(0),
                ..krabka_raft::MetadataLogConfig::default()
            },
            features: test_feature_flags(),
            // Reaper disabled in tests; suites that exercise it set it low.
            txn_abort_cleanup_interval: <Time as TimeExt>::ZERO,
            // The expiry itself keeps its production value, so a test that
            // ticks the sweep by hand sees the real window. The sweep task is
            // disabled, like the abort reaper above.
            txn_id_expiration_cleanup_interval: <Time as TimeExt>::ZERO,
            // Tests drive consumer and share heartbeats directly and expect
            // each change to be assigned at once, so they run without Kafka's
            // assignment interval, and a consumer group resolves its regular
            // expressions again as soon as the metadata changed, without
            // Kafka's ten seconds between two resolutions.
            next_gen_consumer_group: Box::new(NextGenConfig {
                assignment_interval: std::time::Duration::ZERO,
                regex_refresh_min_interval: std::time::Duration::ZERO,
                ..NextGenConfig::default()
            }),
            share_group: Box::new(ShareGroupConfig {
                assignment_interval: std::time::Duration::ZERO,
                ..ShareGroupConfig::default()
            }),
            // Tests drive streams heartbeats directly and expect each change to
            // be assigned at once, so they run without Kafka's initial
            // rebalance delay and assignment interval, as Kafka's own
            // integration tests set `group.streams.initial.rebalance.delay.ms`
            // to 0.
            streams_group: Box::new(StreamsGroupConfig {
                initial_rebalance_delay: std::time::Duration::ZERO,
                assignment_interval: std::time::Duration::ZERO,
                ..StreamsGroupConfig::default()
            }),
            share_coordinator: Box::new(ShareCoordinatorConfig {
                state_topic_replication_factor: 1,
                state_topic_min_isr: 1,
                ..ShareCoordinatorConfig::default()
            }),
            // Short interval so hot-reload tests don't wait long for a
            // watcher tick. Tests that don't care can ignore it.
            tls_reload_interval: millis(200),
            // Disable the disk scanner by default in tests so the
            // background task doesn't tick during short-lived fixtures.
            // Integration tests enable this explicitly when needed.
            partition_disk_scan_interval: <Time as TimeExt>::ZERO,
            // Tests that turn tiered storage on want quick offload, so the
            // for_tests default is well below the 30s production value.
            remote_log_manager_interval: secs(2),
            // Tests use the in-memory RLMM fixture.
            remote_log_metadata: RlmmKind::InMemory,
            // Every other knob keeps its production value: audit on, metrics
            // endpoint and tiered storage off, delegation tokens off,
            // connection caps unlimited, the production remote-tier copy
            // deadline and reader bounds.
            ..Self::default()
        }
    }

    /// Sizes the coordinator internal topics for a test cluster of `brokers`
    /// brokers.
    ///
    /// The replication factor of `__consumer_offsets`,
    /// `__transaction_state`, `__share_group_state` and `__barrier_state`
    /// becomes `brokers`, with a maximum of 3. The minimum ISR of
    /// `__transaction_state` and `__share_group_state` becomes one less than
    /// that replication factor, with a minimum of 1. With 3 or more brokers,
    /// these are Kafka's defaults: replication factor 3 and minimum ISR 2.
    #[must_use]
    pub fn with_internal_topics_for(mut self, brokers: usize) -> Self {
        let replication_factor: i16 = match brokers {
            0 | 1 => 1,
            2 => 2,
            _ => 3,
        };
        let min_isr = i32::from(replication_factor - 1).max(1);
        self.offsets_topic_replication_factor = replication_factor;
        self.transaction_state_replication_factor = replication_factor;
        self.transaction_state_min_isr = min_isr;
        self.barrier_state_replication_factor = replication_factor;
        self.share_coordinator.state_topic_replication_factor = replication_factor;
        self.share_coordinator.state_topic_min_isr = min_isr;
        self
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::BootstrapMode;
    use krabka_units::{convert::ByteSizeExt, mebibytes};

    use super::*;

    fn additional_policy_snapshot(config: BrokerConfig) -> [String; 24] {
        [
            config.self_registration_max_attempts.to_string(),
            config.observer_fetch_max.bytes_u64().to_string(),
            config.audit_event_queue_capacity.to_string(),
            config.audit_tail_window_offsets.to_string(),
            config.audit_tail_read_max.bytes_u64().to_string(),
            config.client_metrics_stale_push_intervals.to_string(),
            config.coordinator_actor_mailbox_capacity.to_string(),
            config.unclean_recovery_queue_capacity.to_string(),
            config
                .share_coordinator_load_buffer_size
                .bytes_u64()
                .to_string(),
            config.share_session_cache_max_when_unlimited.to_string(),
            config.socket_request_max.bytes_u64().to_string(),
            config.sendfile_min.bytes_u64().to_string(),
            config.socket_send_buffer.bytes_u64().to_string(),
            config.socket_receive_buffer.bytes_u64().to_string(),
            config.inter_broker_server_name,
            config.producer_id_expiration.millis_i64().to_string(),
            config
                .producer_id_expiration_scan_interval
                .millis_i64()
                .to_string(),
            config.max_produce_group.to_string(),
            config.partition_writer_queue_depth.to_string(),
            config.default_min_insync_replicas.to_string(),
            config.future_log_move_read_chunk.bytes_u64().to_string(),
            config
                .share_coordinator
                .state_topic_num_partitions
                .to_string(),
            config.transaction_state_num_partitions.to_string(),
            config.transaction_max_timeout.millis_i32().to_string(),
        ]
    }

    #[test]
    fn additional_operational_policy_defaults_match_existing_behavior() {
        let actual = additional_policy_snapshot(BrokerConfig::default());
        assert!(
            actual
                == [
                    "8",
                    "1048576",
                    "8192",
                    "4096",
                    "1048576",
                    "3",
                    "64",
                    "256",
                    "5242880",
                    "10000",
                    "104857600",
                    "4096",
                    "1048576",
                    "1048576",
                    "localhost",
                    "86400000",
                    "600000",
                    "1024",
                    "64",
                    "1",
                    "1048576",
                    "50",
                    "50",
                    "900000",
                ]
        );
        assert!(additional_policy_snapshot(BrokerConfig::for_tests(PathBuf::new())) == actual);
    }

    #[test]
    fn for_tests_uses_port_0() {
        let c = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert!(c.listen_addr.port() == 0);
    }

    #[test]
    fn for_tests_uses_20_mib_metadata_snapshot_threshold() {
        let cfg = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(cfg.metadata_max_bytes_between_snapshots == mebibytes(20));
        assert!(cfg.metadata_max_bytes_between_snapshots.bytes_u64() == 20 * 1024 * 1024);
    }

    #[test]
    fn for_tests_uses_short_raft_timings_for_fast_failover() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        // Short enough that a 3-broker test can detect a dead leader and
        // re-elect within a few hundred ms — the failover tests
        // need failover well under their 10s producer timeout.
        assert!(c.controller_election_timeout <= millis(750));
        assert!(c.controller_heartbeat_interval <= millis(200));
    }

    fn internal_topic_sizes(config: &BrokerConfig) -> (i16, i16, i32, i16, i16, i32) {
        (
            config.offsets_topic_replication_factor,
            config.transaction_state_replication_factor,
            config.transaction_state_min_isr,
            config.barrier_state_replication_factor,
            config.share_coordinator.state_topic_replication_factor,
            config.share_coordinator.state_topic_min_isr,
        )
    }

    #[test]
    fn for_tests_sizes_internal_topics_for_one_broker() {
        let config = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert!(internal_topic_sizes(&config) == (1, 1, 1, 1, 1, 1));
    }

    #[test]
    fn with_internal_topics_for_caps_at_kafka_defaults() {
        let cases = [
            (1, (1, 1, 1, 1, 1, 1)),
            (2, (2, 2, 1, 2, 2, 1)),
            (3, (3, 3, 2, 3, 3, 2)),
            (5, (3, 3, 2, 3, 3, 2)),
        ];
        for (brokers, expected) in cases {
            let config =
                BrokerConfig::for_tests(PathBuf::from("/tmp")).with_internal_topics_for(brokers);
            assert!(
                internal_topic_sizes(&config) == expected,
                "brokers = {brokers}"
            );
        }
    }

    #[test]
    fn for_tests_uses_bootstrap_mode() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }
}
