//! The production [`Default`] constructor: the value the broker takes for
//! every knob an operator does not set.

use {
    crate::{
        authorizer::AllowAllAuthorizer,
        clean_shutdown::UNPROVEN,
        client_metrics::config::INTERVAL_MS_DEFAULT,
        config::{
            BreakGlassConfig, BrokerConfig, DEFAULT_AUDIT_CHECKPOINT_EVERY,
            DEFAULT_AUDIT_CHECKPOINT_EVERY_N, DEFAULT_AUDIT_SPOOL_DIR, DEFAULT_AUDIT_SPOOL_MAX,
            DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N, DEFAULT_AUDIT_TOPIC, DEFAULT_BROKER_ID,
            DEFAULT_CONTROLLER_ELECTION_TIMEOUT, DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL,
            DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL, DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME,
            DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD, DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL,
            DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE, DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE,
            DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT,
            DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT, DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
            DEFAULT_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_TIMEOUT,
            DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, DEFAULT_JWKS_REFRESH_INTERVAL,
            DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL, DEFAULT_LOG_FILTER,
            DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS, DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            DEFAULT_METADATA_SNAPSHOT_FETCH_MAX, DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            DEFAULT_OBSERVER_LAG_BOUND, DEFAULT_QUOTA_WINDOW, DEFAULT_REMOTE_COPIER_THREADS,
            DEFAULT_REMOTE_COPY_TIMEOUT, DEFAULT_REMOTE_EXPIRATION_THREADS,
            DEFAULT_REMOTE_INDEX_CACHE_SIZE, DEFAULT_REMOTE_LOG_MANAGER_INTERVAL,
            DEFAULT_REMOTE_READER_MAX_PENDING_TASKS, DEFAULT_REMOTE_READER_THREADS,
            DEFAULT_REPLICA_LAG_TIME_MAX, DEFAULT_TLS_RELOAD_INTERVAL,
            DEFAULT_TXN_ABORT_CLEANUP_INTERVAL, DEFAULT_TXN_ID_EXPIRATION,
            DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL, FreezeConfig, KafkaRlmmConfig, NodeRole,
            PlainCredentials, ReplicationRuntimeConfig, RlmmKind, StaticConfigOrigins,
            feature_flags::default_feature_flags, shared_epoch_ms,
        },
        coordinator::unified::{
            config::NextGenConfig, share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
        },
        network::auth::SslPrincipalMapper,
        operator_keys::OperatorKeys,
        replica_selector::ReplicaSelectorKind,
        share_coordinator::config::ShareCoordinatorConfig,
        topic_policy::TopicPolicy,
    },
    krabka_compression::RecordDecompressionPolicy,
    krabka_log::LogConfig,
    krabka_raft::{
        BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
        MetadataRaftFetchMax, NodeId,
    },
    krabka_units::{bytes, hours, kibibytes, mebibytes, millis, minutes, secs},
    std::{collections::HashMap, net::SocketAddr, path::PathBuf},
};

impl Default for BrokerConfig {
    #[allow(clippy::too_many_lines)]
    fn default() -> Self {
        let addr: SocketAddr = "127.0.0.1:9092".parse().expect("hard-coded valid addr");
        let controller_addr: SocketAddr = "127.0.0.1:9093".parse().expect("hard-coded valid addr");
        let record_decompression = RecordDecompressionPolicy::default();
        Self {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            startup_leader_wait_timeout: minutes(2),
            self_registration_backoff_min: millis(100),
            self_registration_backoff_max: secs(5),
            observer_poll_interval: millis(100),
            audit_spool_replay_interval: secs(2),
            audit_stats_poll_interval: secs(1),
            audit_partition_wait_timeout: secs(10),
            liveness_tick_interval: secs(1),
            gauge_poll_interval: secs(1),
            isr_scan_interval: secs(1),
            cleaner_interval: secs(30),
            log_retention_check_interval: minutes(5),
            future_log_move_retry_backoff: millis(50),
            client_metrics_eviction_tick: minutes(1),
            client_metrics_stale_floor: minutes(1),
            client_metrics_default_interval: millis(INTERVAL_MS_DEFAULT.unsigned_abs()),
            client_metrics_otlp_queue_capacity: 256,
            client_metrics_telemetry_max: mebibytes(1),
            client_metrics_prom_snapshot_ttl: minutes(5),
            rlmm_reconcile_tick: secs(30),
            rlmm_bootstrap_backoff_initial: millis(250),
            rlmm_bootstrap_backoff_max: secs(10),
            connection_creation_throttle_max: secs(1),
            opa_http_timeout: secs(5),
            schema_registry_http_timeout: secs(5),
            oauth_jwks_http_timeout: secs(10),
            auto_join_retry_backoff: millis(500),
            auto_join_voter_request_timeout: secs(30),
            replication: ReplicationRuntimeConfig::default(),
            coordinator_session_expiry_tick: secs(1),
            coordinator_shutdown_ack_timeout: secs(5),
            classic_group_initial_rebalance_delay: secs(3),
            sync_group_follower_wait: secs(30),
            unclean_recovery_aggressive_deadline: secs(2),
            unclean_recovery_balanced_deadline: secs(30),
            operator_recovery_deadline: secs(25),
            quota_throttle_max: secs(1),
            quota_window: DEFAULT_QUOTA_WINDOW,
            controller_mutation_quota_window: DEFAULT_QUOTA_WINDOW,
            self_registration_max_attempts: 8,
            observer_fetch_max: mebibytes(1),
            fetch_max_bytes: bytes(57_671_680),
            audit_event_queue_capacity: 8_192,
            audit_tail_window_offsets: 4_096,
            audit_tail_read_max: mebibytes(1),
            client_metrics_stale_push_intervals: 3,
            coordinator_actor_mailbox_capacity: 64,
            diskless_wal_local_replica_count: DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT,
            diskless_wal_flush_interval: DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL,
            diskless_wal_flush_max_size: DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
            diskless_wal_hot_tail_max_size: DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE,
            diskless_wal_trim_safety_lag: DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
            diskless_wal_index_projection_timeout: DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT,
            unclean_recovery_queue_capacity: 256,
            share_coordinator_load_buffer_size: mebibytes(5),
            share_session_cache_max_when_unlimited: 10_000,
            socket_request_max: mebibytes(100),
            sasl_server_max_receive: kibibytes(512),
            connection_failed_authentication_delay: millis(100),
            queued_max_requests: 500,
            queued_max_request_bytes: None,
            // The floor of `benches/fetch_drain.rs`'s sweep, not a
            // measured crossover: nothing below 4 KiB has been benchmarked.
            // `BrokerConfig::sendfile_min`'s doc comment has the sweep, and
            // the `bench` job of the `ci` workflow is what re-measures it.
            sendfile_min: kibibytes(4),
            socket_send_buffer: mebibytes(1),
            socket_receive_buffer: mebibytes(1),
            record_decompression_max_ratio: record_decompression.max_ratio(),
            record_decompression_output_floor: record_decompression.output_floor(),
            record_decompression_output_ceiling: record_decompression.output_ceiling(),
            inter_broker_server_name: "localhost".to_string(),
            producer_id_expiration: hours(24),
            producer_id_expiration_scan_interval: minutes(10),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            default_min_insync_replicas: 1,
            default_message_timestamp_before_max_ms: None,
            default_message_timestamp_after_max_ms: Some(3_600_000),
            num_partitions: 1,
            default_replication_factor: 1,
            delete_topic_enable: true,
            auto_create_topics_enable: true,
            max_request_partition_size_limit: 2000,
            offset_metadata_max_bytes: 4096,
            future_log_move_read_chunk: mebibytes(1),
            offsets_topic_num_partitions: 50,
            offsets_retention_override: None,
            offsets_retention_check_interval_override: None,
            offsets_topic_replication_factor: 3,
            offsets_topic_segment_bytes: mebibytes(100),
            transaction_state_num_partitions: 50,
            transaction_recovery_read_max: mebibytes(1),
            transaction_state_replication_factor: 3,
            transaction_state_segment_bytes: mebibytes(100),
            transaction_state_min_isr: 2,
            transaction_max_timeout: minutes(15),
            transaction_partition_verification_enable: true,
            barrier_state_num_partitions: 50,
            barrier_state_replication_factor: 3,
            barrier_min_injection_interval: secs(1),
            barrier_injection_timeout: secs(30),
            barrier_recovery_read_max: mebibytes(1),
            barrier_retained_cuts: 100,
            barrier_max_groups: 100,
            barrier_max_topics_per_group: 100,
            broker_id: DEFAULT_BROKER_ID,
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            listen_addr: addr,
            advertised_listener: addr.to_string(),
            log_dir: PathBuf::from("./krabka-data"),
            extra_log_dirs: Vec::new(),
            metadata_log_dir: None,
            cordoned_log_dirs: None,
            log_config: LogConfig::default(),
            stamp_source: None,
            node_id: NodeId(1),
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
            controller_server_name: None,
            bootstrap_servers: vec![],
            directory_id: uuid::Uuid::from_u128(1),
            incarnation_id: uuid::Uuid::nil(),
            previous_broker_epoch: UNPROVEN,
            broker_epoch: -1,
            auto_join: false,
            observer_lag_bound: DEFAULT_OBSERVER_LAG_BOUND,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            replica_lag_time_max: DEFAULT_REPLICA_LAG_TIME_MAX,
            controller_election_timeout: DEFAULT_CONTROLLER_ELECTION_TIMEOUT,
            controller_heartbeat_interval: DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL,
            controller_heartbeat_interval_explicit: false,
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            metadata_max_bytes_between_snapshots: DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS,
            metadata_max_snapshot_interval: DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            metadata_snapshot_interval_records: DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            metadata_snapshot_fetch_max: DEFAULT_METADATA_SNAPSHOT_FETCH_MAX,
            metadata_log: krabka_raft::MetadataLogConfig::default(),
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            rack: None,
            replica_selector: ReplicaSelectorKind::Leader,
            stretch: None,
            listeners: vec![],
            controller_listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            inter_broker_principal_node_ids: HashMap::new(),
            plain_credentials: PlainCredentials::default(),
            super_users: std::collections::HashSet::new(),
            authorizer: std::sync::Arc::new(AllowAllAuthorizer),
            schema_validator: None,
            operator_keys: OperatorKeys::default(),
            freeze: FreezeConfig::default(),
            break_glass: BreakGlassConfig::default(),
            topic_policy: TopicPolicy::default(),
            tls_config: None,
            tls_principal_mapper: SslPrincipalMapper::default(),
            enabled_sasl_mechanisms: vec![],
            oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
            gssapi: None,
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            oauthbearer_idp_tls_trust: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: shared_epoch_ms(),
            oauthbearer_jwks_cache_generation: std::sync::Arc::new(
                std::sync::atomic::AtomicU64::new(0),
            ),
            oauthbearer_jwks_last_on_demand_refresh_ms: shared_epoch_ms(),
            oauthbearer_jwks_min_on_demand_pause: DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE,
            features: default_feature_flags(),
            // KIP-98/KIP-939 idle-transaction reaper cadence (Kafka's
            // `transaction.abort.timed.out.transaction.cleanup.interval.ms`).
            txn_abort_cleanup_interval: DEFAULT_TXN_ABORT_CLEANUP_INTERVAL,
            // KIP-98 transactional-id expiry (Kafka's
            // `transactional.id.expiration.ms` and
            // `transaction.remove.expired.transaction.cleanup.interval.ms`).
            txn_id_expiration: DEFAULT_TXN_ID_EXPIRATION,
            txn_id_expiration_cleanup_interval: DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL,
            static_config_origins: StaticConfigOrigins::default(),
            next_gen_consumer_group: Box::new(NextGenConfig::default()),
            share_group: Box::new(ShareGroupConfig::default()),
            streams_group: Box::new(StreamsGroupConfig::default()),
            share_coordinator: Box::new(ShareCoordinatorConfig::default()),
            leader_imbalance_check_interval: DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            tls_reload_interval: DEFAULT_TLS_RELOAD_INTERVAL,
            // Default to `None` so multi-broker library users (and
            // multi-broker tests) don't race on a fixed port. The
            // `krabka-broker` binary opts in to `Some(0.0.0.0:9404)`
            // via its `--metrics-listen-addr` CLI flag — the operator
            // sets that via env, so production deployments still get
            // metrics by default.
            metrics_listen_addr: None,
            profiling: krabka_telemetry::profiling::ProfilingConfig::default(),
            client_metrics_enable: false,
            client_metrics_otlp_endpoint: None,
            client_metrics_otlp_protocol: krabka_telemetry::OtlpProtocol::Grpc,
            // The broker binary replaces this with the controller that drives
            // the subscriber it installed. The seed matters for every other
            // way a config is built: it carries the same directives as
            // `DEFAULT_LOG_FILTER`, so a `BROKER_LOGGER` describe lists the
            // krabka targets before anything has logged through them.
            log_levels: krabka_telemetry::LogLevelController::new(DEFAULT_LOG_FILTER).0,
            partition_disk_scan_interval: secs(60),
            max_incremental_fetch_session_cache_slots:
                DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            // Connection caps unlimited by default, matching Kafka's
            // max.connections / max.connections.per.ip (Integer.MAX_VALUE).
            max_connections: usize::MAX,
            max_connections_per_ip: usize::MAX,
            // Unset: the broker runs Kafka's connections.max.idle.ms default
            // and reports the key at DEFAULT_CONFIG. No per-listener override
            // until an operator writes one.
            connections_max_idle: None,
            connections_max_idle_overrides: std::collections::BTreeMap::new(),
            connections_max_reauth: None,
            connections_max_reauth_overrides: std::collections::BTreeMap::new(),
            // Master key off by default. Operators flip this on
            // via `KRABKA_DELEGATION_TOKEN_SECRET_KEY` env var or the
            // `[delegation_token] secret_key` TOML stanza.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME,
            delegation_token_expiry_check_interval: DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL,
            delegation_token_default_renew_period: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD,
            // Tiered storage off by default. Operators enable it
            // via `[remote_storage] storage_dir` in `broker.toml`.
            remote_storage_backend: None,
            remote_log_manager_interval: DEFAULT_REMOTE_LOG_MANAGER_INTERVAL,
            remote_copy_timeout: DEFAULT_REMOTE_COPY_TIMEOUT,
            remote_copier_threads: DEFAULT_REMOTE_COPIER_THREADS,
            remote_expiration_threads: DEFAULT_REMOTE_EXPIRATION_THREADS,
            // Production default: topic-backed RLMM. `bootstrap` and
            // `snapshot_dir` are empty; the broker derives them at startup.
            remote_log_metadata: RlmmKind::TopicBacked(KafkaRlmmConfig::default()),
            // WORM archive mode off by default. Operators enable it via
            // `[remote_storage.worm]` in `broker.toml`.
            remote_storage_worm: None,
            remote_reader_threads: DEFAULT_REMOTE_READER_THREADS,
            remote_reader_max_pending_tasks: DEFAULT_REMOTE_READER_MAX_PENDING_TASKS,
            remote_index_cache_size: DEFAULT_REMOTE_INDEX_CACHE_SIZE,
            // Audit enabled by default (secure-by-default / `FedRAMP` MLA).
            audit_enabled: true,
            audit_failure_mode: krabka_audit::AuditMode::FailOpen,
            audit_topic: DEFAULT_AUDIT_TOPIC.to_string(),
            audit_signing_key_path: None,
            audit_signing_key_id: None,
            audit_checkpoint_every_n: DEFAULT_AUDIT_CHECKPOINT_EVERY_N,
            audit_checkpoint_every: DEFAULT_AUDIT_CHECKPOINT_EVERY,
            audit_spool_dir: std::path::PathBuf::from(DEFAULT_AUDIT_SPOOL_DIR),
            audit_spool_max: DEFAULT_AUDIT_SPOOL_MAX,
            audit_spool_sync_every_n: DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{bytes, gibibytes},
    };

    #[test]
    fn operational_policy_defaults_match_existing_behavior() {
        let config = BrokerConfig::default();

        assert!(
            (
                config.startup_leader_wait_timeout,
                config.self_registration_backoff_min,
                config.self_registration_backoff_max,
                config.observer_poll_interval,
                config.audit_spool_replay_interval,
                config.audit_stats_poll_interval,
                config.audit_partition_wait_timeout,
                config.liveness_tick_interval,
            ) == (
                minutes(2),
                millis(100),
                secs(5),
                millis(100),
                secs(2),
                secs(1),
                secs(10),
                secs(1),
            )
        );
        assert!(
            (
                config.gauge_poll_interval,
                config.isr_scan_interval,
                config.cleaner_interval,
                config.future_log_move_retry_backoff,
                config.client_metrics_eviction_tick,
                config.client_metrics_stale_floor,
            ) == (
                secs(1),
                secs(1),
                secs(30),
                millis(50),
                minutes(1),
                minutes(1)
            )
        );
        assert!(
            (
                config.client_metrics_default_interval,
                config.client_metrics_otlp_queue_capacity,
                config.client_metrics_telemetry_max,
                config.client_metrics_prom_snapshot_ttl,
                config.rlmm_reconcile_tick,
                config.rlmm_bootstrap_backoff_initial,
                config.rlmm_bootstrap_backoff_max,
                config.connection_creation_throttle_max,
                config.opa_http_timeout,
                config.oauth_jwks_http_timeout,
                config.auto_join_retry_backoff,
            ) == (
                minutes(5),
                256,
                mebibytes(1),
                minutes(5),
                secs(30),
                millis(250),
                secs(10),
                secs(1),
                secs(5),
                secs(10),
                millis(500),
            )
        );
        assert!(
            config.replication
                == ReplicationRuntimeConfig {
                    // Kafka's `num.replica.fetchers` default.
                    fetchers: 1,
                    fetch_max: mebibytes(1),
                    fetch_max_wait: millis(500),
                    fetch_min: bytes(1),
                    throttle_exhausted_backoff: millis(100),
                    send_error_backoff: secs(1),
                    unknown_topic_retry_delay: millis(100),
                    epoch_fence_backoff: millis(200),
                    unexpected_error_backoff: millis(500),
                    reconnect_initial_delay: millis(100),
                    reconnect_delay_cap: secs(5),
                }
        );
        assert!(
            (
                config.coordinator_session_expiry_tick,
                config.coordinator_shutdown_ack_timeout,
                config.classic_group_initial_rebalance_delay,
                config.sync_group_follower_wait,
                config.unclean_recovery_aggressive_deadline,
                config.unclean_recovery_balanced_deadline,
                config.operator_recovery_deadline,
                config.quota_throttle_max,
                config.controller_mutation_quota_window,
            ) == (
                secs(1),
                secs(5),
                secs(3),
                secs(30),
                secs(2),
                secs(30),
                secs(25),
                // Kafka's `ClientRequestQuotaManager` bounds the request-quota
                // throttle at one `quota.window.size.seconds` (#709).
                secs(1),
                // KIP-599: Kafka's `controller.quota.window.num` (11) x
                // `controller.quota.window.size.seconds` (1).
                secs(11),
            )
        );
    }

    #[test]
    fn barrier_defaults_match_the_documented_policy() {
        let config = BrokerConfig::default();
        let actual = (
            config.barrier_state_num_partitions,
            config.barrier_state_replication_factor,
            config.barrier_min_injection_interval,
            config.barrier_injection_timeout,
            config.barrier_recovery_read_max,
            config.barrier_retained_cuts,
            config.barrier_max_groups,
            config.barrier_max_topics_per_group,
        );

        assert!(actual == (50, 3, secs(1), secs(30), mebibytes(1), 100, 100, 100));
    }

    #[test]
    fn defaults_use_conservative_raft_timings() {
        let c = BrokerConfig::default();
        assert!(c.controller_election_timeout == DEFAULT_CONTROLLER_ELECTION_TIMEOUT);
        assert!(c.controller_heartbeat_interval == DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL);
    }

    #[test]
    fn defaults_carry_no_schema_validator_and_a_five_second_registry_timeout() {
        let c = BrokerConfig::default();
        assert!(c.schema_validator.is_none());
        assert!(c.schema_registry_http_timeout == secs(5));

        let t = BrokerConfig::for_tests(PathBuf::from("/tmp/schema-registry-defaults"));
        assert!(t.schema_validator.is_none());
        assert!(t.schema_registry_http_timeout == secs(5));
    }

    #[test]
    fn default_metadata_snapshot_interval() {
        let cfg = BrokerConfig::default();
        assert!(cfg.metadata_snapshot_interval_records == 10_000);
        assert!(cfg.metadata_snapshot_fetch_max == gibibytes(1));
    }

    #[test]
    fn defaults_use_bootstrap_mode() {
        let c = BrokerConfig::default();
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }
}
