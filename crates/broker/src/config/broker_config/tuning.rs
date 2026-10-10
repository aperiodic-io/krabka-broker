//! The scalar tuning knobs: how often each background loop runs, how long the
//! broker waits for a peer or a partition, how deep its queues are, how many
//! bytes it reads or accepts in one step, and the partition count and
//! replication factor of each internal topic. They sit together because they
//! are all plain numbers an operator retunes, not policies that carry a type
//! of their own.

// Link 1 of the `BrokerConfig` field chain: it adds this group to the
// fields collected so far and hands them to `identity_fields`.
macro_rules! tuning_fields {
    ($($collected:tt)*) => {
        identity_fields! {
            $($collected)*
            /// Capacity used by every outbound Kafka client connection owned by this
            /// broker process.
            pub client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
            /// Maximum frame size used by every outbound Kafka client connection owned
            /// by this broker process.
            pub client_frame_max: krabka_client_core::ClientFrameMax,
            /// Maximum time to wait for a controller leader during startup.
            pub startup_leader_wait_timeout: Time,
            /// Initial delay between self-registration attempts.
            pub self_registration_backoff_min: Time,
            /// Maximum delay between self-registration attempts.
            pub self_registration_backoff_max: Time,
            /// Observer promotion polling cadence.
            pub observer_poll_interval: Time,
            /// Audit spool replay cadence.
            pub audit_spool_replay_interval: Time,
            /// Audit statistics polling cadence.
            pub audit_stats_poll_interval: Time,
            /// Maximum wait for the audit partition to become available.
            pub audit_partition_wait_timeout: Time,
            /// Broker liveness maintenance cadence.
            pub liveness_tick_interval: Time,
            /// Broker gauge refresh cadence.
            pub gauge_poll_interval: Time,
            /// In-sync replica maintenance cadence.
            pub isr_scan_interval: Time,
            /// Log cleaner maintenance cadence.
            pub cleaner_interval: Time,
            /// Local-retention maintenance cadence: how often the broker
            /// applies `retention.ms`, `retention.bytes` and `segment.ms` to
            /// every log it hosts. Kafka's `log.retention.check.interval.ms`.
            pub log_retention_check_interval: Time,
            /// Retry delay when moving a future log fails.
            pub future_log_move_retry_backoff: Time,
            /// Client-metrics cache eviction cadence.
            pub client_metrics_eviction_tick: Time,
            /// Minimum age at which client metrics are stale.
            pub client_metrics_stale_floor: Time,
            /// Default client telemetry subscription interval.
            pub client_metrics_default_interval: Time,
            /// Capacity of the client-metrics OTLP forwarding queue.
            pub client_metrics_otlp_queue_capacity: usize,
            /// Maximum accepted client telemetry payload size.
            pub client_metrics_telemetry_max: ByteSize,
            /// Prometheus client-metrics snapshot lifetime.
            pub client_metrics_prom_snapshot_ttl: Time,
            /// Remote-log metadata reconciliation cadence.
            pub rlmm_reconcile_tick: Time,
            /// Initial remote-log metadata bootstrap retry delay.
            pub rlmm_bootstrap_backoff_initial: Time,
            /// Maximum remote-log metadata bootstrap retry delay.
            pub rlmm_bootstrap_backoff_max: Time,
            /// Maximum connection-creation quota delay. Governed by KIP-612 connection quota enforcement.
            pub connection_creation_throttle_max: Time,
            /// OPA authorization request timeout.
            pub opa_http_timeout: Time,
            /// Schema registry request timeout.
            pub schema_registry_http_timeout: Time,
            /// OAuth JWKS HTTP request timeout.
            pub oauth_jwks_http_timeout: Time,
            /// Dynamic-quorum auto-join retry delay.
            pub auto_join_retry_backoff: Time,
            /// Timeout carried by dynamic-quorum `AddRaftVoter` requests.
            pub auto_join_voter_request_timeout: Time,
            /// Follower replication runtime policy.
            pub replication: ReplicationRuntimeConfig,
            /// Consumer-group session expiry scan cadence.
            pub coordinator_session_expiry_tick: Time,
            /// Maximum wait for coordinator shutdown acknowledgements.
            pub coordinator_shutdown_ack_timeout: Time,
            /// Initial delay before a classic group begins rebalancing.
            pub classic_group_initial_rebalance_delay: Time,
            /// Maximum time a follower waits for a `SyncGroup` assignment.
            pub sync_group_follower_wait: Time,
            /// Aggressive unclean-recovery collection deadline.
            pub unclean_recovery_aggressive_deadline: Time,
            /// Balanced unclean-recovery collection deadline.
            pub unclean_recovery_balanced_deadline: Time,
            /// Operator-triggered recovery deadline.
            pub operator_recovery_deadline: Time,
            /// Maximum request-quota throttle delay (default 1 s), bounding how long one response over `request_percentage` mutes a client. Equivalent to Kafka's `ClientRequestQuotaManager.maxThrottleTimeMs`, one `quota.window.size.seconds`. Byte-rate and controller-mutation throttles are not bounded.
            pub quota_throttle_max: Time,
            /// Time window sizing the byte-rate quota token bucket burst capacity (default 11 s). Equivalent to Kafka's sampling window `quota.window.num * quota.window.size.seconds`.
            pub quota_window: Time,
            /// Time window sizing the KIP-599 controller-mutation token bucket burst capacity (default 11 s): the bucket holds `window x controller_mutation_rate` tokens. Equivalent to Kafka's `controller.quota.window.num * controller.quota.window.size.seconds`.
            pub controller_mutation_quota_window: Time,
            /// Maximum self-registration attempts before startup fails.
            pub self_registration_max_attempts: u32,
            /// Maximum bytes fetched by a metadata observer request.
            pub observer_fetch_max: ByteSize,
            /// Broker-wide maximum bytes returned by consumer `Fetch` and
            /// `ShareFetch` requests, Kafka's `fetch.max.bytes`.
            pub fetch_max_bytes: ByteSize,
            /// Capacity of the asynchronous audit event queue.
            pub audit_event_queue_capacity: usize,
            /// Number of offsets included in an audit tail request.
            pub audit_tail_window_offsets: i64,
            /// Maximum bytes read by an audit tail request.
            pub audit_tail_read_max: ByteSize,
            /// Number of stale push intervals before client metrics expire.
            pub client_metrics_stale_push_intervals: u32,
            /// Mailbox capacity for coordinator actors.
            pub coordinator_actor_mailbox_capacity: usize,
            /// Local replica count for diskless WAL mode.
            pub diskless_wal_local_replica_count: usize,
            /// Cadence of diskless WAL object-store flushes.
            pub diskless_wal_flush_interval: Time,
            /// Maximum bytes included in one diskless WAL object-store flush.
            pub diskless_wal_flush_max_size: ByteSize,
            /// Broker-wide byte ceiling for quorum-committed hot-tail batches.
            pub diskless_wal_hot_tail_max_size: ByteSize,
            /// Committed offsets retained behind the diskless WAL trim frontier.
            pub diskless_wal_trim_safety_lag: i64,
            /// Maximum wait for a published diskless WAL index record to be projected.
            pub diskless_wal_index_projection_timeout: Time,
            /// Capacity of the unclean-recovery work queue.
            pub unclean_recovery_queue_capacity: usize,
            /// Maximum bytes read while recovering share state.
            pub share_coordinator_load_buffer_size: ByteSize,
            /// Share-session cache ceiling when group count is unlimited.
            pub share_session_cache_max_when_unlimited: usize,
            /// Maximum encoded request size accepted from a socket (matches Kafka socket.request.max.bytes).
            pub socket_request_max: ByteSize,
            /// Largest request frame a connection may send before it finishes
            /// authenticating on a SASL listener. Matches Kafka's
            /// `sasl.server.max.receive.size`, default 512 KiB. It replaces
            /// `socket_request_max` for that stretch, and a larger frame fails
            /// the authentication.
            pub sasl_server_max_receive: ByteSize,
            /// How long a failed SASL authentication holds its response and
            /// the close that follows. Matches Kafka's
            /// `connection.failed.authentication.delay.ms`, default 100 ms.
            /// Zero closes at once.
            pub connection_failed_authentication_delay: Time,
            /// Maximum number of resident queued requests across all connections (matches Kafka queued.max.requests).
            pub queued_max_requests: usize,
            /// Maximum resident request bytes across all connections (matches Kafka queued.max.request.bytes).
            pub queued_max_request_bytes: Option<ByteSize>,
            /// Minimum response size eligible for `sendfile`.
            ///
            /// The default is 4 KiB, the floor of the sweep in
            /// `benches/fetch_drain.rs`. That sweep drains a whole response
            /// over a loopback socket at 4, 16, 32, 64 and 256 KiB, through
            /// the kernel and through the `pread` + write copy, and finds no
            /// crossover inside its range: the kernel drain wins at every
            /// size, by 15% to 23% at 4 KiB and by 30% to 50% from 16 KiB up.
            ///
            /// The sweep has never measured below 4 KiB, so where the
            /// crossover actually is -- or whether there is one at all -- is
            /// unknown. The threshold sits at the smallest size that was
            /// measured rather than below it for that reason and no other.
            /// Widening the sweep downwards is a change to
            /// `benches/fetch_drain.rs`, and the wider result would appear in
            /// the `bench` job of the `ci` workflow, which runs that suite on
            /// the nightly schedule and prints it into the job summary; the
            /// numbers above are what the latest scheduled `ci` run in the
            /// Actions tab reproduces.
            pub sendfile_min: ByteSize,
            /// Broker socket send-buffer size.
            pub socket_send_buffer: ByteSize,
            /// Broker socket receive-buffer size.
            pub socket_receive_buffer: ByteSize,
            /// Maximum accepted Kafka record decompression ratio.
            pub record_decompression_max_ratio: Ratio,
            /// Minimum Kafka record decompression output allowance.
            pub record_decompression_output_floor: ByteSize,
            /// Maximum Kafka record decompression output allowance.
            pub record_decompression_output_ceiling: ByteSize,
            /// TLS server name used for outbound inter-broker connections.
            pub inter_broker_server_name: String,
            /// Producer-id inactivity period before state expires.
            pub producer_id_expiration: Time,
            /// Producer-state expiry scan cadence.
            pub producer_id_expiration_scan_interval: Time,
            /// Maximum produce requests combined into one append group.
            pub max_produce_group: usize,
            /// Capacity of each partition-writer request queue.
            pub partition_writer_queue_depth: usize,
            /// Default minimum in-sync replica count.
            pub default_min_insync_replicas: i32,
            /// `log.message.timestamp.before.max.ms` broker default: how far
            /// behind this broker's clock a `CreateTime` record may sit on a
            /// topic that names no `message.timestamp.before.max.ms`
            /// override. `None` is Kafka's unbounded `Long.MAX_VALUE`
            /// default.
            pub default_message_timestamp_before_max_ms: Option<i64>,
            /// `log.message.timestamp.after.max.ms` broker default: how far
            /// ahead of this broker's clock a `CreateTime` record may sit on
            /// a topic that names no `message.timestamp.after.max.ms`
            /// override. Kafka's default is one hour.
            pub default_message_timestamp_after_max_ms: Option<i64>,
            /// KIP-464: the partition count of a topic that `CreateTopics`
            /// creates with `num_partitions = -1`. Kafka's `num.partitions`,
            /// default 1.
            pub num_partitions: i32,
            /// KIP-464: the replication factor of a topic that `CreateTopics`
            /// creates with `replication_factor = -1`. Kafka's
            /// `default.replication.factor`, default 1.
            pub default_replication_factor: i16,
            /// Whether `DeleteTopics` may delete a topic. Kafka's
            /// `delete.topic.enable`, default `true`.
            pub delete_topic_enable: bool,
            /// Whether `Metadata` may create a topic it is asked about and
            /// that does not exist, when the request allows it. Kafka's
            /// `auto.create.topics.enable`, default `true`.
            pub auto_create_topics_enable: bool,
            /// Upper clamp on `DescribeTopicPartitions`'
            /// `response_partition_limit`. Kafka's
            /// `max.request.partition.size.limit`
            /// (`ServerConfigs.MAX_REQUEST_PARTITION_SIZE_LIMIT_DEFAULT`),
            /// default 2000.
            pub max_request_partition_size_limit: i32,
            /// Longest committed metadata an `OffsetCommit` partition may
            /// carry, in UTF-16 code units. Kafka's
            /// `offset.metadata.max.bytes`, default 4096.
            pub offset_metadata_max_bytes: i32,
            /// Bytes copied per future-log move read.
            pub future_log_move_read_chunk: ByteSize,
            /// Partition count for the consumer-offsets internal topic.
            pub offsets_topic_num_partitions: i32,
            /// KIP-211: how long a committed offset survives after the group
            /// that owns it loses its last member, when the operator set it.
            /// Kafka's `offsets.retention.minutes`, whose default is 10080
            /// (7 days). The retention sweep tombstones an empty group's
            /// offsets once this much time has passed, and then tombstones the
            /// group itself.
            ///
            /// `None` means the operator named nothing, so the broker runs
            /// [`BrokerConfig::offsets_retention`] — and `DescribeConfigs`
            /// reports the key at `DEFAULT_CONFIG` rather than
            /// `STATIC_BROKER_CONFIG`, which is the distinction Kafka draws by
            /// asking whether the key appears in `KafkaConfig.originals`.
            pub offsets_retention_override: Option<Time>,
            /// Cadence of the offset-retention sweep, when the operator set
            /// it. Kafka's `offsets.retention.check.interval.ms`, whose
            /// default is 600000 (10 minutes). `None` carries the same meaning
            /// as on [`offsets_retention_override`](Self::offsets_retention_override).
            pub offsets_retention_check_interval_override: Option<Time>,
            /// Desired replication factor for the consumer-offsets internal topic.
            pub offsets_topic_replication_factor: i16,
            /// `segment.bytes` of the consumer-offsets internal topic. Kafka's
            /// `offsets.topic.segment.bytes`.
            pub offsets_topic_segment_bytes: ByteSize,
            /// Partition count for the transaction-state internal topic.
            pub transaction_state_num_partitions: i32,
            /// Maximum bytes requested by each transaction-state recovery read.
            pub transaction_recovery_read_max: ByteSize,
            /// Desired replication factor for the transaction-state internal topic.
            pub transaction_state_replication_factor: i16,
            /// `segment.bytes` of the transaction-state internal topic. Kafka's
            /// `transaction.state.log.segment.bytes`.
            pub transaction_state_segment_bytes: ByteSize,
            /// `min.insync.replicas` of the transaction-state internal topic.
            /// Kafka's `transaction.state.log.min.isr`.
            pub transaction_state_min_isr: i32,
            /// Maximum accepted transaction timeout.
            pub transaction_max_timeout: Time,
            /// The static layer of Kafka's dynamic
            /// `transaction.partition.verification.enable`
            /// (`KafkaConfig.transactionPartitionVerificationEnable`): whether a
            /// partition leader checks with the transaction coordinator that a
            /// transaction contains a partition before it appends transactional
            /// records to it. A dynamic broker config, per-broker or cluster-wide,
            /// overrides it.
            pub transaction_partition_verification_enable: bool,
            /// Partition count for the `__barrier_state` internal topic.
            pub barrier_state_num_partitions: i32,
            /// Desired replication factor for the `__barrier_state` internal topic.
            pub barrier_state_replication_factor: i16,
            /// Shortest periodic injection interval a barrier group may ask for.
            pub barrier_min_injection_interval: Time,
            /// Deadline for one barrier injection to reach every target partition.
            pub barrier_injection_timeout: Time,
            /// Maximum bytes requested by each `__barrier_state` recovery read.
            pub barrier_recovery_read_max: ByteSize,
            /// Number of cuts a barrier group keeps before it tombstones the oldest.
            pub barrier_retained_cuts: i32,
            /// Maximum number of barrier groups the cluster accepts.
            pub barrier_max_groups: usize,
            /// Maximum number of topics in one barrier group.
            pub barrier_max_topics_per_group: usize,
        }
    };
}
