//! The `[runtime]` TOML table and the entry point that applies it.
//!
//! [`RuntimeFileConfig`] mirrors every operational knob the broker reads from
//! `[runtime]`. Its [`apply_to`][RuntimeFileConfig::apply_to] method dispatches
//! to the per-domain appliers in the sibling `runtime_*` modules, which hold
//! the assignments themselves.

use krabka_units::{ByteSize, Ratio, Time};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::FileConfigError;

/// Validated operational policy loaded from `[runtime]`.
#[krabka_macros::runtime_policy_fields(toml)]
#[krabka_macros::human_units]
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFileConfig {
    /// Whether the broker advertises the KIP-714 client-metrics RPCs,
    /// `GetTelemetrySubscriptions` (71) and `PushTelemetry` (72). Kafka
    /// advertises them only when `metric.reporters` holds a `ClientTelemetry`
    /// implementation, so the default here is `false` as well and a client
    /// starts no telemetry handshake the broker has nowhere to forward. A
    /// configured `[telemetry]` OTLP endpoint turns them on without this key.
    pub client_metrics_enable: Option<bool>,
    /// Whether the broker serves the request versions Kafka 4.x removed:
    /// `Fetch` v0-v3, `ListOffsets` v0 and `Produce` v0-v2. krabka-only, with
    /// no Kafka equivalent. The default is `false`, which advertises and
    /// accepts exactly Kafka 4.3.1's minimums (`Fetch` v4, `ListOffsets` v1,
    /// and `Produce` v3 though it is still advertised from v0, KAFKA-18659)
    /// and closes a connection that sends an older version, as a 4.3.1 broker
    /// does. `true` keeps a pre-0.11 client working.
    pub legacy_request_versions_enable: Option<bool>,
    /// Maximum bytes returned by one consumer `Fetch` or `ShareFetch`,
    /// Kafka's `fetch.max.bytes`. Kafka's default is 57,671,680 bytes.
    pub fetch_max_bytes: Option<ByteSize>,
    /// Timeout for one schema-registry request.
    pub schema_registry_http_timeout: Option<Time>,
    /// Timeout for one OAuth JWKS fetch.
    pub oauth_jwks_http_timeout: Option<Time>,
    /// Retry delay between KIP-853 dynamic-quorum auto-join attempts.
    pub auto_join_retry_backoff: Option<Time>,
    /// Timeout carried by a dynamic-quorum `AddRaftVoter` request.
    pub auto_join_voter_request_timeout: Option<Time>,
    /// How many fetchers this broker runs per leader it follows, Kafka's
    /// `num.replica.fetchers`. Every partition followed from one leader is
    /// hashed onto one of that leader's fetchers, and each fetcher holds one
    /// connection and sends one batched `Fetch` per round.
    #[schemars(range(min = 1))]
    pub replica_fetchers: Option<usize>,
    /// Maximum number of members in one consumer group, Kafka's
    /// `group.consumer.max.size`.
    pub consumer_group_max_size: Option<usize>,
    /// Initial delay before a classic group begins rebalancing, Kafka's
    /// `group.initial.rebalance.delay.ms`. Zero completes a new group's first
    /// rebalance as soon as its first member joins.
    pub classic_group_initial_rebalance_delay: Option<Time>,
    /// Lower bound on a classic `JoinGroup` session timeout, Kafka's
    /// `group.min.session.timeout.ms`.
    pub classic_group_min_session_timeout: Option<Time>,
    /// Upper bound on a classic `JoinGroup` session timeout, Kafka's
    /// `group.max.session.timeout.ms`.
    pub classic_group_max_session_timeout: Option<Time>,
    /// Maximum number of members in one classic group, Kafka's
    /// `group.max.size`.
    pub classic_group_max_size: Option<usize>,
    /// Capacity of the asynchronous audit event queue.
    pub audit_event_queue_capacity: Option<usize>,
    /// Number of offsets included in one audit tail request.
    pub audit_tail_window_offsets: Option<i64>,
    /// Maximum bytes read by one audit tail request.
    pub audit_tail_read_max: Option<ByteSize>,
    /// Number of missed push intervals after which client metrics expire.
    pub client_metrics_stale_push_intervals: Option<u32>,
    /// Capacity of the client-metrics OTLP forwarding queue.
    pub client_metrics_otlp_queue_capacity: Option<usize>,
    /// Mailbox capacity of each coordinator actor.
    pub coordinator_actor_mailbox_capacity: Option<usize>,
    /// Local replica count used in diskless WAL mode.
    pub diskless_wal_local_replica_count: Option<usize>,
    /// Cadence of diskless WAL flushes to the object store.
    pub diskless_wal_flush_interval: Option<Time>,
    /// Maximum bytes included in one diskless WAL object-store flush.
    pub diskless_wal_flush_max_size: Option<ByteSize>,
    /// Broker-wide byte ceiling for quorum-committed diskless hot-tail
    /// batches.
    pub diskless_wal_hot_tail_max_size: Option<ByteSize>,
    /// Committed offsets retained behind the diskless WAL trim frontier.
    pub diskless_wal_trim_safety_lag: Option<i64>,
    /// Maximum wait for a published diskless WAL index record to be projected.
    pub diskless_wal_index_projection_timeout: Option<Time>,
    /// Capacity of the unclean-recovery work queue.
    pub unclean_recovery_queue_capacity: Option<usize>,
    /// Maximum bytes read by one share-state recovery read, Kafka's
    /// `share.coordinator.load.buffer.size`.
    pub share_coordinator_load_buffer_size: Option<ByteSize>,
    /// Ceiling on the share-session cache when the group count is unlimited.
    pub share_session_cache_max_when_unlimited: Option<usize>,
    /// Cap on the initial allocation a decoded or raw segment read makes.
    pub log_read_buffer_cap: Option<ByteSize>,
    /// Cap on how far past a fetch read's own byte range the broker asks the
    /// kernel to read ahead, so a consumer that is behind finds its next fetch
    /// in the page cache. The hint reaches up to one more range of the same
    /// size, and no further than this. The default is 4 MiB, and 0 limits the
    /// hint to the fetch's own range. The hint has no effect on a host other
    /// than Linux.
    pub log_read_ahead_max: Option<ByteSize>,
    /// How much of the newest data a partition with `preallocate=true` keeps
    /// in memory to serve reads with. Such a partition writes its active
    /// segment through `O_DIRECT`, which bypasses the page cache, so without
    /// this a consumer reading right behind the producer reads from disk.
    /// Every such partition holds up to this much, plus one batch. The
    /// default is 1 MiB, and 0 keeps only the newest batch.
    pub log_tail_cache_size: Option<ByteSize>,
    /// Size of the window a timestamp search reads the log in.
    pub log_timestamp_scan_window: Option<ByteSize>,
    /// Roll the active segment once it grows past this. Kafka's
    /// `log.segment.bytes`, the broker default for a topic's `segment.bytes`.
    pub log_segment_bytes: Option<ByteSize>,
    /// Kafka's broker-wide `message.max.bytes`: the largest record batch a
    /// topic that sets no `max.message.bytes` accepts.
    pub message_max_bytes: Option<ByteSize>,
    /// Declared bound on how far this broker's clock can be from true time. It
    /// has an effect only under the scheduled delivery policy: a batch becomes
    /// visible once `max_timestamp + log_delivery_clock_uncertainty <= now`,
    /// so delivery is never early and is late by at most twice this bound.
    pub log_delivery_clock_uncertainty: Option<Time>,
    /// Maximum encoded request size accepted from a socket, Kafka's
    /// `socket.request.max.bytes`.
    pub socket_request_max: Option<ByteSize>,
    /// Largest request frame a connection may send before it finishes
    /// authenticating on a SASL listener, Kafka's
    /// `sasl.server.max.receive.size`. It replaces `socket_request_max` for
    /// that stretch, and a larger frame fails the authentication.
    pub sasl_server_max_receive: Option<ByteSize>,
    /// How long a failed SASL authentication holds its response and the close
    /// that follows, Kafka's `connection.failed.authentication.delay.ms`. Zero
    /// closes at once.
    pub connection_failed_authentication_delay: Option<Time>,
    /// Maximum number of queued requests allowed in the broker dispatch queue,
    /// Kafka's `queued.max.requests`.
    pub queued_max_requests: Option<usize>,
    /// Maximum byte size across all queued requests before accepting
    /// additional requests is paused, Kafka's `queued.max.request.bytes`.
    pub queued_max_request_bytes: Option<ByteSize>,
    /// Minimum response size eligible for a `sendfile` kernel drain. Smaller
    /// responses go through the `pread` and write copy.
    pub sendfile_min: Option<ByteSize>,
    /// Broker socket send-buffer size, Kafka's `socket.send.buffer.bytes`.
    pub socket_send_buffer: Option<ByteSize>,
    /// Broker socket receive-buffer size, Kafka's
    /// `socket.receive.buffer.bytes`.
    pub socket_receive_buffer: Option<ByteSize>,
    /// Upper clamp on `DescribeTopicPartitions`' `response_partition_limit`,
    /// Kafka's `max.request.partition.size.limit`.
    pub max_request_partition_size_limit: Option<i32>,
    /// Maximum accepted decompression ratio for a produced record batch.
    pub record_decompression_max_ratio: Option<Ratio>,
    /// Minimum decompressed-output allowance granted to a record batch,
    /// whatever the ratio bound computes.
    pub record_decompression_output_floor: Option<ByteSize>,
    /// Maximum decompressed-output allowance granted to a record batch.
    pub record_decompression_output_ceiling: Option<ByteSize>,
    /// TLS SNI and SASL server name used for outbound inter-broker
    /// connections.
    pub inter_broker_server_name: Option<String>,
    /// How long a producer id may stay idle before its state expires, Kafka's
    /// `producer.id.expiration.ms`.
    pub producer_id_expiration: Option<Time>,
    /// Cadence of the producer-state expiry scan, Kafka's
    /// `producer.id.expiration.check.interval.ms`.
    pub producer_id_expiration_scan_interval: Option<Time>,
    /// Maximum number of produce requests combined into one append group.
    pub max_produce_group: Option<usize>,
    /// Capacity of each partition-writer request queue.
    pub partition_writer_queue_depth: Option<usize>,
    /// Broker default for a topic's `min.insync.replicas`, Kafka's
    /// `min.insync.replicas`. A topic override wins over it.
    pub default_min_insync_replicas: Option<i32>,
    /// Partition count of a topic that `CreateTopics` creates with
    /// `num_partitions = -1`, Kafka's `num.partitions`.
    pub num_partitions: Option<i32>,
    /// Replication factor of a topic that `CreateTopics` creates with
    /// `replication_factor = -1`, Kafka's `default.replication.factor`.
    pub default_replication_factor: Option<i16>,
    /// Bytes copied per read during a KIP-113 future-log move.
    pub future_log_move_read_chunk: Option<ByteSize>,
    /// Partition count of the `__share_group_state` internal topic, Kafka's
    /// `share.coordinator.state.topic.num.partitions`.
    pub share_state_num_partitions: Option<i32>,
    /// Replication factor of the `__share_group_state` internal topic, Kafka's
    /// `share.coordinator.state.topic.replication.factor`.
    pub share_state_replication_factor: Option<i16>,
    /// `segment.bytes` of the `__share_group_state` internal topic, Kafka's
    /// `share.coordinator.state.topic.segment.bytes`.
    pub share_state_segment_bytes: Option<ByteSize>,
    /// `min.insync.replicas` of the `__share_group_state` internal topic,
    /// Kafka's `share.coordinator.state.topic.min.isr`.
    pub share_state_min_isr: Option<i32>,
    /// Updates of one share key between two snapshots of it, Kafka's
    /// `share.coordinator.snapshot.update.records.per.snapshot`. At most 500.
    pub share_snapshot_update_records_per_snapshot: Option<u32>,
    /// How long an append to `__share_group_state` may take before the share
    /// coordinator answers `COORDINATOR_NOT_AVAILABLE`, Kafka's
    /// `share.coordinator.write.timeout.ms`.
    pub share_coordinator_write_timeout: Option<Time>,
    /// How often the share coordinator trims the redundant prefix of each
    /// `__share_group_state` partition it leads, Kafka's
    /// `share.coordinator.state.topic.prune.interval.ms`.
    pub share_state_prune_interval: Option<Time>,
    /// How old the latest snapshot of a share key may get before the share
    /// coordinator writes a new one, Kafka's
    /// `share.coordinator.cold.partition.snapshot.interval.ms`.
    pub share_cold_partition_snapshot_interval: Option<Time>,
    /// Codec of the batches the share coordinator appends to
    /// `__share_group_state`, Kafka's
    /// `share.coordinator.state.topic.compression.codec`, as Kafka's codec
    /// id: 0 none (the default), 1 gzip, 2 snappy, 3 lz4 or 4 zstd.
    pub share_state_compression_codec: Option<i32>,
    /// Kafka's `share.coordinator.threads`, at least 1. Accepted and has no
    /// effect: the share coordinator runs as tasks on the broker's shared
    /// async runtime rather than on a thread pool of its own.
    pub share_coordinator_threads: Option<i32>,
    /// Kafka's `share.coordinator.append.linger.ms`, a whole number of
    /// milliseconds, or -1 (the default) for an adaptive linger. Accepted and
    /// has no effect: each share-state write is its own append, and the
    /// partition writer groups concurrent appends without waiting for more.
    pub share_coordinator_append_linger_ms: Option<i32>,
    /// Kafka's `share.coordinator.cached.buffer.max.bytes`, at least 512KiB,
    /// default 1MiB plus 12 bytes. Accepted and has no effect: the share
    /// coordinator encodes each record into a new buffer and keeps no buffer
    /// for reuse.
    pub share_coordinator_cached_buffer_max_bytes: Option<ByteSize>,
    /// Partition count of the `__consumer_offsets` internal topic, Kafka's
    /// `offsets.topic.num.partitions`.
    pub offsets_topic_num_partitions: Option<i32>,
    /// Replication factor of the `__consumer_offsets` internal topic, Kafka's
    /// `offsets.topic.replication.factor`.
    pub offsets_topic_replication_factor: Option<i16>,
    /// `segment.bytes` of the `__consumer_offsets` internal topic, Kafka's
    /// `offsets.topic.segment.bytes`.
    pub offsets_topic_segment_bytes: Option<ByteSize>,
    /// How long a committed consumer offset is kept after its group becomes
    /// empty, Kafka's `offsets.retention.minutes`. It must be a whole number
    /// of minutes.
    pub offsets_retention: Option<Time>,
    /// Cadence of the expired-offset sweep, Kafka's
    /// `offsets.retention.check.interval.ms`.
    pub offsets_retention_check_interval: Option<Time>,
    /// Partition count of the `__transaction_state` internal topic, Kafka's
    /// `transaction.state.log.num.partitions`.
    pub transaction_state_num_partitions: Option<i32>,
    /// Maximum bytes requested by one `__transaction_state` recovery read.
    pub transaction_recovery_read_max: Option<ByteSize>,
    /// Replication factor of the `__transaction_state` internal topic, Kafka's
    /// `transaction.state.log.replication.factor`.
    pub transaction_state_replication_factor: Option<i16>,
    /// `segment.bytes` of the `__transaction_state` internal topic, Kafka's
    /// `transaction.state.log.segment.bytes`.
    pub transaction_state_segment_bytes: Option<ByteSize>,
    /// `min.insync.replicas` of the `__transaction_state` internal topic,
    /// Kafka's `transaction.state.log.min.isr`.
    pub transaction_state_min_isr: Option<i32>,
    /// Maximum transaction timeout a producer may request, Kafka's
    /// `transaction.max.timeout.ms`.
    pub transaction_max_timeout: Option<Time>,
    /// Whether a partition leader checks with the transaction coordinator
    /// that a transaction contains a partition before it appends
    /// transactional records to it, Kafka's
    /// `transaction.partition.verification.enable`. This is the static value.
    /// A dynamic broker config of the same name, per-broker or cluster-wide,
    /// overrides it, and the `server_properties` entry of that name applies
    /// only when this key is absent. The default is `true`.
    pub transaction_partition_verification_enable: Option<bool>,
    /// Partition count of the `__barrier_state` internal topic.
    pub barrier_state_num_partitions: Option<i32>,
    /// Replication factor of the `__barrier_state` internal topic.
    pub barrier_state_replication_factor: Option<i16>,
    /// Shortest periodic injection interval a barrier group may ask for.
    pub barrier_min_injection_interval: Option<Time>,
    /// Deadline for one barrier injection to reach every target partition.
    pub barrier_injection_timeout: Option<Time>,
    /// Maximum bytes requested by one `__barrier_state` recovery read.
    pub barrier_recovery_read_max: Option<ByteSize>,
    /// Number of cuts a barrier group keeps before it tombstones the oldest.
    pub barrier_retained_cuts: Option<i32>,
    /// Maximum number of barrier groups the cluster accepts.
    pub barrier_max_groups: Option<usize>,
    /// Maximum number of topics in one barrier group.
    pub barrier_max_topics_per_group: Option<usize>,
    /// Cadence of the partition disk-usage scan that feeds the
    /// `partition_disk_bytes` gauge. Zero disables the scanner and spawns no
    /// background task.
    pub partition_disk_scan_interval: Option<Time>,
    /// Capacity of the metadata Raft engine command queue.
    pub metadata_raft_command_queue_capacity: Option<usize>,
    /// Per-read and per-snapshot-request byte budget on the metadata Raft log.
    pub metadata_raft_fetch_max: Option<ByteSize>,
    /// How long a controlled shutdown waits for the controller to acknowledge
    /// `should_shut_down` before it falls back to a hard shutdown.
    pub controlled_shutdown_drain_timeout: Option<Time>,
    /// Committed metadata-log bytes between snapshots, Kafka's
    /// `metadata.log.max.record.bytes.between.snapshots`.
    pub metadata_max_bytes_between_snapshots: Option<ByteSize>,
    /// Maximum time between metadata-log snapshots, Kafka's
    /// `metadata.log.max.snapshot.interval.ms`. Zero disables the time-based
    /// cap.
    pub metadata_max_snapshot_interval: Option<Time>,
    /// KIP-630: snapshot the metadata log once the committed offset advances
    /// this many records past the last snapshot. The metadata log keeps the
    /// records below a snapshot until its retention limits let them go.
    pub metadata_snapshot_interval_records: Option<u64>,
    /// Maximum metadata snapshot size a follower fetches. The Raft core
    /// enforces an immutable 1 GiB ceiling above it.
    pub metadata_snapshot_fetch_max: Option<ByteSize>,
    /// Largest size of one metadata-log segment, Kafka's
    /// `metadata.log.segment.bytes`. The active segment rolls before an
    /// append that takes it past this size. A whole number of bytes from
    /// 8 MiB to 2147483647 bytes, as Kafka's `INT` with `atLeast(8388608)`.
    pub metadata_log_segment_bytes: Option<ByteSize>,
    /// Longest time one metadata-log segment stays active, Kafka's
    /// `metadata.log.segment.ms`. A positive whole number of milliseconds.
    pub metadata_log_segment_roll_interval: Option<Time>,
    /// Largest combined size of the metadata log and its snapshots before the
    /// oldest snapshot and the log prefix it covers are deleted, Kafka's
    /// `metadata.max.retention.bytes`. A whole number of bytes. Zero is a
    /// value. Kafka's negative value, which sets no size limit, has no form
    /// here.
    pub metadata_max_retention_bytes: Option<ByteSize>,
    /// Age after which a metadata snapshot and the log prefix it covers are
    /// deleted, Kafka's `metadata.max.retention.ms`. A whole number of
    /// milliseconds. Zero is a value. Kafka's negative value, which sets no
    /// age limit, has no form here.
    pub metadata_max_retention: Option<Time>,
    /// KIP-835: how often the active controller appends a `NoOpRecord` to the
    /// metadata log, Kafka's `metadata.max.idle.interval.ms`. A whole number
    /// of milliseconds up to 2147483647 ms. Zero disables the no-op records.
    pub metadata_max_idle_interval: Option<Time>,
    /// KIP-98: how often the idle-transaction reaper scans for `Ongoing`
    /// transactions whose timeout has elapsed and aborts them. Kafka's
    /// `transaction.abort.timed.out.transaction.cleanup.interval.ms`. Zero
    /// disables the reaper and spawns no background task.
    pub txn_abort_cleanup_interval: Option<Time>,
    /// KIP-98: how long a transactional id may sit in a terminal or idle state
    /// before the coordinator tombstones it out of `__transaction_state`.
    /// Kafka's `transactional.id.expiration.ms`.
    pub txn_id_expiration: Option<Time>,
    /// KIP-98: how often the transactional-id expiry sweep scans the
    /// `__transaction_state` partitions this broker leads. Kafka's
    /// `transaction.remove.expired.transaction.cleanup.interval.ms`. Zero
    /// disables the sweep and spawns no background task.
    pub txn_id_expiration_cleanup_interval: Option<Time>,
    /// How often the auto-rebalance ticker fires, Kafka's
    /// `leader.imbalance.check.interval.seconds`.
    pub leader_imbalance_check_interval: Option<Time>,
    /// Cadence at which the TLS watcher polls the certificate, key, and
    /// client-CA files and rebuilds the server configuration if any changed.
    /// Zero disables the periodic watcher.
    pub tls_reload_interval: Option<Time>,
    /// KIP-227: maximum number of incremental-fetch sessions kept in the per-
    /// broker cache, Kafka's `max.incremental.fetch.session.cache.slots`. When
    /// the cache is full, a new session displaces only a session that has been
    /// unused for more than two minutes, or a smaller one created more than two
    /// minutes ago (a follower-fetch session may also displace any consumer
    /// session); otherwise it is refused and the fetch runs sessionless, as in
    /// Kafka's `FetchSessionCacheShard.tryEvict`.
    pub max_incremental_fetch_session_cache_slots: Option<usize>,
    /// Maximum number of live broker connections across all listeners, Kafka's
    /// `max.connections`. A connection accepted past this ceiling is closed
    /// immediately.
    pub max_connections: Option<usize>,
    /// Maximum number of live connections from any single client IP, Kafka's
    /// `max.connections.per.ip`.
    pub max_connections_per_ip: Option<usize>,
    /// KIP-48: hard upper bound on a delegation token's lifetime, Kafka's
    /// `delegation.token.max.lifetime.ms`. A renew request is clamped to it.
    pub delegation_token_max_lifetime: Option<Time>,
    /// KIP-48: cadence of the sweep that tombstones expired delegation tokens,
    /// Kafka's `delegation.token.expiry.check.interval.ms`.
    pub delegation_token_expiry_check_interval: Option<Time>,
    /// KIP-48: default renew period, Kafka's
    /// `delegation.token.expiry.time.ms`. It is the initial expiry offset at
    /// create time, and the implicit renew period when a
    /// `RenewDelegationToken` request asks for `-1`.
    pub delegation_token_default_renew_period: Option<Time>,
    /// KIP-405: tick cadence of the `RemoteLogManager` copy and retention
    /// task. Kafka's `remote.log.manager.task.interval.ms`.
    pub remote_log_manager_interval: Option<Time>,

    /// Maximum number of members in one share group, Kafka's
    /// `group.share.max.size`: from 1 to 1000.
    pub share_group_max_size: Option<usize>,
    /// Cadence of the share-group backlog poll.
    pub share_group_backlog_poll_interval: Option<Time>,
    /// Maximum number of members in one streams group.
    pub streams_group_max_size: Option<usize>,
    /// Number of standby replicas the assignor places for each task, the
    /// group's `streams.num.standby.replicas`.
    pub streams_group_num_standby_replicas: Option<i32>,
    /// Client tag keys every streams-group member must send, Kafka's
    /// `group.streams.rack.aware.assignment.tags` and the default of a
    /// group's `streams.rack.aware.assignment.tags`. A repeated or an empty
    /// tag key is refused.
    pub streams_group_rack_aware_assignment_tags: Option<Vec<String>>,
    /// Maximum number of warm-up replicas the assignor may move at once, the
    /// group's `streams.num.warmup.replicas`.
    pub streams_group_num_warmup_replicas: Option<i32>,
    /// Changelog lag, in records, below which a task is treated as caught up,
    /// the group's `streams.acceptable.recovery.lag`.
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    /// Cadence at which members report task offsets, the group's
    /// `streams.task.offset.interval.ms`.
    pub streams_group_task_offset_interval: Option<Time>,
    /// Server-side task assignor for streams groups: `auto`, `sticky`, or
    /// `highly-available`. `auto` picks `highly-available` when the topology
    /// has a stateful subtopology and `sticky` otherwise.
    pub streams_group_assignor: Option<String>,
}

impl RuntimeFileConfig {
    /// Apply every present runtime value, validating scalar boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`FileConfigError::InvalidConfig`] for an invalid value.
    pub fn apply_to(
        mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.record_supplied_kafka_keys(cfg);
        self.apply_core(cfg)?;
        self.apply_replication(cfg)?;
        self.apply_coordinators(cfg)?;
        self.apply_recovery_and_queues(cfg)?;
        self.apply_network_limits(cfg)?;
        self.apply_transactions(cfg)?;
        self.apply_barrier(cfg)?;
        self.apply_broker_policy(cfg)?;
        self.apply_share_group(cfg)?;
        self.apply_streams_group(cfg)
    }

    /// Records, in [`crate::config::StaticConfigOrigins`], the Kafka keys of
    /// [`crate::config::KAFKA_STATIC_KEYS`] whose `[runtime]` field is set.
    /// The presence of the field is the provenance: a value equal to Kafka's
    /// default still reports at `STATIC_BROKER_CONFIG`.
    ///
    /// The overlay of a command line or the environment applies through this
    /// too, after the file, so it adds to what the file recorded.
    fn record_supplied_kafka_keys(&self, cfg: &mut crate::config::BrokerConfig) {
        let Ok(serde_json::Value::Object(fields)) = serde_json::to_value(self) else {
            return;
        };
        let supplied = crate::config::KAFKA_STATIC_KEYS.iter().filter(|key| {
            key.runtime_field
                .and_then(|field| fields.get(field))
                .is_some_and(|value| !value.is_null())
        });
        cfg.static_config_origins
            .supplied_kafka_keys
            .extend(supplied.map(|key| key.name));
    }
}

#[cfg(test)]
mod tests;
