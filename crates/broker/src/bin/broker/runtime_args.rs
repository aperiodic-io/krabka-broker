//! The broker's runtime-policy flags, as one flattened clap argument group.
//!
//! Every flag here overlays a field of `RuntimeFileConfig`, and the group is
//! large enough to keep apart from the rest of the command line.

use {
    krabka_broker::{
        config_value::{PositiveCount, PositiveI16, PositiveI32, PositiveI64},
        coordinator::unified::streams::config::StreamsAssignorKind,
    },
    krabka_client_core::{
        ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
    },
    krabka_units::{ByteSize, Ratio, Time},
};

fn parse_streams_assignor(value: &str) -> Result<StreamsAssignorKind, String> {
    StreamsAssignorKind::from_config_name(value)
        .ok_or_else(|| "expected `auto`, `sticky`, or `highly-available`".into())
}

/// A field without an `#[arg(...)]` gets `--field-name`, `KRABKA_FIELD_NAME`
/// and the value parser of its type from `#[krabka_env]`, and every field not
/// marked `skip` is copied onto the same-named `RuntimeFileConfig` field by
/// `copy_into`. The `krabka-macros` crate documentation lists both.
#[krabka_macros::runtime_policy_fields(cli)]
#[krabka_macros::krabka_env]
#[derive(Debug, clap::Args, krabka_macros::RuntimeOverlay)]
#[overlay(target = krabka_broker::file_config::RuntimeFileConfig)]
pub struct RuntimeArgs {
    #[arg(
        long,
        env = "KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    #[overlay(skip)]
    pub client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "KRABKA_BROKER_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    #[overlay(skip)]
    pub client_frame_max: ByteSize,
    pub oauth_jwks_http_timeout: Option<Time>,
    pub auto_join_retry_backoff: Option<Time>,
    pub auto_join_voter_request_timeout: Option<Time>,
    #[overlay(refined)]
    pub consumer_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY", value_parser = krabka_units::parse::non_negative_time)]
    pub classic_group_initial_rebalance_delay: Option<Time>,
    pub classic_group_min_session_timeout: Option<Time>,
    pub classic_group_max_session_timeout: Option<Time>,
    #[overlay(refined)]
    pub classic_group_max_size: Option<PositiveCount>,
    #[overlay(refined)]
    pub audit_event_queue_capacity: Option<PositiveCount>,
    #[overlay(refined)]
    pub audit_tail_window_offsets: Option<PositiveI64>,
    pub audit_tail_read_max: Option<ByteSize>,
    pub client_metrics_stale_push_intervals: Option<u32>,
    #[overlay(refined)]
    pub client_metrics_otlp_queue_capacity: Option<PositiveCount>,
    #[overlay(refined)]
    pub coordinator_actor_mailbox_capacity: Option<PositiveCount>,
    #[overlay(refined)]
    pub diskless_wal_local_replica_count: Option<PositiveCount>,
    pub diskless_wal_flush_interval: Option<Time>,
    pub diskless_wal_flush_max_size: Option<ByteSize>,
    pub diskless_wal_hot_tail_max_size: Option<ByteSize>,
    pub diskless_wal_trim_safety_lag: Option<i64>,
    pub diskless_wal_index_projection_timeout: Option<Time>,
    #[overlay(refined)]
    pub unclean_recovery_queue_capacity: Option<PositiveCount>,
    pub share_coordinator_load_buffer_size: Option<ByteSize>,
    #[overlay(refined)]
    pub share_session_cache_max_when_unlimited: Option<PositiveCount>,
    pub log_read_buffer_cap: Option<ByteSize>,
    pub log_read_ahead_max: Option<ByteSize>,
    pub log_tail_cache_size: Option<ByteSize>,
    pub log_timestamp_scan_window: Option<ByteSize>,
    pub log_delivery_clock_uncertainty: Option<Time>,
    #[arg(long, env = "KRABKA_MESSAGE_MAX_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub message_max_bytes: Option<ByteSize>,
    #[arg(long, env = "KRABKA_FETCH_MAX_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub fetch_max_bytes: Option<ByteSize>,
    pub socket_request_max: Option<ByteSize>,
    pub sasl_server_max_receive: Option<ByteSize>,
    #[arg(long, env = "KRABKA_CONNECTION_FAILED_AUTHENTICATION_DELAY", value_parser = krabka_units::parse::non_negative_time)]
    pub connection_failed_authentication_delay: Option<Time>,
    #[overlay(refined)]
    pub queued_max_requests: Option<PositiveCount>,
    pub queued_max_request_bytes: Option<ByteSize>,
    pub sendfile_min: Option<ByteSize>,
    pub socket_send_buffer: Option<ByteSize>,
    pub socket_receive_buffer: Option<ByteSize>,
    #[overlay(refined)]
    pub max_request_partition_size_limit: Option<PositiveI32>,
    pub record_decompression_max_ratio: Option<Ratio>,
    pub record_decompression_output_floor: Option<ByteSize>,
    pub record_decompression_output_ceiling: Option<ByteSize>,
    #[overlay(clone)]
    pub inter_broker_server_name: Option<String>,
    pub producer_id_expiration: Option<Time>,
    pub producer_id_expiration_scan_interval: Option<Time>,
    #[overlay(refined)]
    pub max_produce_group: Option<PositiveCount>,
    #[overlay(refined)]
    pub partition_writer_queue_depth: Option<PositiveCount>,
    #[overlay(refined)]
    pub dedup_max_entries_per_partition: Option<PositiveCount>,
    #[overlay(refined)]
    pub dedup_max_entries: Option<PositiveCount>,
    #[overlay(refined)]
    pub default_min_insync_replicas: Option<PositiveI32>,
    #[overlay(refined)]
    pub num_partitions: Option<PositiveI32>,
    #[overlay(refined)]
    pub default_replication_factor: Option<PositiveI16>,
    pub future_log_move_read_chunk: Option<ByteSize>,
    #[overlay(refined)]
    pub share_state_num_partitions: Option<PositiveI32>,
    #[overlay(refined)]
    pub share_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_SHARE_STATE_SEGMENT_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub share_state_segment_bytes: Option<ByteSize>,
    #[overlay(refined)]
    pub share_state_min_isr: Option<PositiveI32>,
    #[arg(
        long,
        env = "KRABKA_SHARE_COORDINATOR_APPEND_LINGER_MS",
        allow_negative_numbers = true
    )]
    pub share_coordinator_append_linger_ms: Option<i32>,
    pub share_coordinator_cached_buffer_max_bytes: Option<ByteSize>,
    #[overlay(refined)]
    pub offsets_topic_num_partitions: Option<PositiveI32>,
    #[overlay(refined)]
    pub offsets_topic_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_OFFSETS_TOPIC_SEGMENT_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub offsets_topic_segment_bytes: Option<ByteSize>,
    pub offsets_retention: Option<Time>,
    pub offsets_retention_check_interval: Option<Time>,
    #[overlay(refined)]
    pub transaction_state_num_partitions: Option<PositiveI32>,
    pub transaction_recovery_read_max: Option<ByteSize>,
    #[overlay(refined)]
    pub transaction_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "KRABKA_TRANSACTION_STATE_SEGMENT_BYTES", value_parser = parse_kafka_int_byte_size)]
    pub transaction_state_segment_bytes: Option<ByteSize>,
    #[overlay(refined)]
    pub transaction_state_min_isr: Option<PositiveI32>,
    pub transaction_max_timeout: Option<Time>,
    pub transaction_partition_verification_enable: Option<bool>,

    #[overlay(refined)]
    pub share_group_max_size: Option<PositiveCount>,
    #[overlay(refined)]
    pub streams_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_NUM_STANDBY_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    pub streams_group_num_standby_replicas: Option<i32>,
    #[arg(
        long,
        env = "KRABKA_STREAMS_GROUP_RACK_AWARE_ASSIGNMENT_TAGS",
        value_delimiter = ','
    )]
    #[overlay(clone)]
    pub streams_group_rack_aware_assignment_tags: Option<Vec<String>>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_NUM_WARMUP_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    pub streams_group_num_warmup_replicas: Option<i32>,
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    pub streams_group_task_offset_interval: Option<Time>,
    #[arg(long, env = "KRABKA_STREAMS_GROUP_ASSIGNOR", value_parser = parse_streams_assignor)]
    #[overlay(skip)]
    pub streams_group_assignor: Option<StreamsAssignorKind>,
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

/// Parse a byte count in the domain Kafka gives an `INT` config with
/// `atLeast(0)`, which is what `message.max.bytes` is.
///
/// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
/// "Value must be at least 0", and refuses `2147483648` with "Not a number of
/// type INT". The topic-level `max.message.bytes` this key defaults is the
/// same `INT`, so the flag, the TOML file, and `kafka-configs --alter` accept
/// and reject the same values rather than three overlapping domains.
fn parse_kafka_int_byte_size(value: &str) -> Result<ByteSize, String> {
    use krabka_units::convert::ByteSizeExt as _;

    const KAFKA_INT_MAX: u64 = 2_147_483_647;
    const DOMAIN: &str = "must be a whole number of bytes from 0 to 2147483647";

    let size =
        krabka_units::parse::non_negative_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_u64();
    if ByteSize::from_bytes(bytes) != size || bytes > KAFKA_INT_MAX {
        return Err(DOMAIN.to_owned());
    }
    Ok(size)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value =
        krabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

#[cfg(test)]
mod tests {
    use {
        super::RuntimeArgs,
        crate::{cli::Args, test_support::env_guard},
        assert2::check,
        clap::{Args as _, Parser as _},
        krabka_units::convert::ByteSizeExt,
    };

    /// The values each flag is offered in [`runtime_flags_keep_their_shape`].
    /// Between them they tell every value parser the group uses apart: zero
    /// against positive durations and sizes, the `i16`, `i32` and Kafka `INT`
    /// byte ceilings, signed against unsigned, and the `u32` ranges.
    const PROBES: [&str; 13] = [
        "0",
        "1",
        "-1",
        "1.5",
        "40000",
        "3000000000",
        "0ms",
        "1ms",
        "0B",
        "1B",
        "3GiB",
        "true",
        "auto",
    ];

    /// One line per flag: its long name, its environment variable, its help
    /// text, and which of [`PROBES`] its value parser accepts (`+`) or
    /// refuses (`-`).
    fn runtime_flag_shapes() -> String {
        let command = RuntimeArgs::augment_args(clap::Command::new("runtime"));
        command
            .get_arguments()
            .map(|arg| {
                let long = arg.get_long().unwrap_or_default();
                let env = arg
                    .get_env()
                    .map(|env| env.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let help = arg.get_help().map(ToString::to_string);
                let accepted = PROBES
                    .iter()
                    .map(|probe| {
                        let flag = format!("--{long}={probe}");
                        if command
                            .clone()
                            .try_get_matches_from(["runtime", flag.as_str()])
                            .is_ok()
                        {
                            '+'
                        } else {
                            '-'
                        }
                    })
                    .collect::<String>();
                format!("{long} {env} {help:?} {accepted}")
            })
            .flat_map(|line| [line, "\n".to_owned()])
            .collect()
    }

    /// Every runtime flag keeps its long name, environment variable, help
    /// text and value domain.
    #[test]
    fn runtime_flags_keep_their_shape() {
        let _guard = env_guard();

        check!(runtime_flag_shapes() == RUNTIME_FLAG_SHAPES);
    }

    /// The output of [`runtime_flag_shapes`] before `#[krabka_env]` took over
    /// the flags' attributes.
    const RUNTIME_FLAG_SHAPES: &str = "\
        client-dispatch-queue-capacity KRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY None -+--++-------\n\
        client-frame-max KRABKA_BROKER_CLIENT_FRAME_MAX None ---------+---\n\
        startup-leader-wait-timeout KRABKA_STARTUP_LEADER_WAIT_TIMEOUT None -------+-----\n\
        self-registration-backoff-min KRABKA_SELF_REGISTRATION_BACKOFF_MIN None -------+-----\n\
        self-registration-backoff-max KRABKA_SELF_REGISTRATION_BACKOFF_MAX None -------+-----\n\
        observer-poll-interval KRABKA_OBSERVER_POLL_INTERVAL None -------+-----\n\
        audit-spool-replay-interval KRABKA_AUDIT_SPOOL_REPLAY_INTERVAL None -------+-----\n\
        audit-stats-poll-interval KRABKA_AUDIT_STATS_POLL_INTERVAL None -------+-----\n\
        audit-partition-wait-timeout KRABKA_AUDIT_PARTITION_WAIT_TIMEOUT None -------+-----\n\
        liveness-tick-interval KRABKA_LIVENESS_TICK_INTERVAL None -------+-----\n\
        gauge-poll-interval KRABKA_GAUGE_POLL_INTERVAL None -------+-----\n\
        isr-scan-interval KRABKA_ISR_SCAN_INTERVAL None -------+-----\n\
        cleaner-interval KRABKA_CLEANER_INTERVAL None -------+-----\n\
        log-retention-check-interval KRABKA_LOG_RETENTION_CHECK_INTERVAL None -------+-----\n\
        future-log-move-retry-backoff KRABKA_FUTURE_LOG_MOVE_RETRY_BACKOFF None -------+-----\n\
        client-metrics-eviction-tick KRABKA_CLIENT_METRICS_EVICTION_TICK None -------+-----\n\
        client-metrics-stale-floor KRABKA_CLIENT_METRICS_STALE_FLOOR None -------+-----\n\
        client-metrics-default-interval KRABKA_CLIENT_METRICS_DEFAULT_INTERVAL None -------+-----\n\
        client-metrics-telemetry-max KRABKA_CLIENT_METRICS_TELEMETRY_MAX None ---------++--\n\
        client-metrics-prom-snapshot-ttl KRABKA_CLIENT_METRICS_PROM_SNAPSHOT_TTL None -------+-----\n\
        rlmm-reconcile-tick KRABKA_RLMM_RECONCILE_TICK None -------+-----\n\
        rlmm-bootstrap-backoff-initial KRABKA_RLMM_BOOTSTRAP_BACKOFF_INITIAL None -------+-----\n\
        rlmm-bootstrap-backoff-max KRABKA_RLMM_BOOTSTRAP_BACKOFF_MAX None -------+-----\n\
        connection-creation-throttle-max KRABKA_CONNECTION_CREATION_THROTTLE_MAX None -------+-----\n\
        opa-http-timeout KRABKA_OPA_HTTP_TIMEOUT None -------+-----\n\
        oauth-jwks-http-timeout KRABKA_OAUTH_JWKS_HTTP_TIMEOUT None -------+-----\n\
        auto-join-retry-backoff KRABKA_AUTO_JOIN_RETRY_BACKOFF None -------+-----\n\
        auto-join-voter-request-timeout KRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT None -------+-----\n\
        replication-fetch-max KRABKA_REPLICATION_FETCH_MAX None ---------++--\n\
        replication-fetch-max-wait KRABKA_REPLICATION_FETCH_MAX_WAIT None -------+-----\n\
        replication-fetch-min KRABKA_REPLICATION_FETCH_MIN None ---------++--\n\
        replication-throttle-exhausted-backoff KRABKA_REPLICATION_THROTTLE_EXHAUSTED_BACKOFF None -------+-----\n\
        replication-send-error-backoff KRABKA_REPLICATION_SEND_ERROR_BACKOFF None -------+-----\n\
        replication-unknown-topic-retry-delay KRABKA_REPLICATION_UNKNOWN_TOPIC_RETRY_DELAY None -------+-----\n\
        replication-epoch-fence-backoff KRABKA_REPLICATION_EPOCH_FENCE_BACKOFF None -------+-----\n\
        replication-unexpected-error-backoff KRABKA_REPLICATION_UNEXPECTED_ERROR_BACKOFF None -------+-----\n\
        replication-reconnect-initial-delay KRABKA_REPLICATION_RECONNECT_INITIAL_DELAY None -------+-----\n\
        replication-reconnect-delay-cap KRABKA_REPLICATION_RECONNECT_DELAY_CAP None -------+-----\n\
        coordinator-session-expiry-tick KRABKA_COORDINATOR_SESSION_EXPIRY_TICK None -------+-----\n\
        coordinator-shutdown-ack-timeout KRABKA_COORDINATOR_SHUTDOWN_ACK_TIMEOUT None -------+-----\n\
        consumer-group-session-timeout KRABKA_CONSUMER_GROUP_SESSION_TIMEOUT None -------+-----\n\
        consumer-group-heartbeat-interval KRABKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL None -------+-----\n\
        consumer-group-min-session-timeout KRABKA_CONSUMER_GROUP_MIN_SESSION_TIMEOUT None -------+-----\n\
        consumer-group-max-session-timeout KRABKA_CONSUMER_GROUP_MAX_SESSION_TIMEOUT None -------+-----\n\
        consumer-group-min-heartbeat-interval KRABKA_CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL None -------+-----\n\
        consumer-group-max-heartbeat-interval KRABKA_CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL None -------+-----\n\
        consumer-group-max-size KRABKA_CONSUMER_GROUP_MAX_SIZE None -+--++-------\n\
        classic-group-initial-rebalance-delay KRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY None +-----++-----\n\
        classic-group-min-session-timeout KRABKA_CLASSIC_GROUP_MIN_SESSION_TIMEOUT None -------+-----\n\
        classic-group-max-session-timeout KRABKA_CLASSIC_GROUP_MAX_SESSION_TIMEOUT None -------+-----\n\
        classic-group-max-size KRABKA_CLASSIC_GROUP_MAX_SIZE None -+--++-------\n\
        sync-group-follower-wait KRABKA_SYNC_GROUP_FOLLOWER_WAIT None -------+-----\n\
        unclean-recovery-aggressive-deadline KRABKA_UNCLEAN_RECOVERY_AGGRESSIVE_DEADLINE None -------+-----\n\
        unclean-recovery-balanced-deadline KRABKA_UNCLEAN_RECOVERY_BALANCED_DEADLINE None -------+-----\n\
        operator-recovery-deadline KRABKA_OPERATOR_RECOVERY_DEADLINE None -------+-----\n\
        quota-throttle-max KRABKA_QUOTA_THROTTLE_MAX None -------+-----\n\
        quota-window KRABKA_QUOTA_WINDOW None -------+-----\n\
        controller-mutation-quota-window KRABKA_CONTROLLER_MUTATION_QUOTA_WINDOW None -------+-----\n\
        self-registration-max-attempts KRABKA_SELF_REGISTRATION_MAX_ATTEMPTS None -+--++-------\n\
        observer-fetch-max KRABKA_OBSERVER_FETCH_MAX None ---------++--\n\
        audit-event-queue-capacity KRABKA_AUDIT_EVENT_QUEUE_CAPACITY None -+--++-------\n\
        audit-tail-window-offsets KRABKA_AUDIT_TAIL_WINDOW_OFFSETS None -+--++-------\n\
        audit-tail-read-max KRABKA_AUDIT_TAIL_READ_MAX None ---------++--\n\
        client-metrics-stale-push-intervals KRABKA_CLIENT_METRICS_STALE_PUSH_INTERVALS None -+--++-------\n\
        client-metrics-otlp-queue-capacity KRABKA_CLIENT_METRICS_OTLP_QUEUE_CAPACITY None -+--++-------\n\
        coordinator-actor-mailbox-capacity KRABKA_COORDINATOR_ACTOR_MAILBOX_CAPACITY None -+--++-------\n\
        diskless-wal-local-replica-count KRABKA_DISKLESS_WAL_LOCAL_REPLICA_COUNT None -+--++-------\n\
        diskless-wal-flush-interval KRABKA_DISKLESS_WAL_FLUSH_INTERVAL None -------+-----\n\
        diskless-wal-flush-max-size KRABKA_DISKLESS_WAL_FLUSH_MAX_SIZE None ---------++--\n\
        diskless-wal-hot-tail-max-size KRABKA_DISKLESS_WAL_HOT_TAIL_MAX_SIZE None ---------++--\n\
        diskless-wal-trim-safety-lag KRABKA_DISKLESS_WAL_TRIM_SAFETY_LAG None ++--++-------\n\
        diskless-wal-index-projection-timeout KRABKA_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT None -------+-----\n\
        unclean-recovery-queue-capacity KRABKA_UNCLEAN_RECOVERY_QUEUE_CAPACITY None -+--++-------\n\
        share-coordinator-load-buffer-size KRABKA_SHARE_COORDINATOR_LOAD_BUFFER_SIZE None ---------++--\n\
        share-session-cache-max-when-unlimited KRABKA_SHARE_SESSION_CACHE_MAX_WHEN_UNLIMITED None -+--++-------\n\
        log-read-buffer-cap KRABKA_LOG_READ_BUFFER_CAP None ---------++--\n\
        log-read-ahead-max KRABKA_LOG_READ_AHEAD_MAX None ---------++--\n\
        log-tail-cache-size KRABKA_LOG_TAIL_CACHE_SIZE None ---------++--\n\
        log-timestamp-scan-window KRABKA_LOG_TIMESTAMP_SCAN_WINDOW None ---------++--\n\
        log-delivery-clock-uncertainty KRABKA_LOG_DELIVERY_CLOCK_UNCERTAINTY None -------+-----\n\
        message-max-bytes KRABKA_MESSAGE_MAX_BYTES None +-------++---\n\
        fetch-max-bytes KRABKA_FETCH_MAX_BYTES None +-------++---\n\
        socket-request-max KRABKA_SOCKET_REQUEST_MAX None ---------++--\n\
        sasl-server-max-receive KRABKA_SASL_SERVER_MAX_RECEIVE None ---------++--\n\
        connection-failed-authentication-delay KRABKA_CONNECTION_FAILED_AUTHENTICATION_DELAY None +-----++-----\n\
        queued-max-requests KRABKA_QUEUED_MAX_REQUESTS None -+--++-------\n\
        queued-max-request-bytes KRABKA_QUEUED_MAX_REQUEST_BYTES None ---------++--\n\
        sendfile-min KRABKA_SENDFILE_MIN None ---------++--\n\
        socket-send-buffer KRABKA_SOCKET_SEND_BUFFER None ---------++--\n\
        socket-receive-buffer KRABKA_SOCKET_RECEIVE_BUFFER None ---------++--\n\
        max-request-partition-size-limit KRABKA_MAX_REQUEST_PARTITION_SIZE_LIMIT None -+--+--------\n\
        record-decompression-max-ratio KRABKA_RECORD_DECOMPRESSION_MAX_RATIO None -+-+++-------\n\
        record-decompression-output-floor KRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR None ---------++--\n\
        record-decompression-output-ceiling KRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING None ---------++--\n\
        inter-broker-server-name KRABKA_INTER_BROKER_SERVER_NAME None +++++++++++++\n\
        producer-id-expiration KRABKA_PRODUCER_ID_EXPIRATION None -------+-----\n\
        producer-id-expiration-scan-interval KRABKA_PRODUCER_ID_EXPIRATION_SCAN_INTERVAL None -------+-----\n\
        max-produce-group KRABKA_MAX_PRODUCE_GROUP None -+--++-------\n\
        partition-writer-queue-depth KRABKA_PARTITION_WRITER_QUEUE_DEPTH None -+--++-------\n\
        dedup-max-entries-per-partition KRABKA_DEDUP_MAX_ENTRIES_PER_PARTITION None -+--++-------\n\
        dedup-max-entries KRABKA_DEDUP_MAX_ENTRIES None -+--++-------\n\
        default-min-insync-replicas KRABKA_DEFAULT_MIN_INSYNC_REPLICAS None -+--+--------\n\
        num-partitions KRABKA_NUM_PARTITIONS None -+--+--------\n\
        default-replication-factor KRABKA_DEFAULT_REPLICATION_FACTOR None -+-----------\n\
        future-log-move-read-chunk KRABKA_FUTURE_LOG_MOVE_READ_CHUNK None ---------++--\n\
        share-state-num-partitions KRABKA_SHARE_STATE_NUM_PARTITIONS None -+--+--------\n\
        share-state-replication-factor KRABKA_SHARE_STATE_REPLICATION_FACTOR None -+-----------\n\
        share-state-segment-bytes KRABKA_SHARE_STATE_SEGMENT_BYTES None +-------++---\n\
        share-state-min-isr KRABKA_SHARE_STATE_MIN_ISR None -+--+--------\n\
        share-snapshot-update-records-per-snapshot KRABKA_SHARE_SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT None ++-----------\n\
        share-coordinator-write-timeout KRABKA_SHARE_COORDINATOR_WRITE_TIMEOUT None -------+-----\n\
        share-state-prune-interval KRABKA_SHARE_STATE_PRUNE_INTERVAL None -------+-----\n\
        share-cold-partition-snapshot-interval KRABKA_SHARE_COLD_PARTITION_SNAPSHOT_INTERVAL None -------+-----\n\
        share-state-compression-codec KRABKA_SHARE_STATE_COMPRESSION_CODEC None +++-+--------\n\
        share-coordinator-threads KRABKA_SHARE_COORDINATOR_THREADS None +++-+--------\n\
        share-coordinator-append-linger-ms KRABKA_SHARE_COORDINATOR_APPEND_LINGER_MS None +++-+--------\n\
        share-coordinator-cached-buffer-max-bytes KRABKA_SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES None ---------++--\n\
        offsets-topic-num-partitions KRABKA_OFFSETS_TOPIC_NUM_PARTITIONS None -+--+--------\n\
        offsets-topic-replication-factor KRABKA_OFFSETS_TOPIC_REPLICATION_FACTOR None -+-----------\n\
        offsets-topic-segment-bytes KRABKA_OFFSETS_TOPIC_SEGMENT_BYTES None +-------++---\n\
        offsets-retention KRABKA_OFFSETS_RETENTION None -------+-----\n\
        offsets-retention-check-interval KRABKA_OFFSETS_RETENTION_CHECK_INTERVAL None -------+-----\n\
        transaction-state-num-partitions KRABKA_TRANSACTION_STATE_NUM_PARTITIONS None -+--+--------\n\
        transaction-recovery-read-max KRABKA_TRANSACTION_RECOVERY_READ_MAX None ---------++--\n\
        transaction-state-replication-factor KRABKA_TRANSACTION_STATE_REPLICATION_FACTOR None -+-----------\n\
        transaction-state-segment-bytes KRABKA_TRANSACTION_STATE_SEGMENT_BYTES None +-------++---\n\
        transaction-state-min-isr KRABKA_TRANSACTION_STATE_MIN_ISR None -+--+--------\n\
        transaction-max-timeout KRABKA_TRANSACTION_MAX_TIMEOUT None -------+-----\n\
        transaction-partition-verification-enable KRABKA_TRANSACTION_PARTITION_VERIFICATION_ENABLE None -----------+-\n\
        share-group-session-timeout KRABKA_SHARE_GROUP_SESSION_TIMEOUT None -------+-----\n\
        share-group-heartbeat-interval KRABKA_SHARE_GROUP_HEARTBEAT_INTERVAL None -------+-----\n\
        share-group-min-session-timeout KRABKA_SHARE_GROUP_MIN_SESSION_TIMEOUT None -------+-----\n\
        share-group-max-session-timeout KRABKA_SHARE_GROUP_MAX_SESSION_TIMEOUT None -------+-----\n\
        share-group-min-heartbeat-interval KRABKA_SHARE_GROUP_MIN_HEARTBEAT_INTERVAL None -------+-----\n\
        share-group-max-heartbeat-interval KRABKA_SHARE_GROUP_MAX_HEARTBEAT_INTERVAL None -------+-----\n\
        share-group-max-size KRABKA_SHARE_GROUP_MAX_SIZE None -+--++-------\n\
        share-group-record-lock-duration KRABKA_SHARE_GROUP_RECORD_LOCK_DURATION None -------+-----\n\
        share-group-min-record-lock-duration KRABKA_SHARE_GROUP_MIN_RECORD_LOCK_DURATION None -------+-----\n\
        share-group-max-record-lock-duration KRABKA_SHARE_GROUP_MAX_RECORD_LOCK_DURATION None -------+-----\n\
        share-group-delivery-count-limit KRABKA_SHARE_GROUP_DELIVERY_COUNT_LIMIT None +++----------\n\
        share-group-min-delivery-count-limit KRABKA_SHARE_GROUP_MIN_DELIVERY_COUNT_LIMIT None +++----------\n\
        share-group-max-delivery-count-limit KRABKA_SHARE_GROUP_MAX_DELIVERY_COUNT_LIMIT None +++----------\n\
        share-group-partition-max-record-locks KRABKA_SHARE_GROUP_PARTITION_MAX_RECORD_LOCKS None +++-+--------\n\
        share-group-min-partition-max-record-locks KRABKA_SHARE_GROUP_MIN_PARTITION_MAX_RECORD_LOCKS None +++-+--------\n\
        share-group-max-partition-max-record-locks KRABKA_SHARE_GROUP_MAX_PARTITION_MAX_RECORD_LOCKS None +++-+--------\n\
        streams-group-enable KRABKA_STREAMS_GROUP_ENABLE None -----------+-\n\
        streams-group-session-timeout KRABKA_STREAMS_GROUP_SESSION_TIMEOUT None -------+-----\n\
        streams-group-heartbeat-interval KRABKA_STREAMS_GROUP_HEARTBEAT_INTERVAL None -------+-----\n\
        streams-group-min-session-timeout KRABKA_STREAMS_GROUP_MIN_SESSION_TIMEOUT None -------+-----\n\
        streams-group-max-session-timeout KRABKA_STREAMS_GROUP_MAX_SESSION_TIMEOUT None -------+-----\n\
        streams-group-min-heartbeat-interval KRABKA_STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL None -------+-----\n\
        streams-group-max-heartbeat-interval KRABKA_STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL None -------+-----\n\
        streams-group-max-size KRABKA_STREAMS_GROUP_MAX_SIZE None -+--++-------\n\
        streams-group-num-standby-replicas KRABKA_STREAMS_GROUP_NUM_STANDBY_REPLICAS None ++--+--------\n\
        streams-group-rack-aware-assignment-tags KRABKA_STREAMS_GROUP_RACK_AWARE_ASSIGNMENT_TAGS None +++++++++++++\n\
        streams-group-num-warmup-replicas KRABKA_STREAMS_GROUP_NUM_WARMUP_REPLICAS None ++--+--------\n\
        streams-group-acceptable-recovery-lag KRABKA_STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG None ++--++-------\n\
        streams-group-task-offset-interval KRABKA_STREAMS_GROUP_TASK_OFFSET_INTERVAL None -------+-----\n\
        streams-group-assignor KRABKA_STREAMS_GROUP_ASSIGNOR None ------------+\n\
";

    /// `--message-max-bytes` takes exactly the values Kafka's `INT` with
    /// `atLeast(0)` takes.
    ///
    /// `apache/kafka:4.3.1` starts on `message.max.bytes=0`, refuses `-1` with
    /// "Value must be at least 0", and refuses `2147483648` with "Not a number
    /// of type INT". The topic-level `max.message.bytes` this key defaults
    /// enforces the same domain, so an operator cannot set a broker-wide cap
    /// through the flag that `kafka-configs --alter` would refuse on a topic.
    #[test]
    fn message_max_bytes_takes_kafkas_int_at_least_zero() {
        let _guard = env_guard();

        for (value, expected) in [
            ("0B", Some(0)),
            ("2048B", Some(2048)),
            ("2147483647B", Some(2_147_483_647)),
            ("1MiB", Some(1_048_576)),
            ("-1B", None),
            ("2147483648B", None),
            ("2GiB", None),
            ("1.5B", None),
        ] {
            let parsed = Args::try_parse_from(["krabka-broker", "--message-max-bytes", value])
                .ok()
                .and_then(|args| args.runtime.message_max_bytes)
                .map(ByteSizeExt::bytes_u64);
            check!(parsed == expected, "--message-max-bytes={value}");
        }
    }
}
