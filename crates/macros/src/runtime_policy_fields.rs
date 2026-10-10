//! Common policy fields of the TOML and command-line runtime configurations.

use moxy::{
    ast::{Field, List, ParseError, Parser, Token},
    token::{LitStr, Span, ToTokenStream, TokenStream, TokenTree},
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let mode = crate::meta::mode(meta, ["toml", "cli", "node_cli"])?;
    if mode == "node_cli" {
        return controller_fields(item, ControllerFieldInterface::CommandLine);
    }
    let item = if mode == "toml" {
        controller_fields(item, ControllerFieldInterface::Toml)?
    } else {
        item
    };
    let (startup_before, cli) = match mode {
        "toml" => ("client_metrics_enable", false),
        "cli" => ("client_metrics_eviction_tick", true),
        _ => unreachable!(),
    };
    let clients = moxy::template! {
    /// Cadence at which the KIP-714 client-metrics cache evicts entries.
    pub client_metrics_eviction_tick: Option<Time>,
    /// Minimum age at which a client-metrics entry counts as stale.
    pub client_metrics_stale_floor: Option<Time>,
    /// Default KIP-714 client telemetry subscription push interval.
    pub client_metrics_default_interval: Option<Time>,
    /// Maximum accepted KIP-714 client telemetry payload size.
    pub client_metrics_telemetry_max: Option<ByteSize>,
    /// Lifetime of a Prometheus client-metrics snapshot.
    pub client_metrics_prom_snapshot_ttl: Option<Time>,
    /// Cadence of KIP-405 remote-log metadata reconciliation.
    pub rlmm_reconcile_tick: Option<Time>,
    /// Initial retry delay while remote-log metadata bootstrap is incomplete.
    pub rlmm_bootstrap_backoff_initial: Option<Time>,
    /// Maximum retry delay while remote-log metadata bootstrap is incomplete.
    pub rlmm_bootstrap_backoff_max: Option<Time>,
    /// Maximum KIP-612 connection-creation quota delay.
    pub connection_creation_throttle_max: Option<Time>,
    /// Timeout for one OPA authorization request.
    pub opa_http_timeout: Option<Time>,
    };
    let operations = moxy::template! {
    /// Maximum time a classic-protocol follower waits for its `SyncGroup`
    /// assignment.
    pub sync_group_follower_wait: Option<Time>,
    /// Replica-log collection deadline under the aggressive unclean-recovery
    /// strategy.
    pub unclean_recovery_aggressive_deadline: Option<Time>,
    /// Replica-log collection deadline under the balanced unclean-recovery
    /// strategy.
    pub unclean_recovery_balanced_deadline: Option<Time>,
    /// Deadline for an operator-triggered unclean recovery.
    pub operator_recovery_deadline: Option<Time>,
    /// Maximum request-quota throttle delay, which bounds how long one
    /// response over `request_percentage` mutes a client. Equivalent to
    /// Kafka's `quota.window.size.seconds`; byte-rate and controller-mutation
    /// throttles are not bounded.
    pub quota_throttle_max: Option<Time>,
    /// Time window that sizes the client byte-rate quota token bucket's burst
    /// capacity. Equivalent to Kafka's sampling window `quota.window.num *
    /// quota.window.size.seconds`.
    pub quota_window: Option<Time>,
    /// Time window whose throughput defines the KIP-599 controller-mutation
    /// quota burst capacity (default 11 s), Kafka's
    /// `controller.quota.window.num` x `controller.quota.window.size.seconds`.
    pub controller_mutation_quota_window: Option<Time>,
    /// Maximum self-registration attempts before startup fails.
    pub self_registration_max_attempts: Option<u32>,
    /// Maximum bytes fetched by one metadata observer request.
    pub observer_fetch_max: Option<ByteSize>,
    };
    let share_timings = moxy::template! {
    /// Default share-group session timeout, Kafka's
    /// `group.share.session.timeout.ms`.
    pub share_group_session_timeout: Option<Time>,
    /// Default share-group heartbeat interval, Kafka's
    /// `group.share.heartbeat.interval.ms`.
    pub share_group_heartbeat_interval: Option<Time>,
    /// Lower bound on the share-group session timeout, and on a group's
    /// `share.session.timeout.ms`, Kafka's
    /// `group.share.min.session.timeout.ms`.
    pub share_group_min_session_timeout: Option<Time>,
    /// Upper bound on the share-group session timeout, and on a group's
    /// `share.session.timeout.ms`, Kafka's
    /// `group.share.max.session.timeout.ms`.
    pub share_group_max_session_timeout: Option<Time>,
    /// Lower bound on the share-group heartbeat interval, and on a group's
    /// `share.heartbeat.interval.ms`, Kafka's
    /// `group.share.min.heartbeat.interval.ms`.
    pub share_group_min_heartbeat_interval: Option<Time>,
    /// Upper bound on the share-group heartbeat interval, and on a group's
    /// `share.heartbeat.interval.ms`, Kafka's
    /// `group.share.max.heartbeat.interval.ms`.
    pub share_group_max_heartbeat_interval: Option<Time>,
    };
    let share_limits = moxy::template! {
    /// How long an acquired share record stays locked before it is released
    /// for redelivery, Kafka's `group.share.record.lock.duration.ms`: a whole
    /// number of milliseconds from 1s to 1h, within the minimum and maximum
    /// below.
    pub share_group_record_lock_duration: Option<Time>,
    /// Lower bound on the record lock duration, and on a group's
    /// `share.record.lock.duration.ms`, Kafka's
    /// `group.share.min.record.lock.duration.ms`: from 1s to 30s.
    pub share_group_min_record_lock_duration: Option<Time>,
    /// Upper bound on the record lock duration, and on a group's
    /// `share.record.lock.duration.ms`, Kafka's
    /// `group.share.max.record.lock.duration.ms`: from 30s to 1h.
    pub share_group_max_record_lock_duration: Option<Time>,
    /// The delivery count at which a share record is archived, Kafka's
    /// `group.share.delivery.count.limit`: from 2 to 10, within the minimum
    /// and maximum below.
    pub share_group_delivery_count_limit: Option<i16>,
    /// Lower bound on the delivery count limit, and on a group's
    /// `share.delivery.count.limit`, Kafka's
    /// `group.share.min.delivery.count.limit`: from 2 to 5.
    pub share_group_min_delivery_count_limit: Option<i16>,
    /// Upper bound on the delivery count limit, and on a group's
    /// `share.delivery.count.limit`, Kafka's
    /// `group.share.max.delivery.count.limit`: from 5 to 25.
    pub share_group_max_delivery_count_limit: Option<i16>,
    /// Maximum records a share partition may hold in flight, Kafka's
    /// `group.share.partition.max.record.locks`: from 100 to 10000, within
    /// the minimum and maximum below.
    pub share_group_partition_max_record_locks: Option<i32>,
    /// Lower bound on the record lock limit, and on a group's
    /// `share.partition.max.record.locks`, Kafka's
    /// `group.share.min.partition.max.record.locks`: from 100 to 2000.
    pub share_group_min_partition_max_record_locks: Option<i32>,
    /// Upper bound on the record lock limit, and on a group's
    /// `share.partition.max.record.locks`, Kafka's
    /// `group.share.max.partition.max.record.locks`: from 2000 to 10000.
    pub share_group_max_partition_max_record_locks: Option<i32>,
    };
    let streams = moxy::template! {
    /// Whether the broker serves KIP-1071 streams groups.
    pub streams_group_enable: Option<bool>,
    /// Default streams-group session timeout, the group's
    /// `streams.session.timeout.ms`.
    pub streams_group_session_timeout: Option<Time>,
    /// Default streams-group heartbeat interval, the group's
    /// `streams.heartbeat.interval.ms`.
    pub streams_group_heartbeat_interval: Option<Time>,
    /// Lowest session timeout a streams group may run with, Kafka's
    /// `group.streams.min.session.timeout.ms`.
    pub streams_group_min_session_timeout: Option<Time>,
    /// Highest session timeout a streams group may run with, Kafka's
    /// `group.streams.max.session.timeout.ms`.
    pub streams_group_max_session_timeout: Option<Time>,
    /// Lowest heartbeat interval a streams group may run with, Kafka's
    /// `group.streams.min.heartbeat.interval.ms`.
    pub streams_group_min_heartbeat_interval: Option<Time>,
    /// Highest heartbeat interval a streams group may run with, Kafka's
    /// `group.streams.max.heartbeat.interval.ms`.
    pub streams_group_max_heartbeat_interval: Option<Time>,
    };
    let startup = moxy::template! {
    /// Maximum time the broker waits for a controller leader during startup.
    pub startup_leader_wait_timeout: Option<Time>,
    /// Initial delay between broker self-registration attempts.
    pub self_registration_backoff_min: Option<Time>,
    /// Maximum delay between broker self-registration attempts.
    pub self_registration_backoff_max: Option<Time>,
    /// Cadence of the KIP-853 observer promotion poll.
    pub observer_poll_interval: Option<Time>,
    /// Cadence at which the audit spool replays records it could not append.
    pub audit_spool_replay_interval: Option<Time>,
    /// Cadence of the audit statistics poll.
    pub audit_stats_poll_interval: Option<Time>,
    /// Maximum wait for the audit partition to become available.
    pub audit_partition_wait_timeout: Option<Time>,
    /// Cadence of broker liveness maintenance.
    pub liveness_tick_interval: Option<Time>,
    /// Cadence at which broker gauges are refreshed.
    pub gauge_poll_interval: Option<Time>,
    /// Cadence of in-sync-replica maintenance.
    pub isr_scan_interval: Option<Time>,
    /// Cadence of log cleaner maintenance.
    pub cleaner_interval: Option<Time>,
    /// Cadence of local-retention maintenance: how often `retention.ms`,
    /// `retention.bytes` and `segment.ms` are applied to every hosted log.
    pub log_retention_check_interval: Option<Time>,
    /// Retry delay after a KIP-113 future-log move fails.
    pub future_log_move_retry_backoff: Option<Time>,

    };
    let replication = moxy::template! {
    /// Maximum bytes a follower requests from a leader in one replication
    /// fetch. It reaches the leader as the fetch request's `max_bytes`.
    pub replication_fetch_max: Option<ByteSize>,
    /// Maximum time a leader holds a replication fetch that is not yet
    /// satisfied. It reaches the leader as the fetch request's `max_wait_ms`.
    pub replication_fetch_max_wait: Option<Time>,
    /// Minimum bytes that satisfy a replication fetch. It reaches the leader
    /// as the fetch request's `min_bytes`, which the leader honours as a
    /// floor.
    pub replication_fetch_min: Option<ByteSize>,
    /// Delay after a follower exhausts its replication throttle budget.
    pub replication_throttle_exhausted_backoff: Option<Time>,
    /// Retry delay after sending a replication request fails.
    pub replication_send_error_backoff: Option<Time>,
    /// Retry delay when the leader does not yet know the topic.
    pub replication_unknown_topic_retry_delay: Option<Time>,
    /// Retry delay after a leader-epoch fence.
    pub replication_epoch_fence_backoff: Option<Time>,
    /// Retry delay after an unexpected replication error.
    pub replication_unexpected_error_backoff: Option<Time>,
    /// Initial delay before a follower reconnects to a leader.
    pub replication_reconnect_initial_delay: Option<Time>,
    /// Maximum delay between leader reconnection attempts.
    pub replication_reconnect_delay_cap: Option<Time>,
    /// Cadence of the consumer-group session expiry scan.
    pub coordinator_session_expiry_tick: Option<Time>,
    /// Maximum wait for coordinator shutdown acknowledgements.
    pub coordinator_shutdown_ack_timeout: Option<Time>,
    /// Default KIP-848 consumer-group session timeout, Kafka's
    /// `group.consumer.session.timeout.ms`.
    pub consumer_group_session_timeout: Option<Time>,
    /// Default KIP-848 consumer-group heartbeat interval, Kafka's
    /// `group.consumer.heartbeat.interval.ms`.
    pub consumer_group_heartbeat_interval: Option<Time>,
    /// Lower bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.min.session.timeout.ms`.
    pub consumer_group_min_session_timeout: Option<Time>,
    /// Upper bound on the negotiated consumer-group session timeout, Kafka's
    /// `group.consumer.max.session.timeout.ms`.
    pub consumer_group_max_session_timeout: Option<Time>,
    /// Lower bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.min.heartbeat.interval.ms`.
    pub consumer_group_min_heartbeat_interval: Option<Time>,
    /// Upper bound on the negotiated consumer-group heartbeat interval,
    /// Kafka's `group.consumer.max.heartbeat.interval.ms`.
    pub consumer_group_max_heartbeat_interval: Option<Time>,

    };
    let groups = [
        (
            if cli {
                "oauth_jwks_http_timeout"
            } else {
                "schema_registry_http_timeout"
            },
            clients,
        ),
        (startup_before, startup),
        ("audit_event_queue_capacity", operations),
        ("streams_group_max_size", streams),
        ("share_group_max_size", share_timings),
        (
            if cli {
                "streams_group_enable"
            } else {
                "share_group_backlog_poll_interval"
            },
            share_limits,
        ),
        ("consumer_group_max_size", replication),
    ];
    let mut prepared = Vec::new();
    for (before, fields) in groups {
        // TOML documentation is schema text; the CLI fields originally had no
        // help text, and clap would otherwise turn these docs into help.
        let fields = if cli {
            let mut fields = List::<Field, Token![,]>::parse_all(&Parser::from_tokens(&fields))?;
            for field in &mut fields {
                field.attrs.retain(|attr| !attr.path.is_ident("doc"));
            }
            fields.to_token_stream()
        } else {
            fields
        };
        prepared.push((before, fields));
    }
    insert_fields(item, prepared)
}

fn insert_fields(
    item: TokenStream,
    fields: impl IntoIterator<Item = (&'static str, TokenStream)>,
) -> Result<TokenStream, ParseError> {
    let (mut tokens, body) = crate::meta::named_body(item, "runtime_policy_fields")?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    for (before, fields) in fields {
        let mut start = group
            .tokens
            .windows(2)
            .position(|pair| {
                pair[0].is_keyword_pub() && pair[1].as_ident().is_some_and(|ident| ident == before)
            })
            .ok_or_else(|| {
                ParseError::new(
                    Span::call_site(),
                    format!("missing runtime field `{before}`"),
                )
            })?;
        while start >= 2
            && group.tokens[start - 2].is_punct_pound()
            && group.tokens[start - 1]
                .as_group()
                .is_some_and(|attr| attr.delim.is_bracket())
        {
            start -= 2;
        }
        let mut body: Vec<_> = group.tokens.clone().into_iter().collect();
        body.splice(start..start, fields);
        group.tokens = body.into();
    }
    Ok(tokens.into())
}

/// Preserve each interface's documentation while sharing the operational field schema.
#[derive(Clone, Copy)]
enum ControllerFieldInterface {
    CommandLine,
    Toml,
}

fn controller_fields(
    item: TokenStream,
    interface: ControllerFieldInterface,
) -> Result<TokenStream, ParseError> {
    let cli = matches!(interface, ControllerFieldInterface::CommandLine);
    let [observer_lag_bound_doc, heartbeat_interval_doc, heartbeat_timeout_doc, replica_lag_time_max_doc, controller_election_timeout_doc, controller_heartbeat_interval_doc, controller_fetch_miss_limit_doc] = if cli {
        [
            "KIP-853 observer promotion lag bound.",
            "Broker heartbeat interval in milliseconds.",
            "Broker heartbeat timeout in milliseconds.",
            "Follower lag timeout in milliseconds before ISR shrink.",
            "Controller election timeout in milliseconds.",
            "Controller heartbeat interval in milliseconds.",
            "Consecutive controller fetch misses tolerated before election.",
        ]
    } else {
        [
            "KIP-853: maximum log-entry lag an observer may have and still be\npromotable to a voter.",
            "How often this broker sends `BrokerHeartbeat` to the controller leader.",
            "How long the controller waits without a heartbeat before it marks a\nbroker dead.",
            "Maximum follower lag before the leader proposes an ISR shrink. Kafka's\n`replica.lag.time.max.ms`.",
            "Controller election timeout, Kafka's\n`controller.quorum.fetch.timeout.ms`. It is the follower fetch\nwatchdog, and 1.5x of it is the leader's check-quorum window: a leader\nthat a majority of the voters has not fetched from within that window\nresigns its epoch.",
            "Raft heartbeat interval on the controller quorum. It should stay at or\nbelow `controller_election_timeout / 3`.",
            "Consecutive follower fetch misses tolerated before a new election.",
        ]
    }.map(|doc| LitStr::new(doc, Span::call_site()));
    let observer_arg = if cli {
        moxy::template! { #[arg(long, env = "KRABKA_OBSERVER_LAG_BOUND")] }
    } else {
        TokenStream::new()
    };
    let fields = moxy::template! {
        #[doc = {{ observer_lag_bound_doc }}]
        {{ observer_arg }}
        pub observer_lag_bound: Option<u64>,
        #[doc = {{ heartbeat_interval_doc }}]
        pub heartbeat_interval: Option<Time>,
        #[doc = {{ heartbeat_timeout_doc }}]
        pub heartbeat_timeout: Option<Time>,
        #[doc = {{ replica_lag_time_max_doc }}]
        pub replica_lag_time_max: Option<Time>,
        #[doc = {{ controller_election_timeout_doc }}]
        pub controller_election_timeout: Option<Time>,
        #[doc = {{ controller_heartbeat_interval_doc }}]
        pub controller_heartbeat_interval: Option<Time>,
        #[doc = {{ controller_fetch_miss_limit_doc }}]
        pub controller_fetch_miss_limit: Option<u32>,
    };
    insert_fields(item, [("metadata_raft_command_queue_capacity", fields)])
}
