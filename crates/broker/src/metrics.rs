//! Broker-side Prometheus metrics.
//!
//! Mirrors the operator's `telemetry` / `health` pattern: a shared
//! `Registry` is wrapped in `Arc<Mutex<…>>` so hot-path counters can
//! be looked up without holding the registry lock. The
//! [`BrokerMetrics`] struct hands out cheap `Arc<Counter>` / `Arc<Gauge>`
//! handles that handlers and background tasks clone and increment
//! directly.
//!
//! Naming follows Prometheus convention: `krabka_broker_<subject>_<unit>`.
//! Where Kafka has a canonical JMX name, we keep the metric semantics
//! close to it (e.g. `BrokerTopicMetrics:BytesInPerSec` ↔
//! `krabka_broker_topic_bytes_in_total`), but the units convert from
//! per-second gauges to monotonic counters per Prometheus best practice
//! — operators compute rates with `rate()` at scrape time.

use {
    krabka_macros::RegisterMetrics,
    prometheus_client::{
        metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
        registry::Registry,
    },
    std::sync::{Arc, atomic::AtomicU64},
    tokio::sync::Mutex,
};

mod auth;
mod break_glass;
mod delivery;
mod diskless;
mod eviction;
mod fetch_drain;
mod labels;
mod lag;
mod log_cleaner;
mod log_dirs;
mod metadata_load;
mod phases;
mod registration;
mod remote_reader;
mod remote_tier;
mod replication;
mod request;
mod schema_validation;
mod share_dlq;
#[cfg(test)]
pub(crate) mod test_support;
mod traffic;

pub use self::{
    eviction::MetricSeriesIndex,
    labels::{
        ApiKeyLabel, AuthorizationDeniedLabel, BarrierGroupLabel, BreakGlassAction,
        BreakGlassActionLabel, BreakGlassState, BreakGlassStateLabel, CleanerFailureLabel,
        CleanerFailureReason, ClientSoftwareLabel, ConnectionCloseReason,
        ConnectionCloseReasonLabel, ConsumerGroupLabel, DirectoryLabel, FetchDrainPath,
        FetchDrainPathLabel, PartitionLabel, QuotaEntityLabel, QuotaType, QuotaTypeLabel,
        RaftStateLabel, ReplicaLagLabel, SaslMechanismLabel, SchemaRejectionLabel,
        ShareGroupIdLabel, ShareGroupLabel, TopicLabel, WalShardLabel, WalVoterLabel,
    },
    lag::LagSeriesIndex,
    remote_reader::{RemoteReaderLevels, RemoteReaderTotals},
    remote_tier::RemoteTierPath,
};
pub(crate) use self::{
    eviction::spawn_metric_series_evictor, labels::UNKNOWN_LABEL, phases::RequestPhases,
    request::QuotaCharge,
};

/// Shared registry owning every metric the broker emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry`
/// to register and we want lazy registration from multiple init paths.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Cheaply-clonable bundle of counter / gauge handles. Construct once
/// in `Broker::start`; hand out clones (each clone is a single
/// `Arc::clone`) to every subsystem that emits.
///
/// Each field's `#[metric(...)]` attribute is the one place that names the
/// family, gives its help text and, for a histogram, its buckets.
/// `RegisterMetrics` derives the constructor and the registration from them,
/// and registers the families in field order, which is the order of the
/// families on `/metrics`.
///
/// `Debug` is opaque, so that a type holding a `BrokerMetrics` can still
/// derive its own. The bundle is a registry and some seventy metric handles,
/// and printing them says nothing a scrape does not say better.
#[derive(Clone, RegisterMetrics, derive_more::Debug)]
#[debug("BrokerMetrics {{ .. }}")]
pub struct BrokerMetrics {
    #[metric(skip, new = Arc::new(Mutex::new(Registry::with_prefix("krabka_broker"))))]
    pub registry: SharedRegistry,
    #[metric(help = "Bytes received from producers, per topic (cumulative). \
        Operators compute throughput via rate(...).")]
    pub topic_bytes_in: Family<TopicLabel, Counter>,
    #[metric(help = "Bytes delivered to fetchers, per topic (cumulative).")]
    pub topic_bytes_out: Family<TopicLabel, Counter>,
    /// Cumulative count of records received from producers,
    /// per topic. Sums `RecordBatch.records.len()` for every batch on
    /// the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.MessagesInPerSec`; pairs with
    /// `topic_bytes_in` to surface both volume and message rate.
    /// Legacy (v0/v1) producers don't contribute — `RecordsPayload`
    /// keeps their bytes opaque until the v2 conversion, so we
    /// count there. The accompanying `produce_message_conversions`
    /// counter still tracks how often legacy batches arrive, so
    /// operators can detect under-counting from a legacy fleet.
    #[metric(
        name = "messages_in",
        help = "Cumulative count of records received from \
        producers, per topic. Mirrors Kafka's \
        BrokerTopicMetrics.MessagesInPerSec. Legacy v0/v1 \
        produce payloads are not counted (their per-record body \
        stays opaque on the Produce path); the paired \
        produce_message_conversions counter tracks the \
        legacy-arrival rate so operators can detect \
        under-counting."
    )]
    pub topic_messages_in: Family<TopicLabel, Counter>,
    #[metric(help = "Produce requests handled, per topic (cumulative). One \
        increment per topic per Produce request.")]
    pub topic_produce_requests: Family<TopicLabel, Counter>,
    #[metric(help = "Fetch requests handled, per topic (cumulative). One \
        increment per topic per Fetch request.")]
    pub topic_fetch_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Produce partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedProduceRequestsPerSec`. Incremented
    /// once per failed partition (matching the JVM's per-row mark),
    /// so a request whose two partitions both fail bumps the topic
    /// counter by 2. Topic-level authorization denials and
    /// unknown-topic responses count, mirroring JVM behavior.
    #[metric(help = "Cumulative count of Produce partition \
        responses that returned a non-zero error code, per \
        topic. Mirrors Kafka's \
        BrokerTopicMetrics.FailedProduceRequestsPerSec. \
        Operators alert on rate(...) > 0 to catch quota / ACL \
        / NOT_ENOUGH_REPLICAS storms; the ratio against \
        topic_produce_requests yields the per-topic error rate.")]
    pub topic_failed_produce_requests: Family<TopicLabel, Counter>,
    /// Per-topic counter of Fetch partition responses
    /// that carried a non-zero error code. Mirrors Kafka's
    /// `BrokerTopicMetrics.FailedFetchRequestsPerSec`. Pairs with
    /// `topic_fetch_requests` to surface error rate.
    #[metric(help = "Cumulative count of Fetch partition \
        responses that returned a non-zero error code, per \
        topic. Mirrors Kafka's \
        BrokerTopicMetrics.FailedFetchRequestsPerSec. Pairs \
        with topic_fetch_requests for per-topic error rate.")]
    pub topic_failed_fetch_requests: Family<TopicLabel, Counter>,
    #[metric(help = "Number of partitions for which this broker is currently leader.")]
    pub partitions_led: Gauge,
    /// Total number of partitions (leader + follower
    /// replicas) this broker hosts. Mirrors Kafka's
    /// `ReplicaManager.PartitionCount`. Sampled in the same per-second
    /// tick as `partitions_led`.
    #[metric(
        name = "partitions_total",
        help = "Total number of partitions (leader + follower \
        replicas) this broker hosts. Mirrors Kafka's \
        ReplicaManager.PartitionCount."
    )]
    pub partitions_total: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// smaller than the assigned replica set — Kafka's
    /// `ReplicaManager.UnderReplicatedPartitions`. Sampled by reading
    /// the current `MetadataImage` and matching partitions where this
    /// broker is the leader. Operators alert on
    /// `under_replicated_partitions > 0` to spot stuck followers
    /// before they fail an unclean election.
    #[metric(help = "Count of partitions this broker leads whose ISR \
        is smaller than the assigned replica set. Mirrors Kafka's \
        ReplicaManager.UnderReplicatedPartitions; alert on > 0 \
        to spot stuck followers before they fail an unclean \
        election.")]
    pub under_replicated_partitions: Gauge,
    /// Count of partitions this broker leads whose ISR is
    /// strictly less than the topic's `min.insync.replicas`. Mirrors
    /// Kafka's `ReplicaManager.UnderMinIsrPartitionCount`. Operators
    /// alert on `under_min_isr_partition_count > 0`: partitions in
    /// this state reject `acks=all` produces with
    /// `NOT_ENOUGH_REPLICAS`, so the metric
    /// surfaces "writes are blocked" before clients start retrying.
    #[metric(help = "Count of partitions this broker leads whose ISR \
        is strictly less than the topic's min.insync.replicas. \
        Mirrors Kafka's ReplicaManager.UnderMinIsrPartitionCount; \
        alert on > 0 — these partitions reject acks=all produces \
        with NOT_ENOUGH_REPLICAS.")]
    pub under_min_isr_partition_count: Gauge,
    /// Count of partitions this broker leads that
    /// currently have no live leader (leader broker dead with no
    /// eligible ISR replacement). Mirrors Kafka's
    /// `ReplicaManager.OfflinePartitionsCount`. Operators alert on
    /// `> 0`: such partitions are wholly unavailable until an ISR
    /// member returns or an unclean election runs.
    #[metric(help = "Count of partitions this broker leads that have \
        no live leader (leader dead with no eligible ISR \
        replacement). Mirrors Kafka's \
        ReplicaManager.OfflinePartitionsCount; alert on > 0 — \
        these partitions are wholly unavailable until an ISR \
        member returns or an unclean election runs.")]
    pub offline_partitions_count: Gauge,
    #[metric(help = "1 if this broker is the raft (controller) leader, 0 otherwise.")]
    pub active_controller: Gauge,
    /// Configured static voters ignored after `kraft.version` reaches 1.
    #[metric(help = "Configured static controller voters ignored at kraft.version 1.")]
    pub ignored_static_voters: Gauge,
    /// 1 when this node carries the data-bearing witness role, 0 otherwise.
    /// The value comes from the `broker.witness` config in the metadata
    /// image, not from the local flag, so it confirms that the role reached
    /// the controller. An operator reads it to see that the role took effect
    /// on the node they meant to configure.
    #[metric(help = "1 if this node carries the data-bearing witness role, 0 \
        otherwise. The value comes from the broker.witness config in \
        the metadata image, so it confirms that the role reached the \
        controller.")]
    pub witness_role: Gauge,
    /// Count of partitions this broker leads from a site other than the
    /// stretch cluster's preferred leader site. It stays at zero on a cluster
    /// that pins leadership to no site. Operators alert on
    /// `leader_site_drift_partitions > 0` to catch leadership that drifted,
    /// such as a failover that no rebalance has undone yet.
    #[metric(help = "Count of partitions this broker leads from a site other than \
        the stretch cluster's preferred leader site. It stays at zero \
        on a cluster that pins leadership to no site; alert on > 0 to \
        catch leadership that drifted away from the pinned site.")]
    pub leader_site_drift_partitions: Gauge,
    /// One-hot series for the directory identity voted for in this epoch.
    #[metric(help = "1 for the controller directory identity voted for in this epoch.")]
    pub voted_directory: Family<DirectoryLabel, Gauge>,
    /// Cumulative count of distinct controller-leader
    /// transitions this broker has observed (any change in the raft
    /// leader, including this broker becoming or ceasing to be
    /// leader). Mirrors Kafka's
    /// `KafkaController.LeaderElectionRateAndTimeMs`. Operators alert
    /// on `rate(controller_leader_changes_total[5m]) > 0` for sustained
    /// periods to spot flapping raft leadership.
    #[metric(help = "Cumulative count of distinct controller-leader \
        transitions this broker has observed (any change in the \
        raft leader, including this broker becoming or ceasing \
        to be leader). Mirrors Kafka's \
        KafkaController.LeaderElectionRateAndTimeMs; alert on a \
        sustained rate() > 0 to spot flapping raft leadership.")]
    pub controller_leader_changes_total: Counter,
    /// Cumulative count of completed broker-fencing publication passes run
    /// by this broker while it holds the controller leadership — one
    /// increment per liveness tick that ran
    /// `heartbeat::fencing::publish_fencing_changes` to completion, whether
    /// or not that pass had a difference to write.
    ///
    /// The pass awaits its own `submit_change`, so an increment means any
    /// fencing record that pass decided on is committed and applied rather
    /// than still in flight. That is what lets a test tell "nobody is fenced"
    /// apart from "a fencing `BrokerRegistrationChangeRecord` is on its way", which the image
    /// alone cannot distinguish. Mirrors the intent of
    /// [`Self::log_cleaner_runs_total`].
    #[metric(help = "Cumulative count of completed broker-fencing publication \
        passes run by this broker while it holds the controller \
        leadership, whether or not the pass had a difference to \
        write. The pass awaits its own commit, so an increment means \
        the fencing state it decided on is applied rather than in \
        flight.")]
    pub controller_fencing_publications_total: Counter,
    #[metric(help = "Cumulative count of ISR shrinks proposed by this broker's \
        ISR-maintenance loop.")]
    pub isr_shrinks_total: Counter,
    #[metric(help = "Cumulative count of ISR expands proposed by this broker's \
        ISR-maintenance loop.")]
    pub isr_expands_total: Counter,
    #[metric(help = "Bytes received from producers, per partition (cumulative). \
        Rebalancer-targeted; rate(...) for throughput.")]
    pub partition_bytes_in: Family<PartitionLabel, Counter>,
    #[metric(help = "Bytes served to consumers, per partition (cumulative). \
        Rebalancer-targeted; rate(...) for throughput.")]
    pub partition_bytes_out: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker accepted from a partition
    /// leader as a follower (`Fetch(replica_id >= 0)` round-trip). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesInPerSec`. Operators
    /// graph `rate(replication_bytes_in_total[1m])` to spot ISR fall-behind
    /// caused by ingest, not by client read load.
    #[metric(help = "Bytes received from the partition leader by this broker as a \
        follower (cumulative). Rate(...) for follower throughput; \
        plotted alongside partition_bytes_in surfaces ingest vs. \
        replication-driven traffic.")]
    pub replication_bytes_in: Family<PartitionLabel, Counter>,
    /// Cumulative bytes this broker served *to* a follower
    /// (i.e. the leader-side outbound for inter-broker `Fetch`). Mirrors
    /// Kafka's `BrokerTopicMetrics.replicationBytesOutPerSec`. Operators
    /// graph the per-partition rate to attribute leader outbound to
    /// followers vs. consumers (the latter still rolls up to
    /// `partition_bytes_out`).
    #[metric(
        help = "Bytes this broker served to followers as the partition leader \
        (cumulative). Rate(...) for leader-out-to-followers throughput; \
        together with partition_bytes_out (consumer reads) it attributes \
        outbound traffic to its source."
    )]
    pub replication_bytes_out: Family<PartitionLabel, Counter>,
    /// Records each follower of a partition this broker leads still has to
    /// fetch: the leader's log end offset minus the follower's last-fetched
    /// offset, per follower.
    ///
    /// `under_replicated_partitions` says only that a follower fell out of the
    /// ISR. This says how far behind it is while it is still in, which is what
    /// tells an operator whether a follower is drifting toward an ISR shrink
    /// or holding steady. Mirrors the per-partition half of Kafka's
    /// `ReplicaFetcherManager` lag reporting.
    #[metric(
        name = "replica_lag_records",
        help = "Records a follower of a partition this broker leads has yet \
        to fetch: the leader's log end offset minus that follower's \
        last-fetched offset. Where under_replicated_partitions says \
        only that a follower left the ISR, this says how far behind \
        it is while it is still in."
    )]
    pub replica_lag: Family<ReplicaLagLabel, Gauge>,
    /// The largest value `replica_lag` carries on this broker, or zero when it
    /// leads no partition with a follower. Mirrors Kafka's
    /// `ReplicaFetcherManager.MaxLag`: one series an operator alerts on
    /// without having to aggregate a per-follower family first.
    #[metric(
        name = "replica_lag_max_records",
        help = "The largest value replica_lag_records carries on this \
        broker, or zero when it leads no partition with a follower. \
        Mirrors Kafka's ReplicaFetcherManager.MaxLag; alert on it \
        rather than aggregating the per-follower family."
    )]
    pub replica_lag_max: Gauge,
    /// Records a consumer group has yet to consume from one partition: the
    /// partition's high watermark minus the group's committed offset.
    ///
    /// It covers classic and KIP-848 groups alike, because committed offsets
    /// live on the protocol-agnostic `CoordinatorGroup`. This is the metric
    /// most consumer-owning teams alert on, and the broker is the only place
    /// that holds both halves of the subtraction without a client round trip.
    #[metric(
        name = "consumer_group_lag_records",
        help = "Records a consumer group this broker coordinates has yet to \
        consume from one partition: the partition's high watermark \
        minus the group's committed offset. Classic and KIP-848 \
        groups both report here."
    )]
    pub consumer_group_lag: Family<ConsumerGroupLabel, Gauge>,
    #[metric(
        help = "On-disk size of a partition's log directory (gauge). Updated by \
        the broker's periodic disk scanner; suppress if scanner is disabled."
    )]
    pub partition_disk_bytes: Family<PartitionLabel, Gauge>,
    /// Records waiting for acquisition in each share-group partition.
    #[metric(help = "Share-group partition backlog in records, emitted by the group coordinator.")]
    pub share_group_backlog: Family<ShareGroupLabel, Gauge>,
    /// Dead-letter records a share group has had written to its queue
    /// (KIP-1191): the records of the produce rounds that succeeded. Mirrors
    /// Kafka's `ShareGroupMetrics.DeadLetterQueueRecordCount`.
    #[metric(help = "Cumulative count of dead-letter records written to a share \
        group's dead-letter queue (KIP-1191), per group. Mirrors Kafka's \
        ShareGroupMetrics.DeadLetterQueueRecordCount.")]
    pub share_group_dlq_records: Family<ShareGroupIdLabel, Counter>,
    /// Attempts to produce a share group's dead-letter records: one for each
    /// round of each range for each attempt, not one for each coalesced
    /// produce request, as in Kafka. Mirrors Kafka's
    /// `ShareGroupMetrics.DeadLetterQueueTotalProduceRequestsPerSec`.
    #[metric(help = "Cumulative count of attempts to produce a share group's \
        dead-letter records, per group: one for each round of each range \
        for each attempt. Mirrors Kafka's \
        ShareGroupMetrics.DeadLetterQueueTotalProduceRequestsPerSec.")]
    pub share_group_dlq_produce_requests: Family<ShareGroupIdLabel, Counter>,
    /// Dead-letter writes of a share group that ended in a failure: a produce
    /// that a broker refused, or that ran out of attempts. Mirrors Kafka's
    /// `ShareGroupMetrics.DeadLetterQueueFailedProduceRequestsPerSec`.
    #[metric(help = "Cumulative count of dead-letter writes of a share group that \
        ended in a failure, per group. Mirrors Kafka's \
        ShareGroupMetrics.DeadLetterQueueFailedProduceRequestsPerSec; \
        the ratio against share_group_dlq_produce_requests yields the \
        per-group error rate.")]
    pub share_group_dlq_failed_produce_requests: Family<ShareGroupIdLabel, Counter>,
    /// Cumulative handler-thread microseconds spent processing each
    /// (topic, partition). Exported as
    /// `krabka_broker_partition_cpu_micros_total`. Rebalancer takes
    /// `rate(...)` to get micros/sec; dividing by `1_000_000` yields the
    /// per-partition core occupancy. We track microseconds (integer
    /// counter) rather than seconds (float) because `prometheus-client`
    /// counters are `u64`.
    #[metric(help = "Cumulative handler-thread microseconds spent processing each \
        (topic, partition). Rebalancer-targeted; rate(...) divided by \
        1_000_000 yields core occupancy.")]
    pub partition_cpu_micros: Family<PartitionLabel, Counter>,
    /// Cumulative count of drained Fetch responses, split by the path their
    /// records regions took to the socket.
    ///
    /// One increment per response the drain finished, labelled with the
    /// strongest path any of its records regions took: `sendfile` when the
    /// kernel moved a region with no userspace copy, `pread` when a
    /// file-backed region had to be copied through a buffer anyway, and
    /// `vectored` when the response carried no file-backed region at all. A
    /// response the connection failed to write is not counted, because the
    /// path it would have taken is not what the client received.
    ///
    /// This is the only series that says whether the zero-copy fetch path is
    /// being used. Operators alert on
    /// `rate(fetch_response_drain_total{path="sendfile"}[5m]) == 0` on a
    /// cluster whose consumers read plaintext, because a regression that
    /// routes every fetch onto a copy path is otherwise invisible.
    ///
    /// All three series exist on every platform from startup, at zero. On
    /// Windows `sendfile` and `pread` stay at zero for the life of the
    /// process: the platform has no safe file-to-socket call, so every fetch
    /// is `vectored`.
    #[metric(
        help = "Cumulative count of drained Fetch responses, labelled by the path \
        their records regions took to the socket: sendfile (kernel \
        zero-copy), pread (a file-backed region the drain had to copy \
        through a buffer), or vectored (no file-backed region). \
        rate(...{path=\"sendfile\"}) is how an operator sees that the \
        zero-copy fetch path is carrying traffic."
    )]
    pub fetch_response_drain: Family<FetchDrainPathLabel, Counter>,
    /// `1` when this broker's startup probe found working Linux kTLS and TLS
    /// fetch connections therefore drain records through kernel-offloaded
    /// `sendfile`; `0` when they encrypt in userspace.
    ///
    /// It is `0` on a broker with no TLS listener, which never probes, and `0`
    /// on every non-Linux target, where kTLS does not exist. The probe runs
    /// once, so the value is a constant of the running process: an operator
    /// reads it to tell "this build cannot do kTLS" apart from "this kernel
    /// would not take it", and reads it beside
    /// `fetch_response_drain_total{path="sendfile"}` to see whether the
    /// offload is actually carrying traffic.
    #[metric(
        help = "1 when the startup probe found working Linux kTLS and TLS fetch \
        connections drain records through kernel-offloaded sendfile; 0 \
        when they encrypt in userspace, including on a broker with no TLS \
        listener and on every non-Linux target."
    )]
    pub ktls_enabled: Gauge,
    /// KIP-227: current count of live incremental-fetch sessions across the
    /// per-broker cache. Sampled periodically from `FetchSessionCache::len()`.
    #[metric(help = "KIP-227: live incremental-fetch sessions cached by this broker (gauge).")]
    pub incremental_fetch_sessions: Gauge,
    /// KIP-227: cumulative count of incremental-fetch sessions evicted to
    /// make room for a new allocation. Incremented inside the cache.
    #[metric(
        help = "KIP-227: cumulative count of incremental-fetch sessions evicted from \
        the cache to make room for a new allocation."
    )]
    pub incremental_fetch_session_evictions_total: Counter,
    /// KIP-227: sum of `session.partitions.len()` across every live session.
    /// Sampled periodically alongside `incremental_fetch_sessions`.
    #[metric(
        help = "KIP-227: total (topic, partition) tuples held across every live \
        incremental-fetch session (gauge)."
    )]
    pub incremental_fetch_partitions_cached: Gauge,
    /// KIP-511: per-(name, version) counter of accepted v3+ `ApiVersions`
    /// handshakes. Operators graph this to see which client libraries
    /// and versions are connecting.
    #[metric(
        help = "KIP-511: cumulative count of accepted ApiVersions handshakes, \
        labelled by client software name and version. One increment \
        per successful v3+ ApiVersions call."
    )]
    pub client_software_versions: Family<ClientSoftwareLabel, Counter>,
    /// Cumulative count of completed `SaslAuthenticate`
    /// frames per mechanism that ended in a successful auth state
    /// transition. Mirrors Kafka's
    /// `kafka.network:type=Selector,name=successful-authentication-total`.
    /// Paired with `failed_authentication` so operators compute the
    /// auth failure ratio per mechanism at scrape time.
    #[metric(help = "Cumulative count of SaslAuthenticate frames per \
        mechanism that ended in a successful auth state transition. \
        Mirrors Kafka's \
        kafka.network:type=Selector,name=successful-authentication-total. \
        Labelled by the canonical SASL mechanism wire name \
        (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, GSSAPI). \
        Paired with failed_authentication so rate(...) ratios \
        expose per-mechanism credential-failure rates.")]
    pub successful_authentication: Family<SaslMechanismLabel, Counter>,
    /// Cumulative count of `SaslAuthenticate` frames per
    /// mechanism that returned a non-zero error code. Mirrors
    /// Kafka's `failed-authentication-total`. The `"Unknown"`
    /// mechanism label covers `ILLEGAL_SASL_STATE` rejects where
    /// the connection sent `SaslAuthenticate` without first
    /// completing a `SaslHandshake`; per-mechanism failures land
    /// under the canonical wire name (`PLAIN`, `SCRAM-SHA-256`,
    /// `SCRAM-SHA-512`, `OAUTHBEARER`).
    #[metric(help = "Cumulative count of SaslAuthenticate frames per \
        mechanism that returned a non-zero error code. Mirrors \
        Kafka's failed-authentication-total. ILLEGAL_SASL_STATE \
        rejects (SaslAuthenticate without prior SaslHandshake) \
        land under the `Unknown` mechanism label.")]
    pub failed_authentication: Family<SaslMechanismLabel, Counter>,
    /// Cumulative count of authorization decisions that came back Deny,
    /// labelled by the operation and the resource type the request asked
    /// for. Kafka exports no equivalent metric: `StandardAuthorizer` only
    /// writes a line to the `kafka.authorizer.logger` log4j logger, and
    /// operators alert on log volume. Bumped by `AuditingAuthorizer`, which
    /// wraps the configured authorizer whether or not audit is enabled, so
    /// the counter is the one denial signal that survives `audit.enabled=false`.
    /// Alert on `rate(...[5m]) > 0` sustained: a fleet that was authorized
    /// yesterday and is denied today is an ACL change, not a client bug.
    #[metric(
        help = "Cumulative count of authorization decisions that came back Deny, \
        labelled by the requested operation and resource type. Kafka has \
        no equivalent metric: StandardAuthorizer and AclAuthorizer only \
        write a Denied Operation line to the kafka.authorizer.logger \
        log4j logger, which operators alert on by log volume. The \
        counter is bumped whether or not audit is enabled, so it is \
        the one denial signal a cluster with audit.enabled=false still \
        has. Both labels come from closed enums, so cardinality is bounded."
    )]
    pub authorization_denied: Family<AuthorizationDeniedLabel, Counter>,
    /// Per-Kafka-API request counter. Bumped once per
    /// dispatched request from the network dispatcher, labelled by
    /// the `ApiKey` variant name (or `"Unknown"` for unrecognised
    /// keys). Mirrors Kafka's `RequestMetrics.RequestsPerSec`; rate(...)
    /// gives operators per-API request throughput across the
    /// broker without needing to slice the dashboard by handler.
    #[metric(help = "Cumulative count of dispatched requests per \
        Kafka API key (variant name from the `ApiKey` enum, e.g. \
        Produce / Fetch / DescribeQuorum). Unknown api keys land \
        under the `Unknown` label. Mirrors Kafka's \
        RequestMetrics.RequestsPerSec; rate(...) yields per-API \
        throughput.")]
    pub api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API counter of requests the dispatcher
    /// rejected because the request version was outside the registered range.
    /// Operators alert on `rate(unsupported_api_requests_total[5m]) > 0`
    /// to catch clients on `api_key`/version pairs the broker
    /// doesn't speak — frequently the smoking gun for upgrade-skew
    /// or misconfigured clients.
    #[metric(help = "Cumulative count of requests the dispatcher rejected \
        because the request version was outside the registered range. \
        Labelled with the ApiKey variant name. Alert on rate(...) > 0 \
        to catch upgrade-skew or \
        misconfigured clients.")]
    pub unsupported_api_requests: Family<ApiKeyLabel, Counter>,
    /// Per-Kafka-API request-handling latency in seconds
    /// (`krabka_broker_request_duration_seconds{api}`). Observed in the
    /// dispatch path around the full handler round-trip (decode → handle →
    /// encode) for every dispatched frame, labelled by the `ApiKey`
    /// variant name. Operators graph
    /// `histogram_quantile(0.99, rate(request_duration_seconds_bucket[5m]))`
    /// per api to spot handler tail-latency regressions, and use `_count`
    /// as a request-rate stream that pairs with `api_requests`.
    #[metric(help = "Per-Kafka-API request-handling latency in \
        seconds, observed in the dispatch path around the full \
        handler round-trip (decode → handle → encode). Labelled by \
        the ApiKey variant name. Operators graph \
        histogram_quantile(0.99, rate(..._bucket[5m])) per api to \
        spot tail-latency regressions.", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub request_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Per-Kafka-API seconds one request spent on this broker's own log
    /// (`krabka_broker_request_local_duration_seconds{api_key}`). It is the
    /// Produce writer round-trip — the enqueue plus the append acknowledgement
    /// — summed over the request's partitions, and the Fetch read of every
    /// planned partition, including the re-read a long poll performs. Mirrors
    /// Kafka's `RequestMetrics.LocalTimeMs`.
    ///
    /// It is one of the three phase families that partition
    /// [`Self::request_duration_seconds`]. See the family's rustdoc on
    /// [`Self::request_throttle_duration_seconds`] for what the three do and
    /// do not sum to.
    #[metric(help = "Per-Kafka-API seconds one request spent on this \
        broker's own log: the Produce writer round-trip summed over the \
        request's partitions, or the Fetch read of every planned \
        partition. Mirrors Kafka's RequestMetrics.LocalTimeMs. Labelled \
        by the ApiKey variant name, like request_duration_seconds.", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub request_local_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Per-Kafka-API seconds one request spent waiting on something that is
    /// not this broker's own log
    /// (`krabka_broker_request_remote_duration_seconds{api_key}`). For Produce
    /// it is the `acks=all` high-watermark gate, summed over the request's
    /// partitions: the wait for every in-sync replica to take the append. For
    /// Fetch it is the long poll that parks a `min_bytes`-unsatisfied read on
    /// the partitions' notifiers, plus the object-store round trip a KIP-405
    /// tiered read or a diskless WAL cold read makes when the local log no
    /// longer holds the offset. Mirrors Kafka's `RequestMetrics.RemoteTimeMs`,
    /// which covers the tiered read for the same reason: Kafka serves it out
    /// of a `DelayedRemoteFetch` in the purgatory, and the purgatory wait is
    /// what that metric measures.
    ///
    /// This is the series that separates a lagging follower or a slow object
    /// store from a slow local disk: a produce that is slow here and fast in
    /// [`Self::request_local_duration_seconds`] is waiting on replication, not
    /// on this broker, and a fetch that is slow here on a tiered topic is
    /// waiting on the tier.
    #[metric(help = "Per-Kafka-API seconds one request spent waiting on \
        another broker: the acks=all high-watermark gate for Produce, \
        the long poll for Fetch. Mirrors Kafka's \
        RequestMetrics.RemoteTimeMs. High here with a low \
        request_local_duration_seconds is a lagging follower, not a slow \
        disk.", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub request_remote_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Per-Kafka-API seconds one request spent asleep in the KIP-219 quota
    /// throttle (`krabka_broker_request_throttle_duration_seconds{api_key}`).
    /// Mirrors Kafka's `RequestMetrics.ThrottleTimeMs`. It is observed once
    /// per request whose quota the broker accounts for, with an explicit zero
    /// when no quota applied, so a throttled fleet is visible as a shift in
    /// the distribution rather than as an appearing series. The apis the
    /// dispatch registry marks quota-exempt are observed only where they
    /// resolve a throttle of their own — the KIP-599 sleep on `CreateTopics`,
    /// `CreatePartitions` and `DeleteTopics` — and not at all otherwise, so
    /// this `_count` is at most [`Self::request_duration_seconds`]'s.
    ///
    /// The three phase families are disjoint: a request is in exactly one of
    /// them at a time, and each interval is charged to exactly one. They do
    /// **not** cover the total. `local + remote + throttle <=
    /// request_duration_seconds`, and the remainder is the work no phase
    /// names — request decode, authorization, record validation, response
    /// encode. An operator checks the phases against the total by comparing
    /// `_sum` streams; a remainder that grows is handler-side CPU, not disk
    /// and not replication.
    #[metric(help = "Per-Kafka-API seconds one request slept in the \
        KIP-219 quota throttle, or in the KIP-599 controller-mutation \
        throttle the topic-mutating admin apis apply inline. Mirrors \
        Kafka's RequestMetrics.ThrottleTimeMs. Observed once per request \
        whose quota the broker accounts for, with an explicit zero when no \
        quota applied. The three phase families are disjoint and sum to \
        at most the total; the remainder is decode, authorization, \
        validation and encode.", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub request_throttle_duration_seconds: Family<ApiKeyLabel, Histogram>,
    /// Seconds of throttle this broker actually applied, by the quota that
    /// caused it (`krabka_broker_quota_throttle_duration_seconds{quota_type}`).
    ///
    /// A request charges several quotas and sleeps for the largest delay of
    /// them, so the sample lands under the [`QuotaType`] that produced that
    /// largest delay — the quota an operator would have to raise to make the
    /// throttle stop. Requests that no quota delayed are not observed here, so
    /// unlike [`Self::request_throttle_duration_seconds`] the `_count` of this
    /// family is the number of throttled requests, and `_sum` is the wall
    /// time the broker held clients back.
    #[metric(help = "Seconds of throttle the broker actually applied, \
        labelled by the client quota that caused it (Produce = \
        producer_byte_rate, Fetch = consumer_byte_rate, Request = \
        request_percentage, ControllerMutation = \
        controller_mutation_rate). A request sleeps for the largest of the \
        delays it is charged, and the sample lands under the quota that \
        produced it — the one an operator would raise to stop the \
        throttle. Unthrottled requests are not observed, so _count is \
        the number of throttled requests.", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub quota_throttle_duration_seconds: Family<QuotaTypeLabel, Histogram>,
    /// Number of requests currently being handled by this
    /// broker (gauge). Incremented on dispatch entry, decremented on exit
    /// (including the error/close path). A sustained climb signals handler
    /// stalls or a wedged downstream (controller / replication).
    #[metric(help = "Number of requests currently being handled by this broker \
        (gauge). Incremented on dispatch entry, decremented on exit; \
        a sustained climb signals handler stalls.")]
    pub in_flight_requests: Gauge,
    /// Number of client connections currently open to this
    /// broker (gauge). Incremented when a connection is accepted and the
    /// per-connection serve loop starts, decremented when that loop exits
    /// (EOF, error, or SASL-session expiry). Mirrors Kafka's
    /// `kafka.network:type=Acceptor` connection-count intent.
    #[metric(help = "Number of client connections currently open to this broker \
        (gauge). Incremented when the per-connection serve loop \
        starts, decremented when it exits (EOF / error / SASL expiry).")]
    pub active_connections: Gauge,
    /// Cumulative count of client connections the broker closed for one of the
    /// bounded [`ConnectionCloseReason`]s, which are the ways a connection ends
    /// on its own rather than as the tail of a request the request counters
    /// already saw. Kafka has no one counterpart; the closest are
    /// `kafka.network:type=Selector,name=connection-close-total` and the
    /// `expired-connections-killed-count` that only counts the idle arm.
    /// `rate(connection_closes_total{reason="idle"}[5m])` is the signal that a
    /// peer is opening connections and then sending nothing, which the
    /// `max.connections`/`max.connections.per.ip` caps alone do not surface.
    #[metric(
        help = "Cumulative count of client connections the broker closed on its \
        own, labelled by reason: idle, sasl_session_expired, \
        decode_error, peer_closed, max_connections, \
        max_connections_per_ip. Alert on \
        rate(...{reason=\"idle\"}[5m]) to catch a peer that connects and \
        then sends nothing, and on the two max_connections reasons to \
        catch a listener refusing clients at its limit."
    )]
    pub connection_closes: Family<ConnectionCloseReasonLabel, Counter>,
    /// Per-Kafka-API counter of requests whose handler
    /// returned an error (the dispatcher closed the connection). Labelled
    /// by the `ApiKey` variant name; disjoint from
    /// `unsupported_api_requests` (which counts the unsupported-version arm).
    /// Operators alert on
    /// `rate(request_errors_total[5m]) > 0` to catch handler-level faults.
    #[metric(help = "Per-Kafka-API count of requests whose handler \
        returned an error (dispatcher closed the connection). \
        Labelled by the ApiKey variant name; disjoint from \
        unsupported_api_requests. Alert on rate(...) > 0 to catch \
        handler-level faults.")]
    pub request_errors: Family<ApiKeyLabel, Counter>,
    /// KIP-405: `1` when this broker has finished swapping in
    /// the topic-backed `RemoteLogMetadataManager` and is
    /// answering metadata queries from the durable
    /// `__remote_log_metadata` topic; `0` while still on the
    /// fail-closed `NotReadyRlmm` placeholder (the default until a
    /// configured `[remote_storage.kafka_metadata]` bootstrap completes).
    /// Operators alert on
    /// `min_over_time(tiered_storage_rlmm_topic_backed[5m]) == 0`
    /// against clusters that asked for `metadataManager: Topic` to catch
    /// a stuck bootstrap.
    #[metric(help = "KIP-405: 1 when this broker is answering remote-log \
        metadata queries from the durable __remote_log_metadata topic \
        (production RLMM); 0 while still on the fail-closed \
        NotReadyRlmm placeholder. Bumped to 1 by the bootstrap task \
        after a successful SwappableRlmm swap; stays at 0 for \
        clusters that never asked for `metadataManager: Topic`.")]
    pub tiered_storage_rlmm_topic_backed: Gauge,
    /// Number of topic-backed RLMM bootstrap attempts; climbs while stuck
    /// retrying, flat once `tiered_storage_rlmm_topic_backed` flips to 1.
    #[metric(help = "Number of topic-backed RLMM bootstrap attempts; climbs while \
        stuck retrying, flat once tiered_storage_rlmm_topic_backed \
        flips to 1.")]
    pub tiered_storage_rlmm_bootstrap_attempts: Counter,
    /// Per-topic counter of v0/v1 → v2 record-batch
    /// up-conversions on the Produce path. Mirrors Kafka's
    /// `BrokerTopicMetrics.ProduceMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Produce request whose
    /// `records` field arrived as a legacy `MessageSet`.
    #[metric(help = "Cumulative count of v0/v1 → v2 record-batch \
        up-conversions on the Produce path, per topic. Mirrors \
        Kafka's BrokerTopicMetrics.ProduceMessageConversionsPerSec; \
        rate(...) lets operators spot the overhead of legacy \
        producers in the cluster.")]
    pub produce_message_conversions: Family<TopicLabel, Counter>,
    /// Per-topic counter of v2 → v0/v1 record-batch
    /// down-conversions on the Fetch path. Mirrors Kafka's
    /// `BrokerTopicMetrics.FetchMessageConversionsPerSec`. Bumped
    /// once per partition's slice of a Fetch response whose response
    /// payload was down-converted to satisfy a legacy (`Fetch v < 4`)
    /// client.
    #[metric(help = "Cumulative count of v2 → v0/v1 record-batch \
        down-conversions on the Fetch path, per topic. Mirrors \
        Kafka's BrokerTopicMetrics.FetchMessageConversionsPerSec; \
        rate(...) lets operators spot the overhead of legacy \
        consumers in the cluster.")]
    pub fetch_message_conversions: Family<TopicLabel, Counter>,
    /// KIP-841: cumulative count of unclean leader
    /// elections this broker, as controller leader, has driven —
    /// i.e. elections that picked an out-of-ISR replica as the new
    /// leader because the topic had
    /// `unclean.leader.election.enable=true` and the ISR was empty
    /// at failover time. Mirrors Kafka's
    /// `ControllerStats.UncleanLeaderElectionsPerSec`, which counts the
    /// elections Kafka's `ElectionResult` marks `unclean`. KIP-966
    /// recovery from a surviving eligible leader replica loses no
    /// committed record and is not one of them. An operator alert on
    /// `rate(unclean_leader_elections_total[5m]) > 0` flags the
    /// data-loss footgun.
    #[metric(help = "KIP-841: cumulative count of unclean leader \
        elections driven by this broker (as controller leader). An \
        unclean election is one where the new leader was picked \
        from outside the ISR because the partition's ISR was empty \
        at failover time and the topic had \
        unclean.leader.election.enable=true. Each such election \
        accepts possible data loss. Mirrors Kafka's \
        ControllerStats.UncleanLeaderElectionsPerSec; an operator \
        alert on rate(unclean_leader_elections_total[5m]) > 0 \
        flags the data-loss footgun.")]
    pub unclean_leader_elections_total: Counter,
    /// `FedRAMP` MLA: cumulative audit records successfully written to the
    /// audit topic. Incremented by the audit subsystem on each successful
    /// produce to `__krabka_audit`.
    #[metric(help = "Cumulative audit records successfully written to the audit topic")]
    pub audit_events: Counter,
    /// `FedRAMP` MLA: cumulative audit records that failed to write to the
    /// audit topic. Incremented on each produce error; operators alert on
    /// `rate(audit_write_failures_total[5m]) > 0`.
    #[metric(help = "Cumulative audit records that failed to write to the audit topic")]
    pub audit_write_failures: Counter,
    /// Current count of audit records buffered in the durable spool (gauge).
    #[metric(help = "Current count of audit records buffered in the durable spool")]
    pub audit_spool_depth: Gauge,
    /// Current bytes buffered in the durable audit spool (gauge).
    #[metric(help = "Current bytes buffered in the durable audit spool")]
    pub audit_spool_bytes: Gauge,
    /// Cumulative audit records diverted to the spool on topic-write failure.
    #[metric(help = "Cumulative audit records diverted to the spool on topic-write failure")]
    pub audit_records_spooled_total: Counter,
    /// Cumulative audit records drained from the spool back to the topic.
    #[metric(help = "Cumulative audit records drained from the spool back to the topic")]
    pub audit_records_replayed_total: Counter,
    /// Cumulative audit records lost (channel-full or spool-full).
    #[metric(help = "Cumulative audit records lost (channel-full or spool-full)")]
    pub audit_records_dropped_total: Counter,
    /// KIP-714 client-metric batches dropped because the bounded OTLP queue
    /// was full or closed.
    #[metric(help = "Cumulative KIP-714 client-metric batches dropped before OTLP export")]
    pub client_metrics_otlp_dropped_total: Counter,
    /// KIP-714 client-metric export attempts rejected by the collector or
    /// failed at the transport layer.
    #[metric(help = "Cumulative failed KIP-714 client-metric OTLP export attempts")]
    pub client_metrics_otlp_failed_total: Counter,
    /// Cumulative count of *clean* log-compaction sweeps run by this
    /// broker's cleaner — one increment per `tick_all` pass that dispatched
    /// no compaction which then failed, whether or not any partition was
    /// eligible. Lets tests (and operators) observe that the compaction
    /// ticker has completed at least one full pass after a segment was
    /// sealed, replacing fixed `sleep`s with a poll on this counter. Mirrors
    /// the intent of Kafka's `LogCleaner` run accounting.
    ///
    /// A sweep that failed a partition is accounted in
    /// [`Self::log_cleaner_failures`] and leaves this counter where it was,
    /// so a cleaner failing every partition on a dying disk reports a flat
    /// pass rate rather than a healthy one. `rate()` on this series is
    /// therefore the rate of passes that did what they were for.
    #[metric(help = "Cumulative count of clean log-compaction sweeps run by this \
        broker's cleaner (one per tick_all pass that failed no \
        partition).")]
    pub log_cleaner_runs_total: Counter,
    /// Per-partition, per-reason cumulative count of compaction passes that
    /// failed. One increment per failed `Partition::compact_log` call.
    ///
    /// This is the counter that says the cleaner is not doing its job.
    /// `log_cleaner_runs_total` cannot: a sweep whose partitions all failed
    /// is not counted there at all, and a broker with nothing to compact is
    /// indistinguishable from one that compacts everything. Alert on
    /// `rate(log_cleaner_failures_total[15m]) > 0`: compaction and local
    /// retention stop together on a compacted topic, so a failing cleaner is
    /// a disk that fills with nothing else to say so.
    ///
    /// `reason` is [`CleanerFailureReason`]: `io` for a storage failure,
    /// which the writer arm has already reported to the log-dir registry,
    /// `writer` for a partition whose actor is gone, and `other` for
    /// anything else the log layer returned.
    #[metric(
        help = "Per-partition, per-reason cumulative count of compaction passes \
        that failed. Alert on rate(...) > 0: compaction and local \
        retention stop together, so a failing cleaner is a disk that \
        fills with nothing else to say so."
    )]
    pub log_cleaner_failures: Family<CleanerFailureLabel, Counter>,
    /// Cumulative count of *clean* local-retention sweeps run by this broker
    /// — one increment per pass of the broker-wide local-retention loop that
    /// failed no partition, whether or not any segment was evicted.
    /// Mirrors [`Self::log_cleaner_runs_total`] for the half of Kafka's log
    /// maintenance that `LogManager.cleanupLogs` does.
    #[metric(help = "Cumulative count of clean local-retention sweeps run by this \
        broker (one per pass that failed no partition). The half of log \
        maintenance Kafka's LogManager.cleanupLogs does.")]
    pub log_retention_runs_total: Counter,
    /// Per-partition, per-reason cumulative count of local-retention passes
    /// that failed. One increment per failed `Partition::retain_log` call.
    ///
    /// This is the counter that says segments are not coming off the disk.
    /// Nothing else reports it: a broker that never evicts a segment looks
    /// exactly like one with nothing to evict until the disk fills. Alert on
    /// `rate(log_retention_failures_total[15m]) > 0`.
    ///
    /// `reason` is [`CleanerFailureReason`], read exactly as it is on
    /// [`Self::log_cleaner_failures`]: `io` for a storage failure, which the
    /// writer arm has already reported to the log-dir registry, `writer` for
    /// a partition whose actor is gone, and `other` for anything else the log
    /// layer returned.
    #[metric(
        help = "Per-partition, per-reason cumulative count of local-retention \
        passes that failed. Alert on rate(...) > 0: a broker that never \
        evicts a segment looks like one with nothing to evict until the \
        disk fills."
    )]
    pub log_retention_failures: Family<CleanerFailureLabel, Counter>,
    /// Partitions this broker hosts whose most recent compaction attempt
    /// failed and which have not compacted since.
    ///
    /// Kafka's `LogCleanerManager` publishes `uncleanable-partitions-count`
    /// for the same reason: a partition drops out of the cleaner's reach and
    /// nothing else reports it. The cleaner sweeps every hosted replica, as
    /// Kafka's does, so a follower counts here exactly as a leader does. The
    /// gauge is republished at the end of every sweep, so a partition returns
    /// to zero on the first pass that succeeds and is released when the
    /// broker stops hosting the replica or the topic stops being compacted.
    #[metric(help = "Partitions this broker hosts whose most recent compaction \
        attempt failed and which have not compacted since. Mirrors \
        Kafka's LogCleanerManager uncleanable-partitions-count.")]
    pub log_cleaner_uncleanable_partitions: Gauge,
    /// Log directories this broker has marked offline: the ones that failed
    /// the startup writability probe, plus the ones a live write or fsync
    /// failure flipped.
    ///
    /// Mirrors Kafka's `kafka.log:type=LogManager,name=OfflineLogDirectoryCount`.
    /// `DescribeLogDirs` reports the same dirs with `KAFKA_STORAGE_ERROR`,
    /// but only to a client that asks; this is the series that pages. It is
    /// sampled by the broker gauge updater, so it appears on a broker with
    /// no offline dir at zero.
    #[metric(
        help = "Log directories this broker has marked offline, whether by the \
        startup writability probe or by a live write/fsync failure. \
        Mirrors kafka.log:type=LogManager,name=OfflineLogDirectoryCount."
    )]
    pub offline_log_dirs: Gauge,
    /// Committed metadata records a broker-only node could not decode, since
    /// it started.
    ///
    /// Mirrors Kafka's
    /// `kafka.server:type=broker-metadata-metrics,name=metadata-load-error-count`,
    /// which Kafka's `SharedServer` bumps from the non-fatal "metadata
    /// loading" fault handler of a node without the controller role. The
    /// broker gauge updater samples the observer's running count, as Kafka's
    /// gauge reads its `AtomicLong`, so the series is zero on a healthy broker.
    /// A controller stops on such a record instead, and counts nothing here.
    #[metric(help = "Committed metadata records this broker-only node could not \
        decode and skipped, since it started. Mirrors \
        kafka.server:type=broker-metadata-metrics,name=metadata-load-error-count.")]
    pub metadata_load_error_count: Gauge,
    /// Per-partition cumulative count of compaction passes
    /// (`Partition::compact_log`) this broker's cleaner completed
    /// successfully. Bumped once per eligible (leader &&
    /// `cleanup.policy=compact`) partition per sweep. Pairs with
    /// `log_cleaner_runs_total`: a test that seals a segment then waits for
    /// this counter to advance knows the sealed segment has been through a
    /// compaction pass without guessing a duration.
    #[metric(help = "Per-partition cumulative count of compaction passes this \
        broker's cleaner completed successfully.")]
    pub log_compactions_total: Family<PartitionLabel, Counter>,
    /// Per-group count of barrier epochs the coordinator started. It
    /// increments when the coordinator writes the injection-start record that
    /// freezes the target set, before it appends the first marker.
    #[metric(help = "Per-barrier-group cumulative count of epochs the coordinator \
        started. Bumped when it writes the injection-start record that \
        freezes the target set, before the first marker append.")]
    pub barrier_epochs_started_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group count of barrier epochs that reached every partition of the
    /// group. The coordinator published a complete cut for each one.
    #[metric(help = "Per-barrier-group cumulative count of epochs whose marker \
        reached every partition of the group. The coordinator published \
        a complete cut for each one.")]
    pub barrier_epochs_committed_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group count of barrier epochs whose cut names at least one
    /// partition that got no marker. The coordinator publishes the partial cut
    /// and consumes the epoch, so
    /// `rate(barrier_epochs_published_partial_total[5m]) > 0` is the alert an
    /// operator sets on a group that does not reach all of its partitions.
    #[metric(
        help = "Per-barrier-group cumulative count of epochs whose cut names at \
        least one partition that got no marker. The coordinator consumes \
        the epoch either way. Alert on rate(...) > 0 to catch a group \
        that no longer reaches all of its partitions."
    )]
    pub barrier_epochs_published_partial_total: Family<BarrierGroupLabel, Counter>,
    /// Per-group wall-clock seconds from the injection-start record to the
    /// published cut. Operators graph
    /// `histogram_quantile(0.99, rate(..._bucket[5m]))` against
    /// `barrier_injection_timeout` to see how much headroom a group has.
    #[metric(help = "Per-barrier-group wall-clock seconds from the injection-start \
        record to the published cut. Graph histogram_quantile(0.99, \
        rate(..._bucket[5m])) against barrier_injection_timeout to see \
        how much headroom a group has.", buckets = registration::BARRIER_INJECTION_DURATION_BUCKETS)]
    pub barrier_injection_duration_seconds: Family<BarrierGroupLabel, Histogram>,
    /// Per-group epoch of the newest cut this coordinator published (gauge).
    /// A flat value beside a live `barrier_min_injection_interval` says that
    /// injection stopped.
    #[metric(help = "Per-barrier-group epoch of the newest cut this coordinator \
        published (gauge). A flat value beside a live \
        barrier_min_injection_interval says that injection stopped.")]
    pub barrier_latest_epoch: Family<BarrierGroupLabel, Gauge>,
    /// Per-topic count of barrier markers this broker appended, across every
    /// group and every partition it leads. Markers survive compaction, so this
    /// counter also tracks the control batches that accumulate in a compacted
    /// topic.
    #[metric(help = "Per-topic cumulative count of barrier markers this broker \
        appended, across every group and every partition it leads.")]
    pub barrier_markers_written_total: Family<TopicLabel, Counter>,
    /// Number of barrier groups this broker coordinates (gauge). It is zero on
    /// a broker that leads no `__barrier_state` partition.
    #[metric(
        help = "Number of barrier groups this broker coordinates (gauge). Zero \
        on a broker that leads no __barrier_state partition."
    )]
    pub barrier_groups_coordinated: Gauge,
    /// KFC-1 deliver-at-time watermark of each scheduled partition this broker
    /// leads (gauge): the first offset that is not visible to a consumer yet.
    /// Read against `partition_disk_bytes` or the log end offset to see how far
    /// visibility trails durability. Cardinality is bounded by the number of
    /// partitions this broker leads whose topic sets
    /// `delivery.mode=scheduled`; an ordinary partition never creates a series,
    /// because the scheduler drops it before it reports.
    #[metric(
        help = "KFC-1 deliver-at-time watermark of each scheduled partition this \
        broker leads (gauge): the first offset that is not visible to a \
        consumer yet. A partition whose topic delivers immediately \
        reports no series."
    )]
    pub delivery_watermark: Family<PartitionLabel, Gauge>,
    /// KFC-1 records of each scheduled partition that are durable but not
    /// visible yet (gauge): the log end offset minus
    /// `delivery_watermark`. A value that grows without falling is a schedule
    /// whose head-of-line record is far in the future. Cardinality is bounded
    /// exactly as `delivery_watermark` is.
    #[metric(
        help = "KFC-1 records of each scheduled partition that are durable but \
        not visible yet (gauge): the log end offset minus the delivery \
        watermark."
    )]
    pub delivery_pending_records: Family<PartitionLabel, Gauge>,
    /// KFC-1 seconds from a batch's activation deadline to the moment the
    /// broker first made it visible.
    ///
    /// The deadline is the record timestamp plus the topic's declared
    /// `delivery_clock_uncertainty`, so this histogram measures the delay
    /// *beyond* the bound the operator declared, and a healthy broker reports
    /// values at zero. Add `delivery_clock_uncertainty` to read the delay from
    /// the record's own delivery time. A rising tail says the declared bound is
    /// not honest, or that the scheduler is starved of CPU. It carries no
    /// labels, so it is one series per broker.
    #[metric(help = "KFC-1 seconds from a batch's activation deadline to the moment \
        the broker first made it visible. The deadline is the record \
        timestamp plus the topic's declared clock bound, so this measures \
        the delay beyond that bound and a healthy broker sits at zero. A \
        rising tail says the bound is not honest, or that the scheduler \
        is starved of CPU.", buckets = registration::DELIVERY_ACTIVATION_LATENESS_BUCKETS)]
    pub delivery_activation_lateness_seconds: Histogram,
    /// KFC-1 cumulative count of delivery-scheduler wakeups, whether a deadline
    /// came due, a produce re-armed the task, or its idle bound elapsed. Paired
    /// with the lateness histogram it separates "the scheduler never ran" from
    /// "the scheduler ran late". One series per broker.
    #[metric(
        help = "KFC-1 cumulative count of delivery-scheduler wakeups, whether a \
        deadline came due, a produce re-armed the task, or its idle \
        bound elapsed."
    )]
    pub delivery_scheduler_wakeups_total: Counter,
    /// KFC-7 cumulative count of records the broker rejected because they
    /// failed schema validation, per topic and reason.
    ///
    /// The broker bumps it once per rejected record, so a Produce request with
    /// three bad records adds 3. An operator reads the split by reason during
    /// a rollout: a run of `unframed` is a producer that never used a
    /// serializer, and a run of `wrong_subject` is a producer that writes the
    /// right format to the wrong topic.
    #[metric(help = "KFC-7 cumulative count of records rejected by schema \
        validation, per topic and reason. The reason is one of \
        unframed, unknown_id, wrong_subject, body_mismatch, and \
        registry_unavailable. The broker bumps it once per rejected \
        record. Read the split by reason during a rollout to see which \
        producer is at fault.")]
    pub schema_validation_rejections: Family<SchemaRejectionLabel, Counter>,
    /// KFC-7 cumulative count of schema lookups the broker answered from its
    /// local cache. It carries no labels, so it is one series per broker.
    #[metric(help = "KFC-7 cumulative count of schema lookups the broker answered \
        from its local cache, with no call to the registry.")]
    pub schema_validation_cache_hits: Counter,
    /// KFC-7 cumulative count of schema lookups that cost a registry round
    /// trip on the produce path.
    ///
    /// Paired with `schema_validation_cache_hits` it gives the hit rate, and
    /// the hit rate is what says whether this feature costs anything at steady
    /// state. It carries no labels, so it is one series per broker.
    #[metric(
        help = "KFC-7 cumulative count of schema lookups that cost a registry \
        round trip on the produce path. The ratio against \
        schema_validation_cache_hits is what says whether this feature \
        costs anything at steady state."
    )]
    pub schema_validation_cache_misses: Counter,
    /// KFC-8 the clock bound this broker declares, in seconds.
    ///
    /// It is `delivery_clock_uncertainty`, the bound KFC-1 adds to a batch's
    /// timestamp before the batch activates. The value is a constant of the
    /// running process, and it is broker-wide: no topic config overrides it.
    ///
    /// The broker exports it so an alert can compare measured clock
    /// uncertainty against the bound the broker actually relies on. Without
    /// this series a rule has to carry a copy of the threshold, and the copy
    /// goes stale the moment an operator retunes the broker.
    #[metric(
        help = "KFC-8 the clock bound this broker declares: the seconds KFC-1 \
        adds to a batch's timestamp before the batch activates. Compare \
        measured clock uncertainty against this series, so an alert \
        tracks the bound the broker relies on instead of a copy of it."
    )]
    pub delivery_clock_uncertainty_seconds: Gauge<f64, AtomicU64>,
    /// KFC-9 cumulative count of Produce partition rows the broker refused
    /// because a freeze covers the topic.
    ///
    /// The gate sits before the batch is parsed, so a refused row costs no CRC
    /// check and moves no log end offset. The broker bumps this counter once
    /// per refused row, so one request that names three partitions of a frozen
    /// topic adds 3.
    ///
    /// Cardinality is bounded by the number of topics a freeze covers, and
    /// that is at most the number of topics the cluster holds. This is the
    /// bound the other per-topic families here already accept. A client
    /// cannot invent a series, because the label comes from a topic name that
    /// resolved in the metadata image.
    #[metric(help = "KFC-9 cumulative count of Produce partition rows the broker \
        refused because a freeze covers the topic, per topic. The gate \
        runs before the batch is parsed, so a refused row moves no log \
        end offset.")]
    pub topic_freeze_rejections: Family<TopicLabel, Counter>,
    /// KFC-9 live entries in the freeze registry (gauge).
    ///
    /// It counts registry entries and not frozen topics: one prefix entry
    /// covers a whole namespace. The value falls when a thaw removes an entry,
    /// and `freeze.max_entries` caps it. It carries no labels, so it is one
    /// series per broker.
    #[metric(
        help = "KFC-9 live entries in the freeze registry (gauge). One prefix \
        entry covers a whole namespace, so this counts entries and not \
        frozen topics. The freeze max_entries setting caps it."
    )]
    pub topic_freezes_active: Gauge,
    /// KFC-9 break-glass proposals by state (gauge).
    ///
    /// A proposal moves through the states of [`BreakGlassState`], so a rise
    /// in `pending` beside a flat `approved` is an incident where the second
    /// person has not answered yet.
    #[metric(
        help = "KFC-9 break-glass proposals by state (gauge), where state is one \
        of pending, approved, expired, and consumed. A rise in pending \
        beside a flat approved is an incident where the second person \
        has not answered yet."
    )]
    pub break_glass_proposals: Family<BreakGlassStateLabel, Gauge>,
    /// KFC-9 cumulative count of privileged transitions the broker refused
    /// because no approved break-glass proposal covers them, per action.
    ///
    /// A refusal is the expected answer when an operator runs the tool before
    /// the approval lands, so a steady rate here is normal.
    #[metric(help = "KFC-9 cumulative count of privileged transitions the broker \
        refused because no approved break-glass proposal covers them, \
        per action. A refusal is the expected answer when an operator \
        runs the tool before the approval lands.")]
    pub break_glass_refusals: Family<BreakGlassActionLabel, Counter>,
    /// KFC-9 cumulative count of privileged transitions that ran **without**
    /// an approved break-glass proposal, per action.
    ///
    /// This is the series to alert on. It counts data-losing transitions that
    /// no second person approved: the background unclean-recovery path has no
    /// caller to refuse, so `break_glass.background_unclean_recovery =
    /// "audit-only"` lets recovery run and bumps this counter instead. Any
    /// non-zero rate is an unclean recovery that took the cluster past the
    /// two-person rule, and an operator should read the audit log for the
    /// partition it names.
    #[metric(help = "KFC-9 cumulative count of privileged transitions that ran \
        WITHOUT an approved break-glass proposal, per action. This is \
        the series to alert on: it counts data-losing unclean \
        recoveries that no second person approved, which the background \
        policy audit-only permits because that path has no caller to \
        refuse. Any non-zero rate needs an operator to read the audit \
        log for the partition it names.")]
    pub break_glass_bypassed: Family<BreakGlassActionLabel, Counter>,
    /// Quorum-durable offset for each diskless WAL shard led by this broker.
    #[metric(help = "Quorum-durable offset for each diskless WAL shard led by this broker.")]
    pub diskless_wal_durable_watermark: Family<WalShardLabel, Gauge>,
    /// Durable offsets not yet represented by the committed object index.
    #[metric(
        help = "Durable offsets not yet represented by the committed diskless WAL object index."
    )]
    pub diskless_wal_index_projection_lag: Family<WalShardLabel, Gauge>,
    /// Local WAL log-start offset after trimming.
    #[metric(help = "Local log-start offset after trimming a diskless WAL shard.")]
    pub diskless_wal_trim_frontier: Family<WalShardLabel, Gauge>,
    /// Leader log-end minus each WAL voter's durable offset.
    #[metric(help = "Leader log-end offset minus each diskless WAL voter's durable offset.")]
    pub diskless_wal_voter_lag: Family<WalVoterLabel, Gauge>,
    /// Leader-side attempts that could not form a WAL quorum.
    #[metric(help = "Leader-side diskless WAL acknowledgements that failed to form a quorum.")]
    pub diskless_wal_quorum_loss_events_total: Counter,
    /// Non-empty WAL objects submitted to object storage.
    #[metric(help = "Non-empty diskless WAL objects submitted to object storage.")]
    pub diskless_wal_flush_attempts_total: Counter,
    /// Bytes successfully written as WAL objects.
    #[metric(help = "Bytes successfully written as diskless WAL objects.")]
    pub diskless_wal_flush_bytes_total: Counter,
    /// WAL object flushes that failed after an attempt began.
    #[metric(help = "Diskless WAL object flushes that failed after an attempt began.")]
    pub diskless_wal_flush_failures_total: Counter,
    /// Diskless index records rejected during replay because their payload or format is invalid.
    #[metric(
        help = "Diskless WAL index records rejected because their payload or format is invalid."
    )]
    pub diskless_wal_index_decode_failures_total: Counter,
    /// Committed WAL index ranges tombstoned because `retention.ms`,
    /// `retention.bytes`, or a `DeleteRecords` floor expired them.
    #[metric(
        help = "Committed diskless WAL index ranges tombstoned by retention or DeleteRecords."
    )]
    pub diskless_wal_expired_ranges_total: Counter,
    #[metric(help = "Diskless WAL cold reads served from object storage.")]
    pub diskless_wal_cold_read_hits_total: Counter,
    #[metric(help = "Diskless WAL cold reads with no matching committed index entry.")]
    pub diskless_wal_cold_read_misses_total: Counter,
    #[metric(help = "Diskless WAL cold reads that failed while reading object storage.")]
    pub diskless_wal_cold_read_errors_total: Counter,
    /// KFC-5: cumulative WORM manifests the archive sealed and handed back a
    /// valid chain receipt for, one per segment copied into a write-once
    /// archive. Flat while a WORM cluster is tiering means the copy path has
    /// stopped, and the archive stops growing an attestation.
    ///
    /// Unlabelled on purpose: a topic label here would reopen the unbounded
    /// per-topic series problem, and a seal failure is a cluster-level
    /// condition an operator chases in the logs, which name the partition.
    #[metric(help = "WORM manifests sealed by the archive with a valid chain receipt.")]
    pub worm_manifests_sealed_total: Counter,
    /// KFC-5: cumulative segment copies into a write-once archive that ended
    /// without a usable manifest — the copy itself failed, the blocking task
    /// panicked, or the backend returned no receipt or a receipt that did not
    /// match the requested chain position. Each one leaves the segment in
    /// `CopySegmentStarted` for the next tick to retry, so a sustained rate
    /// means the archive is not advancing.
    #[metric(help = "Write-once segment copies that ended without a usable manifest.")]
    pub worm_manifest_seal_failures_total: Counter,

    // --- KRaft quorum, cluster state and request queue metrics (#390, #412) ---
    #[metric(
        help = "Current state of the KRaft consensus state machine (one-hot across leader, follower, candidate, observer)"
    )]
    pub raft_current_state: Family<RaftStateLabel, Gauge>,
    #[metric(help = "Current KRaft leader epoch")]
    pub raft_current_epoch: Gauge,
    #[metric(help = "High watermark offset of the local metadata log")]
    pub raft_high_watermark: Gauge,
    #[metric(help = "Log end offset of the local metadata log")]
    pub raft_log_end_offset: Gauge,
    #[metric(help = "Number of active KRaft voters")]
    pub raft_voters: Gauge,
    #[metric(help = "Number of active KRaft observers")]
    pub raft_observers: Gauge,
    #[metric(help = "Highest metadata record offset applied to the active metadata image")]
    pub metadata_last_applied_offset: Gauge,
    #[metric(
        help = "Append timestamp in Unix epoch milliseconds of the last applied metadata record"
    )]
    pub metadata_last_applied_timestamp_ms: Gauge,
    #[metric(help = "Milliseconds since the last applied metadata record was appended")]
    pub metadata_last_applied_lag_ms: Gauge,
    #[metric(
        help = "Lifetime average microseconds to apply a completed set of pending committed metadata records"
    )]
    pub pending_record_processing_time_us_avg: Gauge,
    #[metric(
        help = "Lifetime maximum microseconds to apply a completed set of pending committed metadata records"
    )]
    pub pending_record_processing_time_us_max: Gauge,
    #[metric(
        help = "Lag in records between the quorum committed high watermark and this node's applied metadata offset"
    )]
    pub metadata_lag_records: Gauge,
    #[metric(
        help = "Kafka BrokerState lifecycle code (1=STARTING, 2=RECOVERY, 3=RUNNING, 6=PENDING_CONTROLLED_SHUTDOWN, 7=SHUTTING_DOWN)"
    )]
    pub broker_state: Gauge,
    #[metric(help = "Number of unfenced brokers in the cluster (reported by active controller)")]
    pub active_brokers: Gauge,
    #[metric(help = "Number of fenced brokers in the cluster (reported by active controller)")]
    pub fenced_brokers: Gauge,
    #[metric(
        help = "Total number of topics in the cluster metadata image (reported by active controller)"
    )]
    pub global_topics: Gauge,
    #[metric(
        help = "Total number of topic partitions in the cluster metadata image (reported by active controller)"
    )]
    pub global_partitions: Gauge,
    #[metric(help = "Number of partitions whose in-sync replica count equals min.insync.replicas")]
    pub at_min_isr_partition_count: Gauge,
    #[metric(help = "Number of partitions currently undergoing replica reassignment")]
    pub reassigning_partitions: Gauge,
    #[metric(help = "Number of partitions whose current leader is not the preferred replica")]
    pub preferred_replica_imbalance: Gauge,
    #[metric(help = "Number of client requests currently queued awaiting execution")]
    pub queued_requests: Gauge,
    #[metric(help = "Total bytes of client requests currently queued awaiting execution")]
    pub queued_request_bytes: Gauge,

    // --- Tiered Storage, replication throttling and quota entity metrics (#420, #418) ---
    #[metric(help = "Cumulative bytes successfully copied to remote storage per topic")]
    pub remote_copy_bytes_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative bytes served from remote storage per topic")]
    pub remote_fetch_bytes_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative copy attempts to remote storage per topic")]
    pub remote_copy_requests_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative fetch attempts from remote storage per topic")]
    pub remote_fetch_requests_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative delete requests submitted to remote storage per topic")]
    pub remote_delete_requests_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative failed remote copy attempts per topic")]
    pub remote_copy_errors_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative failed remote fetch attempts per topic")]
    pub remote_fetch_errors_total: Family<TopicLabel, Counter>,
    #[metric(help = "Cumulative failed remote delete attempts per topic")]
    pub remote_delete_errors_total: Family<TopicLabel, Counter>,
    #[metric(help = "Bytes in eligible log segments pending remote copy per topic")]
    pub remote_copy_lag_bytes: Family<TopicLabel, Gauge>,
    #[metric(help = "Number of eligible log segments pending remote copy per topic")]
    pub remote_copy_lag_segments: Family<TopicLabel, Gauge>,
    #[metric(help = "Bytes in expired remote segments pending remote deletion per topic")]
    pub remote_delete_lag_bytes: Family<TopicLabel, Gauge>,
    #[metric(help = "Number of expired remote segments pending remote deletion per topic")]
    pub remote_delete_lag_segments: Family<TopicLabel, Gauge>,
    #[metric(help = "Outbound replication bytes throttled by leader replication quota")]
    pub replication_throttled_bytes_out_total: Counter,
    #[metric(help = "Inbound replication bytes throttled by follower replication quota")]
    pub replication_throttled_bytes_in_total: Counter,
    #[metric(help = "Replication fetch requests delayed or rejected by replication quota")]
    pub replication_throttle_sleeps_total: Counter,
    /// KIP-599 / KIP-13: cumulative throttle time charged to each quota
    /// entity, in seconds.
    ///
    /// A float counter, because a throttle is routinely a fraction of a
    /// second and an integer one would have to round every delay to a whole
    /// second before adding it -- which turns a hundred 20 ms throttles into
    /// either zero seconds or a hundred.
    #[metric(help = "Cumulative throttle duration in seconds applied per quota entity")]
    pub quota_entity_throttle_seconds_total: Family<QuotaEntityLabel, Counter<f64, AtomicU64>>,

    // --- KIP-405 remote reader pool and index cache (#422) ---
    /// Cold-tier reads waiting for a reader slot. Kafka's
    /// `RemoteLogReaderTaskQueueSize`.
    #[metric(help = "Cold-tier reads waiting for a slot in the bounded remote reader pool")]
    pub remote_log_reader_task_queue_size: Gauge,
    /// The share of the reader pool's slots that are free, as a percentage.
    /// Kafka's `RemoteLogReaderAvgIdlePercent`, reported instantaneously
    /// because Prometheus does its own averaging.
    #[metric(help = "Percentage of the remote reader pool's slots that are currently free")]
    pub remote_log_reader_avg_idle_percent: Gauge<f64, AtomicU64>,
    /// How long each cold-tier read took. The seconds-valued histogram that
    /// stands for Kafka's `RemoteLogReaderFetchRateAndTimeMs` meter: the rate
    /// is `rate(..._count[5m])` and the mean is `..._sum / ..._count`.
    #[metric(help = "Time each cold-tier read spent holding a remote reader slot", buckets = registration::REQUEST_DURATION_BUCKETS)]
    pub remote_log_reader_fetch_duration_seconds: Histogram,
    /// Cold-tier reads refused because the pool's pending queue was full.
    #[metric(
        help = "Cold-tier reads refused because the remote reader pool's pending queue was full"
    )]
    pub remote_log_reader_rejected_total: Counter,
    /// Segment-index lookups served from the on-disk cache.
    #[metric(help = "Remote segment index lookups served from the on-disk index cache")]
    pub remote_index_cache_hits_total: Counter,
    /// Segment-index lookups that had to download the index object.
    #[metric(help = "Remote segment index lookups that downloaded the index object")]
    pub remote_index_cache_misses_total: Counter,
    /// Cache entries dropped to stay inside the byte budget.
    #[metric(help = "Remote index cache entries dropped to stay inside the byte budget")]
    pub remote_index_cache_evictions_total: Counter,
    /// Bytes the index cache currently holds.
    #[metric(help = "Bytes currently held by the remote index cache")]
    pub remote_index_cache_bytes: Gauge,
    /// Entries the index cache currently holds.
    #[metric(help = "Entries currently held by the remote index cache")]
    pub remote_index_cache_entries: Gauge,

    /// The label sets [`Self::replica_lag`] and [`Self::consumer_group_lag`]
    /// currently carry, so that a caller holding only part of a lag label set
    /// can still release the series. See [`LagSeriesIndex`].
    ///
    /// Public only so that the metrics contract suite can destructure the
    /// bundle without `..` and fail to compile when a family is added. The
    /// type is opaque outside the crate.
    #[metric(skip)]
    pub lag_series: LagSeriesIndex,
    /// Label sets materialised by the topic and partition metric families.
    /// See [`MetricSeriesIndex`].
    #[metric(skip)]
    pub metric_series: MetricSeriesIndex,
}

pub(crate) use log_cleaner::failure_reason as cleaner_failure_reason;
