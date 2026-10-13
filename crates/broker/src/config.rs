//! Broker configuration.
//!
//! Build the configuration directly for library use, or from CLI flags. The
//! binary entry point is `bin/broker.rs`.

pub use krabka_raft::BootstrapMode;
use krabka_units::{ByteSize, Time, days, gibibytes, hours, mebibytes, millis, minutes, secs};

mod break_glass;
mod broker_config;
mod defaults;
mod feature_flags;
mod freeze;
mod kafka_static_keys;
mod leader_rebalance;
mod listener;
mod log_storage;
mod offset_retention;
mod record_decompression;
mod replication;
mod roles;
mod scalar_checks;
mod static_origins;
mod stretch;
mod test_defaults;
#[cfg(test)]
mod test_support;
mod tiered_storage;
mod validate;

pub use self::{
    break_glass::{BackgroundUncleanRecovery, BreakGlassConfig},
    broker_config::{BrokerConfig, PlainCredentials},
    feature_flags::BrokerFeatureFlags,
    freeze::FreezeConfig,
    listener::{InterBrokerCredentials, ListenerSpec},
    replication::ReplicationRuntimeConfig,
    roles::NodeRole,
    static_origins::{
        AuthenticationOrigins, LogOrigins, StaticConfigOrigins, TopicAdminOrigins,
        TopicCreationOrigins,
    },
    stretch::StretchProfile,
    tiered_storage::{KafkaRlmmConfig, RemoteStorageBackend, RlmmKind},
};
pub(crate) use self::{
    kafka_static_keys::KAFKA_STATIC_KEYS, listener::validate_oauthbearer_token_file,
};

/// The node id of a broker that neither `--broker-id` nor the file's
/// `broker_id` names.
pub const DEFAULT_BROKER_ID: i32 = 1;

/// Default number of local durable copies in a diskless WAL quorum.
pub const DEFAULT_DISKLESS_WAL_LOCAL_REPLICA_COUNT: usize = 3;
/// Default cadence of diskless WAL object-store flushes.
pub const DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL: Time = millis(250);
/// Default byte ceiling for one diskless WAL object-store flush.
pub const DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE: ByteSize = mebibytes(8);
/// Default broker-wide byte ceiling for quorum-committed hot-tail batches.
pub const DEFAULT_DISKLESS_WAL_HOT_TAIL_MAX_SIZE: ByteSize = mebibytes(64);
/// Default committed-offset lag retained behind the diskless WAL trim frontier.
pub const DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG: i64 = 1;
/// Default wait for a published diskless WAL index record to be projected.
pub const DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT: Time = secs(5);

/// Default broker→controller `BrokerHeartbeat` cadence.
pub const DEFAULT_HEARTBEAT_INTERVAL: Time = secs(3);

/// Default controller-side broker-session timeout (3× the heartbeat
/// interval, so a broker survives two missed heartbeats).
pub const DEFAULT_HEARTBEAT_TIMEOUT: Time = secs(9);

/// Default maximum follower lag before the leader proposes ISR shrink.
/// Matches Kafka's `replica.lag.time.max.ms` default.
pub const DEFAULT_REPLICA_LAG_TIME_MAX: Time = secs(30);

/// Default byte gap between metadata-log snapshots: 20 MiB, matching Kafka's
/// `metadata.log.max.record.bytes.between.snapshots`.
pub const DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS: ByteSize = mebibytes(20);

/// Default time cap between metadata-log snapshots: 1 hour, matching Kafka's
/// `metadata.log.max.snapshot.interval.ms`.
pub const DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL: Time = hours(1);

/// KIP-630: default committed-record gap between metadata-log snapshots.
pub const DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS: u64 = 10_000;

/// Default follower metadata snapshot fetch limit: the 1 GiB core ceiling.
pub const DEFAULT_METADATA_SNAPSHOT_FETCH_MAX: ByteSize = gibibytes(1);

/// Default bound on how far this node's `__cluster_metadata` offset may trail
/// the quorum's committed offset while `/readyz` still answers 200.
///
/// It is the same 100 records as [`DEFAULT_OBSERVER_LAG_BOUND`], and for the
/// same reason: a node within a hundred records of the quorum is one fetch
/// round trip from current, while one further behind than that is answering
/// `Metadata` from an image whose partition leaders may already have moved.
pub const DEFAULT_READINESS_MAX_METADATA_LAG: u64 = 100;

/// KIP-853: default maximum log-entry lag at which an observer is still
/// promotable to a quorum voter.
pub const DEFAULT_OBSERVER_LAG_BOUND: u64 = 100;

/// Default controller election timeout.
pub const DEFAULT_CONTROLLER_ELECTION_TIMEOUT: Time = secs(5);

/// Default controller heartbeat interval.
pub const DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL: Time = millis(500);

/// Default controlled-shutdown leadership drain timeout.
pub const DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT: Time = secs(20);

/// Default idle-transaction abort cleanup interval.
pub const DEFAULT_TXN_ABORT_CLEANUP_INTERVAL: Time = secs(10);

/// Default transactional-id expiry, matching Kafka's
/// `transactional.id.expiration.ms` default of 604800000 ms.
pub const DEFAULT_TXN_ID_EXPIRATION: Time = days(7);

/// Default cadence of the transactional-id expiry sweep, matching Kafka's
/// `transaction.remove.expired.transaction.cleanup.interval.ms` default of
/// 3600000 ms.
pub const DEFAULT_TXN_ID_EXPIRATION_CLEANUP_INTERVAL: Time = hours(1);

/// Default TLS material reload polling interval.
pub const DEFAULT_TLS_RELOAD_INTERVAL: Time = secs(30);

/// Default connection idle window. Matches Apache Kafka's
/// `connections.max.idle.ms`, whose default is 600000.
pub const DEFAULT_CONNECTIONS_MAX_IDLE: Time = minutes(10);

/// KIP-13: default width of the sliding window a client byte-rate quota is
/// measured over, and therefore the burst a bucket allows before it throttles.
/// Kafka's `quota.window.num` (11) times `quota.window.size.seconds` (1).
pub const DEFAULT_QUOTA_WINDOW: Time = secs(11);

/// Default `RemoteLogManager` copy / retention cadence.
pub const DEFAULT_REMOTE_LOG_MANAGER_INTERVAL: Time = secs(30);

/// KIP-405: default deadline on one segment copy to the remote tier.
///
/// It bounds the whole `copy_log_segment_data` call, retries inside the
/// object-store client included, so a store that stalls costs one partition
/// one tick rather than holding one of the sweep's copier slots for as long
/// as the store cares to hold the connection. Kafka has no equivalent: its
/// copy is unbounded.
pub const DEFAULT_REMOTE_COPY_TIMEOUT: Time = minutes(10);

/// KIP-405: default number of partitions whose segment copies may run at
/// once. Matches Kafka's `remote.log.manager.copier.thread.pool.size`.
pub const DEFAULT_REMOTE_COPIER_THREADS: usize = 10;

/// KIP-405: default number of partitions whose retention passes may run at
/// once. Matches Kafka's `remote.log.manager.expiration.thread.pool.size`.
pub const DEFAULT_REMOTE_EXPIRATION_THREADS: usize = 10;

/// KIP-405: default number of concurrent cold-tier reads. Matches Kafka's
/// `remote.log.reader.threads`.
pub const DEFAULT_REMOTE_READER_THREADS: usize = 10;

/// KIP-405: default number of cold-tier reads that may wait for a reader slot
/// before one is refused. Matches Kafka's
/// `remote.log.reader.max.pending.tasks`.
pub const DEFAULT_REMOTE_READER_MAX_PENDING_TASKS: usize = 100;

/// KIP-405: default byte budget of the on-disk remote index cache. Matches
/// Kafka's `remote.log.index.file.cache.total.size.bytes`.
pub const DEFAULT_REMOTE_INDEX_CACHE_SIZE: ByteSize = gibibytes(1);

/// KIP-460: default auto-rebalance tick cadence. Matches Kafka's
/// `leader.imbalance.check.interval.seconds`.
pub const DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL: Time = minutes(5);

/// KIP-227: default incremental-fetch session cache capacity. Matches Kafka's
/// `max.incremental.fetch.session.cache.slots`.
pub const DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS: usize = 1000;

/// Default cadence of the background JWKS re-fetch for the signed
/// OAUTHBEARER validator: 5 minutes.
pub const DEFAULT_JWKS_REFRESH_INTERVAL: Time = minutes(5);

/// Default minimum pause between on-demand JWKS refreshes triggered by
/// validator signals: 1 second (Strimzi parity).
pub const DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE: Time = secs(1);

/// KIP-405: default partition count for `__remote_log_metadata` on first
/// creation. Matches Kafka's `remote.log.metadata.topic.num.partitions`.
pub const DEFAULT_RLMM_TOPIC_NUM_PARTITIONS: i32 = 50;

/// KIP-405: default replication factor for `__remote_log_metadata` on first
/// creation. Matches Kafka's `remote.log.metadata.topic.replication.factor`.
pub const DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR: i32 = 3;

/// KIP-405: default `min.insync.replicas` for `__remote_log_metadata` on first
/// creation. Matches Kafka's `remote.log.metadata.topic.min.isr`.
pub const DEFAULT_RLMM_TOPIC_MIN_ISR: i32 = 2;

/// Default internal topic name for `FedRAMP` MLA audit records.
pub const DEFAULT_AUDIT_TOPIC: &str = "__krabka_audit";

/// The `EnvFilter` spec the stdout log layer starts from when `RUST_LOG` is
/// unset, and the spec [`BrokerConfig::log_levels`] is seeded with.
///
/// The crate-root entries (`krabka_broker`, `krabka_log`) are redundant as
/// *filters* — both sit at the root level — but each one names a logger, and
/// naming is what makes it reachable. Kafka's `BROKER_LOGGER` resource refuses
/// a level for a logger the node does not have, and a `tracing` callsite
/// registers under its module path (`krabka_broker::handlers::produce`), never
/// under the bare crate root. Drop these and `kafka-configs --entity-type
/// broker-loggers --alter --add-config krabka_broker=DEBUG` answers `Logger
/// krabka_broker does not exist!`. A JVM broker gets the same names from the
/// loggers its `log4j2.yaml` declares.
///
/// The binary, the [`Default`] config and the wire test all read it here, so a
/// change to the shipped default cannot silently desync from the spec the
/// tests prove the feature against.
pub const DEFAULT_LOG_FILTER: &str = "krabka_broker=info,krabka_log=info,info";

/// Default number of audit records between signed checkpoints.
pub const DEFAULT_AUDIT_CHECKPOINT_EVERY_N: u64 = 1000;

/// Default maximum interval between signed audit checkpoints.
pub const DEFAULT_AUDIT_CHECKPOINT_EVERY: Time = secs(60);

/// Default durable audit-spool directory (relative paths resolve under the
/// broker's log dir).
pub const DEFAULT_AUDIT_SPOOL_DIR: &str = "audit-spool";

/// Default cap on the durable audit spool: 1 GiB.
pub const DEFAULT_AUDIT_SPOOL_MAX: ByteSize = gibibytes(1);

/// Default audit-spool sync cadence: every appended record.
pub const DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N: std::num::NonZeroU64 = std::num::NonZeroU64::MIN;

/// KIP-48: default hard upper bound on delegation-token lifetime.
/// 7 days, matches Kafka's `delegation.token.max.lifetime.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME: Time = days(7);

/// KIP-48: default cadence of the background expiry sweep task.
/// 1 hour, matches Kafka's `delegation.token.expiry.check.interval.ms`.
pub const DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL: Time = hours(1);

/// KIP-48: default renew period used as the initial
/// `expiry_timestamp_ms` offset at create time, and as the implicit
/// renew period when `RenewDelegationToken.renew_period_ms == -1`.
/// 24 hours, matches Kafka's `delegation.token.expiry.time.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD: Time = hours(24);

/// KIP-211: default retention for a committed consumer-group offset after the
/// group that owns it becomes empty. 7 days, matching Kafka's
/// `offsets.retention.minutes` default of 10080.
pub const DEFAULT_OFFSETS_RETENTION: Time = minutes(10_080);

/// Default cadence of the background offset-retention sweep. 10 minutes,
/// matching Kafka's `offsets.retention.check.interval.ms` default of 600000.
pub const DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL: Time = minutes(10);

/// Default ceiling on live entries in the topic write-freeze registry.
///
/// A prefix-scoped lookup walks the registry in reverse from the topic name,
/// which is unbounded in the worst case, and that lookup is one hop from the
/// produce path. The ceiling bounds the walk.
pub const DEFAULT_FREEZE_MAX_ENTRIES: usize = 1_000;

/// Default tolerance between a signed freeze record's timestamp and the
/// controller's clock.
pub const DEFAULT_FREEZE_SIGNATURE_MAX_SKEW: Time = minutes(5);

/// Lowest `required_approvals` a break-glass proposal accepts.
///
/// A two-person rule with one approval is one person.
pub const MIN_BREAK_GLASS_REQUIRED_APPROVALS: usize = 2;

/// Default number of distinct approving principals a break-glass proposal
/// needs before it authorizes anything.
pub const DEFAULT_BREAK_GLASS_REQUIRED_APPROVALS: usize = MIN_BREAK_GLASS_REQUIRED_APPROVALS;

/// Default lifetime of a break-glass proposal.
///
/// The TTL is also the safety bound on removing an approver: wait it out and
/// every pending approval by that principal is dead.
pub const DEFAULT_BREAK_GLASS_PROPOSAL_TTL: Time = minutes(30);

/// Actions that demand a detached operator signature when `[break_glass]` is
/// configured and `signed_actions` is omitted: the irreversible set.
///
/// [`BrokerConfig::default`] leaves [`BreakGlassConfig::signed_actions`] empty
/// instead, because a broker with no `[break_glass]` section runs no
/// break-glass workflow at all and has no operator key to verify against.
pub const DEFAULT_BREAK_GLASS_SIGNED_ACTIONS: &[&str] =
    &["unclean_elect_leaders", "unclean_recovery", "delete_topic"];

/// Default cadence of the topic-backed RLMM snapshot flush. 60s,
/// matching Kafka's `remote.log.metadata.snapshot.interval` default.
pub const DEFAULT_RLMM_SNAPSHOT_INTERVAL: Time = minutes(1);

/// A shared, zero-valued epoch-millisecond counter.
///
/// The OAUTHBEARER JWKS refresher stamps two of these, and the validator reads
/// them through the same `Arc`.
fn shared_epoch_ms() -> std::sync::Arc<std::sync::atomic::AtomicI64> {
    std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0))
}
