//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! The broker recognizes every key Kafka's `TopicConfig` carries, plus the
//! krabka-private ones. Six of Kafka's propagate live to `Log.config`:
//! `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
//! `compression.type`, and `delivery.mode`. The tiered-storage local-retention pair
//! (`local.retention.ms`, `local.retention.bytes`), the KIP-534
//! delete-horizon grace window (`delete.retention.ms`), and the batch-size cap
//! (`max.message.bytes`) propagate live too, as do the roll interval
//! (`segment.ms`), the sparse-index spacing (`index.interval.bytes`), the
//! sparse-index size cap (`segment.index.bytes`), the cleaner's three
//! selection keys (`min.compaction.lag.ms`,
//! `max.compaction.lag.ms`, `min.cleanable.dirty.ratio`),
//! `message.timestamp.type` and `preallocate`.
//!
//! `cleanup.policy` is a list, as it is on Kafka: `delete`, `compact`, or
//! `compact,delete`, which both compacts the log and applies retention to it.
//! Kafka Streams writes the pair on every windowed-store changelog topic.
//!
//! Compression levels, segment roll jitter, deferred file deletion and flush
//! thresholds also propagate live to `Log.config`. Compression levels apply
//! to broker-side re-encoding; producer passthrough keeps the producer's bytes.
//!
//! One key bounds a single write: `max.message.bytes`. The produce path reads
//! it per topic, falls back to the broker's `message.max.bytes` when the topic
//! sets none, and refuses a larger record batch with `MESSAGE_TOO_LARGE` (10)
//! before it verifies a CRC or decompresses a body.
//!
//! The produce hot path's pre-flight gate reads one key,
//! `min.insync.replicas`, which takes integers >= 1. An `acks=-1` produce
//! against a partition whose ISR is already smaller fails fast with
//! `NOT_ENOUGH_REPLICAS` (19).
//!
//! Two keys are KIP-73 throttle keys:
//! `leader.replication.throttled.replicas` and
//! `follower.replication.throttled.replicas`. `ThrottledReplicas::parse`
//! validates both. One key is the KIP-841 unclean-recovery toggle,
//! `unclean.leader.election.enable`. The controller's automatic failover
//! path reads it on ISR-empty. One key is the KIP-966 offset-aware recovery
//! strategy, `unclean.recovery.strategy`, which supersedes that toggle. Both
//! unclean-recovery settings also accept a cluster-wide default broker config;
//! a topic override takes precedence.
//!
//! One key is krabka's data-path opt-in, `krabka.diskless`. It takes
//! `true`/`false` and it is the only key a partition reads once, when it is
//! opened, so [`validate_diskless_unchanged`] pins it for the life of the
//! topic and `DescribeConfigs` reports it read-only.
//!
//! Three keys carry KFC-1 scheduled delivery: `delivery.mode`,
//! `delivery.max.delay.ms`, and `delivery.schedule.monotonic`. Only the first
//! propagates live to `Log.config`. The produce path reads the other two,
//! which bound how far ahead of produce time a batch may be scheduled and
//! whether a partition's schedule may run backwards.
//!
//! Three keys carry KFC-7 schema validation: `schema.validation.key`,
//! `schema.validation.value`, and `schema.validation.mode`. None of them
//! reaches `Log.config`. The produce path reads all three. The two booleans
//! turn the check on for record keys and for record values. The mode selects
//! how much of each record the check reads.
//!
//! Four pairs of keys exclude each other: a `cleanup.policy` containing
//! `compact` with each of `delivery.mode=scheduled` and
//! `remote.storage.enable=true` (Kafka's
//! `validateNoRemoteStorageForCompactedTopic`), and `krabka.diskless=true`
//! with each of `remote.storage.enable=true` and `delivery.mode=scheduled`.
//! [`validate_topic_config`] sees one pair at a time and cannot see any of
//! them, so [`validate_config_combination`] checks all four rules over a whole
//! override map.
//!
//! KFC-9's [`WRITE_FREEZE`] sits outside the whitelist because only the
//! controller writes it. It is synthesised for `DescribeConfigs` and never
//! stored. KIP-966 ELR state lives in partition metadata, not config.
//! [`validate_topic_config`] does not accept the freeze key. See
//! [`topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! The broker rejects unknown keys with `INVALID_CONFIG`.
//!
//! [`registry`] is the one table all of that reads from. A row states a key's
//! name, the `ConfigDef` type byte the JVM `AdminClient` parses its value
//! with, its default, its documentation, whether the broker may disclose the
//! value, and the value check [`validate_topic_config`] applies. The generated
//! reference page in [`docs`] and the typed metadata `DescribeConfigs` reports
//! are both projections of it, so an operator cannot be told one thing by
//! `kafka-configs --describe` and another by an alter refusal.

mod broker_defaults;
pub(crate) mod broker_dynamic;
mod broker_scope;
mod delivery;
mod diskless;
mod docs;
pub(crate) mod group;
pub(crate) mod kafka_broker;
#[cfg(test)]
mod kafka_parity;
mod log_config;
mod lookup;
mod message_size;
mod min_isr;
pub(crate) mod parse;
mod recovery;
pub(crate) mod registry;
mod schema;
#[cfg(test)]
mod test_support;
mod topic_scope;

mod trunk_log;
mod validation;

pub use self::docs::{TopicConfigDoc, topic_config_docs};
pub(crate) use self::{
    broker_defaults::{BrokerLogDefaults, dynamic_log_base},
    broker_scope::{
        AUTO_CREATE_TOPICS_ENABLE, BROKER_WITNESS, CONNECTIONS_MAX_IDLE_MS,
        DEFAULT_REPLICATION_FACTOR, DELETE_TOPIC_ENABLE, INTER_BROKER_LISTENER_NAME, LOG_ROLL_MS,
        NUM_PARTITIONS, OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
        STRETCH_PREFERRED_LEADER_SITE, TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
        TRANSACTIONAL_ID_EXPIRATION_MS, WITNESS_TRUE, is_controller_managed_broker_config,
        resolve_broker_witness, resolve_preferred_leader_site, resolve_remote_list_offsets_timeout,
        witness_node_ids,
    },
    delivery::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, resolve_delivery_max_delay},
    diskless::{DISKLESS, resolve_diskless, validate_diskless_unchanged},
    log_config::apply_to_log_config,
    message_size::resolve_max_message_bytes,
    min_isr::{clear_elr_records, effective_min_insync_replicas, node_min_insync_replicas},
    recovery::{
        RecoveryStrategy, UNCLEAN_LEADER_ELECTION_ENABLE, resolve_recovery_strategy,
        resolve_unclean_leader_election_enabled,
    },
    schema::resolve_schema_validation,
    topic_scope::{
        WRITE_FREEZE, controller_managed_topic_config_message, is_controller_managed_topic_config,
    },
    trunk_log::{
        RemoteCopyLag, log_max_decompressed_record, resolve_max_decompressed_record_bytes,
        resolve_remote_copy_lag,
    },
    validation::{
        TopicDefaults, canonical_topic_config, canonical_topic_config_map, in_topic_unit,
        parse_cleanup_policy, parse_compression_type, validate_config_combination,
        validate_remote_storage_disable, validate_topic_config,
    },
};
// Reached only from #[cfg(test)] code -- the produce delivery/throttle tests and
// the alter_configs tests -- so an ungated re-export is dead in a normal build.
#[cfg(test)]
pub(crate) use self::{
    broker_scope::{CONTROLLER_MANAGED_BROKER_CONFIGS, REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS},
    delivery::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE_IMMEDIATE, DELIVERY_SCHEDULE_MONOTONIC},
    recovery::UNCLEAN_RECOVERY_STRATEGY,
    topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS,
    validation::{REMOTE_STORAGE_POLICY_MESSAGE, validate_topic_config_map},
};
use crate::api_catalog::UnstableApiVersions;
#[cfg(test)]
pub(crate) use topic_scope::ELIGIBLE_LEADER_REPLICAS;
pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
/// The largest record batch a topic accepts, measured over the batch's whole
/// wire encoding. An oversized batch earns `MESSAGE_TOO_LARGE` (10).
pub(crate) const MAX_MESSAGE_BYTES: &str = "max.message.bytes";

/// Kafka's `segment.ms`: the age at which the active segment rolls.
pub(crate) const SEGMENT_MS: &str = "segment.ms";
/// Kafka's `segment.index.bytes`: the size cap on each of a segment's two
/// sparse indexes. The active segment rolls once either one is full.
pub(crate) const SEGMENT_INDEX_BYTES: &str = "segment.index.bytes";
/// Kafka's `segment.jitter.ms`: random subtraction from the roll interval.
pub(crate) const SEGMENT_JITTER_MS: &str = "segment.jitter.ms";
/// Kafka's `min.compaction.lag.ms`: how long a record is safe from the
/// cleaner after it is written.
pub(crate) const MIN_COMPACTION_LAG_MS: &str = "min.compaction.lag.ms";
/// Kafka's `max.compaction.lag.ms`: how long a dirty record may go
/// uncompacted before the cleaner runs regardless of the dirty ratio.
pub(crate) const MAX_COMPACTION_LAG_MS: &str = "max.compaction.lag.ms";
/// Kafka's `min.cleanable.dirty.ratio`: the share of the log that must be
/// uncleaned before a compaction pass is worth running.
pub(crate) const MIN_CLEANABLE_DIRTY_RATIO: &str = "min.cleanable.dirty.ratio";
/// Kafka's `file.delete.delay.ms`: the delay before a removed segment file is
/// unlinked.
pub(crate) const FILE_DELETE_DELAY_MS: &str = "file.delete.delay.ms";
/// Kafka's `flush.messages`: records between forced fsyncs.
pub(crate) const FLUSH_MESSAGES: &str = "flush.messages";
/// Kafka's `flush.ms`: milliseconds between forced fsyncs.
pub(crate) const FLUSH_MS: &str = "flush.ms";
/// Kafka's `index.interval.bytes`: bytes of `.log` between sparse index
/// entries.
pub(crate) const INDEX_INTERVAL_BYTES: &str = "index.interval.bytes";
/// Kafka's `preallocate`: whether a new segment reserves `segment.bytes` of
/// disk when it is created, and the active segment writes through
/// `O_DIRECT`.
pub(crate) const PREALLOCATE: &str = "preallocate";
/// Kafka's `message.timestamp.type`: whose clock the stored records carry.
pub(crate) const MESSAGE_TIMESTAMP_TYPE: &str = "message.timestamp.type";
/// Kafka's `message.timestamp.after.max.ms`: how far into the future a
/// producer timestamp may sit.
pub(crate) const MESSAGE_TIMESTAMP_AFTER_MAX_MS: &str = "message.timestamp.after.max.ms";
/// Kafka's `message.timestamp.before.max.ms`: how far into the past a
/// producer timestamp may sit.
pub(crate) const MESSAGE_TIMESTAMP_BEFORE_MAX_MS: &str = "message.timestamp.before.max.ms";
/// Kafka's `compression.gzip.level`: the level a broker-side gzip re-encode
/// runs at.
pub(crate) const COMPRESSION_GZIP_LEVEL: &str = "compression.gzip.level";
/// Kafka's `compression.lz4.level`: the level a broker-side lz4 re-encode runs
/// at.
pub(crate) const COMPRESSION_LZ4_LEVEL: &str = "compression.lz4.level";
/// Kafka's `compression.zstd.level`: the level a broker-side zstd re-encode
/// runs at.
pub(crate) const COMPRESSION_ZSTD_LEVEL: &str = "compression.zstd.level";
/// Kafka's `internal.segment.bytes`: the segment-size override Kafka's own
/// coordinators set on the internal topics they create. `ConfigDef` marks it
/// internal, which hides it from `kafka-configs --help` but not from the
/// broker's own topic-config validation, so an alter that carries it is
/// accepted. It carries no floor, and `LogConfig.segmentSize()` prefers it
/// over `segment.bytes` whenever it is set.
pub(crate) const INTERNAL_SEGMENT_BYTES: &str = "internal.segment.bytes";
/// `message.timestamp.type=CreateTime`: the producer's own timestamps.
pub(crate) const MESSAGE_TIMESTAMP_TYPE_CREATE: &str = "CreateTime";
/// `message.timestamp.type=LogAppendTime`: the broker's clock at append time.
pub(crate) const MESSAGE_TIMESTAMP_TYPE_LOG_APPEND: &str = "LogAppendTime";

/// KIP-405: per-topic tiered-storage opt-in.
pub(crate) const REMOTE_STORAGE_ENABLE: &str = "remote.storage.enable";

/// KIP-950 `remote.log.copy.disable`: stop copying new segments to the remote
/// tier while still serving reads from it. Kafka's read-only tier.
pub(crate) const REMOTE_LOG_COPY_DISABLE: &str = "remote.log.copy.disable";

/// KIP-950 `remote.log.delete.on.disable`: the operator's consent to erase the
/// remote copies when tiered storage is turned off. Kafka refuses the
/// `remote.storage.enable` `true -> false` flip without it.
pub(crate) const REMOTE_LOG_DELETE_ON_DISABLE: &str = "remote.log.delete.on.disable";
/// KIP-405: per-topic local-retention time window for tiered partitions.
pub(crate) const LOCAL_RETENTION_MS: &str = "local.retention.ms";
/// KIP-405: per-topic local-retention size budget for tiered partitions.
pub(crate) const LOCAL_RETENTION_BYTES: &str = "local.retention.bytes";
/// Kafka trunk's `remote.copy.lag.ms`: how old a sealed segment may get before
/// it is copied to the remote tier. The copy pass holds a segment back for it,
/// and it is served only under `unstable.api.versions.enable`.
pub(crate) const REMOTE_COPY_LAG_MS: &str = "remote.copy.lag.ms";
/// Kafka trunk's `remote.copy.lag.bytes`: how many newer local bytes a sealed
/// segment may sit behind before it is copied. The copy pass holds a segment
/// back for it, and it is served only under `unstable.api.versions.enable`.
pub(crate) const REMOTE_COPY_LAG_BYTES: &str = "remote.copy.lag.bytes";
/// Kafka trunk's `max.decompressed.message.bytes`: the cap on one record's
/// decompressed size. Produce enforces it on a compressed batch, the log
/// enforces it in compaction and in by-timestamp lookups (through
/// `LogConfig::max_decompressed_record`), and it is served only under
/// `unstable.api.versions.enable`.
pub(crate) const MAX_DECOMPRESSED_MESSAGE_BYTES: &str = "max.decompressed.message.bytes";
/// Kafka trunk's `errors.deadletterqueue.group.enable` (KIP-1191): whether
/// share groups may write undeliverable records to this topic. A share
/// partition at `share.version` 2 writes a group's dead-letter records only
/// to a topic that has it on. It is served only under
/// `unstable.api.versions.enable`.
pub(crate) const ERRORS_DEADLETTERQUEUE_GROUP_ENABLE: &str = "errors.deadletterqueue.group.enable";
/// The topic keys Kafka trunk's `LogConfig` defines and Kafka 4.3.1's does
/// not. A broker serves them only under `unstable.api.versions.enable`;
/// otherwise its topic resource is 4.3.1's, and an alter or create that names
/// one is refused as `LogConfig.validateNames` refuses an unknown key.
pub(crate) const KAFKA_TRUNK_TOPIC_KEYS: &[&str] = &[
    ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
    MAX_DECOMPRESSED_MESSAGE_BYTES,
    REMOTE_COPY_LAG_BYTES,
    REMOTE_COPY_LAG_MS,
];

/// Whether a broker serving `unstable` has the topic key `name`: every key
/// under trunk, and every key but [`KAFKA_TRUNK_TOPIC_KEYS`] otherwise.
pub(crate) fn serves_topic_key(name: &str, unstable: UnstableApiVersions) -> bool {
    unstable == UnstableApiVersions::Enabled || !KAFKA_TRUNK_TOPIC_KEYS.contains(&name)
}

/// Kafka's `Records.SOFT_MAX_ARRAY_LENGTH`, `Integer.MAX_VALUE - 8`: the
/// default and the ceiling of `max.decompressed.message.bytes`.
pub(crate) const SOFT_MAX_ARRAY_LENGTH: i32 = i32::MAX - 8;
/// KIP-534: how long the broker keeps tombstones and transaction markers
/// after they first become compaction-eligible. This is the delete-horizon
/// grace window.
pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";

/// Kafka sentinel for `retention.ms` / `retention.bytes`: `-1` means
/// unlimited retention, and is the lowest legal value.
const RETENTION_UNLIMITED: i64 = -1;

/// KIP-405 sentinel for `local.retention.ms` / `local.retention.bytes`:
/// `-2` means "inherit the corresponding non-local retention setting", and
/// is the lowest legal value (`-1` = unlimited also applies).
const LOCAL_RETENTION_INHERIT: i64 = -2;
