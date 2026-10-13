//! The one typed table every config surface reads.
//!
//! `AlterConfigs` validation, the generated topic-config reference page, and
//! `DescribeConfigs` all used to carry their own idea of what a config key is.
//! The validator knew the accepted values, the reference page knew the type
//! and the default, and `DescribeConfigs` knew neither, so every key it
//! reported came back at `ConfigDef.Type::UNKNOWN` with no default chain and
//! `is_sensitive` hard-wired to `false`. This module holds that knowledge once
//! and the three surfaces read it.
//!
//! A row carries the name, the [`ConfigType`] the JVM `AdminClient` parses the
//! value with, the default an unset key reports, the documentation
//! `include_documentation` returns, whether the value is sensitive, and
//! whether `DescribeConfigs` must mark the key read-only. It also carries the
//! value check `validate_topic_config` applies, so the accepted values are
//! stated beside the type rather than in a second `match`.
//!
//! Rows are keyed by [`ConfigScope`] and name together: a key such as
//! `unclean.leader.election.enable` is both a topic config and a
//! cluster-default broker config, and the two rows differ.

use {
    super::{
        CLEANUP_POLICY, COMPRESSION_GZIP_LEVEL, COMPRESSION_LZ4_LEVEL, COMPRESSION_TYPE,
        COMPRESSION_ZSTD_LEVEL, DELETE_RETENTION_MS, ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
        FILE_DELETE_DELAY_MS, FLUSH_MESSAGES, FLUSH_MS, INDEX_INTERVAL_BYTES,
        INTERNAL_SEGMENT_BYTES, LOCAL_RETENTION_BYTES, LOCAL_RETENTION_INHERIT, LOCAL_RETENTION_MS,
        MAX_COMPACTION_LAG_MS, MAX_DECOMPRESSED_MESSAGE_BYTES, MAX_MESSAGE_BYTES,
        MESSAGE_TIMESTAMP_AFTER_MAX_MS, MESSAGE_TIMESTAMP_BEFORE_MAX_MS, MESSAGE_TIMESTAMP_TYPE,
        MESSAGE_TIMESTAMP_TYPE_CREATE, MESSAGE_TIMESTAMP_TYPE_LOG_APPEND,
        MIN_CLEANABLE_DIRTY_RATIO, MIN_COMPACTION_LAG_MS, MIN_INSYNC_REPLICAS, PREALLOCATE,
        REMOTE_COPY_LAG_BYTES, REMOTE_COPY_LAG_MS, REMOTE_LOG_COPY_DISABLE,
        REMOTE_LOG_DELETE_ON_DISABLE, REMOTE_STORAGE_ENABLE, RETENTION_BYTES, RETENTION_MS,
        RETENTION_UNLIMITED, SEGMENT_BYTES, SEGMENT_INDEX_BYTES, SEGMENT_JITTER_MS, SEGMENT_MS,
        SOFT_MAX_ARRAY_LENGTH,
        broker_scope::{
            AUTO_CREATE_TOPICS_ENABLE, BROKER_WITNESS, CONNECTIONS_MAX_IDLE_MS,
            CONNECTIONS_MAX_REAUTH_MS, DEFAULT_REPLICATION_FACTOR, DELETE_TOPIC_ENABLE,
            NUM_PARTITIONS, OFFSETS_RETENTION_CHECK_INTERVAL_MS, OFFSETS_RETENTION_MINUTES,
            REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS, STRETCH_PREFERRED_LEADER_SITE,
            TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS, TRANSACTIONAL_ID_EXPIRATION_MS,
        },
        delivery::{
            DELIVERY_MAX_DELAY_MS, DELIVERY_MAX_DELAY_UNLIMITED, DELIVERY_MODE,
            DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED, DELIVERY_SCHEDULE_MONOTONIC,
        },
        diskless::DISKLESS,
        recovery::{UNCLEAN_LEADER_ELECTION_ENABLE, UNCLEAN_RECOVERY_STRATEGY},
        schema::{
            SCHEMA_VALIDATION_KEY, SCHEMA_VALIDATION_MODE, SCHEMA_VALIDATION_MODE_FULL,
            SCHEMA_VALIDATION_MODE_ID, SCHEMA_VALIDATION_VALUE,
        },
        topic_scope::WRITE_FREEZE,
    },
    crate::{
        client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS},
        coordinator::unified::streams::config::{
            KEY_ACCEPTABLE_RECOVERY_LAG, KEY_ASSIGNOR_NAME, KEY_HEARTBEAT_INTERVAL_MS,
            KEY_NUM_STANDBY_REPLICAS, KEY_NUM_WARMUP_REPLICAS, KEY_RACK_AWARE_ASSIGNMENT_TAGS,
            KEY_SESSION_TIMEOUT_MS, KEY_SHARE_AUTO_OFFSET_RESET, KEY_TASK_OFFSET_INTERVAL_MS,
        },
        cordoned_log_dirs::{CORDONED_LOG_DIRS, CORDONED_LOG_DIRS_DOC},
        throttle::{
            ALTER_LOG_DIRS_THROTTLED_RATE_KEY, FOLLOWER_THROTTLED_RATE_KEY,
            FOLLOWER_THROTTLED_REPLICAS_KEY, LEADER_THROTTLED_RATE_KEY,
            LEADER_THROTTLED_REPLICAS_KEY,
        },
    },
};

/// The `node.id` a broker resource reports beside its dynamic overrides. The
/// broker reads it from its own static configuration, never from the metadata
/// image, so it is the one broker key with no stored form.
pub(crate) const NODE_ID: &str = "node.id";

/// `DescribeConfigsResponse.ConfigType`, the byte the JVM `AdminClient` reads
/// out of `ConfigEntry.type()`.
///
/// The values are
/// `org.apache.kafka.common.requests.DescribeConfigsResponse.ConfigType`,
/// which mirrors `ConfigDef.Type` one-for-one: `UNKNOWN = 0`, `BOOLEAN = 1`,
/// `STRING = 2`, `INT = 3`, `SHORT = 4`, `LONG = 5`, `DOUBLE = 6`, `LIST = 7`,
/// `CLASS = 8`, `PASSWORD = 9`. `Short`, `Class` and `Password` are the types
/// of the `KafkaConfig` broker keys in [`super::kafka_broker`]; no topic key
/// carries them.
///
/// `UNKNOWN` is deliberately absent. A key krabka reports is a key krabka has
/// a row for, and the `DescribeConfigs` handler treats a missing row as a
/// config it may not disclose rather than as an untyped one.
///
/// `label` is the name the generated reference page prints in its type column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::EnumStr)]
#[enum_str(case = "lowercase", as_str = label)]
pub(crate) enum ConfigType {
    Boolean,
    String,
    Int,
    Short,
    Long,
    Double,
    List,
    Class,
    Password,
}

impl ConfigType {
    /// The wire byte `DescribeConfigsResourceResult.config_type` carries.
    pub(crate) const fn wire(self) -> i8 {
        match self {
            Self::Boolean => 1,
            Self::String => 2,
            Self::Int => 3,
            Self::Short => 4,
            Self::Long => 5,
            Self::Double => 6,
            Self::List => 7,
            Self::Class => 8,
            Self::Password => 9,
        }
    }
}

/// The resource type a row belongs to. A name alone does not identify a row:
/// `unclean.leader.election.enable` is both a topic key and a cluster-default
/// broker key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ConfigScope {
    Topic,
    Broker,
    ClientMetrics,
    Group,
}

/// The value check `validate_topic_config` applies to a topic key.
///
/// The four mechanical checks cover every key whose accepted values are a
/// closed set or a numeric floor. [`ValueCheck::Parsed`] names the keys whose
/// check is a parser their own module owns, and
/// [`ValueCheck::NotAltered`] names the keys no alter path accepts at all.
///
/// The width of a numeric check follows the row's [`ConfigType`], because the
/// width is what the JVM `AdminClient` parses the value with: a key krabka
/// reports as [`ConfigType::Int`] must refuse what Kafka's `INT` cannot hold.
/// `apache/kafka:4.3.1` refuses `segment.bytes=2147483648` with
/// `Invalid value 2147483648 for configuration segment.bytes: Not a number of
/// type INT`, so krabka refuses it too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueCheck {
    /// `true` or `false`, exactly.
    Bool,
    /// One of a closed list, in the order the refusal names them.
    OneOf(&'static [&'static str]),
    /// An `i64` no smaller than the bound. Only a [`ConfigType::Long`] row
    /// may carry it. A bound of `i64::MIN` is how a row spells the key Kafka
    /// defines with no validator at all, such as `retention.bytes`: every
    /// value the type holds is accepted, and only the parse can fail.
    I64AtLeast(i64),
    /// An `i32` no smaller than the bound, which is the width Kafka's `INT`
    /// carries on the wire. `i32::MIN` spells "no validator", as it does for
    /// `internal.segment.bytes`.
    I32AtLeast(i32),
    /// An `i32` inside a closed range, which is what Kafka's `between(min,
    /// max)` validator enforces. `compression.lz4.level` and
    /// `compression.zstd.level` are the rows that carry one.
    I32Between(i32, i32),
    /// Checked by a parser the key's own module owns.
    Parsed,
    /// Never accepted by an alter path: the broker synthesises the key, or
    /// only the controller writes it.
    NotAltered,
}

/// One config key, as every surface that reports it needs to know it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ConfigKey {
    pub(crate) name: &'static str,
    pub(crate) scope: ConfigScope,
    /// The type the JVM `AdminClient` parses the value with.
    pub(crate) config_type: ConfigType,
    /// A unit or range the reference page appends to the type name, such as
    /// `ms` or `>=1`. It is documentation, not a second type.
    pub(crate) type_note: Option<&'static str>,
    /// The value an unset key reports, at `DEFAULT_CONFIG`. `None` means the
    /// broker computes the default at request time, or that the key has none.
    pub(crate) default: Option<&'static str>,
    /// What `include_documentation` returns for the key.
    pub(crate) doc: &'static str,
    /// `true` when no alter path may change the key, which
    /// `DescribeConfigs` reports as `read_only`.
    pub(crate) read_only: bool,
    /// `true` when the broker must not disclose the value. Kafka reads this
    /// off `ConfigDef.Type::PASSWORD`; krabka states it, because a key can be
    /// secret without being a password. `DescribeConfigs` reports a sensitive
    /// key with a null value, in the entry and in every synonym.
    pub(crate) sensitive: bool,
    /// `true` for a key Kafka defines with `defineInternal`. Kafka leaves an
    /// internal key out of `DescribeConfigs` and of the `CreateTopics` v5
    /// config list unless the topic itself sets it
    /// (`KafkaConfigSchema.resolveEffectiveTopicConfigs`).
    pub(crate) internal: bool,
    /// The KIP or KFC the key comes from, for the reference page.
    pub(crate) kip: Option<&'static str>,
    /// The cluster-default broker config the broker falls back to when the
    /// resource sets no value of its own. `None` means the broker consults
    /// none, so the key reports no `DYNAMIC_DEFAULT_BROKER_CONFIG` synonym.
    pub(crate) cluster_default: Option<&'static str>,
    pub(crate) check: ValueCheck,
}

impl ConfigKey {
    /// `true` when the resource's stored override map can hold the key.
    ///
    /// The broker synthesises the rest: `write.freeze` comes from the freeze
    /// registry, and `node.id`, the two KIP-211 retention keys, the two
    /// KIP-98 transactional-id expiry keys, the two KIP-464 topic-creation
    /// defaults and the idle window come from the broker's own static
    /// configuration, which no metadata record holds.
    pub(crate) fn is_stored(&self) -> bool {
        !matches!(
            self.name,
            WRITE_FREEZE
                | NODE_ID
                | OFFSETS_RETENTION_MINUTES
                | OFFSETS_RETENTION_CHECK_INTERVAL_MS
                | CONNECTIONS_MAX_IDLE_MS
                | CONNECTIONS_MAX_REAUTH_MS
                | TRANSACTIONAL_ID_EXPIRATION_MS
                | TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS
                | NUM_PARTITIONS
                | DEFAULT_REPLICATION_FACTOR
                | DELETE_TOPIC_ENABLE
                | AUTO_CREATE_TOPICS_ENABLE
        )
    }

    /// `true` when an alter path may write the key, which is what the
    /// reference page documents and what `validate_topic_config` accepts.
    ///
    /// Two things put a key outside that set. A key the resource's override
    /// map cannot hold is synthesised, so no alter has anywhere to store it.
    /// A key whose check is [`ValueCheck::NotAltered`] is stored but
    /// controller-written: broker fencing and stretch-site keys are published
    /// by the controller as cluster state changes, and an operator who set one
    /// by hand would be overwritten.
    pub(crate) fn is_alterable(&self) -> bool {
        self.is_stored() && !matches!(self.check, ValueCheck::NotAltered)
    }

    /// `true` when the value must not be disclosed. `DescribeConfigs` reports
    /// a sensitive key with a null value, as Kafka does.
    pub(crate) const fn is_sensitive(&self) -> bool { self.sensitive }

    /// The type column of the generated reference page.
    pub(crate) fn value_type(&self) -> String {
        match self.type_note {
            Some(note) => format!("{} ({note})", self.config_type.label()),
            None => self.config_type.label().to_owned(),
        }
    }
}

/// Shorthand for a row that needs none of the uncommon fields.
const fn key(
    name: &'static str,
    scope: ConfigScope,
    config_type: ConfigType,
    default: Option<&'static str>,
    doc: &'static str,
    check: ValueCheck,
) -> ConfigKey {
    ConfigKey {
        name,
        scope,
        config_type,
        type_note: None,
        default,
        doc,
        read_only: false,
        sensitive: false,
        internal: false,
        kip: None,
        cluster_default: None,
        check,
    }
}

/// `compression.lz4.level` bounds. Kafka validates the key with
/// `between(1, 17)`, from `CompressionType.LZ4`'s own `minLevel()` and
/// `maxLevel()`.
const LZ4_MIN_LEVEL: i32 = 1;
const LZ4_MAX_LEVEL: i32 = 17;
/// `compression.zstd.level` bounds. Kafka validates the key with
/// `between(-131072, 22)`, the `ZSTD_minCLevel` and `ZSTD_MAX_CLEVEL` of the
/// zstd library that `CompressionType.ZSTD` copies rather than links.
const ZSTD_MIN_LEVEL: i32 = -131_072;
const ZSTD_MAX_LEVEL: i32 = 22;
/// `compression.gzip.level` bounds. Kafka validates the key with a validator
/// of its own: a level inside `[1, 9]` -- `Deflater.BEST_SPEED` through
/// `Deflater.BEST_COMPRESSION` -- or exactly `-1`, which is
/// `Deflater.DEFAULT_COMPRESSION` and the key's default. `0` is refused,
/// which is why the row cannot carry a plain range check.
pub(super) const GZIP_MIN_LEVEL: i32 = 1;
pub(super) const GZIP_MAX_LEVEL: i32 = 9;
pub(super) const GZIP_DEFAULT_LEVEL: i32 = -1;

/// Kafka's `segment.bytes` floor. `LogConfig` validates the key with
/// `atLeast(1024 * 1024)`, so a topic cannot ask for a segment smaller than
/// one mebibyte; Kafka's own coordinators reach below it through
/// `internal.segment.bytes`, which `defineInternal` gives no validator at all.
const SEGMENT_BYTES_MIN: i32 = 1024 * 1024;

/// The values `cleanup.policy` accepts, in the order Kafka's
/// `ValidList.in(COMPACT, DELETE)` names them. Kafka types the key as a LIST
/// and `LogConfig` derives its `compact` and `delete` booleans by membership,
/// so `compact,delete` is as valid as either name alone, and so is the empty
/// list.
pub(super) const CLEANUP_POLICY_VALUES: &[&str] = &["compact", "delete"];
const MESSAGE_TIMESTAMP_TYPE_VALUES: &[&str] = &[
    MESSAGE_TIMESTAMP_TYPE_CREATE,
    MESSAGE_TIMESTAMP_TYPE_LOG_APPEND,
];
const RECOVERY_STRATEGY_VALUES: &[&str] = &["None", "Balanced", "Aggressive"];
const DELIVERY_MODE_VALUES: &[&str] = &[DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED];
const SCHEMA_VALIDATION_MODE_VALUES: &[&str] =
    &[SCHEMA_VALIDATION_MODE_ID, SCHEMA_VALIDATION_MODE_FULL];

/// Every config key the broker validates, documents, or reports.
pub(crate) const CONFIG_KEYS: &[ConfigKey] = &[
    // ── Topic scope ─────────────────────────────────────────────
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Retention time before log segments become eligible for deletion.",
            ValueCheck::I64AtLeast(RETENTION_UNLIMITED),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            RETENTION_BYTES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-1"),
            "Maximum partition size before old segments are deleted. -1, the default, removes the bound; any other negative value means the same thing, because Kafka defines no validator for the key and its retention test is `retention.bytes < 0`.",
            ValueCheck::I64AtLeast(i64::MIN),
        )
    },
    ConfigKey {
        type_note: Some("bytes, >=1048576"),
        ..key(
            SEGMENT_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1073741824"),
            "Target size of a single log segment file. Kafka floors it at one mebibyte; a coordinator that needs a smaller segment sets internal.segment.bytes, which carries no floor.",
            ValueCheck::I32AtLeast(SEGMENT_BYTES_MIN),
        )
    },
    key(
        CLEANUP_POLICY,
        ConfigScope::Topic,
        ConfigType::List,
        Some("delete"),
        "Any combination of `delete` and `compact`, comma-separated, each named at most once: `delete`, `compact`, `compact,delete`, which both compacts the log and applies retention to it, or the empty list, which runs neither and keeps every segment. A policy containing `compact` cannot be combined with remote.storage.enable=true or with delivery.mode=scheduled.",
        ValueCheck::Parsed,
    ),
    key(
        COMPRESSION_TYPE,
        ConfigScope::Topic,
        ConfigType::String,
        Some("producer"),
        "Broker-side compression codec for the topic.",
        ValueCheck::Parsed,
    ),
    ConfigKey {
        type_note: Some("1..9, or -1 for the codec default"),
        ..key(
            COMPRESSION_GZIP_LEVEL,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("-1"),
            "Level a broker-side gzip re-encode runs at; -1 selects the codec default. Producer passthrough preserves the producer's bytes.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("1..17"),
        ..key(
            COMPRESSION_LZ4_LEVEL,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("9"),
            "Level a broker-side lz4 re-encode runs at. Producer passthrough preserves the producer's bytes.",
            ValueCheck::I32Between(LZ4_MIN_LEVEL, LZ4_MAX_LEVEL),
        )
    },
    ConfigKey {
        type_note: Some("-131072..22"),
        ..key(
            COMPRESSION_ZSTD_LEVEL,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("3"),
            "Level a broker-side zstd re-encode runs at. Producer passthrough preserves the producer's bytes.",
            ValueCheck::I32Between(ZSTD_MIN_LEVEL, ZSTD_MAX_LEVEL),
        )
    },
    ConfigKey {
        type_note: Some(">=1"),
        ..key(
            MIN_INSYNC_REPLICAS,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1"),
            "With acks=all, the minimum in-sync replicas required to accept a write; otherwise NOT_ENOUGH_REPLICAS (19).",
            ValueCheck::I32AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("bytes, >=0"),
        ..key(
            MAX_MESSAGE_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("1048588"),
            "Largest record batch accepted for this topic, measured over the batch's whole wire encoding; a larger one is refused with MESSAGE_TOO_LARGE (10). Unset topics inherit the broker's message.max.bytes.",
            ValueCheck::I32AtLeast(0),
        )
    },
    ConfigKey {
        kip: Some("KIP-841"),
        cluster_default: Some(UNCLEAN_LEADER_ELECTION_ENABLE),
        ..key(
            UNCLEAN_LEADER_ELECTION_ENABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Allow electing an out-of-ISR replica as leader on ISR-empty failover (possible data loss).",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-966"),
        cluster_default: Some(UNCLEAN_RECOVERY_STRATEGY),
        ..key(
            UNCLEAN_RECOVERY_STRATEGY,
            ConfigScope::Topic,
            ConfigType::String,
            Some("None"),
            "Offset-aware unclean recovery: `None`, `Balanced`, or `Aggressive`. Supersedes unclean.leader.election.enable.",
            ValueCheck::OneOf(RECOVERY_STRATEGY_VALUES),
        )
    },
    ConfigKey {
        kip: Some("KIP-405"),
        ..key(
            REMOTE_STORAGE_ENABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Opt this topic into tiered (remote) storage. Refused when this broker has no remote storage backend, and on a topic whose cleanup.policy is anything but `delete` or the empty list.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-950"),
        ..key(
            REMOTE_LOG_COPY_DISABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Stop copying this topic's sealed segments to the remote tier while remote.storage.enable stays true. Reads are still served from the tier, so the topic becomes read-only there rather than losing its history.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-950"),
        ..key(
            REMOTE_LOG_DELETE_ON_DISABLE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Permit turning remote.storage.enable off. The flip erases this topic's remote segments and raises its log start offset to the local log start, so it is refused while this is false.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-405"),
        ..key(
            LOCAL_RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-2"),
            "Local-tier retention time for tiered partitions.",
            ValueCheck::I64AtLeast(LOCAL_RETENTION_INHERIT),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        kip: Some("KIP-405"),
        ..key(
            LOCAL_RETENTION_BYTES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-2"),
            "Local-tier retention size budget for tiered partitions.",
            ValueCheck::I64AtLeast(LOCAL_RETENTION_INHERIT),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            REMOTE_COPY_LAG_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("0"),
            "How old a sealed segment may get before it is copied to the remote tier; -1 derives the lag from the effective local.retention.ms. On a tiered topic it must not exceed the effective local.retention.ms. Kafka trunk key, served only with unstable.api.versions.enable=true. A sealed segment waits until it is this old by its newest record or until `remote.copy.lag.bytes` of newer local data sits after it, whichever comes first; 0 copies it at once.",
            ValueCheck::I64AtLeast(-1),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            REMOTE_COPY_LAG_BYTES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("-1"),
            "How many newer local bytes a sealed segment may sit behind before it is copied to the remote tier; -1 derives the lag from the effective local.retention.bytes. On a tiered topic it must not exceed the effective local.retention.bytes. Kafka trunk key, served only with unstable.api.versions.enable=true. A sealed segment waits until this many bytes of newer local data sit after it or until `remote.copy.lag.ms` has passed since its newest record, whichever comes first; 0 copies it at once.",
            ValueCheck::I64AtLeast(-1),
        )
    },
    ConfigKey {
        type_note: Some("bytes, 1..2147483639"),
        ..key(
            MAX_DECOMPRESSED_MESSAGE_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("2147483639"),
            "Largest decompressed size one record may have. Kafka trunk key, served only with unstable.api.versions.enable=true. A compressed batch with a record whose decompressed body is larger is refused wherever the broker decompresses it, as Kafka trunk does: on produce with INVALID_RECORD; by log compaction, which fails the pass and leaves the partition uncleanable until the limit changes, where trunk holds it until a broker restart; and by a ListOffsets by timestamp or MAX_TIMESTAMP that reads it, on the local log or the remote tier, which answers INVALID_RECORD. Lowering the limit below the size of records already stored makes those fail until it is raised again. An uncompressed record is bounded by max.message.bytes alone, and follower replication is not checked.",
            ValueCheck::I32Between(1, SOFT_MAX_ARRAY_LENGTH),
        )
    },
    key(
        ERRORS_DEADLETTERQUEUE_GROUP_ENABLE,
        ConfigScope::Topic,
        ConfigType::Boolean,
        Some("false"),
        "Whether share groups may write undeliverable records to this topic as their dead-letter queue (KIP-1191). A share group that names the topic in errors.deadletterqueue.topic.name writes a record for each rejected or delivery-exhausted offset to it, and a write to a topic where this is false is refused. Kafka trunk key, served only with unstable.api.versions.enable=true; the dead-letter queue itself needs share.version 2, so unstable.feature.versions.enable=true too.",
        ValueCheck::Bool,
    ),
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-534"),
        ..key(
            DELETE_RETENTION_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("86400000"),
            "How long tombstones and transaction markers are retained after becoming compaction-eligible.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            DISKLESS,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Route this topic through the diskless WAL data path instead of the local log. Fixed when the topic is created, and exclusive with both remote.storage.enable and delivery.mode=scheduled.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_MODE,
            ConfigScope::Topic,
            ConfigType::String,
            Some(DELIVERY_MODE_IMMEDIATE),
            "`immediate` or `scheduled`. Under `scheduled` a batch stays invisible to consumers until its own timestamp comes due.",
            ValueCheck::OneOf(DELIVERY_MODE_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_MAX_DELAY_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Largest delivery delay accepted at produce time, measured forward from produce time; -1 removes the bound.",
            ValueCheck::I64AtLeast(DELIVERY_MAX_DELAY_UNLIMITED),
        )
    },
    ConfigKey {
        kip: Some("KFC-1"),
        ..key(
            DELIVERY_SCHEDULE_MONOTONIC,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Reject a batch whose delivery time precedes the largest delivery time already in the partition.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_KEY,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Validate the schema of every record key produced to this topic.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_VALUE,
            ConfigScope::Topic,
            ConfigType::Boolean,
            Some("false"),
            "Validate the schema of every record value produced to this topic.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KFC-7"),
        ..key(
            SCHEMA_VALIDATION_MODE,
            ConfigScope::Topic,
            ConfigType::String,
            Some(SCHEMA_VALIDATION_MODE_ID),
            "`id` checks the Confluent header alone; `full` also decodes the record body against the schema the header names.",
            ValueCheck::OneOf(SCHEMA_VALIDATION_MODE_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            SEGMENT_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("604800000"),
            "Roll the active segment once its first record is older than this, even when it has not reached segment.bytes.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        internal: true,
        ..key(
            INTERNAL_SEGMENT_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            None,
            "Kafka's internal segment-size override, which its own coordinators set on the internal topics they create. Kafka's ConfigDef marks it internal, which hides it from the CLI's help but not from topic-config validation, so an alter carrying it is accepted. When set it is the segment size the log rolls on, in place of segment.bytes, and it carries no floor of its own.",
            ValueCheck::I32AtLeast(i32::MIN),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            SEGMENT_INDEX_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("10485760"),
            "Size cap on each of a segment's two sparse indexes. The active segment rolls once its offset index holds this many bytes of 8-byte entries, or its time index is one 12-byte entry short of this many bytes.",
            ValueCheck::I32AtLeast(4),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            SEGMENT_JITTER_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("0"),
            "Maximum random subtraction from segment.ms, sampled once per segment to stagger partition rolls and capped by segment.ms.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MIN_COMPACTION_LAG_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("0"),
            "How long a record is safe from the log cleaner after it is written. A pass stops at the first uncompacted segment younger than this and compacts what lies before it. Must not exceed max.compaction.lag.ms.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MAX_COMPACTION_LAG_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "How long a record may stay uncompacted before the cleaner runs whatever the dirty ratio says. Must not be below min.compaction.lag.ms.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("0..1"),
        ..key(
            MIN_CLEANABLE_DIRTY_RATIO,
            ConfigScope::Topic,
            ConfigType::Double,
            Some("0.5"),
            "The share of a compacted partition's log that must be uncleaned before the cleaner spends a pass on it.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            FILE_DELETE_DELAY_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("60000"),
            "Delay before retired segment files are unlinked. Removed records become unavailable immediately; renamed files remain for existing readers until the delay expires.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("records"),
        ..key(
            FLUSH_MESSAGES,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "Offset span between forced fsyncs. Long.MAX_VALUE disables the threshold. Stricter durability settings may sync sooner.",
            ValueCheck::I64AtLeast(1),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            FLUSH_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "Milliseconds between forced fsyncs of dirty data, including idle partitions. Long.MAX_VALUE disables the timer. Stricter durability settings may sync sooner.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("bytes"),
        ..key(
            INDEX_INTERVAL_BYTES,
            ConfigScope::Topic,
            ConfigType::Int,
            Some("4096"),
            "Bytes of .log between entries in the sparse offset and timestamp indexes.",
            ValueCheck::I32AtLeast(0),
        )
    },
    key(
        PREALLOCATE,
        ConfigScope::Topic,
        ConfigType::Boolean,
        Some("false"),
        "Reserve segment.bytes of disk for each new segment without changing the file's length, return the unused part when the segment rolls, and write the active segment through O_DIRECT, keeping its newest data in memory for reads (the broker's log_tail_cache_size, 32 MiB by default). Linux only; elsewhere, or on a filesystem that refuses, a segment grows as it is written through the page cache.",
        ValueCheck::Bool,
    ),
    key(
        MESSAGE_TIMESTAMP_TYPE,
        ConfigScope::Topic,
        ConfigType::String,
        Some(MESSAGE_TIMESTAMP_TYPE_CREATE),
        "`CreateTime` stores the producer's own timestamps; `LogAppendTime` makes the broker stamp its own clock into every batch at append and report it as the produce response's logAppendTimeMs. It cannot be combined with delivery.mode=scheduled, whose activation time is the field the stamp overwrites.",
        ValueCheck::OneOf(MESSAGE_TIMESTAMP_TYPE_VALUES),
    ),
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MESSAGE_TIMESTAMP_AFTER_MAX_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("3600000"),
            "How far ahead of the broker's clock a producer timestamp may sit. A batch holding a record past the window is refused with INVALID_TIMESTAMP. The default is one hour, as in Kafka; Long.MAX_VALUE removes the bound, and a LogAppendTime topic ignores it. A delivery.mode=scheduled topic ignores it too, because there the timestamp is the record's delivery time and delivery.max.delay.ms is the bound on it.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        ..key(
            MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
            ConfigScope::Topic,
            ConfigType::Long,
            Some("9223372036854775807"),
            "How far behind the broker's clock a producer timestamp may sit. A batch holding a record before the window is refused with INVALID_TIMESTAMP. The default of Long.MAX_VALUE removes the bound, and a LogAppendTime topic ignores it, as in Kafka.",
            ValueCheck::I64AtLeast(0),
        )
    },
    ConfigKey {
        kip: Some("KIP-73"),
        ..key(
            LEADER_THROTTLED_REPLICAS_KEY,
            ConfigScope::Topic,
            ConfigType::List,
            Some(""),
            "Replica list throttled on the leader side during reassignment.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-73"),
        ..key(
            FOLLOWER_THROTTLED_REPLICAS_KEY,
            ConfigScope::Topic,
            ConfigType::List,
            Some(""),
            "Replica list throttled on the follower side during reassignment.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        read_only: true,
        kip: Some("KFC-9"),
        ..key(
            WRITE_FREEZE,
            ConfigScope::Topic,
            ConfigType::String,
            Some("false"),
            "Write-freeze state of the topic: `false`, or `frozen:` and the registry scope that matched. Set and cleared with `krabka-guard freeze`.",
            ValueCheck::NotAltered,
        )
    },
    // ── Broker scope ────────────────────────────────────────────
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            LEADER_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to leader-side replication of throttled replicas.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            FOLLOWER_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to follower-side replication of throttled replicas.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("bytes/s"),
        kip: Some("KIP-73"),
        ..key(
            ALTER_LOG_DIRS_THROTTLED_RATE_KEY,
            ConfigScope::Broker,
            ConfigType::Long,
            None,
            "Byte rate ceiling this broker applies to inter-log-dir replica movement.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-841"),
        ..key(
            UNCLEAN_LEADER_ELECTION_ENABLE,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("false"),
            "Cluster-wide default for the topic key of the same name. Valid only on the cluster-default broker resource.",
            ValueCheck::Bool,
        )
    },
    ConfigKey {
        kip: Some("KIP-1066"),
        ..key(
            CORDONED_LOG_DIRS,
            ConfigScope::Broker,
            ConfigType::List,
            Some(""),
            CORDONED_LOG_DIRS_DOC,
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-966"),
        ..key(
            UNCLEAN_RECOVERY_STRATEGY,
            ConfigScope::Broker,
            ConfigType::String,
            Some("None"),
            "Cluster-wide default for the topic key of the same name. Valid only on the cluster-default broker resource.",
            ValueCheck::OneOf(RECOVERY_STRATEGY_VALUES),
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1075"),
        ..key(
            REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("30000"),
            "Server-side deadline for remote ListOffsets work when the request carries no timeout of its own.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            BROKER_WITNESS,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("false"),
            "Marks this node as a data-bearing witness: it replicates and votes but serves no client and leads no partition. Only the controller writes it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            STRETCH_PREFERRED_LEADER_SITE,
            ConfigScope::Broker,
            ConfigType::String,
            None,
            "The `broker.rack` value that should hold partition leadership in a stretch cluster. Only the controller writes it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("minutes"),
        kip: Some("KIP-211"),
        read_only: true,
        ..key(
            OFFSETS_RETENTION_MINUTES,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("10080"),
            "How long a committed consumer-group offset outlives the group that owns it, once that group loses its last member. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-211"),
        read_only: true,
        ..key(
            OFFSETS_RETENTION_CHECK_INTERVAL_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("600000"),
            "Cadence of the background sweep that tombstones expired committed offsets. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        read_only: true,
        ..key(
            CONNECTIONS_MAX_IDLE_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("600000"),
            "How long a client connection may go without a complete request frame before the broker closes it. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-368"),
        read_only: true,
        ..key(
            CONNECTIONS_MAX_REAUTH_MS,
            ConfigScope::Broker,
            ConfigType::Long,
            Some("0"),
            "How long an authenticated SASL session may live before the client must re-authenticate in band. Zero disables re-authentication. The process reads it at startup, so no alter path can change it.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        kip: Some("KIP-464"),
        read_only: true,
        ..key(
            NUM_PARTITIONS,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("1"),
            "The partition count `CreateTopics` gives a topic that asks for `num_partitions = -1`, which is what `kafka-topics --create` sends without `--partitions`. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        kip: Some("KIP-464"),
        read_only: true,
        ..key(
            DEFAULT_REPLICATION_FACTOR,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("1"),
            "The replication factor `CreateTopics` gives a topic that asks for `replication_factor = -1`, which is what `kafka-topics --create` sends without `--replication-factor`. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            DELETE_TOPIC_ENABLE,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("true"),
            "Whether `DeleteTopics` may delete a topic. When it is `false`, every `DeleteTopics` request is refused: `INVALID_REQUEST` below v3 and `TOPIC_DELETION_DISABLED` from v3. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            AUTO_CREATE_TOPICS_ENABLE,
            ConfigScope::Broker,
            ConfigType::Boolean,
            Some("true"),
            "Whether `Metadata` creates a topic it is asked about and that does not exist, when the request sets `allow_auto_topic_creation`. When it is `false`, such a topic answers `UNKNOWN_TOPIC_OR_PARTITION` and is not created. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-98"),
        read_only: true,
        ..key(
            TRANSACTIONAL_ID_EXPIRATION_MS,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("604800000"),
            "How long a transactional id may sit in a terminal or idle state before the transaction coordinator tombstones it out of `__transaction_state`. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-98"),
        read_only: true,
        ..key(
            TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
            ConfigScope::Broker,
            ConfigType::Int,
            Some("3600000"),
            "How often the transactional-id expiry sweep scans the `__transaction_state` partitions this node leads. Read from this node's own configuration; Kafka refuses to alter it dynamically.",
            ValueCheck::NotAltered,
        )
    },
    ConfigKey {
        read_only: true,
        ..key(
            NODE_ID,
            ConfigScope::Broker,
            ConfigType::Int,
            None,
            "The node id this process runs under, from its static configuration.",
            ValueCheck::NotAltered,
        )
    },
    // ── Client-metrics scope (KIP-714) ──────────────────────────
    ConfigKey {
        kip: Some("KIP-714"),
        ..key(
            KEY_METRICS,
            ConfigScope::ClientMetrics,
            ConfigType::List,
            Some(""),
            "Metric name prefixes this subscription collects. `*` collects every metric; an empty list collects none, as it does on Kafka, where an empty subscription contributes no metric name to the client's set.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-714"),
        ..key(
            KEY_INTERVAL_MS,
            ConfigScope::ClientMetrics,
            ConfigType::Int,
            None,
            "How often a matching client pushes metrics. Unset falls back to the broker's default push interval.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-714"),
        ..key(
            KEY_MATCH,
            ConfigScope::ClientMetrics,
            ConfigType::List,
            Some(""),
            "Client selectors this subscription matches on. Empty matches every client.",
            ValueCheck::Parsed,
        )
    },
    // ── Group scope (KIP-1071) ──────────────────────────────────
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            KEY_SESSION_TIMEOUT_MS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "How long the coordinator waits for a heartbeat before it removes a member of this streams group.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            KEY_HEARTBEAT_INTERVAL_MS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "How often a member of this streams group heartbeats.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("records"),
        kip: Some("KIP-1071"),
        ..key(
            KEY_ACCEPTABLE_RECOVERY_LAG,
            ConfigScope::Group,
            ConfigType::Long,
            None,
            "Changelog lag in records at which a standby task counts as caught up enough to take an active task.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            KEY_NUM_WARMUP_REPLICAS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "Cap on the warmup tasks that may migrate state at once.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            KEY_NUM_STANDBY_REPLICAS,
            ConfigScope::Group,
            ConfigType::Int,
            None,
            "Standby copies the assignor places for each stateful task.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        type_note: Some("ms"),
        kip: Some("KIP-1071"),
        ..key(
            KEY_TASK_OFFSET_INTERVAL_MS,
            ConfigScope::Group,
            ConfigType::Long,
            None,
            "How often a member reports its task offsets to the coordinator.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            KEY_ASSIGNOR_NAME,
            ConfigScope::Group,
            ConfigType::String,
            None,
            "Server-side task assignor: `auto`, `sticky`, or `highly_available`.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-1071"),
        ..key(
            KEY_RACK_AWARE_ASSIGNMENT_TAGS,
            ConfigScope::Group,
            ConfigType::List,
            Some(""),
            "Client tag keys every member of this streams group must send. A version 1 heartbeat from a member that leaves one out carries the MISSING_CLIENT_TAGS status.",
            ValueCheck::Parsed,
        )
    },
    ConfigKey {
        kip: Some("KIP-932"),
        ..key(
            KEY_SHARE_AUTO_OFFSET_RESET,
            ConfigScope::Group,
            ConfigType::String,
            None,
            "Where a share partition starts when the share coordinator holds no state for it: `latest` (the default, the high watermark), `earliest` (the log start offset), or `by_duration:<PnDTnHnMn.nS>` (the first record at or after `now - duration`, and the high watermark when no record qualifies). A negative duration is refused with INVALID_CONFIG.",
            ValueCheck::Parsed,
        )
    },
];

/// The row for one key, or `None` when the scope reports no such key.
pub(crate) fn lookup(scope: ConfigScope, name: &str) -> Option<&'static ConfigKey> {
    CONFIG_KEYS
        .iter()
        .find(|entry| entry.scope == scope && entry.name == name)
}

/// Every row in one scope, in the table's own order.
pub(crate) fn keys_in(scope: ConfigScope) -> impl Iterator<Item = &'static ConfigKey> {
    CONFIG_KEYS.iter().filter(move |entry| entry.scope == scope)
}

#[cfg(test)]
mod tests;
