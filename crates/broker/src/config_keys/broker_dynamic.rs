//! Kafka's dynamic broker config rules, `DynamicBrokerConfig.validateConfigs`,
//! for the `BROKER` resource both alter APIs write.
//!
//! Kafka lets an alter write a key it does not define, and stores it. It
//! refuses a Kafka key that is not dynamic, an SSL key without a listener
//! prefix, a per-broker key on the cluster-default resource, and a value its
//! own validator refuses, each with `INVALID_REQUEST` (KAFKA-13609 keeps the
//! code at 42 even for a bad value). The rules here are that check. A `KafkaConfig`
//! key is one [`super::kafka_broker`] holds, and any other name passes
//! through unchecked, as Kafka's `ConfigDef.parse` ignores a name it does not
//! define. The broker synonyms of Kafka trunk's topic keys join the roster
//! under `unstable.api.versions.enable`.

use std::{collections::BTreeMap, path::PathBuf};

use krabka_metadata::NodeId;

use super::{
    MIN_INSYNC_REPLICAS,
    kafka_broker::{self, Dynamic, listener_override_base},
    parse::{check_range, parse_long},
    recovery::UNCLEAN_RECOVERY_STRATEGY,
    registry::{self, ConfigScope},
    serves_topic_key,
    validation::canonical_value,
};
use crate::{
    api_catalog::UnstableApiVersions,
    codes,
    cordoned_log_dirs::{CORDONED_LOG_DIRS, resolve, validate_value_type},
    features::ELR_VERSION,
    throttle::{
        ALTER_LOG_DIRS_THROTTLED_RATE_KEY, FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY,
    },
};

/// The live facts used by either broker-configuration mutation API.
#[derive(Clone, Copy)]
pub(crate) struct BrokerConfigContext<'a> {
    pub image: &'a krabka_metadata::MetadataImage,
    pub serving: NodeId,
    pub log_dirs: &'a [std::path::PathBuf],
    pub unstable: UnstableApiVersions,
}

impl BrokerConfigContext<'_> {
    pub fn resource_node(&self, name: &str) -> Result<NodeId, (i16, String)> {
        broker_resource_node(name, self.serving)
    }
}

/// Kafka's `ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS`: the broker key
/// that sets the cluster-wide default of each topic key, for the topic keys
/// krabka carries. `DynamicLogConfig.RECONFIGURABLE_CONFIGS` is exactly this
/// set, so each of them is dynamic.
pub(crate) const TOPIC_DEFAULT_SYNONYMS: &[(&str, &str)] = &[
    ("log.segment.bytes", "segment.bytes"),
    ("log.roll.ms", "segment.ms"),
    ("log.roll.jitter.ms", "segment.jitter.ms"),
    ("log.index.size.max.bytes", "segment.index.bytes"),
    ("log.flush.interval.messages", "flush.messages"),
    ("log.flush.interval.ms", "flush.ms"),
    ("log.retention.bytes", "retention.bytes"),
    ("log.retention.ms", "retention.ms"),
    ("message.max.bytes", "max.message.bytes"),
    (
        "max.decompressed.message.bytes",
        "max.decompressed.message.bytes",
    ),
    ("log.index.interval.bytes", "index.interval.bytes"),
    ("log.cleaner.delete.retention.ms", "delete.retention.ms"),
    ("log.cleaner.min.compaction.lag.ms", "min.compaction.lag.ms"),
    ("log.cleaner.max.compaction.lag.ms", "max.compaction.lag.ms"),
    ("log.segment.delete.delay.ms", "file.delete.delay.ms"),
    (
        "log.cleaner.min.cleanable.ratio",
        "min.cleanable.dirty.ratio",
    ),
    ("log.cleanup.policy", "cleanup.policy"),
    (
        "unclean.leader.election.enable",
        "unclean.leader.election.enable",
    ),
    ("min.insync.replicas", "min.insync.replicas"),
    ("compression.type", "compression.type"),
    ("compression.gzip.level", "compression.gzip.level"),
    ("compression.lz4.level", "compression.lz4.level"),
    ("compression.zstd.level", "compression.zstd.level"),
    ("log.preallocate", "preallocate"),
    ("log.message.timestamp.type", "message.timestamp.type"),
    (
        "log.message.timestamp.before.max.ms",
        "message.timestamp.before.max.ms",
    ),
    (
        "log.message.timestamp.after.max.ms",
        "message.timestamp.after.max.ms",
    ),
    ("log.local.retention.ms", "local.retention.ms"),
    ("log.local.retention.bytes", "local.retention.bytes"),
    ("log.remote.copy.lag.ms", "remote.copy.lag.ms"),
    ("log.remote.copy.lag.bytes", "remote.copy.lag.bytes"),
];

/// The topic key whose default the broker key `name` sets, when a broker
/// serving `unstable` defines `name`. Under Kafka 4.3.1 the broker keys of
/// Kafka trunk's topic keys are not `KafkaConfig` keys, so they are unknown
/// broker keys like any other.
fn served_topic_default(name: &str, unstable: UnstableApiVersions) -> Option<&'static str> {
    TOPIC_DEFAULT_SYNONYMS
        .iter()
        .find(|(broker, _)| *broker == name)
        .map(|(_, topic)| *topic)
        .filter(|topic| serves_topic_key(topic, unstable))
}

/// The broker keys that set a topic key's default on a broker serving
/// `unstable`.
pub(crate) fn served_topic_default_synonyms(
    unstable: UnstableApiVersions,
) -> impl Iterator<Item = &'static str> {
    TOPIC_DEFAULT_SYNONYMS
        .iter()
        .filter(move |(_, topic)| serves_topic_key(topic, unstable))
        .map(|(broker, _)| *broker)
}

/// One broker synonym of a topic key, with Kafka's broker default for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrokerSynonym {
    pub(crate) name: &'static str,
    pub(crate) default: Option<&'static str>,
}

const fn synonym(name: &'static str, default: Option<&'static str>) -> BrokerSynonym {
    BrokerSynonym { name, default }
}

/// Kafka's `DynamicBrokerConfig.brokerConfigSynonyms` for the broker key of a
/// topic key, most preferred first, each with its `KafkaConfig` default. Four
/// topic keys have a family of broker keys, and only the least preferred of
/// each family carries a default; every other topic key has one broker key,
/// whose default is the topic key's own.
pub(crate) fn topic_broker_synonyms(topic_key: &str) -> Vec<BrokerSynonym> {
    match topic_key {
        "retention.ms" => vec![
            synonym("log.retention.ms", None),
            synonym("log.retention.minutes", None),
            synonym("log.retention.hours", Some("168")),
        ],
        "segment.ms" => vec![
            synonym("log.roll.ms", None),
            synonym("log.roll.hours", Some("168")),
        ],
        "segment.jitter.ms" => vec![
            synonym("log.roll.jitter.ms", None),
            synonym("log.roll.jitter.hours", Some("0")),
        ],
        "flush.ms" => vec![
            synonym("log.flush.interval.ms", None),
            synonym(
                "log.flush.scheduler.interval.ms",
                Some("9223372036854775807"),
            ),
        ],
        _ => TOPIC_DEFAULT_SYNONYMS
            .iter()
            .find(|(_, topic)| *topic == topic_key)
            .map(|(broker, topic)| {
                synonym(
                    broker,
                    registry::lookup(ConfigScope::Topic, topic).and_then(|row| row.default),
                )
            })
            .into_iter()
            .collect(),
    }
}

/// The row that types a broker key, for `DescribeConfigs`: its own registry
/// row, the row of the topic key it sets the default of on a broker serving
/// `unstable`, or, for a `KafkaConfig` key krabka has no row for, the one
/// [`kafka_broker`] states. `None` is a name Kafka does not define, which
/// Kafka reports untyped and withholds the value of.
pub(crate) fn broker_key_row(
    name: &str,
    unstable: UnstableApiVersions,
) -> Option<registry::ConfigKey> {
    registry::lookup(ConfigScope::Broker, name)
        .or_else(|| {
            served_topic_default(name, unstable)
                .and_then(|topic| registry::lookup(ConfigScope::Topic, topic))
        })
        .copied()
        .or_else(|| kafka_broker::resolve(name).map(kafka_broker::KafkaBrokerConfig::row))
}

/// The three KIP-73 replication quotas, which Kafka's
/// `QuotaConfig.brokerQuotaConfigs` defines as `LONG` with `atLeast(0)`.
const THROTTLE_RATES: [&str; 3] = [
    LEADER_THROTTLED_RATE_KEY,
    FOLLOWER_THROTTLED_RATE_KEY,
    ALTER_LOG_DIRS_THROTTLED_RATE_KEY,
];

/// Kafka's `SslConfigs.RECONFIGURABLE_CONFIGS`: dynamic only per listener,
/// under a `listener.name.<listener>.` prefix.
const DYNAMIC_SECURITY_CONFIGS: &[&str] = &[
    "ssl.keystore.type",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "ssl.key.password",
    "ssl.keystore.key",
    "ssl.keystore.certificate.chain",
    "ssl.truststore.type",
    "ssl.truststore.location",
    "ssl.truststore.password",
    "ssl.truststore.certificates",
];

/// Kafka's `CLUSTER_LEVEL_LISTENER_CONFIGS`: listener-prefixed keys that the
/// cluster-default resource may still hold.
const CLUSTER_LEVEL_LISTENER_CONFIGS: &[&str] = &[
    "max.connections",
    "max.connection.creation.rate",
    "num.network.threads",
];

/// Whether a key may be set only on a named broker: a key of Kafka's
/// `PER_BROKER_CONFIGS` (the SSL keys, `DynamicListenerConfig`'s keys and
/// `cordoned.log.dirs`, less the cluster-level listener keys), or a listener
/// override of anything but the cluster-level listener keys.
fn is_per_broker(name: &str) -> bool {
    kafka_broker::lookup(name).is_some_and(|row| row.dynamic == Dynamic::PerBroker)
        || listener_override_base(name)
            .is_some_and(|base| !CLUSTER_LEVEL_LISTENER_CONFIGS.contains(&base))
}

/// What krabka knows of a broker key, for `IncrementalAlterConfigs`'
/// APPEND and SUBTRACT, which Kafka allows on a `LIST` key alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerKeyKind {
    /// A key Kafka types `LIST`.
    List,
    /// A key Kafka defines with another type.
    Scalar,
    /// A key Kafka does not define.
    Unknown,
}

/// The kind of a broker key on a broker serving `unstable`, for the APPEND
/// and SUBTRACT checks.
pub(crate) fn broker_key_kind(name: &str, unstable: UnstableApiVersions) -> BrokerKeyKind {
    if let Some(row) = registry::lookup(ConfigScope::Broker, name)
        && row.config_type == registry::ConfigType::List
    {
        return BrokerKeyKind::List;
    }
    let topic_key = served_topic_default(name, unstable);
    if let Some(row) = topic_key.and_then(|key| registry::lookup(ConfigScope::Topic, key)) {
        return if row.config_type == registry::ConfigType::List {
            BrokerKeyKind::List
        } else {
            BrokerKeyKind::Scalar
        };
    }
    // Kafka's `prepareIncrementalConfigs` reads the type off `KafkaConfig`'s
    // own `ConfigDef`, so it covers the keys that are not dynamic too, which
    // the whole-map validation refuses afterwards.
    if let Some(row) = kafka_broker::lookup(name) {
        return if row.config_type == registry::ConfigType::List {
            BrokerKeyKind::List
        } else {
            BrokerKeyKind::Scalar
        };
    }
    if THROTTLE_RATES.contains(&name) || registry::lookup(ConfigScope::Broker, name).is_some() {
        return BrokerKeyKind::Scalar;
    }
    BrokerKeyKind::Unknown
}

/// The node a `BROKER` resource names: the cluster default for the empty
/// name, else this node, which Kafka's `validateResourceNameIsCurrentNodeId`
/// requires.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message for a name that is not an
/// integer, or that names another node.
pub(crate) fn broker_resource_node(
    resource_name: &str,
    serving: NodeId,
) -> Result<NodeId, (i16, String)> {
    if resource_name.is_empty() {
        return Ok(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID);
    }
    let id = resource_name.parse::<i32>().map_err(|_| {
        (
            codes::INVALID_REQUEST,
            format!("Node id must be an integer, but it is: {resource_name}"),
        )
    })?;
    u64::try_from(id)
        .ok()
        .map(NodeId)
        .filter(|node| *node == serving)
        .ok_or_else(|| {
            (
                codes::INVALID_REQUEST,
                format!(
                    "Unexpected broker id, expected {}, but received {resource_name}",
                    serving.0
                ),
            )
        })
}

/// Kafka's `checkInvalidProps` text: the message and the names, the way a
/// Java `Set` prints them.
fn invalid_props(message: &str, names: &[&str]) -> (i16, String) {
    (
        codes::INVALID_REQUEST,
        format!("{message}: [{}]", names.join(", ")),
    )
}

/// Validate one value of a dynamic broker key krabka knows. A key krabka does
/// not know, or does not serve under `unstable`, passes, as Kafka's
/// `DynamicConfig.Broker.validate` passes a key it does not define.
///
/// The parsed value is dropped: Kafka stores the string the client sent
/// (`ConfigurationControlManager.incrementalAlterConfigResource` takes the
/// operation's value as it is, and `DynamicConfig.Broker.validate` discards
/// the parse), so a describe returns what an alter wrote.
fn check_broker_value(
    name: &str,
    value: &str,
    unstable: UnstableApiVersions,
) -> Result<(), String> {
    if THROTTLE_RATES.contains(&name) {
        return check_range(name, parse_long(name, value)?, Some(0), None).map(drop);
    }
    if name == CORDONED_LOG_DIRS {
        return validate_value_type(value);
    }
    if name == UNCLEAN_RECOVERY_STRATEGY {
        return registry::lookup(ConfigScope::Broker, name)
            .map_or(Ok(()), |row| canonical_value(row, value).map(drop));
    }
    let topic_key = served_topic_default(name, unstable);
    if let Some(row) = topic_key.and_then(|key| registry::lookup(ConfigScope::Topic, key)) {
        // The topic key's validator, under the broker key's name.
        return canonical_value(row, value)
            .map(drop)
            .map_err(|message| message.replacen(row.name, name, 2));
    }
    // A `KafkaConfig` key is parsed by `DynamicConfig.Broker.validate`, and a
    // listener override is checked as the key it overrides, under that key's
    // name. A key that is not dynamic never gets here.
    let checked = listener_override_base(name).unwrap_or(name);
    kafka_broker::lookup(checked)
        .filter(|row| row.dynamic != Dynamic::ReadOnly)
        .map_or(Ok(()), |row| row.canonical(value).map(drop))
}

/// Kafka's `DynamicBrokerConfig.validateConfigs`, over the whole set of
/// dynamic configs the resource ends up with. `per_broker` is `true` for a
/// named broker, and `unstable` decides which topic keys' broker synonyms the
/// broker defines. The caller stores `props` as it is.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message, in Kafka's order: a
/// non-dynamic key, then an unprefixed SSL key, then a bad value, then a
/// per-broker key on the cluster-default resource.
pub(crate) fn validate_dynamic_broker_configs(
    props: &BTreeMap<String, String>,
    per_broker: bool,
    unstable: UnstableApiVersions,
) -> Result<(), (i16, String)> {
    let names = |test: &dyn Fn(&str) -> bool| -> Vec<&str> {
        props
            .keys()
            .map(String::as_str)
            .filter(|name| test(name))
            .collect()
    };
    let non_dynamic = names(&|name| {
        kafka_broker::lookup(name).is_some_and(|row| row.dynamic == Dynamic::ReadOnly)
    });
    if !non_dynamic.is_empty() {
        return Err(invalid_props(
            "Cannot update these configs dynamically",
            &non_dynamic,
        ));
    }
    let unprefixed = names(&|name| DYNAMIC_SECURITY_CONFIGS.contains(&name));
    if !unprefixed.is_empty() {
        return Err(invalid_props(
            "These security configs can be dynamically updated only per-listener using the \
             listener prefix",
            &unprefixed,
        ));
    }
    props.iter().try_for_each(|(name, value)| {
        check_broker_value(name, value, unstable)
            .map_err(|message| (codes::INVALID_REQUEST, message))
    })?;
    if !per_broker {
        let per_broker_keys = names(&is_per_broker);
        if !per_broker_keys.is_empty() {
            return Err(invalid_props(
                "Cannot update these configs at default cluster level, broker id must be \
                 specified",
                &per_broker_keys,
            ));
        }
    }
    Ok(())
}

/// Kafka's `ConfigurationControlManager` ELR rules for one broker config
/// write: while ELR is on, no named broker may carry `min.insync.replicas`,
/// and the cluster-level value may not be removed.
pub(crate) fn elr_min_isr_error(
    image: &krabka_metadata::MetadataImage,
    node: NodeId,
    name: &str,
    value: Option<&str>,
) -> Option<(i16, String)> {
    let elr_enabled = image
        .finalized_feature(ELR_VERSION)
        .is_some_and(|level| level >= 1);
    if name != MIN_INSYNC_REPLICAS || !elr_enabled {
        return None;
    }
    if node != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID {
        return Some((
            codes::INVALID_CONFIG,
            "Broker-level min.insync.replicas cannot be altered while ELR is enabled.".into(),
        ));
    }
    value.is_none().then(|| {
        (
            codes::INVALID_CONFIG,
            "Cluster-level min.insync.replicas cannot be removed while ELR is enabled.".into(),
        )
    })
}

/// The broker's own check of the `cordoned.log.dirs` a named-broker resource
/// ends up with, against this node's `log_dirs`.
///
/// Kafka's `ConfigAdminManager.validateBrokerConfigChange` builds a whole
/// `KafkaConfig` from the resource's dynamic configs, and its
/// `validateCordonedLogDirs` refuses a value that is not `*` alone or a subset
/// of `log.dirs`. The `IllegalArgumentException` it throws reaches the client
/// as `INVALID_REQUEST`. When `log_dirs` is empty, the forwarding broker has
/// already checked directory membership. The authoritative controller still
/// checks that `*` occurs alone in the merged config value.
///
/// # Errors
/// Returns `INVALID_REQUEST` with Kafka's message.
pub(crate) fn cordoned_log_dirs_error(
    props: &BTreeMap<String, String>,
    log_dirs: &[PathBuf],
) -> Result<(), (i16, String)> {
    let Some(value) = props.get(CORDONED_LOG_DIRS) else {
        return Ok(());
    };
    let mentioned: Vec<PathBuf> = if log_dirs.is_empty() {
        value.split(',').map(PathBuf::from).collect()
    } else {
        Vec::new()
    };
    let membership = if log_dirs.is_empty() {
        &mentioned
    } else {
        log_dirs
    };
    resolve(value, membership)
        .map(drop)
        .map_err(|message| (codes::INVALID_REQUEST, message))
}

/// Kafka's `ConfigurationControlManager.isCordonedLogDirsDisabled`: below
/// `metadata.version` `4.3-IV0` the controller refuses every write of
/// `cordoned.log.dirs` to a broker resource, a deletion included.
pub(crate) fn cordoned_log_dirs_disabled_error(
    image: &krabka_metadata::MetadataImage,
    name: &str,
) -> Option<(i16, String)> {
    let supported = image.finalized_metadata_version().is_some_and(|level| {
        level >= krabka_metadata::metadata_version::CORDONED_LOG_DIRS_MIN_LEVEL
    });
    (name == CORDONED_LOG_DIRS && !supported).then(|| {
        (
            codes::INVALID_CONFIG,
            format!(
                "The {name} configuration value cannot be set because it requires \
                 metadata.version >= 4.3-IV0"
            ),
        )
    })
}

/// krabka's own KIP-966 strategy key, which the controller reads from the
/// cluster-default resource alone, so a named broker may not hold it.
pub(crate) const CLUSTER_DEFAULT_ONLY: &[&str] = &[UNCLEAN_RECOVERY_STRATEGY];

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{test_support::config_map as map, throttle::LEADER_THROTTLED_RATE_KEY};

    /// Controller-side validation skips broker-local membership checks but
    /// must reject a wildcard mixed with explicit directories after a merge.
    #[test]
    fn forwarded_cordoned_config_keeps_the_wildcard_invariant() {
        let cases = [
            ("*", Ok(())),
            ("/remote/broker/logs", Ok(())),
            ("*,/remote/broker/logs", Err((codes::INVALID_REQUEST,
                "requirement failed: When cordoned.log.dirs is set to *, it must not contain other values".to_owned()))),
        ];
        for (value, expected) in cases {
            check!(cordoned_log_dirs_error(&map(&[(CORDONED_LOG_DIRS, value)]), &[]) == expected);
        }
    }

    #[test]
    fn broker_resource_names_follow_kafkas_node_id_rule() {
        let serving = NodeId(1);
        let cases = [
            ("", Ok(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID)),
            ("1", Ok(NodeId(1))),
            (
                "2",
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received 2".to_owned(),
                )),
            ),
            (
                "-1",
                Err((
                    codes::INVALID_REQUEST,
                    "Unexpected broker id, expected 1, but received -1".to_owned(),
                )),
            ),
            (
                "abc",
                Err((
                    codes::INVALID_REQUEST,
                    "Node id must be an integer, but it is: abc".to_owned(),
                )),
            ),
        ];
        for (name, want) in cases {
            check!(broker_resource_node(name, serving) == want, "{name:?}");
        }
    }

    #[test]
    fn dynamic_broker_configs_follow_kafkas_validate_configs() {
        let cases = [
            (map(&[("plugin.custom.key", "x")]), false, Ok(())),
            (
                map(&[("log.dirs", "/tmp")]),
                false,
                Err("Cannot update these configs dynamically: [log.dirs]"),
            ),
            (
                map(&[("ssl.keystore.location", "/k")]),
                true,
                Err(
                    "These security configs can be dynamically updated only per-listener using \
                     the listener prefix: [ssl.keystore.location]",
                ),
            ),
            (
                map(&[("listener.name.client.ssl.keystore.location", "/k")]),
                false,
                Err(
                    "Cannot update these configs at default cluster level, broker id must be \
                     specified: [listener.name.client.ssl.keystore.location]",
                ),
            ),
            (
                map(&[("listener.name.client.ssl.keystore.location", "/k")]),
                true,
                Ok(()),
            ),
            (map(&[("log.retention.ms", " 86400000 ")]), false, Ok(())),
            (map(&[("num.io.threads", "16")]), false, Ok(())),
            (
                map(&[(LEADER_THROTTLED_RATE_KEY, "-1")]),
                true,
                Err(
                    "Invalid value -1 for configuration leader.replication.throttled.rate: Value \
                     must be at least 0",
                ),
            ),
            (
                map(&[("min.insync.replicas", "0")]),
                true,
                Err(
                    "Invalid value 0 for configuration min.insync.replicas: Value must be at \
                     least 1",
                ),
            ),
            // Every `KafkaConfig` key that is not dynamic is refused, not only
            // the ones krabka reads at startup.
            (
                map(&[("auto.leader.rebalance.enable", "false")]),
                false,
                Err("Cannot update these configs dynamically: [auto.leader.rebalance.enable]"),
            ),
            (
                map(&[("queued.max.requests", "10")]),
                true,
                Err("Cannot update these configs dynamically: [queued.max.requests]"),
            ),
            (
                map(&[("log.cleaner.enable", "false"), ("num.io.threads", "abc")]),
                false,
                Err("Cannot update these configs dynamically: [log.cleaner.enable]"),
            ),
            (
                map(&[("offsets.topic.replication.factor", "1")]),
                false,
                Err("Cannot update these configs dynamically: [offsets.topic.replication.factor]"),
            ),
            // A dynamic key is parsed and range-checked against its `ConfigDef`.
            (
                map(&[("num.io.threads", "abc")]),
                false,
                Err("Invalid value abc for configuration num.io.threads: Not a number of type INT"),
            ),
            (
                map(&[("max.connections", "-5")]),
                false,
                Err("Invalid value -5 for configuration max.connections: Value must be at least 0"),
            ),
            (
                map(&[("num.network.threads", "0")]),
                false,
                Err(
                    "Invalid value 0 for configuration num.network.threads: Value must be at \
                     least 1",
                ),
            ),
            (
                map(&[("follower.fetch.last.tiered.offset.enable", " TRUE ")]),
                false,
                Ok(()),
            ),
            // A listener override is checked as the key it overrides, under
            // that key's name.
            (
                map(&[("listener.name.external.num.network.threads", "abc")]),
                false,
                Err(
                    "Invalid value abc for configuration num.network.threads: Not a number of \
                     type INT",
                ),
            ),
            // A `PER_BROKER_CONFIGS` key that is not an SSL key is per-broker
            // too.
            (
                map(&[("sasl.enabled.mechanisms", "PLAIN")]),
                false,
                Err(
                    "Cannot update these configs at default cluster level, broker id must be \
                     specified: [sasl.enabled.mechanisms]",
                ),
            ),
            (map(&[("sasl.enabled.mechanisms", "PLAIN")]), true, Ok(())),
        ];
        for (props, per_broker, want) in cases {
            let want = want.map_err(|message| (codes::INVALID_REQUEST, message.to_owned()));
            check!(
                validate_dynamic_broker_configs(&props, per_broker, UnstableApiVersions::Enabled)
                    == want,
                "{props:?} per_broker={per_broker}"
            );
        }
    }

    /// #907: the broker keys of Kafka trunk's topic keys are `KafkaConfig`
    /// keys only on trunk. Under Kafka 4.3.1 they are unknown broker keys,
    /// which `DynamicConfig.Broker.validate` stores unchecked; under
    /// `unstable.api.versions.enable` they carry their topic key's validator.
    #[test]
    fn trunk_topic_keys_broker_synonyms_need_unstable_api_versions() {
        let props = map(&[("log.remote.copy.lag.ms", "-2")]);
        check!(
            validate_dynamic_broker_configs(&props, false, UnstableApiVersions::Disabled) == Ok(())
        );
        check!(
            validate_dynamic_broker_configs(&props, false, UnstableApiVersions::Enabled)
                == Err((
                    codes::INVALID_REQUEST,
                    "Invalid value -2 for configuration log.remote.copy.lag.ms: Value must be at \
                     least -1"
                        .to_owned()
                ))
        );
        let strict: Vec<&str> =
            served_topic_default_synonyms(UnstableApiVersions::Disabled).collect();
        for broker in [
            "log.remote.copy.lag.ms",
            "log.remote.copy.lag.bytes",
            "max.decompressed.message.bytes",
        ] {
            check!(!strict.contains(&broker), "{broker}");
            check!(
                served_topic_default_synonyms(UnstableApiVersions::Enabled)
                    .any(|name| name == broker),
                "{broker}"
            );
            check!(
                broker_key_kind(broker, UnstableApiVersions::Disabled) == BrokerKeyKind::Unknown,
                "{broker}"
            );
        }
    }

    #[test]
    fn broker_key_kinds_decide_append_and_subtract() {
        for (name, kind) in [
            ("log.cleanup.policy", BrokerKeyKind::List),
            ("log.retention.ms", BrokerKeyKind::Scalar),
            (LEADER_THROTTLED_RATE_KEY, BrokerKeyKind::Scalar),
            // Kafka types the operation off `KafkaConfig`'s whole `ConfigDef`,
            // the keys that are not dynamic included.
            ("listeners", BrokerKeyKind::List),
            ("log.dirs", BrokerKeyKind::List),
            ("num.io.threads", BrokerKeyKind::Scalar),
            ("not.a.kafka.key", BrokerKeyKind::Unknown),
        ] {
            check!(
                broker_key_kind(name, UnstableApiVersions::Disabled) == kind,
                "{name}"
            );
        }
    }
}
