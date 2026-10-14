//! Resolving one `DescribeConfigs` resource entry into the configs the broker
//! reports for it.
//!
//! This is the whole source-precedence decision, one resource type at a time:
//! a topic's effective configuration, a broker's per-node override over the
//! cluster default plus its static `node.id` and idle window, a client-metrics
//! subscription's effective values, and a group's dynamic overrides over the
//! values the coordinators run with. Every other resource type gets an empty
//! configs list and no error.
//!
//! A topic reports every key in the registry, not only the ones it overrides.
//! `kafka-configs --describe --all` and `AdminClient.describeConfigs` exist to
//! show effective configuration and where each value came from, and a response
//! that holds only the overrides answers neither question. The layer each
//! value came from travels with it, as the synonym chain
//! [`super::entry::config_entry`] builds.

use {
    super::{
        entry::{DefaultLayer, EntryOptions, Layer, config_entry},
        static_configs::idle_window_entries,
        wire::{
            CONFIG_SOURCE_CLIENT_METRICS, CONFIG_SOURCE_DYNAMIC_BROKER,
            CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER, CONFIG_SOURCE_DYNAMIC_GROUP,
            CONFIG_SOURCE_DYNAMIC_TOPIC, CONFIG_SOURCE_STATIC_BROKER, RESOURCE_TYPE_BROKER,
            RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP,
            RESOURCE_TYPE_TOPIC,
        },
    },
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::{
            self, broker_dynamic, kafka_broker,
            registry::{self, ConfigScope, NODE_ID},
        },
    },
    krabka_protocol::owned::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    },
};

mod broker_logger;
mod static_broker;
mod write_freeze;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use self::static_broker::kafka_default_static_broker;
pub(crate) use self::static_broker::static_settings;
pub(super) use self::{
    broker_logger::BrokerLoggers,
    static_broker::{StaticBrokerConfigs, StaticBrokerSetting},
};
use self::{static_broker::static_broker_entries, write_freeze::write_freeze_override};

/// What a broker-scoped resource reports out of the serving process itself
/// rather than out of the metadata image.
///
/// The three travel together because they answer the same question from three
/// angles -- which node is replying, what it was started with, and what it is
/// logging right now -- and because a `BROKER` or `BROKER_LOGGER` resource is
/// meaningless without the node that serves it.
#[derive(Clone, Copy)]
pub(super) struct ServingBroker<'a> {
    /// The node answering the request. A `BROKER` resource that names any
    /// other node is refused.
    pub(super) node: krabka_metadata::NodeId,
    /// The static broker keys this node was started with, which a named
    /// `BROKER` resource reports beside its dynamic overrides.
    pub(super) static_broker: StaticBrokerConfigs<'a>,
    /// This node's live log levels, and the broker id a `BROKER_LOGGER`
    /// resource must name.
    pub(super) loggers: BrokerLoggers<'a>,
    /// Kafka's `unstable.api.versions.enable`, which decides whether a group
    /// resource carries Kafka trunk's group keys, a topic resource Kafka
    /// trunk's topic keys, and a broker resource their broker synonyms.
    pub(super) unstable_api_versions: UnstableApiVersions,
}

/// Dispatches one resource entry from a `DescribeConfigs` request.
pub(super) fn describe_one(
    image: &krabka_metadata::MetadataImage,
    r: &krabka_protocol::owned::describe_configs_request::DescribeConfigsResource,
    serving: ServingBroker<'_>,
    client_metrics_default_interval_ms: i32,
    options: EntryOptions,
) -> DescribeConfigsResult {
    let ok = |configs| DescribeConfigsResult {
        error_code: codes::NONE,
        error_message: None,
        resource_type: r.resource_type,
        resource_name: r.resource_name.clone(),
        configs,
        ..Default::default()
    };
    // An empty key list asks for every key, not for none: Kafka's
    // `ConfigHelperUtils.toDescribeConfigsResult` filters with
    // `keys == null || keys.isEmpty() || keys.contains(name)`.
    let key_filter: Option<&[String]> = r
        .configuration_keys
        .as_deref()
        .filter(|keys| !keys.is_empty());
    let wanted = |key: &str| key_filter.is_none_or(|keys| keys.iter().any(|f| f == key));
    // Kafka's `ConfigHelper.describeConfigs` turns each exception it throws
    // for one resource into that resource's error, with no configs.
    let failed = |error_code: i16, error_message: Option<String>| DescribeConfigsResult {
        error_code,
        error_message,
        resource_type: r.resource_type,
        resource_name: r.resource_name.clone(),
        configs: Vec::new(),
        ..Default::default()
    };

    if r.resource_type == RESOURCE_TYPE_TOPIC {
        // `Topic.validate` first, then `metadataCache.contains`: a name Kafka
        // refuses is INVALID_TOPIC_EXCEPTION, and a topic that does not exist
        // is UNKNOWN_TOPIC_OR_PARTITION with no message, never a list of
        // defaults that look real.
        if let Err(invalid) = krabka_log::topic_name::validate_topic_name(&r.resource_name) {
            return failed(codes::INVALID_TOPIC_EXCEPTION, Some(invalid.to_string()));
        }
        if image.topic(&r.resource_name).is_none() {
            return failed(codes::UNKNOWN_TOPIC_OR_PARTITION, None);
        }
        let broker = TopicBrokerLayers {
            node: serving.node,
            statics: serving.static_broker.settings,
            unstable: serving.unstable_api_versions,
        };
        return ok(topic_configs(
            image,
            &r.resource_name,
            broker,
            &wanted,
            options,
        ));
    }

    if r.resource_type == RESOURCE_TYPE_BROKER {
        let node_id = if r.resource_name.is_empty() {
            None
        } else {
            // Kafka's `resourceNameToBrokerId` is `String.toInt`, so `-1` is
            // an integer that names some other node.
            let Ok(node_id) = r.resource_name.parse::<i32>() else {
                return failed(
                    codes::INVALID_REQUEST,
                    Some(format!(
                        "Broker id must be an integer, but it is: {}",
                        r.resource_name
                    )),
                );
            };
            let node_id = u64::try_from(node_id)
                .map_or(None, |id| Some(krabka_metadata::NodeId(id)))
                .filter(|id| *id == serving.node);
            let Some(node_id) = node_id else {
                return failed(
                    codes::INVALID_REQUEST,
                    Some(format!(
                        "Unexpected broker id, expected {} or empty string, but received {}",
                        serving.node.0, r.resource_name
                    )),
                );
            };
            // A broker resource is answered from the serving process's own
            // configuration, so Kafka refuses to answer for any other node.
            Some(node_id)
        };
        return ok(broker_configs(image, node_id, serving, &wanted, options));
    }

    if r.resource_type == RESOURCE_TYPE_BROKER_LOGGER {
        if let Err(message) =
            broker_logger::validate_resource_name(&r.resource_name, serving.loggers.node_id)
        {
            return failed(codes::INVALID_REQUEST, Some(message));
        }
        return ok(broker_logger::logger_configs(
            serving.loggers.levels,
            &wanted,
        ));
    }

    if r.resource_type == RESOURCE_TYPE_CLIENT_METRICS {
        if r.resource_name.is_empty() {
            return failed(
                codes::INVALID_REQUEST,
                Some("Client metrics subscription name must not be empty".into()),
            );
        }
        return ok(client_metrics_configs(
            image,
            &r.resource_name,
            client_metrics_default_interval_ms,
            &wanted,
            options,
        ));
    }

    if r.resource_type == RESOURCE_TYPE_GROUP {
        if r.resource_name.is_empty() {
            return failed(
                codes::INVALID_REQUEST,
                Some("Group name must not be empty".into()),
            );
        }
        return ok(group_configs(
            image,
            &r.resource_name,
            GroupServing {
                statics: serving.static_broker.settings,
                unstable: serving.unstable_api_versions,
            },
            &wanted,
            options,
        ));
    }

    // `handle` refuses a request naming any other type before it gets here;
    // this is Kafka's own last `case` in `ConfigHelper.describeConfigs`.
    failed(
        codes::INVALID_REQUEST,
        Some(format!("Unsupported resource type: {}", r.resource_type)),
    )
}

/// A topic's whole effective configuration, for a caller outside
/// `DescribeConfigs`.
///
/// KIP-525 puts the same values on a `CreateTopics` v5+ response, so a client
/// that has just created a topic needs no follow-up `DescribeConfigs` to learn
/// what it created. Kafka's controller calls
/// `ConfigurationControlManager.computeEffectiveTopicConfigs` from both paths;
/// this is the one computation krabka answers both with. The synonym chain and
/// the documentation are left out because `CreatableTopicConfigs` has no
/// fields for them.
/// A topic's whole effective configuration, resolved against a caller-supplied
/// override map rather than the stored one.
///
/// `CreateTopics` (KIP-525) is the caller, and the map it passes is the one the
/// create writes. That is also the answer for a `validateOnly` request, where
/// the topic never reaches the image: Kafka builds the same row from
/// `computeEffectiveTopicConfigs(creationConfigs)` and discards only the
/// *records*, so a dry run reports what the topic would be created as instead
/// of the bare cluster defaults a lookup of an absent topic would give.
///
/// `node` is the node computing the answer and `statics` its static settings.
/// Kafka's `ConfigurationControlManager.computeEffectiveTopicConfigs` hands
/// `resolveEffectiveTopicConfigs` the active controller's own per-node
/// dynamic configs (`currentControllerConfig()`) and its `staticNodeConfig`,
/// so a key's broker synonym reports at `DYNAMIC_BROKER_CONFIG` when `node`
/// sets it and at `STATIC_BROKER_CONFIG` when `statics` does. `unstable` is
/// Kafka's `unstable.api.versions.enable`, which decides whether Kafka
/// trunk's topic keys are on the row.
pub(crate) fn effective_topic_configs(
    image: &krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
    topic: &str,
    overrides: &std::collections::BTreeMap<String, String>,
    unstable: UnstableApiVersions,
    statics: &std::collections::BTreeMap<&'static str, String>,
) -> Vec<DescribeConfigsResourceResult> {
    topic_configs_with_overrides(
        image,
        TopicOverrides {
            topic,
            overrides: Some(overrides),
        },
        TopicBrokerLayers {
            node,
            statics,
            unstable,
        },
        &|_| true,
        EntryOptions {
            include_synonyms: false,
            include_documentation: false,
        },
    )
}

/// A topic's effective configuration: every registry key, with the layer its
/// value came from.
///
/// `write.freeze` is the one key that is never stored. It lives in the freeze
/// registry, and [`write_freeze_override`] reads it from there.
fn topic_configs(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    broker: TopicBrokerLayers<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    topic_configs_with_overrides(
        image,
        TopicOverrides {
            topic,
            overrides: image.topic_config(topic),
        },
        broker,
        wanted,
        options,
    )
}

/// The serving broker's own layers of a topic key's synonym chain: its
/// per-broker dynamic configs and its static settings.
#[derive(Debug, Clone, Copy)]
struct TopicBrokerLayers<'a> {
    /// The node whose per-broker dynamic configs are a layer: the one that
    /// answers a `DescribeConfigs`, or the controller that computes a
    /// `CreateTopics` row.
    node: krabka_metadata::NodeId,
    /// The static settings of the answering node, by broker key: a topic key
    /// reports its broker synonym here at `STATIC_BROKER_CONFIG`.
    statics: &'a std::collections::BTreeMap<&'static str, String>,
    unstable: UnstableApiVersions,
}

/// A topic and the config overrides stored for it, if any.
struct TopicOverrides<'a> {
    topic: &'a str,
    overrides: Option<&'a std::collections::BTreeMap<String, String>>,
}

/// [`topic_configs`] against a caller-supplied override map, which is what a
/// not-yet-committed topic has instead of a stored one.
///
/// Each key's chain is Kafka's `ConfigHelper.createTopicConfigEntry`: the
/// topic override, then every broker synonym of the key (the serving
/// broker's `DYNAMIC_BROKER_CONFIG`, the `DYNAMIC_DEFAULT_BROKER_CONFIG`, the
/// `STATIC_BROKER_CONFIG` and the `DEFAULT_CONFIG`), each under the broker
/// key's name. The value and `config_source` are the head of that chain.
///
/// The keys are the ones the broker serves under `unstable`: Kafka 4.3.1's
/// `LogConfig` and krabka's own by default, and Kafka trunk's too under
/// `unstable.api.versions.enable`.
fn topic_configs_with_overrides(
    image: &krabka_metadata::MetadataImage,
    TopicOverrides { topic, overrides }: TopicOverrides<'_>,
    broker: TopicBrokerLayers<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let unstable = broker.unstable;
    let cluster_defaults = image.default_broker_config();
    let per_broker = image.broker_config(broker.node);
    let freeze = write_freeze_override(image, topic);

    let mut configs: Vec<DescribeConfigsResourceResult> = registry::keys_in(ConfigScope::Topic)
        .filter(|row| wanted(row.name) && config_keys::serves_topic_key(row.name, unstable))
        // Kafka hides an internal key unless the topic itself sets it.
        .filter(|row| {
            !row.internal || overrides.is_some_and(|configs| configs.contains_key(row.name))
        })
        .map(|row| {
            let stored = if row.name == config_keys::WRITE_FREEZE {
                freeze.as_deref()
            } else {
                overrides
                    .and_then(|configs| configs.get(row.name))
                    .map(String::as_str)
            };
            let mut layers = Vec::with_capacity(4);
            if let Some(value) = stored {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_TOPIC,
                    name: row.name,
                    value,
                });
            }
            let synonyms = broker_dynamic::topic_broker_synonyms(row.name);
            if synonyms.is_empty() {
                // A krabka key with no Kafka broker synonym may still fall
                // back to a cluster-default broker config of its own.
                if let Some(cluster_default) = row.cluster_default
                    && let Some(value) = cluster_defaults
                        .and_then(|configs| configs.get(cluster_default))
                        .map(String::as_str)
                {
                    layers.push(Layer {
                        source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                        name: cluster_default,
                        value,
                    });
                }
                return config_entry(
                    Some(row),
                    row.name,
                    &layers,
                    DefaultLayer {
                        value: row.default,
                        name: row.cluster_default,
                    },
                    options,
                );
            }
            for (map, source) in [
                (per_broker, CONFIG_SOURCE_DYNAMIC_BROKER),
                (cluster_defaults, CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER),
            ] {
                for synonym in &synonyms {
                    if let Some(value) = map.and_then(|configs| configs.get(synonym.name)) {
                        layers.push(Layer {
                            source,
                            name: synonym.name,
                            value,
                        });
                    }
                }
            }
            // The static layer is provenance: Kafka reports a synonym the
            // node's `server.properties` names, whatever its value is.
            for synonym in &synonyms {
                if let Some(value) = broker.statics.get(synonym.name) {
                    layers.push(Layer {
                        source: CONFIG_SOURCE_STATIC_BROKER,
                        name: synonym.name,
                        value,
                    });
                }
            }
            let default = synonyms
                .iter()
                .find_map(|synonym| synonym.default.map(|value| (synonym.name, value)));
            let mut entry = config_entry(
                Some(row),
                row.name,
                &layers,
                DefaultLayer {
                    value: default.map(|(_, value)| value),
                    name: default.map(|(name, _)| name),
                },
                options,
            );
            // The default layer may sit under a broker key of another unit,
            // such as `log.retention.hours` beneath `retention.ms`; the entry
            // still reports the topic key's own default.
            if layers.is_empty() {
                entry.value = row.default.map(str::to_owned);
            }
            entry
        })
        .collect();
    configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    configs
}

/// A broker resource: the dynamic overrides it holds, plus the static
/// `node.id` and the static configs in [`super::static_configs`] when the
/// request named one node.
///
/// An empty resource name is Kafka's cluster-wide default resource, which
/// reports the cluster defaults alone. Only keys that hold a value are
/// reported, which is what a Kafka broker does for this resource type.
///
/// A *named* node reports more: `node.id` and the static settings in
/// [`static_broker_entries`], both read from the running process rather than
/// from the metadata image, the way Kafka answers `--describe --all` out of a
/// node's own `server.properties`. That is why [`describe_one`] refuses a
/// name that is not the serving node: those values belong to this process
/// alone, and reporting them under another node's id would label one broker's
/// static configuration as another's.
fn broker_configs(
    image: &krabka_metadata::MetadataImage,
    node_id: Option<krabka_metadata::NodeId>,
    serving: ServingBroker<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let ServingBroker {
        static_broker,
        unstable_api_versions: unstable,
        ..
    } = serving;
    let defaults = image.default_broker_config();
    let per_broker = node_id.and_then(|node_id| image.broker_config(node_id));
    let mut keys: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    keys.extend(
        defaults
            .into_iter()
            .flat_map(std::collections::BTreeMap::keys)
            .map(String::as_str),
    );
    keys.extend(
        per_broker
            .into_iter()
            .flat_map(std::collections::BTreeMap::keys)
            .map(String::as_str),
    );
    // A named broker reports every non-internal `KafkaConfig` key, set or
    // not (`ConfigHelperUtils.createResponseConfig` walks
    // `config.nonInternalValues()`), and every topic-default broker key it
    // serves. Another emitter below answers the keys the process synthesises.
    if node_id.is_some() {
        for name in broker_dynamic::served_topic_default_synonyms(unstable) {
            keys.insert(name);
        }
        keys.extend(
            kafka_broker::KAFKA_BROKER_CONFIGS
                .iter()
                .filter(|row| !row.internal && !EMITTED_ELSEWHERE.contains(&row.name))
                .map(|row| row.name),
        );
    }

    let mut configs: Vec<DescribeConfigsResourceResult> = keys
        .into_iter()
        .filter(|key| wanted(key))
        .map(|key| {
            let mut layers = Vec::with_capacity(3);
            if let Some(value) = per_broker
                .and_then(|configs| configs.get(key))
                .map(String::as_str)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_BROKER,
                    name: key,
                    value,
                });
            }
            if let Some(value) = defaults
                .and_then(|configs| configs.get(key))
                .map(String::as_str)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                    name: key,
                    value,
                });
            }
            if node_id.is_some()
                && let Some(value) = static_broker.settings.get(key)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_STATIC_BROKER,
                    name: key,
                    value,
                });
            }
            let row = broker_dynamic::broker_key_row(key, unstable);
            // Kafka filters the cluster-default resource's chain to its
            // `DYNAMIC_DEFAULT_BROKER_CONFIG` entries, so only a named broker
            // shows the default beneath them.
            let default = if node_id.is_some() {
                broker_default(key, unstable).map_or_default(|value| DefaultLayer {
                    value: Some(value),
                    name: Some(key),
                })
            } else {
                DefaultLayer::default()
            };
            let mut entry = config_entry(row.as_ref(), key, &layers, default, options);
            // Kafka reads `readOnly` off `ALL_DYNAMIC_CONFIGS.contains(name)`,
            // so a `listener.name.<listener>.` override, which is not a member
            // of it, reads back read-only whatever the key it overrides is.
            entry.read_only |= config_keys::is_controller_managed_broker_config(key)
                || kafka_broker::listener_override_base(key).is_some();
            entry
        })
        .collect();

    if let Some(node_id) = node_id {
        if wanted(NODE_ID) {
            let value = node_id.to_string();
            configs.push(config_entry(
                registry::lookup(ConfigScope::Broker, NODE_ID),
                NODE_ID,
                &[Layer {
                    source: CONFIG_SOURCE_STATIC_BROKER,
                    name: NODE_ID,
                    value: &value,
                }],
                DefaultLayer::default(),
                options,
            ));
        }
        configs.extend(static_broker_entries(static_broker, wanted, options));
        configs.extend(idle_window_entries(static_broker, wanted, options));
        configs.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    }
    configs
}

/// The keys a named broker resource has another emitter answer: the process
/// synthesises them, out of [`StaticBrokerConfigs`] and `node.id`, rather than
/// out of the metadata image or the roster.
const EMITTED_ELSEWHERE: &[&str] = &[
    NODE_ID,
    config_keys::TRANSACTIONAL_ID_EXPIRATION_MS,
    config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS,
    config_keys::OFFSETS_RETENTION_MINUTES,
    config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS,
    config_keys::NUM_PARTITIONS,
    config_keys::DEFAULT_REPLICATION_FACTOR,
    config_keys::DELETE_TOPIC_ENABLE,
    config_keys::AUTO_CREATE_TOPICS_ENABLE,
    config_keys::CONNECTIONS_MAX_IDLE_MS,
];

/// Kafka's `KafkaConfig` default of a broker key krabka reports: its own
/// registry row's, the one its topic-key synonym family gives it on a broker
/// serving `unstable`, or the roster's.
fn broker_default(key: &str, unstable: UnstableApiVersions) -> Option<&'static str> {
    if let Some(row) = registry::lookup(ConfigScope::Broker, key) {
        return row.default;
    }
    let synonym_of = broker_dynamic::TOPIC_DEFAULT_SYNONYMS
        .iter()
        .find(|(broker, topic)| *broker == key && config_keys::serves_topic_key(topic, unstable));
    match synonym_of {
        Some((_, topic)) => broker_dynamic::topic_broker_synonyms(topic)
            .into_iter()
            .find(|synonym| synonym.name == key)
            .and_then(|synonym| synonym.default),
        None => kafka_broker::lookup(key).and_then(|row| row.default),
    }
}

/// A KIP-714 client-metrics subscription: all three keys, with the ones the
/// subscription does not set reported at their default (KAFKA-17516 — tooling
/// needs effective values, not blanks).
fn client_metrics_configs(
    image: &krabka_metadata::MetadataImage,
    subscription: &str,
    default_interval_ms: i32,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    use crate::client_metrics::config::{KEY_INTERVAL_MS, KEY_MATCH, KEY_METRICS};

    let overrides = image
        .client_metrics_config(subscription)
        .cloned()
        .unwrap_or_default();
    let default_interval = default_interval_ms.to_string();

    [KEY_METRICS, KEY_INTERVAL_MS, KEY_MATCH]
        .into_iter()
        .filter(|key| wanted(key))
        .map(|key| {
            let row = registry::lookup(ConfigScope::ClientMetrics, key);
            let default = if key == KEY_INTERVAL_MS {
                Some(default_interval.as_str())
            } else {
                row.and_then(|row| row.default)
            };
            let layers: Vec<Layer<'_>> = overrides
                .get(key)
                .map(|value| Layer {
                    source: CONFIG_SOURCE_CLIENT_METRICS,
                    name: key,
                    value,
                })
                .into_iter()
                .collect();
            config_entry(
                row,
                key,
                &layers,
                DefaultLayer {
                    value: default,
                    name: None,
                },
                options,
            )
        })
        .collect()
}

/// The node's static settings and the API-version stability that decide which
/// group keys a node serves.
struct GroupServing<'a> {
    statics: &'a std::collections::BTreeMap<&'static str, String>,
    unstable: UnstableApiVersions,
}

/// A group resource: every key of Kafka's `GroupConfig` the broker serves --
/// 4.3.1's by default, trunk's under `unstable.api.versions.enable` -- with
/// the group's override above the value this broker runs the group with.
///
/// The chain is Kafka's `ConfigHelper.createGroupConfigEntry`: the override
/// at `DYNAMIC_GROUP_CONFIG`, then the key's broker synonym, at
/// `STATIC_BROKER_CONFIG` when `statics` states it and at `DEFAULT_CONFIG`
/// otherwise, or, for a key with no broker synonym, a `DEFAULT_CONFIG` entry
/// under the group key's own name.
///
/// `statics` are the node's static settings, [`static_settings`], which state
/// a broker synonym when the operator named it or the coordinator runs a
/// value other than Kafka's default, and carry the value the coordinator runs
/// with: the `consumer.*` keys are the settings of the consumer coordinator's
/// `NextGenConfig`, the `share.*` keys those of `ShareGroupConfig`, and the
/// `streams.*` keys those of `StreamsGroupConfig`. This is
/// `extractGroupConfigMap`: the broker's value is every group's default.
fn group_configs(
    image: &krabka_metadata::MetadataImage,
    group: &str,
    GroupServing { statics, unstable }: GroupServing<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let overrides = image.group_config(group).cloned().unwrap_or_default();

    config_keys::group::served_group_keys(unstable)
        .filter(|key| wanted(key.name))
        .map(|key| {
            let own_row = registry::lookup(ConfigScope::Group, key.name);
            let row = own_row
                .copied()
                .unwrap_or_else(|| config_keys::group::group_row(key));
            let mut layers: Vec<Layer<'_>> = overrides
                .get(key.name)
                .map(|value| Layer {
                    source: CONFIG_SOURCE_DYNAMIC_GROUP,
                    name: key.name,
                    value,
                })
                .into_iter()
                .collect();
            let broker_name = key.broker_synonym.unwrap_or(key.name);
            if let Some(synonym) = key.broker_synonym
                && let Some(value) = statics.get(synonym)
            {
                layers.push(Layer {
                    source: CONFIG_SOURCE_STATIC_BROKER,
                    name: synonym,
                    value,
                });
            }
            let mut entry = config_entry(
                Some(&row),
                key.name,
                &layers,
                DefaultLayer {
                    value: key.default,
                    name: key.default.map(|_| broker_name),
                },
                options,
            );
            entry.read_only = false;
            entry
        })
        .collect()
}
