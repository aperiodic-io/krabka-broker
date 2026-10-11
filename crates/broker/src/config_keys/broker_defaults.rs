//! The broker-wide defaults of the topic keys, as this broker runs them.
//!
//! A topic key has a broker-wide default under another name (`message.max.bytes`
//! for `max.message.bytes`, `log.segment.bytes` for `segment.bytes`, and so on:
//! [`super::broker_dynamic::TOPIC_DEFAULT_SYNONYMS`]). Each is a dynamic broker
//! config, so `kafka-configs --entity-type brokers` can set it cluster-wide
//! (`--entity-default`) or for one node (`--entity-name N`).
//!
//! Kafka applies such a value at once. `DynamicBrokerConfig.DynamicLogConfig`
//! pushes it into the `LogManager`'s default `LogConfig` and into every live
//! log (`updateLogsConfig`), so the next produce validates against it. A topic
//! key resolves in `KafkaConfigSchema.resolveEffectiveTopicConfig` order: the
//! topic's own override, then this node's dynamic broker config, then the
//! cluster-wide dynamic default, then the static `server.properties`, then the
//! built-in default.
//!
//! This module reads the two dynamic layers out of the metadata image and lays
//! them over the static base the process started with. Both paths that
//! enforce a topic key read the result: the partition `LogConfig` push
//! (`push_topic_configs`), and the produce handler for the keys it checks
//! before a batch reaches the log.

use std::collections::BTreeMap;

use krabka_compression::CompressionType;
use krabka_log::{CleanupPolicy, LogConfig};
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::TimestampType;
use krabka_units::ByteSize;

use super::{
    MESSAGE_TIMESTAMP_AFTER_MAX_MS, MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
    broker_dynamic::TOPIC_DEFAULT_SYNONYMS, log_config::apply_to_log_config, parse::long_value,
};
use crate::config::BrokerConfig;

/// The dynamic broker-wide defaults for `node`, by the topic key each one is
/// the default of: this node's own value where it has one, else the
/// cluster-wide one. A key neither layer holds is absent, which leaves the
/// static default in force.
#[must_use]
pub(crate) fn dynamic_topic_defaults(
    image: &MetadataImage,
    node: NodeId,
) -> BTreeMap<String, String> {
    let per_broker = image.broker_config(node);
    let cluster = image.default_broker_config();
    if per_broker.is_none() && cluster.is_none() {
        return BTreeMap::new();
    }
    TOPIC_DEFAULT_SYNONYMS
        .iter()
        .filter_map(|(broker_key, topic_key)| {
            per_broker
                .and_then(|configs| configs.get(*broker_key))
                .or_else(|| cluster.and_then(|configs| configs.get(*broker_key)))
                .map(|value| ((*topic_key).to_owned(), value.clone()))
        })
        .collect()
}

/// `base` with the dynamic broker-wide defaults for `node` laid over it: the
/// `LogConfig` a partition falls back to for every key its topic does not
/// override.
#[must_use]
pub(crate) fn dynamic_log_base(image: &MetadataImage, node: NodeId, base: &LogConfig) -> LogConfig {
    apply_to_log_config(&dynamic_topic_defaults(image, node), base)
}

/// The broker-wide defaults the produce path checks a batch against, as this
/// node runs them. A topic override still wins over each; see
/// `resolve_max_message_bytes`, `resolve_topic_compression`,
/// `resolve_compacted_topic` and `resolve_timestamp_policy`, which take these
/// as their fallback.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct BrokerLogDefaults {
    /// `message.max.bytes`, the default of `max.message.bytes`.
    pub(crate) max_message_size: ByteSize,
    /// `log.message.timestamp.type`.
    pub(crate) message_timestamp_type: TimestampType,
    /// `log.message.timestamp.before.max.ms`; `None` is `Long.MAX_VALUE`, the
    /// absence of a bound.
    pub(crate) message_timestamp_before_max_ms: Option<i64>,
    /// `log.message.timestamp.after.max.ms`; `None` is `Long.MAX_VALUE`.
    pub(crate) message_timestamp_after_max_ms: Option<i64>,
    /// `log.cleanup.policy`.
    pub(crate) cleanup_policy: CleanupPolicy,
    /// The broker-wide `compression.type`; `None` is Kafka's `producer`, which
    /// keeps whatever codec the producer used.
    pub(crate) compression_type: Option<CompressionType>,
}

impl BrokerLogDefaults {
    /// The defaults `config`'s node runs with: the dynamic layers in `image`
    /// over its static `LogConfig` and timestamp windows.
    #[must_use]
    pub(crate) fn for_broker(image: &MetadataImage, config: &BrokerConfig) -> Self {
        Self::resolve(
            image,
            NodeId(config.node_id.0),
            &config.log_config,
            (
                config.default_message_timestamp_before_max_ms,
                config.default_message_timestamp_after_max_ms,
            ),
        )
    }

    /// The defaults of `node`: the dynamic layers in `image` over the static
    /// `base` and the static timestamp windows the process started with.
    #[must_use]
    pub(crate) fn resolve(
        image: &MetadataImage,
        node: NodeId,
        base: &LogConfig,
        static_timestamp_windows: (Option<i64>, Option<i64>),
    ) -> Self {
        let dynamic = dynamic_topic_defaults(image, node);
        let applied = (!dynamic.is_empty()).then(|| apply_to_log_config(&dynamic, base));
        let log = applied.as_ref().unwrap_or(base);
        // An explicit `Long.MAX_VALUE`, like an unparseable stored value, is no
        // bound, as `resolve_timestamp_policy` reads a topic's own window.
        let window = |key: &str, static_window: Option<i64>| match dynamic.get(key) {
            Some(value) => long_value(value).filter(|ms| *ms != i64::MAX),
            None => static_window,
        };
        Self {
            max_message_size: log.max_message_size,
            message_timestamp_type: log.message_timestamp_type,
            message_timestamp_before_max_ms: window(
                MESSAGE_TIMESTAMP_BEFORE_MAX_MS,
                static_timestamp_windows.0,
            ),
            message_timestamp_after_max_ms: window(
                MESSAGE_TIMESTAMP_AFTER_MAX_MS,
                static_timestamp_windows.1,
            ),
            cleanup_policy: log.cleanup_policy,
            compression_type: log.compression_type,
        }
    }
}

#[cfg(test)]
mod tests;
