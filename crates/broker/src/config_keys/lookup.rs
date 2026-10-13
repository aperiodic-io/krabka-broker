//! The topic-over-broker-default config lookups the resolvers share.
//!
//! Kafka resolves a topic config by asking the topic for an override and
//! falling back to the broker configs of the config schema: this node's own
//! dynamic value, then the cluster-wide one
//! (`KafkaConfigSchema.resolveEffectiveTopicConfig`). Several krabka keys need
//! exactly that -- the two unclean-recovery keys and `min.insync.replicas` --
//! so the walk lives here rather than once per key.

/// The value of `key` for `topic`: the topic override if it has one, else the
/// cluster-wide default broker config, else `None`.
///
/// This is the layering of a resolver that has no node of its own to consult,
/// the controller's. See [`topic_node_or_cluster_default`] for the broker's.
pub(super) fn topic_or_cluster_default<'a>(
    image: &'a krabka_metadata::MetadataImage,
    topic: &str,
    key: &str,
) -> Option<&'a str> {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(key))
        .or_else(|| image.default_broker_config()?.get(key))
        .map(String::as_str)
}

/// The value of `key` for `topic` as `node` sees it: the topic override, else
/// `node`'s own dynamic broker config, else the cluster-wide default broker
/// config, else `None`.
pub(super) fn topic_node_or_cluster_default<'a>(
    image: &'a krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
    topic: &str,
    key: &str,
) -> Option<&'a str> {
    topic_node_or_cluster_synonym(image, node, topic, ConfigSynonym {
        topic_key: key,
        broker_key: key,
    })
}

/// A topic key and the broker key that carries its broker-wide default.
#[derive(Clone, Copy)]
pub(super) struct ConfigSynonym<'a> {
    pub(super) topic_key: &'a str,
    pub(super) broker_key: &'a str,
}

/// [`topic_node_or_cluster_default`] for a topic key whose broker-wide default
/// goes by another name: `topic_key` on the topic, `broker_key` on the node
/// and cluster configs (`remote.copy.lag.ms` and `log.remote.copy.lag.ms`).
pub(super) fn topic_node_or_cluster_synonym<'a>(
    image: &'a krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
    topic: &str,
    ConfigSynonym {
        topic_key,
        broker_key,
    }: ConfigSynonym<'_>,
) -> Option<&'a str> {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(topic_key))
        .or_else(|| image.broker_config(node)?.get(broker_key))
        .or_else(|| image.default_broker_config()?.get(broker_key))
        .map(String::as_str)
}
