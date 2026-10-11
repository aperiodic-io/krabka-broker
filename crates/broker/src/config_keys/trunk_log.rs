//! The resolution of the Kafka trunk topic keys that the data path enforces.
//!
//! `max.decompressed.message.bytes` and `remote.copy.lag.ms` /
//! `remote.copy.lag.bytes` (KIP-1241) exist only on Kafka trunk, so a broker
//! serves them only under `unstable.api.versions.enable`. The callers decide
//! that; this module answers what a served key resolves to. Each is a topic
//! key with a broker-wide default under another name
//! ([`super::broker_dynamic::TOPIC_DEFAULT_SYNONYMS`]) that is a dynamic
//! broker config, so `KafkaConfigSchema.resolveEffectiveTopicConfig` walks the
//! topic override, this node's dynamic config, the cluster-wide default and
//! then Kafka's own default.

use krabka_metadata::{MetadataImage, NodeId};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::{
    MAX_DECOMPRESSED_MESSAGE_BYTES, REMOTE_COPY_LAG_BYTES, REMOTE_COPY_LAG_MS,
    SOFT_MAX_ARRAY_LENGTH,
    lookup::{topic_node_or_cluster_default, topic_node_or_cluster_synonym},
    parse::{int_value, long_value},
};
use crate::api_catalog::UnstableApiVersions;

/// The broker key of `remote.copy.lag.ms`: `RemoteLogManagerConfig.LOG_REMOTE_COPY_LAG_MS_PROP`.
const LOG_REMOTE_COPY_LAG_MS: &str = "log.remote.copy.lag.ms";
/// The broker key of `remote.copy.lag.bytes`.
const LOG_REMOTE_COPY_LAG_BYTES: &str = "log.remote.copy.lag.bytes";

/// The largest decompressed record body `topic` accepts on `node`, or `None`
/// when it accepts any: `max.decompressed.message.bytes` resolved through the
/// layers above, where Kafka's default of `Records.SOFT_MAX_ARRAY_LENGTH` is
/// the largest record a broker could allocate and so no limit at all.
///
/// An unparseable stored value reads as no limit, like the other produce-side
/// config reads: the alter paths refused it, so a string that does not parse
/// here means a damaged metadata image and not an operator's intent.
#[must_use]
pub(crate) fn resolve_max_decompressed_record_bytes(
    image: &MetadataImage,
    node: NodeId,
    topic: &str,
) -> Option<usize> {
    let limit = topic_node_or_cluster_default(image, node, topic, MAX_DECOMPRESSED_MESSAGE_BYTES)
        .and_then(int_value)?;
    if limit >= SOFT_MAX_ARRAY_LENGTH {
        return None;
    }
    usize::try_from(limit).ok()
}

/// The `LogConfig::max_decompressed_record` that `topic` runs with on a broker
/// serving `unstable`: [`resolve_max_decompressed_record_bytes`] under trunk's
/// keys, and no limit under Kafka 4.3.1, which has no such key and reads none
/// of what an operator stores under the name.
///
/// This is the limit of the log's own decompressing reads, compaction and the
/// by-timestamp lookups. `Produce` resolves the same value for its own check.
#[must_use]
pub(crate) fn log_max_decompressed_record(
    image: &MetadataImage,
    node: NodeId,
    topic: &str,
    unstable: UnstableApiVersions,
) -> Option<ByteSize> {
    (unstable == UnstableApiVersions::Enabled)
        .then(|| resolve_max_decompressed_record_bytes(image, node, topic))
        .flatten()
        .map(|limit| ByteSize::from_bytes(limit as u64))
}

/// A topic's configured `remote.copy.lag.ms` and `remote.copy.lag.bytes`, as
/// stored: `0` is "eligible at once", a positive value a lag to wait out, and
/// `-1` a lag derived from the effective local retention. Kafka's defaults are
/// `0` and `-1`, so a topic that sets neither copies every sealed segment as
/// soon as it is sealed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RemoteCopyLag {
    /// `remote.copy.lag.ms`.
    pub(crate) ms: i64,
    /// `remote.copy.lag.bytes`.
    pub(crate) bytes: i64,
}

impl RemoteCopyLag {
    /// Kafka's defaults: copy as soon as a segment is sealed.
    pub(crate) const DEFAULT: Self = Self { ms: 0, bytes: -1 };
}

/// `topic`'s copy lag as `node` resolves it: the topic override, else `node`'s
/// own `log.remote.copy.lag.*`, else the cluster-wide one, else Kafka's
/// defaults. An unparseable value reads as the default.
#[must_use]
pub(crate) fn resolve_remote_copy_lag(
    image: &MetadataImage,
    node: NodeId,
    topic: &str,
) -> RemoteCopyLag {
    let read = |keys: (&str, &str), default: i64| {
        topic_node_or_cluster_synonym(image, node, topic, keys)
            .and_then(long_value)
            .unwrap_or(default)
    };
    RemoteCopyLag {
        ms: read(
            (REMOTE_COPY_LAG_MS, LOG_REMOTE_COPY_LAG_MS),
            RemoteCopyLag::DEFAULT.ms,
        ),
        bytes: read(
            (REMOTE_COPY_LAG_BYTES, LOG_REMOTE_COPY_LAG_BYTES),
            RemoteCopyLag::DEFAULT.bytes,
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, TopicConfigRecord,
    };

    use super::*;
    use crate::test_support::single_partition_topic;

    const NODE: NodeId = NodeId(1);

    /// An image with topic `t`, its overrides, and broker configs given as
    /// `(node, key, value)`.
    fn image(overrides: &[(&str, &str)], broker: &[(NodeId, &str, &str)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(single_partition_topic(
            "t",
            uuid::Uuid::from_u128(1),
        )));
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: overrides
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        }));
        for (node, key, value) in broker {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: *node,
                config_name: (*key).to_owned(),
                config_value: Some((*value).to_owned()),
            }));
        }
        image
    }

    #[test]
    fn a_decompressed_record_limit_resolves_topic_then_node_then_cluster() {
        let key = MAX_DECOMPRESSED_MESSAGE_BYTES;
        let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
        for (label, overrides, broker, expected) in [
            ("nothing set is no limit", vec![], vec![], None),
            (
                "the topic's own limit",
                vec![(key, "512")],
                vec![],
                Some(512),
            ),
            (
                "a cluster default",
                vec![],
                vec![(cluster, key, "1024")],
                Some(1024),
            ),
            (
                "a node beats the cluster",
                vec![],
                vec![(cluster, key, "1024"), (NODE, key, "2048")],
                Some(2048),
            ),
            (
                "another node's is not this node's",
                vec![],
                vec![(NodeId(2), key, "2048")],
                None,
            ),
            (
                "the topic beats both",
                vec![(key, "512")],
                vec![(cluster, key, "1024"), (NODE, key, "2048")],
                Some(512),
            ),
            (
                "Kafka's default is no limit",
                vec![(key, "2147483639")],
                vec![],
                None,
            ),
            (
                "an unparseable value is no limit",
                vec![(key, "many")],
                vec![],
                None,
            ),
        ] {
            check!(
                resolve_max_decompressed_record_bytes(&image(&overrides, &broker), NODE, "t")
                    == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_copy_lag_resolves_topic_then_node_then_cluster_under_the_broker_keys() {
        let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
        for (label, overrides, broker, expected) in [
            (
                "Kafka's defaults",
                vec![],
                vec![],
                RemoteCopyLag { ms: 0, bytes: -1 },
            ),
            (
                "the topic's own lag",
                vec![
                    (REMOTE_COPY_LAG_MS, "60000"),
                    (REMOTE_COPY_LAG_BYTES, "4096"),
                ],
                vec![],
                RemoteCopyLag {
                    ms: 60_000,
                    bytes: 4096,
                },
            ),
            (
                "the broker keys carry a default",
                vec![],
                vec![
                    (cluster, LOG_REMOTE_COPY_LAG_MS, "1000"),
                    (cluster, LOG_REMOTE_COPY_LAG_BYTES, "100"),
                ],
                RemoteCopyLag {
                    ms: 1000,
                    bytes: 100,
                },
            ),
            (
                "a node beats the cluster and the topic beats both, key by key",
                vec![(REMOTE_COPY_LAG_BYTES, "7")],
                vec![
                    (cluster, LOG_REMOTE_COPY_LAG_MS, "1000"),
                    (NODE, LOG_REMOTE_COPY_LAG_MS, "2000"),
                    (NODE, LOG_REMOTE_COPY_LAG_BYTES, "9"),
                ],
                RemoteCopyLag { ms: 2000, bytes: 7 },
            ),
            (
                "an unparseable value is the default",
                vec![(REMOTE_COPY_LAG_MS, "soon")],
                vec![],
                RemoteCopyLag::DEFAULT,
            ),
        ] {
            check!(
                resolve_remote_copy_lag(&image(&overrides, &broker), NODE, "t") == expected,
                "{label}"
            );
        }
    }
}
