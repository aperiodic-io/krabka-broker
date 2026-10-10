//! Propagation of a topic's config overrides from the metadata image onto
//! every locally-hosted partition of that topic.

use std::collections::HashSet;

use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::TopicPartition;
use crate::partition_registry::PartitionRegistry;

/// Push topic-config overrides onto every locally-hosted partition in
/// `desired`. The call is idempotent, because the same `LogConfig` sent twice
/// is a cheap noop write inside `Log::set_config`. Errors on individual
/// partitions log through `warn!` and do not propagate.
///
/// `base` is the static `LogConfig` the process started with. The dynamic
/// broker-wide defaults of `node` and of the cluster go over it first, so a
/// partition whose topic overrides a key gets the override, and every other
/// key gets `kafka-configs --entity-type brokers`' value, the way Kafka's
/// `DynamicLogConfig` rebuilds each log's config when one changes. The
/// reconcile that calls this runs on every metadata image, so a change to a
/// broker config reaches the logs on the next one.
///
/// Kafka trunk's `max.decompressed.message.bytes` reaches the log the same way,
/// resolved per topic through the topic override, the node's dynamic config and
/// the cluster default, and only when `unstable` says the broker serves trunk's
/// topic keys: under Kafka 4.3.1 a stored value of it is an unknown key that
/// nothing reads, so the log runs with no limit.
pub(crate) async fn push_topic_configs(
    desired: &HashSet<TopicPartition>,
    partitions: &PartitionRegistry,
    image: &MetadataImage,
    base: &LogConfig,
    node: krabka_metadata::NodeId,
    unstable: crate::api_catalog::UnstableApiVersions,
) {
    let base = &crate::config_keys::dynamic_log_base(image, node, base);
    let empty: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (topic, partition) in desired {
        let Some(part) = partitions.get(topic, PartitionIndex(*partition)) else {
            continue;
        };
        let overrides = image.topic_config(topic).unwrap_or(&empty);
        // `apply_to_log_config` leaves the field alone, so the topic's own
        // limit travels in the base it merges over.
        let topic_base = LogConfig {
            max_decompressed_record: crate::config_keys::log_max_decompressed_record(
                image, node, topic, unstable,
            ),
            ..base.clone()
        };
        if let Err(e) = part
            .apply_log_config_overrides(overrides, &topic_base)
            .await
        {
            warn!(
                topic = %topic, partition = partition, error = %e,
                "supervisor: apply_log_config_overrides failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord, NodeId,
    };
    use uuid::Uuid;

    use super::*;
    use crate::{
        api_catalog::UnstableApiVersions,
        replicator_supervisor::test_support::{
            await_until, materialized_partition_with_config, single_partition_image,
        },
    };

    #[derive(Clone, Copy)]
    struct PushedConfigSetup<'a> {
        base: &'a LogConfig,
        unstable: UnstableApiVersions,
        what: &'a str,
    }

    impl Default for PushedConfigSetup<'_> {
        fn default() -> Self {
            Self {
                base: crate::replicator_supervisor::test_support::MaterializeSetup::default()
                    .log_config,
                unstable: UnstableApiVersions::Disabled,
                what: "log config updated",
            }
        }
    }

    async fn pushed_config(
        image: &MetadataImage,
        setup: PushedConfigSetup<'_>,
        ready: impl Fn(&LogConfig) -> bool,
    ) -> LogConfig {
        let PushedConfigSetup {
            base,
            unstable,
            what,
        } = setup;
        let (_dir, partitions) = materialized_partition_with_config(base);
        let desired = HashSet::from([("t".to_owned(), 0)]);
        push_topic_configs(&desired, &partitions, image, base, NodeId(1), unstable).await;
        let part = partitions.get("t", PartitionIndex(0)).expect("partition");
        await_until(what, || {
            ready(&part.log.lock().expect("log lock").config_snapshot())
        })
        .await;
        part.log.lock().expect("log lock").config_snapshot()
    }

    #[tokio::test]
    async fn push_topic_configs_pushes_overrides_to_local_partition() {
        // Build an image with a topic + partition record + V1TopicConfig.
        let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
        let mut overrides = BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        img.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides,
            },
        ));

        let base = LogConfig {
            segment_size: krabka_units::mebibytes(1),
            ..LogConfig::default()
        };
        let snap = pushed_config(
            &img,
            PushedConfigSetup {
                base: &base,
                what: "retention.ms=60s applied to partition log",
                ..Default::default()
            },
            |config| config.retention == Some(krabka_units::minutes(1)),
        )
        .await;
        assert!(snap.retention == Some(krabka_units::minutes(1)));
        assert!(snap.segment_size == krabka_units::mebibytes(1));
    }

    /// Kafka trunk's `max.decompressed.message.bytes` reaches the log a
    /// partition runs with, so the cleaner and the by-timestamp lookups read it,
    /// only on a broker serving trunk's topic keys. Under Kafka 4.3.1 the key is
    /// an unknown one whose stored value nothing reads, and the log has no limit.
    /// The limit is the topic's own, else the node's, else the cluster's.
    #[tokio::test]
    async fn push_topic_configs_carries_the_decompressed_record_limit_only_under_trunk() {
        const KEY: &str = "max.decompressed.message.bytes";
        let cluster = DEFAULT_BROKER_CONFIG_NODE_ID;
        let node = NodeId(1);
        // (name, unstable flag, the topic's override, the broker layers, the
        // limit the log runs with).
        for (name, unstable, topic_override, broker, expected) in [
            (
                "trunk, the topic's own",
                UnstableApiVersions::Enabled,
                Some("4096"),
                vec![(cluster, "512")],
                Some(krabka_units::bytes(4096)),
            ),
            (
                "trunk, the cluster default",
                UnstableApiVersions::Enabled,
                None,
                vec![(cluster, "512")],
                Some(krabka_units::bytes(512)),
            ),
            (
                "trunk, the node beats the cluster",
                UnstableApiVersions::Enabled,
                None,
                vec![(cluster, "512"), (node, "2048")],
                Some(krabka_units::bytes(2048)),
            ),
            (
                "trunk, nothing set",
                UnstableApiVersions::Enabled,
                None,
                vec![],
                None,
            ),
            (
                "Kafka 4.3.1 ignores the topic's",
                UnstableApiVersions::Disabled,
                Some("4096"),
                vec![],
                None,
            ),
            (
                "Kafka 4.3.1 ignores the cluster's",
                UnstableApiVersions::Disabled,
                None,
                vec![(cluster, "512")],
                None,
            ),
        ] {
            let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
            for (node_id, value) in broker {
                img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id,
                    config_name: KEY.into(),
                    config_value: Some(value.into()),
                }));
            }
            // A retention override rides along so the wait below can tell the
            // push has landed even when the limit stays unset.
            let mut overrides = BTreeMap::from([("retention.ms".to_string(), "60000".to_string())]);
            if let Some(value) = topic_override {
                overrides.insert(KEY.to_string(), value.to_string());
            }
            img.apply(&MetadataRecord::V1TopicConfig(
                krabka_metadata::TopicConfigRecord {
                    topic: "t".into(),
                    overrides,
                },
            ));

            let snap = pushed_config(
                &img,
                PushedConfigSetup {
                    unstable,
                    what: "the push reached the partition log",
                    ..Default::default()
                },
                |config| config.retention == Some(krabka_units::minutes(1)),
            )
            .await;
            assert!(snap.max_decompressed_record == expected, "{name}");
        }
    }

    #[tokio::test]
    async fn push_topic_configs_with_no_overrides_uses_defaults() {
        let img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));

        // No overrides → the writer retains the default log config.
        let snap = pushed_config(
            &img,
            PushedConfigSetup {
                what: "default retention applied to partition log",
                ..Default::default()
            },
            |config| config.retention == LogConfig::default().retention,
        )
        .await;
        assert!(snap.retention == LogConfig::default().retention);
    }

    /// Kafka's `DynamicLogConfig` rebuilds every live log's config from the
    /// broker's dynamic defaults. A key the topic does not override takes the
    /// cluster-wide default (`--entity-default`), a node's own value beats it,
    /// and a topic override beats both.
    #[tokio::test]
    async fn push_topic_configs_applies_the_dynamic_broker_defaults() {
        let mut img = single_partition_image("t", Uuid::new_v4(), krabka_raft::NodeId(1));
        for (node_id, name, value) in [
            (DEFAULT_BROKER_CONFIG_NODE_ID, "log.retention.ms", "120000"),
            (
                DEFAULT_BROKER_CONFIG_NODE_ID,
                "message.max.bytes",
                "2097152",
            ),
            (NodeId(1), "message.max.bytes", "4194304"),
            (
                DEFAULT_BROKER_CONFIG_NODE_ID,
                "log.segment.bytes",
                "2097152",
            ),
        ] {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: name.into(),
                config_value: Some(value.into()),
            }));
        }
        // The topic overrides the segment size and nothing else.
        img.apply(&MetadataRecord::V1TopicConfig(
            krabka_metadata::TopicConfigRecord {
                topic: "t".into(),
                overrides: BTreeMap::from([("segment.bytes".to_string(), "1048576".to_string())]),
            },
        ));

        let snap = pushed_config(
            &img,
            PushedConfigSetup {
                what: "dynamic broker defaults applied to the partition log",
                ..Default::default()
            },
            |config| config.max_message_size == krabka_units::bytes(4_194_304),
        )
        .await;
        // The cluster default, the node's own override over it, and the
        // topic's override over the cluster default.
        assert!(snap.retention == Some(krabka_units::minutes(2)));
        assert!(snap.max_message_size == krabka_units::bytes(4_194_304));
        assert!(snap.segment_size == krabka_units::mebibytes(1));
    }
}
