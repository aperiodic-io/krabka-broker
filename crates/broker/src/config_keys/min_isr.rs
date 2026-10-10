//! The resolution of `min.insync.replicas` that the controller and the
//! produce path share.
//!
//! Kafka resolves a topic's `min.insync.replicas` through one layered lookup
//! -- `KafkaConfigSchema.resolveEffectiveTopicConfigs(staticNodeConfig,
//! dynamicClusterConfigs, dynamicNodeConfigs, dynamicTopicConfigs)` -- and
//! both halves of KIP-966 read it through that: the controller's
//! `ReplicationControlManager.getTopicEffectiveMinIsr` calls
//! `ConfigurationControlManager.getTopicConfig`, and the broker's produce
//! gate reads the same value off the partition's `LogConfig`. The two
//! agreeing is load-bearing. ELR names the replicas that are still known to
//! hold every committed record, and what is committed is exactly what the
//! produce gate accepted, so a controller that maintained ELR against a
//! higher threshold than the gate enforced would keep naming a replica that
//! writes had already moved past.
//!
//! Kafka does not leave the agreement to chance. Reconstructed from
//! `kafka-metadata-4.3.1.jar`, `ConfigurationControlManager` refuses two
//! alterations outright while the ELR feature is enabled:
//! `isDisallowedBrokerMinIsrTransition` rejects any *per-node*
//! `min.insync.replicas`, and `isDisallowedClusterMinIsrTransition` rejects
//! *removing* the cluster-wide one -- removal would drop resolution back to
//! each node's static config, which the controller cannot see.
//!
//! Both krabka paths walk the same layers, in Kafka's order
//! (`KafkaConfigSchema.resolveEffectiveTopicConfig`): the topic override,
//! then a per-node dynamic value, then the cluster-wide dynamic broker
//! default. [`node_min_insync_replicas`] is the broker's, which knows its own
//! node and so reads the per-node layer (`kafka-configs --entity-type brokers
//! --entity-name N`). The controller's [`effective_min_insync_replicas`] reads
//! the topic and the cluster layer only, and that costs nothing: a per-node
//! `min.insync.replicas` cannot exist while the ELR is on. The alter paths
//! refuse to write one ([`super::broker_dynamic::elr_min_isr_error`]) and
//! enabling the feature deletes the ones already there
//! (`maybeGenerateElrSafetyRecords`), and the ELR is the only thing the
//! controller resolves min ISR for. The one layer that stays split is the last
//! one: the broker falls back to its own `default_min_insync_replicas`
//! command-line value, and the controller cannot, because the answer decides
//! what it writes into the metadata log and a value that lives on one node's
//! command line is not what another node would compute from the same image.
//! So the controller falls back to Kafka's own default of 1.
//!
//! That residue can only run one way. The broker's fallback is used only
//! when no dynamic layer names a value, where the controller resolves 1, and
//! a broker default is at least 1; every other case resolves the same number
//! on both sides, modulo the replication-factor cap the controller applies
//! and Kafka applies with it. So the controller's threshold is never above
//! the gate's, and the rule it drives -- clear the ELR once the ISR reaches
//! min ISR -- can only clear the set early. It cannot leave a replica in it
//! that an accepted write has moved past.

use super::{
    MIN_INSYNC_REPLICAS,
    lookup::{topic_node_or_cluster_default, topic_or_cluster_default},
};

pub(crate) fn clear_elr_records(
    image: &krabka_metadata::MetadataImage,
    topic: Option<&str>,
) -> Vec<krabka_metadata::MetadataRecord> {
    if !crate::features::feature_enabled(image, crate::features::ELR_VERSION, 1) {
        return Vec::new();
    }
    crate::elr::unclean_restart::clear_published_elr_for_topic(image, topic)
}

/// Apache Kafka's `min.insync.replicas` default, used when neither the topic
/// nor the cluster-wide broker config names one.
const KAFKA_DEFAULT_MIN_INSYNC_REPLICAS: usize = 1;

/// The `min.insync.replicas` the metadata image names for `topic` as broker
/// `node` resolves it: the topic override, else `node`'s own dynamic broker
/// config, else the cluster-wide dynamic broker default, else `None`.
///
/// The produce gate, the ISR maintenance loop and the partition-health gauges
/// resolve through this, and the controller's ELR threshold through
/// [`effective_min_insync_replicas`], so that the two KIP-966 halves agree.
/// `None` leaves each caller its own last resort: the broker's command-line
/// default here, Kafka's own default of 1 on the controller side. See the
/// module docs for why that residue is safe.
///
/// An unparseable value reads as `None`. The alter paths reject those, so a
/// string here that does not parse means a corrupt metadata image, and every
/// caller would rather fall back than fail the request.
pub(crate) fn node_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    node: krabka_metadata::NodeId,
    topic: &str,
) -> Option<i32> {
    super::parse::int_value(topic_node_or_cluster_default(
        image,
        node,
        topic,
        MIN_INSYNC_REPLICAS,
    )?)
}

/// The effective `min.insync.replicas` of one partition, as Kafka's
/// `ReplicationControlManager.getTopicEffectiveMinIsr` computes it: the
/// resolved config value, capped by the replication factor.
///
/// The cap is what makes the KIP-966 rule "ELR is empty while the ISR is at
/// or above min ISR" reachable on a topic whose `min.insync.replicas` exceeds
/// its replication factor. Without it such a topic could never leave the
/// below-min state and every ISR change would move replicas into the ELR.
///
/// Kafka reads the replica count of partition 0 and applies it to the whole
/// topic; krabka passes the replica count of the partition it is deciding,
/// which is the same number on a topic whose partitions share a replication
/// factor.
pub(crate) fn effective_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    replication_factor: usize,
) -> usize {
    let configured = topic_or_cluster_default(image, topic, MIN_INSYNC_REPLICAS)
        .and_then(super::parse::int_value)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(KAFKA_DEFAULT_MIN_INSYNC_REPLICAS);
    configured.min(replication_factor)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord, NodeId,
        PartitionElrRecord, PartitionRecord, TopicConfigRecord,
    };

    use super::*;
    use crate::test_support::TopicSetup;

    fn image(topic_override: Option<&str>, cluster_default: Option<&str>) -> MetadataImage {
        let mut image = crate::test_support::topic_image(TopicSetup {
            topic: "t",
            ..Default::default()
        });
        if let Some(value) = topic_override {
            image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides: [(MIN_INSYNC_REPLICAS.to_string(), value.to_string())]
                    .into_iter()
                    .collect(),
            }));
        }
        if let Some(value) = cluster_default {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: MIN_INSYNC_REPLICAS.to_string(),
                config_value: Some(value.to_string()),
            }));
        }
        image
    }

    #[test]
    fn the_topic_override_wins_then_the_cluster_default_then_kafkas_own() {
        for (label, topic_override, cluster_default, replication_factor, expected) in [
            ("nothing published", None, None, 3, 1),
            ("cluster default only", None, Some("2"), 3, 2),
            ("topic override wins", Some("3"), Some("2"), 3, 3),
            ("an unparseable value falls back", Some("many"), None, 3, 1),
            ("the replication factor caps it", Some("5"), None, 3, 3),
            ("a cluster default is capped too", None, Some("9"), 2, 2),
        ] {
            check!(
                effective_min_insync_replicas(
                    &image(topic_override, cluster_default),
                    "t",
                    replication_factor,
                ) == expected,
                "{label}"
            );
        }
    }

    /// Node 2 carries a per-node `min.insync.replicas` where node 3 carries
    /// none. Each row is what `(node 2, node 3)` resolve for `t`.
    #[test]
    fn a_node_resolves_the_topic_then_its_own_value_then_the_cluster_default() {
        for (label, topic_override, cluster_default, node_two, expected) in [
            ("nothing published", None, None, None, (None, None)),
            (
                "a node beats the cluster",
                None,
                Some("2"),
                Some("3"),
                (Some(3), Some(2)),
            ),
            (
                "a node without a cluster default",
                None,
                None,
                Some("2"),
                (Some(2), None),
            ),
            (
                "the topic beats both",
                Some("1"),
                Some("2"),
                Some("3"),
                (Some(1), Some(1)),
            ),
            (
                "an unparseable node value reads as none",
                None,
                Some("2"),
                Some("many"),
                (None, Some(2)),
            ),
        ] {
            let mut image = image(topic_override, cluster_default);
            if let Some(value) = node_two {
                image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: NodeId(2),
                    config_name: MIN_INSYNC_REPLICAS.to_string(),
                    config_value: Some(value.to_string()),
                }));
            }
            check!(
                (
                    node_min_insync_replicas(&image, NodeId(2), "t"),
                    node_min_insync_replicas(&image, NodeId(3), "t"),
                ) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_min_isr_change_clears_only_the_selected_topics_elr() {
        let mut image = image(None, None);
        crate::test_support::finalize_elr_version(&mut image);
        for topic in ["t", "other"] {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.into(),
                partition: 0,
                ..Default::default()
            }));
            image.apply(&MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: topic.into(),
                partition: 0,
                eligible_leader_replicas: vec![NodeId(2)],
                last_known_elr: vec![],
            }));
        }

        check!(
            clear_elr_records(&image, Some("t"))
                == [MetadataRecord::V1PartitionElr(PartitionElrRecord {
                    topic: "t".into(),
                    partition: 0,
                    eligible_leader_replicas: vec![],
                    last_known_elr: vec![],
                })]
        );
        check!(clear_elr_records(&image, None).len() == 2);
    }
}
