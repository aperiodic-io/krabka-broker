//! Internal-topic specs and their materialization.
//!
//! A topology needs a repartition topic for every `repartition_source_topics`
//! entry and a changelog topic for every `state_changelog_topics` entry. This
//! module turns the internal topics that the configured topology still misses
//! into the specs that the heartbeat hands to `CreateTopics`.

use {super::configured::ConfiguredTopology, std::collections::BTreeMap};

/// A fully-resolved internal topic that the heartbeat must create.
///
/// The topic is a repartition topic or a changelog topic. The spec holds its
/// partition count, its replication factor, and its config overrides. A
/// replication factor of 0 means "use the default of the broker".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalTopicSpec {
    pub name: String,
    pub partitions: i32,
    /// Replication factor that the client requested.
    ///
    /// A value of `0` means that the topology names no replication factor.
    /// The broker then sends -1 in `CreateTopics`, and the controller applies
    /// `default.replication.factor`.
    pub replication_factor: i16,
    pub configs: BTreeMap<String, String>,
}

/// The specs of the internal topics that `configured` must create, in name
/// order.
///
/// Kafka's `toCreatableTopic` copies the partition count, the replication
/// factor and the configs of the topology as they are. The Streams client
/// sends the `cleanup.policy` of each internal topic, so the broker adds
/// none.
#[must_use]
pub fn internal_topic_specs(configured: &ConfiguredTopology) -> Vec<InternalTopicSpec> {
    configured
        .internal_topics_to_create
        .values()
        .map(|topic| InternalTopicSpec {
            name: topic.name.clone(),
            partitions: topic.partitions,
            replication_factor: topic.replication_factor.unwrap_or(0),
            configs: topic.configs.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::streams::topology::configured::{
                ConfiguredInternalTopic, ConfiguredSubtopology,
            },
            test_support::string_pairs,
        },
        assert2::assert,
    };

    #[test]
    fn internal_topic_specs_copy_the_topology_values() {
        let topic =
            |name: &str, replication_factor, configs: &[(&str, &str)]| ConfiguredInternalTopic {
                name: name.into(),
                partitions: 5,
                replication_factor,
                configs: string_pairs(configs),
            };
        let rp = topic("rp", Some(2), &[("cleanup.policy", "delete")]);
        let cl = topic("cl", None, &[]);
        let configured = ConfiguredTopology {
            topology_epoch: 1,
            subtopologies: Some(BTreeMap::from([("0".to_string(), ConfiguredSubtopology {
                number_of_tasks: 5,
                source_topics: std::collections::BTreeSet::new(),
                repartition_source_topics: BTreeMap::from([("rp".to_string(), rp.clone())]),
                repartition_sink_topics: std::collections::BTreeSet::new(),
                state_changelog_topics: BTreeMap::from([("cl".to_string(), cl.clone())]),
            })])),
            internal_topics_to_create: BTreeMap::from([
                ("cl".to_string(), cl),
                ("rp".to_string(), rp),
            ]),
            status: None,
        };

        assert!(
            internal_topic_specs(&configured)
                == vec![
                    InternalTopicSpec {
                        name: "cl".to_string(),
                        partitions: 5,
                        replication_factor: 0,
                        configs: BTreeMap::new(),
                    },
                    InternalTopicSpec {
                        name: "rp".to_string(),
                        partitions: 5,
                        replication_factor: 2,
                        configs: maplit::btreemap! {
                            "cleanup.policy".to_string() => "delete".to_string()
                        },
                    },
                ]
        );
    }
}
