//! Kafka's `handleStreamsGroupDescribe` topology topic-`Describe` filter: a
//! group whose topology names a topic the caller cannot `Describe` (across
//! every source, repartition sink, repartition source and changelog topic the
//! topology references) is hidden. Kafka replaces that group's row with
//! `TOPIC_AUTHORIZATION_FAILED` (29), no topology and no members, instead of
//! disclosing the topic names through the topology.
//!
//! Topic traversal is shared with the heartbeat wire topology through
//! `streams_group_heartbeat::topic_authz::define_required_topics`.

use crate::{
    coordinator::unified::streams::persistence::StreamsGroupTopologyValue,
    handlers::streams_group_heartbeat::topic_authz::define_required_topics,
};

define_required_topics!(StreamsGroupTopologyValue);

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{StoredSubtopology, StoredTopicInfo};

    fn topic_info(name: &str) -> StoredTopicInfo {
        StoredTopicInfo {
            name: name.into(),
            partitions: 0,
            replication_factor: 0,
            topic_configs: Vec::new(),
        }
    }

    fn subtopology(id: &str) -> StoredSubtopology {
        StoredSubtopology {
            subtopology_id: id.into(),
            ..Default::default()
        }
    }

    /// Every kind of topic the topology names -- source, sink, repartition
    /// source and changelog -- contributes to `requiredTopics`, and a name
    /// that recurs is counted once, in the order it was first seen.
    #[test]
    fn required_topics_collects_every_kind_once_each() {
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![
                StoredSubtopology {
                    source_topics: vec!["orders".into()],
                    repartition_sink_topics: vec!["rp".into()],
                    ..subtopology("0")
                },
                StoredSubtopology {
                    source_topics: vec!["orders".into()],
                    repartition_source_topics: vec![topic_info("rp")],
                    state_changelog_topics: vec![topic_info("store-changelog")],
                    ..subtopology("1")
                },
            ],
        };

        check!(
            required_topics(&topology)
                == vec![
                    "orders".to_string(),
                    "rp".to_string(),
                    "store-changelog".to_string(),
                ]
        );
    }

    #[test]
    fn required_topics_empty_topology_yields_no_topics() {
        let topology = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: Vec::new(),
        };

        check!(required_topics(&topology).is_empty());
    }
}
