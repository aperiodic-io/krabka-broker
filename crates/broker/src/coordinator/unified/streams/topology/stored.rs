//! Conversion of the client-supplied wire topology into its persisted form.
//!
//! The mapping is field-for-field. It is its own module because it is the only
//! part of topology handling that touches the `StreamsGroupHeartbeat` request
//! types; everything downstream reasons over the stored
//! `StreamsGroupTopologyValue` alone.

// Alias the request wire module for readability.
use krabka_protocol::owned::streams_group_heartbeat_request as wire;

use crate::coordinator::unified::streams::persistence::{
    StoredCopartitionGroup, StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue,
};

/// Converts a client-supplied [`wire::Topology`] into a persisted
/// [`StreamsGroupTopologyValue`].
///
/// The [`wire::Topology`] comes from a `StreamsGroupHeartbeat` request. The
/// coordinator stores the [`StreamsGroupTopologyValue`] and reasons over it.
///
/// This function is a straight field-for-field map. `KeyValue` config pairs
/// collapse to `(String, String)` tuples. The per-subtopology
/// `CopartitionGroup` and `TopicInfo` structs become their `Stored*` analogs.
#[must_use]
pub fn to_stored_topology(t: &wire::Topology) -> StreamsGroupTopologyValue {
    StreamsGroupTopologyValue {
        epoch: t.epoch,
        subtopologies: t.subtopologies.iter().map(to_stored_subtopology).collect(),
    }
}

fn to_stored_subtopology(s: &wire::Subtopology) -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: s.subtopology_id.clone(),
        source_topics: s.source_topics.clone(),
        source_topic_regex: s.source_topic_regex.clone(),
        repartition_sink_topics: s.repartition_sink_topics.clone(),
        state_changelog_topics: s
            .state_changelog_topics
            .iter()
            .map(to_stored_topic_info)
            .collect(),
        repartition_source_topics: s
            .repartition_source_topics
            .iter()
            .map(to_stored_topic_info)
            .collect(),
        copartition_groups: s
            .copartition_groups
            .iter()
            .map(to_stored_copartition_group)
            .collect(),
    }
}

fn to_stored_topic_info(
    t: &krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo,
) -> StoredTopicInfo {
    StoredTopicInfo {
        name: t.name.clone(),
        partitions: t.partitions,
        replication_factor: t.replication_factor,
        topic_configs: t
            .topic_configs
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect(),
    }
}

fn to_stored_copartition_group(c: &wire::CopartitionGroup) -> StoredCopartitionGroup {
    StoredCopartitionGroup {
        source_topics: c.source_topics.clone(),
        source_topic_regex: c.source_topic_regex.clone(),
        repartition_source_topics: c.repartition_source_topics.clone(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::topology::test_support::{
        example_source_topics, example_subtopology,
    };

    #[test]
    fn to_stored_topology_maps_all_fields() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::{
            key_value::KeyValue, topic_info::TopicInfo,
        };

        let wire_topology = wire::Topology {
            epoch: 9,
            subtopologies: vec![wire::Subtopology {
                subtopology_id: "0".into(),
                source_topics: example_source_topics(),
                source_topic_regex: vec!["^orders-.*".into()],
                repartition_sink_topics: vec!["rp-1".into()],
                state_changelog_topics: vec![TopicInfo {
                    name: "store-changelog".into(),
                    partitions: 4,
                    replication_factor: 3,
                    topic_configs: vec![KeyValue {
                        key: "cleanup.policy".into(),
                        value: "compact".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                repartition_source_topics: vec![TopicInfo {
                    name: "rp-1".into(),
                    partitions: 4,
                    replication_factor: 3,
                    topic_configs: vec![],
                    ..Default::default()
                }],
                copartition_groups: vec![wire::CopartitionGroup {
                    source_topics: vec![0, 1],
                    source_topic_regex: vec![0],
                    repartition_source_topics: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let stored = to_stored_topology(&wire_topology);
        assert!(
            stored
                == StreamsGroupTopologyValue {
                    epoch: 9,
                    subtopologies: vec![example_subtopology()],
                }
        );
    }
}
