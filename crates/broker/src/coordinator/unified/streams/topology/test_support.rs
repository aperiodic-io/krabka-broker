//! Fixtures shared by the unit tests of the `topology` module tree.
//!
//! The module builds a [`MetadataImage`] that holds a given set of topics with
//! their partition records, and an empty [`StoredSubtopology`] that a test then
//! fills in with only the fields the scenario needs.

use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
use uuid::Uuid;

use crate::coordinator::unified::streams::persistence::{
    StoredCopartitionGroup, StoredSubtopology, StoredTopicInfo,
};

krabka_macros::single_replica_partition_fixture!(partition_record);

fn topic_record(name: &str, id: u8, partitions: i32) -> TopicRecord {
    TopicRecord {
        name: name.to_string(),
        topic_id: Uuid::from_bytes([id; 16]),
        partitions,
        replication_factor: 1,
    }
}

/// Builds an image that holds each `(name, id, partitions)` topic.
///
/// Each topic has `partitions` partition records, so
/// `topic_partition_count` resolves.
pub fn image_with(topics: &[(&str, u8, i32)]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for &(name, id, partitions) in topics {
        image.apply(&MetadataRecord::V1Topic(topic_record(name, id, partitions)));
        for p in 0..partitions {
            image.apply(&MetadataRecord::V1Partition(partition_record(
                name,
                krabka_ids::PartitionIndex(p),
                krabka_audit::NodeId(1),
            )));
        }
    }
    image
}

pub fn sub(id: &str) -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: id.to_string(),
        source_topics: vec![],
        source_topic_regex: vec![],
        repartition_sink_topics: vec![],
        state_changelog_topics: vec![],
        repartition_source_topics: vec![],
        copartition_groups: vec![],
    }
}

/// The two named sources used by the independent wire and persisted topology fixtures.
pub fn example_source_topics() -> Vec<String> {
    vec!["in-a".into(), "in-b".into()]
}

/// A persisted topology fixture, independent of the wire-to-stored mapper.
pub fn example_subtopology() -> StoredSubtopology {
    StoredSubtopology {
        subtopology_id: "0".into(),
        source_topics: example_source_topics(),
        source_topic_regex: vec!["^orders-.*".into()],
        repartition_sink_topics: vec!["rp-1".into()],
        state_changelog_topics: vec![StoredTopicInfo {
            name: "store-changelog".into(),
            partitions: 4,
            replication_factor: 3,
            topic_configs: vec![("cleanup.policy".into(), "compact".into())],
        }],
        repartition_source_topics: vec![StoredTopicInfo {
            name: "rp-1".into(),
            partitions: 4,
            replication_factor: 3,
            topic_configs: vec![],
        }],
        copartition_groups: vec![StoredCopartitionGroup {
            source_topics: vec![0, 1],
            source_topic_regex: vec![0],
            repartition_source_topics: vec![0],
        }],
    }
}
