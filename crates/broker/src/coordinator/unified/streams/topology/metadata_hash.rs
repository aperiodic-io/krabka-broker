//! The metadata hash of a streams group: one number that changes when any
//! topic that the topology reads or keeps changes.
//!
//! Kafka stores this hash in `StreamsGroupMetadataValue.MetadataHash`. On a
//! heartbeat it computes the hash again from the current metadata image, and
//! when the value differs it configures the topology again and bumps the group
//! epoch. A created, deleted or grown source topic, and an internal topic that
//! the controller created, therefore reach the assignment on the next
//! heartbeat.
//!
//! The hash itself is Kafka's `Utils.computeGroupHash` over
//! `Utils.computeTopicHash`, which [`topic_hash`] implements for all three
//! group types.

use std::collections::BTreeSet;

use krabka_metadata::MetadataImage;

use super::configured::input_topics;
use crate::coordinator::unified::{streams::persistence::StreamsGroupTopologyValue, topic_hash};

/// The names of the topics that `StreamsTopology.requiredTopics` returns: the
/// source topics, the repartition source topics and the changelog topics of
/// every subtopology.
#[must_use]
pub fn required_topics(topology: &StreamsGroupTopologyValue) -> BTreeSet<&str> {
    topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| {
            input_topics(subtopology).chain(
                subtopology
                    .state_changelog_topics
                    .iter()
                    .map(|topic| topic.name.as_str()),
            )
        })
        .collect()
}

/// Kafka's `StreamsGroup.computeMetadataHash`: the group hash over the topic
/// hash of every required topic that exists in `image`.
#[must_use]
pub fn metadata_hash(topology: &StreamsGroupTopologyValue, image: &MetadataImage) -> i64 {
    topic_hash::image_metadata_hash(required_topics(topology), image)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    use super::*;
    use crate::coordinator::unified::streams::{
        persistence::StoredTopicInfo,
        topology::test_support::{image_with, sub},
    };

    fn topology() -> StreamsGroupTopologyValue {
        let mut s0 = sub("0");
        s0.source_topics = vec!["in".into()];
        s0.repartition_sink_topics = vec!["sink-only".into()];
        s0.state_changelog_topics = vec![StoredTopicInfo {
            name: "store-changelog".into(),
            partitions: 0,
            replication_factor: 0,
            topic_configs: vec![],
        }];
        StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![s0],
        }
    }

    #[test]
    fn required_topics_leave_out_the_repartition_sink_topics() {
        check!(required_topics(&topology()) == BTreeSet::from(["in", "store-changelog"]),);
    }

    /// The hash changes exactly when a required topic appears, grows, goes
    /// away or moves to another rack, and ignores a topic that the topology
    /// does not read.
    #[test]
    fn metadata_hash_follows_the_required_topics() {
        let with_racks = |racks: &[Option<&str>]| {
            let mut image = image_with(&[("in", 1, 2), ("other", 2, 1)]);
            for (index, rack) in racks.iter().enumerate() {
                image.apply(&MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        rack: rack.map(str::to_owned),
                        ..crate::test_support::broker_registration(krabka_raft::NodeId(
                            1 + u64::try_from(index).unwrap(),
                        ))
                    },
                ));
            }
            image
        };
        let base = metadata_hash(&topology(), &image_with(&[("in", 1, 2)]));
        // (image, hash equals the base hash)
        let rows = [
            ("no required topic", image_with(&[("other", 2, 1)]), false),
            ("same image", image_with(&[("in", 1, 2)]), true),
            (
                "unrelated topic added",
                image_with(&[("in", 1, 2), ("other", 2, 1)]),
                true,
            ),
            ("partitions added", image_with(&[("in", 1, 3)]), false),
            ("topic id changed", image_with(&[("in", 9, 2)]), false),
            (
                "changelog created",
                image_with(&[("in", 1, 2), ("store-changelog", 3, 2)]),
                false,
            ),
            ("rack of a replica", with_racks(&[Some("r1")]), false),
            ("replica without a rack", with_racks(&[None]), true),
        ];
        check!(metadata_hash(&topology(), &image_with(&[])) == 0);
        for (name, image, same) in rows {
            check!(
                (metadata_hash(&topology(), &image) == base) == same,
                "{name}"
            );
        }
    }
}
