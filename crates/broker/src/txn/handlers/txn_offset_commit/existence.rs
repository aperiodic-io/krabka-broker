//! The existence check `TxnOffsetCommit` runs after the per-topic `Read` ACL
//! gate, matching Kafka's `KafkaApis.handleTxnOffsetCommitRequest`
//! (lines 2124-2150): an authorized topic the metadata image does not hold,
//! or a partition index outside that topic's partition count, answers
//! `UNKNOWN_TOPIC_OR_PARTITION (3)` and never reaches the group coordinator.
//!
//! This check does not require the partition to currently have a leader:
//! `TxnOffsetCommit` appends to `__consumer_offsets`, not to the named
//! source topic-partition, so a partition mid-leader-election or with every
//! replica temporarily offline (the `NodeId(0)` sentinel) is still a valid
//! target as long as its metadata record exists.
//!
//! A denied topic is excluded from this check: it already carries
//! `TOPIC_AUTHORIZATION_FAILED` on every row, and Kafka's `filterByAuthorized`
//! never hands a denied topic to the existence check at all.

use std::collections::HashSet;

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequestTopic;

/// The `(topic, partition)` keys of `topics` that fail the existence check:
/// the topic is not in `image`, or the image has no partition record at that
/// index for it (missing entirely, or out of the topic's partition range).
///
/// Denied topics never appear in the returned set, since the per-topic `Read`
/// ACL gate already decided their fate.
pub(super) fn unknown_partitions(
    topics: &[TxnOffsetCommitRequestTopic],
    denied_topics: &HashSet<String>,
    image: &MetadataImage,
) -> HashSet<(String, i32)> {
    let mut unknown = HashSet::new();
    for topic in topics {
        if denied_topics.contains(&topic.name) {
            continue;
        }
        for part in &topic.partitions {
            let exists = image.partition(&topic.name, part.partition_index).is_some();
            if !exists {
                unknown.insert((topic.name.clone(), part.partition_index));
            }
        }
    }
    unknown
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::NodeId;
    use krabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequestPartition;
    use uuid::Uuid;

    use super::*;

    fn topic(name: &str, partitions: &[i32]) -> TxnOffsetCommitRequestTopic {
        TxnOffsetCommitRequestTopic {
            name: name.into(),
            partitions: partitions
                .iter()
                .map(|&partition_index| TxnOffsetCommitRequestPartition {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn image_with_topic(name: &str, partitions: i32, leader: NodeId) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(1));
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: name.to_owned(),
                topic_id: Uuid::from_u128(2),
                partitions,
                replication_factor: 1,
            },
        ));
        for partition in 0..partitions {
            image.apply(&krabka_metadata::MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    leader,
                    ..crate::coordinator::test_support::single_replica_partition(
                        name,
                        krabka_ids::PartitionIndex(partition),
                        NodeId(1),
                    )
                },
            ));
        }
        image
    }

    #[test]
    fn existing_partition_with_a_leader_is_not_unknown() {
        let image = image_with_topic("a", 1, NodeId(1));
        let unknown = unknown_partitions(&[topic("a", &[0])], &HashSet::new(), &image);
        check!(unknown.is_empty());
    }

    #[test]
    fn partition_out_of_the_topics_range_is_unknown() {
        let image = image_with_topic("a", 1, NodeId(1));
        let unknown = unknown_partitions(&[topic("a", &[5])], &HashSet::new(), &image);
        check!(unknown == HashSet::from([("a".to_string(), 5)]));
    }

    #[test]
    fn topic_missing_from_the_image_is_unknown() {
        let image = MetadataImage::new(Uuid::from_u128(1));
        let unknown = unknown_partitions(&[topic("missing", &[0])], &HashSet::new(), &image);
        check!(unknown == HashSet::from([("missing".to_string(), 0)]));
    }

    #[test]
    fn partition_with_no_leader_is_not_unknown() {
        let image = image_with_topic("a", 1, NodeId(0));
        let unknown = unknown_partitions(&[topic("a", &[0])], &HashSet::new(), &image);
        check!(unknown.is_empty());
    }

    #[test]
    fn denied_topic_is_excluded_from_the_existence_check() {
        let image = MetadataImage::new(Uuid::from_u128(1));
        let denied = HashSet::from(["missing".to_string()]);
        let unknown = unknown_partitions(&[topic("missing", &[0])], &denied, &image);
        check!(unknown.is_empty());
    }
}
