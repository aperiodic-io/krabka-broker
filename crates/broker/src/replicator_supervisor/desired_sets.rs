//! Pure derivations, from one metadata image, of the partition sets this
//! broker must host: the follower replicator set, the local materialization
//! set, and the diskless WAL voter placements.

use std::collections::{HashMap, HashSet};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use krabka_raft::NodeId;

use super::TopicPartition;

/// `(topic, partition)` pairs where `node_id` is in `replicas` AND
/// `leader != node_id`. For each such pair the broker should run a follower
/// replicator task. This is a single O(P) walk. It runs on every
/// metadata-image change, so it must stay proportional to total partitions.
pub(crate) fn desired_follower_set(
    node_id: NodeId,
    image: &MetadataImage,
) -> HashSet<TopicPartition> {
    image
        .all_partitions()
        .filter(|p| p.replicas.contains(&node_id) && p.leader != node_id)
        .map(|p| (p.topic.clone(), p.partition))
        .collect()
}

/// `(topic, partition)` pairs where `node_id` is in `replicas`,
/// regardless of leader/follower role. Every entry here means this
/// broker hosts partition data on disk and must materialize the
/// on-disk `Partition` locally. This is a single O(P) walk, the same as
/// [`desired_follower_set`].
pub(crate) fn desired_local_set(node_id: NodeId, image: &MetadataImage) -> HashSet<TopicPartition> {
    image
        .all_partitions()
        .filter(|p| p.replicas.contains(&node_id))
        .map(|p| (p.topic.clone(), p.partition))
        .collect()
}

/// WAL voter placement for every diskless partition in one metadata image.
/// The partition leader is first, then rack-distinct registered brokers are
/// preferred by the placement policy.
pub(crate) fn desired_wal_placements(
    image: &MetadataImage,
    voter_count: usize,
) -> HashMap<crate::wal::quorum::registry::ShardId, crate::wal::quorum::registry::WalPlacement> {
    let brokers = image.brokers().cloned().collect::<Vec<_>>();
    image
        .all_partitions()
        .filter(|partition| {
            crate::config_keys::resolve_diskless(image.topic_config(&partition.topic))
        })
        .filter(|partition| image.broker(partition.leader).is_some())
        .filter_map(|partition| {
            let topic_id = image.topic(&partition.topic)?.topic_id;
            Some((
                crate::wal::quorum::registry::ShardId {
                    topic_id,
                    partition: PartitionIndex(partition.partition),
                },
                crate::wal::quorum::registry::WalPlacement {
                    voters: crate::wal::quorum::placement::select_voters(
                        brokers.iter().cloned(),
                        partition.leader,
                        voter_count,
                    ),
                    leader_epoch: partition.leader_epoch.0,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        broker_record, image_with, partition_record, topic_record,
    };

    #[test]
    fn includes_partition_where_self_is_follower() {
        let img = crate::replicator_supervisor::test_support::three_replica_image(
            NodeId(1),
            krabka_metadata::LeaderEpoch(0),
        );
        let d = desired_follower_set(NodeId(2), &img);
        assert!(d.contains(&("t".into(), 0)));
        assert!(d.len() == 1);
    }

    #[test]
    fn desired_follower_set_includes_followers_excludes_leader_and_non_replicas() {
        let img = crate::replicator_supervisor::test_support::three_replica_image(
            NodeId(1),
            krabka_metadata::LeaderEpoch(0),
        );
        let cases = [
            // Self is a follower replica → included.
            (NodeId(2), HashSet::from_iter([("t".to_string(), 0)])),
            // Self is the leader → excluded.
            (NodeId(1), HashSet::new()),
            // Self is not a replica at all → excluded.
            (NodeId(99), HashSet::new()),
        ];
        for (node_id, want) in cases {
            assert!(
                desired_follower_set(node_id, &img) == want,
                "node {}",
                node_id.0
            );
        }
    }

    #[test]
    fn desired_local_set_exactly_includes_all_local_replicas() {
        let img = image_with(&[
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "a",
                    partitions: crate::test_support::PartitionCount(2),
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "a",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "a",
                    partition: krabka_ids::PartitionIndex(1),
                    leader: NodeId(2),
                    ..Default::default()
                },
            ),
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "b",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "b",
                    leader: NodeId(3),
                    replicas: vec![NodeId(1), NodeId(3)],
                    ..Default::default()
                },
            ),
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "c",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "c",
                    partition: krabka_ids::PartitionIndex(-1),
                    replicas: vec![NodeId(2), NodeId(4)],
                    ..Default::default()
                },
            ),
        ]);

        let local = desired_local_set(NodeId(2), &img);

        assert!(
            local
                == HashSet::from_iter([
                    ("a".to_string(), 0),
                    ("a".to_string(), 1),
                    ("c".to_string(), -1),
                ])
        );
    }

    #[test]
    fn desired_wal_placements_cover_only_diskless_topics_and_prefer_distinct_racks() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(17);
        let mut overrides = BTreeMap::new();
        overrides.insert("krabka.diskless".into(), "true".into());
        let mut broker1 = broker_record(NodeId(1));
        broker1.rack = Some("a".into());
        let mut broker2 = broker_record(NodeId(2));
        broker2.rack = Some("b".into());
        let mut broker3 = broker_record(NodeId(3));
        broker3.rack = Some("c".into());
        let mut broker4 = broker_record(NodeId(4));
        broker4.rack = Some("a".into());
        let image = image_with(&[
            MetadataRecord::V1BrokerRegistration(broker4),
            MetadataRecord::V1BrokerRegistration(broker2),
            MetadataRecord::V1BrokerRegistration(broker1),
            MetadataRecord::V1BrokerRegistration(broker3),
            MetadataRecord::V1Topic(TopicRecord {
                name: "diskless".into(),
                topic_id,
                partitions: 1,
                replication_factor: 3,
            }),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "diskless",
                    leader: NodeId(2),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "classic",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "classic",
                    replicas: vec![NodeId(1)],
                    ..Default::default()
                },
            ),
        ]);

        let placements = desired_wal_placements(&image, 3);

        assert!(placements.len() == 1);
        assert!(
            placements.get(&crate::wal::quorum::registry::ShardId {
                topic_id,
                partition: PartitionIndex(0),
            }) == Some(&crate::wal::quorum::registry::WalPlacement {
                voters: vec![NodeId(2), NodeId(1), NodeId(3)],
                leader_epoch: 0,
            })
        );
    }

    #[test]
    fn multiple_topics_aggregated() {
        let img = image_with(&[
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "a",
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "a",
                    ..Default::default()
                },
            ),
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    topic: "b",
                    partitions: crate::test_support::PartitionCount(2),
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "b",
                    leader: NodeId(3),
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "b",
                    partition: krabka_ids::PartitionIndex(1),
                    leader: NodeId(2),
                    ..Default::default()
                },
            ),
        ]);
        let d = desired_follower_set(NodeId(2), &img);
        // b/1 is excluded: self is leader for it.
        assert!(d == HashSet::from_iter([("a".to_string(), 0), ("b".to_string(), 0)]));
        assert!(d.contains(&("a".into(), 0)));
        assert!(d.contains(&("b".into(), 0)));
        assert!(!d.contains(&("b".into(), 1))); // self is leader for b/1
        assert!(d.len() == 2);
    }
}
