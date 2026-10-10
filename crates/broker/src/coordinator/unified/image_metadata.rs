//! The [`MetadataProvider`] the running broker uses: a projection of the
//! controller's current `MetadataImage` into the topic ids, partition counts,
//! and partition racks that the assignors need.
//!
//! It is the one place where cluster metadata crosses into the coordinator, so
//! it sits apart from the coordinator state it feeds.

use std::sync::Arc;

use super::{actor::MetadataProvider, reconciler};

/// `MetadataProvider` backed by `krabka_raft::ControllerHandle::current_image()`.
#[derive(derive_more::Debug)]
pub struct ImageMetadataProvider {
    #[debug(skip)]
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        use krabka_protocol::primitives::uuid::Uuid as ProtoUuid;
        let image = self.controller.current_image();
        let mut topic_id_by_name = std::collections::HashMap::new();
        let mut partitions_per_topic = std::collections::HashMap::new();
        let mut partition_racks: std::collections::HashMap<(ProtoUuid, i32), Vec<String>> =
            std::collections::HashMap::new();
        for topic in image.topics() {
            let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
            topic_id_by_name.insert(topic.name.clone(), proto_id);
            // Kafka's `KRaftCoordinatorMetadataImage.TopicMetadata` counts the
            // partitions that the image holds.
            partitions_per_topic.insert(proto_id, image.topic_partition_count(&topic.name));
            // The rack of each replica whose broker has one, as Kafka's
            // `partitionRacks` lists them: the metadata hash reads every
            // entry, and the assignors read the set. Partitions whose
            // replicas have no rack info don't get an entry.
            for pr in image.partitions_of(&topic.name) {
                let racks: Vec<String> = pr
                    .replicas
                    .iter()
                    .filter_map(|&node_id| image.broker(node_id).and_then(|b| b.rack.clone()))
                    .collect();
                if !racks.is_empty() {
                    partition_racks.insert((proto_id, pr.partition), racks);
                }
            }
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            partition_racks,
        }
    }

    fn topic_name(&self, topic_id: &krabka_protocol::primitives::uuid::Uuid) -> Option<String> {
        self.controller
            .current_image()
            .topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))
            .map(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::test_support::{fixed_source, real_uuid};

    #[test]
    fn image_metadata_provider_snapshot_projects_topics_partitions_and_racks() {
        let mut image = krabka_metadata::MetadataImage::new(real_uuid(9));
        let topic_id = real_uuid(8);
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "input".into(),
                topic_id,
                partitions: 3,
                replication_factor: 2,
            },
        ));
        for (node_id, rack) in [
            (1, Some("rack-a".to_string())),
            (2, Some("rack-b".to_string())),
            (3, None),
        ] {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    broker_epoch: i64::try_from(node_id).unwrap(),
                    incarnation_id: real_uuid(u8::try_from(node_id).unwrap()),
                    host: format!("broker-{node_id}"),
                    rack,
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 0,
                leader: krabka_metadata::NodeId(1),
                replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                isr: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                directories: vec![real_uuid(1), real_uuid(2)],
                ..Default::default()
            },
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 1,
                leader: krabka_metadata::NodeId(3),
                replicas: vec![krabka_metadata::NodeId(3)],
                isr: vec![krabka_metadata::NodeId(3)],
                directories: vec![real_uuid(3)],
                ..Default::default()
            },
        ));

        let provider = ImageMetadataProvider {
            controller: fixed_source(image),
        };
        let snapshot = provider.snapshot();
        let proto_topic_id = krabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());

        check!(snapshot.topic_id_by_name.get("input") == Some(&proto_topic_id));
        check!(snapshot.partitions_per_topic.get(&proto_topic_id) == Some(&2));
        check!(
            snapshot.partition_racks.get(&(proto_topic_id, 0))
                == Some(&vec!["rack-a".to_string(), "rack-b".to_string()])
        );
        check!(snapshot.partition_racks.get(&(proto_topic_id, 1)) == None);
    }

    /// The consumer and share groups hash the provider's snapshot, and the
    /// streams groups hash the image itself: both must give Kafka's
    /// `Utils.computeTopicHash`, which lists the rack of every replica, two
    /// replicas on one rack included.
    #[test]
    fn the_snapshot_hashes_a_topic_as_the_image_does() {
        use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, NodeId};

        use crate::coordinator::unified::topic_hash::image_topic_hash;

        let mut image = krabka_metadata::MetadataImage::new(real_uuid(9));
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "input".into(),
            topic_id: real_uuid(8),
            partitions: 2,
            replication_factor: 3,
        }));
        for (node_id, rack) in [
            (1, Some("rack-b")),
            (2, Some("rack-a")),
            (3, Some("rack-b")),
        ] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    rack: rack.map(str::to_owned),
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        for (partition, replicas) in [(0, vec![1, 2, 3]), (1, vec![3])] {
            image.apply(&MetadataRecord::V1Partition(
                crate::test_support::in_sync_partition(crate::test_support::InSyncPartitionSetup {
                    topic: "input",
                    partition: krabka_ids::PartitionIndex(partition),
                    directories: replicas.iter().map(|_| real_uuid(1)).collect(),
                    replicas: replicas.iter().copied().map(NodeId).collect(),
                    ..Default::default()
                }),
            ));
        }
        let snapshot = ImageMetadataProvider {
            controller: fixed_source(image.clone()),
        }
        .snapshot();

        let from_image = image_topic_hash("input", &image);
        check!(from_image.is_some());
        check!(snapshot.topic_hash("input") == from_image);
        check!(
            snapshot.metadata_hash(["input", "absent"])
                == crate::coordinator::unified::topic_hash::image_metadata_hash(["input"], &image)
        );
    }
}
