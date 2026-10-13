//! The questions the share path asks of the metadata image: which broker leads
//! a partition, what a topic id is called, and which leader epoch is current.
//!
//! The `ShareFetch` and `ShareAcknowledge` wire protocol carries a topic id and
//! no topic name, so every one of these lookups starts by resolving the id
//! against the current image. They sit together because they share that first
//! step and because they are the only methods here that read metadata rather
//! than acquisition state.

use {super::SharePartitionLeaderManager, krabka_ids::PartitionIndex};

impl SharePartitionLeaderManager {
    /// Resolves the wire `(leader_id, leader_epoch)` for
    /// `(topic_id, partition)`.
    ///
    /// The values come from the metadata image. A not-leader `ShareFetch` or
    /// `ShareAcknowledge` response carries them as the `current_leader`
    /// redirect hint. This method returns `(-1, -1)` when the topic or the
    /// partition is unknown.
    pub(crate) fn current_leader_of(&self, topic_id: uuid::Uuid, partition: i32) -> (i32, i32) {
        let image = self.controller.current_image();
        let Some(topic) = image.topic_name_by_id(&topic_id) else {
            return (-1, -1);
        };
        image.partition(topic, partition).map_or((-1, -1), |p| {
            (i32::try_from(p.leader.0).unwrap_or(-1), p.leader_epoch.0)
        })
    }

    /// Resolves the data-topic name for `topic_id` from the metadata image.
    ///
    /// Returns `None` when the id is unknown. The share path carries only
    /// `topic_id`. The handlers need the name to look up the local
    /// [`PartitionRegistry`](crate::partition_registry::PartitionRegistry)
    /// entry and to key the per-topic `Read` ACL checks.
    pub(crate) fn topic_name_for(&self, topic_id: uuid::Uuid) -> Option<String> {
        self.controller
            .current_image()
            .topic_name_by_id(&topic_id)
            .map(str::to_owned)
    }

    /// Returns `true` if this broker leads the partition of the data topic
    /// `topic_id`.
    ///
    /// The method resolves the topic name from the metadata image, because the
    /// share path carries only `topic_id`. It then compares the partition
    /// leader to `node_id`.
    ///
    /// The `ShareFetch` and `ShareAcknowledge` handlers call this method.
    pub(crate) fn topic_leader_is_self(&self, topic_id: uuid::Uuid, partition: i32) -> bool {
        let image = self.controller.current_image();
        let Some(topic) = image.topic_name_by_id(&topic_id) else {
            return false;
        };
        image
            .partition(topic, partition)
            .is_some_and(|p| p.leader == self.node_id)
    }

    /// Current `leader_epoch` for `(topic_id, partition)`.
    ///
    /// The value comes from the atomic of the local partition. The method
    /// returns `0` when the partition is not materialized on this broker.
    pub(super) fn leader_epoch_for(&self, topic_id: uuid::Uuid, partition: i32) -> i32 {
        let image = self.controller.current_image();
        let Some(topic) = image.topic_name_by_id(&topic_id) else {
            return 0;
        };
        self.partitions
            .get(topic, PartitionIndex(partition))
            .map_or(0, |p| {
                p.current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire)
            })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::share_partition::manager::test_support::{manager, manager_with_image},
        assert2::assert,
        krabka_ids::LeaderEpoch,
        krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord},
        std::sync::Arc,
    };

    #[tokio::test]
    async fn topic_leader_is_self_false_for_unknown_topic() {
        let mgr = manager();
        let tid = uuid::Uuid::from_bytes([23; 16]);
        assert!(!mgr.topic_leader_is_self(tid, 0));
    }

    #[tokio::test]
    async fn current_leader_of_reads_image_leader_and_epoch() {
        let tid = uuid::Uuid::from_bytes([31; 16]);
        // A topic-partition led by node 2 at leader epoch 5. Both components are
        // non-default and differ from every fixed-tuple mutant
        // ((0,0)/(0,1)/(-1,0)/(1,1)).
        let image = Arc::new(MetadataImage::from_records(uuid::Uuid::nil(), &[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: tid,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: 0,
                leader: NodeId(2),
                replicas: vec![NodeId(2)],
                isr: vec![NodeId(2)],
                leader_epoch: LeaderEpoch(5),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
        ]));
        let mgr = manager_with_image(image);

        // Known partition resolves to (leader_id, leader_epoch) from the image.
        assert!(mgr.current_leader_of(tid, 0) == (2, 5));
        // Unknown partition of a known topic -> (-1, -1).
        assert!(mgr.current_leader_of(tid, 9) == (-1, -1));
        // Unknown topic -> (-1, -1).
        assert!(mgr.current_leader_of(uuid::Uuid::from_bytes([99; 16]), 0) == (-1, -1));
    }
}
