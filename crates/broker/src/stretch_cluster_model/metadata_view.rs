//! The projection of a model state onto the real `krabka_metadata` types.
//!
//! The production election code takes a `PartitionRecord`, a `MetadataImage`,
//! and a `ControllerLivenessState`, so the model builds those three values
//! from its own state and hands them over unchanged. Keeping the projection in
//! one file keeps the rest of the model free of the metadata vocabulary.

use {
    super::{
        block_on,
        config::{StretchModel, TOPIC},
        state::StretchState,
    },
    crate::heartbeat::controller_state::ControllerLivenessState,
    krabka_metadata::{LeaderEpoch, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord},
    uuid::Uuid,
};

impl StretchModel {
    /// The `PartitionRecord` that the metadata image holds in `state`.
    pub fn record(&self, state: &StretchState) -> PartitionRecord {
        PartitionRecord {
            topic: TOPIC.to_string(),
            partition: 0,
            leader: state.leader,
            replicas: self.replicas.clone(),
            isr: state.isr.clone(),
            leader_epoch: LeaderEpoch(state.leader_epoch),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }
    }

    /// A metadata image that holds the one topic and its one partition, for
    /// the real
    /// [`select_new_leader_for_partition`](crate::leader_election::select_new_leader_for_partition).
    pub fn image(&self, state: &StretchState) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.to_string(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(self.replicas.len())
                .expect("replica count fits in i16"),
        }));
        image.apply(&MetadataRecord::V1Partition(self.record(state)));
        image
    }

    /// A liveness registry that holds a fresh heartbeat for every broker the
    /// controller reaches, for the real
    /// [`select_new_leader_for_partition`](crate::leader_election::select_new_leader_for_partition).
    pub fn liveness(&self, state: &StretchState) -> ControllerLivenessState {
        let liveness = ControllerLivenessState::new(krabka_units::secs(60));
        let alive = self.alive(state);
        block_on(async {
            // Node-id order, not hash order, so the registry is the same on
            // every visit to the state.
            for broker in &self.brokers {
                if alive.contains(&broker.node_id) {
                    liveness.record_heartbeat(broker.node_id.0).await;
                }
            }
        });
        liveness
    }
}
