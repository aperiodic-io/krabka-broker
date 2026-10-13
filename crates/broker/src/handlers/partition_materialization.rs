//! Opens committed partitions and installs their initial leadership.

use {
    crate::{
        broker::Broker,
        handlers::create_topics::{INITIAL_LEADER_EPOCH, InitialLeadership},
        partition::PartitionRuntimeConfig,
        replicator_supervisor::{MaterializePartitionConfig, materialize_partition},
        wal::{ControllerSequencer, OffsetSequencer},
    },
    krabka_raft::NodeId,
    std::path::PathBuf,
};

/// Only assigned replicas open a local partition.
pub(crate) fn should_materialize_locally(replicas: &[NodeId], node_id: NodeId) -> bool {
    replicas.contains(&node_id)
}

#[derive(Clone, Copy)]
/// The broker resources and topic settings of a committed partition batch.
pub(crate) struct PartitionMaterialization<'a> {
    pub(crate) broker: &'a Broker,
    pub(crate) log_dirs: &'a [PathBuf],
    pub(crate) diskless: bool,
    pub(crate) topic_id: uuid::Uuid,
}

impl PartitionMaterialization<'_> {
    /// Materializes each assigned local replica after the metadata commit.
    /// A failure leaves the supervisor to retry that partition.
    pub(crate) async fn materialize(
        self,
        operation: &str,
        topic: &str,
        indices: impl IntoIterator<Item = i32>,
        assignments: &[Vec<NodeId>],
        leaderships: &[InitialLeadership],
    ) {
        let broker = self.broker;
        for (index, (replicas, leadership)) in
            indices.into_iter().zip(assignments.iter().zip(leaderships))
        {
            if !should_materialize_locally(replicas, broker.config.node_id) {
                continue;
            }
            if let Err(error) = materialize_partition(MaterializePartitionConfig {
                partitions: &broker.partitions,
                topic,
                topic_id: Some(self.topic_id),
                partition: index,
                log_dirs: self.log_dirs,
                log_config: &broker.config.log_config,
                log_dir_status: &broker.log_dir_status,
                producer_state: &broker.producer_state,
                runtime: PartitionRuntimeConfig::from_broker(
                    &broker.config,
                    self.diskless,
                    (
                        Some(broker.hot_tail.clone()),
                        Some(broker.wal_shards.clone()),
                        self.diskless.then(|| {
                            std::sync::Arc::new(ControllerSequencer::new(broker.controller.clone()))
                                as std::sync::Arc<dyn OffsetSequencer>
                        }),
                    ),
                ),
            }) {
                tracing::error!(topic, partition = index, error = %error,
                    "{operation}: materialize after quorum commit failed");
                continue;
            }
            let Some(partition) = broker
                .partitions
                .get(topic, krabka_ids::PartitionIndex(index))
            else {
                continue;
            };
            // The same epoch as the committed PartitionRecord. Disk-backed
            // leaders record it at the log end before publishing their role.
            if let Err(error) = leadership
                .install(
                    &partition,
                    &broker.producer_state,
                    self.topic_id,
                    broker.config.node_id,
                    replicas,
                    INITIAL_LEADER_EPOCH,
                )
                .await
            {
                tracing::error!(topic, partition = index, error = %error,
                    "{operation}: failed to record the initial leader epoch");
            }
        }
    }
}
