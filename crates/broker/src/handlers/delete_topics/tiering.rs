//! The remote-tier half of a topic deletion: which partitions have archived
//! segments, and the detached cascade that clears them.
//!
//! The snapshot has to be taken before the local tear-down, because after it
//! the `Partition` is gone and with it both the `remote.storage.enable` flag
//! and the topic id. The cascade itself runs afterwards, so the two steps are
//! separate functions that the handler calls on either side of the commit.

use krabka_ids::PartitionIndex;
use krabka_remote_storage::TopicIdPartition;

use crate::{
    broker::Broker,
    partition_registry::PartitionRegistry,
    remote_log_manager::{ArchiveMode, cascade_remote_partition_delete},
};

/// Snapshots the `(topic_id, partition_id)` of every tiered partition of
/// `topic_name` BEFORE the controller commits the delete and the broker tears
/// down in-memory state.
///
/// After teardown the `Partition` is gone and the broker loses the
/// `remote.storage.enable` flag plus the topic id; this snapshot is the sole
/// record that drives the remote-tier partition-delete cascade.
pub(super) fn tiered_partitions(
    broker: &Broker,
    partitions: &PartitionRegistry,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    local_partitions: &[PartitionIndex],
) -> Vec<TopicIdPartition> {
    if broker.remote_reader.is_none() {
        return Vec::new();
    }
    let Some(topic_id) = image.topic(topic_name).map(|topic| topic.topic_id) else {
        return Vec::new();
    };
    local_partitions
        .iter()
        .copied()
        .filter(|&index| {
            partitions.get(topic_name, index).is_some_and(|partition| {
                partition
                    .log
                    .lock()
                    .is_ok_and(|log| log.config_snapshot().remote_storage_enable)
            })
        })
        .map(|index| TopicIdPartition::new(topic_id, topic_name.to_string(), index.get()))
        .collect()
}

/// Fires off the detached tasks that walk each tiered partition's remote
/// segments through `DeletePartitionMarked` → `DeletePartitionStarted` →
/// per-segment lifecycle → `DeletePartitionFinished`.
///
/// The response returns immediately; failures inside the cascade log at WARN.
/// A write-once archive keeps every archived byte: the cascade clears the
/// broker's metadata but deletes nothing. Deleting a topic must not erase a
/// compliance archive.
pub(super) fn spawn_remote_cascades(broker: &Broker, tiered_to_cascade: Vec<TopicIdPartition>) {
    if let Some(reader) = broker.remote_reader.as_ref() {
        let broker_id = broker.config.broker_id;
        let archive = ArchiveMode::from_worm(broker.config.remote_storage_worm.as_ref());
        for tp in tiered_to_cascade {
            let rsm = reader.rsm.clone();
            let rlmm = reader.rlmm.clone();
            let index_cache = reader.index_cache.clone();
            tokio::spawn(cascade_remote_partition_delete(
                tp,
                broker_id,
                archive,
                rsm,
                rlmm,
                index_cache,
            ));
        }
    }
}
