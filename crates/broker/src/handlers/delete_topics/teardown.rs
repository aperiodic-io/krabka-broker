//! Tearing down the local state of a topic whose deletion the metadata quorum
//! has already committed.
//!
//! Three things have to go, in order: the partition-registry entries, the
//! diskless WAL shards, and the on-disk log directories. JBOD means a
//! partition may live in any configured log dir, so each one is resolved
//! rather than assumed.

use std::path::PathBuf;

use krabka_ids::PartitionIndex;
use uuid::Uuid;

use crate::{broker::Broker, log_dir, partition_registry::PartitionRegistry};

/// Removes every local partition of a deleted topic from the registry, the WAL
/// shard registry, and disk.
pub(super) fn remove_local_partitions(
    broker: &Broker,
    partitions: &PartitionRegistry,
    log_dirs: &[PathBuf],
    name: &str,
    topic_id: Option<Uuid>,
    local_partitions: Vec<PartitionIndex>,
) {
    for idx in local_partitions {
        if let Some(partition) = partitions.remove(name, idx)
            && let Some(writer) = partition.take_writer_handle()
        {
            writer.abort();
        }
        if let Some(topic_id) = topic_id {
            broker.hot_tail.remove_partition(topic_id, idx);
        }
        // JBOD: the partition may live in any log dir; resolve
        // its actual location (existing-location wins).
        let dir = log_dir::place_partition_dir(log_dirs, name, idx.get());
        if let (Some(topic_id), Some(owning_dir)) = (topic_id, dir.parent())
            && let Err(error) = crate::wal::quorum::remove_shard(
                broker.wal_shards.as_ref(),
                owning_dir,
                name,
                topic_id,
                idx,
                crate::wal::quorum::ShardRemoval::All,
            )
        {
            tracing::warn!(
                topic = %name,
                partition = idx.get(),
                error = %error,
                "failed to remove deleted topic WAL shard"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
