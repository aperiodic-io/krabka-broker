//! [`SwappableRlmm`]: a [`RemoteLogMetadataManager`] facade whose backing
//! implementation can be replaced atomically after construction.
//!
//! It lets [`crate::TopicBasedRemoteLogMetadataManager`] take over from
//! [`crate::NotReadyRlmm`] *after* the
//! broker's listener accept loop is serving, so the manager's loopback
//! `AdminClient` call to provision `__remote_log_metadata` has a server to
//! talk to. Until a caller calls [`SwappableRlmm::swap`], every RLMM method
//! delegates to the placeholder. After the swap, every call delegates to the
//! new implementation.
//!
//! ## Invariant during the swap window
//!
//! Trait calls between construction and the first `swap` go to the
//! placeholder, which is normally [`crate::NotReadyRlmm`]. Once `swap` returns,
//! every later call uses the new implementation. Each trait method snapshots
//! the inner [`Arc`] under a read lock and releases the lock before it
//! delegates. A call already in progress can finish on the old implementation
//! after the swap. Separate calls within a larger operation can therefore use
//! different implementations.

use std::sync::{Arc, RwLock};

use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemotePartitionDeleteMetadata, RemoteStorageError, TopicIdPartition,
};

/// Hot-swappable [`RemoteLogMetadataManager`] wrapper.
pub struct SwappableRlmm {
    inner: RwLock<Arc<dyn RemoteLogMetadataManager>>,
}

impl SwappableRlmm {
    /// Construct the facade with an initial backing implementation. The
    /// first [`Self::swap`], if there is one, replaces it.
    #[must_use]
    pub fn new(initial: Arc<dyn RemoteLogMetadataManager>) -> Self {
        Self {
            inner: RwLock::new(initial),
        }
    }

    /// Replace the backing implementation. The next trait call
    /// observes the new one.
    /// # Panics
    /// Panics if the backing-implementation lock is poisoned.
    pub fn swap(&self, new: Arc<dyn RemoteLogMetadataManager>) {
        *self
            .inner
            .write()
            .expect("SwappableRlmm write lock poisoned") = new;
    }

    /// Snapshot the current inner [`Arc`] and do not hold the read lock past
    /// the call site. Every trait method below calls this.
    fn current(&self) -> Arc<dyn RemoteLogMetadataManager> {
        self.inner
            .read()
            .expect("SwappableRlmm read lock poisoned")
            .clone()
    }
}

impl RemoteLogMetadataManager for SwappableRlmm {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.current().add_remote_log_segment_metadata(metadata)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        self.current().update_remote_log_segment_metadata(update)
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.current()
            .remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        self.current()
            .highest_offset_for_epoch(topic_id_partition, leader_epoch)
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.current().list_remote_log_segments(topic_id_partition)
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.current()
            .list_remote_log_segments_by_epoch(topic_id_partition, leader_epoch)
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.current()
            .put_remote_partition_delete_metadata(metadata)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_remote_storage::InmemoryRemoteLogMetadataManager;

    use super::*;
    use crate::manager::test_support::{started, tp};

    #[test]
    fn delegates_to_initial_then_to_swapped() {
        let first: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let swap = SwappableRlmm::new(first.clone());

        // Write through the facade lands in `first`.
        swap.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        assert!(first.list_remote_log_segments(&tp()).unwrap().len() == 1);

        // Swap in a fresh empty backing impl; reads now show the new
        // (empty) one, not the original.
        let second: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        swap.swap(second.clone());
        assert!(swap.list_remote_log_segments(&tp()).unwrap().is_empty());
        // The previous `first` is undisturbed — the facade just stopped
        // pointing at it.
        assert!(first.list_remote_log_segments(&tp()).unwrap().len() == 1);

        // Writes after the swap go to `second`.
        swap.add_remote_log_segment_metadata(started(11, 100, 199))
            .unwrap();
        assert!(second.list_remote_log_segments(&tp()).unwrap().len() == 1);
        assert!(first.list_remote_log_segments(&tp()).unwrap().len() == 1);
    }
}
