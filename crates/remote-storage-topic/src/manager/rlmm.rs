//! The [`RemoteLogMetadataManager`] implementation for the topic-backed
//! manager, with the publish-and-wait bridge its mutation methods use.
//!
//! Every mutation serializes an event, publishes it to the metadata log, and
//! blocks until the consumer pump returns the result of applying the published
//! offset. The bridge lives in the sibling `mutations` module. Each gated read
//! consults [`ReadGate`] before it delegates to the inner cache.

use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
    RemoteStorageError, TopicIdPartition,
};

use super::{TopicBasedRemoteLogMetadataManager, assignment::ReadGate};
use crate::{partitioning::metadata_partition_for, serde::MetadataEvent};

impl TopicBasedRemoteLogMetadataManager {
    /// Apply the assignment/readiness gate before consulting cached metadata.
    fn read_if_ready<T: Default>(
        &self,
        partition: &TopicIdPartition,
        read: impl FnOnce(&InmemoryRemoteLogMetadataManager) -> Result<T, RemoteStorageError>,
    ) -> Result<T, RemoteStorageError> {
        let mp = metadata_partition_for(partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // An unassigned partition is a genuine miss, even if the cache
            // retains entries consumed under an earlier assignment.
            ReadGate::Unassigned => Ok(T::default()),
            // Catching up is retryable and must remain distinct from a miss.
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => read(&self.inner),
        }
    }
}

impl RemoteLogMetadataManager for TopicBasedRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        // Mirror the in-memory manager's eager precondition: fail
        // fast before paying a round trip through Kafka.
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id: metadata.remote_log_segment_id().clone(),
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let event = MetadataEvent::AddSegment(metadata).encode();
        self.publish_and_wait(&tp, event)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let tp = update.remote_log_segment_id.topic_id_partition.clone();
        let event = MetadataEvent::UpdateSegment(update).encode();
        self.publish_and_wait(&tp, event)
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
        })
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.highest_offset_for_epoch(topic_id_partition, leader_epoch)
        })
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.list_remote_log_segments(topic_id_partition)
        })
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.read_if_ready(topic_id_partition, |inner| {
            inner.list_remote_log_segments_by_epoch(topic_id_partition, leader_epoch)
        })
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.topic_id_partition.clone();
        let event = MetadataEvent::PartitionDelete(metadata).encode();
        self.publish_and_wait(&tp, event)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_remote_storage::RemotePartitionDeleteState;

    use super::*;
    use crate::{
        log::{InProcessMetadataEventLog, MetadataEventLog},
        manager::test_support::{
            finish, on_blocking, seed_finished, start_manager, start_manager_all, started, tp,
        },
    };

    krabka_macros::remote_segment_check!(check_finished_segment);

    #[tokio::test(flavor = "multi_thread")]
    async fn add_finish_query_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = start_manager_all(log).await;
        seed_finished(&m, &[(10, 0, 99)]).await;

        check_finished_segment(m.as_ref(), &tp());
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_with_wrong_state_is_rejected_eagerly() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log.clone());
        // Force a non-Started state via the lifecycle helper.
        let bad = started(10, 0, 9).with_update(&finish(10)).unwrap();
        let m2 = m.clone();
        let err = on_blocking(move || m2.add_remote_log_segment_metadata(bad).unwrap_err()).await;
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
        // Eager rejection means nothing was published.
        assert!(log.high_water_marks().await.unwrap() == vec![0; 2]);
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partition_delete_lifecycle_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager_all(log).await;
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            let m2 = m.clone();
            on_blocking(move || {
                m2.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                    topic_id_partition: tp(),
                    state,
                    event_timestamp_ms: 500,
                    broker_id: 1,
                })
                .unwrap();
            })
            .await;
        }
        m.shutdown();
    }
}
