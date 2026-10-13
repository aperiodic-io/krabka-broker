//! Completion of local mutations by the single cache consumer.

use {
    super::{TopicBasedRemoteLogMetadataManager, assignment::ReadGate},
    crate::{error::MetadataLogError, partitioning::metadata_partition_for},
    bytes::Bytes,
    krabka_remote_storage::{RemoteStorageError, TopicIdPartition},
    std::collections::HashMap,
    tokio::sync::{Mutex, oneshot},
};

type MutationReply = oneshot::Sender<Result<(), RemoteStorageError>>;
pub(super) type MutationWaiters = Mutex<HashMap<(i32, i64), MutationReply>>;

impl TopicBasedRemoteLogMetadataManager {
    pub(super) fn publish_and_wait(
        &self,
        tp: &TopicIdPartition,
        event: Bytes,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata_partition_for(tp, self.log.partition_count());
        self.runtime.block_on(async {
            let (offset, reply) = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(MetadataLogError::Closed.into_storage()),
                published = self.publish_tracked(partition, event) => published?,
            };
            let result = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => Err(MetadataLogError::Closed.into_storage()),
                result = reply => result.unwrap_or_else(|_| Err(MetadataLogError::Closed.into_storage())),
            };
            self.mutation_waiters.lock().await.remove(&(partition, offset));
            result
        })
    }

    async fn publish_tracked(
        &self,
        partition: i32,
        event: Bytes,
    ) -> Result<(i64, oneshot::Receiver<Result<(), RemoteStorageError>>), RemoteStorageError> {
        // The pump takes this lock before applying a record. Hold it across
        // the publish acknowledgement so even a record consumed before the
        // acknowledgement cannot outrun registration of its exact offset.
        let mut waiters = self.mutation_waiters.lock().await;
        if !matches!(self.metadata_partition_gate(partition), ReadGate::Ready) {
            return Err(RemoteStorageError::NotReady { partition });
        }
        let offset = self
            .log
            .publish(partition, event)
            .await
            .map_err(MetadataLogError::into_storage)?;
        let (send, receive) = oneshot::channel();
        waiters.insert((partition, offset), send);
        Ok((offset, receive))
    }
}

#[cfg(test)]
mod tests;
