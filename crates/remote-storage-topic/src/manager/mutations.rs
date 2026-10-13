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

/// A published record, named by the metadata partition and offset it landed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct MutationKey {
    pub partition: i32,
    pub offset: i64,
}

pub(super) type MutationWaiters = Mutex<HashMap<MutationKey, MutationReply>>;

/// A published record's offset and the channel that reports its outcome.
struct TrackedPublish {
    offset: i64,
    reply: oneshot::Receiver<Result<(), RemoteStorageError>>,
}

impl TopicBasedRemoteLogMetadataManager {
    pub(super) fn publish_and_wait(
        &self,
        tp: &TopicIdPartition,
        event: Bytes,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata_partition_for(tp, self.log.partition_count());
        self.runtime.block_on(async {
            let TrackedPublish { offset, reply } = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(MetadataLogError::Closed.into_storage()),
                published = self.publish_tracked(partition, event) => published?,
            };
            let result = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => Err(MetadataLogError::Closed.into_storage()),
                result = reply => result.unwrap_or_else(|_| Err(MetadataLogError::Closed.into_storage())),
            };
            self.mutation_waiters
                .lock()
                .await
                .remove(&MutationKey { partition, offset });
            result
        })
    }

    async fn publish_tracked(
        &self,
        partition: i32,
        event: Bytes,
    ) -> Result<TrackedPublish, RemoteStorageError> {
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
        waiters.insert(MutationKey { partition, offset }, send);
        Ok(TrackedPublish {
            offset,
            reply: receive,
        })
    }
}

#[cfg(test)]
mod tests;
