//! The remote-tier reads that answer with the log's own bytes: a share fetch
//! (KIP-932) and a dead-letter copy (KIP-1191) of an offset that only the
//! remote tier holds (KIP-405).
//!
//! Kafka serves both through `ReplicaManager`'s remote read. A share fetch
//! goes through `DelayedShareFetch`, which hands the partition to
//! `RemoteLogManager.asyncRead` when the share-partition start offset is
//! below the local log start. A dead-letter copy goes through
//! `ShareGroupDLQRecordFetcher`, whose `LogReader.readAsync` reads remote data
//! the same way. Both take one task of the remote reader pool, so both take
//! one permit of [`super::ReaderPool`] here.

use {
    super::{AbortedTxnEntry, RemoteReader},
    crate::{
        metrics::{BrokerMetrics, RemoteTierPath},
        partition::Partition,
    },
    bytes::Bytes,
    krabka_ids::LeaderEpoch,
    krabka_log::Offset,
    krabka_remote_storage::{RemoteStorageError, TopicIdPartition},
};

impl RemoteReader {
    /// Whether the remote tier, and not the local log, serves `offset` of
    /// `partition`.
    ///
    /// The tier serves the offsets of a tiered partition from the global log
    /// start up to the local log start. Only a floor that this process moved
    /// bounds it from below: a floor inferred at `Log::open` from the segments
    /// still on disk sits above everything the tier holds. See
    /// `Log::established_log_start`. An offset below an established floor is
    /// gone from every tier, and a local read answers it with `OffsetTooLow`.
    pub(crate) fn serves(partition: &Partition, offset: Offset) -> bool {
        let log = partition.log.lock().expect("log mutex poisoned");
        log.config_snapshot().remote_storage_enable
            && offset < log.local_log_start_offset()
            && log
                .established_log_start()
                .is_none_or(|floor| offset >= floor)
    }

    /// Reads the whole batches of `partition` from `offset` on out of the
    /// remote tier, as [`RemoteReader::fetch_raw`] returns them, and counts
    /// the read where KIP-405 counts a remote fetch.
    ///
    /// The read looks the segment up under the leader epoch that owned
    /// `offset`. That epoch comes from the partition's leader-epoch
    /// checkpoint, as Kafka's `epochForOffset` does, and is the current leader
    /// epoch when the checkpoint has no entry. The read holds one permit of the
    /// reader pool, and a pool whose queue is full refuses it.
    ///
    /// # Errors
    ///
    /// Returns the tier's error, or [`RemoteStorageError::Backend`] when the
    /// reader pool refuses the read.
    pub(crate) async fn read_partition(
        &self,
        partition: &Partition,
        tp: &TopicIdPartition,
        offset: Offset,
        max_bytes: usize,
        metrics: &BrokerMetrics,
    ) -> Result<Option<Bytes>, RemoteStorageError> {
        metrics.record_remote_request(RemoteTierPath::Fetch, &tp.topic);
        let leader_epoch = owning_epoch(partition, offset);
        let Ok(_permit) = self.pool.acquire().await else {
            metrics.record_remote_error(RemoteTierPath::Fetch, &tp.topic);
            return Err(RemoteStorageError::Backend(
                "the remote reader pool is saturated".to_owned(),
            ));
        };
        let started = std::time::Instant::now();
        let read = self.fetch_raw(tp, leader_epoch, offset.0, max_bytes).await;
        metrics.observe_remote_reader_fetch(started.elapsed());
        match &read {
            Ok(Some(bytes)) => metrics.record_remote_bytes(
                RemoteTierPath::Fetch,
                &tp.topic,
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            ),
            // The metadata partition is still catching up after a restart:
            // expected churn, not a failed fetch.
            Ok(None) | Err(RemoteStorageError::NotReady { .. }) => {}
            Err(_) => metrics.record_remote_error(RemoteTierPath::Fetch, &tp.topic),
        }
        read
    }

    /// The aborted transactions of `partition` that overlap the inclusive
    /// range `[from, to]`, out of the remote tier's transaction indexes. It
    /// looks the segment up under the leader epoch that owned `from`, as
    /// [`Self::read_partition`] does.
    ///
    /// # Errors
    ///
    /// Returns the tier's error.
    pub(crate) async fn aborted_in_partition(
        &self,
        partition: &Partition,
        tp: &TopicIdPartition,
        from: Offset,
        to: Offset,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        self.aborted_transactions(tp, owning_epoch(partition, from), from.0, to.0)
            .await
    }
}

/// The leader epoch that owned `offset` of `partition`: Kafka's
/// `epochForOffset` over the leader-epoch checkpoint, or the current leader
/// epoch when the checkpoint has no entry for it.
///
/// The checkpoint is cut only from its end and by an established log start,
/// never by local eviction, so a tiered offset still finds the epoch it was
/// copied under.
fn owning_epoch(partition: &Partition, offset: Offset) -> LeaderEpoch {
    let current = LeaderEpoch(
        partition
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire),
    );
    partition
        .log
        .lock()
        .expect("log mutex poisoned")
        .epoch_checkpoint()
        .epoch_for_offset(offset)
        .unwrap_or(current)
}
