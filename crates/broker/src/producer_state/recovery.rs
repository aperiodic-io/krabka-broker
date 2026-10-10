//! Installation of producer state that was rebuilt from a partition's durable
//! log.
//!
//! Startup, follower-prefix hydration and a promotion to leader call these
//! functions before a partition becomes request-visible, so a recovered
//! `ProducerState` carries the sequence and epoch state that survived a
//! restart or a remote-tier copy, or that a follower replicated.

use std::{collections::HashMap, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::ProducerId;
use tokio::sync::Mutex;

use super::{PartitionProducerState, ProducerEntry, ProducerState, RetainedBatch};

impl ProducerState {
    /// Replace one partition's producer sequence state with the state rebuilt
    /// from its durable log.
    ///
    /// Startup calls this for disk-backed and diskless partitions before the
    /// partition writer starts. `Log::open` has already loaded the latest
    /// valid Kafka-compatible producer snapshot and replayed its uncovered
    /// tail, including state whose source segment was removed locally after
    /// remote-tier copy.
    ///
    /// # Errors
    /// This currently cannot fail; the result remains fallible so startup can
    /// preserve its existing error boundary if snapshot projection gains a
    /// checked conversion later.
    pub async fn rebuild_from_log(
        &self,
        topic: &str,
        partition: PartitionIndex,
        log: &krabka_log::Log,
    ) -> Result<(), krabka_log::LogError> {
        self.rebuild_from_snapshot(topic, partition, log.recovered_producers())
            .await;
        Ok(())
    }

    pub(crate) async fn rebuild_from_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<krabka_log::RecoveredProducer>,
    ) {
        self.handle(topic, partition).lock().await.entries = entries_from_snapshot(snapshot);
    }

    /// Install recovered producer state before a partition becomes
    /// request-visible as leader.
    ///
    /// Unlike [`Self::rebuild_from_snapshot`], this replaces the map handle
    /// synchronously. The partition does not exist in `PartitionRegistry` yet,
    /// so no request can have acquired the new handle. Vacant materialization
    /// can therefore make follower-prefix hydration and idempotent-producer
    /// recovery one atomic publication boundary from the request path's point
    /// of view.
    pub(crate) fn install_snapshot_before_materialization(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<krabka_log::RecoveredProducer>,
    ) {
        let parts = self.topic_partitions(topic);
        parts.insert(
            partition,
            Arc::new(Mutex::new(PartitionProducerState {
                entries: entries_from_snapshot(snapshot),
            })),
        );
    }

    /// Copy the log's producer entries into the tracker after the partition
    /// writer appended a control batch for those producers.
    ///
    /// The log applies a transaction marker to its own producer state. A
    /// marker at a new producer epoch (transaction version 2) clears the
    /// retained batch, so the next batch at that epoch must start at sequence
    /// 0. The tracker takes the same projection that recovery takes, so a
    /// produce after the marker gets the same decision before and after a
    /// restart.
    pub(crate) async fn mirror_log_entries(
        &self,
        topic: &str,
        partition: PartitionIndex,
        entries: Vec<krabka_log::ProducerSnapshotEntry>,
    ) {
        if entries.is_empty() {
            return;
        }
        let handle = self.handle(topic, partition);
        let mut state = handle.lock().await;
        for entry in entries {
            let mut mirrored = entry_from_snapshot(entry);
            // The log keeps only the last batch. A marker that leaves the
            // producer at its epoch and its last batch (transaction version 1)
            // keeps the earlier batches too, as Kafka's retained batches do.
            // The marker moves the log entry's timestamp to its own, but
            // Kafka's `ProducerStateEntry.update` for a marker keeps the
            // retained `BatchMetadata`, with the data batch's timestamp.
            if let Some(tracked) = state.entries.get(&entry.producer_id)
                && tracked.epoch == mirrored.epoch
                && tracked.last_sequence == mirrored.last_sequence
                && tracked.base_offset == mirrored.base_offset
                && tracked.last_offset == mirrored.last_offset
            {
                mirrored.earlier = tracked.earlier;
                mirrored.last_timestamp = tracked.last_timestamp;
            }
            state.entries.insert(entry.producer_id, mirrored);
        }
    }
}

fn entries_from_snapshot(
    snapshot: Vec<krabka_log::RecoveredProducer>,
) -> HashMap<ProducerId, ProducerEntry> {
    snapshot
        .into_iter()
        .map(|recovered| {
            let mut entry = entry_from_snapshot(recovered.entry);
            entry.earlier = earlier_from_log(&recovered.earlier);
            (recovered.entry.producer_id, entry)
        })
        .collect()
}

/// The tracker's earlier batches for the log's retained batches before the
/// last one, oldest first. Kafka's `rebuildProducerState` replays each
/// batch through `ProducerStateEntry.addBatch`, so a reopen retains the
/// snapshot's batch and the replayed tail, up to five in all.
fn earlier_from_log(earlier: &[krabka_log::ProducerBatchMetadata]) -> super::entry::EarlierBatches {
    let mut slots = super::NO_EARLIER_BATCHES;
    let skip = earlier.len().saturating_sub(slots.len());
    for (slot, batch) in slots.iter_mut().zip(&earlier[skip..]) {
        *slot = Some(RetainedBatch {
            base_sequence: krabka_verified::decrement_sequence(
                batch.last_sequence,
                batch.offset_delta,
            ),
            last_sequence: batch.last_sequence,
            base_offset: batch.last_offset.0 - i64::from(batch.offset_delta),
            last_offset: batch.last_offset.0,
            timestamp: batch.timestamp,
        });
    }
    slots
}

/// The tracker entry for one log producer entry.
fn entry_from_snapshot(entry: krabka_log::ProducerSnapshotEntry) -> ProducerEntry {
    let base_offset = if entry.last_offset >= 0 {
        entry.last_offset.0 - i64::from(entry.offset_delta)
    } else {
        // A marker-only producer has no retained data batch.
        -1
    };
    ProducerEntry {
        epoch: entry.producer_epoch,
        last_sequence: entry.last_sequence,
        last_offset: entry.last_offset.0,
        base_offset,
        last_timestamp: entry.timestamp,
        entry_timestamp: entry.timestamp,
        current_txn_first_offset: entry.current_txn_first_offset.map(|offset| offset.0),
        // The caller fills the earlier batches the log retained, if any.
        earlier: super::NO_EARLIER_BATCHES,
    }
}
