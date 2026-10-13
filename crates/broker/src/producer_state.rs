//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.
//!
//! This tracker is a second copy of the producer state that the partition log
//! keeps (`krabka_log::Log::recovered_producers`). The produce path reads only
//! this copy. A follower copies the transaction markers that it replicates
//! into it, but not the data batches, so the copy is complete only on a
//! leader. `Partition::install_local_leadership` replaces it with the state of
//! the log on each promotion. A reader that must also answer on a follower
//! reads the log instead: `DescribeProducers`, and the log cleaner, which
//! keeps the last record of each producer.

use {
    crate::partition::LogOffset, dashmap::DashMap, krabka_ids::PartitionIndex,
    krabka_log::ProducerId, krabka_verified::increment_sequence, std::sync::Arc,
    tokio::sync::Mutex,
};

#[cfg(test)]
mod completed_commit;
mod decision;
mod entry;
mod expiry;
mod recovery;
#[cfg(test)]
mod replication_mirror;
#[cfg(test)]
mod restart_agreement;
#[cfg(test)]
mod snapshot_tail;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod truncation_replay;

#[cfg(test)]
pub(crate) use self::decision::check_pure;
pub use self::{
    decision::{Checked, Decision, SequenceContext},
    entry::{NO_EARLIER_BATCHES, PartitionProducerState, ProducerEntry, RetainedBatch},
};

/// Per-partition idempotent-producer state, nested under the owning
/// topic. The partition index (`i32`, `Copy`) is the key, so per-call
/// lookups allocate nothing. The outer topic map is keyed by `String`, but
/// its `get`/`entry` accept a borrowed `&str`. That map allocates the owned
/// topic key only on the first produce to a topic it has not seen before.
type PartitionMap = DashMap<PartitionIndex, Arc<Mutex<PartitionProducerState>>>;

#[derive(Debug, Default)]
pub struct ProducerState {
    by_topic: Arc<DashMap<String, Arc<PartitionMap>>>,
}

impl ProducerState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_topic: Arc::new(DashMap::new()),
        }
    }

    /// Commit a successful append into the tracker.
    ///
    /// A completion at an older epoch preserves the tracked entry. At the
    /// same epoch, completions merge by physical offset into the latest five
    /// distinct batches; a repeated completion keeps the existing metadata.
    /// Earlier completions never replace the latest sequence, timestamp or
    /// transaction state. `AppendCommit::record` defers this
    /// call until the `acks=all` high-watermark gate for its own append
    /// resolves, so it can run long after the append itself, on a task
    /// unrelated to the writer's serial handling of later messages on the
    /// same partition. [`Self::mirror_log_entries`] mirrors a transaction
    /// marker's epoch bump as soon as the marker is durable, with no such
    /// wait. A commit for a batch from before that marker can therefore
    /// resolve after the mirror already ran, and an unconditional overwrite
    /// would put the pre-marker epoch back, undoing the fence. This is
    /// Kafka's own invariant: `ProducerAppendInfo.checkProducerEpoch` never
    /// lets a producer's tracked epoch move backward.
    ///
    /// `append` is the batch's base offset, its max timestamp and whether it
    /// is transactional. A transactional batch opens a transaction when none
    /// is open, as Kafka's `ProducerAppendInfo.appendDataBatch` does. A
    /// commit that resolves after the tracked entry already holds a later
    /// position of the same epoch reopens nothing: the marker mirrored in
    /// between may have closed that batch's transaction.
    pub async fn commit(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer: (i64, i16),
        sequence: (i32, i32),
        append: (LogOffset, i64, bool),
    ) {
        let (producer_id, producer_epoch) = producer;
        let (base_sequence, last_offset_delta) = sequence;
        let (base_offset, last_timestamp, is_transactional) = append;
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let existing = s.entries.get(&ProducerId(producer_id)).copied();
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
        let last_offset = base_offset + i64::from(last_offset_delta);
        let incoming = RetainedBatch {
            base_sequence: krabka_verified::decrement_sequence(last_sequence, last_offset_delta),
            last_sequence,
            base_offset,
            last_offset,
            timestamp: last_timestamp,
        };
        let (accepted, earlier) =
            entry::earlier_after_completion(existing, producer_epoch, incoming);
        if !accepted {
            return;
        }
        if let Some(mut tracked) = existing
            && tracked.epoch == producer_epoch
            && tracked.last_offset >= last_offset
        {
            // Deferred completions can fill an earlier slot, but never undo
            // newer sequence, timestamp or transaction-marker state.
            tracked.earlier = earlier;
            s.entries.insert(ProducerId(producer_id), tracked);
            return;
        }
        let current_txn_first_offset = existing
            .and_then(|entry| entry.current_txn_first_offset)
            .or_else(|| is_transactional.then_some(base_offset));
        s.entries.insert(ProducerId(producer_id), ProducerEntry {
            epoch: producer_epoch,
            last_sequence,
            last_offset,
            base_offset,
            last_timestamp,
            entry_timestamp: last_timestamp,
            current_txn_first_offset,
            earlier,
        });
    }

    /// Drop idempotent-producer entries whose last accepted batch was
    /// truncated off the log, that is `last_offset >= offset`. Also drop
    /// every marker-only entry, whatever `offset` is.
    ///
    /// The broker calls this after it truncates the partition log below the
    /// recorded batch. Two paths do that: KIP-320 divergence truncation on
    /// rejoin, and an `OFFSET_OUT_OF_RANGE` reset.
    ///
    /// Without this call, the broker deduplicates a producer that retries a
    /// batch from the truncated tail against a `base_offset` that is no longer
    /// in the log. The `acks=all` HW gate
    /// (`await_hw_at_least(base_offset + delta + 1)`) then waits forever for a
    /// high watermark that can never reach the truncated offset. That is a
    /// permanent produce stall after failover. When this function drops the
    /// entry, the retry re-appends fresh instead. This mirrors Kafka's
    /// `ProducerStateManager.truncateAndReload`. It does not create state for a
    /// partition that the broker has never tracked.
    ///
    /// A marker-only entry (`last_offset < 0`, installed by
    /// [`Self::mirror_log_entries`] after a transaction-version-2 marker
    /// clears the retained batch) carries no offset of its own, so the
    /// `last_offset >= offset` test cannot place it relative to the cut. Kafka
    /// clears the batch metadata at the marker, not at a stored offset, so
    /// this tracker cannot tell whether the marker itself survived a
    /// divergent-tail truncation or was the very record that diverged. Every
    /// marker-only entry is therefore dropped on any truncation of its
    /// partition. Dropping is always safe: the next batch from that producer
    /// is then treated the way an unknown producer's first batch is treated,
    /// which never wrongly deduplicates or wrongly accepts a sequence. A
    /// restart or a promotion rebuilds the exact state from the (correctly
    /// truncated) log through [`Self::rebuild_from_log`].
    pub async fn truncate(&self, topic: &str, partition: PartitionIndex, offset: LogOffset) {
        let Some(parts) = self.by_topic.get(topic).map(|e| e.value().clone()) else {
            return;
        };
        let Some(handle) = parts.get(&partition).map(|e| e.value().clone()) else {
            return;
        };
        let mut s = handle.lock().await;
        s.entries
            .retain(|_pid, e| e.last_offset >= 0 && e.last_offset < offset);
        // Every earlier batch ends below the last batch, so a kept entry keeps
        // all of them.
    }

    /// Resolve the per-partition state handle, and create it on a miss.
    ///
    /// The outer topic lookup borrows `&str`. It allocates an owned `String`
    /// key only on the first lookup of that topic. The inner partition lookup
    /// is keyed by `i32` and never allocates.
    fn handle(&self, topic: &str, partition: PartitionIndex) -> Arc<Mutex<PartitionProducerState>> {
        // `get` first to avoid allocating the topic `String` on the hot
        // path (the topic almost always already exists).
        let parts = self.topic_partitions(topic);
        parts
            .entry(partition)
            .or_insert_with(|| Arc::new(Mutex::new(PartitionProducerState::default())))
            .value()
            .clone()
    }

    /// Get the topic's partition map without allocating its key on a hit.
    fn topic_partitions(&self, topic: &str) -> Arc<PartitionMap> {
        if let Some(existing) = self.by_topic.get(topic) {
            existing.value().clone()
        } else {
            self.by_topic
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone()
        }
    }

    /// Read-only snapshot of every tracked producer entry on
    /// `(topic, partition)`, for tests.
    ///
    /// This function returns an empty list when the partition has no entries.
    /// `DescribeProducers` does not read the tracker: it answers from the
    /// producer state of the partition log, which a follower also updates.
    #[cfg(test)]
    pub async fn snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
    ) -> Vec<(i64, ProducerEntry)> {
        // Cheaper to bypass `handle` (which inserts on miss): a snapshot
        // for an unknown partition should report "no producers", not
        // wire up an empty entry. The borrowed `&str` / `i32` lookups
        // allocate nothing and map a miss to an empty result.
        let Some(topic_ref) = self.by_topic.get(topic) else {
            return Vec::new();
        };
        let parts = topic_ref.value().clone();
        drop(topic_ref);
        let Some(part_ref) = parts.get(&partition) else {
            return Vec::new();
        };
        let handle = part_ref.value().clone();
        drop(part_ref);
        let state = handle.lock().await;
        state
            .entries
            .iter()
            .map(|(pid, e)| (pid.get(), *e))
            .collect()
    }
}

#[cfg(test)]
#[path = "producer_state_model.rs"]
mod producer_state_model;
