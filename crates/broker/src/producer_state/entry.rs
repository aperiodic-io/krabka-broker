//! The per-producer sequence record and the per-partition map that holds it.
//!
//! `ProducerEntry` is one idempotent producer's last-accepted-batch state on a
//! partition, and `PartitionProducerState` is the map of those records that a
//! per-partition mutex guards. They sit in their own file because every other
//! part of the tracker reads or writes them.

use {crate::partition::LogOffset, krabka_log::ProducerId, std::collections::HashMap};

/// Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`: the number of a
/// producer's most recent batches whose retry answers as a duplicate.
pub const NUM_BATCHES_TO_RETAIN: usize = 5;

/// One earlier accepted batch of a producer, kept so that a retry of it
/// answers as a duplicate. Kafka's `BatchMetadata`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetainedBatch {
    pub base_sequence: i32,
    pub last_sequence: i32,
    pub base_offset: LogOffset,
    pub last_offset: LogOffset,
    /// The batch's max timestamp, as the log stored it.
    pub timestamp: i64,
}

/// The batches a producer accepted before its last one, oldest first, at the
/// producer's current epoch. The last batch lives in the entry's own fields.
pub type EarlierBatches = [Option<RetainedBatch>; NUM_BATCHES_TO_RETAIN - 1];

/// No earlier batch: a new producer, a new epoch, or recovered state.
pub const NO_EARLIER_BATCHES: EarlierBatches = [None; NUM_BATCHES_TO_RETAIN - 1];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Last absolute offset of the last accepted batch for this producer
    /// (`base_offset + last_offset_delta`).
    /// [`ProducerState::truncate`](super::ProducerState::truncate) reads it to
    /// drop entries whose batch was truncated off the log.
    pub last_offset: LogOffset,
    pub base_offset: LogOffset,
    /// Timestamp of the last accepted batch for this producer.
    pub last_timestamp: i64,
    /// Kafka's `ProducerStateEntry.lastTimestamp`: the max timestamp of the
    /// producer's last data batch, or the timestamp of the transaction marker
    /// that followed it.
    /// [`ProducerState::expire_older_than`](super::ProducerState::expire_older_than)
    /// ages the entry by it, as `ProducerStateManager.isProducerExpired` does.
    pub entry_timestamp: i64,
    /// Kafka's `ProducerStateEntry.currentTxnFirstOffset`: the first offset
    /// of the producer's open transaction on the partition, or `None` when
    /// no transaction is open. A producer with an open transaction never
    /// expires.
    pub current_txn_first_offset: Option<LogOffset>,
    /// Up to four batches accepted before the last one, at `epoch`, oldest
    /// first. With the last batch they are Kafka's five retained batches.
    pub earlier: EarlierBatches,
}

impl ProducerEntry {
    /// Kafka's `ProducerStateManager.isProducerExpired`: no open transaction,
    /// and the entry's timestamp is at least `expiration_ms` behind `now_ms`.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64, expiration_ms: i64) -> bool {
        self.current_txn_first_offset.is_none()
            && now_ms.saturating_sub(self.entry_timestamp) >= expiration_ms
    }

    /// The last accepted batch, or `None` for a marker-only entry.
    #[must_use]
    pub fn last_batch(&self) -> Option<RetainedBatch> {
        let delta = i32::try_from(self.last_offset.checked_sub(self.base_offset)?).ok()?;
        (self.last_offset >= 0).then(|| RetainedBatch {
            base_sequence: krabka_verified::decrement_sequence(self.last_sequence, delta),
            last_sequence: self.last_sequence,
            base_offset: self.base_offset,
            last_offset: self.last_offset,
            timestamp: self.last_timestamp,
        })
    }

    /// Kafka's retained batches for this entry: the earlier batches, oldest
    /// first, then the last batch. Every occupied slot is at `epoch`.
    #[must_use]
    pub fn retained_batches(&self) -> [Option<RetainedBatch>; NUM_BATCHES_TO_RETAIN] {
        let mut retained = [None; NUM_BATCHES_TO_RETAIN];
        retained[..NUM_BATCHES_TO_RETAIN - 1].copy_from_slice(&self.earlier);
        retained[NUM_BATCHES_TO_RETAIN - 1] = self.last_batch();
        retained
    }
}

/// Whether a completed append is accepted, and the earlier batches it leaves
/// retained when it is.
pub(crate) struct Completion {
    pub(crate) accepted: bool,
    pub(crate) earlier: EarlierBatches,
}

/// The shared live/model completion projection, keeping metadata by physical
/// offset instead of asynchronous acknowledgement order.
pub(crate) fn earlier_after_completion(
    existing: Option<ProducerEntry>,
    epoch: i16,
    incoming: RetainedBatch,
) -> Completion {
    if existing.is_none() {
        return Completion {
            accepted: true,
            earlier: NO_EARLIER_BATCHES,
        };
    }
    let mut retained = [incoming; NUM_BATCHES_TO_RETAIN];
    let mut ends = [0; NUM_BATCHES_TO_RETAIN];
    let mut count = 0;
    for batch in existing
        .into_iter()
        .flat_map(|entry| entry.retained_batches().into_iter().flatten())
    {
        retained[count] = batch;
        ends[count] = batch.last_offset;
        count += 1;
    }
    let (accepted, selected) = krabka_verified::producer::producer_completion_window(
        existing.map(|entry| entry.epoch),
        epoch,
        &ends[..count],
        incoming.last_offset,
    );
    let mut earlier = NO_EARLIER_BATCHES;
    if accepted {
        for (slot, &source) in selected.iter().take(selected.len() - 1).enumerate() {
            earlier[slot] = Some(if source == count {
                incoming
            } else {
                retained[source]
            });
        }
    }
    Completion { accepted, earlier }
}

#[derive(Debug, Default)]
pub struct PartitionProducerState {
    pub entries: HashMap<ProducerId, ProducerEntry>,
}
