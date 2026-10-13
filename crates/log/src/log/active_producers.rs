//! The producer state that `DescribeProducers` reports and that log compaction
//! keeps, and the expiry of the producers that stopped writing.
//!
//! Kafka keeps one producer state for each partition log. Every replica
//! updates it for each batch that it appends: a leader for a client batch, and
//! a follower for a batch that it replicates. `ReplicaManager
//! .activeProducerState` answers `DescribeProducers` from that state on every
//! replica that hosts the partition, through `Partition.activeProducerState`
//! and `UnifiedLog.activeProducers`. The log cleaner of every replica reads the
//! same state: `Cleaner.cleanSegments` takes `UnifiedLog
//! .lastRecordsOfActiveProducers`. `ProducerStateManager
//! .removeExpiredProducers` removes a producer when `producer.id.expiration.ms`
//! has passed since its last write and it has no open transaction.

use {
    super::Log,
    crate::compact::ProducerLastRecord,
    krabka_ids::{Offset, ProducerId},
    std::collections::HashMap,
};

/// One producer as `DescribeProducers` reports it.
///
/// This is Kafka's `DescribeProducersResponseData.ProducerState`, which
/// `UnifiedLog.activeProducers` fills from a `ProducerStateEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveProducer {
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    /// The last sequence of the last data batch of the producer at
    /// `producer_epoch`, or `-1` when the log holds no such batch.
    pub last_sequence: i32,
    /// The max timestamp of the last data batch of the producer, or the
    /// timestamp of the transaction marker that came after it.
    pub last_timestamp: i64,
    /// The coordinator epoch of the last transaction marker of the producer,
    /// or `-1` before its first marker.
    pub coordinator_epoch: i32,
    /// The first offset of the open transaction of the producer, or `None`
    /// when it has no open transaction.
    pub current_txn_start_offset: Option<Offset>,
}

impl Log {
    /// Every producer that this log holds state for, in producer id order.
    ///
    /// This is Kafka's `UnifiedLog.activeProducers`. An append from a client,
    /// a replicated append and a reopen all update the same state, so a
    /// leader and its followers give the same answer at the same log end.
    #[must_use]
    pub fn active_producers(&self) -> Vec<ActiveProducer> {
        let mut producers: Vec<ActiveProducer> = self
            .producer_state
            .values()
            .map(|entry| ActiveProducer {
                producer_id: entry.producer_id,
                producer_epoch: entry.producer_epoch,
                last_sequence: entry.last_sequence,
                last_timestamp: entry.timestamp,
                coordinator_epoch: entry.coordinator_epoch,
                current_txn_start_offset: entry.current_txn_first_offset,
            })
            .collect();
        producers.sort_unstable_by_key(|producer| producer.producer_id);
        producers
    }

    /// The last record of every producer that this log holds state for. A
    /// compaction pass keeps that record, so that the producer state
    /// survives the pass.
    ///
    /// This is Kafka's `UnifiedLog.lastRecordsOfActiveProducers`: the last
    /// offset of the last data batch of the producer, and its epoch. A
    /// producer has no last data offset when a transaction marker at a new
    /// epoch has cleared its batches (transaction version 2), or when it has
    /// written only markers. Every append path updates this state, so a
    /// leader and a follower give the same answer at the same log end.
    pub(crate) fn last_records_of_active_producers(
        &self,
    ) -> HashMap<ProducerId, ProducerLastRecord> {
        self.producer_state
            .values()
            .map(|entry| {
                let last_record = ProducerLastRecord {
                    last_data_offset: entry.last_batch().map(|batch| batch.last_offset),
                    producer_epoch: entry.producer_epoch,
                };
                (entry.producer_id, last_record)
            })
            .collect()
    }

    /// Remove every producer that has no open transaction and whose last
    /// timestamp is `expiration_ms` or more before `now_ms`. Also remove the
    /// transaction verification state that is that old.
    ///
    /// This is Kafka's `ProducerStateManager.removeExpiredProducers` with
    /// `isProducerExpired`, for `producer.id.expiration.ms`. The next
    /// producer-state snapshot does not hold a removed producer.
    pub fn remove_expired_producers(&mut self, now_ms: i64, expiration_ms: i64) {
        let expired: Vec<ProducerId> = self
            .producer_state
            .values()
            .filter(|entry| {
                entry.current_txn_first_offset.is_none()
                    && now_ms.saturating_sub(entry.timestamp) >= expiration_ms
            })
            .map(|entry| entry.producer_id)
            .collect();
        for producer_id in expired {
            self.producer_state.remove(&producer_id);
            self.earlier_batches.remove(&producer_id);
            // Kafka keeps the coordinator epoch in the `ProducerStateEntry`,
            // so it goes with the expired producer.
            self.coordinator_epochs.remove(&producer_id);
        }
        self.verification_states
            .retain(|_, state| now_ms.saturating_sub(state.created_ms) < expiration_ms);
    }
}

#[cfg(test)]
mod tests;
