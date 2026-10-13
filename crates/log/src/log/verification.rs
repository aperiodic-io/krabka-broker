//! KIP-890 part 1: the per-producer verification state that ties a
//! transactional append to a transaction the coordinator knows.
//!
//! Before a transactional batch starts a transaction on this partition, the
//! broker asks the transaction coordinator to verify (or, with transaction
//! version 2, to add) the partition. The log hands out a
//! [`VerificationGuard`] when that check starts and keeps it until the next
//! transactional append or end marker of the producer. An append must present
//! the same guard. A marker that lands between the check and the append clears
//! the guard, so a stale check can never start a transaction the coordinator
//! already ended.
//!
//! This is Kafka's `VerificationStateEntry`, `VerificationGuard`,
//! `UnifiedLog.maybeStartTransactionVerification` and the
//! `batchMissingRequiredVerification` check in
//! `UnifiedLog.analyzeAndValidateProducerState`.

use {
    super::Log,
    krabka_ids::ProducerId,
    std::sync::atomic::{AtomicU64, Ordering},
};

/// A token for one verification of one producer on one partition.
///
/// [`VerificationGuard::SENTINEL`] verifies nothing. Kafka's
/// `VerificationGuard.SENTINEL` has the value 0 and every real guard takes the
/// next value of a process-wide counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VerificationGuard(u64);

impl VerificationGuard {
    /// The guard that verifies nothing.
    pub const SENTINEL: Self = Self(0);

    fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// Whether `presented` is this guard and is not the sentinel. Kafka's
    /// `VerificationGuard.verify`.
    #[must_use]
    pub fn verifies(self, presented: Self) -> bool {
        presented != Self::SENTINEL && presented == self
    }
}

/// The verification state of one producer. Kafka's `VerificationStateEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VerificationState {
    pub(super) guard: VerificationGuard,
    pub(super) created_ms: i64,
    pub(super) epoch: i16,
    pub(super) lowest_sequence: i32,
    pub(super) supports_epoch_bump: bool,
}

/// Why a transactional produce may not start or continue its append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionAppendRefusal {
    /// The batch epoch is below the producer's epoch on this partition. Kafka
    /// throws `InvalidProducerEpochException`.
    StaleProducerEpoch,
    /// A transactional batch has no open transaction and no matching guard, or
    /// a non-transactional batch comes from a producer with an open
    /// transaction. Kafka throws `InvalidTxnStateException`.
    InvalidTransactionState,
    /// The first sequence of a verified batch does not fit the verification
    /// state. Kafka throws `OutOfOrderSequenceException` from
    /// `ProducerAppendInfo.checkSequence`.
    OutOfOrderSequence,
}

/// One batch as the transaction checks see it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionalBatch {
    pub producer_id: ProducerId,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub is_transactional: bool,
    pub is_control: bool,
}

impl Log {
    /// Whether `producer_id` has an open transaction on this partition at
    /// `producer_epoch`. Kafka's `UnifiedLog.hasOngoingTransaction`.
    #[must_use]
    pub fn has_ongoing_transaction(&self, producer_id: ProducerId, producer_epoch: i16) -> bool {
        self.producer_state.get(&producer_id).is_some_and(|entry| {
            entry.current_txn_first_offset.is_some() && entry.producer_epoch == producer_epoch
        })
    }

    /// Start a verification for a transactional batch, and return its guard.
    ///
    /// A producer with an open transaction at the batch epoch needs no
    /// verification, and gets [`VerificationGuard::SENTINEL`]. Otherwise the
    /// producer's guard is created, or kept with its lowest sequence moved as
    /// Kafka's `maybeUpdateLowestSequenceAndEpoch` moves it. The call also
    /// drops verification state older than `expiration_ms`, as Kafka's
    /// `ProducerStateManager.removeExpiredProducers` does.
    ///
    /// # Errors
    ///
    /// Returns [`TransactionAppendRefusal::StaleProducerEpoch`] when the batch
    /// epoch is below the producer's epoch on this partition.
    pub fn maybe_start_transaction_verification(
        &mut self,
        batch: TransactionalBatch,
        supports_epoch_bump: bool,
        (now_ms, expiration_ms): (i64, i64),
    ) -> Result<VerificationGuard, TransactionAppendRefusal> {
        let TransactionalBatch {
            producer_id,
            producer_epoch,
            base_sequence,
            ..
        } = batch;
        if self
            .producer_state
            .get(&producer_id)
            .is_some_and(|entry| producer_epoch < entry.producer_epoch)
        {
            return Err(TransactionAppendRefusal::StaleProducerEpoch);
        }
        if self.has_ongoing_transaction(producer_id, producer_epoch) {
            return Ok(VerificationGuard::SENTINEL);
        }
        self.verification_states
            .retain(|_, state| now_ms.saturating_sub(state.created_ms) < expiration_ms);
        let state = self
            .verification_states
            .entry(producer_id)
            .or_insert_with(|| VerificationState {
                guard: VerificationGuard::next(),
                created_ms: now_ms,
                epoch: producer_epoch,
                lowest_sequence: base_sequence,
                supports_epoch_bump,
            });
        if producer_epoch == state.epoch && base_sequence < state.lowest_sequence {
            state.lowest_sequence = base_sequence;
        }
        if producer_epoch > state.epoch {
            state.epoch = producer_epoch;
            state.lowest_sequence = base_sequence;
        }
        Ok(state.guard)
    }

    /// Check a batch against the producer's transaction state just before it
    /// appends, under the same lock as the append.
    ///
    /// A transactional data batch with no open transaction at its epoch must
    /// present the producer's current guard. A non-transactional data batch
    /// from a producer with an open transaction is refused. This is Kafka's
    /// check in `UnifiedLog.analyzeAndValidateProducerState` and in
    /// `ProducerAppendInfo.appendDataBatch`.
    ///
    /// # Errors
    ///
    /// Returns the refusal that Kafka's append throws.
    pub fn check_transactional_append(
        &self,
        batch: TransactionalBatch,
        presented: VerificationGuard,
    ) -> Result<(), TransactionAppendRefusal> {
        if batch.producer_id.get() < 0 || batch.is_control {
            return Ok(());
        }
        let entry = self.producer_state.get(&batch.producer_id);
        if !batch.is_transactional {
            return if entry.is_some_and(|entry| entry.current_txn_first_offset.is_some()) {
                Err(TransactionAppendRefusal::InvalidTransactionState)
            } else {
                Ok(())
            };
        }
        if self.has_ongoing_transaction(batch.producer_id, batch.producer_epoch) {
            return Ok(());
        }
        if entry.is_some_and(|entry| batch.producer_epoch < entry.producer_epoch) {
            return Err(TransactionAppendRefusal::StaleProducerEpoch);
        }
        let Some(state) = self
            .verification_states
            .get(&batch.producer_id)
            .filter(|state| state.guard.verifies(presented))
        else {
            return Err(TransactionAppendRefusal::InvalidTransactionState);
        };
        // Kafka's `ProducerAppendInfo.checkSequence` with a verification
        // state: a transaction version 2 producer with no retained batch on
        // the partition starts at 0, and no batch may start above the lowest
        // sequence a verification saw.
        let has_batch = entry.is_some_and(|entry| entry.last_offset.0 >= 0);
        if (state.supports_epoch_bump && batch.base_sequence != 0 && !has_batch)
            || batch.base_sequence > state.lowest_sequence
        {
            return Err(TransactionAppendRefusal::OutOfOrderSequence);
        }
        Ok(())
    }

    /// Drop the verification state after a transactional append, as Kafka's
    /// `UnifiedLog.updateProducers` does. A data batch now has an open
    /// transaction, and a marker ended one. The one exception is a transaction
    /// version 2 marker at the verified epoch: the producer already started
    /// its next transaction at that epoch, so its guard stays.
    pub(super) fn clear_verification_after_append(
        &mut self,
        producer_id: ProducerId,
        producer_epoch: i16,
        is_control: bool,
    ) {
        let next_transaction_started =
            self.verification_states
                .get(&producer_id)
                .is_some_and(|state| {
                    state.supports_epoch_bump && is_control && producer_epoch == state.epoch
                });
        if !next_transaction_started {
            self.verification_states.remove(&producer_id);
        }
    }
}

#[cfg(test)]
mod tests;
