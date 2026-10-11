//! Completion of transactions whose `Prepare*` record is durable.
//!
//! Kafka's coordinator answers `EndTxn` with `NONE` as soon as the
//! `PrepareCommit` or `PrepareAbort` record is in the transaction log. Its
//! `TransactionMarkerChannelManager` then writes the markers and the
//! `Complete*` record, and it retries until that work is done or the
//! coordinator loses the partition. `TransactionStateManager` hands every
//! `Prepare*` transaction it loads to the same channel.
//!
//! This module is that channel. `EndTxn` completes a transaction inline when
//! it can. When the broker stops between the two appends, or the marker
//! fan-out or the `Complete*` append fails, the transaction stays in
//! `Prepare*`. Recovery and the failed request queue the transactional id
//! here, and [`crate::txn::completion`] retries it until it completes.

use krabka_log::ProducerId;
use krabka_verified::transaction::{
    TransactionReaperCompletionDecision as CompletionDecision, TransactionSnapshot,
};
use tracing::{info, warn};

use super::TxnCoordinator;
use crate::txn::{
    handlers::end_txn::completion_producer_identity,
    marker::{MarkerType, classify_marker_failure},
    state::{TxnEntry, TxnState},
    util::now_millis,
    version::TxnVersion,
};

/// The result of one attempt to complete a prepared transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionAttempt {
    /// The `Complete*` record is durable, from this attempt or an earlier one.
    Completed,
    /// There is nothing for this broker to complete: the transactional id is
    /// unknown, it is not in a `Prepare*` state, this broker does not
    /// coordinate it, or another caller changed it.
    NothingToComplete,
    /// The marker fan-out or the `Complete*` append failed. Try again later.
    Retry,
}

/// The `(marker, complete state)` that finish a transaction in `prepare`, or
/// `None` when `prepare` is not a `Prepare*` state.
pub(crate) fn completion_for(prepare: TxnState) -> Option<(MarkerType, TxnState)> {
    match prepare {
        TxnState::PrepareCommit => Some((MarkerType::Commit, TxnState::CompleteCommit)),
        TxnState::PrepareAbort => Some((MarkerType::Abort, TxnState::CompleteAbort)),
        TxnState::Empty
        | TxnState::Ongoing
        | TxnState::CompleteCommit
        | TxnState::CompleteAbort
        | TxnState::Dead => None,
    }
}

/// Apply `Prepare* → complete` with the new identity `(new_pid, new_epoch)`.
/// The prior producer ID is recorded only when the identity rotated to a new
/// producer ID. Kafka's `prepareComplete` also clears `hasFailedEpochFence`,
/// because the abort that fence prepared has now been written.
pub(crate) fn apply_completion(
    entry: &mut TxnEntry,
    complete: TxnState,
    (new_pid, new_epoch): (ProducerId, i16),
    now_ms: i64,
) {
    if new_pid != entry.producer_id {
        entry.prev_producer_id = entry.producer_id;
    }
    entry.state = complete;
    entry.producer_id = new_pid;
    entry.producer_epoch = new_epoch;
    entry.next_producer_id = ProducerId(-1);
    entry.next_producer_epoch = -1;
    entry.has_failed_epoch_fence = false;
    entry.partitions.clear();
    entry.last_update_ms = now_ms;
}

/// Recheck the complete prepared snapshot after the marker fan-out. Comparing
/// every persisted field prevents a concurrent registration, recovery-identity
/// change, timeout change, or generation change from being overwritten.
///
/// `(prepare, complete)` must be a pairing [`completion_for`] returns; the
/// verified kernel requires the two states to differ.
pub(crate) fn completion_decision(
    entry: &TxnEntry,
    prepared: &TxnEntry,
    (prepare, complete): (TxnState, TxnState),
) -> CompletionDecision {
    let (completion_pid, completion_epoch) = completion_producer_identity(prepared);
    krabka_verified::transaction_reaper_completion_decision(
        TransactionSnapshot {
            pid: entry.producer_id.get(),
            epoch: entry.producer_epoch,
            state: entry.state.to_kafka_status(),
        },
        TransactionSnapshot {
            pid: prepared.producer_id.get(),
            epoch: prepared.producer_epoch,
            state: prepare.to_kafka_status(),
        },
        TransactionSnapshot {
            pid: completion_pid.get(),
            epoch: completion_epoch,
            state: complete.to_kafka_status(),
        },
        entry == prepared,
    )
}

impl TxnCoordinator {
    /// Queue `transactional_id` for completion and wake the completion task.
    ///
    /// A caller whose marker fan-out or `Complete*` append failed after a
    /// durable `Prepare*` append calls this, and so does recovery for every
    /// `Prepare*` transaction it loads.
    pub(crate) fn request_completion(&self, transactional_id: &str) {
        self.pending_completions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(transactional_id.to_owned());
        self.completion_requested.notify_one();
    }

    /// Wait until a caller requests a completion. A request that arrives while
    /// nothing waits is kept, so the next wait returns at once.
    pub(crate) async fn completion_requested(&self) {
        self.completion_requested.notified().await;
    }

    /// Take every queued transactional id, in order.
    pub(crate) fn take_completion_requests(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .pending_completions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
        .into_iter()
        .collect()
    }

    /// Write the markers and the `Complete*` record for `transactional_id`
    /// when its current entry is `PrepareCommit` or `PrepareAbort`.
    ///
    /// The entry lock is not held across the marker fan-out. Before the
    /// `Complete*` append, the method holds the state-partition write lock and
    /// the entry lock, and it requires the entry to be the exact snapshot the
    /// markers were written for.
    ///
    /// `txnv` is the broker's current `transaction.version`, and it selects
    /// only the `Complete*` record's wire format. The record's
    /// `client_transaction_version` is not re-derived from it: `apply_completion`
    /// leaves the field the `Prepare*` record already stamped in place, since
    /// that is the version this transaction completes under, independent of
    /// whatever level the cluster has reached by the time completion runs.
    /// The marker fan-out reads the same stamp.
    // cargo-mutants: I/O over live entry locks, marker fan-out and log appends;
    // `completion_for`, `apply_completion` and `completion_decision` carry the
    // decisions and are tested on their own.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn complete_prepared_transaction(
        &self,
        transactional_id: &str,
        txnv: TxnVersion,
    ) -> CompletionAttempt {
        if !self.is_coordinator_for(transactional_id).await {
            return CompletionAttempt::NothingToComplete;
        }
        let Some(handle) = self.get(transactional_id) else {
            return CompletionAttempt::NothingToComplete;
        };
        let mut prepared = handle.lock().await.clone();
        let Some((marker, complete)) = completion_for(prepared.state) else {
            return CompletionAttempt::NothingToComplete;
        };
        if let Err(error) = self
            .dispatch_transaction_markers(&mut prepared, marker)
            .await
        {
            let class = classify_marker_failure(&error);
            if class.stops_fan_out() {
                // A fenced producer or coordinator generation has already
                // superseded this fan-out, or a leader answered a code
                // Kafka's completion handler throws on (#882): retrying here
                // can never succeed, and would otherwise loop forever at
                // `RETRY_BACKOFF`. Whatever superseded this generation, or
                // the coordinator's next load of the partition, completes the
                // transaction now.
                warn!(
                    tid = transactional_id,
                    %error,
                    ?class,
                    "transaction completion: marker fan-out cannot succeed; giving up"
                );
                return CompletionAttempt::NothingToComplete;
            }
            warn!(
                tid = transactional_id,
                %error,
                "transaction completion: marker fan-out failed; will retry"
            );
            return CompletionAttempt::Retry;
        }

        let _state_partition_write = self.lock_state_partition_for(transactional_id).await;
        let Some(handle) = self.get(transactional_id) else {
            return CompletionAttempt::NothingToComplete;
        };
        let entry = handle.lock().await;
        if !self.is_current_entry(transactional_id, &handle) {
            return CompletionAttempt::Retry;
        }
        match completion_decision(&entry, &prepared, (prepared.state, complete)) {
            CompletionDecision::AlreadyComplete => CompletionAttempt::Completed,
            CompletionDecision::RejectMalformed
            | CompletionDecision::RejectStaleIdentity
            | CompletionDecision::RejectChangedPreparedState => {
                CompletionAttempt::NothingToComplete
            }
            CompletionDecision::Proceed => {
                let mut completed = entry.clone();
                let identity = completion_producer_identity(&completed);
                apply_completion(&mut completed, complete, identity, now_millis());
                match self.put_under_state_partition_lock(completed, txnv).await {
                    Ok(_) => {
                        info!(
                            tid = transactional_id,
                            state = ?complete,
                            "transaction completion: completed a prepared transaction"
                        );
                        CompletionAttempt::Completed
                    }
                    Err(error) => {
                        warn!(
                            tid = transactional_id,
                            %error,
                            "transaction completion: Complete append failed; will retry"
                        );
                        CompletionAttempt::Retry
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
