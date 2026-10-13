//! KIP-939 idle-transaction reaper: the timeout-driven abort path.
//!
//! The module holds the [`ReaperBackend`] seam, the pure transition and guard
//! helpers that decide each step of an idle abort, the orchestration loop that
//! runs the three phases over that seam, and the live adapter that runs those
//! phases against the in-memory state map, the `__transaction_state` log, the
//! partition leaders, and the producer-id allocator.

use {
    super::{
        TxnCoordinator,
        completion::{apply_completion, completion_decision},
    },
    crate::txn::{
        handlers::end_txn::{completion_producer_identity, prepare_server_abort_identities},
        marker::MarkerType,
        state::{TxnEntry, TxnState},
        two_pc::{NO_TIMEOUT_MS, should_abort_idle_txn},
        version::TxnVersion,
    },
    async_trait::async_trait,
    krabka_log::ProducerId,
    krabka_verified::transaction::TransactionReaperCompletionDecision as CompletionDecision,
    std::sync::Arc,
    tracing::{info, warn},
};

#[cfg(test)]
mod tests;

/// Live-dependency seam for the KIP-939 idle-transaction reaper.
///
/// `sweep_expired` orchestrates a three-phase abort for each tid. That
/// orchestration is pure decision logic around four irreducible side effects:
/// a coordinator-ownership check, two compare-and-swap-style persisted
/// transitions (`Ongoing → PrepareAbort`, then
/// `PrepareAbort → CompleteAbort`), the abort-marker fan-out, and
/// producer-identity allocation. Each one touches a live
/// `__transaction_state` partition, partition leaders, or the producer-id
/// allocator.
///
/// This trait puts those effects behind a seam, so a unit test can drive the
/// orchestration against a [`mockall`] mock. Every method returns
/// already-extracted plain data, that is, snapshots, so a mock can kill the
/// decisions that read them. The live adapter is [`TxnCoordinator`] itself.
///
/// The two `*_transition` methods mutate the entry **atomically under the
/// per-tid lock**, so a concurrent `EndTxn` or `InitProducerId` is not
/// overwritten. They return the resulting persisted snapshot, or `None` when
/// the guard failed because the caller lost a race or the entry is no longer
/// present. The pure helpers [`apply_prepare_abort`], [`apply_complete_abort`],
/// and [`complete_abort_decision`] compute the transitions. The backend owns
/// only the compare-and-swap and the persistence, which is the irreducible
/// part.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(crate) trait ReaperBackend: Send + Sync {
    /// Is this broker the transaction coordinator for `tid` right now?
    async fn is_coordinator_for(&self, tid: &str) -> bool;

    /// Moves `Ongoing → PrepareAbort` under `tid`'s entry lock, atomically.
    ///
    /// If the entry should abort as an idle transaction at `now_ms`, this
    /// method makes the transition, persists it, and returns the persisted
    /// snapshot. It returns `None` when the entry is absent, when the entry
    /// must not be reaped, or when the persistence failed.
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry>;

    /// Fan out abort markers for `entry`, and drop each partition whose
    /// marker is written from it. Returns `false` if any marker could not be
    /// written, leaving the transaction in `PrepareAbort` for retry.
    async fn dispatch_abort_markers(&self, entry: &mut TxnEntry) -> bool;

    /// Moves `PrepareAbort → CompleteAbort` under `tid`'s entry lock,
    /// atomically.
    ///
    /// The method first checks that the current entry still matches the
    /// `prepared` snapshot this reaper wrote: the same identity, and still
    /// `PrepareAbort`. If it matches, the method moves the entry to
    /// `CompleteAbort`, bumps the producer identity at `now_ms` as KIP-890
    /// requires, persists the entry, and returns the persisted snapshot. It
    /// returns `None` when another caller advanced the entry or when the
    /// persistence failed.
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry>;
}

/// Applies the `Ongoing → PrepareAbort` mutation for an idle-reaped entry. It
/// changes the state and stamps `last_update_ms`. The function is pure, so a
/// unit test can kill the transition without any persistence.
fn apply_prepare_abort(entry: &mut TxnEntry, now_ms: i64) {
    entry.state = TxnState::PrepareAbort;
    entry.last_update_ms = now_ms;
}

/// Applies the `PrepareAbort → CompleteAbort` mutation from the newly
/// allocated `(producer_id, producer_epoch)` of the KIP-890 identity bump. It
/// records the prior id as `prev_producer_id` only when a roll happened, that
/// is, when the allocator gave out a fresh pid. The function is pure, so a
/// unit test can kill the transition.
fn apply_complete_abort(entry: &mut TxnEntry, new_pid: ProducerId, new_epoch: i16, now_ms: i64) {
    apply_completion(entry, TxnState::CompleteAbort, (new_pid, new_epoch), now_ms);
}

/// Recheck the complete prepared snapshot after marker dispatch. Comparing
/// every persisted field prevents a concurrent registration, recovery-identity
/// change, timeout change, or generation change from being overwritten.
fn complete_abort_decision(entry: &TxnEntry, prepared: &TxnEntry) -> CompletionDecision {
    completion_decision(
        entry,
        prepared,
        (TxnState::PrepareAbort, TxnState::CompleteAbort),
    )
}

/// A coordinator persist replaces the map value with a fresh `Arc`. A caller
/// that queued on the prior entry lock must not publish from that stale value.
fn handle_is_current(
    coordinator: &TxnCoordinator,
    tid: &str,
    handle: &Arc<tokio::sync::Mutex<TxnEntry>>,
) -> bool {
    coordinator.is_current_entry(tid, handle)
}

/// Runs the reaper orchestration loop.
///
/// The loop is generic over the [`ReaperBackend`] seam, so a unit test can
/// drive it against a mock. For each candidate tid it runs the three-phase
/// abort: ownership check, `prepare_abort` (compare-and-swap), marker fan-out,
/// then `complete_abort` (compare-and-swap). Returns the tids it finalised, in
/// iteration order.
async fn sweep_with_backend<B: ReaperBackend + ?Sized>(
    backend: &B,
    candidates: Vec<String>,
    now_ms: i64,
    txnv: TxnVersion,
) -> Vec<String> {
    let mut aborted = Vec::new();
    for tid in candidates {
        // Only reap transactions this broker currently coordinates: a
        // partition we used to lead may have moved, leaving stale state.
        if !backend.is_coordinator_for(&tid).await {
            continue;
        }

        // Phase 1: decide + Ongoing → PrepareAbort, persisted under the lock.
        let Some(mut prepared) = backend.prepare_abort(&tid, now_ms, txnv).await else {
            continue;
        };

        // Phase 2: fan out abort markers to local partition leaders.
        if !backend.dispatch_abort_markers(&mut prepared).await {
            continue;
        }

        // Phase 3: PrepareAbort → CompleteAbort, re-validating identity + state
        // under the lock so a concurrent EndTxn / InitProducerId is not
        // clobbered.
        if backend
            .complete_abort(&prepared, now_ms, txnv)
            .await
            .is_some()
        {
            info!(tid, "txn reaper: aborted timed-out transaction");
            aborted.push(tid);
        }
    }
    aborted
}

impl TxnCoordinator {
    async fn current_entry_guard(
        &self,
        tid: &str,
    ) -> Option<tokio::sync::OwnedMutexGuard<TxnEntry>> {
        let handle = self.get(tid)?;
        let entry = Arc::clone(&handle).lock_owned().await;
        handle_is_current(self, tid, &handle).then_some(entry)
    }

    /// KIP-939 idle-transaction reaper: aborts every locally-coordinated,
    /// non-2PC, `Ongoing` transaction whose timeout has elapsed at `now_ms`.
    ///
    /// The reaper skips 2PC transactions, where
    /// `txn_timeout_ms == NO_TIMEOUT_MS`. Their external transaction manager
    /// owns the commit or abort decision, and Kafka must never abort a
    /// prepared 2PC transaction on its own. [`should_abort_idle_txn`] makes
    /// the decision; it is the exhaustively model-checked core. See
    /// [`crate::txn::two_pc_model`].
    ///
    /// Each abort runs the same two-step transition + marker fan-out as an
    /// `EndTxn(committed=false)` and bumps the producer epoch, at every
    /// transaction version, so the timed-out producer is fenced. A marker failure leaves the
    /// entry in `PrepareAbort`; the next sweep retries the fan-out. A concurrent
    /// caller that changed the entry out from under us aborts this reap of that
    /// tid (re-validated before the Complete write). Returns the tids it
    /// finalized.
    // cargo-mutants: I/O orchestration over live DashMap/partition state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_sweep_expired",
        level = "debug",
        skip_all,
        fields(now_ms)
    )]
    pub(crate) async fn sweep_expired(&self, now_ms: i64, txnv: TxnVersion) -> Vec<String> {
        // Snapshot the candidate tids first so we don't hold DashMap shard locks
        // across the async abort work; the orchestration then drives the live
        // `ReaperBackend` (this coordinator), which re-acquires each entry's lock
        // per phase.
        let candidates: Vec<String> = self.state.iter().map(|e| e.key().clone()).collect();
        sweep_with_backend(self, candidates, now_ms, txnv).await
    }
}

/// Live adapter that runs the real reaper side effects against the in-memory
/// state map, the `__transaction_state` partition log, partition leaders, and
/// the producer-id allocator. Only the irreducible IO lives here. The pure
/// helpers and the orchestration logic above hold every decision.
#[async_trait]
impl ReaperBackend for TxnCoordinator {
    // cargo-mutants: thin adapter over inherent method / live lock state
    #[cfg_attr(test, mutants::skip)]
    async fn is_coordinator_for(&self, tid: &str) -> bool {
        TxnCoordinator::is_coordinator_for(self, tid).await
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn prepare_abort(&self, tid: &str, now_ms: i64, txnv: TxnVersion) -> Option<TxnEntry> {
        let _partition_lock = self.lock_state_partition_for(tid).await;
        let entry = self.current_entry_guard(tid).await?;
        if entry.state == TxnState::PrepareAbort {
            // A prepared 2PC transaction remains under its external
            // coordinator's control, including after recovery.
            return (entry.txn_timeout_ms != NO_TIMEOUT_MS).then(|| entry.clone());
        }
        if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms) {
            return None;
        }
        // Stage on a clone. Allocation or persistence failure must leave the
        // live entry exactly as it was before this sweep.
        let mut prepared = entry.clone();
        apply_prepare_abort(&mut prepared, now_ms);
        // Kafka `abortTimedOutTransactions`: `prepareFenceProducerEpoch`, then
        // an abort at the cluster's transaction version. It fences the
        // timed-out producer at every level: the completion bump at `TV_2`,
        // the fence's own bump below it.
        if let Err(error) =
            prepare_server_abort_identities(&mut prepared, txnv, &self.producer_ids).await
        {
            warn!(tid, %error, "txn reaper: failed to allocate completion identity");
            return None;
        }
        // `complete_abort` compares the snapshot this returns against the live
        // entry by full equality, so it has to be the entry publication holds.
        match self.put_under_state_partition_lock(prepared, txnv).await {
            Ok(persisted) => Some(persisted),
            Err(e) => {
                warn!(tid, error = %e, "txn reaper: failed to persist PrepareAbort; skipping");
                None
            }
        }
    }

    // cargo-mutants: writes abort markers to live partition logs
    #[cfg_attr(test, mutants::skip)]
    async fn dispatch_abort_markers(&self, entry: &mut TxnEntry) -> bool {
        match self
            .dispatch_transaction_markers(entry, MarkerType::Abort)
            .await
        {
            Ok(()) => true,
            Err(error) => {
                warn!(
                    tid = %entry.transactional_id,
                    %error,
                    "txn reaper: abort marker fan-out failed; queued for completion"
                );
                // The PrepareAbort record is durable. The completion task
                // retries it without waiting for the next sweep.
                self.request_completion(&entry.transactional_id);
                false
            }
        }
    }

    // cargo-mutants: I/O over live entry locks + raft persistence
    #[cfg_attr(test, mutants::skip)]
    async fn complete_abort(
        &self,
        prepared: &TxnEntry,
        now_ms: i64,
        txnv: TxnVersion,
    ) -> Option<TxnEntry> {
        let tid = prepared.transactional_id.as_str();
        let _state_partition_write = self.lock_state_partition_for(tid).await;
        let entry = self.current_entry_guard(tid).await?;
        match complete_abort_decision(&entry, prepared) {
            CompletionDecision::AlreadyComplete => return Some(entry.clone()),
            CompletionDecision::Proceed => {}
            CompletionDecision::RejectMalformed
            | CompletionDecision::RejectStaleIdentity
            | CompletionDecision::RejectChangedPreparedState => return None,
        }
        // Keep the current entry unchanged unless its complete record reaches
        // durable persistence and publication.
        let mut complete = entry.clone();
        let (new_pid, new_epoch) = completion_producer_identity(&complete);
        apply_complete_abort(&mut complete, new_pid, new_epoch, now_ms);
        match self.put_under_state_partition_lock(complete, txnv).await {
            Ok(persisted) => Some(persisted),
            Err(e) => {
                warn!(tid, error = %e, "txn reaper: failed to persist CompleteAbort; skipping");
                None
            }
        }
    }
}
