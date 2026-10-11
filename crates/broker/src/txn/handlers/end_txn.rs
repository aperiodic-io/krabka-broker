//! `EndTxn` (`api_key=26`). Finalises a transaction. The producer calls
//! `commitTransaction()` or `abortTransaction()`, which drives two state
//! transitions with a `WriteTxnMarkers` fan-out in between.
//!
//! ## Flow
//!
//! 1. Verify coordinator-ness, pid, epoch.
//! 2. `Ongoing` → `PrepareCommit` (or `PrepareAbort`); persist.
//! 3. Fan out `WriteTxnMarkers` to every involved partition's leader, and
//!    wait until each marker is committed on its partition:
//!    - **local** leader  → an append as the partition's leader.
//!    - **remote** leader → `WriteTxnMarkersRequest` over the shared
//!      `InterBrokerClient` (runs inter-broker TLS / SASL as the listener demands).
//! 4. `PrepareCommit` → `CompleteCommit` (or `PrepareAbort` → `CompleteAbort`); persist.
//! 5. Return `NONE` to the producer.
//!
//! After step 2 the answer is `NONE`, as in Kafka. When step 3 or step 4
//! fails, the handler hands the transaction to
//! [`crate::txn::completion`], which retries both steps until the `Complete*`
//! record is durable. A broker that stops after step 2 finishes the
//! transaction when recovery loads it.
//!
//! Wire format: v0-2 non-flexible, v3-5 flexible (tagged fields).
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `committed`.
//! Response fields: `throttle_time_ms`, `error_code`.

use krabka_log::ProducerId;
use krabka_protocol::owned::{end_txn_request::EndTxnRequest, end_txn_response::EndTxnResponse};

use crate::{
    api_catalog::UnstableApiVersions,
    broker::Broker,
    error::BrokerError,
    handlers::RequestContext,
    txn::{
        decision::{CompletionDecision, decide_end_txn_completion},
        util::now_millis,
        version::{TxnVersion, resolve_txn_version},
    },
};

mod marker_rpc;
mod markers;
mod prepare;
mod producer_identity;
mod reacquire;
mod response;
mod state_table;
mod validation;

#[cfg(test)]
mod test_support;

// Only the #[cfg(test)] stateright models in txn/ reach these variants.
#[cfg(test)]
pub(crate) use self::producer_identity::{
    prepare_completion_identities_with_fresh, prepare_server_abort_identities_with_fresh,
};
pub(crate) use self::{
    markers::{MarkerDispatchContext, MarkerFanOut, dispatch_markers, write_local_markers},
    producer_identity::{
        client_producer_identity, completion_producer_identity, next_producer_identity,
        next_recovery_producer_identity, prepare_server_abort_identities,
    },
    reacquire::{ReacquireDecision, validate_complete_reacquire},
};
use self::{
    markers::{MarkerFanOutOutcome, dispatch_transaction_markers},
    prepare::prepare_transaction,
    response::{err_response, ok_response},
    validation::{EndTxnValidation, validate_end_txn},
};

pub(crate) async fn handle(
    broker: &Broker,
    req: EndTxnRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<EndTxnResponse, BrokerError> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let authorizer = broker.config.authorizer.as_ref();

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness.
    let image = controller.current_image();
    // Kafka's `handleEndTxnRequest` gives the coordinator the transaction
    // version of the request's API version, `TV_2` from v5 and `TV_0` below.
    // The state table, the epoch bump and the stamps on the record and on the
    // markers follow that client version. The cluster's `transaction.version`
    // only picks the `__transaction_state` value format, so an EndTxn v4 on a
    // `TV_2` cluster keeps its epoch, as a client that cannot read the new
    // epoch out of the response needs.
    let txnv = resolve_txn_version(&image);
    let client_txnv = TxnVersion::for_end_txn(version);
    // Trunk's answer to a commit that races a coordinator-side abort
    // (KAFKA-20785) is not in Kafka 4.3.1.
    let trunk_rules = broker.config.features.unstable_api_versions == UnstableApiVersions::Enabled;
    drop(coord.refresh_leader_partitions(&image).await);

    let tid = req.transactional_id.as_str();
    let (entry_mutex, no_partition_added) = match validate_end_txn(
        &coord,
        authorizer,
        &image,
        ctx,
        &req,
        (client_txnv, trunk_rules),
    )
    .await
    {
        Ok(EndTxnValidation::Proceed {
            entry,
            no_partition_added,
        }) => (entry, no_partition_added),
        Ok(EndTxnValidation::AlreadyComplete(pid, epoch)) => {
            return Ok(ok_response(pid.get(), epoch));
        }
        Err(code) => return Ok(err_response(version, code)),
    };

    // ── Phase 1: Ongoing → Prepare{Commit,Abort} ──────────────────────

    let (marker_type, prepare, complete, mut prepare_snap) = match prepare_transaction(
        &coord,
        &entry_mutex,
        (req.committed, no_partition_added),
        (client_txnv, txnv),
        tid,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(code) => return Ok(err_response(version, code)),
    };

    // The Prepare record is durable. From here Kafka answers NONE with the
    // completion identity whatever happens to the markers: its
    // `TransactionMarkerChannelManager` finishes the work. A failure below
    // hands the transaction to the completion task, which retries it.
    let (prepared_completion_pid, prepared_completion_epoch) =
        completion_producer_identity(&prepare_snap);

    // ── Phase 2: Fan out WriteTxnMarkers ──────────────────────────────

    match dispatch_transaction_markers(broker, &mut prepare_snap, marker_type, tid).await {
        MarkerFanOutOutcome::Complete => {}
        MarkerFanOutOutcome::Retry => {
            coord.request_completion(tid);
            return Ok(ok_response(
                prepared_completion_pid.get(),
                prepared_completion_epoch,
            ));
        }
        // A fenced generation has already superseded this fan-out attempt
        // (#882): retrying cannot succeed, so the transaction is left in
        // Prepare* for whatever superseded it to resolve, rather than queued
        // for a completion retry that would never converge.
        MarkerFanOutOutcome::GivenUp => {
            return Ok(ok_response(
                prepared_completion_pid.get(),
                prepared_completion_epoch,
            ));
        }
    }

    // ── Phase 3: Prepare{Commit,Abort} → Complete{Commit,Abort} ───────
    //
    // The entry lock was *intentionally* dropped before the Phase-2 marker
    // fan-out (network I/O to remote brokers); holding it across the fan-out
    // would serialize/deadlock the coordinator. That window lets a concurrent
    // caller (the completion task, or an `AddPartitionsToTxn`) interleave on
    // this same transactional-id.
    //
    // We must NOT re-lock the original `entry_mutex` captured at the top of the
    // handler: every persist publishes a *fresh* `Arc<Mutex<TxnEntry>>` (see
    // `TxnCoordinator::put`), so a concurrent caller operates on a different
    // Arc than the one we hold. The only authoritative view is the entry
    // currently registered under `tid`.
    //
    // Take the state-partition write lock and then the current entry lock, in
    // the order the reaper and the completion task use, and re-validate that
    // nothing advanced underneath us BEFORE writing Complete. The locks stay
    // held through the append, so no other caller completes the transaction
    // in between.
    let _state_partition_write = coord.lock_state_partition_for(tid).await;
    let Some(current_mutex) = coord.get(tid) else {
        // The entry vanished while markers were in flight: an unload of the
        // coordinator partition answers its retriable coordinator error, and
        // anything else is a producer-mapping loss.
        return Ok(err_response(version, coord.missing_entry_error(tid).await));
    };

    // The completion identity was selected and persisted with the Prepare
    // state. Phase 3 adopts that identity after marker fan-out; it does not
    // allocate or increment it again.
    let entry = current_mutex.lock().await;
    // The Prepare record already contains both identities: the marker uses
    // the incremented epoch of the producer that wrote the transaction, while
    // the staged completion identity is returned to the client. This
    // revalidation prevents Phase 3 from adopting a stale staged identity.
    let (complete_snap, response_pid, response_epoch) = match decide_end_txn_completion(
        &entry,
        prepare_snap.producer_id,
        prepare_snap.producer_epoch,
        prepared_completion_pid,
        prepared_completion_epoch,
        prepare,
        complete,
    ) {
        CompletionDecision::Proceed {
            next_state,
            response_pid: new_pid,
            response_epoch: new_epoch,
        } => {
            // Stage on a clone: until the Complete record is durable, other
            // callers must still see the Prepare state.
            let mut staged = entry.clone();
            if new_pid != ProducerId(req.producer_id) {
                // Epoch rolled over to a new producer_id: record the prior id
                // so the transition is traceable (KIP-890 PreviousProducerId).
                staged.prev_producer_id = ProducerId(req.producer_id);
            }
            staged.state = next_state;
            staged.last_update_ms = now_millis();
            staged.producer_id = new_pid;
            staged.producer_epoch = new_epoch;
            staged.next_producer_id = ProducerId(-1);
            staged.next_producer_epoch = -1;
            // Kafka's `prepareComplete`: the abort of a failed epoch fence has
            // now been written.
            staged.has_failed_epoch_fence = false;
            staged.partitions.clear();
            (staged, new_pid, new_epoch)
        }
        CompletionDecision::AlreadyComplete {
            response_pid: pid,
            response_epoch: epoch,
        } => {
            // Another caller already drove this exact transition to
            // completion (or we are an idempotent EndTxn retry that lost the
            // race). Report success without re-writing, returning the
            // persisted (possibly already-bumped) identity so a KIP-890
            // client that retried picks up the authoritative value.
            return Ok(ok_response(pid.get(), epoch));
        }
        CompletionDecision::Reject(code) => {
            tracing::warn!(
                tid,
                expected_epoch = req.producer_epoch,
                found_epoch = entry.producer_epoch,
                expected_state = ?prepare,
                found_state = ?entry.state,
                error_code = code,
                "EndTxn: entry changed underneath the marker fan-out; \
                 aborting Complete write"
            );
            return Ok(err_response(version, code));
        }
    };

    if let Err(e) = coord
        .put_under_state_partition_lock(complete_snap, txnv)
        .await
    {
        tracing::error!(
            tid,
            state = ?complete,
            error = %e,
            "EndTxn: failed to persist CompleteCommit/CompleteAbort; queued for completion"
        );
        coord.request_completion(tid);
    }
    drop(entry);

    // Unwrap the post-completion `ProducerId` into the raw-`i64` wire response.
    Ok(ok_response(response_pid.get(), response_epoch))
}
