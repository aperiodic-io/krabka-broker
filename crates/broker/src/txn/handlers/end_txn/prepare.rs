//! Phase 1 of `EndTxn`: the `Ongoing` → `Prepare{Commit,Abort}` transition.
//! It stages the producer identity the client continues with after completion,
//! then persists the Prepare record before any marker leaves this broker.

use {
    super::producer_identity::prepare_completion_identities,
    crate::{
        codes,
        txn::{
            coordinator::TxnCoordinator,
            decision::{apply_end_txn_prepare, decide_phase1_transition},
            marker::MarkerType,
            state::{TxnEntry, TxnState},
            util::now_millis,
            version::TxnVersion,
        },
    },
};

/// `client_version` is the transaction version of the `EndTxn` request, which
/// decides the epoch bump and is the version the Prepare record and its markers
/// carry. `format_version` is the cluster's level, which only picks the
/// `__transaction_state` value format.
pub(super) async fn prepare_transaction(
    coordinator: &TxnCoordinator,
    entry: &std::sync::Arc<tokio::sync::Mutex<TxnEntry>>,
    (committed, no_partition_added): (bool, bool),
    (client_version, format_version): (TxnVersion, TxnVersion),
    transactional_id: &str,
) -> Result<(MarkerType, TxnState, TxnState, TxnEntry), i16> {
    let marker_type = if committed {
        MarkerType::Commit
    } else {
        MarkerType::Abort
    };
    // Lock order: the state-partition write lock, then the entry lock. The
    // reaper and the completion task take them in the same order.
    let _state_partition_write = coordinator.lock_state_partition_for(transactional_id).await;
    let mut state = entry.lock().await;
    if let Some(code) = coordinator.coordinator_error(transactional_id).await {
        return Err(code);
    }
    if !coordinator.is_current_entry(transactional_id, entry) {
        // Another request persisted this transaction after validation read
        // it. Kafka answers a transition in progress the same way, and the
        // client retries.
        return Err(codes::CONCURRENT_TRANSACTIONS);
    }
    // Stage on a clone. Until the Prepare record is durable, every other
    // caller must still see the state before it.
    let mut staged = state.clone();
    // Kafka `prepareAbortOrCommit(..., noPartitionAdded = true)`: transaction
    // version 2 accepts an abort of a transaction that added no partition,
    // from `Empty`, `CompleteCommit` and `CompleteAbort`. It has nothing to
    // mark, and its start time is the update time. Every other transition
    // comes from `Ongoing`.
    let (prepare, complete) = if no_partition_added {
        staged.partitions.clear();
        staged.start_ms = now_millis();
        apply_end_txn_prepare(&mut staged, committed)
    } else {
        decide_phase1_transition(&mut staged, committed)?
    };
    prepare_completion_identities(&mut staged, client_version, &coordinator.producer_ids)
        .await
        .map_err(|error| {
            tracing::error!(
                tid = transactional_id,
                %error,
                "EndTxn: failed to allocate completion producer identity"
            );
            // Kafka's `TransactionCoordinator.endTransaction` answers a failed
            // `generateProducerId` with its error code:
            // `COORDINATOR_LOAD_IN_PROGRESS` while no block is ready.
            codes::from_broker_error(&error)
        })?;
    staged.last_update_ms = now_millis();
    // Kafka `prepareAbortOrCommit` stamps the request's transaction version.
    staged.client_transaction_version = client_version.level();
    if let Err(error) = coordinator
        .put_under_state_partition_lock(staged.clone(), format_version)
        .await
    {
        tracing::error!(
            tid = transactional_id,
            state = ?prepare,
            error = %error,
            "EndTxn: failed to persist PrepareCommit/PrepareAbort"
        );
        return Err(coordinator
            .append_error_code(transactional_id, &error)
            .await);
    }
    // The append published a new handle. A caller that already waits on this
    // one sees the durable Prepare state too.
    *state = staged.clone();
    Ok((marker_type, prepare, complete, staged))
}
