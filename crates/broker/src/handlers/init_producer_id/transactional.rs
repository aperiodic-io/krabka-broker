//! The transactional sub-path of `InitProducerId`: coordinator-side
//! allocation, epoch bump, and KIP-939 recovery of a prepared transaction.
//!
//! Once the request has passed the ACL preamble and this broker is confirmed
//! as the coordinator for the transactional id, everything that follows is one
//! state machine over the persisted `TxnEntry`. A fresh id allocates, a
//! prepared id is recovered, and a reused id aborts any ongoing transaction
//! before it bumps the epoch, so the transitions and the abort-marker fan-out
//! they depend on stay together.

use std::sync::Arc;

use krabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;
use krabka_verified::transaction::InitProducerIdIdentityDecision;

use super::identity::{next_init_producer_identity, stage_recovery_identity};
use crate::{
    codes,
    error::BrokerError,
    txn::{
        coordinator::{TxnCoordinator, completion::completion_for},
        state::{TxnEntry, TxnState},
        util::now_millis,
    },
};

/// Transactional sub-path: allocate or bump-epoch for `tid`.
///
/// `request_identity` is the `(producer_id, producer_epoch)` the caller
/// claims, `(-1, -1)` when it claims none. Kafka's
/// `TransactionCoordinator.prepareInitProducerIdTransit` fences a stale claim
/// with `PRODUCER_FENCED` before it matches on the persisted state, so an
/// ongoing transaction is aborted only for the producer that owns it
/// (KIP-360). It does that inside `txnMetadata.inLock`, together with the
/// transit it prepares, and so does every branch below: the check and the
/// mutation it admits share one guard.
///
/// A transactional id with no entry is not fenced, whatever the caller
/// claims. Kafka's `isValidProducerId` opens with
/// `txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH`, which the
/// metadata it just created always satisfies: a producer recovering from
/// `UNKNOWN_PRODUCER_ID` names its old identity, and the freshly allocated one
/// is what it gets back.
pub(super) async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    tid: &str,
    txnv: crate::txn::version::TxnVersion,
    txn_timeout: i32,
    enable_2pc: bool,
    keep_prepared_txn: bool,
    request_identity: (i64, i16),
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();

    match coord.get(tid) {
        None => {
            // Fresh tid — allocate a new producer id.
            let (pid, epoch) = coord.producer_ids.allocate().await?;
            let entry = TxnEntry::new_empty(tid.to_string(), pid, epoch, txn_timeout, now_ms);
            // Kafka's `putTransactionStateIfNotExists` creates the metadata
            // under the state lock and hands a racing caller the winner's
            // object. That caller runs `prepareInitProducerIdTransit` on it:
            // `CONCURRENT_TRANSACTIONS` while the winner's transit is pending,
            // and an epoch bump once the winner's append has completed.
            // `allocate` yields, so another first `InitProducerId` for this id
            // can create the entry meanwhile; recheck under the partition's
            // write lock, which every append takes, so the two never persist
            // two identities for one id. The winner publishes only after its
            // append, so a loser here would find it completed and Kafka would
            // bump at once. It answers `CONCURRENT_TRANSACTIONS` instead, and
            // the client's retry finds the winner's entry and takes that bump.
            // The id ends at the same identity and timeout either way, one
            // round trip later, so this does not run the reuse path below
            // from here.
            let _state_partition_write = coord.lock_state_partition_for(tid).await;
            if coord.get(tid).is_some() {
                return Ok(concurrent_transactions_response());
            }
            coord.put_under_state_partition_lock(entry, txnv).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                producer_id: pid.get(),
                producer_epoch: epoch,
                ..Default::default()
            })
        }
        Some(existing) => {
            // KIP-360: a caller that names a producer identity must name the
            // live one, or the epoch this entry held before an epoch fence
            // whose abort failed. Kafka runs that check inside
            // `txnMetadata.inLock` together with the transit it prepares, so
            // every branch below re-runs it under the very lock that performs
            // its mutation. Two overlapping v3 calls that name the same live
            // identity therefore serialize on that lock, and the one that
            // loses finds the epoch the winner already advanced. A zombie
            // neither recovers a prepared transaction nor aborts the ongoing
            // transaction of the producer that fenced it.
            if keep_prepared_txn {
                let recovery = {
                    let mut entry = existing.lock().await;
                    if let Some(response) = pending_or_fenced_response(&entry, request_identity) {
                        return Ok(response);
                    }
                    if entry.state == TxnState::Ongoing {
                        let ongoing_pid = entry.producer_id;
                        let ongoing_epoch = entry.producer_epoch;
                        if enable_2pc {
                            entry.txn_timeout_ms = i32::MAX;
                        }
                        let (next_pid, next_epoch) =
                            stage_recovery_identity(&mut entry, &coord.producer_ids).await?;
                        entry.last_update_ms = now_ms;
                        Some((
                            entry.clone(),
                            next_pid,
                            next_epoch,
                            ongoing_pid,
                            ongoing_epoch,
                        ))
                    } else {
                        None
                    }
                };
                if let Some((snapshot, next_pid, next_epoch, ongoing_pid, ongoing_epoch)) = recovery
                {
                    coord.put(snapshot, txnv).await?;
                    return Ok(InitProducerIdResponse {
                        error_code: codes::NONE,
                        producer_id: next_pid.get(),
                        producer_epoch: next_epoch,
                        ongoing_txn_producer_id: ongoing_pid.get(),
                        ongoing_txn_producer_epoch: ongoing_epoch,
                        ..Default::default()
                    });
                }
                let entry = existing.lock().await;
                if let Some(response) = pending_or_fenced_response(&entry, request_identity) {
                    return Ok(response);
                }
            }

            // Reusing tid. Kafka fences the epoch of an ongoing transaction,
            // aborts it, and answers CONCURRENT_TRANSACTIONS. Otherwise it
            // bumps the epoch and answers the new identity.
            {
                // Lock order: the state-partition write lock, then the entry
                // lock, as `EndTxn`, the reaper and the completion task take
                // them. Every append takes the partition lock, so the entry
                // read under it is the published one.
                let state_partition_write = coord.lock_state_partition_for(tid).await;
                let current = coord.get(tid).unwrap_or_else(|| Arc::clone(&existing));
                let mut e = current.lock().await;
                if let Some(response) = pending_or_fenced_response(&e, request_identity) {
                    return Ok(response);
                }
                // A `Retry`-classified identity names the epoch this entry
                // held before its *last* bump, never its live one (`Bump`
                // covers that). If the live producer has since opened a new
                // `Ongoing` transaction at the bumped epoch -- with or
                // without another `InitProducerId` call in between -- this
                // stale retry must answer the identity already on the entry,
                // not fall into the abort below and tear down a transaction
                // it does not own. Kafka's `prepareIncrementProducerEpoch`
                // answers a retry from the identity alone, never from the
                // entry's state.
                if let Some(response) = retried_bump_response(&e, request_identity) {
                    return Ok(response);
                }
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    let (request_pid, fenced_from_epoch) =
                        crate::txn::handlers::end_txn::client_producer_identity(&e);
                    // Stage on a clone: until the PrepareAbort record is
                    // durable, other callers must still see Ongoing.
                    let mut prepared = e.clone();
                    prepared.state = TxnState::PrepareAbort;
                    // Kafka `prepareFenceProducerEpoch` and the abort it runs
                    // at the cluster's transaction version. The epoch of the
                    // ongoing transaction is raised once before the markers, so
                    // every abort marker fences the producer at its partitions,
                    // and that epoch is never answered to the client. Below
                    // `TV_2` the fence raises it and clears the last epoch, a
                    // retry token from an earlier, unrelated bump that a
                    // zombie could otherwise present as a retry of this fence.
                    // At `TV_2` the completion bump is the only bump, and the
                    // last epoch is the one the producer still holds.
                    crate::txn::handlers::end_txn::prepare_server_abort_identities(
                        &mut prepared,
                        txnv,
                        &coord.producer_ids,
                    )
                    .await?;
                    prepared.last_update_ms = now_ms;
                    let persisted = coord
                        .put_under_state_partition_lock(prepared.clone(), txnv)
                        .await?;
                    *e = persisted;
                    drop(e);
                    drop(state_partition_write);
                    // `put` republishes the tid under a fresh handle, so the
                    // one this call started from is no longer the entry a
                    // concurrent `coord.get` finds. Everything below must act
                    // on the published entry.
                    let published = coord.get(tid).unwrap_or(current);
                    if let Err(error) = dispatch_abort_markers(coord, &mut prepared).await {
                        // KIP-360: the epoch fence is persisted but the abort
                        // it was prepared for did not complete. The producer
                        // that owns the transaction still holds
                        // `fenced_from_epoch`, so record it on the published
                        // entry and let only that producer retry its
                        // `InitProducerId`. Kafka keeps `hasFailedEpochFence`
                        // in memory too: a coordinator that loses it fails
                        // closed, and the producer is fenced. At `TV_2` the
                        // fence raised nothing, so a retry raises nothing
                        // twice and Kafka never sets the flag there.
                        let mut fenced = published.lock().await;
                        fenced.last_producer_epoch = fenced_from_epoch;
                        if !txnv.verified() {
                            fenced.has_failed_epoch_fence = true;
                        }
                        drop(fenced);
                        // The PrepareAbort record is durable. Kafka answers
                        // the fence with CONCURRENT_TRANSACTIONS and finishes
                        // the abort in its marker channel; the producer
                        // retries.
                        tracing::warn!(
                            tid,
                            %error,
                            "InitProducerId: abort marker fan-out failed; queued for completion"
                        );
                        coord.request_completion(tid);
                        return Ok(concurrent_transactions_response());
                    }
                    // Re-acquire + transition to CompleteAbort, staged on a
                    // clone so a failed append leaves PrepareAbort for the
                    // completion task.
                    let mut completed = published.lock().await.clone();
                    completed.state = TxnState::CompleteAbort;
                    completed.last_update_ms = now_millis();
                    let (completed_pid, completed_epoch) =
                        crate::txn::handlers::end_txn::completion_producer_identity(&completed);
                    if completed_pid != request_pid {
                        completed.prev_producer_id = request_pid;
                    }
                    completed.producer_id = completed_pid;
                    completed.producer_epoch = completed_epoch;
                    completed.next_producer_id = krabka_log::ProducerId(-1);
                    completed.next_producer_epoch = -1;
                    // Kafka's `prepareComplete`: the abort of a failed epoch
                    // fence has now been written.
                    completed.has_failed_epoch_fence = false;
                    completed.partitions.clear();
                    if let Err(error) = coord.put(completed, txnv).await {
                        tracing::warn!(
                            tid,
                            %error,
                            "InitProducerId: CompleteAbort append failed; queued for completion"
                        );
                        coord.request_completion(tid);
                        return Ok(concurrent_transactions_response());
                    }
                    // Kafka answers the fenced producer
                    // `CONCURRENT_TRANSACTIONS`. The client retries, and that
                    // retry takes the epoch bump below on the completed
                    // transaction.
                    return Ok(concurrent_transactions_response());
                }
            }

            // Bump epoch on the existing entry. Persist a new TxnEntry with
            // new epoch, Empty state, cleared partitions.
            //
            // Lock order: the state-partition write lock, then the entry
            // lock, same as the abort above and every other append to this
            // tid's log. The entry lock stays held across the append itself
            // -- staged on a local clone, published into the entry only once
            // `coord.put_under_state_partition_lock` returns -- so a caller
            // already parked on this exact handle never observes the bumped
            // identity before it is durable (the bug class PR #1046 fixed for
            // `AddOffsetsToTxn`'s `add_offsets_partition`).
            let _state_partition_write = coord.lock_state_partition_for(tid).await;
            let current = coord.get(tid).unwrap_or(existing);
            let mut e3 = current.lock().await;
            // A `Dead` entry is one the KIP-98 expiry sweep marked under this
            // very lock before it appended the tid's tombstone. This call was
            // parked on the lock while that happened, so its handle is no
            // longer the coordinator's: reviving from it would persist a
            // producer identity for a transactional id whose tombstone is
            // already in the log, and race a second `InitProducerId` that
            // found no entry and took the fresh-id path above. Kafka answers
            // a metadata object mid-transition with `CONCURRENT_TRANSACTIONS`,
            // which the client retries; the retry finds no entry and
            // allocates cleanly.
            // The check comes before the identity one below because a `Dead`
            // entry holds no live mapping to be fenced against, exactly like
            // the tid that was never there: Kafka's `isValidProducerId` opens
            // by admitting freshly created metadata, and the retry this answer
            // asks for finds no entry and allocates.
            if e3.state == TxnState::Dead {
                return Ok(concurrent_transactions_response());
            }
            if let Some(response) = pending_completion_response(&e3, request_identity) {
                return Ok(response);
            }
            if let Some(response) = retried_bump_response(&e3, request_identity) {
                return Ok(response);
            }
            if is_fenced(&e3, request_identity) {
                return Ok(fenced_response());
            }
            let (previous_pid, previous_epoch) =
                crate::txn::handlers::end_txn::client_producer_identity(&e3);
            let (new_pid, new_epoch) =
                next_init_producer_identity(&e3, &coord.producer_ids).await?;
            let mut staged =
                TxnEntry::new_empty(tid.to_string(), new_pid, new_epoch, txn_timeout, now_ms);
            // The transition keeps the transaction version of the record before
            // it (Kafka's `TransitionData` defaults to it).
            staged.client_transaction_version = e3.client_transaction_version;
            // Kafka's `prepareIncrementProducerEpoch` and
            // `prepareProducerIdRotation` record the epoch the entry held, so
            // a retry of this call is recognised. A caller that named no
            // identity records no last epoch.
            if request_identity.0 >= 0 {
                staged.last_producer_epoch = previous_epoch;
            }
            if new_pid != previous_pid {
                staged.prev_producer_id = previous_pid;
            }
            // Stage on a clone: until the append is durable, a caller already
            // parked on this entry's lock must still see the identity it held
            // before this bump.
            let persisted = coord
                .put_under_state_partition_lock(staged.clone(), txnv)
                .await?;
            *e3 = persisted;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the entry's `ProducerId` into the raw-`i64` wire field.
                producer_id: e3.producer_id.get(),
                producer_epoch: e3.producer_epoch,
                ..Default::default()
            })
        }
    }
}

/// What `request_identity` may do to `entry` (KIP-360).
///
/// The caller holds `entry`'s lock, and the mutation the verdict admits runs
/// under that same guard.
fn identity_decision(
    entry: &TxnEntry,
    request_identity: (i64, i16),
) -> InitProducerIdIdentityDecision {
    let (entry_pid, entry_epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    krabka_verified::transaction::init_producer_id_identity_decision(
        entry_pid.get(),
        entry_epoch,
        entry.last_producer_epoch,
        entry.prev_producer_id.get(),
        request_identity.0,
        request_identity.1,
    )
}

/// Whether `request_identity` is fenced against `entry`'s live identity.
fn is_fenced(entry: &TxnEntry, request_identity: (i64, i16)) -> bool {
    identity_decision(entry, request_identity) == InitProducerIdIdentityDecision::Fenced
}

/// Reject a pending transition before checking the live producer identity.
/// Callers hold the same entry lock while checking and staging their mutation.
fn pending_or_fenced_response(
    entry: &TxnEntry,
    request_identity: (i64, i16),
) -> Option<InitProducerIdResponse> {
    pending_completion_response(entry, request_identity)
        .or_else(|| is_fenced(entry, request_identity).then(fenced_response))
}

/// Kafka `prepareIncrementProducerEpoch`: a retry of a bump that already
/// happened answers the entry's identity and writes nothing.
fn retried_bump_response(
    entry: &TxnEntry,
    request_identity: (i64, i16),
) -> Option<InitProducerIdResponse> {
    if identity_decision(entry, request_identity) != InitProducerIdIdentityDecision::Retry {
        return None;
    }
    let (pid, epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    Some(InitProducerIdResponse {
        error_code: codes::NONE,
        producer_id: pid.get(),
        producer_epoch: epoch,
        ..Default::default()
    })
}

/// Kafka `prepareInitProducerIdTransit` for an entry whose `Prepare*` record
/// is durable and not yet complete.
///
/// Kafka checks only the producer ID here, not the epoch
/// (`isValidProducerId`): a caller that names no producer ID, the entry's
/// producer ID, or the prior producer ID at an exhausted epoch gets
/// `CONCURRENT_TRANSACTIONS` and retries after the completion. Any other
/// producer ID is `PRODUCER_FENCED`. Returns `None` for any other state.
///
/// Before this check, `keepPreparedTxn=false` overwrote a `PrepareCommit` with
/// a new empty entry, which erased a commit decision whose markers some
/// partitions may already hold.
fn pending_completion_response(
    entry: &TxnEntry,
    (request_pid, request_epoch): (i64, i16),
) -> Option<InitProducerIdResponse> {
    completion_for(entry.state)?;
    let names_this_transaction = request_pid < 0
        || request_pid == entry.producer_id.get()
        || (entry.has_staged_producer_identity() && request_pid == entry.next_producer_id.get())
        || (request_pid == entry.prev_producer_id.get() && request_epoch >= i16::MAX - 1);
    Some(if names_this_transaction {
        concurrent_transactions_response()
    } else {
        fenced_response()
    })
}

/// Kafka's `initTransactionError(Errors.CONCURRENT_TRANSACTIONS)`.
fn concurrent_transactions_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::CONCURRENT_TRANSACTIONS,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
}

/// Kafka's `initTransactionError(Errors.PRODUCER_FENCED)`.
fn fenced_response() -> InitProducerIdResponse {
    InitProducerIdResponse {
        error_code: codes::PRODUCER_FENCED,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    }
}

async fn dispatch_abort_markers(
    coord: &TxnCoordinator,
    entry: &mut TxnEntry,
) -> Result<(), BrokerError> {
    coord
        .dispatch_transaction_markers(entry, crate::txn::marker::MarkerType::Abort)
        .await
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, ProducerId};
    use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};

    use super::*;
    use crate::{
        partition::Partition,
        partition_registry::PartitionRegistry,
        txn::{bootstrap, state::TopicPartition, version::TxnVersion},
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum LogMutation {
        Appended,
        Unchanged,
    }

    #[derive(Clone, Copy)]
    struct ProducerEpoch(i16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct InitIdentitySetup {
        #[default(ProducerId(1000))]
        producer_id: ProducerId,
        #[default(ProducerEpoch(3))]
        producer_epoch: ProducerEpoch,
    }

    #[derive(Clone, Copy)]
    enum InitIdentity {
        Anonymous,
        Existing(InitIdentitySetup),
    }

    impl Default for InitIdentity {
        fn default() -> Self {
            Self::Existing(InitIdentitySetup::default())
        }
    }

    /// `dispatch_abort_markers` appends an abort control-marker batch to each
    /// locally-led partition in the entry's partition set. Each append advances
    /// that partition's LEO by one. A whole-function `Ok(())` replacement would
    /// skip the dispatch entirely and leave the LEO at 0.
    async fn init_verified(
        coordinator: &Arc<TxnCoordinator>,
        tid: &str,
        identity: InitIdentity,
    ) -> Result<InitProducerIdResponse, BrokerError> {
        let identity = match identity {
            InitIdentity::Anonymous => (-1, -1),
            InitIdentity::Existing(setup) => (setup.producer_id.0, setup.producer_epoch.0),
        };
        handle_transactional(
            coordinator,
            tid,
            TxnVersion::Verified,
            60_000,
            false,
            false,
            identity,
        )
        .await
    }

    async fn init_near_exhaustion(
        coordinator: &Arc<TxnCoordinator>,
        tid: &str,
    ) -> InitProducerIdResponse {
        init_verified(
            coordinator,
            tid,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(i16::MAX - 1),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds")
    }

    #[tokio::test]
    async fn dispatch_abort_markers_appends_marker_to_local_partition() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );

        // Materialize a local partition for `__transaction_state`-style data.
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        let part = crate::test_support::spawn_standalone_partition(
            dir.path(),
            log,
            crate::test_support::StandalonePartitionSetup::default(),
        );
        assert!(part.log_end_offset() == 0);
        // The metadata reconcile installs this broker, node 1, as the leader.
        part.install_leader_change(1, 0).await;
        partitions.insert("orders".into(), PartitionIndex(0), Arc::clone(&part));

        // Build a txn entry that names this partition.
        let mut entry = TxnEntry::new_empty("tx-1".to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "orders".to_string(),
            partition: PartitionIndex(0),
        });

        dispatch_abort_markers(&coord, &mut entry)
            .await
            .expect("dispatch markers");

        // The abort marker is a single control record → LEO advances to 1.
        assert!(
            part.log_end_offset() == 1,
            "abort marker must be appended (LEO 1), got {:?}",
            part.log_end_offset()
        );
    }

    /// Without remote transport, a partition that is not hosted locally must
    /// fail the abort. Advancing the transaction without its marker would leave
    /// an open transaction in the data partition.
    #[tokio::test]
    async fn dispatch_abort_markers_rejects_missing_remote_transport() {
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            krabka_audit::NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            krabka_units::mebibytes(1),
        );
        let mut entry = TxnEntry::new_empty("tx-2".to_string(), ProducerId(2000), 0, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "ghost".to_string(),
            partition: PartitionIndex(0),
        });
        assert!(dispatch_abort_markers(&coord, &mut entry).await.is_err());
    }

    #[derive(Clone, Copy, Default)]
    enum InitScheduling {
        #[default]
        YieldAfterEach,
        SpawnTogether,
    }

    #[derive(Clone, Copy)]
    struct InitCallCount(usize);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ConcurrentInitSetup {
        #[default("tid")]
        tid: &'static str,
        identity: InitIdentity,
        scheduling: InitScheduling,
        #[default(InitCallCount(2))]
        count: InitCallCount,
    }

    async fn spawn_inits(
        coordinator: &Arc<TxnCoordinator>,
        setup: ConcurrentInitSetup,
    ) -> Vec<tokio::task::JoinHandle<Result<InitProducerIdResponse, BrokerError>>> {
        let mut calls = Vec::new();
        for _ in 0..setup.count.0 {
            let coordinator = Arc::clone(coordinator);
            calls.push(tokio::spawn(async move {
                init_verified(&coordinator, setup.tid, setup.identity).await
            }));
            if matches!(setup.scheduling, InitScheduling::YieldAfterEach) {
                tokio::task::yield_now().await;
            }
        }
        calls
    }

    /// Kafka's `transactional.id.expiration.ms` default.
    const EXPIRY_MS: i64 = 604_800_000;

    /// A coordinator that leads the single `__transaction_state` partition,
    /// with one committed transactional id already persisted into it.
    async fn coordinator_with_completed_transaction(
        dir: &std::path::Path,
        tid: &str,
    ) -> (Arc<TxnCoordinator>, Arc<Partition>) {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: bootstrap::TOPIC.to_string(),
            topic_id: uuid::Uuid::from_u128(1),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: bootstrap::TOPIC.to_string(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            ..Default::default()
        }));

        let partitions = Arc::new(PartitionRegistry::new());
        let part = crate::test_support::open_partition(
            dir,
            crate::test_support::StandalonePartitionSetup {
                topic: bootstrap::TOPIC,
                ..Default::default()
            },
        );
        partitions.insert(
            bootstrap::TOPIC.into(),
            PartitionIndex(0),
            Arc::clone(&part),
        );
        let coordinator = Arc::new(TxnCoordinator::new(
            NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            1,
            krabka_units::mebibytes(1),
        ));
        coordinator
            .refresh_leader_partitions(&image)
            .await
            .finished()
            .await;

        let mut entry = TxnEntry::new_empty(tid.to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.state = TxnState::CompleteCommit;
        entry.last_update_ms = 0;
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
        (coordinator, part)
    }

    /// Seeds one `TxnEntry` into a coordinator that already leads its
    /// `__transaction_state` partition.
    async fn seed(coordinator: &TxnCoordinator, entry: TxnEntry) {
        coordinator
            .put(entry, TxnVersion::Verified)
            .await
            .expect("seed __transaction_state");
    }

    /// A transactional id the coordinator has no entry for is answered with a
    /// freshly allocated identity even when the caller names one of its own.
    ///
    /// Kafka's `isValidProducerId` opens with `txnMetadata.producerEpoch ==
    /// RecordBatch.NO_PRODUCER_EPOCH`, and the metadata
    /// `handleInitProducerId` creates for an unknown id always satisfies it:
    /// the case the clause exists for is a producer recovering from
    /// `UNKNOWN_PRODUCER_ID`, which names the identity it last held and must
    /// get the new one back rather than `PRODUCER_FENCED`.
    #[tokio::test]
    async fn an_unknown_transactional_id_is_not_fenced_by_a_supplied_identity() {
        const SEEDED: &str = "tid-seeded";
        const UNKNOWN: &str = "tid-unknown";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), SEEDED).await;
        check!(coordinator.get(UNKNOWN).is_none());

        let response = init_verified(
            &coordinator,
            UNKNOWN,
            InitIdentity::Existing(InitIdentitySetup {
                producer_id: ProducerId(4242),
                producer_epoch: ProducerEpoch(7),
            }),
        )
        .await
        .expect("init responds");

        check!(response.error_code == codes::NONE);
        check!(response.producer_id != 4242);
        check!(response.producer_epoch == 0);
    }

    /// A coordinator that leads `__transaction_state-0`, with one ongoing
    /// transaction at `(1000, 3)` over a local `orders-0` data partition. The
    /// data partition comes back so a test can read the abort marker.
    async fn coordinator_with_ongoing_transaction(
        dir: &std::path::Path,
        tid: &str,
    ) -> (Arc<TxnCoordinator>, Arc<Partition>) {
        let (coordinator, _state) = coordinator_with_completed_transaction(dir, tid).await;
        let data_dir = crate::log_dir::partition_dir(dir, "orders", 0);
        std::fs::create_dir_all(&data_dir).expect("create the data partition directory");
        let data = crate::test_support::spawn_standalone_partition(
            dir,
            Log::open(&data_dir, LogConfig::default()).expect("open the data log"),
            crate::test_support::StandalonePartitionSetup::default(),
        );
        // The metadata reconcile installs this broker, node 1, as the leader.
        data.install_leader_change(1, 0).await;
        coordinator
            .partitions
            .insert("orders".into(), PartitionIndex(0), Arc::clone(&data));
        let handle = coordinator.get(tid).expect("the seeded entry");
        let ongoing = {
            let mut entry = handle.lock().await;
            entry.state = TxnState::Ongoing;
            entry.partitions.insert(TopicPartition {
                topic: "orders".to_string(),
                partition: PartitionIndex(0),
            });
            entry.clone()
        };
        coordinator
            .put(ongoing, TxnVersion::Verified)
            .await
            .expect("seed the ongoing transaction");
        (coordinator, data)
    }

    /// The producer epoch of the one control marker in `partition`.
    fn marker_producer_epoch(partition: &Partition) -> Option<i16> {
        let read = partition
            .read_log(krabka_log::Offset(0), krabka_units::mebibytes(1))
            .expect("read the data partition");
        read.batches.first().map(|batch| batch.producer_epoch)
    }

    /// Kafka `prepareInitProducerIdTransit` on an `ONGOING` transaction:
    /// `prepareFenceProducerEpoch` names the epoch above the live one, the
    /// coordinator aborts the transaction at that epoch, and the client gets
    /// `CONCURRENT_TRANSACTIONS`. The retry then bumps the epoch a second
    /// time.
    ///
    /// Below transaction version 2 the fence raises the epoch and keeps no last
    /// epoch, so the epoch the producer held is fenced. At version 2 the fence
    /// raises nothing and `prepareAbortOrCommit` bumps once, keeping the held
    /// epoch as the last epoch, so a retry that names it is answered the epoch
    /// the abort landed on.
    #[tokio::test]
    async fn an_ongoing_transaction_is_fenced_aborted_and_answered_concurrent() {
        struct Case {
            name: &'static str,
            txnv: TxnVersion,
            /// The epoch the abort completes at, which is also the epoch its
            /// markers carry.
            aborted_epoch: i16,
            /// The last epoch the completed abort records, `-1` for none.
            last_epoch: i16,
            /// What the producer that held the fenced epoch is answered when it
            /// names it again: `(error code, producer id, producer epoch)`.
            stale: (i16, i64, i16),
            /// The epoch the retry of a producer that names no identity
            /// hands to the client.
            retried_epoch: i16,
        }
        let cases = [
            Case {
                name: "transaction version 1",
                txnv: TxnVersion::Flexible,
                aborted_epoch: 4,
                last_epoch: -1,
                stale: (codes::PRODUCER_FENCED, -1, -1),
                retried_epoch: 5,
            },
            Case {
                name: "transaction version 2",
                txnv: TxnVersion::Verified,
                aborted_epoch: 4,
                last_epoch: 3,
                stale: (codes::NONE, 1000, 4),
                retried_epoch: 5,
            },
        ];
        for case in cases {
            let dir = tempfile::tempdir().expect("tempdir");
            let (coordinator, data) =
                coordinator_with_ongoing_transaction(dir.path(), "tid-fenced").await;

            let fenced = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (1000, 3),
            )
            .await
            .expect("init responds");
            check!(
                fenced
                    == InitProducerIdResponse {
                        error_code: codes::CONCURRENT_TRANSACTIONS,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    },
                "{}",
                case.name
            );
            let entry = coordinator
                .get("tid-fenced")
                .expect("entry")
                .lock()
                .await
                .clone();
            check!(
                (
                    entry.state,
                    entry.producer_id,
                    entry.producer_epoch,
                    entry.last_producer_epoch
                ) == (
                    TxnState::CompleteAbort,
                    ProducerId(1000),
                    case.aborted_epoch,
                    case.last_epoch
                ),
                "{}",
                case.name
            );
            // The abort marker fenced the producer at its data partition: the
            // marker carries an epoch above the one the client still holds.
            check!(
                marker_producer_epoch(&data) == Some(case.aborted_epoch),
                "{}: marker epoch",
                case.name
            );

            // Kafka's `prepareIncrementProducerEpoch` matches an expected
            // epoch against the current and the last epoch only. The
            // producer that held the fenced epoch is fenced below
            // transaction version 2, where the fence keeps no last epoch, and
            // it is answered the epoch the abort landed on at version 2, where
            // that epoch is the last one.
            let stale = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (1000, 3),
            )
            .await
            .expect("init responds");
            check!(
                stale
                    == InitProducerIdResponse {
                        error_code: case.stale.0,
                        producer_id: case.stale.1,
                        producer_epoch: case.stale.2,
                        ..Default::default()
                    },
                "{}: the stale identity",
                case.name
            );

            // `initTransactions()` names no identity, so the retry bumps the
            // epoch a second time, as the comment in
            // `prepareInitProducerIdTransit` says.
            let retried = handle_transactional(
                &coordinator,
                "tid-fenced",
                case.txnv,
                60_000,
                false,
                false,
                (-1, -1),
            )
            .await
            .expect("init responds");
            check!(
                retried
                    == InitProducerIdResponse {
                        producer_id: 1000,
                        producer_epoch: case.retried_epoch,
                        ..Default::default()
                    },
                "{}: the retry",
                case.name
            );
        }
    }

    /// KIP-360: a retry of an epoch bump whose response was lost gets the
    /// epoch the coordinator already wrote, and bumps nothing. Kafka's
    /// `prepareIncrementProducerEpoch` recognises it by the last epoch, which
    /// every bump records.
    #[tokio::test]
    async fn a_retried_epoch_bump_answers_the_epoch_it_already_wrote() {
        const TID: &str = "tid-retried-bump";

        struct Step {
            name: &'static str,
            request: (i64, i16),
            answer: (i16, i64, i16),
            /// Whether the call appended a `__transaction_state` record.
            log_mutation: LogMutation,
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        let steps = [
            Step {
                name: "the live identity bumps the epoch",
                request: (1000, 3),
                answer: (codes::NONE, 1000, 4),
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "the retry answers the bumped epoch and writes nothing",
                request: (1000, 3),
                answer: (codes::NONE, 1000, 4),
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "the bumped epoch bumps again",
                request: (1000, 4),
                answer: (codes::NONE, 1000, 5),
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "an older epoch is fenced",
                request: (1000, 3),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "another producer id is fenced",
                request: (1001, 5),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                log_mutation: LogMutation::Unchanged,
            },
            Step {
                name: "a caller that names no identity bumps",
                request: (-1, -1),
                answer: (codes::NONE, 1000, 6),
                log_mutation: LogMutation::Appended,
            },
            Step {
                name: "and records no last epoch, so the old epoch is fenced",
                request: (1000, 5),
                answer: (codes::PRODUCER_FENCED, -1, -1),
                log_mutation: LogMutation::Unchanged,
            },
        ];
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for step in steps {
            let before = part.log_end_offset().0;
            let response = handle_transactional(
                &coordinator,
                TID,
                TxnVersion::Verified,
                60_000,
                false,
                false,
                step.request,
            )
            .await
            .expect("init responds");
            expected.push((step.name, step.answer, step.log_mutation));
            actual.push((
                step.name,
                (
                    response.error_code,
                    response.producer_id,
                    response.producer_epoch,
                ),
                if part.log_end_offset().0 > before {
                    LogMutation::Appended
                } else {
                    LogMutation::Unchanged
                },
            ));
        }
        assert!(actual == expected);
    }

    /// Kafka rotates the producer id once the epoch is exhausted, records the
    /// rotated id as the previous one, and admits the old id at that exhausted
    /// epoch as a retry (`isValidProducerId`).
    #[tokio::test]
    async fn a_rotation_at_the_exhausted_epoch_admits_the_old_identity_as_a_retry() {
        const TID: &str = "tid-rotated";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        {
            let handle = coordinator.get(TID).expect("the seeded entry");
            let mut entry = handle.lock().await;
            entry.producer_epoch = i16::MAX - 1;
        }

        let rotated = init_near_exhaustion(&coordinator, TID).await;
        check!(rotated.error_code == codes::NONE);
        check!(rotated.producer_id != 1000);
        check!(rotated.producer_epoch == 0);
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.prev_producer_id == ProducerId(1000));
        check!(entry.last_producer_epoch == i16::MAX - 1);

        let retried = init_near_exhaustion(&coordinator, TID).await;
        check!(
            (
                retried.error_code,
                retried.producer_id,
                retried.producer_epoch
            ) == (codes::NONE, rotated.producer_id, 0)
        );
    }

    /// Two `InitProducerId` v3 calls that name the same live identity race for
    /// one transactional id. Exactly one bump lands: the identity check and
    /// the epoch bump it admits run under one lock, so the loser wakes to the
    /// epoch the winner already wrote.
    ///
    /// Kafka cannot tell that second call from the retry of a lost response,
    /// so `prepareIncrementProducerEpoch` answers both with the same identity
    /// and writes once (`expectedProducerEpoch == lastProducerEpoch`).
    ///
    /// With the check hoisted above the mutation, both calls read epoch 3,
    /// both bump, and the first caller's response is already stale when it
    /// reaches the client -- two live producers for one transactional id.
    ///
    /// The runtime is single-threaded and both tasks are stepped to their park
    /// on the entry lock with an explicit yield, so the interleaving is the
    /// same every run.
    #[tokio::test]
    async fn overlapping_inits_for_one_identity_answer_one_bumped_identity() {
        const TID: &str = "tid-overlapping-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Both calls park on the entry lock while the test holds it.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let calls = spawn_inits(
            &coordinator,
            ConcurrentInitSetup {
                tid: TID,
                ..Default::default()
            },
        )
        .await;
        drop(guard);

        let mut answered = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answered.push((
                response.error_code,
                response.producer_id,
                response.producer_epoch,
            ));
        }
        check!(answered == vec![(codes::NONE, 1000, 4), (codes::NONE, 1000, 4)]);

        // One bump landed, not two.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!((entry.producer_id, entry.producer_epoch) == (ProducerId(1000), 4));
    }

    /// Two first `InitProducerId` calls race for one unknown transactional id.
    /// Kafka's `putTransactionStateIfNotExists` gives the loser the winner's
    /// metadata, which it finds mid-transition and answers
    /// `CONCURRENT_TRANSACTIONS`, so one identity exists. Both calls allocate a
    /// producer id, which yields; the test holds the partition's write lock so
    /// both are parked behind it when the winner creates the entry.
    #[tokio::test]
    async fn two_first_inits_for_one_unknown_id_create_one_identity() {
        const TID: &str = "tid-fresh-race";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) =
            coordinator_with_completed_transaction(dir.path(), "tid-seeded").await;
        check!(coordinator.get(TID).is_none());

        let write_lock = coordinator.lock_state_partition_for(TID).await;
        let calls = spawn_inits(
            &coordinator,
            ConcurrentInitSetup {
                tid: TID,
                identity: InitIdentity::Anonymous,
                scheduling: InitScheduling::SpawnTogether,
                ..Default::default()
            },
        )
        .await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        drop(write_lock);

        let mut answers = Vec::new();
        for call in calls {
            let response = call.await.expect("init task").expect("init responds");
            answers.push((
                response.error_code,
                response.producer_id,
                response.producer_epoch,
            ));
        }
        // NONE sorts before CONCURRENT_TRANSACTIONS.
        answers.sort_unstable();
        let entry = coordinator.get(TID).expect("the winner's entry");
        let entry = entry.lock().await.clone();
        check!(
            answers
                == vec![
                    (codes::NONE, entry.producer_id.get(), 0),
                    (codes::CONCURRENT_TRANSACTIONS, -1, -1),
                ]
        );
        check!(entry.producer_epoch == 0);
    }

    /// A failed abort fan-out records the epoch fence on the entry the
    /// coordinator publishes, not on the handle this call started from: `put`
    /// republishes the tid under a fresh `Arc`, so a write to the superseded
    /// handle is invisible to the retry that comes to read it.
    ///
    /// The `PrepareAbort` record is durable, so the call answers
    /// `CONCURRENT_TRANSACTIONS` and queues the abort for completion, as
    /// Kafka's fence abort does. While the abort is pending, every caller
    /// that names the transaction's producer ID gets the same answer. Once
    /// the abort completes, KIP-360 lets exactly the producer that still holds
    /// the pre-fence epoch retry, and a zombie that names any other epoch is
    /// fenced.
    ///
    /// Only below transaction version 2 does the entry carry the failed fence
    /// flag: at version 2 the fence raises no epoch, so Kafka never sets
    /// `hasFailedEpochFence` there, and the `PrepareAbort` record already keeps
    /// the pre-fence epoch as its last epoch.
    #[tokio::test]
    async fn a_failed_abort_fan_out_records_the_fence_and_completes_before_a_retry() {
        const TID: &str = "tid-failed-fence";

        // (transaction version, whether the fence is recorded as failed)
        for (txnv, failed_fence) in [(TxnVersion::Flexible, true), (TxnVersion::Verified, false)] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (coordinator, _part) =
                coordinator_with_completed_transaction(dir.path(), TID).await;

            // An ongoing transaction over a partition this broker does not
            // host yet: the abort marker cannot be delivered, so the fan-out
            // fails after the epoch fence is already persisted.
            let mut ongoing = TxnEntry::new_empty(TID.to_string(), ProducerId(1000), 3, 60_000, 0);
            ongoing.state = TxnState::Ongoing;
            ongoing.partitions.insert(TopicPartition {
                topic: "ghost".to_string(),
                partition: PartitionIndex(0),
            });
            seed(&coordinator, ongoing).await;

            let init = |identity| {
                let coordinator = Arc::clone(&coordinator);
                async move {
                    handle_transactional(&coordinator, TID, txnv, 60_000, false, false, identity)
                        .await
                        .expect("InitProducerId responds")
                }
            };
            let concurrent = InitProducerIdResponse {
                error_code: codes::CONCURRENT_TRANSACTIONS,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            };

            check!(init((1000, 3)).await == concurrent, "{txnv:?}");
            let published = coordinator.get(TID).expect("entry").lock().await.clone();
            check!(published.has_failed_epoch_fence == failed_fence, "{txnv:?}");
            check!(published.last_producer_epoch == 3, "{txnv:?}");
            check!(published.state == TxnState::PrepareAbort, "{txnv:?}");

            // While the abort is pending, the producer ID decides, not the
            // epoch.
            for identity in [(1000, 2), (1000, 3), (-1, -1)] {
                check!(init(identity).await == concurrent, "{txnv:?} {identity:?}");
            }
            check!(init((2000, 0)).await == fenced_response(), "{txnv:?}");

            // The partition appears, and the completion task finishes the
            // abort.
            let ghost_dir = crate::log_dir::partition_dir(dir.path(), "ghost", 0);
            std::fs::create_dir_all(&ghost_dir).expect("create ghost partition dir");
            let ghost = crate::test_support::spawn_standalone_partition(
                dir.path(),
                Log::open(&ghost_dir, LogConfig::default()).expect("open ghost log"),
                crate::test_support::StandalonePartitionSetup {
                    topic: "ghost",
                    ..Default::default()
                },
            );
            ghost.install_leader_change(1, 0).await;
            coordinator
                .partitions
                .insert("ghost".into(), PartitionIndex(0), ghost);
            check!(
                coordinator.complete_prepared_transaction(TID, txnv).await
                    == crate::txn::coordinator::completion::CompletionAttempt::Completed,
                "{txnv:?}"
            );

            // Kafka's `prepareComplete` clears the failed fence, so a later
            // fence of this transactional id raises the epoch again.
            let completed = coordinator.get(TID).expect("entry").lock().await.clone();
            check!(!completed.has_failed_epoch_fence, "{txnv:?}");

            check!(
                init((1000, 2)).await.error_code == codes::PRODUCER_FENCED,
                "{txnv:?}"
            );
            check!(init((1000, 3)).await.error_code == codes::NONE, "{txnv:?}");
        }
    }

    /// The KIP-98 expiry sweep and an `InitProducerId` already parked on the
    /// entry's lock must never both persist an identity for one transactional
    /// id.
    ///
    /// The sweep unpublishes the entry from the coordinator's map while this
    /// call holds a clone of its `Arc`. Reviving through that detached handle
    /// would append a producer identity for a tid whose tombstone is already
    /// in the log, and a second `InitProducerId` that found no entry would
    /// allocate a competing one -- two live identities for one id, with
    /// whichever append landed last deciding the coordinator's state. The
    /// sweep marks the entry `Dead` under the same lock, so the parked call
    /// wakes to Kafka's `CONCURRENT_TRANSACTIONS` and retries onto the
    /// fresh-id path.
    ///
    /// The runtime is single-threaded and each task is stepped to its park
    /// with an explicit yield, so the interleaving is the same every run:
    /// the sweep queues on the lock first, the call queues behind it.
    #[tokio::test]
    async fn an_init_parked_on_the_expiry_sweep_does_not_revive_the_tombstoned_id() {
        const TID: &str = "tid-parked-init";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;
        assert!(part.log_end_offset() == 1);

        // The test holds the entry lock, so both tasks below park on it in
        // the order they are stepped.
        let handle = coordinator.get(TID).expect("the seeded entry");
        let guard = handle.lock().await;

        let sweep = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                coordinator
                    .expire_transactional_ids(EXPIRY_MS + 1, EXPIRY_MS)
                    .await
            })
        };
        tokio::task::yield_now().await;

        let init = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(
                async move { init_verified(&coordinator, TID, InitIdentity::Anonymous).await },
            )
        };
        tokio::task::yield_now().await;

        drop(guard);
        let expired = sweep.await.expect("sweep task");
        let response = init.await.expect("init task").expect("init responds");

        check!(expired == vec![TID.to_string()]);
        check!(
            response
                == InitProducerIdResponse {
                    error_code: codes::CONCURRENT_TRANSACTIONS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
        );
        // The id stays expired: nothing was published back into the map, and
        // the log ends at the tombstone the sweep appended.
        check!(coordinator.get(TID).is_none());
        check!(part.log_end_offset() == 2);
    }

    /// KIP-360 + producer id rotation: an `Ongoing` transaction fenced and
    /// aborted right at the epoch-exhaustion boundary rotates to a fresh
    /// producer id during the abort's own completion, exactly as a normal
    /// commit or abort does (`prepare_completion_identities`). The client
    /// that owned the exhausted epoch is not fenced: Kafka records that epoch
    /// as the last epoch of the rotation, and `isValidProducerId` admits the
    /// old id at an exhausted epoch, so its retry is answered the rotated pair.
    /// A caller that names no identity -- Kafka's `initTransactions()` --
    /// bumps the rotated pair.
    #[tokio::test]
    async fn an_ongoing_transaction_fenced_at_the_exhausted_epoch_rotates_the_producer_id() {
        const TID: &str = "tid-abort-rotation";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // An Ongoing transaction at the exhausted epoch boundary, with no
        // partitions, so the abort's marker fan-out succeeds at once and its
        // own `prepare_completion_identities` rotates to a fresh producer id.
        let mut ongoing =
            TxnEntry::new_empty(TID.to_string(), ProducerId(1000), i16::MAX - 1, 60_000, 0);
        ongoing.state = TxnState::Ongoing;
        seed(&coordinator, ongoing).await;

        let fenced = init_near_exhaustion(&coordinator, TID).await;
        check!(
            fenced
                == InitProducerIdResponse {
                    error_code: codes::CONCURRENT_TRANSACTIONS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
        );

        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.state == TxnState::CompleteAbort);
        check!(
            entry.producer_id != ProducerId(1000),
            "the exhausted epoch rotates the producer id"
        );
        // The pre-abort identity is the one recorded, not whatever the
        // abort's own completion step landed on.
        check!(entry.prev_producer_id == ProducerId(1000));

        // The producer that names its old, exhausted identity again is
        // recognised as a retry of the rotation, and answered the rotated pair
        // without a write.
        let stale = init_near_exhaustion(&coordinator, TID).await;
        check!(
            stale
                == InitProducerIdResponse {
                    producer_id: entry.producer_id.get(),
                    producer_epoch: entry.producer_epoch,
                    ..Default::default()
                }
        );

        // `initTransactions()` names no identity, so the retry gets the
        // rotated identity the abort's completion already staged.
        let retried = init_verified(&coordinator, TID, InitIdentity::Anonymous)
            .await
            .expect("init responds");
        check!(retried.error_code == codes::NONE);
        check!(retried.producer_id == entry.producer_id.get());
        check!(retried.producer_epoch == entry.producer_epoch + 1);
    }

    /// A zombie holding the retry token from an earlier, unrelated epoch
    /// bump must not be admitted once that generation has since been fenced
    /// and aborted.
    ///
    /// A legitimate bump from epoch 4 to 5 records 4 as the recognized retry
    /// token. The live producer then opens a transaction at epoch 5 without
    /// another `InitProducerId` call, exactly as `a_stale_retry_answers_...`
    /// sets up. A second `InitProducerId` call finds that transaction
    /// `Ongoing` and fences it: abort, `CompleteAbort` at epoch 6, and the
    /// epoch 5 the fenced producer held becomes the last epoch. The old
    /// token from the 4->5 bump names a generation the fence just ended, so
    /// a zombie that still holds it must be fenced, not answered the live
    /// identity as a recognized retry.
    #[tokio::test]
    async fn a_fence_clears_the_retry_token_from_an_earlier_bump() {
        const TID: &str = "tid-stale-token-across-fence";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, _part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // The seeded entry starts at epoch 3. Bump it once (3 -> 4) so the
        // legitimate 4 -> 5 bump below has a live epoch to name.
        init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("first bump responds");

        // The legitimate 4 -> 5 bump: recorded epoch 4 as the retry token.
        let bumped = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(4),
                ..Default::default()
            }),
        )
        .await
        .expect("bump responds");
        check!(
            (bumped.error_code, bumped.producer_id, bumped.producer_epoch)
                == (codes::NONE, 1000, 5)
        );
        {
            let handle = coordinator.get(TID).expect("bumped entry");
            let mut entry = handle.lock().await;
            check!(entry.last_producer_epoch == 4);
            entry.state = TxnState::Ongoing;
            entry.start_ms = 0;
        }

        // A second InitProducerId call finds the transaction Ongoing and
        // fences it, aborting at epoch 6.
        // (Kafka `prepareAbortOrCommit` at TV2: `epoch + 1`, `lastEpoch = epoch`.)
        let fenced = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(5),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds");
        check!(fenced == concurrent_transactions_response());
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        // TV2 bumps once, and the epoch the fenced producer held is the last
        // epoch.
        check!(
            (entry.state, entry.producer_epoch, entry.last_producer_epoch)
                == (TxnState::CompleteAbort, 6, 5)
        );

        // The zombie holding the pre-bump token (4) belongs to the
        // generation the fence just ended and must be fenced, not admitted
        // as a retry of the entry the fence produced.
        let zombie = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup {
                producer_epoch: ProducerEpoch(4),
                ..Default::default()
            }),
        )
        .await
        .expect("init responds");
        check!(zombie.error_code == codes::PRODUCER_FENCED, "{zombie:?}");
    }

    /// KIP-360: a caller retrying its own lost `InitProducerId` response must
    /// never abort a transaction the live producer has since opened at the
    /// epoch that response bumped to, whether or not another
    /// `InitProducerId` call carried that epoch to the producer.
    ///
    /// Kafka's `prepareIncrementProducerEpoch` decides a retry from the
    /// identity alone, never from the entry's state: the retry answers the
    /// current identity and touches nothing.
    #[tokio::test]
    async fn a_stale_retry_answers_the_bumped_identity_without_aborting_the_live_transaction() {
        const TID: &str = "tid-stale-retry-vs-live-ongoing";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Bumps epoch 3 -> 4 and records 3 as the last epoch, exactly as a
        // caller whose response was lost would leave it.
        let bumped = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("bump responds");
        check!(
            (bumped.error_code, bumped.producer_id, bumped.producer_epoch)
                == (codes::NONE, 1000, 4)
        );
        check!(part.log_end_offset() == 2);

        // The live producer opens a transaction at the bumped epoch without
        // another `InitProducerId` call -- Kafka's implicit-begin path, where
        // `AddPartitionsToTxn` opens a transaction directly once a producer
        // holds a live epoch.
        {
            let handle = coordinator.get(TID).expect("bumped entry");
            let mut entry = handle.lock().await;
            entry.state = TxnState::Ongoing;
            entry.start_ms = 0;
        }

        // The zombie's retransmitted InitProducerId, still naming the
        // pre-bump epoch, arrives after the live transaction is already
        // open.
        let retried = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("retry responds");

        check!(
            (
                retried.error_code,
                retried.producer_id,
                retried.producer_epoch
            ) == (codes::NONE, 1000, 4)
        );
        // Nothing appended: the retry answered from the entry alone.
        check!(part.log_end_offset() == 2);
        // The live transaction is untouched.
        let entry = coordinator.get(TID).expect("entry").lock().await.clone();
        check!(entry.state == TxnState::Ongoing);
    }

    /// PR #1046 fixed this exact bug class for `AddOffsetsToTxn`
    /// (`add_offsets_partition`): the shared entry must publish the bumped
    /// identity only through the append that makes it durable, into the very
    /// same handle a caller from before this call already holds -- never a
    /// detached copy, and never before `coord.put_under_state_partition_lock`
    /// returns `Ok`.
    ///
    /// This locks in the fix's shape (stage on a local clone, append while
    /// still holding the entry lock, publish into `*e3` only on success): a
    /// caller that captured the handle before the bump ran sees the durable
    /// outcome afterward, on the exact same `Arc`, with nothing appended
    /// beyond that one durable record.
    #[tokio::test]
    async fn a_caller_already_holding_the_handle_sees_the_durable_bumped_identity() {
        const TID: &str = "tid-durable-publish";

        let dir = tempfile::tempdir().expect("tempdir");
        let (coordinator, part) = coordinator_with_completed_transaction(dir.path(), TID).await;

        // Captured before the bump call, the way a concurrent caller parked
        // on this same tid's lock would hold it.
        let pre_call_handle = coordinator.get(TID).expect("the seeded entry");

        let response = init_verified(
            &coordinator,
            TID,
            InitIdentity::Existing(InitIdentitySetup::default()),
        )
        .await
        .expect("bump responds");
        check!(
            (
                response.error_code,
                response.producer_id,
                response.producer_epoch
            ) == (codes::NONE, 1000, 4)
        );

        // One append: the seed, then the bump.
        check!(part.log_end_offset() == 2);

        // The pre-existing handle reflects the same durable identity as the
        // response, not a stale snapshot and not a copy that only a fresh
        // `coord.get` would see.
        let via_old_handle = pre_call_handle.lock().await.clone();
        check!(via_old_handle.producer_id == ProducerId(1000));
        check!(via_old_handle.producer_epoch == 4);
        check!(via_old_handle.state == TxnState::Empty);
    }
}
