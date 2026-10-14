//! The writer's non-produce log mutations: replicate, truncate, reset, and
//! trim.
//!
//! Each of these arms runs one blocking log call, acks it, and then repairs
//! whatever derived state the call moved, so they share a module and the
//! blocking-pool wrapper they all call.

use {
    super::{
        WriterPartition,
        append::append_may_block,
        storage::{
            StorageStatus, flag_storage_failure, lock_log, run_log_append, run_log_mutation,
        },
    },
    crate::{
        error::BrokerError, log_dir_status::LogDirRegistry, partition::ReplicatedBatch,
        producer_state::ProducerState, replica_state::ReplicaState, wal::SharedWal,
    },
    arc_swap::ArcSwap,
    krabka_log::{Log, Offset},
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio::sync::Notify,
};

/// The producer tracker and replica watermark a replicated batch updates.
#[derive(Clone, Copy)]
pub(super) struct ReplicationState<'a> {
    pub(super) producer_state: &'a ProducerState,
    pub(super) replica_state: &'a tokio::sync::Mutex<ReplicaState>,
}

pub(super) async fn handle_replicate(
    identity: WriterPartition<'_>,
    log: &Arc<Mutex<Log>>,
    storage_status: StorageStatus<'_>,
    state: ReplicationState<'_>,
    mut batch: krabka_protocol::records::RecordBatch,
    ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    append_notify: &Notify,
) {
    let ReplicationState {
        producer_state,
        replica_state,
    } = state;
    let offset = batch.base_offset;
    // Read before `batch` moves into the closure. A control batch is the only
    // shape `handle_replicate` ever receives that can change a producer's
    // tracked epoch: `replicator/response.rs` decodes every control batch and
    // sends it here, verbatim passthrough is for ordinary data batches only.
    let control_producer = batch
        .attributes
        .is_control_batch()
        .then_some(krabka_log::ProducerId(batch.producer_id))
        .filter(|producer_id| producer_id.get() >= 0);
    // A replicated transaction marker adds a complete transaction that holds
    // the last stable offset until the high watermark passes the marker.
    // Release the ones the high watermark already passed, so the set stays
    // bounded on a follower that no reader fetches from. The high watermark is
    // read here, on this task, through its async mutex, before the blocking
    // closure takes the log lock.
    let high_watermark = match control_producer {
        Some(_) => Some(replica_state.lock().await.hw),
        None => None,
    };
    let log_for_blocking = Arc::clone(log);
    // Read the mirror entry inside this same closure, under the lock the
    // append already takes here through `run_log_mutation`, rather than by a
    // second, separate `lock_log` call afterward on the calling async task. A
    // `std::sync::Mutex` acquired directly on that task blocks its worker
    // thread for as long as whatever else holds the lock -- for example a
    // diskless flush's trim, itself running in `run_log_mutation` on a
    // different thread -- and does not yield the thread back to the runtime
    // the way an uncontended `.await` would. On a freshly promoted leader
    // catching up on replicated markers across many partitions at once, that
    // can starve this broker's own heartbeat-sending task past the
    // controller's liveness timeout.
    let result = run_log_mutation(
        move || {
            let mut guard = lock_log(&log_for_blocking);
            if let Some(high_watermark) = high_watermark {
                guard.release_replicated_transactions(high_watermark);
            }
            guard
                .append_at(&mut batch, Offset(offset))
                .map_err(BrokerError::from)?;
            Ok(control_producer.and_then(|producer_id| guard.producer_state_entry(producer_id)))
        },
        "replicate task panicked",
        storage_status,
    )
    .await;
    match result {
        Ok(entry) => {
            // A follower mirrors a replicated marker's producer-state effect,
            // as a leader does for a marker it appends, so the tracker keeps
            // the epoch fence of a transaction-version-2 marker. This arm
            // does not mirror data batches. A promotion copies all of the
            // log's producer state into the tracker before the new leader
            // takes a produce (`Partition::install_local_leadership`).
            if let Some(entry) = entry {
                producer_state
                    .mirror_log_entries(identity.topic, identity.partition, vec![entry])
                    .await;
            }
            append_notify.notify_waiters();
            let _ = ack.send(Ok(()));
        }
        Err(error) => {
            let _ = ack.send(Err(error));
        }
    }
}

pub(super) async fn handle_replicate_verbatim(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    log_dir_status: &LogDirRegistry,
    batches: Vec<ReplicatedBatch>,
    ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    append_notify: &Notify,
) {
    let storage = StorageStatus {
        log_dir,
        registry: log_dir_status,
    };
    // One batch per lock, not the whole run: a fetch row can carry megabytes,
    // and every reader of this partition -- a fetch, an epoch check, an offset
    // lookup -- waits on the lock while it is held. Each batch also decides
    // for itself whether its append may block.
    let mut result = Ok(());
    for ReplicatedBatch { batch, base_offset } in batches {
        let footprint = (batch.bytes.len(), batch.max_timestamp);
        result = run_log_append(
            log,
            move |log| !append_may_block(log, footprint),
            move |log| {
                log.append_verbatim_at(&batch, base_offset)
                    .map(|_| ())
                    .map_err(BrokerError::from)
            },
            "replicate verbatim task panicked",
            storage,
        )
        .await;
        if result.is_err() {
            break;
        }
    }
    let succeeded = result.is_ok();
    let _ = ack.send(result);
    if succeeded {
        append_notify.notify_waiters();
    }
}

/// The two mutations that reset the writer's replication and WAL frontiers.
pub(super) enum ResetMutation {
    Truncate(Offset),
    Reset(Offset),
}

/// Apply the selected reset and answer its acknowledgement after resetting the frontiers.
pub(super) async fn handle_log_reset(
    log: &Arc<Mutex<Log>>,
    storage_status: StorageStatus<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    wal: Option<&SharedWal>,
    mutation: ResetMutation,
    ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
) -> bool {
    let panic_context = match mutation {
        ResetMutation::Truncate(_) => "truncate task panicked",
        ResetMutation::Reset(_) => "reset_to task panicked",
    };
    let log_for_blocking = Arc::clone(log);
    let result = run_log_mutation(
        move || {
            let mut log = lock_log(&log_for_blocking);
            match mutation {
                ResetMutation::Truncate(offset) => log.truncate_to(offset),
                ResetMutation::Reset(offset) => log.reset_to(offset),
            }
            .map_err(BrokerError::from)
        },
        panic_context,
        storage_status,
    )
    .await;
    finish_log_reset(result, log, replica_state, wal, ack).await
}

pub(super) async fn handle_trim(
    log: &Arc<Mutex<Log>>,
    storage_status: StorageStatus<'_>,
    wal: Option<&SharedWal>,
    new_start: Offset,
    ack: tokio::sync::oneshot::Sender<Result<Offset, BrokerError>>,
) {
    let result = if let Some(wal) = wal {
        // The WAL is the first durable trim step. Include an already-advanced
        // local start so a retry can repair either side without regression.
        let local_start = lock_log(log).log_start_offset();
        let wal_target = new_start.max(local_start);
        match wal.trim_to_offset(wal_target).await {
            Err(error) => {
                flag_storage_failure(&error, storage_status.log_dir, storage_status.registry);
                Err(error)
            }
            Ok(wal_start) => {
                reconcile_trim_frontiers(log, storage_status, new_start, wal_start).await
            }
        }
    } else {
        let log_for_blocking = Arc::clone(log);
        run_log_mutation(
            move || {
                lock_log(&log_for_blocking)
                    .trim_to_offset(new_start)
                    .map_err(BrokerError::from)
            },
            "trim_to_offset task panicked",
            storage_status,
        )
        .await
    };
    let _ = ack.send(result);
}

async fn reconcile_trim_frontiers(
    log: &Arc<Mutex<Log>>,
    storage_status: StorageStatus<'_>,
    requested: Offset,
    wal_start: Offset,
) -> Result<Offset, BrokerError> {
    use krabka_verified::DeleteRecordsTrimApplication::{
        Complete, RejectMalformed, TrimLocal, TrimWal,
    };

    let local_start = lock_log(log).log_start_offset();
    match krabka_verified::delete_records_trim_application(requested.0, wal_start.0, local_start.0)
    {
        Complete { frontier } => Ok(Offset(frontier)),
        TrimLocal { frontier } => {
            let log_for_blocking = Arc::clone(log);
            let local_result = run_log_mutation(
                move || {
                    lock_log(&log_for_blocking)
                        .trim_to_offset(Offset(frontier))
                        .map_err(BrokerError::from)
                },
                "trim reconciliation task panicked",
                storage_status,
            )
            .await?;
            if local_result == Offset(frontier) {
                Ok(local_result)
            } else {
                Err(BrokerError::Replication(format!(
                    "trim frontiers diverged: WAL {frontier}, local {}",
                    local_result.0
                )))
            }
        }
        TrimWal { frontier } => Err(BrokerError::Replication(format!(
            "WAL trim stopped at {} before required frontier {frontier}",
            wal_start.0
        ))),
        RejectMalformed => Err(BrokerError::Replication(format!(
            "invalid trim frontiers: requested {}, WAL {}, local {}",
            requested.0, wal_start.0, local_start.0
        ))),
    }
}

/// Publish a successful reset to the WAL cache and replica watermark, then ack.
async fn finish_log_reset(
    result: Result<(), BrokerError>,
    log: &Mutex<Log>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    wal: Option<&SharedWal>,
    ack: super::storage::MaintenanceAck,
) -> bool {
    let succeeded = result.is_ok();
    if succeeded {
        if let Some(wal) = wal {
            wal.invalidate_hot_tail();
        }
        let new_leo = lock_log(log).log_end_offset();
        replica_state
            .lock()
            .await
            .recompute_hw_for_leader_append(new_leo);
    }
    let _ = ack.send(result);
    succeeded
}
