//! The writer's Produce arm: group drain, append, ack, and high-watermark
//! advance.
//!
//! This is the only arm that batches several queued messages into one log
//! mutation and the only one that has to reconcile a diskless offset
//! assignment with the durable watermark, so it gets its own module.

use {
    super::{
        WriterPartition,
        append::{run_produce_append_batch, run_produce_append_batch_at},
        storage::{StorageStatus, flag_storage_failure, storage_failure_error},
    },
    crate::{
        error::BrokerError,
        partition::{ProduceData, ProduceJob, WriterMessage},
        producer_state::ProducerState,
        replica_state::ReplicaState,
        wal::{OffsetSequencer, SharedWal},
    },
    krabka_log::Log,
    krabka_raft::RaftError,
    std::sync::{Arc, Mutex},
    tokio::sync::{Notify, mpsc},
};

/// The first queued produce job and the most jobs one group may drain.
pub(super) struct ProduceGroup {
    pub(super) first: ProduceJob,
    pub(super) max_size: usize,
}

/// The log, its storage health, and the producer tracker a produce group touches.
#[derive(Clone, Copy)]
pub(super) struct ProduceStorage<'a> {
    pub(super) log: &'a Arc<Mutex<Log>>,
    pub(super) status: StorageStatus<'a>,
    pub(super) producer_state: &'a ProducerState,
}

/// The notifications and replica state a produce group signals.
#[derive(Clone, Copy)]
pub(super) struct ProduceSignals<'a> {
    pub(super) append_notify: &'a Arc<Notify>,
    pub(super) replica_state: &'a Arc<tokio::sync::Mutex<ReplicaState>>,
    pub(super) hw_advance_notify: &'a Arc<Notify>,
}

/// The shared WAL and offset sequencer of a diskless partition, if it has them.
#[derive(Clone, Copy)]
pub(super) struct DisklessParts<'a> {
    pub(super) wal: Option<&'a SharedWal>,
    pub(super) sequencer: Option<&'a Arc<dyn OffsetSequencer>>,
}

pub(super) async fn handle_produce(
    identity: WriterPartition<'_>,
    group: ProduceGroup,
    rx: &mut mpsc::Receiver<WriterMessage>,
    pending: &mut Option<WriterMessage>,
    storage: ProduceStorage<'_>,
    signals: ProduceSignals<'_>,
    diskless: DisklessParts<'_>,
) {
    let ProduceGroup {
        first,
        max_size: max_produce_group,
    } = group;
    let mut jobs = vec![first];
    while jobs.len() < max_produce_group {
        match rx.try_recv() {
            Ok(WriterMessage::Produce(job)) => jobs.push(job),
            Ok(other) => {
                *pending = Some(other);
                break;
            }
            Err(_) => break,
        }
    }
    append_group(identity, jobs, storage, signals, diskless).await;
}

/// Appends a group of produce jobs, in order, and answers each one.
pub(super) async fn append_group(
    identity: WriterPartition<'_>,
    jobs: Vec<ProduceJob>,
    storage: ProduceStorage<'_>,
    signals: ProduceSignals<'_>,
    diskless: DisklessParts<'_>,
) {
    let DisklessParts { wal, sequencer } = diskless;
    let ProduceStorage {
        log,
        status: StorageStatus {
            log_dir,
            registry: log_dir_status,
        },
        producer_state,
    } = storage;
    let ProduceSignals {
        append_notify,
        replica_state,
        hw_advance_notify,
    } = signals;

    let mut acks = Vec::with_capacity(jobs.len());
    let mut datas = Vec::with_capacity(jobs.len());
    let mut checks = Vec::with_capacity(jobs.len());
    for ProduceJob {
        data,
        ack,
        producer_check,
    } in jobs
    {
        acks.push(ack);
        datas.push(data);
        checks.push(producer_check);
    }

    // A transaction marker adds a complete transaction that holds the last
    // stable offset until the high watermark passes the marker. Release the
    // ones the high watermark already passed before the group appends, so the
    // set stays bounded on a partition that no reader fetches from.
    let high_watermark = if datas
        .iter()
        .any(|data| data.control_producer_id().is_some())
    {
        Some(replica_state.lock().await.hw)
    } else {
        None
    };

    let append_result = if wal.is_some() {
        let Some(sequencer) = sequencer else {
            for ack in acks {
                let _ = ack.send(Err(storage_failure_error(
                    "diskless append missing offset sequencer",
                    "no sequencer configured",
                )));
            }
            return;
        };
        let count = datas.iter().map(ProduceData::record_count).sum();
        match sequencer
            .assign(identity.topic, identity.partition, count)
            .await
        {
            Ok(base) => {
                run_produce_append_batch_at(Arc::clone(log), base, high_watermark, (datas, checks))
                    .await
            }
            Err(error) => {
                for ack in acks {
                    let _ = ack.send(Err(reservation_failure(&error)));
                }
                return;
            }
        }
    } else {
        run_produce_append_batch(Arc::clone(log), high_watermark, (datas, checks)).await
    };
    let (results, leo, control_entries) = match append_result {
        Ok(value) => value,
        Err(err) => {
            flag_storage_failure(&err, log_dir, log_dir_status);
            for ack in acks {
                let _ = ack.send(Err(storage_failure_error(
                    "append task panicked",
                    "group append panic",
                )));
            }
            return;
        }
    };

    // A transaction marker changes the producer state in the log. Without
    // this copy the tracker keeps the state from before the marker until a
    // restart rebuilds it from the log, and a produce gets a different
    // answer before and after that restart. `control_entries` comes from the
    // same lock acquisition the append itself already ran under
    // `block_in_place`/`spawn_blocking` (see `append_produce_batch`'s doc
    // comment): mirroring here takes no further `std::sync::Mutex` lock on
    // this async task, only the `ProducerState` per-partition `tokio::sync::
    // Mutex` this call already awaits cooperatively.
    if !control_entries.is_empty() {
        producer_state
            .mirror_log_entries(identity.topic, identity.partition, control_entries)
            .await;
    }

    let any_ok = results.iter().any(Result::is_ok);
    for err in results.iter().filter_map(|result| result.as_ref().err()) {
        flag_storage_failure(err, log_dir, log_dir_status);
    }

    // An in-process (non-diskless) leader recompute takes no I/O, only the
    // uncontended `replica_state` lock, so it runs here, before any job's ack
    // fires. Doing it after the ack (as a diskless append still does, below)
    // leaves a window where a caller that only awaits the ack -- such as
    // `EndTxn`'s synchronous marker append, which never calls
    // `await_hw_at_least` the way an `acks=-1` client produce does -- can
    // observe success before this partition's own high watermark reflects
    // the append. A concurrent `ListOffsets`/`Fetch` against the same broker
    // then reads a high watermark that is briefly behind what the caller was
    // just told is committed. Recomputing first closes that gap: by the time
    // any ack is visible, `replica_state.hw` already covers it.
    let leader_hw_advanced = if any_ok && wal.is_none() {
        let mut state = replica_state.lock().await;
        let previous = state.hw;
        Some(state.recompute_hw_for_leader_append(leo) > previous)
    } else {
        None
    };

    for (ack, result) in acks.into_iter().zip(results) {
        let _ = ack.send(result);
    }

    if any_ok {
        append_notify.notify_waiters();
        // The diskless path keeps its ack ahead of durability on purpose: the
        // ack only promises an assigned offset, and a caller that needs the
        // write durable awaits a separate `SyncDurable` command explicitly.
        let advanced = if let Some(advanced) = leader_hw_advanced {
            advanced
        } else {
            let wal = wal.expect("any_ok && wal.is_none() is false, so wal is Some");
            match wal.sync_durable(leo).await {
                Ok(durable) => {
                    let mut state = replica_state.lock().await;
                    let previous = state.hw;
                    state.recompute_hw_for_wal_durable(durable) > previous
                }
                Err(error) => {
                    flag_storage_failure(&error, log_dir, log_dir_status);
                    false
                }
            }
        };
        if advanced {
            hw_advance_notify.notify_waiters();
        }
    }
}

/// The answer to each job of a group whose offset reservation failed.
///
/// A controller quorum without an active leader refuses the reservation
/// before it reserves an offset, so the group appended nothing. That is a
/// leader election, not a disk failure. The job gets the same
/// [`RaftError::NotLeader`] that the sequencer returned, and the produce path
/// answers `NOT_LEADER_OR_FOLLOWER`, as Kafka does when a broker cannot append
/// because leadership moves. The client refreshes its metadata and sends the
/// batch again. Every other failure of the reservation fails the group as a
/// storage failure, and the log directory stays online.
fn reservation_failure(error: &BrokerError) -> BrokerError {
    match error {
        BrokerError::Raft(RaftError::NotLeader { current_leader }) => {
            BrokerError::Raft(RaftError::NotLeader {
                current_leader: *current_leader,
            })
        }
        other => storage_failure_error("offset assignment failed", other),
    }
}
