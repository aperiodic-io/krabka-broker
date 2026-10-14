//! Spawned actor task that serializes appends for a single partition.
//!
//! The task owns the only `&mut Log` reference, through the shared
//! `Arc<Mutex<Log>>`. Reads bypass the actor. They take the same mutex for a
//! short time. The actor sends ordered acks back to producers. It also wakes
//! long-poll Fetch consumers with a shared `Notify` after each successful
//! append.

#[cfg(test)]
use crate::config::BrokerConfig;
use {
    crate::{
        delivery::DeliveryHandles,
        error::BrokerError,
        log_dir_status::LogDirRegistry,
        partition::{
            Partition, ProduceJob, ReplicatedBatch, SwapOutcome, WriterGate, WriterMessage,
        },
        producer_state::ProducerState,
        replica_state::ReplicaState,
        wal::{OffsetSequencer, SharedWal},
    },
    arc_swap::ArcSwap,
    krabka_ids::PartitionIndex,
    krabka_log::Log,
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio::sync::{Notify, mpsc},
};

mod append;
mod compaction;
mod mutations;
mod produce;
mod retention;
mod storage;
mod swap;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests;

// Only crate::wal's test modules reach the append helper through this path, so
// the re-export is test-gated: in a normal build it would be dead.
#[cfg(test)]
pub(crate) use self::append::run_produce_append_batch;
pub(crate) use self::storage::{flag_storage_failure, storage_failure_error};
use self::{
    compaction::handle_compact,
    mutations::{ReplicationState, ResetMutation, handle_log_reset, handle_replicate, handle_trim},
    produce::{
        DisklessParts, ProduceGroup, ProduceSignals, ProduceStorage, append_jobs, finish_group,
        handle_produce,
    },
    retention::handle_retention,
    storage::{StorageStatus, lock_log},
    swap::swap_future_log,
};

/// The topic name and partition index a writer message belongs to.
#[derive(Clone, Copy)]
struct WriterPartition<'a> {
    topic: &'a str,
    partition: PartitionIndex,
}

/// The topic and partition owned by a writer task.
pub struct WriterIdentity {
    pub topic: String,
    pub partition: PartitionIndex,
}

/// The log and its current directory.
pub struct WriterStorage {
    pub log: Arc<Mutex<Log>>,
    pub log_dir: Arc<ArcSwap<PathBuf>>,
}

/// Notifications, replica state, and scheduled-delivery handles used by the writer.
pub struct WriterSignals {
    pub append_notify: Arc<Notify>,
    pub replica_state: Arc<tokio::sync::Mutex<ReplicaState>>,
    pub hw_advance_notify: Arc<Notify>,
    pub delivery: DeliveryHandles,
}

/// Storage health, producer state, and optional shared WAL used by the writer.
#[derive(Default)]
pub struct WriterServices {
    pub log_dir_status: LogDirRegistry,
    pub producer_state: Arc<ProducerState>,
    pub wal: Option<SharedWal>,
}

/// Loop on the receive side of the partition's `WriterMessage` channel.
///
/// The loop stops when the channel closes, that is, when all senders drop.
#[cfg(test)]
pub async fn run(
    identity: WriterIdentity,
    storage: WriterStorage,
    rx: mpsc::Receiver<WriterMessage>,
    signals: WriterSignals,
    services: WriterServices,
) {
    run_with_sequencer(
        identity,
        storage,
        rx,
        signals,
        services,
        BrokerConfig::default().max_produce_group,
        None,
    )
    .await;
}

/// The writer loop behind a [`WriterGate`] that admits no inline append:
/// every append runs on the writer task.
#[cfg(any(test, feature = "test-helpers"))]
pub async fn run_with_sequencer(
    identity: WriterIdentity,
    storage: WriterStorage,
    rx: mpsc::Receiver<WriterMessage>,
    signals: WriterSignals,
    services: WriterServices,
    max_produce_group: usize,
    sequencer: Option<Arc<dyn OffsetSequencer>>,
) {
    run_gated(identity, storage, rx, signals, services, WriterPolicy {
        max_produce_group,
        sequencer,
        gate: WriterGate::shared(false),
    })
    .await;
}

/// How a writer groups produces, assigns offsets, and shares its log.
pub struct WriterPolicy {
    /// The most produce jobs one append drains from the channel.
    pub max_produce_group: usize,
    /// A diskless partition's offset sequencer.
    pub sequencer: Option<Arc<dyn OffsetSequencer>>,
    /// The gate that lets a produce append on its own task while this
    /// writer is idle; see [`WriterGate`].
    pub gate: Arc<WriterGate>,
}

/// Runs log maintenance for as long as it is due, and returns the delay until
/// it is due again. A failed run backs off for 100 ms while the writer keeps
/// accepting control messages.
async fn maintain_while_due(
    log: &Arc<Mutex<Log>>,
    gate: &WriterGate,
    storage_status: StorageStatus<'_>,
) -> Option<std::time::Duration> {
    loop {
        let delay = lock_log(log).maintenance_delay(std::time::SystemTime::now());
        if delay != Some(std::time::Duration::ZERO) {
            return delay;
        }
        // Maintenance mutates the log too, so it waits out an inline append.
        let _busy = gate.writer_busy().await;
        let maintenance_log = Arc::clone(log);
        let result = storage::run_log_mutation(
            move || {
                lock_log(&maintenance_log)
                    .maintain(std::time::SystemTime::now())
                    .map_err(BrokerError::from)
            },
            "log maintenance task panicked",
            storage_status,
        )
        .await;
        if let Err(error) = result {
            tracing::warn!(%error, "log maintenance failed");
            return Some(std::time::Duration::from_millis(100));
        }
    }
}

/// The partition's writer loop, under `policy`.
pub async fn run_gated(
    WriterIdentity { topic, partition }: WriterIdentity,
    WriterStorage { log, log_dir }: WriterStorage,
    mut rx: mpsc::Receiver<WriterMessage>,
    WriterSignals {
        append_notify,
        replica_state,
        hw_advance_notify,
        delivery,
    }: WriterSignals,
    WriterServices {
        log_dir_status,
        producer_state,
        wal,
    }: WriterServices,
    policy: WriterPolicy,
) {
    let WriterPolicy {
        max_produce_group,
        sequencer,
        gate,
    } = policy;
    let storage_status = StorageStatus {
        log_dir: &log_dir,
        registry: &log_dir_status,
    };
    let writer_partition = WriterPartition {
        topic: &topic,
        partition,
    };
    // `pending` holds a non-Produce message that was pulled off the channel
    // while group-draining Produce jobs (see the Produce arm). It is handled on
    // the next iteration so control messages are never reordered ahead of the
    // produces that preceded them in the channel.
    let mut pending: Option<WriterMessage> = None;
    // The lowest offset a truncation or reset cut the log to since the KIP-113
    // move task last asked. A future log must be cut to it before it may
    // replace the current log.
    let mut unreported_cut: Option<krabka_log::Offset> = None;
    loop {
        // Disarmed while the deadline is computed, so a nudge that lands in
        // between counts as earlier and leaves a wake-up for the select below;
        // see `WriterGate::disarm`.
        gate.disarm();
        let delay = maintain_while_due(&log, &gate, storage_status).await;
        gate.arm(delay);
        let msg = match pending.take() {
            Some(m) => m,
            None => match tokio::select! {
                message = std::future::poll_fn(|cx| gate.poll_recv(&mut rx, cx)) => message,
                () = tokio::time::sleep(delay.unwrap_or(std::time::Duration::ZERO)), if delay.is_some() => continue,
                // An inline append brought the next maintenance forward.
                () = gate.nudged() => continue,
            } {
                Some(m) => m,
                None => break, // channel closed: every sender dropped
            },
        };
        // Held through the message, so an inline produce never lands inside
        // it; see `WriterGate`.
        let _busy = gate.writer_busy().await;
        match msg {
            WriterMessage::Barrier { ack } => {
                let _ = ack.send(());
            }
            WriterMessage::Produce(first) => {
                handle_produce(
                    writer_partition,
                    ProduceGroup {
                        first,
                        max_size: max_produce_group,
                    },
                    &mut rx,
                    &mut pending,
                    ProduceStorage {
                        log: &log,
                        status: storage_status,
                        producer_state: &producer_state,
                    },
                    ProduceSignals {
                        append_notify: &append_notify,
                        replica_state: &replica_state,
                        hw_advance_notify: &hw_advance_notify,
                    },
                    DisklessParts {
                        wal: wal.as_ref(),
                        sequencer: sequencer.as_ref(),
                    },
                )
                .await;
            }
            WriterMessage::SyncDurable { leo, ack } => {
                let result = sync_durable(
                    leo,
                    &log,
                    storage_status,
                    &replica_state,
                    &hw_advance_notify,
                    wal.as_ref(),
                )
                .await;
                let _ = ack.send(result);
            }
            WriterMessage::Replicate { batch, ack } => {
                handle_replicate(
                    writer_partition,
                    &log,
                    storage_status,
                    ReplicationState {
                        producer_state: &producer_state,
                        replica_state: &replica_state,
                    },
                    batch,
                    ack,
                    &append_notify,
                )
                .await;
            }
            WriterMessage::ReplicateVerbatim { batches, ack } => {
                mutations::handle_replicate_verbatim(
                    &log,
                    &log_dir,
                    &log_dir_status,
                    batches,
                    ack,
                    &append_notify,
                )
                .await;
            }
            WriterMessage::Truncate { offset, ack } => {
                if handle_log_reset(
                    &log,
                    storage_status,
                    &replica_state,
                    wal.as_ref(),
                    ResetMutation::Truncate(offset),
                    ack,
                )
                .await
                {
                    unreported_cut = Some(unreported_cut.map_or(offset, |cut| cut.min(offset)));
                }
            }
            WriterMessage::ResetTo { new_base, ack } => {
                if handle_log_reset(
                    &log,
                    storage_status,
                    &replica_state,
                    wal.as_ref(),
                    ResetMutation::Reset(new_base),
                    ack,
                )
                .await
                {
                    unreported_cut = Some(unreported_cut.map_or(new_base, |cut| cut.min(new_base)));
                }
            }
            WriterMessage::TakeFutureTruncation { ack } => {
                let _ = ack.send(unreported_cut.take());
            }
            WriterMessage::TrimToOffset { new_start, ack } => {
                handle_trim(&log, storage_status, wal.as_ref(), new_start, ack).await;
            }
            WriterMessage::SetLogConfig { config, ack } => {
                lock_log(&log).set_config(config);
                let _ = ack.send(());
            }
            WriterMessage::Retain { ack } => {
                handle_retention((&log, &log_dir, &log_dir_status), &replica_state, ack).await;
            }
            WriterMessage::Compact { ack } => {
                handle_compact((&log, &log_dir, &log_dir_status), &replica_state, ack).await;
            }
            #[cfg(any(test, feature = "test-helpers"))]
            WriterMessage::TestSetLogStart { new_start, ack } => {
                use crate::error::BrokerError;

                let result = lock_log(&log)
                    .set_log_start_offset(new_start)
                    .map_err(BrokerError::from);
                let _ = ack.send(result);
            }
            WriterMessage::SwapFutureLog {
                target_log_dir,
                future_log,
                future_path,
                target_partition_path,
                ack,
            } => {
                // Equal log ends do not make the two logs equal once the
                // current log was cut: it may have grown back with records
                // the future log lacks.
                let result = if unreported_cut.is_some() {
                    Ok(SwapOutcome::NotCaughtUp)
                } else {
                    swap_future_log(
                        &log,
                        &log_dir,
                        target_log_dir,
                        &future_log,
                        &future_path,
                        &target_partition_path,
                    )
                };
                let _ = ack.send(result);
                // No `append_notify` — swap doesn't deliver new data,
                // and consumers re-read from the swapped `log` against
                // identical offsets.
            }
        }
        // Every arm above can move the log end, the log start, or the delivery
        // policy itself, so one refresh here covers all of them rather than
        // leaving the mirror stale after the arms that do not append.
        publish_delivery(&log, &delivery);
    }
}

/// Make the log prefix ending at `leo` durable: sync the WAL of a diskless
/// partition and advance the high watermark to what it made durable, or fsync
/// the log of an ordinary partition.
async fn sync_durable(
    leo: krabka_log::Offset,
    log: &Arc<Mutex<Log>>,
    storage_status: StorageStatus<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    hw_advance_notify: &Notify,
    wal: Option<&SharedWal>,
) -> Result<(), BrokerError> {
    let Some(wal) = wal else {
        let log = Arc::clone(log);
        return storage::run_log_mutation(
            move || lock_log(&log).sync().map_err(BrokerError::from),
            "durable sync task panicked",
            storage_status,
        )
        .await;
    };
    if replica_state.lock().await.hw >= leo {
        return Ok(());
    }
    let durable = wal.sync_durable(leo).await?;
    let mut state = replica_state.lock().await;
    let previous = state.hw;
    state.recompute_hw_for_wal_durable(durable);
    if state.hw > previous {
        hw_advance_notify.notify_waiters();
    }
    Ok(())
}

/// Appends a follower's replicated batches on the calling task when
/// `partition`'s writer is idle, as the writer's `ReplicateVerbatim` arm
/// would have, and hands them back when the writer is busy or has messages
/// waiting.
pub(crate) async fn replicate_inline(
    partition: &Partition,
    batches: Vec<ReplicatedBatch>,
    log_dir_status: &LogDirRegistry,
) -> Result<Result<(), BrokerError>, Vec<ReplicatedBatch>> {
    let Some(_busy) = partition.writer_gate.try_inline(partition) else {
        return Err(batches);
    };
    let (ack, answer) = tokio::sync::oneshot::channel();
    mutations::handle_replicate_verbatim(
        &partition.log,
        &partition.log_dir,
        log_dir_status,
        batches,
        ack,
        &partition.append_notify,
    )
    .await;
    after_inline_mutation(partition);
    Ok(answer.await.unwrap_or_else(|_| {
        Err(BrokerError::Replication(
            "inline replicate dropped its answer".into(),
        ))
    }))
}

/// Appends `job` on the calling task when `partition`'s writer is idle, as
/// the writer itself would have, and hands it back when the writer is busy
/// or has messages waiting, for the caller to queue.
///
/// The job's acknowledgement is usually answered before this returns, so the
/// caller finds it ready instead of waiting on the writer task. What follows
/// the append -- the high watermark, the acknowledgement, the wakeups -- has
/// to happen once the records are in the log, even if the request that made
/// the append goes away. So it is polled here once, which finishes it when
/// nothing it locks is contended, and handed to a task of its own otherwise,
/// never dropped half done.
pub(crate) async fn produce_inline(
    partition: &Partition,
    job: ProduceJob,
    log_dir_status: &LogDirRegistry,
    producer_state: &Arc<ProducerState>,
) -> Result<(), ProduceJob> {
    let Some(_busy) = partition.writer_gate.try_inline(partition) else {
        return Err(job);
    };
    let appended = append_jobs(
        WriterPartition {
            topic: &partition.topic,
            partition: partition.index,
        },
        vec![job],
        ProduceStorage {
            log: &partition.log,
            status: StorageStatus {
                log_dir: &partition.log_dir,
                registry: log_dir_status,
            },
            producer_state,
        },
        ProduceSignals {
            append_notify: &partition.append_notify,
            replica_state: &partition.replica_state,
            hw_advance_notify: &partition.hw_advance_notify,
        },
        DisklessParts {
            wal: None,
            sequencer: None,
        },
    )
    .await;
    if let Some(appended) = appended {
        let mut finish = Box::pin(finish_group(appended));
        if futures_util::poll!(finish.as_mut()).is_pending() {
            tokio::spawn(finish);
        }
    }
    after_inline_mutation(partition);
    Ok(())
}

/// What the writer does after every message, and what it would have computed
/// before its next one, for a mutation an inline append made in its place:
/// refresh the delivery watermark, and bring the writer's next maintenance
/// forward if the append made it due sooner.
fn after_inline_mutation(partition: &Partition) {
    publish_delivery(&partition.log, &partition.delivery);
    let delay = lock_log(&partition.log).maintenance_delay(std::time::SystemTime::now());
    partition.writer_gate.nudge_if_earlier(delay);
}

/// Refresh the partition's delivery watermark after a writer message, and
/// re-arm the broker-wide delivery scheduler when the log is left with a batch
/// waiting.
///
/// The writer runs this after every message, because a truncation, a trim, a
/// compaction, a log-dir swap and a config change all move something the
/// watermark is derived from, not only an append.
///
/// This is a liveness step, not a correctness one. A fetch recomputes the
/// watermark under the log mutex it already holds, so a skipped refresh delays
/// a parked consumer and can never let it read a batch early. On a topic that
/// delivers immediately the call costs one uncontended mutex and no I/O.
fn publish_delivery(log: &Mutex<Log>, delivery: &DeliveryHandles) {
    if let Some(state) = delivery.publish_now(log)
        && let Some(deadline_ms) = state.next_deadline_ms
    {
        delivery.wake_scheduler(deadline_ms);
    }
}
