//! Per-broker `ShareCoordinator`, the KIP-932 persister.
//!
//! The coordinator owns the in-memory delivery state for each
//! `(group, topicId, partition)` of every `__share_group_state` partition that
//! this broker hosts as leader. It writes each state change as a
//! `ShareSnapshot` or `ShareUpdate` record in the matching
//! `__share_group_state` partition. It replays a partition when it becomes the
//! leader of it.
//!
//! This coordinator mirrors [`crate::txn::coordinator::TxnCoordinator`].
//!
//! Leadership follows Kafka's `CoordinatorRuntime`. When this broker becomes
//! the leader of a `__share_group_state` partition (a new entry in the
//! metadata image, or a new leader epoch), the coordinator drops the keys of
//! that partition and replays its log in a background task. Until the replay
//! ends, every state-machine method answers `COORDINATOR_LOAD_IN_PROGRESS` for
//! the keys of that partition. When the broker stops leading the partition,
//! the coordinator drops the keys and answers `NOT_COORDINATOR`.
//!
//! Operations on one state partition run serially, including initialization
//! of a key that has no state cell yet. Each method holds a read guard on the
//! leadership map from its status check to its last in-memory change. A load
//! or an unload takes the write guard, so it never interleaves with an operation
//! that is still running on the same partition.
//!
//! An operation answers only when the records of its partition are
//! committed, as a write of Kafka's `CoordinatorRuntime` does. The coordinator
//! appends a record only while the partition leads locally at the leader epoch
//! of the term, and it stamps that epoch on the batch. It applies the record
//! to the in-memory state at once, as the runtime replays a record when it
//! appends it. It then releases the read guard and waits until the high
//! watermark covers the last record that the term wrote. If the partition
//! gets a new leader or a new leader epoch before that, the operation answers
//! `NOT_COORDINATOR`. If the wait takes longer than
//! `share.coordinator.write.timeout.ms`, it answers `COORDINATOR_NOT_AVAILABLE`.
//! An append that times out or loses its caller can still complete in the
//! writer. Its shard stops serving operations and wakes the supervisor to
//! drain prior writer commands and replay, including when metadata has not
//! changed. Requests during recovery answer `COORDINATOR_LOAD_IN_PROGRESS`;
//! a retry wakes another bounded recovery attempt after a failed load.
//!
//! This file holds the coordinator's identity: the shared types, the struct,
//! and the leadership and partitioning accessors. The state machine lives in
//! `state_machine`, the durable append and the log prune in `persist`, and the
//! log replay in `recovery`.

use {
    dashmap::DashMap,
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    qubit_clock::WallClock,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    },
    tokio::{
        sync::{Mutex, OwnedMutexGuard, RwLock, RwLockReadGuard},
        task::JoinHandle,
    },
};

pub(crate) mod jobs;
mod persist;
mod recovery;
mod state_machine;

#[cfg(test)]
pub(crate) mod test_support;

use crate::{
    codes::{COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR},
    partition_registry::PartitionRegistry,
    share_coordinator::{
        bootstrap, config::ShareCoordinatorConfig, partitioner::partition_for_share_key,
        persistence::StateBatch, state::SharePartitionState,
    },
    time_util::epoch_millis,
};

/// In-memory map key: `(group_id, topic_id, partition)`.
type ShareStateKey3 = (String, uuid::Uuid, i32);

/// KIP-932 share-group state epoch.
///
/// The coordinator bumps this epoch on initialization and on
/// re-initialization, for example `AlterShareGroupOffsets`. It fences a write
/// or initialize that carries an older epoch with `FENCED_STATE_EPOCH`.
pub(crate) type StateEpoch = i32;

/// Leader epoch of the share-partition leader that issued a state write.
///
/// The coordinator fences a stale value with `FENCED_LEADER_EPOCH`.
pub(crate) type LeaderEpoch = i32;

/// Per-partition Kafka wire error code for a failed state-machine operation.
///
/// See [`crate::codes`].
pub(crate) type ShareErrorCode = i16;

/// Summary tuple that [`ShareCoordinator::read_summary`] returns.
///
/// The fields are `state_epoch`, `leader_epoch`, `start_offset`, and
/// `delivery_complete_count`.
pub(crate) type ShareStateSummary = (StateEpoch, LeaderEpoch, Offset, i32);

/// Kafka's messages for the share-state error rows: the `Errors` default
/// messages and the validation messages of `ShareCoordinatorShard`.
pub(crate) mod message {
    pub(crate) const NEGATIVE_PARTITION_ID: &str = "The partition id cannot be a negative number.";
    pub(crate) const NEGATIVE_LEADER_EPOCH: &str = "The leader epoch cannot be a negative number.";
    pub(crate) const NEGATIVE_STATE_EPOCH: &str = "The state epoch cannot be a negative number.";
    pub(crate) const WRITE_UNINITIALIZED_SHARE_PARTITION: &str =
        "Write operation on uninitialized share partition not allowed.";
    pub(crate) const READ_UNINITIALIZED_SHARE_PARTITION: &str =
        "Read operation on uninitialized share partition not allowed.";
    pub(crate) const UNKNOWN_SERVER_ERROR: &str =
        "The server experienced an unexpected error when processing the request.";
    pub(crate) const UNKNOWN_TOPIC_OR_PARTITION: &str =
        "This server does not host this topic-partition.";
    pub(crate) const COORDINATOR_LOAD_IN_PROGRESS: &str =
        "The coordinator is loading and hence can't process requests.";
    pub(crate) const NOT_COORDINATOR: &str = "This is not the correct coordinator.";
    pub(crate) const REQUEST_TIMED_OUT: &str = "The request timed out.";
    pub(crate) const KAFKA_STORAGE_ERROR: &str =
        "Disk error when trying to access log file on the disk.";
    pub(crate) const FENCED_LEADER_EPOCH: &str =
        "The leader epoch in the request is older than the epoch on the broker.";
    pub(crate) const FENCED_STATE_EPOCH: &str =
        "The coordinator rejected the request because the state epoch did not match.";
}

/// A `ReadShareGroupState` or `WriteShareGroupState` that the coordinator
/// did not apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShareStateError {
    /// The shard refused the request. Kafka puts `message` on the row as it
    /// is.
    Refused {
        code: ShareErrorCode,
        message: &'static str,
    },
    /// The operation did not run to its end: this broker is not the
    /// coordinator, the state partition loads, or the append failed. `code`
    /// is the code after Kafka's
    /// `CoordinatorOperationExceptionHelper.handleOperationException`, and
    /// `message` is the message of the error before that mapping. Kafka
    /// prefixes it with `Unable to read share group state: ` or
    /// `Unable to write share group state: `.
    Operation {
        code: ShareErrorCode,
        message: &'static str,
    },
}

impl ShareStateError {
    /// The wire error code.
    pub(crate) fn code(self) -> ShareErrorCode {
        match self {
            Self::Refused { code, .. } | Self::Operation { code, .. } => code,
        }
    }

    /// The wire error message. `operation` is `read` or `write`.
    pub(crate) fn row_message(self, operation: &str) -> String {
        match self {
            Self::Refused { message, .. } => message.to_owned(),
            Self::Operation { message, .. } => {
                format!("Unable to {operation} share group state: {message}")
            }
        }
    }

    /// The error of a status check that [`ShareCoordinator::active`] refused.
    fn inactive(code: ShareErrorCode) -> Self {
        let message = if code == COORDINATOR_LOAD_IN_PROGRESS {
            message::COORDINATOR_LOAD_IN_PROGRESS
        } else {
            message::NOT_COORDINATOR
        };
        Self::Operation { code, message }
    }
}

/// The fields of one `WriteShareGroupState` partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShareWrite {
    pub(crate) state_epoch: StateEpoch,
    pub(crate) leader_epoch: LeaderEpoch,
    pub(crate) start_offset: Offset,
    /// `-1` when the request version has no such field.
    pub(crate) delivery_complete_count: i32,
    pub(crate) batches: Vec<StateBatch>,
}

/// `start_offset` sentinel for "no persisted share state".
///
/// This value tells the share-partition leader to initialize delivery from
/// scratch (KIP-932).
pub(crate) const UNINITIALIZED_START_OFFSET: i64 = -1;

/// Load status of one `__share_group_state` partition that this broker leads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadStatus {
    /// The metadata image names this broker as leader, but the partition log
    /// is not open locally yet, or an indeterminate append still needs a
    /// successful writer barrier and replay. The next refresh retries load.
    Pending,
    /// A replay of the partition log runs.
    Loading,
    /// The replay ended. The partition serves requests.
    Active,
    /// The replay failed. The partition answers `NOT_COORDINATOR`, and the
    /// next refresh loads it again.
    Failed,
}

/// One led `__share_group_state` partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LedPartition {
    /// The partition leader epoch that the metadata image gave for this term.
    leader_epoch: i32,
    /// Identifies the load of this term. A load that ends after a newer term
    /// started does not install its state.
    generation: u64,
    status: LoadStatus,
}

/// Map from led `__share_group_state` partition to its load status.
pub(super) type LeaderPartitions = HashMap<PartitionIndex, LedPartition>;

/// One term of a `__share_group_state` partition that this broker leads: the
/// partition and the leader epoch that its load ran under.
///
/// A record of the term is committed only while the partition leads locally
/// at this epoch. Kafka's `CoordinatorRuntime` unloads the shard, and fails
/// its pending writes, when the leader epoch changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Term {
    pub(super) partition: PartitionIndex,
    pub(super) leader_epoch: i32,
}

/// The serialized operation and leadership guards of one active state
/// partition, and the term that the partition serves.
pub(super) struct Active<'a> {
    _led: RwLockReadGuard<'a, LeaderPartitions>,
    _operation: OwnedMutexGuard<()>,
    pub(super) term: Term,
}

/// The load tasks that one [`ShareCoordinator::refresh_leader_partitions`]
/// call started.
///
/// A caller that does not wait drops this value. The tasks run on.
#[derive(Debug, Default, krabka_macros::ScheduledLoads)]
#[scheduled_loads(failure_message = "__share_group_state load task failed")]
pub(crate) struct ScheduledLoads(Vec<JoinHandle<()>>);

/// Per-broker share-state coordinator.
///
/// `Broker::start` constructs the coordinator and shares it with the
/// share-state wire handlers through an `Arc`.
pub(crate) struct ShareCoordinator {
    pub(crate) node_id: krabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// Live in-memory state: `(group, topicId, partition)` → locked state.
    state: DashMap<ShareStateKey3, Arc<Mutex<SharePartitionState>>>,
    /// The `__share_group_state` partitions this broker leads, with the load
    /// status of each.
    leader_partitions: RwLock<LeaderPartitions>,
    /// Serializes shard operations even when their logical key is absent.
    /// The map has at most one entry per configured state partition.
    operation_locks: DashMap<PartitionIndex, Arc<Mutex<()>>>,
    /// An accepted append may outlive its caller without applying live state.
    /// Only a writer barrier followed by successful replay clears this fence.
    uncertain_appends: DashMap<PartitionIndex, ()>,
    /// Wakes recovery without requiring a metadata change. Failed attempts
    /// wait for the next client retry or metadata change rather than spinning.
    recovery_needed: tokio::sync::Notify,
    /// Source of [`LedPartition::generation`].
    next_generation: AtomicU64,
    config: ShareCoordinatorConfig,
    /// The clock of the snapshot timestamps and of the cold-partition
    /// snapshot, Kafka's `time`.
    wall_clock: Arc<dyn WallClock>,
}

impl ShareCoordinator {
    pub(crate) fn new(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        config: ShareCoordinatorConfig,
    ) -> Self {
        Self::with_wall_clock(
            node_id,
            partitions,
            config,
            Arc::new(qubit_clock::StdWallClock::new()),
        )
    }

    /// [`ShareCoordinator::new`] with the wall clock injected.
    pub(crate) fn with_wall_clock(
        node_id: krabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        config: ShareCoordinatorConfig,
        wall_clock: Arc<dyn WallClock>,
    ) -> Self {
        Self {
            node_id,
            partitions,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashMap::new()),
            operation_locks: DashMap::new(),
            uncertain_appends: DashMap::new(),
            recovery_needed: tokio::sync::Notify::new(),
            next_generation: AtomicU64::new(0),
            config,
            wall_clock,
        }
    }

    /// The current time in milliseconds since the epoch.
    fn now_ms(&self) -> i64 { epoch_millis(self.wall_clock.now()) }

    /// Wait for an indeterminate append or a client retry to request a
    /// bounded replay against the supervisor's current metadata image.
    pub(crate) async fn recovery_requested(&self) { self.recovery_needed.notified().await; }

    /// Applies the leadership of `image` to the led partitions.
    ///
    /// For each partition that this broker now leads with a new leader epoch,
    /// the method drops the in-memory keys of the partition and starts a
    /// replay of its log, as Kafka's `ShareCoordinatorService.onElection`
    /// does. For each partition that this broker no longer leads, the method
    /// drops the keys, as `onResignation` does. The broker calls this method
    /// on every metadata change.
    pub(crate) async fn refresh_leader_partitions(
        self: &Arc<Self>,
        image: &MetadataImage,
    ) -> ScheduledLoads {
        let desired: HashMap<PartitionIndex, i32> = image
            .partitions_of(bootstrap::TOPIC)
            .filter(|p| p.leader == self.node_id)
            .map(|p| (PartitionIndex(p.partition), p.leader_epoch.0))
            .collect();
        // Most refreshes change nothing. Check that under the read guard, so
        // a refresh does not wait for the operations that are running.
        if !self.leadership_changes(&desired).await {
            return ScheduledLoads::default();
        }
        let mut to_load = Vec::new();
        let mut to_drop = Vec::new();
        {
            let mut led = self.leader_partitions.write().await;
            led.retain(|partition, _| {
                let keep = desired.contains_key(partition);
                if !keep {
                    to_drop.push(*partition);
                }
                keep
            });
            for (partition, leader_epoch) in desired {
                let local = self.partitions.contains(bootstrap::TOPIC, partition);
                let entry = led.get(&partition).copied();
                let new_term = entry.is_none_or(|e| e.leader_epoch != leader_epoch);
                let reload = entry
                    .is_some_and(|e| matches!(e.status, LoadStatus::Pending | LoadStatus::Failed))
                    && local;
                let reload = reload
                    || (local
                        && entry.is_some_and(|e| e.status == LoadStatus::Active)
                        && self.uncertain_appends.contains_key(&partition));
                if !new_term && !reload {
                    continue;
                }
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                let status = if local {
                    LoadStatus::Loading
                } else {
                    LoadStatus::Pending
                };
                led.insert(partition, LedPartition {
                    leader_epoch,
                    generation,
                    status,
                });
                to_drop.push(partition);
                if local {
                    to_load.push((partition, generation));
                }
            }
            // Drop the old keys while the write guard still holds, so no
            // operation sees a half-dropped partition.
            for partition in &to_drop {
                self.drop_partition_state(*partition);
            }
        }
        let handles = to_load
            .into_iter()
            .map(|(partition, generation)| {
                let coord = Arc::clone(self);
                tokio::spawn(async move {
                    coord.load_partition(partition, generation).await;
                })
            })
            .collect();
        ScheduledLoads(handles)
    }

    /// Whether `desired` (led partition to leader epoch) differs from the led
    /// partitions, or a pending or failed partition can load now.
    async fn leadership_changes(&self, desired: &HashMap<PartitionIndex, i32>) -> bool {
        let led = self.leader_partitions.read().await;
        led.len() != desired.len()
            || desired.iter().any(|(partition, leader_epoch)| {
                led.get(partition).is_none_or(|entry| {
                    entry.leader_epoch != *leader_epoch
                        || (matches!(entry.status, LoadStatus::Pending | LoadStatus::Failed)
                            && self.partitions.contains(bootstrap::TOPIC, *partition))
                        || (entry.status == LoadStatus::Active
                            && self.uncertain_appends.contains_key(partition))
                })
            })
    }

    /// Removes every in-memory key that maps to `state_partition`.
    fn drop_partition_state(&self, state_partition: PartitionIndex) {
        self.state.retain(|(group, topic_id, partition), _| {
            self.state_partition_for(group, topic_id, *partition) != state_partition
        });
    }

    /// The load status of `state_partition`, or `None` when this broker does
    /// not lead it.
    pub(crate) async fn load_status(&self, state_partition: PartitionIndex) -> Option<LoadStatus> {
        self.leader_partitions
            .read()
            .await
            .get(&state_partition)
            .map(|led| {
                if led.status == LoadStatus::Active
                    && self.uncertain_appends.contains_key(&state_partition)
                {
                    LoadStatus::Pending
                } else {
                    led.status
                }
            })
    }

    /// Returns a read guard on the leadership map, and the term of
    /// `state_partition`, when the partition is active.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_LOAD_IN_PROGRESS` while the partition loads, and
    /// `NOT_COORDINATOR` when this broker does not lead it or its load failed. These are the
    /// codes of Kafka's `CoordinatorRuntime.withActiveContextOrThrow`.
    pub(super) async fn active(
        &self,
        state_partition: PartitionIndex,
    ) -> Result<Active<'_>, ShareErrorCode> {
        let operation = self
            .operation_locks
            .entry(state_partition)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        // Wait before taking the leadership guard, so an unload can proceed
        // while this operation is queued. Check the active term after the wait.
        let operation = operation.lock_owned().await;
        let led = self.leader_partitions.read().await;
        if led.contains_key(&state_partition)
            && self.uncertain_appends.contains_key(&state_partition)
        {
            self.recovery_needed.notify_one();
            return Err(COORDINATOR_LOAD_IN_PROGRESS);
        }
        match led.get(&state_partition).copied() {
            Some(LedPartition {
                status: LoadStatus::Active,
                leader_epoch,
                ..
            }) => Ok(Active {
                _led: led,
                _operation: operation,
                term: Term {
                    partition: state_partition,
                    leader_epoch,
                },
            }),
            Some(LedPartition {
                status: LoadStatus::Pending | LoadStatus::Loading,
                ..
            }) => Err(COORDINATOR_LOAD_IN_PROGRESS),
            Some(LedPartition {
                status: LoadStatus::Failed,
                ..
            })
            | None => Err(NOT_COORDINATOR),
        }
    }

    /// Returns `true` if this broker leads `__share_group_state`-`state_partition`.
    ///
    /// The answer is `true` also while the partition loads. The persister
    /// client uses it to route a call to the local coordinator, which then
    /// answers `COORDINATOR_LOAD_IN_PROGRESS` until the load ends.
    pub(crate) async fn is_leader(&self, state_partition: PartitionIndex) -> bool {
        self.load_status(state_partition).await.is_some()
    }

    /// Test-only: makes this broker the leader of every local state partition
    /// at leader epoch 0, as the metadata reconcile does, and marks each
    /// partition active with no load.
    #[cfg(test)]
    pub(crate) async fn lead_all_partitions_for_test(&self) {
        self.lead_local_partitions_for_test().await;
        let mut led = self.leader_partitions.write().await;
        led.clear();
        for p in 0..self.config.state_topic_num_partitions {
            led.insert(PartitionIndex(p), LedPartition {
                leader_epoch: 0,
                generation: self.next_generation.fetch_add(1, Ordering::Relaxed),
                status: LoadStatus::Active,
            });
        }
    }

    /// Test-only: installs this broker as the leader of every local state
    /// partition at leader epoch 0, as the metadata reconcile does before the
    /// coordinator loads a partition.
    #[cfg(test)]
    async fn lead_local_partitions_for_test(&self) {
        for p in 0..self.config.state_topic_num_partitions {
            if let Some(part) = self.partitions.get(bootstrap::TOPIC, PartitionIndex(p)) {
                part.install_leader_change(self.node_id.0, 0).await;
            }
        }
    }

    /// Test-only: starts a new term on every state partition and replays each
    /// log, as a load after an election does.
    #[cfg(test)]
    pub(crate) async fn reload_all_partitions_for_test(&self) {
        self.lead_local_partitions_for_test().await;
        let mut terms = Vec::new();
        {
            let mut led = self.leader_partitions.write().await;
            for p in 0..self.config.state_topic_num_partitions {
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                led.insert(PartitionIndex(p), LedPartition {
                    leader_epoch: 0,
                    generation,
                    status: LoadStatus::Loading,
                });
                self.drop_partition_state(PartitionIndex(p));
                terms.push((PartitionIndex(p), generation));
            }
        }
        for (partition, generation) in terms {
            self.load_partition(partition, generation).await;
        }
    }

    /// Returns the `__share_group_state` partition index responsible for the
    /// share key `(group, topic_id, partition)`.
    #[must_use]
    pub(crate) fn state_partition_for(
        &self,
        group: &str,
        topic_id: &uuid::Uuid,
        partition: i32,
    ) -> PartitionIndex {
        PartitionIndex(partition_for_share_key(
            group,
            topic_id,
            partition,
            self.config.state_topic_num_partitions,
        ))
    }
}
