//! The `ShareCoordinator` state machine: `initialize`, `write`, `read`,
//! `read_summary`, and `delete`.
//!
//! These are the five operations the KIP-932 persister RPCs drive. They hold
//! the validation and epoch-fencing rules of Kafka's `ShareCoordinatorShard`,
//! and pick the record each operation appends. Every record goes through
//! [`ShareCoordinator::append_state_record`], which appends it and then
//! applies it to the in-memory state exactly as the log replay in `recovery`
//! does. Every operation answers through [`ShareCoordinator::answer`], which
//! waits until the records of the partition are committed.

use {
    super::{
        Active, LeaderEpoch, ShareCoordinator, ShareErrorCode, ShareStateError, ShareStateSummary,
        ShareWrite, StateEpoch, Term, UNINITIALIZED_START_OFFSET, message, persist::AppendError,
    },
    crate::{
        codes,
        share_coordinator::{
            persistence::{
                KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
                ShareUpdateValue, StateBatch, UNKNOWN_DELIVERY_COMPLETE_COUNT,
            },
            state::{SharePartitionState, combine_state_batches},
        },
    },
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    std::sync::Arc,
    tokio::sync::{Mutex, MutexGuard},
    tracing::{debug, warn},
};

#[cfg(test)]
mod tests;

/// One `__share_group_state` record value that an operation appends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StateRecord {
    Snapshot(ShareSnapshotValue),
    Update(ShareUpdateValue),
}

/// The progress fields of a write, as Kafka's
/// `WriteShareGroupStateRequestData.PartitionData` carries them into
/// `generateShareStateRecord`.
struct Progress<'a> {
    leader_epoch: LeaderEpoch,
    start_offset: Offset,
    delivery_complete_count: i32,
    batches: &'a [StateBatch],
}

/// Each state operation holds the active term through its validation and append,
/// then answers only after the original commit boundary.
macro_rules! state_operation {
    ($(#[$doc:meta])* $name:ident($this:ident, $image:ident, $group:ident, $topic_id:ident, $partition:ident;
        $($arg:ident: $arg_type:ty),* $(,)?) -> $answer:ty; $active:ident => $result:expr) => {
        $(#[$doc])*
        pub(crate) async fn $name(
            &$this,
            $image: &MetadataImage,
            $group: &str,
            $topic_id: uuid::Uuid,
            $partition: i32,
            $($arg: $arg_type,)*
        ) -> Result<$answer, ShareStateError> {
            let state_partition = $this.state_partition_for($group, &$topic_id, $partition);
            let $active = $this
                .active(state_partition)
                .await
                .map_err(ShareStateError::inactive)?;
            let result = $result;
            $this.answer($active, result).await
        }
    };
}

/// The key of one share-partition state.
struct StateKeyRef<'a> {
    group: &'a str,
    topic_id: uuid::Uuid,
    partition: i32,
}

impl ShareCoordinator {
    state_operation! {
        /// Serves an `InitializeShareGroupState` partition, as Kafka's
        /// `ShareCoordinatorShard.initializeState` does.
        ///
        /// The checks run in Kafka's order (`maybeGetInitializeStateError`): a
        /// negative partition, a stored state epoch above the request, and a
        /// topic partition that `image` does not hold. A state epoch of `-1` is
        /// "not supplied" and skips the fence, as in Kafka 4.3.1. Any request
        /// writes a `ShareSnapshot` with the next snapshot epoch (`0` for a new
        /// key), leader epoch `0`, no batches, and a delivery complete count of
        /// `-1` for an uninitialized start offset and `0` otherwise.
        ///
        /// With `ShareCoordinatorConfig::trunk_rules`, a negative state epoch is
        /// `INVALID_REQUEST`, and a request that repeats the stored state epoch
        /// and start offset is a no-op.
        ///
        /// # Errors
        ///
        /// Returns [`ShareStateError::Refused`] when a check fails, and
        /// [`ShareStateError::Operation`] when this broker is not the active
        /// coordinator of the key, the append fails, or the records of the
        /// partition do not commit.
        initialize(self, image, group, topic_id, partition; state_epoch: StateEpoch,
        start_offset: Offset,) -> ();
        active => self
            .initialize_in_term(
                active.term,
                image,
                StateKeyRef {
                    group,
                    topic_id,
                    partition,
                },
                state_epoch,
                start_offset,
            )
            .await
    }

    state_operation! {
        /// Applies a `WriteShareGroupState` partition, as Kafka's
        /// `ShareCoordinatorShard.writeState` does.
        ///
        /// The checks run in Kafka's order (`maybeGetWriteStateError`): a
        /// negative partition, an uninitialized key, a recorded leader epoch or
        /// state epoch above the request, and a topic partition that `image` does
        /// not hold. A leader epoch or state epoch of `-1` is "not supplied" and
        /// skips its fence, as in Kafka 4.3.1; with
        /// `ShareCoordinatorConfig::trunk_rules`, a negative one is
        /// `INVALID_REQUEST`. The record is the one `generateShareStateRecord`
        /// picks: see [`ShareCoordinator::share_state_record`].
        ///
        /// # Errors
        ///
        /// Returns [`ShareStateError::Refused`] when a check fails, and
        /// [`ShareStateError::Operation`] when this broker is not the active
        /// coordinator of the key, the append fails, or the records of the
        /// partition do not commit.
        write(self, image, group, topic_id, partition; request: ShareWrite,) -> ();
        active => self
            .write_in_term(active.term, image, group, topic_id, partition, request)
            .await
    }

    state_operation! {
        /// Serves a `ReadShareGroupState` partition, as Kafka's
        /// `ShareCoordinatorShard.readStateAndMaybeUpdateLeaderEpoch` does.
        ///
        /// The checks run in Kafka's order (`maybeGetReadStateError`): a negative
        /// partition, an uninitialized key, a recorded leader epoch above the
        /// request, and a topic partition that `image` does not hold. A leader
        /// epoch of `-1` is "not supplied": it skips the fence and answers the
        /// stored state with no record, as in Kafka 4.3.1. With
        /// `ShareCoordinatorConfig::trunk_rules`, a negative leader epoch is
        /// `INVALID_REQUEST`. When `leader_epoch` differs from the recorded
        /// leader epoch, the method appends the record of a write with the new
        /// leader epoch and the stored progress before it answers. A later write
        /// from a share-partition leader with an older epoch is then fenced.
        ///
        /// # Errors
        ///
        /// As [`ShareCoordinator::write`].
        read(self, image, group, topic_id, partition; leader_epoch: LeaderEpoch,) -> SharePartitionState;
        active => self
            .read_in_term(active.term, image, group, topic_id, partition, leader_epoch)
            .await
    }

    state_operation! {
        /// Serves a `ReadShareGroupStateSummary` partition, as Kafka's
        /// `ShareCoordinatorShard.readStateSummary` does.
        ///
        /// The checks run in Kafka's order: the state partition must be active,
        /// then `maybeGetReadStateSummaryError` refuses a negative partition and
        /// a topic partition that `image` does not hold. A key with no state
        /// answers `Ok(None)`.
        ///
        /// # Errors
        ///
        /// Returns the error of the refused partition, or
        /// [`ShareStateError::Operation`] when the records of the partition do
        /// not commit.
        read_summary_checked(self, image, group, topic_id, partition; ) -> Option<ShareStateSummary>;
        active => if partition < 0 {
            Err(invalid_request(message::NEGATIVE_PARTITION_ID))
        } else {
            match check_topic_partition(image, topic_id, partition) {
                Ok(()) => Ok(self.summary(group, topic_id, partition).await),
                Err(error) => Err(error),
            }
        }
    }

    state_operation! {
        /// Serves a `DeleteShareGroupState` partition, as Kafka's
        /// `ShareCoordinatorShard.deleteState` does.
        ///
        /// The checks run in Kafka's order (`maybeGetDeleteStateError`): a
        /// negative partition, then a topic partition that `image` does not hold.
        /// A key with no state is not an error, and nothing is appended for it.
        /// Otherwise the method writes a tombstone with the snapshot key and
        /// drops the in-memory entry.
        ///
        /// # Errors
        ///
        /// As [`ShareCoordinator::initialize`].
        delete(self, image, group, topic_id, partition; ) -> ();
        active => self
            .delete_in_term(active.term, image, group, topic_id, partition)
            .await
    }

    /// The body of [`ShareCoordinator::initialize`], under the read guard of
    /// `term`.
    async fn initialize_in_term(
        &self,
        term: Term,
        image: &MetadataImage,
        StateKeyRef {
            group,
            topic_id,
            partition,
        }: StateKeyRef<'_>,
        state_epoch: StateEpoch,
        start_offset: Offset,
    ) -> Result<(), ShareStateError> {
        let trunk = self.config.trunk_rules;
        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if trunk && state_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_STATE_EPOCH));
        }
        let entry = self.entry(group, topic_id, partition);
        let mut stored = match &entry {
            Some(entry) => Some(entry.lock().await),
            None => None,
        };
        // Kafka 4.3.1 reads a state epoch of -1 as "not supplied" and skips
        // the fence.
        if state_epoch != -1
            && stored
                .as_ref()
                .is_some_and(|st| st.fence_state_epoch > state_epoch)
        {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_STATE_EPOCH,
                message: message::FENCED_STATE_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;
        // Trunk only: Kafka 4.3.1 always writes a new snapshot.
        if trunk
            && stored.as_ref().is_some_and(|st| {
                st.fence_state_epoch == state_epoch && st.start_offset == start_offset
            })
        {
            return Ok(());
        }

        let now = self.now_ms();
        let snapshot = ShareSnapshotValue {
            snapshot_epoch: stored
                .as_ref()
                .map_or(0, |st| st.snapshot_epoch.wrapping_add(1)),
            state_epoch,
            leader_epoch: 0,
            start_offset,
            delivery_complete_count: if start_offset.0 == UNINITIALIZED_START_OFFSET {
                UNKNOWN_DELIVERY_COMPLETE_COUNT
            } else {
                0
            },
            create_timestamp: now,
            write_timestamp: now,
            state_batches: Vec::new(),
        };
        if let Some(st) = stored.as_mut() {
            return self
                .append_state_record(
                    term,
                    st,
                    group,
                    topic_id,
                    partition,
                    StateRecord::Snapshot(snapshot),
                )
                .await;
        }
        let key = state_key(KEY_SHARE_SNAPSHOT, group, topic_id, partition);
        let offset = self
            .persist_record(term, key, Some(snapshot.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share initialize persist failed");
                e.share_error()
            })?;
        self.state.insert(
            (group.to_string(), topic_id, partition),
            Arc::new(Mutex::new(SharePartitionState::from_snapshot(
                &snapshot, offset,
            ))),
        );
        Ok(())
    }

    /// The body of [`ShareCoordinator::write`], under the read guard of
    /// `term`.
    async fn write_in_term(
        &self,
        term: Term,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        request: ShareWrite,
    ) -> Result<(), ShareStateError> {
        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if self.config.trunk_rules {
            if request.leader_epoch < 0 {
                return Err(invalid_request(message::NEGATIVE_LEADER_EPOCH));
            }
            if request.state_epoch < 0 {
                return Err(invalid_request(message::NEGATIVE_STATE_EPOCH));
            }
        }
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Err(invalid_request(
                message::WRITE_UNINITIALIZED_SHARE_PARTITION,
            ));
        };
        let mut st = entry.lock().await;
        if request.leader_epoch != -1 && st.fence_leader_epoch > request.leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        if request.state_epoch != -1 && st.fence_state_epoch > request.state_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_STATE_EPOCH,
                message: message::FENCED_STATE_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        // A write never changes the leader epoch. Only a read raises it.
        let record = self.share_state_record(&st, &Progress {
            leader_epoch: st.leader_epoch,
            start_offset: request.start_offset,
            delivery_complete_count: request.delivery_complete_count,
            batches: &request.batches,
        });
        self.append_state_record(term, &mut st, group, topic_id, partition, record)
            .await
    }

    /// The body of [`ShareCoordinator::read`], under the read guard of
    /// `term`.
    async fn read_in_term(
        &self,
        term: Term,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        leader_epoch: LeaderEpoch,
    ) -> Result<SharePartitionState, ShareStateError> {
        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        if self.config.trunk_rules && leader_epoch < 0 {
            return Err(invalid_request(message::NEGATIVE_LEADER_EPOCH));
        }
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Err(invalid_request(message::READ_UNINITIALIZED_SHARE_PARTITION));
        };
        let mut st = entry.lock().await;
        if leader_epoch != -1 && st.fence_leader_epoch > leader_epoch {
            return Err(ShareStateError::Refused {
                code: codes::FENCED_LEADER_EPOCH,
                message: message::FENCED_LEADER_EPOCH,
            });
        }
        check_topic_partition(image, topic_id, partition)?;

        let current = st.clone();
        if leader_epoch == -1 || st.fence_leader_epoch == leader_epoch {
            return Ok(current);
        }
        let record = self.share_state_record(&st, &Progress {
            leader_epoch,
            start_offset: st.start_offset,
            delivery_complete_count: st.delivery_complete_count,
            batches: &[],
        });
        self.append_state_record(term, &mut st, group, topic_id, partition, record)
            .await?;
        Ok(current)
    }

    /// The record of a write, as Kafka's `generateShareStateRecord` builds
    /// it.
    ///
    /// Once the key has had `snapshot_update_records_per_snapshot` updates,
    /// the record is a `ShareSnapshot` with the next snapshot epoch, the
    /// stored batches combined with the written ones, and fresh timestamps.
    /// Otherwise it is a `ShareUpdate` that holds only the written batches,
    /// combined among themselves and clipped at the start offset.
    ///
    /// As in Kafka 4.3.1, the record carries the request as sent: the start
    /// offset of a snapshot is the request's, or the stored one when the
    /// request says `-1`, an update takes the request's start offset as it
    /// is, and the delivery complete count is the request's. A write with a
    /// lower start offset therefore lowers the stored one. With
    /// `ShareCoordinatorConfig::trunk_rules`, the start offset never goes
    /// back, and the delivery complete count is picked by
    /// [`delivery_complete_count`].
    fn share_state_record(&self, st: &SharePartitionState, progress: &Progress<'_>) -> StateRecord {
        let requested = progress.start_offset;
        let (snapshot_start, update_start, delivery_complete_count) = if self.config.trunk_rules {
            let start = requested.max(st.start_offset);
            (
                start,
                start,
                delivery_complete_count(st, requested, progress.delivery_complete_count),
            )
        } else {
            let snapshot_start = if requested.0 == -1 {
                st.start_offset
            } else {
                requested
            };
            (snapshot_start, requested, progress.delivery_complete_count)
        };
        if st.updates_since_snapshot >= self.config.snapshot_update_records_per_snapshot {
            let now = self.now_ms();
            StateRecord::Snapshot(ShareSnapshotValue {
                snapshot_epoch: st.snapshot_epoch.wrapping_add(1),
                state_epoch: st.state_epoch,
                leader_epoch: progress.leader_epoch,
                start_offset: snapshot_start,
                delivery_complete_count,
                create_timestamp: now,
                write_timestamp: now,
                state_batches: combine_state_batches(
                    &st.state_batches,
                    progress.batches,
                    snapshot_start,
                ),
            })
        } else {
            StateRecord::Update(ShareUpdateValue {
                snapshot_epoch: st.snapshot_epoch,
                leader_epoch: progress.leader_epoch,
                start_offset: update_start,
                delivery_complete_count,
                state_batches: combine_state_batches(&[], progress.batches, update_start),
            })
        }
    }

    /// Appends `record` for the key in `term` and applies it to `st` after
    /// the append succeeds, as the replay of the record does.
    ///
    /// The record is applied before it commits, as Kafka's
    /// `CoordinatorRuntime` replays a record when it appends it. The
    /// in-memory state then matches the local log. If the term ends before
    /// the record commits, the unload drops the state, and the next load
    /// replays the log of the new leader.
    pub(super) async fn append_state_record(
        &self,
        term: Term,
        st: &mut MutexGuard<'_, SharePartitionState>,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        record: StateRecord,
    ) -> Result<(), ShareStateError> {
        let (record_type, value) = match &record {
            StateRecord::Snapshot(snapshot) => (KEY_SHARE_SNAPSHOT, snapshot.encode()),
            StateRecord::Update(update) => (KEY_SHARE_UPDATE, update.encode()),
        };
        let key = state_key(record_type, group, topic_id, partition);
        let offset = self
            .persist_record(term, key, Some(value))
            .await
            .map_err(|e| {
                warn!(error = %e, "share state persist failed");
                e.share_error()
            })?;
        match &record {
            StateRecord::Snapshot(snapshot) => st.apply_snapshot(
                snapshot,
                offset,
                self.config.snapshot_update_records_per_snapshot,
            ),
            StateRecord::Update(update) => st.apply_update(update),
        }
        Ok(())
    }

    /// Answers `result` once every record of the term up to the last written
    /// one is committed, as Kafka's `CoordinatorRuntime` completes an
    /// operation.
    ///
    /// The method releases the read guard of `active` before it waits, so a
    /// load or an unload does not wait for a commit. A result that holds an
    /// append failure answers at once: the runtime fails a batch that did not
    /// append, with its operations. Every other result waits for the last
    /// written offset of the partition, whether the operation wrote a record
    /// or not. As in Kafka, an operation then never answers a state that the
    /// next leader of the partition can lose.
    ///
    /// # Errors
    ///
    /// Returns the error of `result`, or the error of
    /// [`ShareCoordinator::await_committed`] when the records do not commit.
    async fn answer<T>(
        &self,
        active: Active<'_>,
        result: Result<T, ShareStateError>,
    ) -> Result<T, ShareStateError> {
        if matches!(result, Err(ShareStateError::Operation { .. })) {
            return result;
        }
        let term = active.term;
        let Some(last_written) = self.last_written(term) else {
            return result;
        };
        drop(active);
        self.await_committed(term, last_written)
            .await
            .map_err(|error| {
                if matches!(error, AppendError::TimedOut) {
                    warn!(partition = term.partition.get(), %error, "share state not committed");
                } else {
                    debug!(partition = term.partition.get(), %error, "share state not committed");
                }
                error.share_error()
            })?;
        result
    }

    /// The state cell of `(group, topic_id, partition)`, when the key has
    /// state.
    pub(super) fn entry(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<Arc<Mutex<SharePartitionState>>> {
        self.state
            .get(&(group.to_string(), topic_id, partition))
            .map(|entry| entry.value().clone())
    }

    /// Test-only: the in-memory state of a key, with no status check and no
    /// side effect.
    #[cfg(test)]
    pub(crate) async fn state_for_test(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<SharePartitionState> {
        let entry = self.entry(group, topic_id, partition)?;
        let st = entry.lock().await;
        Some(st.clone())
    }

    /// Returns `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`.
    ///
    /// Returns `Ok(None)` when the key has no state.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_LOAD_IN_PROGRESS` or `NOT_COORDINATOR` when the
    /// state partition of the key is not active on this broker,
    /// `NOT_COORDINATOR` when the partition stops leading on this broker
    /// before its records commit, and `COORDINATOR_NOT_AVAILABLE` when they
    /// do not commit within the write timeout.
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<Option<ShareStateSummary>, ShareErrorCode> {
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let active = self.active(state_partition).await?;
        let summary = self.summary(group, topic_id, partition).await;
        self.answer(active, Ok(summary))
            .await
            .map_err(ShareStateError::code)
    }

    /// The summary of the stored state of the key, or `None` when the key
    /// has no state.
    async fn summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<ShareStateSummary> {
        let handle = self.entry(group, topic_id, partition)?;
        let st = handle.lock().await;
        Some((
            st.state_epoch,
            st.leader_epoch,
            st.start_offset,
            st.delivery_complete_count,
        ))
    }

    /// The body of [`ShareCoordinator::delete`], under the read guard of
    /// `term`.
    async fn delete_in_term(
        &self,
        term: Term,
        image: &MetadataImage,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareStateError> {
        if partition < 0 {
            return Err(invalid_request(message::NEGATIVE_PARTITION_ID));
        }
        check_topic_partition(image, topic_id, partition)?;
        let Some(entry) = self.entry(group, topic_id, partition) else {
            return Ok(());
        };
        let _st = entry.lock().await;
        self.tombstone(term, group, topic_id, partition).await
    }

    /// Appends the tombstone of a key in `term` and drops its in-memory
    /// entry. The caller holds the key lock.
    pub(super) async fn tombstone(
        &self,
        term: Term,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareStateError> {
        let key = state_key(KEY_SHARE_SNAPSHOT, group, topic_id, partition);
        self.persist_record(term, key, None).await.map_err(|e| {
            warn!(error = %e, "share delete persist failed");
            e.share_error()
        })?;
        self.state.remove(&(group.to_string(), topic_id, partition));
        Ok(())
    }
}

/// The key of a record of `record_type` for `(group, topic_id, partition)`.
fn state_key(record_type: i16, group: &str, topic_id: uuid::Uuid, partition: i32) -> ShareStateKey {
    ShareStateKey {
        record_type,
        group_id: group.to_string(),
        topic_id,
        partition,
    }
}

/// A refusal with `INVALID_REQUEST` and Kafka's validation `message`.
fn invalid_request(message: &'static str) -> ShareStateError {
    ShareStateError::Refused {
        code: codes::INVALID_REQUEST,
        message,
    }
}

/// Refuses a topic id that `image` does not know, or a partition that the
/// topic does not have, with `UNKNOWN_TOPIC_OR_PARTITION`.
fn check_topic_partition(
    image: &MetadataImage,
    topic_id: uuid::Uuid,
    partition: i32,
) -> Result<(), ShareStateError> {
    let known = image
        .topic_by_id(&topic_id)
        .is_some_and(|topic| image.partition(&topic.name, partition).is_some());
    if known {
        Ok(())
    } else {
        Err(ShareStateError::Refused {
            code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            message: message::UNKNOWN_TOPIC_OR_PARTITION,
        })
    }
}

/// The delivery complete count of a write, as Kafka's
/// `generateShareStateRecord` picks it: the greater count when the request
/// start offset equals the stored one, the stored count when the request
/// start offset is smaller, and the request count when it is greater.
fn delivery_complete_count(
    stored: &SharePartitionState,
    request_start_offset: Offset,
    request_count: i32,
) -> i32 {
    match request_start_offset.cmp(&stored.start_offset) {
        std::cmp::Ordering::Equal => request_count.max(stored.delivery_complete_count),
        std::cmp::Ordering::Less => stored.delivery_complete_count,
        std::cmp::Ordering::Greater => request_count,
    }
}
