//! The partition's leadership identity: the replication target that fences a
//! follower write, and the installation path that publishes a new leader and
//! epoch from the metadata image. That publication is a locking protocol of its
//! own, shared with produce admission, so it lives in its own module.

use {
    crate::{
        codes::{FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH},
        error::BrokerError,
        partition::Partition,
        producer_state::ProducerState,
    },
    krabka_log::Log,
    std::sync::{Arc, atomic::Ordering},
};

#[cfg(test)]
mod promotion_producer_state;

/// `Partition.makeLeader`'s `leaderLog.assignEpochStartOffset(leaderEpoch,
/// leaderLog.logEndOffset)`.
fn assign_leader_epoch_start(log: &mut Log, epoch: i32) -> Result<(), BrokerError> {
    let log_end = log.log_end_offset();
    Ok(log.assign_epoch_start_offset(krabka_ids::LeaderEpoch(epoch), log_end)?)
}

/// Immutable topic identity plus the leader generation allowed to mutate a
/// follower log. A read guard over this value linearizes replication writes
/// with [`Partition::install_leader_change`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicationTarget {
    pub(crate) topic_id: Option<uuid::Uuid>,
    pub(crate) leader_node_id: krabka_raft::NodeId,
    pub(crate) leader_epoch: krabka_metadata::LeaderEpoch,
}

pub(crate) fn initial_replication_target(
    topic_id: Option<uuid::Uuid>,
) -> Arc<tokio::sync::RwLock<ReplicationTarget>> {
    Arc::new(tokio::sync::RwLock::new(ReplicationTarget {
        topic_id,
        leader_node_id: krabka_raft::NodeId(0),
        leader_epoch: krabka_metadata::LeaderEpoch(0),
    }))
}

impl Partition {
    /// Hold the partition's metadata-transition barrier through one Produce.
    /// A diskless nominal-leader promotion takes the matching write lock while
    /// it hydrates the canonical log and rebuilds producer state, so no append
    /// can race that publication boundary.
    pub(crate) async fn lock_produce_transition(
        &self,
    ) -> tokio::sync::OwnedRwLockReadGuard<ReplicationTarget> {
        self.replication_target.clone().read_owned().await
    }

    /// Lock this partition for a mutation from `expected`, rejecting a stale
    /// task before it can enqueue work on the single writer.
    pub(crate) async fn lock_replication_target(
        &self,
        expected: ReplicationTarget,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<ReplicationTarget>, BrokerError> {
        let current = self.replication_target.clone().read_owned().await;
        if *current != expected {
            return Err(BrokerError::Replication(format!(
                "stale replication target: expected {expected:?}, current {:?}",
                *current
            )));
        }
        Ok(current)
    }

    /// Apply a leader change observed in the metadata image. This updates
    /// the cached `current_leader` and `current_leader_epoch`. If the
    /// leader or epoch changed, it clears the per-follower stats, which are
    /// stale under the new leader's view. On an idempotent re-install with
    /// the same leader and epoch, it keeps the per-follower progress. The
    /// supervisor calls this on every reconcile, and an unconditional clear
    /// would reset follower LEOs each time. That would drop HW back
    /// to 0 and block acks=-1 producers until followers re-fetch.
    /// The method fires `hw_advance_notify` so waiting Produce gates can
    /// re-check.
    pub async fn install_leader_change(&self, new_leader: u64, new_epoch: i32) {
        self.install_replication_target(None, new_leader, new_epoch)
            .await;
    }

    /// Install the complete metadata identity used to fence follower writes.
    /// `topic_id` is optional so callers that only process leader changes can
    /// preserve an identity installed when the partition was materialized.
    pub(crate) async fn install_replication_target(
        &self,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
    ) {
        // Reconciliation also runs for metadata-only changes such as an ISR
        // update. Do not queue for the exclusive transition barrier when the
        // target itself is unchanged: an acks=all Produce holds a read guard
        // while it waits for that ISR update to advance the HW.
        if self
            .replication_target_is(topic_id, new_leader, new_epoch)
            .await
        {
            return;
        }
        // Wait for any accepted follower mutation to finish before making the
        // new local role visible. Conversely, a mutation that arrives after
        // this write lock observes the new tuple and is fenced.
        let target = self.replication_target.write().await;
        self.publish_replication_target(target, topic_id, new_leader, new_epoch)
            .await;
    }

    /// Whether the installed target already is `(topic_id, leader, epoch)`,
    /// where a `None` topic id matches any installed identity.
    async fn replication_target_is(
        &self,
        topic_id: Option<uuid::Uuid>,
        leader: u64,
        epoch: i32,
    ) -> bool {
        Self::target_matches(
            &*self.replication_target.read().await,
            topic_id,
            leader,
            epoch,
        )
    }

    fn target_matches(
        target: &ReplicationTarget,
        topic_id: Option<uuid::Uuid>,
        leader: u64,
        epoch: i32,
    ) -> bool {
        topic_id.is_none_or(|id| target.topic_id == Some(id))
            && target.leader_node_id == krabka_raft::NodeId(leader)
            && target.leader_epoch == krabka_metadata::LeaderEpoch(epoch)
    }

    /// Install this broker as the partition's leader at `epoch`, Kafka's
    /// `Partition.makeLeader`.
    ///
    /// Before the role is published, the leader epoch is recorded at the
    /// current log end (`UnifiedLog.assignEpochStartOffset` with
    /// `leaderLog.logEndOffset`). A leader that has not written in its new
    /// epoch yet can then still place a follower's `last_fetched_epoch`: an
    /// epoch the new leader never saw ends where the new epoch starts, and the
    /// follower is told to truncate there instead of being answered
    /// `OFFSET_OUT_OF_RANGE` until the first write.
    ///
    /// The supervisor calls this on every reconcile. `assign` is a no-op
    /// while `epoch` is the latest recorded epoch, so a repeat only records
    /// the epoch again where it is missing -- the first reconcile after a
    /// restart, whose materialization already installed this target. That is
    /// where Kafka's fresh `Partition` object sees a new leader epoch too.
    ///
    /// A changed target also replaces this partition's entries in
    /// `producer_state` with the producer state of the log, before the role is
    /// published. Kafka keeps one producer state per log, and
    /// `UnifiedLog.appendAsFollower` updates it for each batch that a follower
    /// replicates. A new leader's `ProducerStateEntry.findDuplicateBatch` thus
    /// sees the last five batches of each producer up to the log end, and
    /// answers a retry of a replicated batch as a duplicate. The produce path
    /// reads only `producer_state`, and a follower does not add its replicated
    /// data batches there. The log does keep them, as Kafka's producer state
    /// does, so the promotion copies them from the log. Without the copy, a new
    /// leader would append a retry a second time, and a broker that leads again
    /// would refuse the next sequence as out of order against its previous
    /// term.
    ///
    /// # Errors
    /// Returns the checkpoint's error when the epoch cannot be recorded. A
    /// changed target is then not published, as a failed diskless promotion
    /// is not.
    pub(crate) async fn install_local_leadership(
        &self,
        producer_state: &ProducerState,
        topic_id: Option<uuid::Uuid>,
        local_node: u64,
        epoch: i32,
    ) -> Result<(), BrokerError> {
        {
            // The read guard is held from the target check through the
            // checkpoint write: a concurrent promotion to a newer epoch needs
            // the write guard, so it cannot land in between and have its entry
            // removed by this call's `assign`. A read guard does not wait for
            // in-flight Produce requests, which hold read guards too.
            let target = self.replication_target.read().await;
            if Self::target_matches(&target, topic_id, local_node, epoch) {
                let mut log = self
                    .log
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                return assign_leader_epoch_start(&mut log, epoch);
            }
        }
        let target = self.replication_target.write().await;
        if target.leader_epoch.0 > epoch {
            // A reconcile that raced a newer promotion: the newer epoch is
            // installed and recorded, and recording this one would rewind it.
            return Ok(());
        }
        // The write guard keeps every follower append and every produce out
        // of the log, so the producer state read here is the state at the log
        // end where the new epoch starts.
        let prepared = {
            let mut log = self
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assign_leader_epoch_start(&mut log, epoch).map(|()| log.recovered_producers())
        };
        producer_state
            .rebuild_from_snapshot(&self.topic, self.index, prepared?)
            .await;
        self.publish_replication_target(target, topic_id, local_node, epoch)
            .await;
        Ok(())
    }

    /// Serialize a local promotion with all follower mutations, prepare the
    /// canonical log, and only then publish the new leader role.
    ///
    /// Produce admission also checks `current_leader`, so it cannot observe
    /// the metadata promotion until `prepare` and producer-state recovery have
    /// completed successfully.
    pub(crate) async fn install_replication_target_after_log_prepare<T, F>(
        &self,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
        prepare: impl FnOnce(&mut Log) -> Result<T, BrokerError>,
        after_prepare: impl FnOnce(T) -> F,
    ) -> Result<(), BrokerError>
    where
        F: std::future::Future<Output = ()>,
    {
        let target = self.replication_target.write().await;
        let prepared = {
            let mut log = self
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prepare(&mut log)?
        };
        after_prepare(prepared).await;
        self.publish_replication_target(target, topic_id, new_leader, new_epoch)
            .await;
        Ok(())
    }

    async fn publish_replication_target(
        &self,
        mut target: tokio::sync::RwLockWriteGuard<'_, ReplicationTarget>,
        topic_id: Option<uuid::Uuid>,
        new_leader: u64,
        new_epoch: i32,
    ) {
        let prev_leader = self.current_leader.swap(new_leader, Ordering::AcqRel);
        let prev_epoch = self.current_leader_epoch.swap(new_epoch, Ordering::AcqRel);
        if let Some(topic_id) = topic_id {
            target.topic_id = Some(topic_id);
        }
        target.leader_node_id = krabka_raft::NodeId(new_leader);
        target.leader_epoch = krabka_metadata::LeaderEpoch(new_epoch);
        drop(target);
        let leader_changed = prev_leader != new_leader || prev_epoch != new_epoch;
        let mut st = self.replica_state.lock().await;
        if leader_changed {
            // Diagnostic: every broker hosting this partition logs the
            // leader/epoch transition it observes in committed metadata. Logged
            // on ALL replicas, so the full leadership sequence survives even
            // when the controller-leader pod that drove the change is killed —
            // used to trace failover leadership churn / flip-flop.
            tracing::info!(
                topic = %self.topic,
                partition = self.index.get(),
                prev_leader,
                new_leader,
                prev_epoch,
                new_epoch,
                "partition leadership changed (observed in committed metadata)"
            );
            st.reset_for_leader(krabka_raft::NodeId(new_leader));
        }
        st.current_leader_epoch = krabka_ids::LeaderEpoch(new_epoch);
        drop(st);
        self.hw_advance_notify.notify_waiters();
    }

    /// KIP-320's leader-epoch comparison, Kafka's
    /// `Partition.checkCurrentLeaderEpoch`. An asserted epoch below this
    /// partition's live epoch belongs to a leader generation that has already
    /// been superseded and takes `FENCED_LEADER_EPOCH`; one above it names a
    /// generation this broker has not observed yet and takes
    /// `UNKNOWN_LEADER_EPOCH`.
    ///
    /// Returns the error code together with the live epoch, which a response
    /// shape that reports the current leader back to the client needs.
    ///
    /// Which request epochs count as *asserted* is not spelled the same way in
    /// every API, so each caller decides that before it gets here. See
    /// [`Self::fetch_leader_epoch_fence`] and
    /// [`Self::list_offsets_leader_epoch_fence`].
    fn check_leader_epoch(&self, request_epoch: i32) -> Option<(i16, i32)> {
        let current = self.current_leader_epoch.load(Ordering::Acquire);
        if request_epoch == current {
            return None;
        }
        let code = if request_epoch < current {
            FENCED_LEADER_EPOCH
        } else {
            UNKNOWN_LEADER_EPOCH
        };
        Some((code, current))
    }

    /// [`Self::check_leader_epoch`] as Fetch reaches it.
    /// `FetchRequest.optionalEpoch` maps *any* negative epoch to
    /// `Optional.empty`, so a Fetch that carries one asserts nothing and
    /// passes unfenced.
    pub(crate) fn fetch_leader_epoch_fence(&self, request_epoch: i32) -> Option<(i16, i32)> {
        if request_epoch < 0 {
            return None;
        }
        self.check_leader_epoch(request_epoch)
    }

    /// [`Self::check_leader_epoch`] as `ListOffsets` reaches it.
    /// `RequestUtils.getLeaderEpoch` maps only `NO_PARTITION_LEADER_EPOCH`
    /// (`-1`) to `Optional.empty`, so every *other* negative epoch is an
    /// assertion and is fenced. The two APIs really do differ: on
    /// apache/kafka:4.3.1 with a live epoch of 0, `ListOffsets` v4 answers
    /// `current_leader_epoch = -2` with `FENCED_LEADER_EPOCH` while Fetch v11
    /// serves that same request its records.
    pub(crate) fn list_offsets_leader_epoch_fence(&self, request_epoch: i32) -> Option<(i16, i32)> {
        let current = self.current_leader_epoch.load(Ordering::Acquire);
        match krabka_verified::list_offsets_epoch_decision(request_epoch, current) {
            krabka_verified::ListOffsetsEpochDecision::Proceed => None,
            krabka_verified::ListOffsetsEpochDecision::Fenced => {
                Some((FENCED_LEADER_EPOCH, current))
            }
            krabka_verified::ListOffsetsEpochDecision::RejectMalformed
            | krabka_verified::ListOffsetsEpochDecision::Unknown => {
                Some((UNKNOWN_LEADER_EPOCH, current))
            }
        }
    }

    /// Test-only: directly set the partition's `current_leader_epoch`
    /// and do not use the supervisor's metadata-image-driven path.
    /// `tests/leader_epoch.rs` uses this to simulate split-brain with a
    /// forced epoch bump mid-Produce.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_leader_epoch(&self, epoch: i32) {
        self.current_leader_epoch
            .store(epoch, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::{FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH},
            partition::test_support::{
                EpochCheckpointFull, epoch_history as test_epoch_history, test_partition,
            },
        },
        assert2::assert,
        krabka_log::Offset,
        tokio::sync::Notify,
    };

    #[tokio::test]
    async fn replication_target_guard_fences_stale_generation_and_topic_identity() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let partition = Arc::new(partition);
        let topic_id = uuid::Uuid::new_v4();
        let current = ReplicationTarget {
            topic_id: Some(topic_id),
            leader_node_id: krabka_raft::NodeId(1),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
        };
        partition
            .install_replication_target(current.topic_id, 1, 7)
            .await;

        for stale in [
            ReplicationTarget {
                leader_node_id: krabka_raft::NodeId(2),
                ..current
            },
            ReplicationTarget {
                leader_epoch: krabka_metadata::LeaderEpoch(6),
                ..current
            },
            ReplicationTarget {
                topic_id: Some(uuid::Uuid::new_v4()),
                ..current
            },
        ] {
            assert!(partition.lock_replication_target(stale).await.is_err());
        }

        let guard = partition
            .lock_replication_target(current)
            .await
            .expect("current target");
        let update = tokio::spawn({
            let partition = partition.clone();
            async move { partition.install_replication_target(None, 2, 8).await }
        });
        tokio::task::yield_now().await;
        assert!(
            !update.is_finished(),
            "leader install waits for mutation guard"
        );
        drop(guard);
        update.await.expect("leader install");
        assert!(partition.lock_replication_target(current).await.is_err());
    }

    #[tokio::test]
    async fn idempotent_replication_target_install_does_not_wait_for_produce_guard() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let topic_id = uuid::Uuid::new_v4();
        partition
            .install_replication_target(Some(topic_id), 1, 7)
            .await;

        let produce_guard = partition.lock_produce_transition().await;
        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            partition.install_replication_target(Some(topic_id), 1, 7),
        )
        .await
        .expect("unchanged target should not wait for the Produce read guard");
        drop(produce_guard);
    }

    #[tokio::test]
    async fn install_leader_change_clears_followers() {
        // (new_leader, new_epoch, seeded_follower_leo):
        // first case changes only the leader, second only the epoch —
        // either change alone must clear follower state.
        let cases = [(2u64, 0i32, 11i64), (0, 9, 17)];
        for (leader, epoch, seeded_leo) in cases {
            let (p, _td) = test_partition(Arc::new(Notify::new()));
            p.install_isr(
                &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                krabka_audit::NodeId(1),
            )
            .await;
            {
                let mut st = p.replica_state.lock().await;
                st.per_follower
                    .get_mut(&krabka_audit::NodeId(2))
                    .expect("follower")
                    .leo = Offset(seeded_leo);
            }

            p.install_leader_change(leader, epoch).await;

            assert!(
                p.current_leader.load(Ordering::Acquire) == leader,
                "case ({leader}, {epoch})"
            );
            assert!(
                p.current_leader_epoch.load(Ordering::Acquire) == epoch,
                "case ({leader}, {epoch})"
            );
            let st = p.replica_state.lock().await;
            assert!(st.per_follower.is_empty(), "case ({leader}, {epoch})");
            assert!(
                st.current_leader_epoch == krabka_ids::LeaderEpoch(epoch),
                "case ({leader}, {epoch})"
            );
        }
    }

    fn append_at_epoch(partition: &Partition, epoch: i32) {
        let mut batch = krabka_protocol::records::RecordBatch {
            partition_leader_epoch: epoch,
            records: vec![krabka_protocol::records::Record::default()],
            ..Default::default()
        };
        partition
            .log
            .lock()
            .expect("log mutex")
            .append(&mut batch)
            .expect("append");
    }

    fn epoch_history(partition: &Partition) -> Vec<(i32, i64)> {
        test_epoch_history(&partition.log.lock().expect("log mutex"))
    }

    /// One promotion step: an optional write at an epoch, or a
    /// `Partition.makeLeader` at an epoch.
    #[derive(Clone, Copy)]
    enum Step {
        Write(i32),
        Lead(i32),
    }

    /// Kafka's `Partition.makeLeader` records the new leader epoch at the log
    /// end (`assignEpochStartOffset`) under `LeaderEpochFileCache.assign`'s
    /// rules: a repeat of the latest epoch is a no-op, and an epoch that
    /// was never written is replaced by the next one at the same offset.
    #[tokio::test]
    async fn local_leadership_records_the_new_epoch_at_the_log_end() {
        use Step::{Lead, Write};
        let cases = [
            ("empty log", &[Lead(3)][..], vec![(3, 0)]),
            (
                "promotion after two epochs of writes",
                &[Write(0), Write(0), Write(1), Write(1), Lead(3)][..],
                vec![(0, 0), (1, 2), (3, 4)],
            ),
            (
                "a repeat after writing in the epoch keeps its start",
                &[Write(0), Lead(3), Write(3), Write(3), Lead(3)][..],
                vec![(0, 0), (3, 1)],
            ),
            (
                "an epoch never written is replaced by the next one",
                &[Write(0), Write(0), Lead(3), Lead(4)][..],
                vec![(0, 0), (4, 2)],
            ),
            (
                "writes in the promoted epoch add no entry",
                &[Lead(2), Write(2), Write(2)][..],
                vec![(2, 0)],
            ),
        ];
        for (name, steps, expected) in cases {
            let (partition, _dir) = test_partition(Arc::new(Notify::new()));
            let mut led = None;
            for step in steps {
                match *step {
                    Write(epoch) => append_at_epoch(&partition, epoch),
                    Lead(epoch) => {
                        partition
                            .install_local_leadership(&ProducerState::new(), None, 1, epoch)
                            .await
                            .expect("promote");
                        led = Some(epoch);
                    }
                }
            }
            assert!(epoch_history(&partition) == expected, "{name}");
            assert!(
                partition.current_leader.load(Ordering::Acquire) == 1,
                "{name}"
            );
            assert!(
                Some(partition.current_leader_epoch.load(Ordering::Acquire)) == led,
                "{name}"
            );
        }
    }

    /// Materialization installs the target a restarted leader already
    /// holds, and its epoch history lost the unwritten entry on reopen. The
    /// first reconcile records the epoch again, as Kafka's fresh `Partition`
    /// does on its first `makeLeader`.
    #[tokio::test]
    async fn an_already_installed_leadership_still_records_its_epoch() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let topic_id = Some(uuid::Uuid::new_v4());
        append_at_epoch(&partition, 0);
        partition.install_replication_target(topic_id, 1, 6).await;
        assert!(epoch_history(&partition) == [(0, 0)]);

        partition
            .install_local_leadership(&ProducerState::new(), topic_id, 1, 6)
            .await
            .expect("record epoch");

        assert!(epoch_history(&partition) == [(0, 0), (6, 1)]);
    }

    #[tokio::test]
    async fn a_reconcile_that_raced_a_newer_promotion_does_not_rewind_its_epoch() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        let topic_id = Some(uuid::Uuid::new_v4());
        append_at_epoch(&partition, 0);
        partition
            .install_local_leadership(&ProducerState::new(), topic_id, 1, 7)
            .await
            .expect("promote to 7");

        partition
            .install_local_leadership(&ProducerState::new(), topic_id, 1, 6)
            .await
            .expect("a stale reconcile is a no-op");

        assert!(epoch_history(&partition) == [(0, 0), (7, 1)]);
        assert!(partition.replication_target_is(topic_id, 1, 7).await);
    }

    /// A promotion whose epoch cannot be recorded is not published, as a
    /// diskless promotion whose log preparation fails is not.
    #[tokio::test]
    async fn a_promotion_whose_epoch_cannot_be_recorded_is_not_published() {
        let (partition, _dir) = test_partition(Arc::new(Notify::new()));
        partition.install_replication_target(None, 2, 4).await;
        partition
            .log
            .lock()
            .expect("log mutex")
            .test_set_io(Arc::new(EpochCheckpointFull));

        let result = partition
            .install_local_leadership(&ProducerState::new(), None, 1, 5)
            .await;

        assert!(let Err(BrokerError::Log(_)) = result);
        assert!(epoch_history(&partition).is_empty());
        assert!(
            *partition.replication_target.read().await
                == ReplicationTarget {
                    topic_id: None,
                    leader_node_id: krabka_raft::NodeId(2),
                    leader_epoch: krabka_metadata::LeaderEpoch(4),
                }
        );
        assert!(partition.current_leader.load(Ordering::Acquire) == 2);
        assert!(partition.current_leader_epoch.load(Ordering::Acquire) == 4);
    }

    #[tokio::test]
    async fn each_api_spells_the_no_epoch_sentinel_the_way_kafka_spells_it() {
        // Live epoch 3. Kafka fences a `ListOffsets` row for every request
        // epoch but `-1` and the live one, while Fetch also lets every other
        // negative epoch through. Verified on apache/kafka:4.3.1: with a live
        // epoch of 0, `ListOffsets` v4 answers `current_leader_epoch = -2`
        // with FENCED_LEADER_EPOCH (74) and Fetch v11 serves it records.
        const LIVE: i32 = 3;
        let (p, _td) = test_partition(Arc::new(Notify::new()));
        p.test_set_leader_epoch(LIVE);

        let fenced = Some((FENCED_LEADER_EPOCH, LIVE));
        let unknown = Some((UNKNOWN_LEADER_EPOCH, LIVE));
        // (request epoch, Fetch verdict, ListOffsets verdict)
        let cases = [
            (LIVE - 1, fenced, fenced),
            (LIVE, None, None),
            (LIVE + 1, unknown, unknown),
            (-1, None, None),
            (-2, None, fenced),
            (i32::MIN, None, fenced),
        ];
        for (request_epoch, fetch, list_offsets) in cases {
            assert!(
                p.fetch_leader_epoch_fence(request_epoch) == fetch,
                "{request_epoch}"
            );
            assert!(
                p.list_offsets_leader_epoch_fence(request_epoch) == list_offsets,
                "{request_epoch}"
            );
        }
    }

    #[tokio::test]
    async fn test_set_leader_epoch_updates_cached_epoch() {
        let (p, _td) = test_partition(Arc::new(Notify::new()));

        p.test_set_leader_epoch(6);

        assert!(p.current_leader_epoch.load(Ordering::Acquire) == 6);
    }
}
