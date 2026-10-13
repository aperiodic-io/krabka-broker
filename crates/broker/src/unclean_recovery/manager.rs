//! The controller-side Unclean Recovery Manager task.
//!
//! The manager owns the dispatch loop, the per-partition in-flight set, and
//! the decision to commit a new leader. It is the only part of the module that
//! talks to the metadata source, so the selection helpers and the replica
//! query stay free of controller state.

use {
    super::{
        Election, RecoveryJob, RecoveryOutcome, RecoveryPolicy, ReplicaLogInfo,
        UncleanRecoveryHandle, has_newer_leader,
        query::{ReplicaQuery, gather_responses, query_replica},
        select_leader,
    },
    crate::{
        config_keys::witness_node_ids,
        elr::{ElrPublisher, TopicElr},
        heartbeat::controller_state::ControllerLivenessState,
        metadata_epoch::next_partition_change,
        metadata_source::MetadataSource,
        metrics::BrokerMetrics,
        network::client::InterBrokerClient,
    },
    futures_util::FutureExt as _,
    krabka_metadata::{
        LeaderRecoveryState, MetadataImage, MetadataRecord, PartitionRecord,
        PartitionRecoveryRecord,
    },
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    krabka_raft::{NodeId, RaftError},
    krabka_units::convert::TimeExt as _,
    krabka_verified::unclean_recovery_commit_admission,
    std::{collections::HashSet, sync::Arc},
    tokio::sync::{Mutex, mpsc},
    tracing::warn,
};

#[cfg(test)]
mod tests;

/// The controller-side Unclean Recovery Manager.
///
/// It receives [`RecoveryJob`] values, dedups the in-flight work for each
/// partition, queries surviving replicas for their log state, and elects one
/// of them through an image-guarded submission: an eligible leader replica if one
/// answered, and otherwise the most complete log that did.
pub(crate) struct UncleanRecoveryManager {
    controller: Arc<dyn MetadataSource>,
    liveness: Arc<ControllerLivenessState>,
    node_id: NodeId,
    inter_broker_client: Arc<InterBrokerClient>,
    listener_protocol: krabka_security::ListenerProtocol,
    metrics: BrokerMetrics,
    policy: RecoveryPolicy,
    in_flight: Arc<Mutex<HashSet<(String, i32)>>>,
}

impl UncleanRecoveryManager {
    /// Spawns the URM dispatch loop and returns a cloneable handle that
    /// enqueues jobs. The loop exits when `shutdown` fires or when the last
    /// handle drops.
    pub(crate) fn spawn(
        controller: Arc<dyn MetadataSource>,
        liveness: Arc<ControllerLivenessState>,
        node_id: NodeId,
        inter_broker_client: Arc<InterBrokerClient>,
        metrics: BrokerMetrics,
        policy: RecoveryPolicy,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> UncleanRecoveryHandle {
        let (tx, mut rx) = mpsc::channel::<RecoveryJob>(policy.queue_capacity);
        let mgr = Arc::new(Self {
            controller,
            liveness,
            node_id,
            inter_broker_client,
            listener_protocol: policy.listener_protocol,
            metrics,
            policy,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        });
        tokio::spawn(async move {
            loop {
                let job = tokio::select! {
                    () = shutdown.cancelled() => return,
                    j = rx.recv() => match j { Some(j) => j, None => return },
                };
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    mgr.recover_one(job).await;
                });
            }
        });
        UncleanRecoveryHandle { tx }
    }

    /// Per-job entry point. It dedups against the in-flight recoveries for
    /// the same partition, runs the recovery, then releases the in-flight slot
    /// and replies if the caller supplied a reply channel.
    async fn recover_one(self: Arc<Self>, job: RecoveryJob) {
        let key = (job.topic.clone(), job.partition);
        {
            let mut set = self.in_flight.lock().await;
            if !set.insert(key.clone()) {
                if let Some(r) = job.reply {
                    let _ = r.send(RecoveryOutcome::InProgress);
                }
                return;
            }
        }
        let outcome = self.run_recovery(&job).await;
        self.in_flight.lock().await.remove(&key);
        if let Some(r) = job.reply {
            let _ = r.send(outcome);
        }
    }

    /// Core recovery routine.
    ///
    /// It confirms that this node is the controller leader and that the
    /// partition still needs recovery, then queries the surviving replicas.
    /// If a winner emerges and no newer leader has appeared, it submits the
    /// leader change.
    async fn run_recovery(&self, job: &RecoveryJob) -> RecoveryOutcome {
        let is_leader = self
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == self.node_id);
        if !is_leader {
            return RecoveryOutcome::NotNeeded;
        }

        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        // If the current leader is alive, there's nothing to recover.
        if self.liveness.is_alive(pr.leader.0).await {
            return RecoveryOutcome::NotNeeded;
        }
        // KIP-966: the replicas that are known to hold every committed record.
        // Read here, with the rest of the partition state, so the selection and
        // the break-glass rule below both see one image.
        let eligible = TopicElr::of_topic(&image, &job.topic)
            .partition(job.partition)
            .eligible_leader_replicas;
        // KIP-966's other lossless rule, and the first disjunct of Kafka's
        // `isValidNewLeader`. The failover scan that enqueued this recovery
        // left the ISR alone, so a member liveness called dead is still named
        // here and may yet answer the poll below; electing it loses nothing.
        let in_sync = pr.isr.clone();
        // KFC-9: the recovery is needed from here on, so this is where the
        // fail-closed rule bites. What it guards is a data-losing election, and
        // a partition with an eligible leader replica may yet have a lossless
        // one, so only a partition with no ELR at all is refused here. The
        // check runs again below, once the poll has said which of the two
        // elections this recovery reached. Refusing the no-ELR case up front is
        // what keeps a recovery the broker will not commit from spending a
        // round trip to every surviving replica, which is the common case.
        if eligible.is_empty() && self.policy.background.refuses(job) {
            return self.refuse_background_now(job);
        }
        let known_epoch = pr.leader_epoch;
        let selected_partition_epoch = pr.partition_epoch;
        let selected_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();
        let topic_id = image
            .topic(&job.topic)
            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));

        // Gather the surviving (alive) replicas to query.
        let mut alive: Vec<NodeId> = Vec::new();
        for &r in &pr.replicas {
            if self.liveness.is_alive(r.0).await {
                alive.push(r);
            }
        }
        if alive.is_empty() {
            return RecoveryOutcome::NoEligibleReplica;
        }

        let mut futs = Vec::with_capacity(alive.len());
        for r in alive {
            let Some(reg) = image.broker(r) else { continue };
            let (host, port) = (reg.host.clone(), reg.port);
            let client = self.inter_broker_client.clone();
            let proto = self.listener_protocol;
            let partition = job.partition;
            let server_name = self.policy.inter_broker_server_name.clone();
            let my_id = i32::try_from(self.node_id.0).unwrap_or(-1);
            futs.push(
                async move {
                    query_replica(&client, ReplicaQuery {
                        proto,
                        host,
                        port,
                        my_broker_id: my_id,
                        topic_id,
                        partition,
                        replica: r,
                        server_name,
                    })
                    .await
                }
                .boxed(),
            );
        }

        let deadline = self.policy.deadline(job.strategy);
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, deadline.to_std()).await;

        if has_newer_leader(&collected, known_epoch.0) {
            return RecoveryOutcome::Stale;
        }
        let Some(election) = Self::elect_from(&image, &in_sync, &eligible, &collected) else {
            return RecoveryOutcome::NoEligibleReplica;
        };
        // KFC-9 again: the ELR did not save this one, so the rule applies.
        if self.policy.background.refuses_election(job, election) {
            return self.refuse_background_now(job);
        }

        // Re-read the image and re-check before committing: the leader may
        // have come back, or the partition may have been deleted, while we
        // were polling replicas.
        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        let current_leader_alive = self.liveness.is_alive(pr.leader.0).await;

        self.commit_elected_leader(
            job,
            &image,
            pr,
            election,
            (
                selected_partition_epoch,
                &selected_replicas,
                current_leader_alive,
            ),
        )
        .await
    }

    /// KFC-9: carry out a refusal the background rule has already decided on,
    /// at either of the two points that can decide it.
    ///
    /// The refusal is audited here, because the partition then keeps no leader
    /// and the audit log is the only place that says why.
    fn refuse_background_now(&self, job: &RecoveryJob) -> RecoveryOutcome {
        self.policy.background.audit_refusal(job, self.node_id);
        warn!(
            topic = %job.topic,
            partition = job.partition,
            "unclean recovery refused: break_glass.background_unclean_recovery is require \
             and no proposal approved it; the partition stays offline"
        );
        RecoveryOutcome::BreakGlassRequired
    }

    /// The leader this recovery elects out of the poll it collected, and the
    /// rule that chose it. `None` when nothing that may lead answered.
    ///
    /// Every rule is pure and lives in [`select_leader`]. `in_sync` is the ISR
    /// the partition record still names, which this recovery has not rewritten
    /// and which a returning member can therefore still be in. What the image
    /// adds is the `broker.witness` set: a witness replicates the partition
    /// and can sit in both the ISR and the eligible-leader-replica set, and it
    /// must still never lead, because it serves no client. The poll above
    /// queries it anyway -- its `current_leader_epoch` is what tells this
    /// recovery that a newer leader has already superseded it -- so the
    /// exclusion belongs here, between the staleness check and the election.
    fn elect_from(
        image: &MetadataImage,
        in_sync: &[NodeId],
        eligible: &[i32],
        responses: &[ReplicaLogInfo],
    ) -> Option<Election> {
        select_leader(responses, in_sync, eligible, &witness_node_ids(image))
    }

    /// Builds and submits the `PartitionRecord` that elects `election.leader`
    /// as the new leader. The record bumps the epoch and shrinks the ISR to
    /// the elected replica alone.
    ///
    /// `election.basis` decides everything the commit says about the election
    /// rather than anything it writes: only a fallback to the most complete
    /// surviving log counts on the unclean-leader-election meter or reads as a
    /// break-glass bypass, because only that one can drop a committed record.
    /// The `PartitionRecord` is the same either way. Admission keeps the
    /// planning image across audit awaits; a changed image, controller, or
    /// pending Raft tail rejects this selection without forwarding it.
    async fn commit_elected_leader(
        &self,
        job: &RecoveryJob,
        image: &Arc<MetadataImage>,
        pr: &PartitionRecord,
        election: Election,
        selected: (i32, &[u64], bool),
    ) -> RecoveryOutcome {
        let (selected_partition_epoch, selected_replicas, current_leader_alive) = selected;
        let winner = election.leader;
        let current_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();
        if !unclean_recovery_commit_admission(
            selected_partition_epoch,
            pr.partition_epoch,
            selected_replicas,
            &current_replicas,
            winner.0,
            current_leader_alive,
        ) {
            return if current_leader_alive {
                RecoveryOutcome::NotNeeded
            } else {
                RecoveryOutcome::Stale
            };
        }
        let Some((partition_epoch, leader_epoch)) =
            next_partition_change(pr.partition_epoch, pr.leader_epoch, true)
        else {
            warn!(
                topic = %job.topic,
                partition = job.partition,
                "unclean recovery refused because a metadata epoch is exhausted"
            );
            return RecoveryOutcome::Stale;
        };
        let new_pr = PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: winner,
            replicas: pr.replicas.clone(),
            isr: vec![winner],
            leader_epoch,
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch,
        };
        if let Err(error) = self
            .policy
            .background
            .require_audit(job, self.node_id, election)
            .await
        {
            warn!(%error, "unclean recovery refused by fail-closed audit policy");
            return RecoveryOutcome::AuditUnavailable;
        }
        // KIP-966: `ElrPublisher` recomputes the published state from the
        // record. An ELR member stays in the set the rules carry forward; a
        // most-complete-log winner need not hold every committed record, so
        // nothing that was eligible before it still is and the publisher
        // clears the state outright.
        let mut changes = vec![MetadataRecord::V1Partition(new_pr)];
        if election.basis.loses_data() {
            changes.push(MetadataRecord::V1PartitionRecovery(
                PartitionRecoveryRecord {
                    topic: job.topic.clone(),
                    partition: job.partition,
                    state: LeaderRecoveryState::Recovering,
                },
            ));
        }
        ElrPublisher::new(image).extend(&mut changes);
        if let Err(error) = self
            .controller
            .submit_change_if_image(changes, Arc::clone(image))
            .await
        {
            warn!(%error, "unclean recovery guarded submission failed");
            return match error {
                RaftError::StaleMetadataImage
                | RaftError::UncommittedTail
                | RaftError::NotLeader { .. }
                | RaftError::LeaderUnknown => RecoveryOutcome::Stale,
                _ => RecoveryOutcome::NoEligibleReplica,
            };
        }
        warn!(
            topic = %job.topic,
            partition = job.partition,
            leader = winner.0,
            basis = election.basis.describe(),
            "unclean recovery elected a leader"
        );
        if election.basis.loses_data() {
            self.metrics.record_unclean_leader_election();
        }
        // KFC-9: the election is durable now, so a bypass is a fact rather
        // than an intention. A job that carries a proposal took the operator
        // path, and its handler already audited the approval it spent.
        self.policy
            .background
            .audit_election(job, self.node_id, election, &self.metrics);
        RecoveryOutcome::Elected(winner)
    }
}
