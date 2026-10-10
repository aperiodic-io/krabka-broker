//! The controller failover scans. Each walks the metadata image once, asks
//! [`failover_one`] about every partition it touches, and turns the answers
//! into a [`FailoverPlan`]. [`compute_failover_changes`] reacts to a dead
//! broker; [`compute_offline_dir_failover_changes`] reacts to a live broker
//! that lost a log directory (KIP-112); [`compute_unclean_restart_changes`]
//! reacts to a broker that came back without proving it stopped cleanly.
//!
//! A partition none of them can elect is left as its record has it, and the
//! scan tells its [`ElrPublisher`] that it has no leader, which publishes the
//! last-known ELR that says so. [`compute_unfence_changes`] is the other half:
//! it elects again for such a partition when a broker unfences, which is what
//! Kafka's `handleBrokerUnfenced` does over `partitionsWithNoLeader`.

use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord};
use krabka_raft::NodeId;
use tracing::warn;

use super::policy::{
    FailoverDecision, FailoverPlan, elect_leaderless_one, failover_one, push_partition_change,
    unclean_restart_one,
};
use crate::{
    config_keys::{
        RecoveryStrategy, resolve_recovery_strategy, resolve_unclean_leader_election_enabled,
        witness_node_ids,
    },
    elr::{ElrPublisher, TopicElr, state::PartitionElr},
    heartbeat::controller_state::ControllerLivenessState,
};

#[cfg(test)]
mod dead_broker_tests;
#[cfg(test)]
mod offline_dir_tests;
#[cfg(test)]
mod unclean_restart_tests;
#[cfg(test)]
mod unfence_tests;

/// The published eligible and last-known replica sets a scan reads, parsed
/// once per topic rather than once per partition.
///
/// [`TopicElr::of_topic`] hits the topic's config map and parses the whole
/// value, which holds every partition of the topic that carries ELR state. A
/// scan walks partitions, not topics, so without this a thousand-partition
/// topic would parse that value a thousand times.
#[derive(Default)]
struct ScanElr(std::collections::HashMap<String, TopicElr>);

impl ScanElr {
    /// The ELR state `image` publishes for one partition.
    fn state(&mut self, image: &MetadataImage, topic: &str, partition: i32) -> PartitionElr {
        self.0
            .entry(topic.to_owned())
            .or_insert_with(|| TopicElr::of_topic(image, topic))
            .partition(partition)
    }

    /// Resolve topic policy and cached ELR before evaluating one departure.
    fn failover(
        &mut self,
        image: &MetadataImage,
        pr: &PartitionRecord,
        departed: NodeId,
        alive: &std::collections::HashSet<NodeId>,
        witnesses: &std::collections::HashSet<NodeId>,
    ) -> FailoverDecision {
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        let elr_state = self.state(image, &pr.topic, pr.partition);
        failover_one(
            pr,
            departed,
            alive,
            witnesses,
            &elr_state,
            strategy,
            unclean_enabled,
        )
    }
}

/// Snapshot liveness once and prepare per-topic ELR caching for a scan.
async fn scan_inputs(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
) -> (
    std::collections::HashSet<NodeId>,
    std::collections::HashSet<NodeId>,
    ScanElr,
) {
    let alive = liveness.alive_node_ids().await;
    let witnesses = witness_node_ids(image);
    (alive, witnesses, ScanElr::default())
}

/// Tell `publisher` that the scan leaves `pr` without a leader once `gone`
/// leaves its ISR, because no rung of the ladder could elect one.
///
/// Kafka writes `leader = -1` with that ISR for the same partition; see
/// [`ElrPublisher::leaderless`].
fn mark_leaderless(publisher: &mut ElrPublisher<'_>, pr: &PartitionRecord, gone: NodeId) {
    publisher.leaderless(pr, pr.isr.iter().copied().filter(|n| *n != gone).collect());
}

/// Queue offset-aware recovery and publish the departed replica's leaderless state.
fn record_recovery(
    recoveries: &mut Vec<(String, i32, RecoveryStrategy)>,
    publisher: &mut ElrPublisher<'_>,
    pr: &PartitionRecord,
    gone: NodeId,
    strategy: RecoveryStrategy,
) {
    recoveries.push((pr.topic.clone(), pr.partition, strategy));
    mark_leaderless(publisher, pr, gone);
}

/// Validate a scan's epoch change, preserving its exhaustion diagnostic.
fn checked_epochs(
    pr: &PartitionRecord,
    leader_changes: bool,
    exhausted_message: &str,
) -> Option<(i32, krabka_metadata::LeaderEpoch)> {
    crate::metadata_epoch::next_partition_change(
        pr.partition_epoch,
        pr.leader_epoch,
        leader_changes,
    )
    .or_else(|| {
        warn!(topic = %pr.topic, partition = pr.partition, "{exhausted_message}");
        None
    })
}

/// Check the partition epoch before publishing a clean ISR shrink.
fn push_checked_isr_shrink(
    changes: &mut Vec<MetadataRecord>,
    pr: &PartitionRecord,
    isr: Vec<NodeId>,
    exhausted_message: &str,
    applied: impl FnOnce(&[NodeId]),
) -> bool {
    let Some((partition_epoch, leader_epoch)) = checked_epochs(pr, false, exhausted_message) else {
        return false;
    };
    applied(&isr);
    push_partition_change(
        changes,
        pr,
        pr.leader,
        isr,
        partition_epoch,
        leader_epoch,
        false,
    );
    true
}

/// Compute the failover `MetadataRecord` changes for `dead` against
/// `image`. Pure: no I/O beyond `liveness.is_alive` lookups. This function is
/// separate so the failover policy, including the KIP-841 unclean toggle, is
/// unit-testable without spinning up a controller.
pub(crate) async fn compute_failover_changes(
    image: &MetadataImage,
    dead: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let FailoverPlan {
        mut changes,
        mut recoveries,
        mut unavailable,
    } = FailoverPlan::default();
    let (alive, witnesses, mut elr) = scan_inputs(image, liveness).await;
    // A partition with nothing to elect has no leader, which its ELR says.
    let mut publisher = ElrPublisher::new(image);
    // Single O(P) walk over every partition in the image.
    for pr in image.all_partitions() {
        if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
            continue;
        }
        match elr.failover(image, pr, dead, &alive, &witnesses) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                let Some((partition_epoch, new_leader_epoch)) = checked_epochs(
                    pr,
                    true,
                    "failover skipped because a metadata epoch is exhausted",
                ) else {
                    unavailable.push((pr.topic.clone(), pr.partition));
                    continue;
                };
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    // KIP-841: account this election so operators can alert on a
                    // non-zero rate of unclean failovers in their cluster.
                    metrics.record_unclean_leader_election();
                }
                // One source of truth for the bumped epoch: used by both the
                // log line and the emitted record, so the failover tests that
                // assert the incremented `leader_epoch` also pin the logged
                // value (no un-killable log-only arithmetic).
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    dead = dead.0,
                    old_leader = pr.leader.0,
                    new_leader = leader.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    new_leader_epoch = new_leader_epoch.0,
                    unclean,
                    "failover: re-electing partition leader (triggered by dead broker)"
                );
                push_partition_change(
                    &mut changes,
                    pr,
                    leader,
                    isr,
                    partition_epoch,
                    new_leader_epoch,
                    unclean,
                );
            }
            FailoverDecision::ShrinkIsr { isr } => {
                if !push_checked_isr_shrink(
                    &mut changes,
                    pr,
                    isr,
                    "ISR shrink skipped because the partition epoch is exhausted",
                    |_| {},
                ) {
                    unavailable.push((pr.topic.clone(), pr.partition));
                }
            }
            FailoverDecision::Recover(strategy) => {
                // KIP-966: defer to the offset-aware Unclean Recovery Manager —
                // it polls surviving replicas and elects the most complete log.
                record_recovery(&mut recoveries, &mut publisher, pr, dead, strategy);
            }
            FailoverDecision::Unavailable => {
                unavailable.push((pr.topic.clone(), pr.partition));
                mark_leaderless(&mut publisher, pr, dead);
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966: a failover that shrinks the ISR below min ISR leaves the
    // replicas it dropped eligible to lead, and one that reaches min ISR
    // again clears them.
    publisher.extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        unavailable,
    }
}

/// Compute failover changes for partitions whose replica on `broker` lives
/// on a now-offline log directory (`offline_uuids`). KIP-112: a broker stays
/// alive after a disk failure, so the dead-broker scan never fires. This scan
/// does, and the broker's `offline_log_dirs` heartbeat drives it.
///
/// For each affected partition:
/// - if `broker` is the leader, elect a new leader from the ISR minus
///   `broker`, drop `broker` from ISR, and bump epoch. The clean / KIP-966 /
///   KIP-841 policy is the same as [`compute_failover_changes`].
/// - if `broker` is a non-leader ISR member, drop it from ISR. No epoch bump,
///   unless the leader is itself down, in which case the same policy elects
///   its replacement, as Kafka's `handleDirectoriesOffline` does.
///
/// Pure and idempotent. After the change `broker` is neither leader nor in
/// ISR, so a repeat yields an empty plan.
pub(crate) async fn compute_offline_dir_failover_changes(
    image: &MetadataImage,
    broker: NodeId,
    offline_uuids: &std::collections::HashSet<uuid::Uuid>,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let mut changes: Vec<MetadataRecord> = Vec::new();
    let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();
    let (alive, witnesses, mut elr) = scan_inputs(image, liveness).await;
    let mut publisher = ElrPublisher::new(image);
    for pr in image.all_partitions() {
        let Some(slot) = pr.replicas.iter().position(|n| *n == broker) else {
            continue;
        };
        let on_offline = pr
            .directories
            .get(slot)
            .is_some_and(|d| offline_uuids.contains(d));
        if !on_offline {
            continue;
        }
        match elr.failover(image, pr, broker, &alive, &witnesses) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                let Some((partition_epoch, leader_epoch)) = checked_epochs(
                    pr,
                    true,
                    "offline-dir failover skipped because a metadata epoch is exhausted",
                ) else {
                    continue;
                };
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "offline-dir unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                push_partition_change(
                    &mut changes,
                    pr,
                    leader,
                    isr,
                    partition_epoch,
                    leader_epoch,
                    unclean,
                );
            }
            FailoverDecision::ShrinkIsr { isr } => {
                push_checked_isr_shrink(
                    &mut changes,
                    pr,
                    isr,
                    "offline-dir ISR shrink skipped because the partition epoch is exhausted",
                    |_| {},
                );
            }
            FailoverDecision::Recover(strategy) => {
                record_recovery(&mut recoveries, &mut publisher, pr, broker, strategy);
            }
            FailoverDecision::Unavailable => {
                warn!(
                    topic = %pr.topic, partition = pr.partition,
                    "offline dir on leader, no live ISR replica; partition unavailable"
                );
                mark_leaderless(&mut publisher, pr, broker);
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966: a failover that shrinks the ISR below min ISR leaves the
    // replicas it dropped eligible to lead, and one that reaches min ISR
    // again clears them.
    publisher.extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        // The offline-dir scan runs once per heartbeat that reports the dir,
        // so it warns above and reports nothing here.
        unavailable: Vec::new(),
    }
}

/// The failover changes a rejoining broker needs when it cannot prove its
/// last stop was clean, in the order they apply.
///
/// This is Apache Kafka's `handleBrokerUncleanShutdown`, whose two
/// `generateLeaderAndIsrUpdates` calls -- read out of
/// `kafka-metadata-4.3.1.jar` -- are the two halves of one answer to one
/// event: eligibility, and the ISR the next eligibility is derived from.
/// [`withdraw_elr_membership`](crate::elr::withdraw_elr_membership) is the
/// `partitionsWithBrokerInElr` call and comes first, so a replay that stops
/// mid-batch has already stopped trusting the returning log rather than not
/// yet started. [`unclean_restart_one`] is the `partitionsWithBrokerInIsr`
/// call. Then the publisher runs over the whole batch with the broker named
/// as an unclean-shutdown replica, which is what stops the ISR removals this
/// batch just made from deriving the broker straight back into the eligible
/// sets the first half withdrew it from.
///
/// The caller decides whether any of it happens. Kafka enters this branch on
/// `isElrFeatureEnabled() && !isCleanShutdown`, and krabka's
/// `clean_shutdown_proven` is that boolean, so a broker that offers back the
/// epoch the cluster still holds for it never reaches here and keeps both its
/// ISR seat and its ELR membership.
///
/// The plan is otherwise shaped exactly like [`compute_failover_changes`]'s,
/// and the caller drives `recoveries` and `unavailable` the same way, because
/// a partition the returning broker was leading is a partition with a dead
/// leader whichever event the controller noticed first.
pub(crate) async fn compute_unclean_restart_changes(
    image: &MetadataImage,
    returning: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> FailoverPlan {
    let FailoverPlan {
        mut changes,
        mut recoveries,
        mut unavailable,
    } = FailoverPlan {
        changes: crate::elr::withdraw_elr_membership(image, returning),
        ..Default::default()
    };
    let (alive, witnesses, mut elr) = scan_inputs(image, liveness).await;
    // The ISR removals below are the candidate set the next eligibility is
    // derived from, so the broker they remove has to be excluded from that
    // derivation too.
    let mut publisher = ElrPublisher::after_unclean_shutdown(image, returning);
    for pr in image.all_partitions() {
        if pr.leader != returning && !pr.isr.contains(&returning) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        let elr_state = elr.state(image, &pr.topic, pr.partition);
        match unclean_restart_one(
            pr,
            returning,
            &alive,
            &witnesses,
            &elr_state,
            strategy,
            unclean_enabled,
        ) {
            FailoverDecision::Elect {
                leader,
                isr,
                unclean,
            } => {
                let Some((partition_epoch, new_leader_epoch)) = checked_epochs(
                    pr,
                    true,
                    "unclean-restart failover skipped because a metadata epoch is exhausted",
                ) else {
                    unavailable.push((pr.topic.clone(), pr.partition));
                    continue;
                };
                if unclean {
                    warn!(
                        topic = %pr.topic, partition = pr.partition, leader = leader.0,
                        "unclean leader election: returning broker led an empty-ISR partition (possible data loss)"
                    );
                    metrics.record_unclean_leader_election();
                }
                tracing::info!(
                    topic = %pr.topic,
                    partition = pr.partition,
                    returning = returning.0,
                    old_leader = pr.leader.0,
                    new_leader = leader.0,
                    old_isr = ?pr.isr,
                    new_isr = ?isr,
                    new_leader_epoch = new_leader_epoch.0,
                    unclean,
                    "unclean restart: re-electing partition leader"
                );
                push_partition_change(
                    &mut changes,
                    pr,
                    leader,
                    isr,
                    partition_epoch,
                    new_leader_epoch,
                    unclean,
                );
            }
            FailoverDecision::ShrinkIsr { isr } => {
                if !push_checked_isr_shrink(
                    &mut changes,
                    pr,
                    isr,
                    "unclean-restart ISR shrink skipped because the partition epoch is exhausted",
                    |isr| {
                        tracing::info!(topic = %pr.topic, partition = pr.partition,
                            returning = returning.0, old_isr = ?pr.isr, new_isr = ?isr,
                            "unclean restart: dropping returning broker from ISR");
                    },
                ) {
                    unavailable.push((pr.topic.clone(), pr.partition));
                }
            }
            FailoverDecision::Recover(strategy) => {
                record_recovery(&mut recoveries, &mut publisher, pr, returning, strategy);
            }
            FailoverDecision::Unavailable => {
                unavailable.push((pr.topic.clone(), pr.partition));
                mark_leaderless(&mut publisher, pr, returning);
            }
            FailoverDecision::NoChange => {}
        }
    }
    // KIP-966, and the reason this function exists: see the publisher above.
    publisher.extend(&mut changes);
    FailoverPlan {
        changes,
        recoveries,
        unavailable,
    }
}

/// The elections Kafka runs when `unfenced` unfences: every partition that has
/// no leader takes the election ladder again with `unfenced` as an acceptable
/// leader.
///
/// This is `ReplicationControlManager.handleBrokerUnfenced`, whose
/// `generateLeaderAndIsrUpdates` call walks
/// `brokersToIsrs.partitionsWithNoLeader()` and lets `brokerToAdd` lead
/// although the cluster still shows it fenced. The partitions are the ones
/// the ELR marks leaderless (see [`PartitionElr::is_leaderless`]), and the
/// ladder is [`elect_leaderless_one`]'s: the last leader returns as a clean
/// ELR election when the fence put it there, and as the last known leader --
/// unclean, `RECOVERING` -- when an unclean restart kept it out. A partition
/// nothing can elect is left as it is, and the scans that marked it and the
/// heartbeat that brings the next broker back answer for it.
///
/// The caller submits the records after the registration change that unfences
/// the broker, in Kafka's order.
pub(crate) async fn compute_unfence_changes(
    image: &MetadataImage,
    unfenced: NodeId,
    liveness: &ControllerLivenessState,
    metrics: &crate::metrics::BrokerMetrics,
) -> Vec<MetadataRecord> {
    let mut alive = liveness.alive_node_ids().await;
    // The registry still holds the unfencing broker fenced.
    alive.insert(unfenced);
    let witnesses = witness_node_ids(image);
    let mut elr = ScanElr::default();
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for pr in image.all_partitions() {
        let elr_state = elr.state(image, &pr.topic, pr.partition);
        if !elr_state.is_leaderless(pr.leader) {
            continue;
        }
        let strategy = resolve_recovery_strategy(image, &pr.topic);
        let unclean_enabled = resolve_unclean_leader_election_enabled(image, &pr.topic);
        let FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } = elect_leaderless_one(
            pr,
            &alive,
            &witnesses,
            &elr_state,
            strategy,
            unclean_enabled,
        )
        else {
            continue;
        };
        let Some((partition_epoch, leader_epoch)) = checked_epochs(
            pr,
            true,
            "election of a leaderless partition skipped because a metadata epoch is exhausted",
        ) else {
            continue;
        };
        if unclean {
            warn!(
                topic = %pr.topic, partition = pr.partition, leader = leader.0,
                "unclean leader election: the last known leader returned to a partition with no leader (possible data loss)"
            );
            metrics.record_unclean_leader_election();
        }
        tracing::info!(
            topic = %pr.topic,
            partition = pr.partition,
            unfenced = unfenced.0,
            new_leader = leader.0,
            new_isr = ?isr,
            new_leader_epoch = leader_epoch.0,
            unclean,
            "unfence: electing a leader for a partition with none"
        );
        push_partition_change(
            &mut changes,
            pr,
            leader,
            isr,
            partition_epoch,
            leader_epoch,
            unclean,
        );
    }
    // A leader is what clears the last-known ELR, and the election it comes
    // from settles the eligible set.
    ElrPublisher::new(image).extend(&mut changes);
    changes
}
