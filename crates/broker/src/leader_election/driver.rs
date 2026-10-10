//! The controller-side failover driver. [`run_liveness_tick`] is the one entry
//! point the broker's liveness ticker calls: it seeds and discovers broker
//! sessions, re-drives stuck failovers through [`sweep_dead_leaders`], and
//! runs [`on_broker_dead`] on every fresh death edge.

use std::{sync::Arc, time::Duration};

use krabka_metadata::MetadataImage;
use krabka_raft::NodeId;
use tracing::warn;

use super::scan::compute_failover_changes;
use crate::{error::BrokerError, heartbeat::controller_state::ControllerLivenessState};

#[cfg(test)]
mod failover_tests;
#[cfg(test)]
mod tick_tests;

/// Upper bound on one failover commit. The liveness ticker awaits
/// [`on_broker_dead`] inline. A stalled raft commit must not block every later
/// tick, so the wait turns into an error and the sweep retries next tick.
const FAILOVER_SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// What asked for a dead-broker failover. The edge fires once per death and
/// warns about every partition it cannot fail over. The sweep repeats the
/// same question on every tick while the broker stays dead, so it reports
/// those partitions at debug level only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailoverTrigger {
    Edge,
    Sweep,
}

/// `true` when the leader watch names this node. Only the controller leader
/// receives heartbeats and can `submit_change`.
fn is_controller_leader(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
) -> bool {
    controller
        .watch_leader()
        .borrow()
        .is_some_and(|n| n == node_id)
}

/// What the liveness ticker remembers between two ticks.
#[derive(Default)]
pub(crate) struct LivenessTickState {
    /// Whether the previous tick saw this node as controller leader. The
    /// first tick of a new term seeds the registry before anything else, so a
    /// registry that expired every session while this node was a follower
    /// (followers receive no heartbeats) cannot drive a failover.
    was_leader: bool,
    /// The image and dead set of the last sweep that found no dead broker
    /// with a partition still to re-drive. While both are unchanged, the
    /// sweep has nothing new to learn and skips its walk over the image.
    clean_sweep: Option<(Arc<MetadataImage>, std::collections::HashSet<u64>)>,
}

/// One tick of the controller-side failover driver. The liveness ticker in
/// `broker.rs` calls it at every `liveness_tick_interval` and hands it the
/// same [`LivenessTickState`] each time. Four steps run in order:
///
/// 1. New term. On the first tick that sees this node as controller leader,
///    every registered broker is seeded alive with a full timeout window.
///    The leadership watcher seeds too, but it runs on its own task; the
///    ticker must not sweep this term before the registry reflects it.
/// 2. Discovery. When this node is the controller leader, every broker that
///    is registered in the metadata image but unknown to the liveness
///    registry starts a fenced session now. The registry otherwise only
///    knows brokers that heartbeated this controller or that a leadership
///    change seeded. A broker that registers and dies before its first
///    heartbeat reaches this controller would never expire, and the
///    partitions it leads would never fail over.
/// 3. Level. [`sweep_dead_leaders`] re-drives the failover for every broker
///    that was already dead before this tick and still leads a partition or
///    still sits in an ISR. That guarantees convergence when an earlier edge
///    was lost. It runs before the edge step so a death handled by this
///    tick's edge is not submitted a second time before the image catches
///    up.
/// 4. Edge. `liveness.tick()` emits `AliveToDead` once per death, and this
///    step runs [`on_broker_dead`] at once. That is the fast path.
/// 5. Publication.
///    [`publish_fencing_changes`](crate::heartbeat::fencing::publish_fencing_changes)
///    writes the registry's fencing decisions into the metadata log, so a
///    `Metadata` or `DescribeTopicPartitions` request served by a follower
///    reports the same `offline_replicas` as one served here. It runs last,
///    so a death this tick detected is published in the same tick.
pub(crate) async fn run_liveness_tick(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
    recovery: &crate::unclean_recovery::UncleanRecoveryHandle,
    state: &mut LivenessTickState,
) {
    let leader_now = is_controller_leader(controller, node_id);
    if leader_now {
        let image = controller.current_image();
        if !state.was_leader {
            liveness
                .seed_term(
                    controller.current_controller_epoch(),
                    crate::heartbeat::controller_state::replicated_registrations(&image),
                )
                .await;
        }
        liveness
            .track_registered(image.brokers().map(|broker| broker.node_id.0))
            .await;
    } else {
        state.clean_sweep = None;
    }
    state.was_leader = leader_now;
    sweep_dead_leaders(controller, node_id, liveness, metrics, recovery, state).await;
    for transition in liveness.tick().await {
        use crate::heartbeat::controller_state::LivenessTransition::{AliveToDead, DeadToAlive};
        match transition {
            AliveToDead(broker_id) => {
                if let Err(error) = on_broker_dead(
                    controller,
                    node_id,
                    NodeId(broker_id),
                    liveness,
                    metrics,
                    recovery,
                )
                .await
                {
                    warn!(broker = broker_id, %error, "broker-death election failed");
                }
            }
            DeadToAlive(broker_id) => {
                on_broker_alive(controller, node_id, NodeId(broker_id), liveness);
            }
        }
    }
    if is_controller_leader(controller, node_id) {
        crate::heartbeat::fencing::publish_fencing_changes(controller, liveness).await;
        // After the await, so the count answers "the fencing this pass
        // decided on is applied", not "a pass started". The publication
        // awaits its own commit, and that is the whole difference between a
        // published decision and one still in flight.
        metrics.controller_fencing_publications_total.inc();
    }
}

/// Called when the liveness ticker observes `AliveToDead(dead)`. This function
/// scans every partition where `dead` is leader OR in the ISR. It proposes
/// updated `PartitionRecord`s.
///
/// This is a no-op unless `controller` is currently the openraft leader. Only
/// the leader can `submit_change`.
pub(crate) async fn on_broker_dead(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
    dead: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
    recovery: &crate::unclean_recovery::UncleanRecoveryHandle,
) -> Result<(), BrokerError> {
    fail_over_dead_broker(
        FailoverTrigger::Edge,
        node_id,
        dead,
        FailoverServices {
            controller,
            liveness,
            metrics,
            recovery,
        },
    )
    .await
}

/// Services shared by death-edge and retry-sweep failover.
struct FailoverServices<'a> {
    controller: &'a Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: &'a Arc<ControllerLivenessState>,
    metrics: &'a crate::metrics::BrokerMetrics,
    recovery: &'a crate::unclean_recovery::UncleanRecoveryHandle,
}

/// The failover behind [`on_broker_dead`] and [`sweep_dead_leaders`].
/// `trigger` only selects how loudly a partition with no live ISR replica is
/// reported.
#[tracing::instrument(
    name = "leader_election_on_broker_dead",
    level = "info",
    skip_all,
    fields(node_id, dead, ?trigger),
    err
)]
async fn fail_over_dead_broker(
    trigger: FailoverTrigger,
    node_id: NodeId,
    dead: NodeId,
    services: FailoverServices<'_>,
) -> Result<(), BrokerError> {
    let FailoverServices {
        controller,
        liveness,
        metrics,
        recovery,
    } = services;
    if !is_controller_leader(controller, node_id) {
        return Ok(());
    }

    let image = controller.current_image();
    let plan = compute_failover_changes(&image, dead, liveness, metrics).await;
    for (topic, partition) in &plan.unavailable {
        match trigger {
            FailoverTrigger::Edge => warn!(
                %topic, partition,
                "leader dead, no live ISR replica; partition unavailable"
            ),
            FailoverTrigger::Sweep => tracing::debug!(
                %topic, partition,
                "leader still dead, no live ISR replica; partition stays unavailable"
            ),
        }
    }
    if !plan.changes.is_empty() {
        // Bound the commit. A stall here would wedge the liveness ticker, and
        // with it every later edge and sweep. On elapse the caller logs the
        // error and the sweep retries on the next tick.
        let submit = controller.submit_change(plan.changes);
        match tokio::time::timeout(FAILOVER_SUBMIT_TIMEOUT, submit).await {
            Ok(result) => {
                result.map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
            }
            Err(_elapsed) => {
                return Err(BrokerError::Replication(format!(
                    "submit_change: no commit within {FAILOVER_SUBMIT_TIMEOUT:?}"
                )));
            }
        }
    }
    // KIP-966: partitions whose topic opted into an offset-aware recovery
    // strategy are handed to the Unclean Recovery Manager, which polls
    // surviving replicas for their log state before electing. Fire and
    // forget — the failover path does not await the outcome.
    for (topic, partition, strategy) in plan.recoveries {
        recovery
            .enqueue(crate::unclean_recovery::RecoveryJob {
                topic,
                partition,
                strategy,
                reply: None,
                // KFC-9: nobody asked for this recovery, so there is no
                // proposal to name and nobody to refuse. A dead leader at
                // 03:00 has no caller waiting on an answer, and
                // `break_glass.background_unclean_recovery` is what decides
                // whether the URM runs it, audits it, or leaves the partition
                // offline.
                proposal: None,
            })
            .await;
    }
    Ok(())
}

/// Level-triggered companion to [`on_broker_dead`]. [`run_liveness_tick`]
/// calls it on every tick, before it drains the edge transitions.
///
/// The `AliveToDead` edge fires once per death. The edge is lost when this
/// node is not the controller leader at that instant, when no ISR replica is
/// alive at that instant, or when the commit stalls. This sweep asks the level
/// question instead: is a dead broker still the leader of a partition, or
/// still an ISR member? If so, it runs the same failover as
/// [`on_broker_dead`] again for that broker, with [`FailoverTrigger::Sweep`]
/// so a partition that has no live replica to elect is not warned about on
/// every tick. [`compute_failover_changes`] is idempotent, so a repeat after a
/// completed failover yields an empty plan and no commit.
///
/// The sweep is cheap on the common path. It reads the leader watch and the
/// dead set. It walks the image only when at least one broker is dead, and
/// only when the image or the dead set changed since the last walk that
/// found nothing to re-drive. A broker that stays dead for good, for example
/// one that was decommissioned, therefore costs one walk per image change,
/// not one per tick.
pub(crate) async fn sweep_dead_leaders(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    node_id: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
    recovery: &crate::unclean_recovery::UncleanRecoveryHandle,
    state: &mut LivenessTickState,
) {
    if !is_controller_leader(controller, node_id) {
        return;
    }
    let dead = liveness.dead_snapshot().await;
    if dead.is_empty() {
        state.clean_sweep = None;
        return;
    }
    let image = controller.current_image();
    if let Some((seen_image, seen_dead)) = &state.clean_sweep
        && Arc::ptr_eq(seen_image, &image)
        && *seen_dead == dead
    {
        return;
    }
    // Dead brokers that still lead a partition or still sit in an ISR. A
    // `BTreeSet` gives a stable retry order across ticks.
    let mut stuck: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for pr in image.all_partitions() {
        if dead.contains(&pr.leader.0) {
            stuck.insert(pr.leader.0);
        }
        stuck.extend(pr.isr.iter().map(|n| n.0).filter(|n| dead.contains(n)));
    }
    if stuck.is_empty() {
        state.clean_sweep = Some((image, dead));
        return;
    }
    state.clean_sweep = None;
    for broker_id in stuck {
        if let Err(error) = fail_over_dead_broker(
            FailoverTrigger::Sweep,
            node_id,
            NodeId(broker_id),
            FailoverServices {
                controller,
                liveness,
                metrics,
                recovery,
            },
        )
        .await
        {
            warn!(broker = broker_id, %error, "broker-death election failed");
        }
    }
}

/// Called when the liveness ticker observes `DeadToAlive(alive)`. This
/// is a no-op. ISR expand happens on its own through
/// `isr_maintenance` once the rejoined broker's replicator catches up.
/// The hook is here for future enhancements, for example auto-rebalance.
pub(crate) fn on_broker_alive(
    _controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    _node_id: NodeId,
    _alive: NodeId,
    _liveness: &Arc<ControllerLivenessState>,
) {
}
