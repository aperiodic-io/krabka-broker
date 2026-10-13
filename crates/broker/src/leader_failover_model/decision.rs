//! The per-decision safety invariants: what must hold of one `failover_one`
//! result given the state the controller saw before it.
//!
//! These checks run inside the transition rather than as stateright
//! properties, because they relate a decision to its own pre-state. A property
//! only sees the states, and the pre-state is gone by the time the successor
//! is checked.
//!
//! The rules are stated from Apache Kafka rather than read off
//! `failover_one`. `ReplicationControlManager.handleBrokerFenced` visits only
//! the partitions whose ISR holds the broker, and hands
//! `PartitionChangeBuilder` a target ISR that is the old one without that
//! broker, and nothing else. `electAnyLeader` keeps the current leader while it
//! is a valid new leader of that target, and otherwise walks the ladder: the
//! first acceptable target-ISR member in assignment order; with no live target
//! ISR, the first acceptable eligible leader replica in assignment order,
//! reported clean; only then krabka's offset-aware recovery, and last the
//! KIP-841 election of the first acceptable replica, reported unclean. Each
//! decision is checked to have taken the highest rung the pre-state offered,
//! so a data-losing rung is never chosen while a cleaner one was available.

use {
    super::failover_state::{FailoverModel, FailoverState},
    crate::{config_keys::RecoveryStrategy, leader_election::FailoverDecision},
    assert2::assert,
    krabka_raft::NodeId,
};

/// What the pre-failover state offered each rung of the election ladder.
struct Rungs {
    /// Whether Kafka's `partitionsWithBrokerInIsr(dead)` visits the partition
    /// at all: `dead` leads it or sits in its ISR.
    touched: bool,
    /// Kafka's `targetIsr`: the ISR without `dead`, in ISR order.
    target_isr: Vec<NodeId>,
    /// `isValidNewLeader(partition.leader)`: the current leader keeps its
    /// place.
    leader_stays: bool,
    /// Target-ISR members that are alive, witnesses included.
    live_isr: Vec<NodeId>,
    /// Kafka's clean pick: the first replica, in assignment order, that is in
    /// the target ISR and may lead.
    first_electable_isr: Option<NodeId>,
    /// Kafka's ELR pick: the first replica, in assignment order, that the
    /// published set names and that may lead.
    first_electable_elr: Option<NodeId>,
    /// Kafka's KIP-841 pick: the first replica, in assignment order, that may
    /// lead.
    first_acceptable: Option<NodeId>,
}

impl Rungs {
    fn of(model: &FailoverModel, pre: &FailoverState, dead: NodeId) -> Self {
        let live = |n: &NodeId| *n != dead && pre.alive.contains(n);
        // Kafka's `isAcceptableLeader`, plus the rule that a witness never
        // leads.
        let acceptable = |n: &NodeId| live(n) && !model.witnesses.contains(n);
        let target_isr: Vec<NodeId> = pre.isr.iter().copied().filter(|n| *n != dead).collect();
        let live_isr = target_isr.iter().copied().filter(live).collect();
        let first_electable_isr = pre
            .replicas
            .iter()
            .copied()
            .find(|n| target_isr.contains(n) && acceptable(n));
        let first_electable_elr = pre
            .replicas
            .iter()
            .copied()
            .filter(acceptable)
            .find(|n| i32::try_from(n.0).is_ok_and(|id| pre.elr.contains(&id)));
        let first_acceptable = pre.replicas.iter().copied().find(acceptable);
        Self {
            touched: pre.leader == dead || pre.isr.contains(&dead),
            leader_stays: target_isr.contains(&pre.leader) && acceptable(&pre.leader),
            target_isr,
            live_isr,
            first_electable_isr,
            first_electable_elr,
            first_acceptable,
        }
    }

    /// No rung could replace the leader: every live target-ISR member is a
    /// witness, or the live target ISR is empty and no out-of-ISR rung is
    /// open.
    fn nothing_could_lead(&self, model: &FailoverModel) -> bool {
        let out_of_isr_open = self.first_electable_elr.is_some()
            || model.strategy != RecoveryStrategy::None
            || (model.unclean_enabled && self.first_acceptable.is_some());
        self.first_electable_isr.is_none() && (!self.live_isr.is_empty() || !out_of_isr_open)
    }
}

/// Verify a `failover_one` decision against the pre-failover state. These are
/// the safety-critical invariants. They hold per-decision under any ordering.
pub(super) fn assert_decision(
    model: &FailoverModel,
    pre: &FailoverState,
    dead: NodeId,
    d: &FailoverDecision,
) {
    let rungs = Rungs::of(model, pre, dead);
    if !rungs.touched {
        assert!(
            *d == FailoverDecision::NoChange,
            "{dead} is neither leader nor ISR member, yet the decision was {d:?}"
        );
        return;
    }
    match d {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            assert!(
                !rungs.leader_stays,
                "elected over the valid leader {}",
                pre.leader
            );
            assert!(*leader != dead, "elected the dead broker {dead}");
            assert!(
                pre.alive.contains(leader),
                "elected leader {leader} not alive"
            );
            assert!(
                isr.contains(leader),
                "elected leader {leader} not in new ISR {isr:?}"
            );
            assert!(
                !model.witnesses.contains(leader),
                "elected witness {leader} as leader"
            );
            if *unclean {
                // KIP-841 is the last rung: it may be taken only once the
                // toggle allows it and every cleaner rung is out of reach.
                assert!(
                    model.unclean_enabled,
                    "unclean election without unclean_enabled"
                );
                assert!(
                    rungs.live_isr.is_empty(),
                    "unclean election with live ISR members {:?}",
                    rungs.live_isr
                );
                assert!(
                    rungs.first_electable_elr.is_none(),
                    "unclean election while ELR member {:?} could lead",
                    rungs.first_electable_elr
                );
                assert!(
                    model.strategy == RecoveryStrategy::None,
                    "unclean election ahead of the {:?} offset-aware recovery",
                    model.strategy
                );
                assert!(
                    Some(*leader) == rungs.first_acceptable && *isr == vec![*leader],
                    "unclean election of {leader} with ISR {isr:?}, not Kafka's first acceptable replica {:?} alone",
                    rungs.first_acceptable
                );
            } else if pre.isr.contains(leader) {
                // Clean election from the ISR: the new leader holds every
                // committed record, and the ISR is Kafka's target ISR, so a
                // live witness stays in it.
                assert!(
                    Some(*leader) == rungs.first_electable_isr && *isr == rungs.target_isr,
                    "clean election of {leader} with ISR {isr:?}, not Kafka's pick {:?} with target ISR {:?}",
                    rungs.first_electable_isr,
                    rungs.target_isr
                );
            } else {
                // Clean election from the ELR: legal only once the live ISR is
                // empty, and then it is Kafka's first acceptable member, alone
                // in the new ISR.
                assert!(
                    rungs.live_isr.is_empty(),
                    "ELR election of {leader} while ISR members {:?} are live",
                    rungs.live_isr
                );
                assert!(
                    Some(*leader) == rungs.first_electable_elr && *isr == vec![*leader],
                    "clean election of {leader} with ISR {isr:?} from outside the ISR, not the ELR pick {:?} alone",
                    rungs.first_electable_elr
                );
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            // Only `dead` leaves, and the leader stays in the ISR it leads.
            assert!(pre.leader != dead, "ShrinkIsr for a dead leader");
            assert!(
                *isr == rungs.target_isr && isr.len() < pre.isr.len(),
                "shrink to {isr:?}, not Kafka's target ISR {:?}",
                rungs.target_isr
            );
            assert!(
                isr.contains(&pre.leader),
                "shrink to {isr:?} drops the leader {}",
                pre.leader
            );
            // A leader that cannot serve keeps its place only when nothing
            // could replace it.
            assert!(
                rungs.leader_stays || rungs.nothing_could_lead(model),
                "shrink kept the invalid leader {} although a rung was open: ISR pick {:?}, ELR pick {:?}",
                pre.leader,
                rungs.first_electable_isr,
                rungs.first_electable_elr
            );
        }
        FailoverDecision::Recover(s) => {
            assert!(*s != RecoveryStrategy::None, "Recover with strategy None");
            assert!(
                *s == model.strategy,
                "Recover({s:?}) under the {:?} strategy",
                model.strategy
            );
            assert!(
                !rungs.leader_stays,
                "Recover over the valid leader {}",
                pre.leader
            );
            // The offset-aware recovery may pick an incomplete log, so it
            // sits below both lossless rungs.
            assert!(
                rungs.live_isr.is_empty() && rungs.first_electable_elr.is_none(),
                "Recover while the ISR {:?} or the ELR pick {:?} could lead",
                rungs.live_isr,
                rungs.first_electable_elr
            );
        }
        FailoverDecision::Unavailable => {
            // Unavailable is safe, but only right when nothing could lead.
            // It writes nothing, so it is reserved for the partition `dead`
            // leads: any other partition still loses `dead` from its ISR.
            assert!(pre.leader == dead, "Unavailable with a live leader");
            assert!(
                rungs.nothing_could_lead(model),
                "Unavailable although a rung was open: live ISR {:?}, ELR pick {:?}",
                rungs.live_isr,
                rungs.first_electable_elr
            );
        }
        FailoverDecision::NoChange => {
            // Reached only for a touched partition, which always changes.
            assert!(
                !rungs.touched,
                "NoChange for {dead}, which leads or sits in ISR {:?}",
                pre.isr
            );
        }
    }
}
