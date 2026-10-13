//! The stateright [`Model`] implementation for the failover scan: the initial
//! state, the enabled actions, the transition that calls the real
//! `failover_one`, the search boundary, and the properties the checker proves.
//!
//! A `Model` implementation is one indivisible unit, because the action
//! generator, the transition and the properties only make sense against each
//! other, so it stays whole in this file. The state it moves and the decision
//! invariants it asserts live in the sibling modules.

use {
    super::{
        decision::assert_decision,
        elr,
        failover_state::{FailoverAction, FailoverModel, FailoverState, pr_of},
    },
    crate::{
        elr::state::PartitionElr,
        leader_election::{FailoverDecision, Ladder, failover_one},
    },
    krabka_raft::NodeId,
    krabka_verified::isr::{
        AlterPartitionFacts, IsrAdmission, IsrMemberFacts, IsrMemberRole, ProposedIsr,
        isr_admission, isr_maintenance_selected,
    },
    stateright::{Model, Property},
    std::collections::HashSet,
};

impl FailoverModel {
    /// The ISR that re-admitting `follower` produces, or `None` when the
    /// change does not go through.
    ///
    /// The model holds no logs, so taking this action is its stand-in for "the
    /// follower fetched up to the leader's log end within
    /// `replica.lag.time.max.ms`". The two production decisions the expansion
    /// passes through are then driven with the facts that follow from the
    /// state: the leader's scan admits an out-of-sync follower that caught up
    /// ([`isr_maintenance_selected`]), and the controller rules on the
    /// resulting `AlterPartition` ([`isr_admission`]), which refuses it with
    /// `INELIGIBLE_REPLICA` while any proposed member is fenced -- here, dead.
    /// The proposal is sorted, as the leader's scan builds it.
    fn expanded_isr(state: &FailoverState, follower: NodeId) -> Option<Vec<NodeId>> {
        let selected = isr_maintenance_selected(IsrMemberFacts {
            role: IsrMemberRole::OutOfSyncFollower,
            log_end_matches_leader: true,
            caught_up_within_lag: true,
            fetch_within_lag: true,
        });
        if !selected {
            return None;
        }
        let mut proposed = state.isr.clone();
        proposed.push(follower);
        proposed.sort_unstable();
        let admission = isr_admission(AlterPartitionFacts {
            request_leader_epoch: state.leader_epoch,
            current_leader_epoch: state.leader_epoch,
            request_partition_epoch: 0,
            current_partition_epoch: 0,
            requester_is_leader: true,
            proposed_isr: if proposed.contains(&state.leader) {
                ProposedIsr::Valid
            } else {
                ProposedIsr::WithoutLeader
            },
            recovery_state_valid: true,
            replicas_eligible: proposed.iter().all(|n| state.alive.contains(n)),
        });
        (admission == IsrAdmission::Admit).then_some(proposed)
    }
}

impl Model for FailoverModel {
    type Action = FailoverAction;
    type State = FailoverState;

    krabka_macros::model_transition!(last, action, state; {
            match action {
                FailoverAction::Die(n) => {
                    if last.alive.len() <= 1 || !state.alive.remove(&n) {
                        return None;
                    }
                }
                FailoverAction::Revive(n) => {
                    if !state.alive.insert(n) {
                        return None;
                    }
                }
                FailoverAction::Failover(dead) => {
                    if state.alive.contains(&dead) {
                        return None;
                    }
                    let pr = pr_of(&state);
                    let alive: HashSet<NodeId> = state.alive.iter().copied().collect();
                    let elr = PartitionElr {
                        eligible_leader_replicas: state.elr.clone(),
                        last_known_elr: Vec::new(),
                    };
                    let ladder = Ladder::builder()
                        .alive(&alive)
                        .witnesses(&self.witnesses)
                        .elr(&elr)
                        .strategy(self.strategy)
                        .unclean_enabled(self.unclean_enabled)
                        .build();
                    let decision = failover_one(&pr, dead, &ladder);
                    assert_decision(self, &state, dead, &decision);
                    match decision {
                        FailoverDecision::Elect {
                            leader,
                            isr,
                            unclean,
                        } => {
                            if !unclean && !state.isr.contains(&leader) {
                                state.elected_from_elr = true;
                            }
                            state.leader = leader;
                            state.isr = isr;
                            state.leader_epoch += 1;
                        }
                        FailoverDecision::ShrinkIsr { isr } => {
                            state.isr = isr;
                        }
                        FailoverDecision::Recover(_)
                        | FailoverDecision::Unavailable
                        | FailoverDecision::NoChange => return None,
                    }
                    elr::maintain(&self.image, &mut state, &pr);
                }
                FailoverAction::ExpandIsr(follower) => {
                    let pr = pr_of(&state);
                    state.isr = Self::expanded_isr(&state, follower)?;
                    elr::maintain(&self.image, &mut state, &pr);
                }
            }
            Some(state)

    });

    fn init_states(&self) -> Vec<Self::State> {
        vec![FailoverState {
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            replicas: self.replicas.clone(),
            leader_epoch: 0,
            alive: self.replicas.iter().copied().collect(),
            // A partition whose ISR meets min ISR publishes no ELR.
            elr: Vec::new(),
            elected_from_elr: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Die: any alive broker, keeping >= 1 alive.
        if state.alive.len() > 1 {
            for &r in &self.replicas {
                if state.alive.contains(&r) {
                    actions.push(FailoverAction::Die(r));
                }
            }
        }
        // Revive: any dead broker.
        for &r in &self.replicas {
            if !state.alive.contains(&r) {
                actions.push(FailoverAction::Revive(r));
            }
        }
        // Failover: any dead broker (the real scan's filter is replicas-or-isr;
        // all model brokers are replicas), under the epoch cap.
        if state.leader_epoch < self.max_epoch {
            for &r in &self.replicas {
                if !state.alive.contains(&r) {
                    actions.push(FailoverAction::Failover(r));
                }
            }
        }
        // ExpandIsr: only a live leader proposes an ISR change, and only for
        // a live follower outside the ISR.
        if state.alive.contains(&state.leader) {
            for &r in &self.replicas {
                if state.alive.contains(&r) && !state.isr.contains(&r) {
                    actions.push(FailoverAction::ExpandIsr(r));
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("isr_subset_replicas", |_, s: &FailoverState| {
                s.isr.iter().all(|n| s.replicas.contains(n))
            }),
            Property::always("leader_in_replicas", |_, s: &FailoverState| {
                s.replicas.contains(&s.leader)
            }),
            // Kafka never writes a leader outside its ISR: `tryElection` only
            // elects a target-ISR member or narrows the ISR to the winner, and
            // a target ISR drops only the fenced broker. A record that broke
            // this could not be repaired by `AlterPartition`, whose proposals
            // must contain the leader.
            Property::always("leader_in_isr", |_, s: &FailoverState| {
                s.isr.contains(&s.leader)
            }),
            // The witness invariant: no reachable state has a witness leader.
            Property::always(
                "leader_never_witness",
                |model: &FailoverModel, s: &FailoverState| !model.witnesses.contains(&s.leader),
            ),
            Property::sometimes("can_elect", |_, s: &FailoverState| s.leader_epoch > 0),
            // Without re-admission every election would ratchet the ISR down
            // for good; this is the witness that a revived replica gets back.
            Property::sometimes("can_rejoin_isr_after_election", |_, s: &FailoverState| {
                s.leader_epoch > 0 && s.isr.len() == s.replicas.len()
            }),
            Property::sometimes("can_singleton_isr", |_, s: &FailoverState| s.isr.len() == 1),
            Property::sometimes("can_lose_isr_member", |_, s: &FailoverState| {
                s.isr.iter().any(|n| !s.alive.contains(n))
            }),
        ];
        if !self.witnesses.is_empty() {
            // The witness must survive an election that skipped it, because it
            // is what keeps `acks=all` writable after a site loss.
            properties.push(Property::sometimes(
                "witness_stays_in_isr_after_election",
                |model: &FailoverModel, s: &FailoverState| {
                    s.leader_epoch > 0 && s.isr.iter().any(|n| model.witnesses.contains(n))
                },
            ));
        }
        // KIP-966 keeps the two sets apart: a replica is eligible only while
        // it is out of the ISR.
        properties.push(Property::always(
            "elr_disjoint_from_isr",
            |_, s: &FailoverState| {
                s.isr
                    .iter()
                    .all(|n| i32::try_from(n.0).is_ok_and(|id| !s.elr.contains(&id)))
            },
        ));
        if self.elr_configured() {
            properties.extend([
                // Anti-vacuity: the maintenance rule does publish a set, and
                // `failover_one` does elect out of it.
                Property::sometimes("elr_published", |_, s: &FailoverState| !s.elr.is_empty()),
                Property::sometimes("elr_election_taken", |_, s: &FailoverState| {
                    s.elected_from_elr
                }),
            ]);
        }
        properties
    }

    fn within_boundary(&self, state: &Self::State) -> bool { state.leader_epoch <= self.max_epoch }
}
