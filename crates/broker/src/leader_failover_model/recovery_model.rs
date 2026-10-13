//! The stateright [`Model`] implementation for KIP-966 winner selection: the
//! fan-out over the bounded response domain and the properties that state what
//! `select_leader` and `has_newer_leader` must compute.
//!
//! A `Model` implementation is one indivisible unit, because the action
//! generator, the transition and the properties only make sense against each
//! other, so it stays whole in this file. The state it gathers lives in the
//! sibling `recovery_state` module.
//!
//! # What this model can and cannot say about the ELR rule
//!
//! `select_leader` elects a surviving eligible leader replica ahead of a
//! longer log and reports that election as losing nothing. There are two
//! separate claims in that sentence, and only one of them is checkable here.
//!
//! The *ordering* is: which replica the rule picks, out of which group, given
//! a published set and a witness set. That is a function of the responses and
//! nothing else, and this model checks it exhaustively -- including that the
//! shortest ELR member outranks the longest non-member, that an ELR member
//! that did not answer cannot be elected, and that no rule elects a witness.
//!
//! The *losslessness* is: that the replica so elected really does hold every
//! record the partition acknowledged. That is not a claim about selection at
//! all, it is a claim about how the controller maintained the published set,
//! and this model holds no logs, no committed prefix and no ELR maintenance to
//! check it against. `data_path_model`'s `data_elr` configuration is where
//! that half lives: it runs the real `next_partition_elr` over a partition
//! whose per-replica logs it also carries, and states the losslessness as a
//! property over both. Neither half is worth much without the other.

use {
    super::recovery_state::{
        RecoveryAction, RecoveryModel, RecoveryState, ReplicaLog, infos_of, wire_id,
    },
    crate::unclean_recovery::{ReplicaLogInfo, has_newer_leader, select_leader},
    krabka_raft::NodeId,
    stateright::{Model, Property},
    std::collections::BTreeMap,
};

/// What an election is, reduced to the two things the rest of the controller
/// reads off it: who leads, and whether the election lost committed data.
///
/// The second is [`ElectionBasis::loses_data`], not the basis itself, because
/// that is the form the claim is actually made in: it is what
/// `unclean_leader_elections_total` counts, what the audit reason says, and
/// what KFC-9's `require` gate tests.
///
/// [`ElectionBasis::loses_data`]: crate::unclean_recovery::Election
type Outcome = Option<(NodeId, bool)>;

impl RecoveryModel {
    /// The election this configuration must produce for `responses`, stated
    /// from KIP-966 rather than read off the implementation.
    ///
    /// A witness may never lead, so the witnesses leave first. An eligible
    /// leader replica that answered then wins outright, however short its log,
    /// and only when none did does the most complete surviving log win and the
    /// election report itself as data-losing.
    fn expected(&self, responses: &[ReplicaLogInfo]) -> Outcome {
        let electable: Vec<ReplicaLogInfo> = responses
            .iter()
            .copied()
            .filter(|r| !self.witnesses.contains(&r.broker_id))
            .collect();
        let from_elr: Vec<ReplicaLogInfo> = electable
            .iter()
            .copied()
            .filter(|r| self.eligible.contains(&wire_id(r.broker_id)))
            .collect();
        most_complete(&from_elr)
            .map(|leader| (leader, false))
            .or_else(|| most_complete(&electable).map(|leader| (leader, true)))
    }
}

/// The most complete log of `candidates`: the highest last-written leader
/// epoch, then the highest log-end offset, then the lowest broker id.
fn most_complete(candidates: &[ReplicaLogInfo]) -> Option<NodeId> {
    candidates
        .iter()
        .max_by(|a, b| {
            (a.last_written_leader_epoch, a.log_end_offset)
                .cmp(&(b.last_written_leader_epoch, b.log_end_offset))
                // A lower broker id is the better candidate, so it must
                // compare as the larger one here.
                .then(b.broker_id.cmp(&a.broker_id))
        })
        .map(|r| r.broker_id)
}

/// The election `select_leader` actually returns, in the same reduced form
/// [`RecoveryModel::expected`] states.
fn actual(model: &RecoveryModel, responses: &[ReplicaLogInfo]) -> Outcome {
    // `RecoveryModel` has no ISR of its own -- it states the ELR-over-longest-log
    // ranking. An empty in-sync set skips `select_leader`'s first rung, which is
    // what keeps this model checking exactly the property it was written for.
    select_leader(responses, &[], &model.eligible, &model.witnesses)
        .map(|election| (election.leader, election.basis.loses_data()))
}

impl Model for RecoveryModel {
    type Action = RecoveryAction;
    type State = RecoveryState;

    krabka_macros::model_transition!(last, action, state; {
            match action {
                RecoveryAction::AddResponse {
                    node,
                    last_written_epoch,
                    leo,
                    current_epoch,
                } => {
                    if state.responses.contains_key(&node) {
                        return None;
                    }
                    state.responses.insert(
                        node,
                        ReplicaLog {
                            last_written_leader_epoch: last_written_epoch,
                            log_end_offset: leo,
                            current_leader_epoch: current_epoch,
                        },
                    );
                }
            }
            Some(state)

    });

    fn init_states(&self) -> Vec<Self::State> {
        vec![RecoveryState {
            responses: BTreeMap::new(),
            known_leader_epoch: self.known_leader_epoch,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Each replica reports at most one log state; fan out over the bounded
        // (epoch, leo, current_epoch) domain. current_epoch ranges one past the
        // known epoch so has_newer_leader is reachable both ways.
        for &node in &self.replicas {
            if state.responses.contains_key(&node) {
                continue;
            }
            for last_written_epoch in 0..=self.max_epoch {
                for leo in 0..=self.max_leo {
                    for current_epoch in self.known_leader_epoch..=(self.known_leader_epoch + 1) {
                        actions.push(RecoveryAction::AddResponse {
                            node,
                            last_written_epoch,
                            leo,
                            current_epoch,
                        });
                    }
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            // The whole election, compared whole: who leads and whether the
            // election reports itself as losing data. An implementation that
            // ranked correctly but named the wrong basis would pass a property
            // stated over the leader alone, and the basis is the half that
            // decides whether the loss is metered.
            Property::always("election_matches_kip_966", |model, s: &RecoveryState| {
                let infos = infos_of(s);
                actual(model, &infos) == model.expected(&infos)
            }),
            // A witness replicates the partition but serves no client, so a
            // partition it leads is as unavailable as the offline one. No rule
            // may reach one -- ELR membership does not override the role.
            Property::always("no_rule_elects_a_witness", |model, s: &RecoveryState| {
                actual(model, &infos_of(s))
                    .is_none_or(|(leader, _)| !model.witnesses.contains(&leader))
            }),
            // The real has_newer_leader matches its specification.
            Property::always("has_newer_leader_matches", |_, s: &RecoveryState| {
                let infos = infos_of(s);
                has_newer_leader(&infos, s.known_leader_epoch)
                    == infos
                        .iter()
                        .any(|i| i.current_leader_epoch > s.known_leader_epoch)
            }),
            Property::sometimes("can_pick_winner", |_, s: &RecoveryState| {
                !s.responses.is_empty()
            }),
            Property::sometimes("can_detect_newer", |_, s: &RecoveryState| {
                s.responses
                    .values()
                    .any(|l| l.current_leader_epoch > s.known_leader_epoch)
            }),
        ];
        if self.can_reach_an_elr_upset() {
            // Anti-vacuity for the configurations that publish an ELR: an
            // election in which the ELR rule and the most-complete-log
            // fallback disagree, so `election_matches_kip_966` is checking the
            // ordering and not merely agreeing with it by coincidence.
            props.push(Property::sometimes(
                "the_elr_rule_outranks_a_longer_log",
                |model, s: &RecoveryState| {
                    let infos = infos_of(s);
                    let Some((leader, loses_data)) = actual(model, &infos) else {
                        return false;
                    };
                    let electable: Vec<ReplicaLogInfo> = infos
                        .iter()
                        .copied()
                        .filter(|r| !model.witnesses.contains(&r.broker_id))
                        .collect();
                    !loses_data && most_complete(&electable) != Some(leader)
                },
            ));
        }
        props
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.responses.len() <= self.replicas.len()
            && state.responses.values().all(|l| {
                l.last_written_leader_epoch <= self.max_epoch && l.log_end_offset <= self.max_leo
            })
    }
}
