//! The stateright [`Model`] implementation: the initial state, the enabled
//! actions, the transition function, the search boundary, and the ten
//! properties that the checker proves.
//!
//! The transitions themselves live in the sibling modules. This file only
//! dispatches to them and states what must hold.

use {
    super::{
        config::StretchModel,
        state::{StretchAction, StretchState, WriteOutcome, impaired},
    },
    stateright::{Model, Property},
    std::collections::BTreeSet,
};

impl StretchModel {
    /// The precondition of the single-site-loss availability claim. The two
    /// proved kernels gate it, so a configuration that is not site-loss safe
    /// makes no availability claim at all.
    fn single_site_loss_holds(&self, state: &StretchState) -> bool {
        self.min_insync_safe
            && self.quorum_tolerates_one_loss
            && impaired(state) <= 1
            && self.converged(state)
            && self.reachable_replicas_in_isr(state)
    }

    /// `true` when every replica the controller reaches is in the in-sync
    /// replica set. This is the "replicas caught up" half of the claim.
    fn reachable_replicas_in_isr(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        self.replicas
            .iter()
            .all(|replica| !alive.contains(replica) || state.isr.contains(replica))
    }
}

impl Model for StretchModel {
    type Action = StretchAction;
    type State = StretchState;

    krabka_macros::model_transition!(last, action, state; {
            match action {
                StretchAction::SiteDown(site) => {
                    let already_impaired = state.isolated.remove(&site);
                    if state.down.contains(&site)
                        || (!already_impaired && impaired(last) >= self.max_impaired)
                    {
                        return None;
                    }
                    state.down.insert(site);
                }
                StretchAction::SiteUp(site) => {
                    if !state.down.remove(&site) {
                        return None;
                    }
                    self.rejoin_isr(&mut state, site);
                }
                StretchAction::SitePartition(site) => {
                    if state.down.contains(&site)
                        || state.isolated.contains(&site)
                        || impaired(last) >= self.max_impaired
                    {
                        return None;
                    }
                    state.isolated.insert(site);
                }
                StretchAction::SiteHeal(site) => {
                    if !state.isolated.remove(&site) {
                        return None;
                    }
                    self.rejoin_isr(&mut state, site);
                }
                StretchAction::Failover(dead) => return self.apply_failover(last, dead),
                StretchAction::PreferredElection => return self.apply_preferred(last),
                StretchAction::ProduceAcksAll => self.apply_produce(&mut state),
            }
            Some(state)

    });

    fn init_states(&self) -> Vec<Self::State> {
        vec![StretchState {
            down: BTreeSet::new(),
            isolated: BTreeSet::new(),
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            leader_epoch: 0,
            last_write: None,
            commit_in_minority: false,
            epoch_reused: false,
            preferred_pinning_broken: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let room = impaired(state) < self.max_impaired;
        for site in 0..self.site_count() {
            if state.down.contains(&site) {
                actions.push(StretchAction::SiteUp(site));
                continue;
            }
            if state.isolated.contains(&site) {
                actions.push(StretchAction::SiteHeal(site));
                actions.push(StretchAction::SiteDown(site));
            } else if room {
                actions.push(StretchAction::SitePartition(site));
                actions.push(StretchAction::SiteDown(site));
            }
        }
        let alive = self.alive(state);
        for &replica in &self.replicas {
            if !alive.contains(&replica) {
                actions.push(StretchAction::Failover(replica));
            }
        }
        actions.push(StretchAction::PreferredElection);
        actions.push(StretchAction::ProduceAcksAll);
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // 1. A witness serves no client, so it never leads a partition.
            Property::always(
                "leader_never_witness",
                |model: &StretchModel, state: &StretchState| {
                    !model.witnesses.contains(&state.leader)
                },
            ),
            // 2. The headline claim: one site loss keeps `acks=all` writable.
            Property::always(
                "single_site_loss_keeps_acks_all_writable",
                |model: &StretchModel, state: &StretchState| {
                    if !model.single_site_loss_holds(state) {
                        return true;
                    }
                    let isr_size = i64::try_from(state.isr.len()).expect("ISR size fits in i64");
                    model.produce_outcome(state) == WriteOutcome::Committed
                        && isr_size >= model.survivors
                },
            ),
            // 3. A leader change always carries a greater epoch.
            Property::always(
                "one_leader_per_epoch",
                |_: &StretchModel, state: &StretchState| !state.epoch_reused,
            ),
            // 4. A minority of the voters never commits a write.
            Property::always(
                "minority_never_commits",
                |_: &StretchModel, state: &StretchState| !state.commit_in_minority,
            ),
            // 5. The preferred site keeps leadership while it can take it.
            Property::always(
                "preferred_site_keeps_leadership",
                |_: &StretchModel, state: &StretchState| !state.preferred_pinning_broken,
            ),
            // The witness is what keeps `acks=all` writable after a site loss,
            // so an election that skipped it must still leave it in the ISR.
            Property::sometimes(
                "witness_stays_in_isr_after_failover",
                |model: &StretchModel, state: &StretchState| {
                    state.leader_epoch > 0
                        && state.isr.iter().any(|node| model.witnesses.contains(node))
                },
            ),
            // Non-vacuity for property 2. The precondition of the headline
            // claim is reachable with a whole site lost.
            Property::sometimes(
                "single_site_loss_precondition_is_reachable",
                |model: &StretchModel, state: &StretchState| {
                    impaired(state) == 1 && model.single_site_loss_holds(state)
                },
            ),
            Property::sometimes(
                "one_site_loss_still_commits",
                |_: &StretchModel, state: &StretchState| {
                    impaired(state) == 1 && state.last_write == Some(WriteOutcome::Committed)
                },
            ),
            Property::sometimes(
                "two_site_loss_rejects_the_write",
                |_: &StretchModel, state: &StretchState| {
                    impaired(state) == 2 && state.last_write == Some(WriteOutcome::Rejected)
                },
            ),
            Property::sometimes(
                "preferred_election_returns_leadership",
                |model: &StretchModel, state: &StretchState| {
                    impaired(state) == 0
                        && state.leader_epoch > 0
                        && model.site_of(state.leader) == model.preferred_site
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch && impaired(state) <= self.max_impaired
    }
}
