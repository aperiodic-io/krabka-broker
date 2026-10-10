//! The stateright [`Model`] impl: how the state graph is enumerated, and what
//! must hold at every state in it.
//!
//! One file holds the whole impl because a trait impl cannot be split across
//! modules. It supplies the initial universe, the step alphabet, the dispatch
//! into [`CrossSpendModel`]'s transitions, the safety and non-vacuity
//! properties, and the boundary that keeps the search finite. The module doc
//! of [`cross_spend_model`](super) says what each property is worth.

use stateright::{Model, Property};

use super::{
    transitions::CrossSpendModel,
    universe::{EXPIRES_AT, PROPOSALS, ProposalState, Step, Universe},
};
use crate::break_glass::state_model::distinct;

impl Model for CrossSpendModel {
    type State = Universe;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Universe {
            proposals: vec![
                ProposalState {
                    approvals: Vec::new(),
                    withdrawn: false,
                    consumed: false,
                };
                PROPOSALS
            ],
            now_ms: 0,
            spends: vec![0; PROPOSALS],
            cross_spent: false,
            under_approved: false,
        }]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        for index in 0..PROPOSALS {
            for principal in &self.principals {
                actions.push(Step::Approve(index, principal));
                actions.push(Step::Withdraw(index, principal));
            }
        }
        actions.push(Step::Expire);
        for request in 0..self.requests.len() {
            actions.push(Step::Consume(request));
        }
    }

    krabka_macros::model_transition!(last, action, state; {
            match action {
                Step::Approve(index, principal) => self.settle(&mut state, index, principal, false),
                Step::Withdraw(index, principal) => self.settle(&mut state, index, principal, true),
                Step::Expire => state.now_ms = (state.now_ms + 1).min(EXPIRES_AT),
                Step::Consume(request) => self.consume(&mut state, request),
            }
            // Headline safety, per transition, so a counterexample names the step
            // that broke it rather than surfacing at the end of the run.
            assert2::assert!(
                !state.cross_spent,
                "a proposal was spent on a request it does not cover after {action:?}: {state:?}"
            );
            assert2::assert!(
                state.spends.iter().all(|spent| *spent <= 1),
                "a proposal was spent twice after {action:?}: {state:?}"
            );
            Some(state)

    });

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_cross_spend", |_, state: &Universe| !state.cross_spent),
            Property::always("no_double_spend", |_, state: &Universe| {
                state.spends.iter().all(|spent| *spent <= 1)
            }),
            Property::always("no_under_approved", |_, state: &Universe| {
                !state.under_approved
            }),
            Property::always(
                "a_withdrawn_proposal_is_never_consumed",
                |_, state: &Universe| {
                    state
                        .proposals
                        .iter()
                        .all(|proposal| !(proposal.withdrawn && proposal.consumed))
                },
            ),
            // Non-vacuity witnesses. Without them a gate that refused every
            // request would satisfy every safety property above.
            Property::sometimes("first_spent", |_, state: &Universe| state.spends[0] == 1),
            Property::sometimes("second_spent", |_, state: &Universe| state.spends[1] == 1),
            Property::sometimes("both_spent", |_, state: &Universe| {
                state.spends.iter().all(|spent| *spent == 1)
            }),
            // The state a cross-spend would need: one proposal fully approved
            // and unspent while the other is the one a request names. If this
            // never held, `no_cross_spend` would pass for want of opportunity.
            Property::sometimes(
                "one_approved_while_the_other_is_requested",
                |model: &CrossSpendModel, state: &Universe| {
                    state.proposals.iter().enumerate().any(|(index, proposal)| {
                        distinct(&proposal.approvals) >= model.config.required_approvals
                            && !proposal.consumed
                            && model
                                .requests
                                .iter()
                                .any(|request| !request.covered_by[index])
                    })
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.now_ms <= EXPIRES_AT
            && state
                .proposals
                .iter()
                .all(|proposal| proposal.approvals.len() <= self.principals.len())
    }
}
