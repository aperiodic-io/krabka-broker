//! Exhaustive stateright enumeration of one proposal's whole lifecycle.
//!
//! The model drives the REAL decision code. Each transition builds a
//! [`BreakGlassProposalRecord`] out of the model state and calls
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) for an
//! approval and a withdrawal, and
//! [`gate::authorize`](crate::break_glass::gate::authorize) for a consume,
//! against a real [`MetadataImage`]. A rule that the model checks is therefore
//! the rule the broker runs, and not a second copy of it.
//!
//! The alphabet is every interleaving of approve, withdraw, expire, and
//! consume, over a tiny universe of principals. The two headline properties are
//! the promises the feature makes:
//!
//! - `no_double_spend`: no interleaving consumes one proposal twice. One
//!   approval authorizes one transition.
//! - `no_under_approved`: no interleaving consumes a proposal that fewer than
//!   `required_approvals` distinct principals approved. A rule about people
//!   cannot be satisfied by one person acting twice.
//!
//! The clock is a small integer of logical milliseconds. The proposal is
//! created at zero and expires at [`EXPIRES_AT`], and an expire action advances
//! the clock by one. The bound keeps the state graph exhaustive.

use krabka_metadata::{
    BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord, MetadataImage, MetadataRecord,
};
use krabka_units::millis;
use stateright::{Model, Property};
use uuid::Uuid;

use crate::{
    break_glass::{
        config::BreakGlassPolicy,
        gate,
        handlers::approve::{self, Attempt},
    },
    config::BreakGlassConfig,
    operator_keys::OperatorKeys,
};

/// The logical millisecond at which the proposal expires.
const EXPIRES_AT: i64 = 2;

/// The action the model gates. The rule under test does not depend on which
/// one it is, so the model fixes one and varies the people and the clock.
const ACTION: BreakGlassAction = BreakGlassAction::DeleteTopic;

/// The target the proposal names.
const TARGET: &str = "doomed";

/// The principal that opened the proposal. It cannot approve.
const PROPOSER: &str = "User:alice";

const TARGET_STATE_COUNT: usize = 1_000_000;
const MAX_UNIQUE_STATES: usize = 200_000;
const MAX_DEPTH: usize = 20;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_TWO_OF_THREE: usize = 36;
const PINNED_UNIQUE_STATES_THREE_OF_FOUR: usize = 114;

/// One proposal, projected onto the fields a transition reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ProposalState {
    /// The approving principals, in the order they approved.
    approvals: Vec<&'static str>,
    /// `true` once an operator withdrew the proposal.
    withdrawn: bool,
    /// `true` once a transition consumed the proposal.
    consumed: bool,
    /// The logical clock, in milliseconds.
    now_ms: i64,
    /// How many times a consume succeeded. The headline property reads it.
    consumes: u8,
    /// `true` once a consume succeeded with too few distinct approvers. The
    /// second headline property reads it.
    under_approved: bool,
}

/// One step an operator or the controller can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Step {
    /// One principal approves the proposal.
    Approve(&'static str),
    /// One principal withdraws the proposal.
    Withdraw(&'static str),
    /// The clock advances by one millisecond.
    Expire,
    /// A gated transition tries to spend the proposal.
    Consume,
}

struct BreakGlassModel {
    config: BreakGlassConfig,
    /// Every principal that can send a request, inside the approver set and
    /// outside it.
    principals: Vec<&'static str>,
}

/// The stored record that one model state stands for.
fn record(state: &ProposalState) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(1),
        action: ACTION,
        target: TARGET.to_owned(),
        proposer: PROPOSER.to_owned(),
        reason: "incident 42".to_owned(),
        created_at_ms: 0,
        expires_at_ms: EXPIRES_AT,
        approvals: model_approvals(&state.approvals),
        consumed_at_ms: i64::from(state.consumed),
        withdrawn: state.withdrawn,
    }
}

/// The image that a gated handler reads for one model state.
fn image_of(state: &ProposalState) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BreakGlassProposal(record(state)));
    image
}

impl BreakGlassModel {
    fn policy(&self) -> BreakGlassPolicy<'_> {
        BreakGlassPolicy::new(&self.config)
    }

    /// Apply one approval or one withdrawal through the real handler decision.
    fn settle(&self, state: &mut ProposalState, principal: &'static str, withdraw: bool) {
        let stored = record(state);
        if let Ok(updated) = model_settle(self.policy(), &stored, principal, withdraw, state.now_ms)
        {
            state.withdrawn = updated.withdrawn;
            state.approvals = model_approval_principals(&self.principals, &updated.approvals);
        }
    }

    /// Try to spend the proposal through the real gate.
    fn consume(&self, state: &mut ProposalState) {
        let image = image_of(state);
        if gate::authorize(&image, &self.config, ACTION, TARGET, state.now_ms).is_ok() {
            state.consumes = state.consumes.saturating_add(1);
            if distinct(&state.approvals) < self.policy().required_approvals() {
                state.under_approved = true;
            }
            state.consumed = true;
        }
    }
}

/// How many different principals appear in `approvals`.
pub(super) fn distinct(approvals: &[&'static str]) -> usize {
    let mut seen: Vec<&str> = Vec::with_capacity(approvals.len());
    for principal in approvals {
        if !seen.contains(principal) {
            seen.push(principal);
        }
    }
    seen.len()
}

impl Model for BreakGlassModel {
    type State = ProposalState;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProposalState {
            approvals: Vec::new(),
            withdrawn: false,
            consumed: false,
            now_ms: 0,
            consumes: 0,
            under_approved: false,
        }]
    }

    fn actions(&self, _state: &Self::State, actions: &mut Vec<Self::Action>) {
        for principal in &self.principals {
            actions.push(Step::Approve(principal));
            actions.push(Step::Withdraw(principal));
        }
        actions.push(Step::Expire);
        actions.push(Step::Consume);
    }

    krabka_macros::model_transition!(last, action, state; {
            match action {
                Step::Approve(principal) => self.settle(&mut state, principal, false),
                Step::Withdraw(principal) => self.settle(&mut state, principal, true),
                Step::Expire => state.now_ms = (state.now_ms + 1).min(EXPIRES_AT),
                Step::Consume => self.consume(&mut state),
            }
            // Headline safety, per transition. It fires the moment an interleaving
            // spends one approval twice, rather than at the end of the run.
            assert2::assert!(
                state.consumes <= 1,
                "a proposal was consumed twice after {action:?}: {state:?}"
            );
            Some(state)

    });

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("no_double_spend", |_, state: &ProposalState| {
                state.consumes <= 1
            }),
            Property::always("no_under_approved", |_, state: &ProposalState| {
                !state.under_approved
            }),
            Property::always(
                "a_withdrawn_proposal_is_never_consumed",
                |_, state: &ProposalState| !(state.withdrawn && state.consumes > 0),
            ),
            // Non-vacuity witnesses. Without them a model that refuses every
            // action would pass every safety property.
            Property::sometimes("consumed", |_, state: &ProposalState| state.consumes == 1),
            Property::sometimes("withdrawn", |_, state: &ProposalState| state.withdrawn),
            Property::sometimes("expired", |_, state: &ProposalState| {
                state.now_ms >= EXPIRES_AT
            }),
            Property::sometimes(
                "fully_approved",
                |model: &BreakGlassModel, state: &ProposalState| {
                    distinct(&state.approvals) >= model.config.required_approvals
                },
            ),
            Property::sometimes(
                "expired_before_it_was_spent",
                |model: &BreakGlassModel, state: &ProposalState| {
                    state.now_ms >= EXPIRES_AT
                        && distinct(&state.approvals) >= model.config.required_approvals
                        && state.consumes == 0
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.approvals.len() <= self.principals.len() && state.now_ms <= EXPIRES_AT
    }
}

pub(super) fn config(
    approvers: &[&str],
    required_approvals: usize,
    expires_at: i64,
) -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: approvers.iter().map(|name| (*name).to_owned()).collect(),
        required_approvals,
        proposal_ttl: millis(u32::try_from(expires_at).expect("a small logical expiry")),
        signed_actions: Vec::new(),
        ..BreakGlassConfig::default()
    }
}

fn run(model: BreakGlassModel, label: &str, pinned_unique_states: usize) {
    crate::model_check::check_model(
        model,
        label,
        (MAX_DEPTH, TARGET_STATE_COUNT, MAX_UNIQUE_STATES),
        pinned_unique_states,
    );
}

#[test]
fn two_approvals_of_three_approvers() {
    // The default rule. `User:mallory` is outside the approver set, and
    // `User:alice` proposed, so neither can supply an approval.
    run(
        BreakGlassModel {
            config: config(&["User:alice", "User:bob", "User:carol"], 2, EXPIRES_AT),
            principals: vec!["User:alice", "User:bob", "User:carol", "User:mallory"],
        },
        "two_approvals_of_three_approvers",
        PINNED_UNIQUE_STATES_TWO_OF_THREE,
    );
}

#[test]
fn three_approvals_of_four_approvers() {
    // A stricter rule, so an interleaving needs three different people before
    // a consume can succeed.
    run(
        BreakGlassModel {
            config: config(
                &["User:alice", "User:bob", "User:carol", "User:dave"],
                3,
                EXPIRES_AT,
            ),
            principals: vec!["User:alice", "User:bob", "User:carol", "User:dave"],
        },
        "three_approvals_of_four_approvers",
        PINNED_UNIQUE_STATES_THREE_OF_FOUR,
    );
}

/// Project the model's ordered principals into unsigned stored approvals.
pub(super) fn model_approvals(principals: &[&str]) -> Vec<BreakGlassApproval> {
    principals
        .iter()
        .map(|principal| BreakGlassApproval {
            principal: (*principal).to_owned(),
            approved_at_ms: 0,
            key_id: String::new(),
            signature: Vec::new(),
        })
        .collect()
}

/// Resolve stored approval names back to the model's static principal universe.
pub(super) fn model_approval_principals(
    principals: &[&'static str],
    approvals: &[BreakGlassApproval],
) -> Vec<&'static str> {
    approvals
        .iter()
        .map(|approval| {
            principals
                .iter()
                .copied()
                .find(|name| *name == approval.principal)
                .expect("an approval names a principal of the model universe")
        })
        .collect()
}

/// Apply an unsigned model step through the real approval decision.
pub(super) fn model_settle(
    policy: BreakGlassPolicy<'_>,
    stored: &BreakGlassProposalRecord,
    principal: &str,
    withdraw: bool,
    now_ms: i64,
) -> Result<BreakGlassProposalRecord, crate::break_glass::handlers::Refusal> {
    let attempt = Attempt {
        principal,
        key_id: "",
        signature: &[],
        withdraw,
        now_ms,
    };
    approve::decide(policy, &OperatorKeys::default(), stored, &attempt)
}
