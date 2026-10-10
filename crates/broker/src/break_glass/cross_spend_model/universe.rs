//! What a state of the two-proposal run is, and the alphabet of steps that
//! move between states.
//!
//! Nothing here calls broker code. A [`Universe`] carries only the fields a
//! transition reads or writes, which is what keeps the state graph small
//! enough to enumerate, and [`Step`] names every move an operator or the
//! controller can take against it. [`ProposalSpec`] and [`Request`] are the
//! parts a scenario fixes before the run starts, so they stay outside the
//! state.

use krabka_metadata::BreakGlassAction;

/// The logical millisecond at which both proposals expire.
pub(super) const EXPIRES_AT: i64 = 2;

/// How many proposals the image holds. Two is the smallest number that can
/// express a cross-spend, and each extra proposal multiplies the state graph.
pub(super) const PROPOSALS: usize = 2;

/// One proposal the model holds fixed across the run.
#[derive(Clone, Copy, Debug)]
pub(super) struct ProposalSpec {
    /// Distinguishes the stored records, and breaks `better_candidate` ties.
    pub(super) id: u128,
    pub(super) action: BreakGlassAction,
    pub(super) target: &'static str,
    /// The principal that opened it. That principal cannot approve *this*
    /// proposal, and may approve the other one.
    pub(super) proposer: &'static str,
}

/// One transition a gated handler might ask the gate to authorize.
#[derive(Clone, Copy, Debug)]
pub(super) struct Request {
    pub(super) action: BreakGlassAction,
    pub(super) target: &'static str,
    /// The proposals that may legitimately authorize this request, written out
    /// from KFC-9's rule rather than from the code under test. See the module
    /// doc.
    pub(super) covered_by: [bool; PROPOSALS],
}

/// One proposal, projected onto the fields a transition reads.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct ProposalState {
    /// The approving principals, in the order they approved.
    pub(super) approvals: Vec<&'static str>,
    /// `true` once an operator withdrew the proposal.
    pub(super) withdrawn: bool,
    /// `true` once a transition consumed the proposal.
    pub(super) consumed: bool,
}

/// Both proposals, the clock, and what the run has observed.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Universe {
    pub(super) proposals: Vec<ProposalState>,
    /// The logical clock, in milliseconds.
    pub(super) now_ms: i64,
    /// How many times each proposal was spent. `no_double_spend` reads it.
    pub(super) spends: Vec<u8>,
    /// `true` once a spend landed on a proposal that did not cover the
    /// request. The headline property reads it.
    pub(super) cross_spent: bool,
    /// `true` once a spend succeeded with too few distinct approvers.
    pub(super) under_approved: bool,
}

/// One step an operator or the controller can take.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Step {
    /// One principal approves one proposal.
    Approve(usize, &'static str),
    /// One principal withdraws one proposal.
    Withdraw(usize, &'static str),
    /// The clock advances by one millisecond.
    Expire,
    /// A gated transition tries to spend whatever covers one request.
    Consume(usize),
}
