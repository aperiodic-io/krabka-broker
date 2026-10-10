//! Exhaustive stateright enumeration of two proposals that exist at once.
//!
//! [`state_model`](super::state_model) drives one proposal through its whole
//! lifecycle. It cannot see the rule that matters once a cluster holds more
//! than one open proposal: **an approval authorizes the transition it names,
//! and no other.** A model with a single proposal satisfies that rule
//! vacuously, because every consume that succeeds spends the only candidate
//! there is.
//!
//! So this model puts two proposals in one
//! [`MetadataImage`](krabka_metadata::MetadataImage) and lets every
//! interleaving of approve, withdraw, expire and consume run against them. As
//! in the sibling model the transitions call the real decision code --
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) and
//! [`gate::authorize`](crate::break_glass::gate::authorize) -- so a rule the
//! model checks is the rule the broker runs.
//!
//! # The oracle is written by hand
//!
//! Each [`Request`](universe::Request) carries `covered_by`, the set of
//! proposals that may legitimately authorize it. Those flags are written out
//! from KFC-9's target-matching rule rather than computed by calling the
//! broker's own `covers`. Deriving them from the code under test would make
//! the headline property a tautology: the model would only prove that
//! `covers` agrees with itself.
//!
//! # Properties
//!
//! - `no_cross_spend`: no interleaving spends a proposal on a request that
//!   proposal does not cover. This is what one proposal cannot test.
//! - `no_double_spend`: each proposal is spent at most once, now stated per
//!   proposal rather than over a single counter.
//! - `no_under_approved`: no spend of a proposal that fewer than
//!   `required_approvals` distinct principals approved.
//! - `a_withdrawn_proposal_is_never_consumed`, per proposal.
//!
//! # What these properties do and do not carry
//!
//! `no_cross_spend` and `no_double_spend` are load-bearing here, and that was
//! checked rather than assumed: dropping the `action_targets_partition` limit
//! from `gate::covers` fails `a_topic_that_looks_like_a_partition_of_another`
//! and only that scenario, and dropping the consumed check from
//! `unusable_because` fails all three.
//!
//! `no_under_approved` is carried as a guard rather than independently
//! exercised. Replacing `distinct_approvers` with `approvals.len()` in the gate
//! does not fail this model, because
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) refuses a
//! second approval from a principal who already approved, so the two
//! expressions agree on every state this model can reach. The property still
//! earns its place: it would fail the day a change to `decide` let a duplicate
//! through. Making it load-bearing here would mean injecting an approval list
//! the handler cannot produce, which is a different model than this one.
//!
//! The clock is a small integer of logical milliseconds, bounded exactly as the
//! sibling model bounds it, so the state graph stays exhaustive.

use krabka_metadata::BreakGlassAction;

use self::{
    transitions::CrossSpendModel,
    universe::{EXPIRES_AT, ProposalSpec, Request},
};
use super::state_model::config;

mod properties;
mod transitions;
mod universe;

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 12;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_TWO_TOPICS: usize = 432;
const PINNED_UNIQUE_STATES_PARTITION_LOOKALIKE: usize = 432;
const PINNED_UNIQUE_STATES_BOTH_COVER: usize = 432;

fn run(model: CrossSpendModel, label: &str, pinned_unique_states: usize) {
    crate::model_check::check_model(
        model,
        label,
        (MAX_DEPTH, TARGET_STATE_COUNT, MAX_UNIQUE_STATES),
        pinned_unique_states,
    );
}

/// The approver set both scenarios use. `User:alice` and `User:bob` each open
/// one proposal, so each is barred from their own and free on the other.
const APPROVERS: [&str; 3] = ["User:alice", "User:bob", "User:carol"];

/// Two proposals for the same action on different topics.
///
/// This is the plain cross-spend shape: an approved proposal to delete
/// `orders` must never authorize deleting `payments`, however the approvals
/// interleave.
#[test]
fn one_action_on_two_topics() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2, EXPIRES_AT),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteTopic,
                    target: "orders",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteTopic,
                    target: "payments",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "orders",
                    covered_by: [true, false],
                },
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "payments",
                    covered_by: [false, true],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "one_action_on_two_topics",
        PINNED_UNIQUE_STATES_TWO_TOPICS,
    );
}

/// A topic whose name reads as a partition of the other.
///
/// `DeleteTopic` does not name a partition, so a proposal on `logs` must not
/// reach the topic `logs-2024` -- the hazard `gate::covers` limits the wider
/// rule to avoid. Modelling it here means an interleaving, not one unit case,
/// has to respect the limit.
#[test]
fn a_topic_that_looks_like_a_partition_of_another() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2, EXPIRES_AT),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs-2024",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs",
                    covered_by: [true, false],
                },
                Request {
                    action: BreakGlassAction::DeleteTopic,
                    target: "logs-2024",
                    covered_by: [false, true],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "a_topic_that_looks_like_a_partition_of_another",
        PINNED_UNIQUE_STATES_PARTITION_LOOKALIKE,
    );
}

/// Two proposals that both cover the same request.
///
/// `DeleteRecords` names a partition, so a proposal on the bare topic `logs`
/// covers `logs-7`, and so does a proposal on `logs-7` itself. Both are
/// legitimate candidates, which is the case `better_candidate` exists to
/// settle. What must hold is that one consume spends exactly one of them, and
/// that neither is spent twice across the whole interleaving.
#[test]
fn two_proposals_that_both_cover_one_partition() {
    run(
        CrossSpendModel {
            config: config(&APPROVERS, 2, EXPIRES_AT),
            proposals: [
                ProposalSpec {
                    id: 1,
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs",
                    proposer: "User:alice",
                },
                ProposalSpec {
                    id: 2,
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-7",
                    proposer: "User:bob",
                },
            ],
            requests: vec![
                Request {
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-7",
                    covered_by: [true, true],
                },
                // Only the bare-topic proposal reaches a different partition.
                Request {
                    action: BreakGlassAction::DeleteRecords,
                    target: "logs-3",
                    covered_by: [true, false],
                },
            ],
            principals: APPROVERS.to_vec(),
        },
        "two_proposals_that_both_cover_one_partition",
        PINNED_UNIQUE_STATES_BOTH_COVER,
    );
}
