//! Every gated transition, refused with no proposal and run with one.
//!
//! The five handlers each carry their own gate call, target spelling and
//! refusal counter, so the table drives all five rather than trusting one to
//! stand for the rest. Two of the five complete on a one-broker cluster; the
//! other three answer a question about the partition's own state once the gate
//! opens, and that change of answer is the proof it opened.

use {
    crate::{
        cluster::{Cluster, boot},
        principals::ALICE,
        proposals::{
            ACTION_CANCEL_REASSIGNMENT, ACTION_DELETE_RECORDS, ACTION_DELETE_TOPIC,
            ACTION_UNCLEAN_ELECT_LEADERS, ACTION_UNREGISTER_BROKER, approved_proposal, stored,
        },
        topics::create_topic,
        transitions::{Transition, refusals},
    },
    assert2::check,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_metadata::BreakGlassAction as GatedAction,
};

/// One row of the gated-transition table.
struct GatedCase {
    /// What the row is about. First, so a failure names it.
    label: &'static str,
    /// The transition, as a request makes it.
    transition: Transition,
    /// The `action` wire value a proposal must carry to authorize it.
    action: i8,
    /// The same action, as the metric families label it.
    metric: GatedAction,
    /// What the broker answers once an approved proposal covers the request.
    ///
    /// Two of the five complete on a one-broker cluster. The other three then
    /// answer a question about the partition's own state instead — there is no
    /// reassignment to cancel and no election to hold — and that change of
    /// answer is itself the proof that the authority gate opened. The gate runs
    /// before any of those state questions, exactly so that "does this need an
    /// approval" cannot depend on state a concurrent request can move.
    approved: i16,
}

/// Every transition the two-person rule gates through a Kafka API.
///
/// The unregistration is last on purpose: it drops the one broker registration
/// this cluster has, and a later row would then find no broker to place a
/// replica on.
const GATED: [GatedCase; 5] = [
    GatedCase {
        label: "an unclean leader election",
        transition: Transition::UncleanElection,
        action: ACTION_UNCLEAN_ELECT_LEADERS,
        metric: GatedAction::UncleanElectLeaders,
        approved: codes::ELECTION_NOT_NEEDED,
    },
    GatedCase {
        label: "a reassignment cancel",
        transition: Transition::CancelReassignment,
        action: ACTION_CANCEL_REASSIGNMENT,
        metric: GatedAction::CancelReassignment,
        approved: codes::NO_REASSIGNMENT_IN_PROGRESS,
    },
    GatedCase {
        label: "a record trim",
        transition: Transition::TrimRecords,
        action: ACTION_DELETE_RECORDS,
        metric: GatedAction::DeleteRecords,
        approved: codes::NONE,
    },
    GatedCase {
        label: "a topic deletion",
        transition: Transition::DeleteTopic,
        action: ACTION_DELETE_TOPIC,
        metric: GatedAction::DeleteTopic,
        approved: codes::NONE,
    },
    GatedCase {
        label: "a broker unregistration",
        transition: Transition::Unregister,
        action: ACTION_UNREGISTER_BROKER,
        metric: GatedAction::UnregisterBroker,
        approved: codes::NONE,
    },
];

/// Run one row: refuse it with no proposal, then run it with one.
async fn run_gated_case(cluster: &Cluster, alice: &Client, case: &GatedCase, topic: &str) {
    let label = case.label;
    let before = refusals(&cluster.broker, case.metric);
    check!(
        case.transition.run(alice, topic).await == codes::POLICY_VIOLATION,
        "case {label}: refused while no proposal covers it"
    );
    check!(
        refusals(&cluster.broker, case.metric) == before + 1,
        "case {label}: the refusal is counted"
    );

    let id = approved_proposal(cluster, case.action, &case.transition.target(topic)).await;
    check!(
        case.transition.run(alice, topic).await == case.approved,
        "case {label}: the approval opened the gate"
    );

    // The approval is spent exactly when the transition ran. A request that
    // gets past the gate and then fails on the partition's own state leaves the
    // proposal usable, so an operator does not have to gather two signatures
    // again over a typo.
    check!(
        (stored(alice, id).await.consumed_at_ms != 0) == (case.approved == codes::NONE),
        "case {label}: an approval is spent exactly when the transition ran"
    );
}

/// Every gated transition refuses without an approved proposal and runs with
/// one, and each refusal reaches the caller as `POLICY_VIOLATION` (44).
///
/// The five handlers each carry their own copy of the gate call, their own
/// target spelling, and their own refusal row, so a gate that was dropped from
/// one of them would leave the other four green. Code 44 is load-bearing too:
/// the design chose an existing Kafka code precisely so a JVM client classifies
/// the refusal as non-retriable, and an unassigned code would map to
/// `UNKNOWN_SERVER_ERROR` and be retried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_gated_transition_refuses_without_a_proposal_and_runs_with_one() {
    let cluster = boot().await;
    let alice = cluster.client(ALICE).await;

    for (index, case) in GATED.iter().enumerate() {
        let topic = format!("gated-{index}");
        create_topic(&alice, &topic, 1).await;
        run_gated_case(&cluster, &alice, case, &topic).await;
    }

    cluster.broker.shutdown().await;
}
