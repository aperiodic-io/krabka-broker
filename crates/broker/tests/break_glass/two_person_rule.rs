//! The rule itself: one complete loop, and the three refusals that keep it a
//! rule about distinct people.
//!
//! The loop case is the one no `PLAINTEXT` suite can write, because it needs
//! three principals on one cluster. The refusals sit beside it so the pair
//! reads together: a broker that honoured nothing and a broker that honoured
//! everything both fail here, and only one of the two fails anywhere else.

use assert2::{assert, check};
use krabka_broker::codes;
use krabka_protocol::krabka::break_glass::{
    ApproveBreakGlassResponse, BreakGlassApproval as WireApproval, DescribedBreakGlassProposal,
};

use crate::{
    cluster::boot,
    principals::{ALICE, BOB, CAROL, MALLORY, principal},
    proposals::{ACTION_DELETE_TOPIC, TTL_CONFIGURED, approve, open, propose, stored},
    support,
    topics::{delete_topic, topic_exists},
};

/// The whole two-person loop: Alice opens a proposal, Bob and Carol approve it,
/// and the transition it authorizes then runs.
///
/// This is the case no other suite in the workspace can write. Every existing
/// break-glass test over the wire ends at a refusal, because one `PLAINTEXT`
/// listener has one principal. Delete this and the feature keeps every refusal
/// it promises and loses the only proof that an approval is ever honoured: a
/// gate that refused a fully approved proposal, or an approve handler that
/// stored nothing, would pass the rest of this file untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proposal_two_approvals_and_the_transition_they_authorize() {
    crate::cluster::client_fixture!(cluster = boot();
        alice => ALICE, bob => BOB, carol => CAROL; topic(alice, "doomed", 1));

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;

    // Each approval reports the running count, so an operator knows how many
    // more people the transition still needs.
    check!(
        approve(&bob, id).await
            == ApproveBreakGlassResponse {
                approvals_held: 1,
                approvals_required: 2,
                ..ApproveBreakGlassResponse::default()
            },
        "the first approval"
    );
    check!(
        approve(&carol, id).await
            == ApproveBreakGlassResponse {
                approvals_held: 2,
                approvals_required: 2,
                ..ApproveBreakGlassResponse::default()
            },
        "the second approval"
    );

    // The record names both people, in the order they agreed, and neither
    // approval carries a signature because `delete_topic` is not in this
    // broker's `signed_actions`.
    let ready = stored(&alice, id).await;
    assert!(ready.approvals.len() == 2, "{ready:?}");
    check!(
        ready.approvals
            == vec![
                WireApproval {
                    principal: principal(BOB),
                    approved_at_ms: ready.approvals[0].approved_at_ms,
                    ..WireApproval::default()
                },
                WireApproval {
                    principal: principal(CAROL),
                    approved_at_ms: ready.approvals[1].approved_at_ms,
                    ..WireApproval::default()
                },
            ]
    );
    check!(ready.proposer == principal(ALICE));
    check!(ready.consumed_at_ms == 0, "nothing has spent it yet");

    check!(delete_topic(&alice, "doomed").await == codes::NONE);
    check!(!topic_exists(&alice, "doomed").await, "the topic is gone");

    // Spending the approval stamps `consumed_at_ms` and changes nothing else.
    // That single-field difference is what makes the proposal unusable twice.
    let spent = stored(&alice, id).await;
    check!(spent.consumed_at_ms != 0);
    check!(
        spent
            == DescribedBreakGlassProposal {
                consumed_at_ms: spent.consumed_at_ms,
                ..ready
            }
    );

    cluster.broker.shutdown().await;
}

async fn proposed_delete_fixture() -> (
    crate::cluster::Cluster,
    krabka_client_core::Client,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid,
) {
    crate::cluster::client_fixture!(cluster = boot();
        alice => ALICE, bob => BOB; topic(alice, "doomed", 1));
    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    (cluster, alice, bob, id)
}

/// A second approval from one principal is not a second person.
///
/// `required_approvals = 2` is a rule about people, not about rows. A handler
/// that appended Bob's approval twice would satisfy the count while one
/// credential authorized a data-losing transition on its own, which is the
/// exact failure the feature exists to stop. The refusal is asserted, and so is
/// the consequence: the transition still refuses afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_principal_cannot_stand_in_for_two() {
    let (cluster, alice, bob, id) = proposed_delete_fixture().await;
    check!(approve(&bob, id).await.error_code == codes::NONE);

    let again = approve(&bob, id).await;
    check!(
        again.error_code == codes::BREAK_GLASS_DUPLICATE_APPROVER,
        "{again:?}"
    );
    check!(again.approvals_held == 1, "the second try added nothing");
    check!(
        again
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&principal(BOB))),
        "{again:?}"
    );

    check!(stored(&alice, id).await.approvals.len() == 1);
    check!(
        delete_topic(&alice, "doomed").await == codes::POLICY_VIOLATION,
        "one approval is not two"
    );
    check!(topic_exists(&alice, "doomed").await, "the topic survives");

    cluster.broker.shutdown().await;
}

/// The proposer may not approve their own proposal.
///
/// Without this check the rule is a two-click rule: one person opens a proposal
/// and approves it, and only one more click stands between one credential and
/// an unclean election. The broker answers `BREAK_GLASS_DUPLICATE_APPROVER`
/// (1007), because the proposer already counts as one of the two people.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proposer_cannot_approve_their_own_proposal() {
    crate::cluster::client_fixture!(cluster = boot();
        alice => ALICE; topic(alice, "doomed", 1));

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    let refused = approve(&alice, id).await;

    check!(
        refused.error_code == codes::BREAK_GLASS_DUPLICATE_APPROVER,
        "{refused:?}"
    );
    check!(refused.approvals_held == 0);
    check!(stored(&alice, id).await.approvals.is_empty());

    cluster.broker.shutdown().await;
}

/// A principal outside `break_glass.approvers` is refused on both ends of the
/// workflow.
///
/// The approver set comes from `broker.toml` and not from the metadata log, so
/// an attacker who can write the log still cannot add themselves to it. That
/// only holds if the broker actually consults the set. Mallory holds a valid
/// SASL credential and every Kafka right the cluster asks for, and the broker
/// refuses her `BREAK_GLASS_NOT_AN_APPROVER` (1008) whether she proposes or
/// approves. The proposer check matters as much as the approver one: a
/// proposer outside the set turns a rule about three people into a rule about
/// two people and a stranger.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_principal_outside_the_approver_set_is_refused() {
    crate::cluster::client_fixture!(cluster = boot();
        alice => ALICE, mallory => MALLORY; topic(alice, "doomed", 1));

    let opened = propose(&mallory, ACTION_DELETE_TOPIC, "doomed", TTL_CONFIGURED).await;
    check!(
        opened.error_code == codes::BREAK_GLASS_NOT_AN_APPROVER,
        "{opened:?}"
    );

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;
    let refused = approve(&mallory, id).await;
    check!(
        refused.error_code == codes::BREAK_GLASS_NOT_AN_APPROVER,
        "{refused:?}"
    );
    check!(
        refused
            .error_message
            .as_deref()
            .is_some_and(|message| message.contains(&principal(MALLORY))),
        "{refused:?}"
    );
    check!(stored(&alice, id).await.approvals.is_empty());

    cluster.broker.shutdown().await;
}

/// An approval joins back to the login that made it.
///
/// The `PrivilegedAction` row says `User:bob` approved from an address. On its
/// own that is a claim about a name; what makes it evidence is the
/// `Authentication` row from the same session, which says that address
/// presented Bob's credential to this broker. The two rows join by
/// `principal.name` and `src_endpoint`, so this case reads the approval back
/// off the audit topic and then finds its login.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_approval_joins_to_the_login_that_made_it() {
    let (_cluster, alice, bob, id) = proposed_delete_fixture().await;
    check!(approve(&bob, id).await.approvals_held == 1);

    let records = support::wait_for_audit_record(&alice, "delete_topic.approved", |j| {
        j["class_uid"] == 6003 && j["api"]["operation"] == "delete_topic.approved"
    })
    .await;
    let approval = records
        .iter()
        .find(|j| j["class_uid"] == 6003 && j["api"]["operation"] == "delete_topic.approved")
        .expect("the approve record just waited for");
    check!(approval["actor"]["user"]["name"] == principal(BOB).as_str());

    // The login the approval rests on: same person, same peer, and it
    // succeeded.
    let login = records.iter().find(|j| {
        j["class_uid"] == 3002
            && j["status_id"] == 1
            && j["actor"]["user"]["name"] == approval["actor"]["user"]["name"]
            && j["src_endpoint"] == approval["src_endpoint"]
    });
    assert!(
        login.is_some(),
        "no Authentication row joins to {approval:?}; rows were {records:?}"
    );
}
