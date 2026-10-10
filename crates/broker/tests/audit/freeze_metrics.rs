//! The KFC-9 metric families, watched across the requests that move them.
//!
//! A metric that is defined but never fed passes every unit test, so the
//! registry gauge, the proposal-state gauge and the refusal counter are each
//! observed here over a real freeze, a real approval and a real refusal, on
//! the same cluster the audit cases use.

use assert2::check;
use krabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};

use crate::{
    freeze_workflow::{
        FREEZE_REASON, FROZEN_TARGET, PROPOSE_REASON, SignedFreeze, THAW_REASON,
        boot_workflow_cluster, now_ms, propose_thaw, send_signed_freeze, settle_proposal,
    },
    support,
    support::topics::{creatable_topic, create_topic_request},
};

/// Verifies that the KFC-9 metric families move on a real request.
///
/// A metric that is defined but never fed passes every unit test, and the
/// KFC-7 suite found exactly that gap the hard way. These four series are the
/// ones an operator alerts on, so each is watched across the request that is
/// supposed to move it: the registry gauge up on the freeze and back down on
/// the thaw, and a proposal walking `pending` → `approved` → `consumed`.
///
/// The refusal counter is the fourth, and it needs a Kafka transition rather
/// than a private one: `DeleteTopics` on a gated broker with no approval is
/// refused with `POLICY_VIOLATION`, bumps `break_glass_refusals`, and writes
/// its own refused row to the audit topic. Counting the refusal and auditing
/// it are two different promises, and this asserts both of them at once.
#[tokio::test]
async fn the_kfc9_gauges_and_counters_move_on_real_requests() {
    use krabka_broker::metrics::{
        BreakGlassAction as ActionLabel, BreakGlassActionLabel, BreakGlassState,
        BreakGlassStateLabel, BrokerMetrics,
    };

    fn proposals(metrics: &BrokerMetrics, state: BreakGlassState) -> i64 {
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel { state })
            .get()
    }
    fn refusals(metrics: &BrokerMetrics, action: krabka_metadata::BreakGlassAction) -> u64 {
        metrics
            .break_glass_refusals
            .get_or_create(&BreakGlassActionLabel {
                action: ActionLabel(action),
            })
            .get()
    }

    let cluster = boot_workflow_cluster().await;
    let (alice, bob, carol, cluster_id) = cluster.workflow_clients().await;
    check!(cluster.broker.metrics().topic_freezes_active.get() == 0);

    let set_at_ms = now_ms();
    let freeze = SignedFreeze {
        key: &cluster.alice_key,
        cluster_id: &cluster_id,
        frozen: true,
        reason: FREEZE_REASON,
        proposal_id: uuid::Uuid::nil(),
        set_at_ms,
    };
    check!(send_signed_freeze(&alice, &freeze).await.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the registry gauge counts the freeze", |m| {
            m.topic_freezes_active.get() == 1
        })
        .await;

    let proposed = propose_thaw(&alice, FROZEN_TARGET, PROPOSE_REASON).await;
    check!(proposed.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the proposal counts as pending", |m| {
            proposals(m, BreakGlassState::Pending) == 1
        })
        .await;

    for client in [&bob, &carol] {
        check!(
            settle_proposal(client, proposed.proposal_id, false)
                .await
                .error_code
                == 0
        );
    }
    cluster
        .broker
        .wait_for_metrics("two approvals move it to approved", |m| {
            proposals(m, BreakGlassState::Approved) == 1
                && proposals(m, BreakGlassState::Pending) == 0
        })
        .await;

    let thaw = SignedFreeze {
        frozen: false,
        reason: THAW_REASON,
        proposal_id: uuid::Uuid::from_bytes(proposed.proposal_id.0),
        set_at_ms: now_ms().max(set_at_ms + 1),
        ..freeze
    };
    check!(send_signed_freeze(&alice, &thaw).await.error_code == 0);
    cluster
        .broker
        .wait_for_metrics("the thaw drops the gauge and spends the proposal", |m| {
            m.topic_freezes_active.get() == 0 && proposals(m, BreakGlassState::Consumed) == 1
        })
        .await;

    // A gated Kafka transition with no approval behind it.
    let created = alice
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("doomed").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    check!(created.topics[0].error_code == 0);

    let before = refusals(
        cluster.broker.metrics(),
        krabka_metadata::BreakGlassAction::DeleteTopic,
    );
    let deleted = alice
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some("doomed".into()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    check!(deleted.responses[0].error_code == krabka_broker::codes::POLICY_VIOLATION);
    check!(
        refusals(
            cluster.broker.metrics(),
            krabka_metadata::BreakGlassAction::DeleteTopic
        ) == before + 1,
        "the refusal counter did not move"
    );
    support::wait_for_audit_record(&alice, "the refused deletion", |j| {
        j["api"]["operation"] == "delete_topic.refused" && j["status_id"] == 2
    })
    .await;

    cluster.broker.shutdown().await;
}
