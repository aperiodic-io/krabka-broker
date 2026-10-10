//! Lifting a freeze, which takes two people and a signature.
//!
//! The reverse direction is its own case because a freeze that could not be
//! lifted would be a broken cluster rather than a safe one, and no refusal case
//! can tell a working thaw from a registry entry that nothing removes. It runs
//! over SASL because the two-person rule needs three distinct principals, and
//! every connection on a plaintext listener is the same anonymous one.

use assert2::{assert, check};
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::{
        break_glass::{ApproveBreakGlassRequest, ProposeBreakGlassRequest},
        freeze::PATTERN_TYPE_LITERAL,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    control_plane::{cluster_id, freeze_scope, set_freeze, wait_for_registry_len},
    signing::{SignedFreeze, signed_request},
    support,
    wire::{CONTROL, accepted, create_topic, now_ms, refused},
};

/// The `ThawTopicFreeze` break-glass action, on the wire.
const ACTION_THAW: i8 = 1;

/// Open a break-glass proposal to thaw `target`, and return its id.
async fn propose_thaw(client: &Client, target: &str) -> uuid::Uuid {
    let response = client
        .send(ProposeBreakGlassRequest {
            action: ACTION_THAW,
            target: target.to_owned(),
            reason: "the cutover finished".to_owned(),
            // Zero asks for `break_glass.proposal_ttl`.
            ttl_ms: 0,
            ..ProposeBreakGlassRequest::default()
        })
        .await
        .expect("ProposeBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ProposeBreakGlass: {response:?}"
    );
    uuid::Uuid::from_bytes(response.proposal_id.0)
}

/// Add one approval, and return how many the proposal now holds.
async fn approve(client: &Client, proposal_id: uuid::Uuid) -> i32 {
    let response = client
        .send(ApproveBreakGlassRequest {
            proposal_id: WireUuid(*proposal_id.as_bytes()),
            ..ApproveBreakGlassRequest::default()
        })
        .await
        .expect("ApproveBreakGlass");
    assert!(
        response.error_code == codes::NONE,
        "ApproveBreakGlass: {response:?}"
    );
    response.approvals_held
}

/// A thaw lifts the freeze and the topic takes writes again.
///
/// The reverse direction has to be proved as its own case, because a freeze
/// that could not be lifted would be a broken cluster rather than a safe one,
/// and none of the refusal cases in this suite can tell a working thaw from a
/// registry entry that nothing removes. It runs over SASL because the
/// two-person rule needs three distinct principals -- a proposer who may not
/// approve, and two approvers -- and every connection on a plaintext listener
/// is the same anonymous principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thaw_restores_writes() {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    let key = support::mint_operator_key(keys.path(), "alice-yubi", "User:alice");
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        logs.path(),
        crate::support::OperatorSaslSetup {
            trust: crate::support::OperatorKeysSetup { keys: &[&key], approvers: // The proposer has to be in the set as well: a proposer outside it
        // would turn a rule about three people into a rule about two people
        // and a stranger. Alice still may not approve her own proposal.
        &["User:alice", "User:bob", "User:carol"] },
            users: &[("alice", "pw"), ("bob", "pw"), ("carol", "pw")],
        },
    )
    .await;
    let alice = support::sasl_client(&bootstrap, "alice", "pw").await;
    let frozen = create_topic(&broker, &alice, "orders").await;
    let control = create_topic(&broker, &alice, CONTROL).await;
    crate::wire::check_produce!(&broker, &alice, "orders", frozen => accepted(1));

    freeze_scope(&alice, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    crate::wire::check_produce!(&broker, &alice, "orders", frozen => refused("literal", "orders", "cutover", 1));

    // Alice proposes and may not approve her own proposal, so the two
    // approvals come from two other people.
    let proposal_id = propose_thaw(&alice, "literal:orders").await;
    let bob = support::sasl_client(&bootstrap, "bob", "pw").await;
    let carol = support::sasl_client(&bootstrap, "carol", "pw").await;
    check!(approve(&bob, proposal_id).await == 1);
    check!(approve(&carol, proposal_id).await == 2);

    let thaw = set_freeze(
        &alice,
        signed_request(&SignedFreeze {
            key: &key,
            cluster_id: &cluster_id(&alice).await,
            pattern_type: PATTERN_TYPE_LITERAL,
            scope: "orders",
            frozen: false,
            reason: "the cutover finished",
            set_at_ms: now_ms(),
            proposal_id,
        }),
    )
    .await;
    check!(thaw.error_code == codes::NONE, "thaw: {thaw:?}");
    wait_for_registry_len(&alice, 0).await;

    crate::wire::check_produce!(&broker, &alice, "orders", frozen => accepted(2));
    crate::wire::check_produce!(&broker, &alice, CONTROL, control => accepted(1));
    broker.shutdown().await;
}
