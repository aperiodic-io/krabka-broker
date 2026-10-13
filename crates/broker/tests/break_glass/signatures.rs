//! The detached operator signature `break_glass.signed_actions` demands on
//! every approval of a named action.
//!
//! Three outcomes have to stay apart — a missing signature, one that does not
//! verify, and one the broker accepts and keeps — and the case pins all three
//! against a broker booted with `delete_topic` in that list.

use {
    crate::{
        cluster::boot_with_signed_actions,
        principals::{ALICE, BOB, CAROL},
        proposals::{
            ACTION_DELETE_TOPIC, approval_signing_bytes, approve, approve_signed, approve_with,
            open, stored,
        },
        topics::delete_topic,
    },
    assert2::{assert, check},
    krabka_broker::codes,
};

/// An action in `break_glass.signed_actions` takes a detached operator
/// signature on every approval.
///
/// The name on an approval is the broker's word for it, and the whole point of
/// the signature is that the record stays provable without trusting the broker
/// that minted it. Three outcomes have to stay apart, and this case pins all
/// three: no signature is `OPERATOR_SIGNATURE_REQUIRED` (1010), a signature
/// that does not verify is `OPERATOR_SIGNATURE_INVALID` (1009), and a good one
/// is accepted and stored. The bytes are built in this file from the layout
/// KFC-9 documents, so the pass also proves the broker rebuilds the same bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_action_takes_a_signature_on_every_approval() {
    crate::cluster::client_fixture!(cluster = boot_with_signed_actions(&["delete_topic"]);
        alice => ALICE, bob => BOB, carol => CAROL; topic(alice, "doomed", 1));

    let id = open(&alice, ACTION_DELETE_TOPIC, "doomed").await;

    let unsigned = approve(&bob, id).await;
    check!(
        unsigned.error_code == codes::OPERATOR_SIGNATURE_REQUIRED,
        "{unsigned:?}"
    );

    // One flipped bit anywhere in the signature, presented under a real key id
    // by the principal that key is bound to.
    let key = cluster.key(BOB);
    let mut tampered = key
        .pair()
        .sign(&approval_signing_bytes(&stored(&bob, id).await))
        .as_ref()
        .to_vec();
    tampered[0] ^= 0x01;
    let bad = approve_with(&bob, id, &key.key_id, tampered).await;
    check!(
        bad.error_code == codes::OPERATOR_SIGNATURE_INVALID,
        "{bad:?}"
    );

    check!(
        stored(&alice, id).await.approvals.is_empty(),
        "neither refusal stored an approval"
    );

    let first = approve_signed(&bob, id, cluster.key(BOB)).await;
    check!(first.error_code == codes::NONE, "{first:?}");
    let second = approve_signed(&carol, id, cluster.key(CAROL)).await;
    check!(second.error_code == codes::NONE, "{second:?}");

    // The broker keeps what it verified, so an auditor can re-verify it later
    // against the operator public keys alone.
    let ready = stored(&alice, id).await;
    assert!(ready.approvals.len() == 2, "{ready:?}");
    check!(ready.approvals[0].key_id == cluster.key(BOB).key_id);
    check!(ready.approvals[1].key_id == cluster.key(CAROL).key_id);
    check!(ready.approvals.iter().all(|a| !a.signature.is_empty()));

    check!(delete_topic(&alice, "doomed").await == codes::NONE);

    cluster.broker.shutdown().await;
}
