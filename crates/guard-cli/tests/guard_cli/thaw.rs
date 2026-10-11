//! Clearing a freeze, which is the dangerous direction and the one the broker
//! guards hardest.
//!
//! Each case drives `freeze clear` at a refusal and asserts the exit code a
//! runbook branches on: a missing approval and a signature the broker will not
//! act on are told apart, and neither reaches the operator as a plain refusal.

use assert2::{assert, check};

use crate::support::{
    BAD_SIGNATURE, KEY_ID, NO_APPROVAL, PRINCIPAL, TOPIC, cli, cluster,
    frozen_cluster_with_stranger, set_signed_freeze, signed_as,
};

/// A thaw is the dangerous direction. A signed one that names no approved
/// proposal is refused with the code a runbook branches on, and not with a
/// generic refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_thaw_with_no_approval_reports_that_an_approval_is_missing() {
    let (_broker, _dir, bootstrap, key) = cluster().await;
    let signing = signed_as(&key, PRINCIPAL);

    assert!(set_signed_freeze(&bootstrap, &signing).await == 0);

    let nobody_approved = uuid::Uuid::new_v4().to_string();
    let mut clear = vec![
        "freeze",
        "clear",
        "--topic",
        TOPIC,
        "--proposal",
        &nobody_approved,
    ];
    clear.extend_from_slice(&signing);
    check!(cli(&bootstrap, &clear).await == NO_APPROVAL);

    // The freeze is still there, which is the point of the refusal.
    check!(cli(&bootstrap, &["freeze", "list", "--scope", TOPIC]).await == 0);
}

/// Every one of these is a signature the broker will not act on, and each has
/// to reach the operator as the signature exit code rather than as a plain
/// refusal, so a runbook sends them to their key material.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signature_the_broker_refuses_reports_a_signature_failure() {
    let (_broker, _dir, bootstrap, key, stranger) = frozen_cluster_with_stranger().await;

    let proposal = uuid::Uuid::new_v4().to_string();
    let clear = vec!["freeze", "clear", "--topic", TOPIC, "--proposal", &proposal];

    // A thaw with no signature at all. The broker needs one whatever
    // `freeze.require_signature` says, because a thaw is the dangerous
    // direction.
    check!(
        cli(&bootstrap, &clear).await == BAD_SIGNATURE,
        "an unsigned thaw"
    );

    // A key id the broker's trust set does not hold.
    let mut unknown_key = clear.clone();
    unknown_key.extend_from_slice(&[
        "--sign-with",
        stranger.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        "mallory-yubi",
        "--principal",
        PRINCIPAL,
    ]);
    check!(
        cli(&bootstrap, &unknown_key).await == BAD_SIGNATURE,
        "an unknown key id"
    );

    // A record that claims an author the signing key does not speak for.
    let mut wrong_author = clear.clone();
    wrong_author.extend_from_slice(&[
        "--sign-with",
        key.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        KEY_ID,
        "--principal",
        "User:mallory",
    ]);
    check!(
        cli(&bootstrap, &wrong_author).await == BAD_SIGNATURE,
        "an author the key does not speak for"
    );
}
