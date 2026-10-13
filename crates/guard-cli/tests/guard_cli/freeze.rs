//! Placing a freeze and reading the registry back, which is the safe direction.
//!
//! It covers the whole `freeze set` and `freeze list` loop, signed and
//! unsigned, and the one scope the broker will not accept at all.

use {
    crate::support::{
        PREFIX, PRINCIPAL, REFUSED, TOPIC, cli, cluster, set_signed_freeze, signed_as,
        verify_registry,
    },
    assert2::check,
};

/// The whole freeze loop: sign a freeze here, send it, read the registry back,
/// and prove the entry against the operator public key on this machine.
///
/// The verify is the strongest statement the tool makes. A zero exit says that
/// the signature the broker stored was made by this key, over this scope, in
/// this cluster, and that no broker was trusted to say so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_freeze_loop_runs_end_to_end() {
    let (_broker, _dir, bootstrap, key) = cluster().await;
    let signing = signed_as(&key, PRINCIPAL);

    check!(
        set_signed_freeze(&bootstrap, &signing).await == 0,
        "a signed freeze"
    );

    check!(cli(&bootstrap, &["freeze", "list"]).await == 0, "list");
    check!(
        cli(&bootstrap, &["freeze", "list", "--scope", TOPIC]).await == 0,
        "list one scope"
    );
    check!(
        verify_registry(&bootstrap, key.trust_file.to_str().expect("utf-8 path")).await == 0,
        "the entry verifies against the local operator key"
    );

    // A prefix freeze needs no signature, because a freeze is the safe
    // direction and an incident can start on a cluster with no key material.
    check!(
        cli(&bootstrap, &[
            "freeze",
            "set",
            "--prefix",
            PREFIX,
            "--reason",
            "tenant offboarding",
        ],)
        .await
            == 0,
        "an unsigned prefix freeze"
    );
    // The registry now holds a proved entry and an attested one. An unsigned
    // entry is reported as unsigned rather than counted as a failure, because
    // that mixture is what `freeze.require_signature = false` allows.
    check!(
        verify_registry(&bootstrap, key.trust_file.to_str().expect("utf-8 path")).await == 0,
        "a mixed registry still verifies"
    );
}

/// A scope that reaches an internal topic would take the cluster down, so the
/// broker refuses it. It is an ordinary refusal, and not one of the two codes a
/// runbook branches on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_that_reaches_an_internal_topic_is_an_ordinary_refusal() {
    let (_broker, _dir, bootstrap, _key) = cluster().await;

    check!(
        cli(&bootstrap, &[
            "freeze", "set", "--prefix", "__", "--reason", "no"
        ],)
        .await
            == REFUSED
    );
}
