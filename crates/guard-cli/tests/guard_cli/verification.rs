//! Proving the registry from this machine with `freeze list
//! --verify-signatures`.
//!
//! The claim the flag makes is that the local key material proved the entry, so
//! a trust set that cannot prove one has to report that rather than pass it,
//! and the two ways it can fail carry different exit codes.

use {
    crate::support::{BAD_SIGNATURE, frozen_cluster_with_stranger, verify_registry},
    assert2::check,
};

/// `freeze list --verify-signatures` says the registry is authentic from this
/// machine, so a trust set that cannot prove an entry has to say so rather than
/// pass the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registry_the_local_keys_cannot_prove_does_not_pass() {
    let (_broker, _dir, bootstrap, _key, stranger) = frozen_cluster_with_stranger().await;

    // The stranger's trust file names another key id, so the entry cannot be
    // checked here at all. That is the mismatch code, not the signature code:
    // the tool could not check, rather than checked and found it wrong.
    check!(
        verify_registry(
            &bootstrap,
            stranger.trust_file.to_str().expect("utf-8 path")
        )
        .await
            == krabka_guard::EXIT_MISMATCH
    );

    // A trust file that is not there stops the verify before it starts.
    check!(verify_registry(&bootstrap, "/nonexistent/keys.toml").await == BAD_SIGNATURE);
}
