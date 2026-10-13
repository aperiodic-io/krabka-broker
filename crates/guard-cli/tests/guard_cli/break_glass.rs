//! The break-glass subcommand: proposing, approving, and withdrawing.
//!
//! The cases follow one proposal through its whole life over a single
//! listener, where the two-person rule can only ever be refused, and then check
//! that a proposal nobody opened is refused rather than reported as an empty
//! success.

use {
    crate::support::{KEY_ID, REFUSED, cli, cluster, only_proposal},
    assert2::check,
};

/// The break-glass loop: open a proposal, read it back, fail to approve it
/// alone, and withdraw it.
///
/// The self-approval refusal is the two-person rule working. One principal
/// cannot be both people, and the broker says so rather than counting the
/// proposer's own approval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_break_glass_loop_runs_end_to_end() {
    let (_broker, _dir, bootstrap, key) = cluster().await;

    check!(
        cli(&bootstrap, &[
            "break-glass",
            "propose",
            "--action",
            "delete-topic",
            "--target",
            "doomed",
            "--reason",
            "the topic holds test data only",
            "--ttl",
            "30m",
        ],)
        .await
            == 0,
        "propose"
    );
    check!(cli(&bootstrap, &["break-glass", "list"]).await == 0, "list");
    check!(
        cli(&bootstrap, &["break-glass", "list", "--pending"]).await == 0,
        "list pending"
    );

    let proposal = only_proposal(&bootstrap).await.to_string();
    check!(
        cli(&bootstrap, &[
            "break-glass",
            "approve",
            "--proposal",
            &proposal
        ],)
        .await
            == REFUSED,
        "the proposer cannot also approve"
    );
    // The same refusal holds with a signature, because the distinct-principal
    // rule is checked before the signature is worth anything.
    check!(
        cli(&bootstrap, &[
            "break-glass",
            "approve",
            "--proposal",
            &proposal,
            "--sign-with",
            key.pkcs8.to_str().expect("utf-8 path"),
            "--key-id",
            KEY_ID,
        ],)
        .await
            == REFUSED,
        "a signed self-approval is still a self-approval"
    );

    check!(
        cli(&bootstrap, &[
            "break-glass",
            "withdraw",
            "--proposal",
            &proposal
        ],)
        .await
            == 0,
        "the proposer may withdraw"
    );
    // A withdrawn proposal is spent. Nothing can approve it afterwards, which
    // is what makes a withdraw worth having.
    check!(
        cli(&bootstrap, &[
            "break-glass",
            "approve",
            "--proposal",
            &proposal
        ],)
        .await
            == REFUSED,
        "a withdrawn proposal cannot be approved"
    );
}

/// A proposal nobody opened is refused, and not reported as an empty success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proposal_that_does_not_exist_is_refused() {
    let (_broker, _dir, bootstrap, _key) = cluster().await;
    let absent = uuid::Uuid::new_v4().to_string();

    check!(
        cli(&bootstrap, &[
            "break-glass",
            "approve",
            "--proposal",
            &absent
        ],)
        .await
            == REFUSED,
        "approve"
    );
    check!(
        cli(&bootstrap, &[
            "break-glass",
            "withdraw",
            "--proposal",
            &absent
        ],)
        .await
            == REFUSED,
        "withdraw"
    );
    check!(
        cli(&bootstrap, &["break-glass", "list", "--proposal", &absent],).await == REFUSED,
        "a read that names one absent proposal is a refusal, not an empty list"
    );
    check!(
        cli(&bootstrap, &["break-glass", "list"]).await == 0,
        "a read of the whole empty registry is an empty success"
    );
}
