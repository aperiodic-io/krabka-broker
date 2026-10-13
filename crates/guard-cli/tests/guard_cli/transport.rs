//! A broker that was never reached, across every subcommand that writes or
//! reads.
//!
//! Nothing is known about such a request, so the exit code has to separate it
//! from a refusal on each of the four paths rather than on one of them.

use {
    crate::support::{TOPIC, UNREACHABLE, cli},
    assert2::check,
};

/// A broker that cannot be reached is not a refusal. Nothing is known about the
/// outcome, and the exit code has to say so, because a runbook that read this
/// as a refusal would assume the freeze did not land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreachable_bootstrap_reports_that_nothing_is_known() {
    // Port 1 is reserved and nothing listens on it.
    let nowhere = "127.0.0.1:1";

    check!(
        cli(nowhere, &[
            "freeze",
            "set",
            "--topic",
            TOPIC,
            "--reason",
            "DR cutover"
        ],)
        .await
            == UNREACHABLE,
        "freeze set"
    );
    check!(
        cli(nowhere, &["freeze", "list"]).await == UNREACHABLE,
        "freeze list"
    );
    check!(
        cli(nowhere, &[
            "break-glass",
            "propose",
            "--action",
            "delete-topic",
            "--target",
            "doomed",
            "--reason",
            "no",
        ],)
        .await
            == UNREACHABLE,
        "break-glass propose"
    );
    check!(
        cli(nowhere, &["break-glass", "list"]).await == UNREACHABLE,
        "break-glass list"
    );
}
