//! The tool driven end to end against a real broker.
//!
//! Every case calls [`krabka_barrier::run_from_args`] in process rather than
//! spawning the binary. A subprocess needs a Cargo working tree to build from
//! and a Bazel test sandbox has none, which is the same reason `krabka-format`
//! is a library as well as a binary.
//!
//! What these cover that the unit tests cannot: that each subcommand's request
//! actually reaches a broker that answers it, and that the exit code reports
//! what happened.

use {
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
};

/// The exit code for a request the broker refused.
const REFUSED: i32 = 1;
/// The exit code for a cut the log does not back.
const MISMATCH: i32 = 3;

const GROUP: &str = "orders-cut";
const TOPIC: &str = "orders";

/// Boot a single-node broker and return it with its bootstrap address.
///
/// `BrokerConfig::for_tests` asks for 50 `__barrier_state` partitions at
/// replication factor 3, which one node cannot host, so the partition a group
/// hashes to may never open and the coordinator answers
/// `COORDINATOR_NOT_AVAILABLE`. One partition at factor one is led here, which
/// makes every case below deterministic.
async fn broker() -> (BrokerHandle, tempfile::TempDir, String) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let config = BrokerConfig {
        barrier_state_num_partitions: 1,
        barrier_state_replication_factor: 1,
        ..BrokerConfig::for_tests(dir.path().to_path_buf())
    };
    let handle = Broker::start(config).await.expect("broker starts");
    let bootstrap = handle.listen_addr().to_string();
    wait_for_coordinator(&bootstrap).await;
    (handle, dir, bootstrap)
}

/// Block until the coordinator answers, so a case never races topic creation.
///
/// `describe` is the cheapest request that needs the state partition open.
async fn wait_for_coordinator(bootstrap: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if cli(bootstrap, &["describe"]).await == 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the barrier coordinator never became available"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Run the tool, returning its exit code.
async fn cli(bootstrap: &str, args: &[&str]) -> i32 {
    let mut line = vec!["krabka-barrier", "--bootstrap-server", bootstrap];
    line.extend_from_slice(args);
    krabka_barrier::run_from_args(line).await
}

/// Create the topic the barrier group cuts across.
async fn create_topic(bootstrap: &str, partitions: i32) {
    let mut admin =
        krabka_client_admin::AdminClient::connect(std::slice::from_ref(&bootstrap.to_owned()))
            .await
            .expect("admin connect");
    admin
        .create_topics(
            &[krabka_client_admin::CreateTopicSpec {
                name: TOPIC.to_string(),
                partitions,
                replicas: 1,
                configs: std::collections::BTreeMap::default(),
                replica_assignments: std::collections::BTreeMap::new(),
            }],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(10)),
        )
        .await
        .expect("create topic");
}

/// The whole operator loop: define a group, cut, read the cut back, and prove
/// the markers behind it are in the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_operator_loop_runs_end_to_end() {
    let (_broker, _dir, bootstrap) = broker().await;
    create_topic(&bootstrap, 3).await;

    check!(
        cli(&bootstrap, &[
            "define",
            "--group",
            GROUP,
            "--topic",
            TOPIC,
            "--retained-cuts",
            "4"
        ],)
        .await
            == 0,
        "define"
    );
    check!(
        cli(&bootstrap, &["describe", "--group", GROUP]).await == 0,
        "describe"
    );
    check!(
        cli(&bootstrap, &["trigger", "--group", GROUP]).await == 0,
        "trigger"
    );
    check!(
        cli(&bootstrap, &["list", "--group", GROUP]).await == 0,
        "list"
    );

    // Epoch 1 is the first cut a group takes: the broker spells "never
    // injected" as 0, so a real epoch is always positive. `verify` reads the
    // log at each offset the cut names, which makes a zero exit here the
    // strongest statement the tool makes -- the cut is not merely published,
    // it is backed.
    check!(
        cli(&bootstrap, &["verify", "--group", GROUP, "--epoch", "1"]).await == 0,
        "verify"
    );

    check!(
        cli(&bootstrap, &["delete", "--group", GROUP]).await == 0,
        "delete"
    );
}

/// A group nobody defined is refused, not reported as an empty success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_group_is_refused() {
    let (_broker, _dir, bootstrap) = broker().await;

    check!(
        cli(&bootstrap, &["trigger", "--group", "nope"]).await == REFUSED,
        "trigger"
    );
    check!(
        cli(&bootstrap, &["list", "--group", "nope"]).await == REFUSED,
        "list"
    );
    check!(
        cli(&bootstrap, &["verify", "--group", "nope", "--epoch", "0"]).await == REFUSED,
        "verify"
    );
}

/// An epoch the group never took cannot be verified. This is the case that
/// separates "no cut here" from "a cut whose markers are missing": the first is
/// a refusal, the second a mismatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_epoch_with_no_cut_is_refused_rather_than_mismatched() {
    let (_broker, _dir, bootstrap) = broker().await;
    create_topic(&bootstrap, 1).await;
    assert!(cli(&bootstrap, &["define", "--group", GROUP, "--topic", TOPIC]).await == 0);
    assert!(cli(&bootstrap, &["trigger", "--group", GROUP]).await == 0);

    check!(
        cli(&bootstrap, &["verify", "--group", GROUP, "--epoch", "1"]).await == 0,
        "the epoch that exists verifies"
    );
    check!(
        cli(&bootstrap, &["verify", "--group", GROUP, "--epoch", "99"]).await == REFUSED,
        "an epoch with no cut is a refusal, not a mismatch"
    );
    check!(
        cli(&bootstrap, &["verify", "--group", GROUP, "--epoch", "99"]).await != MISMATCH,
        "an epoch with no cut must not read as a cut the log contradicts"
    );
}

/// A group may name a topic that does not exist yet.
///
/// The coordinator freezes a group's target set from the metadata image at
/// each injection, not at definition, so a topic created later is picked up by
/// the next epoch. Refusing here would make a group undefinable before its
/// topics exist, and would buy nothing: the name still has to be re-resolved
/// every epoch because a topic can be deleted after the fact.
///
/// The cut over such a group names no partition for the absent topic, which is
/// how a reader sees the difference.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_may_name_a_topic_that_does_not_exist_yet() {
    let (_broker, _dir, bootstrap) = broker().await;

    check!(
        cli(&bootstrap, &[
            "define",
            "--group",
            GROUP,
            "--topic",
            "not-created-yet"
        ],)
        .await
            == 0,
        "define accepts an unresolved topic name"
    );
    check!(
        cli(&bootstrap, &["trigger", "--group", GROUP]).await == 0,
        "a cut over it still succeeds"
    );
}
