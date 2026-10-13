//! The two PLAINTEXT loopback tests: the topic-backed manager activates
//! against the broker's own listener, and a sealed segment copied through it
//! reads back at offset 0.
//!
//! Both boot the plain `start_broker_with_topic_rlmm` cluster, so they sit
//! together and away from the `SASL_PLAINTEXT` and fail-closed variants.

use {
    crate::{
        rlmm_cluster::{await_activation, build_client, start_broker_with_topic_rlmm},
        rlmm_round_trip::copy_then_fetch_round_trip,
        run_broker_test,
    },
    assert2::assert,
};

const METADATA_TOPIC: &str = "__remote_log_metadata";

/// The bootstrap completes against the loopback listener. The activation
/// gauge flips and the broker provisions the `__remote_log_metadata` topic.
#[test]
fn topic_rlmm_activates_against_loopback() {
    run_broker_test(topic_rlmm_activates_against_loopback_case());
}

async fn topic_rlmm_activates_against_loopback_case() {
    let (broker, _log_dir, _remote_dir) = start_broker_with_topic_rlmm().await;

    await_activation(&broker).await;
    assert!(
        broker.has_partition(METADATA_TOPIC, 0),
        "__remote_log_metadata-0 should be hosted after bootstrap"
    );

    broker.shutdown().await;
}

/// Produce enough to seal several segments, wait for the RLM copy task to
/// tier one through the topic-backed RLMM, then read the records back at
/// offset 0. That RLMM publishes `CopySegment*` events to
/// `__remote_log_metadata` over the loopback and consumes them back to update
/// its cache.
#[test]
fn topic_rlmm_copy_then_fetch_round_trip() {
    run_broker_test(topic_rlmm_copy_then_fetch_round_trip_case());
}

async fn topic_rlmm_copy_then_fetch_round_trip_case() {
    const TOPIC: &str = "tiered-topic-rlmm-itest";

    let (broker, _log_dir, remote_dir) = start_broker_with_topic_rlmm().await;
    await_activation(&broker).await;

    let client = build_client(&broker).await;
    copy_then_fetch_round_trip(&broker, &client, remote_dir.path(), TOPIC).await;
    // Close the test client before broker shutdown; shutdown drains active
    // listener connections, so leaving the client alive can make nextest
    // report this test as slow while waiting for connection tasks to exit.
    drop(client);
    broker.shutdown().await;
}
