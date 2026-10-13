//! The fail-closed test: while the topic-backed manager has not activated, the
//! copy task must tier nothing.
//!
//! The broker boots with a bootstrap address on a dead port, so the retry loop
//! never swaps the manager in and every `add_remote_log_segment_metadata` call
//! returns `NotReady`. The dead-port override keeps the manager unavailable.

use {
    crate::{
        rlmm_cluster::{await_tiered_config, build_client, start_configured_topic_rlmm},
        rlmm_round_trip::remote_log_files,
        run_broker_test,
        topic_fixture::{create_configured_topic, tiered_configs},
    },
    assert2::assert,
    krabka_broker::RlmmKind,
    std::time::Duration,
};

/// While the topic-backed RLMM has not yet activated, the RLM copy task must
/// not tier any segment. Bootstrap points at a dead port, so the retry loop
/// never succeeds. The copy task calls `add_remote_log_segment_metadata`
/// first, and a `NotReady` error makes the copy task skip the segment
/// entirely. This proves the fail-closed guarantee: no orphaned objects
/// accumulate in the remote store while the RLMM is unavailable.
///
/// The topic config and produce volume mirror
/// [`topic_rlmm_copy_then_fetch_round_trip`] exactly, so "0 tiered objects"
/// is genuinely discriminating. The analogous loopback test tiers ≥ 1.
#[test]
fn copy_task_skips_tiering_while_rlmm_not_ready() {
    run_broker_test(copy_task_skips_tiering_while_rlmm_not_ready_case());
}

async fn copy_task_skips_tiering_while_rlmm_not_ready_case() {
    const TOPIC: &str = "tiered-not-ready-itest";

    let (broker, _log_dir, remote_dir) = start_configured_topic_rlmm(|cfg, log_dir| {
        cfg.remote_log_manager_interval = krabka_units::millis(200);
        // The dead bootstrap port keeps the SwappableRlmm on NotReady.
        let RlmmKind::TopicBacked(metadata) = &mut cfg.remote_log_metadata else {
            unreachable!("topic-backed fixture");
        };
        metadata.bootstrap = "127.0.0.1:1".into();
        metadata.snapshot_dir = log_dir.join("rlmm-snap");
    })
    .await;
    let client = build_client(&broker).await;

    create_configured_topic(&client, TOPIC, tiered_configs(Some("1024"))).await;

    // Wait for the tiered config to propagate into the partition's LogConfig
    // (same gate as the loopback round-trip test).
    // intentional: local `LogConfig` override applied by the reconcile loop —
    // no awaiter/metric exists for it, so poll directly.
    await_tiered_config(&broker, TOPIC).await;

    // Same 80 records as the loopback round-trip — enough to seal several
    // 1 KiB segments and give the copy task ample segments to try to tier.
    broker
        .produce_records_for_test(TOPIC, 0, 80)
        .await
        .expect("produce records");

    // intentional: this is the behaviour under test — a deliberate "observe
    // nothing tiered within a window" wait. We let several copy-task ticks
    // elapse (200 ms interval × ~10 ticks) and then assert 0 tiered objects;
    // there is no "did-not-happen" event to await on.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The RLMM is still NotReady, so add_remote_log_segment_metadata returns
    // NotReady and the copy task must have skipped every segment.
    let tiered = remote_log_files(remote_dir.path()).len();
    assert!(
        tiered == 0,
        "expected no tiered objects while RLMM not ready, found {tiered}"
    );

    // Close the test client before broker shutdown for the same reason as
    // `topic_rlmm_copy_then_fetch_round_trip`.
    drop(client);
    broker.shutdown().await;
}
