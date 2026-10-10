//! The byte format of `leader-epoch-checkpoint`, which Kafka's own tooling and
//! a restarting broker both parse: a version header, a row count, and one
//! `epoch offset` row per epoch.
//!
//! This test checks the file's byte format directly, separately from the
//! leader-epoch behavior exercised over the wire.

use assert2::check;

use crate::{
    epoch_harness::{boot_single, create_topic, record, set_leader_epoch, topic_id_for},
    support::client::connect_client,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_checkpoint_byte_compat() {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ckpt").await;

    // Produce at epoch 0.
    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "ckpt").await;
    client
        .send(crate::support::produce::batch_request(
            record("v0"),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("ckpt").into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .expect("produce");

    // Bump epoch to 1 + produce another.
    set_leader_epoch(&broker, "ckpt", 1).await;
    client
        .send(crate::support::produce::batch_request(
            record("v1"),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("ckpt").into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .expect("produce");

    // Read the checkpoint file from disk.
    let path = dir.path().join("ckpt-0").join("leader-epoch-checkpoint");
    let s = std::fs::read_to_string(&path).expect("checkpoint file");
    // Format: header "0\n", count "2\n", rows "0 0\n1 1\n".
    check!(s.starts_with("0\n"), "header should be '0\\n', got: {s:?}");
    check!(s.contains("\n2\n"), "count should be 2, got: {s:?}");
    check!(s.contains("0 0\n"), "epoch 0 row missing: {s:?}");
    check!(s.contains("1 1\n"), "epoch 1 row missing: {s:?}");

    broker.shutdown().await;
}
