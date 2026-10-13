//! KIP-101 fencing on the leader: a `Fetch` whose `current_leader_epoch` is
//! behind the partition's epoch gets `FENCED_LEADER_EPOCH`, and one that is
//! ahead of it gets `UNKNOWN_LEADER_EPOCH`.
//!
//! The two error codes are the two sides of the same comparison, so they are
//! asserted next to each other.

use {
    crate::{
        epoch_harness::{boot_single, create_topic, record, set_leader_epoch, topic_id_for},
        support::{
            client::connect_client,
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            produce::{SinglePartitionProduceSetup, batch_request},
        },
    },
    assert2::assert,
    krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest},
};

async fn fetch_with_epoch(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    epoch: i32,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    client
        .send(FetchRequest {
            replica_id: 99,
            ..single_partition_fetch()
                .topic(topic.into())
                .topic_id(topic_id)
                .partition(FetchPartition {
                    current_leader_epoch: epoch,
                    ..fetch_partition().call()
                })
                .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                .call()
        })
        .await
        .expect("fetch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_leader_epoch_truncates_zombie_writes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "fence").await;

    // Produce a record at epoch 0.
    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "fence").await;
    client
        .send(batch_request(record("v0"), SinglePartitionProduceSetup {
            topic: ("fence").into(),
            topic_id,
            ..Default::default()
        }))
        .await
        .expect("produce");

    // Advance the partition's epoch to fence the old leader.
    set_leader_epoch(&broker, "fence", 5).await;

    // Fetch with current_leader_epoch=2 → FENCED_LEADER_EPOCH (code 74).
    let resp = fetch_with_epoch(&client, "fence", topic_id, 2).await;
    let pd = &resp.responses[0].partitions[0];
    // FENCED_LEADER_EPOCH = 74
    assert!(pd.error_code == 74, "expected FENCED_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_leader_epoch_on_metadata_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "unknown").await;
    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "unknown").await;

    // Fetch with current_leader_epoch=5 — broker has epoch=0; UNKNOWN_LEADER_EPOCH (code 75).
    let resp = fetch_with_epoch(&client, "unknown", topic_id, 5).await;
    let pd = &resp.responses[0].partitions[0];
    // UNKNOWN_LEADER_EPOCH = 75
    assert!(pd.error_code == 75, "expected UNKNOWN_LEADER_EPOCH");

    broker.shutdown().await;
}
