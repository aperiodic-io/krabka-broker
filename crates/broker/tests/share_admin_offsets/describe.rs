//! Tests for `DescribeShareGroupOffsets`, `api_key` 90.
//!
//! The module holds the Describe request helpers that the Alter and Delete
//! surfaces also read their results through, and the tests that cover the
//! reported SPSO and lag of an initialized partition, an unknown topic, and a
//! broker with share groups disabled.

use std::time::Duration;

use assert2::{assert, check};
use krabka_broker::Broker;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
            DescribeShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
        },
        update_features_request::UpdateFeaturesRequest,
    },
    primitives::uuid::Uuid,
};

use crate::{
    harness::{
        NONE, UNSUPPORTED_VERSION, acquired_count, bootstrap_share_state, broker_config,
        broker_test_permit, connect, create_topic,
    },
    support::configs::feature_update,
};

/// `UpdateFeatures` `UpgradeType` 2, `SAFE_DOWNGRADE`.
const SAFE_DOWNGRADE: i8 = 2;

/// Sends `DescribeShareGroupOffsets` for one `(group, topic, partitions)`
/// and returns the group row.
pub async fn describe_offsets(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    describe_group(
        client,
        group,
        Some(vec![DescribeShareGroupOffsetsRequestTopic {
            topic_name: topic.into(),
            partitions,
            ..Default::default()
        }]),
    )
    .await
}

/// Sends `DescribeShareGroupOffsets` for every initialized partition of
/// `group`, a request with no topic list, and returns the group row.
pub async fn describe_all_offsets(
    client: &Client,
    group: &str,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    describe_group(client, group, None).await
}

async fn describe_group(
    client: &Client,
    group: &str,
    topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    let resp = client
        .send(DescribeShareGroupOffsetsRequest {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group.into(),
                topics,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeShareGroupOffsets");
    resp.groups[0].clone()
}

/// Polls Describe until the partition reports the expected SPSO.
///
/// The persister writes the advanced SPSO asynchronously after the Accept ack.
pub async fn describe_until(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
    want_spso: i64,
) -> krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    let mut last = describe_offsets(client, group, topic, partitions.clone()).await;
    for _ in 0..40 {
        if let Some(p) = last.topics.first().and_then(|t| t.partitions.first())
            && p.start_offset == want_spso
        {
            return last;
        }
        // intentional: bounded Describe-RPC poll for the async persister write of the
        // SPSO; returns the response for assertions and also serves the deleted (-1)
        // case that no share-SPSO awaiter covers.
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = describe_offsets(client, group, topic, partitions.clone()).await;
    }
    last
}

/// Describe reflects the SPSO after a consume that Accepts all records.
///
/// The SPSO advances to 3, and the lag, from the end offset the leader
/// reports to `ListOffsets`, is 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reflects_spso_after_consume() {
    // Acquire 0..2, Accept all → SPSO advances to 3.
    let (_permit, _broker, client, _dir, tid, member, row) = crate::harness::acquired_topic().await;
    assert!(acquired_count(&row) == 3, "must acquire all 3 offsets");
    crate::support::share::acknowledge_success(
        &client,
        crate::support::share::ShareAck::prefix_for(&member, tid, krabka_ids::Offset(2)),
    )
    .await;

    // Let the persister land the advanced SPSO durably.
    let group = describe_until(&client, "g1", "t", vec![0], 3).await;
    let part = &group.topics[0].partitions[0];
    check!(
        group.error_code == NONE,
        "group error: {}",
        group.error_code
    );
    check!(group.topics[0].topic_name == "t");
    check!(
        part.error_code == NONE,
        "partition error: {}",
        part.error_code
    );
    check!(
        part.start_offset == 3,
        "SPSO must be 3 after Accept of 0..2, got {}",
        part.start_offset
    );
    // End offset 3 (3 produced), SPSO 3, 0 delivered past it ⇒ lag 0.
    check!(
        part.lag == 0,
        "lag must be 0 (end offset 3 − SPSO 3), got {}",
        part.lag
    );
}

/// Describe of an unknown topic reports no data, not an error: start offset
/// and lag -1, leader epoch 0 and error 0 per partition, the all-zero topic
/// id, as Kafka's `GroupCoordinatorService.describeShareGroupOffsets`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_topic() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    // The persister must exist for the handler to reach topic resolution. The
    // bootstrap makes the share coordinator available.
    bootstrap_share_state(&broker, &client, "g1").await;

    let group = describe_offsets(&client, "g1", "nonexistent", vec![0]).await;
    assert!(
        group.error_code == NONE,
        "group-level describe must succeed, got {}",
        group.error_code
    );
    let expected = vec![DescribeShareGroupOffsetsResponseTopic {
        topic_name: "nonexistent".into(),
        topic_id: Uuid::default(),
        partitions: vec![DescribeShareGroupOffsetsResponsePartition {
            partition_index: 0,
            start_offset: -1,
            leader_epoch: 0,
            lag: -1,
            error_code: NONE,
            ..Default::default()
        }],
        ..Default::default()
    }];
    assert!(group.topics == expected);
}

/// With `share.version` finalized at 0, as `kafka-features downgrade
/// --feature share.version=0` leaves it, the admin offset RPCs are
/// unavailable: `DescribeShareGroupOffsets` marks each requested group
/// `UNSUPPORTED_VERSION`, as Kafka's `isShareGroupProtocolEnabled` gate does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_offsets_rejected_when_share_disabled() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = broker_config(dir.path().to_path_buf());
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(
        &broker,
        &client,
        crate::support::share::ShareTopicSetup::default(),
    )
    .await;
    let enabled = describe_offsets(&client, "g1", "t", vec![0]).await;
    assert!(
        enabled.error_code != UNSUPPORTED_VERSION,
        "a fresh cluster finalizes share.version 1, got {}",
        enabled.error_code
    );

    let downgrade = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update("share.version", 0, SAFE_DOWNGRADE)],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(downgrade.error_code == NONE, "{downgrade:?}");

    let group = describe_offsets(&client, "g1", "t", vec![0]).await;
    assert!(
        group.error_code == UNSUPPORTED_VERSION,
        "share-disabled describe must be UNSUPPORTED_VERSION (35), got {}",
        group.error_code
    );
}

/// The initialized partition indexes for a topic; missing topics have no indexes.
pub fn initialized_partitions(
    group: &krabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup,
    topic: &str,
) -> Vec<i32> {
    group
        .topics
        .iter()
        .find(|row| row.topic_name == topic)
        .map(|row| {
            row.partitions
                .iter()
                .map(|partition| partition.partition_index)
                .collect()
        })
        .unwrap_or_default()
}
