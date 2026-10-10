//! `OffsetDelete` (`api_key` 47, KIP-496) broker integration tests.
//!
//! Each test boots a single broker with `support::start`, waits until its
//! group coordinator is ready, drives the relevant flows over the PLAINTEXT
//! data plane, and asserts on the response shape. Single-broker tests stay
//! unconditional, and this file uses only `support::start`, which is
//! cross-platform, so it needs no platform gate.

use assert2::{assert, check};

use crate::support::{
    classic::{classic_join_request, join_protocol},
    offsets::{
        offset_commit_partition, offset_commit_topic, offset_delete_partition,
        offset_delete_request, offset_delete_topic, offset_fetch_group, offset_fetch_request,
        offset_fetch_topic,
    },
};
mod support;

use bytes::BufMut;
use krabka_protocol::{
    Encode,
    owned::{
        consumer_protocol_subscription::ConsumerProtocolSubscription,
        offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
        offset_delete_response::{
            OffsetDeleteResponse, OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
        },
    },
};
use support::topic_id_for;

const OFFSET_ABSENT_SENTINEL: i64 = -1; // OffsetFetch returns -1 when no offset is committed.

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    crate::support::client::create_topic(&p.client, name, num_partitions).await;
}

async fn commit_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32, off: i64) {
    let id = topic_id_for(&p.client, topic).await;
    let resp = p
        .client
        .send(OffsetCommitRequest {
            group_id: group.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![offset_commit_topic(
                topic,
                id,
                vec![OffsetCommitRequestPartition {
                    committed_leader_epoch: -1,
                    ..offset_commit_partition(partition, off, Some(String::new()))
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        resp.topics[0].partitions[0].error_code == 0,
        "OffsetCommit({group},{topic},{partition})"
    );
}

async fn fetch_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32) -> i64 {
    let id = topic_id_for(&p.client, topic).await;
    let resp = p
        .client
        .send(offset_fetch_request(offset_fetch_group(
            group,
            Some(vec![offset_fetch_topic(topic, id, vec![partition])]),
        )))
        .await
        .expect("OffsetFetch");
    resp.groups[0].topics[0].partitions[0].committed_offset
}

/// T1, the happy path. Commit an offset for an Empty group and delete it.
/// `OffsetFetch` then returns `-1`, which means no committed offset.
#[tokio::test]
async fn delete_offsets_from_empty_group_round_trip() {
    let p = start().await;
    create_topic(&p, "t1", 2).await;

    commit_offset(&p, "g1", "t1", 0, 42).await;
    commit_offset(&p, "g1", "t1", 1, 100).await;

    // Group is Empty (no JoinGroup), delete should succeed.
    let resp = p
        .client
        .send(offset_delete_request(
            "g1",
            vec![offset_delete_topic("t1", vec![offset_delete_partition(0)])],
        ))
        .await
        .expect("OffsetDelete");
    check!(resp.error_code == 0, "top-level NONE");
    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    check!(
        resp.topics[0].partitions[0].error_code == 0,
        "partition 0 NONE"
    );

    // Verify partition 0 is gone, partition 1 still has its offset.
    check!(
        fetch_offset(&p, "g1", "t1", 0).await == OFFSET_ABSENT_SENTINEL,
        "partition 0 offset cleared"
    );
    check!(
        fetch_offset(&p, "g1", "t1", 1).await == 100,
        "partition 1 offset untouched"
    );

    p.broker.shutdown().await;
}

/// T2, unknown group. `OffsetDelete` against a group that was never
/// joined or committed returns `GROUP_ID_NOT_FOUND` at the top level.
#[tokio::test]
async fn delete_offsets_unknown_group_returns_group_id_not_found() {
    let p = start().await;
    create_topic(&p, "t2", 1).await;

    let resp = p
        .client
        .send(offset_delete_request(
            "ghost",
            vec![offset_delete_topic("t2", vec![offset_delete_partition(0)])],
        ))
        .await
        .expect("OffsetDelete");
    // A group-level refusal carries only the top-level code (#734).
    assert!(
        resp == OffsetDeleteResponse {
            error_code: 69,
            ..Default::default()
        },
        "top-level GROUP_ID_NOT_FOUND (69) and no topic rows"
    );

    p.broker.shutdown().await;
}

/// T3, missing topic. The response carries per-partition
/// `UNKNOWN_TOPIC_OR_PARTITION` (3) for a topic that does not exist, even when
/// the group does exist.
#[tokio::test]
async fn delete_offsets_missing_topic_returns_unknown_topic_or_partition() {
    let p = start().await;
    create_topic(&p, "t3", 1).await;
    commit_offset(&p, "g3", "t3", 0, 7).await;

    let resp = p
        .client
        .send(offset_delete_request(
            "g3",
            vec![offset_delete_topic(
                "nonexistent",
                vec![offset_delete_partition(0)],
            )],
        ))
        .await
        .expect("OffsetDelete");
    assert!(resp.error_code == 0, "top-level NONE");
    assert!(
        resp.topics[0].partitions[0].error_code == 3,
        "UNKNOWN_TOPIC_OR_PARTITION (3) for missing topic"
    );

    p.broker.shutdown().await;
}

/// T4, out-of-range partition. The response carries per-partition
/// `UNKNOWN_TOPIC_OR_PARTITION` (3) for a partition index beyond the
/// topic's partition count.
#[tokio::test]
async fn delete_offsets_partition_out_of_range_returns_unknown_topic_or_partition() {
    let p = start().await;
    create_topic(&p, "t4", 1).await;
    commit_offset(&p, "g4", "t4", 0, 5).await;

    let resp = p
        .client
        .send(offset_delete_request(
            "g4",
            vec![offset_delete_topic(
                "t4",
                vec![offset_delete_partition(0), offset_delete_partition(99)],
            )],
        ))
        .await
        .expect("OffsetDelete");
    // Kafka's broker answers p=99 before the coordinator answers p=0, and
    // `OffsetDeleteResponse.Builder.merge` appends p=0 after it.
    let expected = OffsetDeleteResponse {
        error_code: 0,
        topics: vec![OffsetDeleteResponseTopic {
            name: "t4".into(),
            partitions: vec![
                OffsetDeleteResponsePartition {
                    partition_index: 99,
                    error_code: 3,
                    ..Default::default()
                },
                OffsetDeleteResponsePartition {
                    partition_index: 0,
                    error_code: 0,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    check!(resp == expected);

    p.broker.shutdown().await;
}

/// T5, the subscription guard. A non-empty consumer-protocol group that is
/// still subscribed to the topic returns per-partition
/// `GROUP_SUBSCRIBED_TO_TOPIC` (86). The offset survives the request.
#[tokio::test]
async fn delete_offsets_for_subscribed_topic_returns_group_subscribed() {
    let p = start().await;
    create_topic(&p, "t5", 1).await;
    commit_offset(&p, "g5", "t5", 0, 9).await;

    // Build a ConsumerProtocolSubscription bytestring that subscribes to t5.
    let sub = ConsumerProtocolSubscription {
        topics: vec!["t5".into()],
        ..Default::default()
    };
    let mut sub_bytes = bytes::BytesMut::new();
    sub_bytes.put_i16(0); // protocol version negotiation prefix
    sub.encode(&mut sub_bytes, 0).expect("encode subscription");
    let sub_bytes = bytes::Bytes::from(sub_bytes.to_vec());

    // JoinGroup to create a live member with that subscription.
    let r1 = p
        .client
        .send(classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                group_id: ("g5").into(),
                timeouts: crate::support::classic::ClassicTimeouts {
                    rebalance: krabka_units::millis(1_500),
                    ..Default::default()
                },
                protocols: vec![join_protocol("range", sub_bytes.clone())],
                ..Default::default()
            },
        ))
        .await
        .expect("JoinGroup1");
    // First JoinGroup with empty member_id returns MEMBER_ID_REQUIRED (79).
    assert!(r1.error_code == 79);
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // Re-join with the assigned member_id → become an actual member.
    let r2 = p
        .client
        .send(classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                group_id: ("g5").into(),
                member_id: mid,
                timeouts: crate::support::classic::ClassicTimeouts {
                    rebalance: krabka_units::millis(1_500),
                    ..Default::default()
                },
                protocols: vec![join_protocol("range", sub_bytes)],
                ..Default::default()
            },
        ))
        .await
        .expect("JoinGroup2");
    assert!(r2.error_code == 0);

    // OffsetDelete on the subscribed topic → GROUP_SUBSCRIBED_TO_TOPIC.
    let resp = p
        .client
        .send(offset_delete_request(
            "g5",
            vec![offset_delete_topic("t5", vec![offset_delete_partition(0)])],
        ))
        .await
        .expect("OffsetDelete");
    check!(resp.error_code == 0, "top-level NONE");
    check!(
        resp.topics[0].partitions[0].error_code == 86,
        "GROUP_SUBSCRIBED_TO_TOPIC (86)"
    );

    // Offset is unchanged.
    check!(
        fetch_offset(&p, "g5", "t5", 0).await == 9,
        "offset survived"
    );

    p.broker.shutdown().await;
}
