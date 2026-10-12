//! The cold-upgrade scenario: a drained classic group flips to a streams group
//! in place when a `StreamsGroupHeartbeat` arrives, and its committed offsets
//! survive the flip.
//!
//! The scenario runs the full classic lifecycle first (join, commit, leave), so
//! it is kept apart from the rejection scenario that only needs a live member.

use assert2::assert;
use krabka_protocol::owned::{
    leave_group_request::{LeaveGroupRequest, MemberIdentity},
    offset_commit_request::OffsetCommitRequest,
};

use crate::{
    support::offsets::{offset_commit_partition, offset_commit_topic},
    upgrade_classic::classic_join_sync,
    upgrade_harness::{
        ERR_NONE, assert_committed_offset, boot, connect, create_topic, finalize_streams_version,
        topic_id_for,
    },
    upgrade_streams::{streams_join_and_converge, topology},
};

/// The broker converts a drained classic group to a streams group when a
/// `StreamsGroupHeartbeat` arrives. A drained group has zero live members and
/// keeps its committed offsets. Committed offsets survive the flip, and
/// `OffsetFetch` can read them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_classic_group_converts_and_preserves_offsets() {
    let (broker, bootstrap, _dir) = boot().await;

    // Separate connections: JoinGroup parks the classic-protocol client for
    // the full rebalance-delay; the streams heartbeat must not be queued
    // behind it on the same socket.
    let classic_client = connect(&bootstrap).await;
    let streams_client = connect(&bootstrap).await;

    finalize_streams_version(&classic_client).await;
    create_topic(&classic_client, "in", 1).await;
    let topic_id = topic_id_for(&classic_client, "in").await;

    // ── Phase 1: form a classic group, commit offset 42, then leave. ──
    let (member_id, generation_id) = classic_join_sync(&classic_client, "g").await;

    // Commit offset 42 for ("in", 0).
    let cr = classic_client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: generation_id,
            member_id: member_id.clone(),
            topics: vec![{
                let mut p = offset_commit_partition(0, 42, Some(String::new()));
                p.committed_leader_epoch = 0;
                offset_commit_topic("in", topic_id, vec![p])
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        cr.topics[0].partitions[0].error_code == ERR_NONE,
        "OffsetCommit failed: {cr:?}"
    );

    // Leave — group is now drained (no live members).
    // Use the `members` field (v3+ shape) since the client negotiates the
    // max supported version (v5), which uses `members` not `member_id`.
    let lr = classic_client
        .send(LeaveGroupRequest {
            group_id: "g".into(),
            member_id: member_id.clone(),
            members: vec![MemberIdentity {
                member_id: member_id.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");
    assert!(lr.error_code == ERR_NONE, "LeaveGroup failed: {lr:?}");

    // Precondition: the group must be Classic-typed.
    broker
        .wait_until_group_type("g", krabka_broker::coordinator::unified::GroupType::Classic)
        .await;
    assert!(
        broker.group_type_for_test("g")
            == Some(krabka_broker::coordinator::unified::GroupType::Classic),
        "precondition: group_type must be Classic before upgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 2: StreamsGroupHeartbeat for the same group_id → converge. ──
    let (_, resp) = streams_join_and_converge(
        &streams_client,
        "g",
        topology("in"),
        1, // 1 partition
        15,
    )
    .await;
    assert!(
        resp.error_code == ERR_NONE,
        "streams heartbeat after conversion must succeed, got {resp:?}"
    );

    // Group must now be Streams-typed.
    broker
        .wait_until_group_type("g", krabka_broker::coordinator::unified::GroupType::Streams)
        .await;
    assert!(
        broker.group_type_for_test("g")
            == Some(krabka_broker::coordinator::unified::GroupType::Streams),
        "group_type must be Streams after upgrade, got {:?}",
        broker.group_type_for_test("g")
    );

    // ── Phase 3: committed offsets survive the flip. ──
    assert_committed_offset(&streams_client, topic_id, 42).await;
}
