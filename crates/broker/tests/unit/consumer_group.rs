//! The classic consumer-group protocol over one broker.
//!
//! `JoinGroup` hands out a member id before it admits a member, and the whole
//! sequence of join, sync, heartbeat, commit, fetch, and leave runs against a
//! single-member group here.

use assert2::{assert, check};
use krabka_protocol::owned::{
    heartbeat_request::HeartbeatRequest,
    leave_group_request::LeaveGroupRequest,
    offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
};

use crate::{
    harness::{create_topic, topic_id_for},
    support::{
        classic::{classic_sync_request, sync_assignment},
        offsets::{
            offset_commit_partition, offset_commit_topic, offset_fetch_group, offset_fetch_request,
            offset_fetch_topic,
        },
        start_ready_group as start,
    },
};

fn join_setup() -> crate::support::classic::ClassicJoinSetup {
    crate::support::classic::ClassicJoinSetup {
        group_id: "g".into(),
        timeouts: crate::support::classic::ClassicTimeouts {
            rebalance: krabka_units::millis(1_500),
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn join_group_with_empty_member_returns_member_id_required() {
    let p = start().await;
    let req =
        crate::support::classic::classic_join_request(crate::support::classic::ClassicJoinSetup {
            timeouts: crate::support::classic::ClassicTimeouts {
                rebalance: krabka_units::millis(2_000),
                ..Default::default()
            },
            ..join_setup()
        });
    let r = p.client.send(req).await.expect("JoinGroup");
    assert!(r.error_code == 79); // MEMBER_ID_REQUIRED
    assert!(!r.member_id.is_empty());
    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_single_member_completes_after_deadline() {
    let p = start().await;
    // First call to obtain a server-assigned member_id.
    let r1 = p
        .client
        .send(crate::support::classic::classic_join_request(join_setup()))
        .await
        .expect("JoinGroup1");
    // Retry with the assigned member_id. The handler will block ~1.5s
    // waiting for the rebalance deadline.
    let r2 = p
        .client
        .send(crate::support::classic::classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                member_id: r1.member_id.clone(),
                ..join_setup()
            },
        ))
        .await
        .expect("JoinGroup2");
    check!(r2.error_code == 0);
    check!(r2.leader == r1.member_id);
    check!(r2.member_id == r1.member_id);
    check!(!r2.members.is_empty(), "leader sees member list");
    p.broker.shutdown().await;
}

#[tokio::test]
async fn full_group_flow_join_sync_heartbeat_commit_fetch_leave() {
    let p = start().await;

    // KIP-516: OffsetCommit/OffsetFetch negotiate to v10/v8+, which key by
    // topic_id on the wire — so the topic must exist to carry a real UUID.
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p.client, "t").await;

    // Step 1: empty member_id → broker returns one.
    let r1 = p
        .client
        .send(crate::support::classic::classic_join_request(join_setup()))
        .await
        .unwrap();
    assert!(r1.error_code == 79);
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // Step 2: re-join with assigned member_id → wait for rebalance, become leader.
    let r2 = p
        .client
        .send(crate::support::classic::classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                member_id: mid.clone(),
                ..join_setup()
            },
        ))
        .await
        .unwrap();
    assert!(r2.error_code == 0);
    assert!(r2.leader == mid);
    let generation = r2.generation_id;

    // Step 3: leader SyncGroup with a single-member assignment.
    let r3 = p
        .client
        .send(classic_sync_request(
            crate::support::classic::ClassicSyncSetup {
                group_id: ("g").into(),
                generation_id: crate::support::classic::GenerationId(generation),
                member_id: mid.clone(),
                assignments: vec![sync_assignment(
                    mid.clone(),
                    bytes::Bytes::from_static(b"asgn"),
                )],
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(r3.error_code == 0);
    assert!(r3.assignment.as_ref() == b"asgn");

    // Step 4: Heartbeat → 0.
    let r4 = p
        .client
        .send(HeartbeatRequest {
            group_id: "g".into(),
            generation_id: generation,
            member_id: mid.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r4.error_code == 0);

    // Step 5: OffsetCommit → 0.
    let r5 = p
        .client
        .send(OffsetCommitRequest {
            group_id: "g".into(),
            generation_id_or_member_epoch: generation,
            member_id: mid.clone(),
            topics: vec![offset_commit_topic(
                "t",
                tid,
                vec![OffsetCommitRequestPartition {
                    committed_leader_epoch: 0,
                    ..offset_commit_partition(0, 42, Some(String::new()))
                }],
            )],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r5.topics[0].partitions[0].error_code == 0);

    // Step 6: OffsetFetch → returns 42. v8+ uses the multi-group `groups[]`
    // shape, keyed by topic_id at v10.
    let r6 = p
        .client
        .send(offset_fetch_request(offset_fetch_group(
            "g",
            Some(vec![offset_fetch_topic("t", tid, vec![0])]),
        )))
        .await
        .unwrap();
    assert!(r6.groups[0].topics[0].partitions[0].committed_offset == 42);

    // Step 7: LeaveGroup.
    let r7 = p
        .client
        .send(LeaveGroupRequest {
            group_id: "g".into(),
            member_id: mid.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(r7.error_code == 0);

    p.broker.shutdown().await;
}
