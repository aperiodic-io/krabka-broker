//! Streams-group membership scenarios: a lone stateless member converging on
//! the whole task assignment, and a member leaving with `member_epoch == -1`.
//!
//! Both scenarios use a stateless topology, so they exercise the membership
//! path on its own, without the internal-topic provisioning that a stateful
//! subtopology drags in.

use assert2::{assert, check};

use crate::streams_harness::{
    active_partition_count, active_partitions_for, boot, connect, create_topic, describe,
    finalize_streams_version, follow_up, join_and_converge, topology,
};

/// A lone stateless member joins a 2-partition topic and is assigned both tasks
/// for the single subtopology.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_single_member_converges() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "streams-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-1",
        topology("streams-input", vec![]),
        2,
        10,
    )
    .await;

    check!(resp.error_code == 0, "heartbeat error: {resp:?}");
    check!(!member_id.is_empty(), "broker must mint a member id");
    check!(
        resp.member_epoch >= 1,
        "first join advances the member epoch, got {}",
        resp.member_epoch
    );
    // The single member owns both partitions of subtopology "0".
    check!(
        active_partition_count(&resp) == 2,
        "lone member must own both input partitions, got {:?}",
        resp.active_tasks
    );
    check!(
        active_partitions_for(&resp, "0") == vec![0, 1],
        "subtopology 0 must be assigned partitions [0, 1], got {:?}",
        resp.active_tasks
    );
}

/// A member that has joined can leave with `member_epoch == -1`. The leave
/// succeeds, and a later Describe shows the group without that member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leave_removes_member() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "leave-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-4",
        topology("leave-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");
    assert!(!member_id.is_empty());

    // Leave: member_epoch == -1.
    let leave = client
        .send(follow_up(crate::support::streams::StreamsFollowUpSetup {
            group: "streams-app-4",
            member_id: &member_id,
            epoch: crate::support::streams::StreamsMemberEpoch(-1),
            ..Default::default()
        }))
        .await
        .expect("leave heartbeat");
    assert!(leave.error_code == 0, "leave failed: {leave:?}");

    // The group is retained (Empty) but the member is gone.
    let desc = describe(&client, "streams-app-4").await;
    assert!(
        desc.groups.len() == 1,
        "group row still present after leave, got {}",
        desc.groups.len()
    );
    let g = &desc.groups[0];
    assert!(
        g.error_code == 0,
        "retained group describe error: {:?}",
        g.error_code
    );
    assert!(
        !g.members.iter().any(|m| m.member_id == member_id),
        "left member {member_id} must be gone, got {:?}",
        g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
    );
}
