//! The type lock that guards the downgrade: a streams group with a live member
//! refuses a classic `JoinGroup`, static or dynamic, and the classic admin path
//! sees the converted group as `streams` and refuses to delete it while a
//! member is live.

use std::time::Duration;

use assert2::{assert, check};
use krabka_protocol::owned::{
    delete_groups_request::DeleteGroupsRequest,
    join_group_request::JoinGroupRequest,
    join_group_response::JoinGroupResponse,
    leave_group_request::{LeaveGroupRequest, MemberIdentity},
    list_groups_request::ListGroupsRequest,
};

use crate::{
    CONVERGE_TRIES, ERR_INCONSISTENT_GROUP_PROTOCOL, ERR_NON_EMPTY_GROUP, ERR_NONE,
    downgrade_classic_join::{classic_join_sync, join_request},
    downgrade_harness::{boot, connect, create_topic, finalize_streams_version},
    downgrade_streams_join::{streams_join_and_converge, topology},
};

/// A streams group with a LIVE member refuses every classic `JoinGroup`, static
/// or dynamic, and stays Streams-typed with its member.
///
/// Kafka 4.3.1's `classicGroupJoin` accepts a streams group only when it is
/// empty, and answers any other with `INCONSISTENT_GROUP_PROTOCOL` (23) and
/// the unknown member id, before the `MEMBER_ID_REQUIRED` exchange and before
/// it looks at the instance id (`testClassicGroupJoinWithNonEmptyStreamsGroup`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_group_with_live_member_rejects_classic_join() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    let classic_client = connect(&bootstrap).await;

    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in2", 1).await;

    // Live streams member (converge, do NOT leave).
    let (_mid, resp) =
        streams_join_and_converge(&streams_client, "g2", topology("in2"), 1, CONVERGE_TRIES).await;
    assert!(resp.error_code == ERR_NONE);
    broker
        .wait_until_group_type(
            "g2",
            krabka_broker::coordinator::unified::GroupType::Streams,
        )
        .await;
    broker.wait_until_streams_group_member_count("g2", 1).await;

    // (row, the JoinGroup)
    let rows = [
        ("a dynamic member's first join", join_request("g2", "")),
        (
            "a dynamic member's join with a member id",
            join_request("g2", "m-1"),
        ),
        (
            "a static member's join",
            JoinGroupRequest {
                group_instance_id: Some("instance-1".into()),
                ..join_request("g2", "")
            },
        ),
        (
            "a static member's join with a member id",
            JoinGroupRequest {
                group_instance_id: Some("instance-1".into()),
                ..join_request("g2", "m-1")
            },
        ),
    ];
    let refused = JoinGroupResponse {
        error_code: ERR_INCONSISTENT_GROUP_PROTOCOL,
        member_id: String::new(),
        // Kafka's `JoinGroupResponse` sends an empty protocol name as null
        // from version 7 on.
        protocol_name: None,
        ..Default::default()
    };
    for (row, request) in rows {
        let response = tokio::time::timeout(Duration::from_secs(5), classic_client.send(request))
            .await
            .expect("JoinGroup timeout")
            .expect("JoinGroup");
        check!(response == refused, "{row}");
    }

    assert!(
        broker.group_type_for_test("g2")
            == Some(krabka_broker::coordinator::unified::GroupType::Streams),
        "group_type must remain Streams after rejected downgrade"
    );
    broker.wait_until_streams_group_member_count("g2", 1).await;
}

/// After a classic-to-streams conversion, `ListGroups` reports
/// the converted group as `streams`. The classic path can NOT delete it while
/// the streams group has a live member.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn converted_group_admin_views_respect_type_lock() {
    let (broker, bootstrap, _dir) = boot().await;
    let (classic_client, streams_client) = (connect(&bootstrap).await, connect(&bootstrap).await);

    create_topic(&classic_client, "in3", 1).await;
    finalize_streams_version(&classic_client).await;

    // Drain a classic group, then upgrade it to streams via a heartbeat.
    let (cm, _gen) = classic_join_sync(&classic_client, "g3").await;
    // Leave so the classic group is drained.
    let _ = classic_client
        .send(LeaveGroupRequest {
            group_id: "g3".into(),
            member_id: cm.clone(),
            members: vec![MemberIdentity {
                member_id: cm.clone(),
                group_instance_id: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("LeaveGroup");

    // Wait for the classic leave to propagate before upgrading to streams.
    broker.wait_until_group_empty("g3").await;

    let (_sm, hb) =
        streams_join_and_converge(&streams_client, "g3", topology("in3"), 1, CONVERGE_TRIES).await;
    assert!(hb.error_code == ERR_NONE);
    broker
        .wait_until_group_type(
            "g3",
            krabka_broker::coordinator::unified::GroupType::Streams,
        )
        .await;
    broker.wait_until_streams_group_member_count("g3", 1).await;
    assert!(
        broker.group_type_for_test("g3")
            == Some(krabka_broker::coordinator::unified::GroupType::Streams)
    );

    // ListGroups: the converted group appears exactly once, as `streams`.
    let lg = classic_client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups");
    let rows: Vec<_> = lg.groups.iter().filter(|g| g.group_id == "g3").collect();
    assert!(rows.len() == 1, "g3 listed once, got {}", rows.len());
    assert!(
        rows[0].group_type.eq_ignore_ascii_case("streams"),
        "g3 must be typed streams, got {:?}",
        rows[0].group_type
    );

    // DeleteGroups via the classic path must NOT remove the live streams group's
    // offset home: with a live streams member it is NON_EMPTY_GROUP.
    let dg = classic_client
        .send(DeleteGroupsRequest {
            groups_names: vec!["g3".into()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    assert!(
        dg.results[0].error_code == ERR_NON_EMPTY_GROUP,
        "delete of a live streams group must be NON_EMPTY_GROUP, got {}",
        dg.results[0].error_code
    );
    assert!(
        broker.group_type_for_test("g3")
            == Some(krabka_broker::coordinator::unified::GroupType::Streams),
        "the streams group must survive the rejected delete"
    );
}
