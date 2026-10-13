//! The rejection scenario: a classic group that still has a live member, static
//! or dynamic, must refuse the `StreamsGroupHeartbeat` with
//! `GROUP_ID_NOT_FOUND` (69) and stay Classic-typed.
//!
//! The scenario parks a `JoinGroup` on its own connection to hold the member
//! live, which is why it does not reuse the full `classic_join_sync` driver and
//! lives apart from the conversion scenario.

use {
    crate::{
        support::client::connect_client,
        upgrade_classic::join_request,
        upgrade_harness::{
            ERR_GROUP_ID_NOT_FOUND, ERR_MEMBER_ID_REQUIRED, boot, connect, create_topic,
            finalize_streams_version,
        },
        upgrade_streams::{first_join, topology},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        join_group_request::JoinGroupRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::time::Duration,
};

/// Parks the `JoinGroup` of one classic member of `group` on a connection of
/// its own, so that the member stays in the group, and waits until the classic
/// actor holds it.
///
/// A dynamic member first takes the `MEMBER_ID_REQUIRED` round. A static
/// member, with `instance_id`, joins at once.
async fn hold_live_classic_member(
    broker: &krabka_broker::BrokerHandle,
    bootstrap: &str,
    group: &str,
    instance_id: Option<&str>,
) {
    let mut member_id = String::new();
    if instance_id.is_none() {
        let client = connect(bootstrap).await;
        let r1 = tokio::time::timeout(Duration::from_secs(5), client.send(join_request(group, "")))
            .await
            .expect("JoinGroup1 timeout")
            .expect("JoinGroup1");
        assert!(
            r1.error_code == ERR_MEMBER_ID_REQUIRED,
            "expected MEMBER_ID_REQUIRED, got {r1:?}"
        );
        member_id = r1.member_id;
    }
    let request = JoinGroupRequest {
        group_instance_id: instance_id.map(str::to_string),
        ..join_request(group, &member_id)
    };
    // The join parks in the rebalance-delay wait. The member stays joined (no
    // leave), so the group has a live member.
    let join_bootstrap = bootstrap.to_string();
    tokio::spawn(async move {
        let c = connect_client(&join_bootstrap, Some("classic-joiner")).await;
        let _ = tokio::time::timeout(Duration::from_secs(30), c.send(request)).await;
    });
    broker.wait_until_classic_group_member_count(group, 1).await;
}

/// A classic group with a **live** member, dynamic or static, rejects the
/// `StreamsGroupHeartbeat` and remains Classic-typed.
///
/// Kafka 4.3.1's `getOrCreateStreamsGroup` converts only an empty classic
/// group. Any other is refused by `castToStreamsGroup` with
/// `GROUP_ID_NOT_FOUND` and the message "Group {id} is not a streams group.",
/// and a static member holds its group like a dynamic one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_with_live_member_rejects_streams_heartbeat() {
    let (broker, bootstrap, _dir) = boot().await;
    let streams_client = connect(&bootstrap).await;
    finalize_streams_version(&streams_client).await;
    create_topic(&streams_client, "in2", 1).await;

    // (row, group id, the classic member's instance id)
    let rows = [
        ("a dynamic classic member", "g2", None),
        ("a static classic member", "g3", Some("instance-1")),
    ];
    for (row, group, instance_id) in rows {
        hold_live_classic_member(&broker, &bootstrap, group, instance_id).await;
        assert!(
            broker.group_type_for_test(group)
                == Some(krabka_broker::coordinator::unified::GroupType::Classic),
            "{row}: precondition: group_type must be Classic, got {:?}",
            broker.group_type_for_test(group)
        );

        let resp = streams_client
            .send(first_join(group, topology("in2")))
            .await
            .expect("StreamsGroupHeartbeat");
        check!(
            resp == StreamsGroupHeartbeatResponse {
                error_code: ERR_GROUP_ID_NOT_FOUND,
                error_message: Some(format!("Group {group} is not a streams group.")),
                ..Default::default()
            },
            "{row}"
        );

        // Group must STILL be Classic-typed (no flip), with its member.
        check!(
            broker.group_type_for_test(group)
                == Some(krabka_broker::coordinator::unified::GroupType::Classic),
            "{row}: group_type must remain Classic after rejected upgrade"
        );
        broker.wait_until_classic_group_member_count(group, 1).await;
    }
}
