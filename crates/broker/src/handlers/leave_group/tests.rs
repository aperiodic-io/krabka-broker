//! Live-broker tests for the `LeaveGroup` handler.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::*,
    crate::{
        broker::Broker,
        test_support::{DenyAll, start_broker_no_audit_with},
    },
    assert2::assert,
    krabka_protocol::owned::{
        leave_group_request::MemberIdentity,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
    },
};

async fn leave(broker: &Broker, request: &LeaveGroupRequest, version: i16) -> LeaveGroupResponse {
    krabka_macros::request_test_identity! {
        bind(principal, peer, context);
        user ["alice"];
        client ["leave-client"];
    };
    handle(broker, request.clone(), version, &context)
        .await
        .expect("LeaveGroup")
}

fn identity(member_id: &str, group_instance_id: Option<&str>) -> MemberIdentity {
    MemberIdentity {
        member_id: member_id.into(),
        group_instance_id: group_instance_id.map(Into::into),
        ..Default::default()
    }
}

fn unknown_row(member_id: &str, group_instance_id: Option<&str>) -> MemberResponse {
    MemberResponse {
        member_id: member_id.into(),
        group_instance_id: group_instance_id.map(Into::into),
        error_code: codes::UNKNOWN_MEMBER_ID,
        ..Default::default()
    }
}

/// (label, version, request, whole expected response) against a broker that
/// holds no group. Kafka's `GroupCoordinatorService.leaveGroup` answers an
/// empty group id `INVALID_GROUP_ID`, and a missing group with one
/// `UNKNOWN_MEMBER_ID` row per identity under a top-level `NONE`, which
/// `LeaveGroupResponse` folds into the top-level code at v0-v2.
#[tokio::test]
async fn leave_group_answers_kafka_shapes_for_missing_and_invalid_groups() {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker_no_audit_with(|config| config.offsets_topic_replication_factor = 1).await;
    broker_handle.wait_until_group_coordinator_ready().await;
    let broker = broker_handle.broker_arc_for_test();
    let rows = [
        (
            "v2 missing group",
            2,
            LeaveGroupRequest {
                group_id: "ghost".into(),
                member_id: "m".into(),
                ..Default::default()
            },
            LeaveGroupResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            },
        ),
        (
            "v5 missing group",
            5,
            LeaveGroupRequest {
                group_id: "ghost".into(),
                members: vec![identity("m-a", None), identity("", Some("inst-b"))],
                ..Default::default()
            },
            LeaveGroupResponse {
                error_code: codes::NONE,
                members: vec![unknown_row("m-a", None), unknown_row("", Some("inst-b"))],
                ..Default::default()
            },
        ),
        (
            "v5 empty group id",
            5,
            LeaveGroupRequest {
                group_id: String::new(),
                members: vec![identity("m-a", None)],
                ..Default::default()
            },
            LeaveGroupResponse {
                error_code: codes::INVALID_GROUP_ID,
                ..Default::default()
            },
        ),
        (
            "v0 empty group id",
            0,
            LeaveGroupRequest {
                group_id: String::new(),
                member_id: "m".into(),
                ..Default::default()
            },
            LeaveGroupResponse {
                error_code: codes::INVALID_GROUP_ID,
                ..Default::default()
            },
        ),
    ];
    for (label, version, request, expected) in rows {
        let response = leave(&broker, &request, version).await;
        assert!(response == expected, "{label}");
    }
    assert!(broker.group_coordinator.find("ghost").is_none());
    broker_handle.shutdown().await;
}

/// The `Read` check on the group runs first, so a denied principal gets
/// `GROUP_AUTHORIZATION_FAILED` even for an empty group id.
#[tokio::test]
async fn denied_read_answers_group_authorization_failed_before_validation() {
    krabka_macros::broker_test_setup! { bind(broker_handle, _dir, broker); boot [start_broker_no_audit_with(|config| config.authorizer = std::sync::Arc::new(DenyAll))]; };
    let request = LeaveGroupRequest {
        group_id: String::new(),
        members: vec![identity("m-a", None)],
        ..Default::default()
    };

    let response = leave(&broker, &request, 5).await;

    assert!(
        response
            == LeaveGroupResponse {
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                ..Default::default()
            }
    );
    broker_handle.shutdown().await;
}
