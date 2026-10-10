// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-559: the `JoinGroup` v7+ and `SyncGroup` v5+ responses carry
//! `protocol_type` and `protocol_name`, so an L7 proxy can identify the
//! group-coordination exchange without remembering the earlier one.
//!
//! `Client::send` negotiates to the broker `MAX_VERSION`, which is v9 for
//! `JoinGroup` and v5 for `SyncGroup`, so these tests always exercise the
//! KIP-559 fields.

use assert2::{assert, check};

use crate::support::classic::{
    classic_join_request, classic_sync_request, join_protocol, sync_assignment,
};
mod support;

use krabka_protocol::owned::{
    join_group_response::JoinGroupResponse, sync_group_response::SyncGroupResponse,
};

const GROUP: &str = "kip559-grp";
const PROTOCOL_TYPE: &str = "consumer";
const PROTOCOL_NAME: &str = "range";

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

fn join_setup() -> crate::support::classic::ClassicJoinSetup {
    crate::support::classic::ClassicJoinSetup {
        group_id: GROUP.into(),
        timeouts: crate::support::classic::ClassicTimeouts {
            rebalance: krabka_units::millis(1_500),
            ..Default::default()
        },
        protocol_type: PROTOCOL_TYPE.into(),
        protocols: vec![join_protocol(PROTOCOL_NAME, bytes::Bytes::new())],
        ..Default::default()
    }
}

async fn bootstrap_member(p: &support::InProcess) -> (String, i32) {
    // Step 1: empty member_id → broker returns MEMBER_ID_REQUIRED with a
    // generated id. KIP-559 doesn't require fields here (the group state
    // hasn't recorded a protocol yet).
    let r1 = p
        .client
        .send(classic_join_request(join_setup()))
        .await
        .expect("JoinGroup (bootstrap)");
    assert!(r1.error_code == 79, "expected MEMBER_ID_REQUIRED");
    let mid = r1.member_id.clone();
    assert!(!mid.is_empty());

    // Step 2: re-join with the assigned member_id, falls out as leader.
    let r2 = p
        .client
        .send(classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                member_id: mid.clone(),
                ..join_setup()
            },
        ))
        .await
        .expect("JoinGroup");
    check!(r2.error_code == 0, "JoinGroup must succeed: {r2:?}");

    // KIP-559: protocol_type and protocol_name must be set on the
    // success response.
    check!(
        r2.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "JoinGroup must echo protocol_type: {r2:?}"
    );
    check!(
        r2.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "JoinGroup must echo protocol_name: {r2:?}"
    );

    (mid, r2.generation_id)
}

#[tokio::test]
async fn join_group_response_carries_protocol_type_and_name_on_success() {
    let p = start().await;
    let (_mid, _generation) = bootstrap_member(&p).await;
    p.broker.shutdown().await;
}

#[tokio::test]
async fn sync_group_response_carries_protocol_type_and_name_on_success() {
    let p = start().await;
    let (mid, generation) = bootstrap_member(&p).await;

    let r3 = p
        .client
        .send(classic_sync_request(
            crate::support::classic::ClassicSyncSetup {
                group_id: (GROUP).into(),
                generation_id: crate::support::classic::GenerationId(generation),
                member_id: mid.clone(),
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(PROTOCOL_NAME.into()),
                assignments: vec![sync_assignment(
                    mid.clone(),
                    bytes::Bytes::from_static(b"asgn"),
                )],
            },
        ))
        .await
        .expect("SyncGroup");
    check!(r3.error_code == 0, "SyncGroup must succeed: {r3:?}");

    // KIP-559: both fields must be echoed on the v5+ response.
    check!(
        r3.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "SyncGroup must echo protocol_type: {r3:?}"
    );
    check!(
        r3.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "SyncGroup must echo protocol_name: {r3:?}"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_inconsistent_protocol_error_carries_no_protocol_fields() {
    let p = start().await;
    // Bootstrap the group as `consumer/range`.
    let (_mid, _gen) = bootstrap_member(&p).await;

    // A second member with a different protocol_type is turned away with
    // INCONSISTENT_GROUP_PROTOCOL (23). Kafka's `classicGroupJoinNewMember`
    // answers it with the member id and the error alone: the protocol
    // fields stay null, as in every `JoinGroup` error Kafka sends.
    let r = p
        .client
        .send(classic_join_request(
            crate::support::classic::ClassicJoinSetup {
                protocol_type: ("stream").into(),
                protocols: vec![join_protocol("doesnt-matter", bytes::Bytes::new())],
                ..join_setup()
            },
        ))
        .await
        .expect("JoinGroup");
    assert!(
        r == JoinGroupResponse {
            error_code: 23,
            protocol_name: None,
            ..JoinGroupResponse::default()
        },
        "expected a bare INCONSISTENT_GROUP_PROTOCOL (23), got {r:?}"
    );
    p.broker.shutdown().await;
}

/// A `SyncGroup` that Kafka refuses carries only the error code. Kafka's
/// `GroupCoordinatorService.syncGroup` answers every error with
/// `new SyncGroupResponseData().setErrorCode(...)`, so `protocol_type` and
/// `protocol_name` stay null even though the group has a recorded protocol.
#[tokio::test]
async fn sync_group_error_carries_no_protocol_fields() {
    let p = start().await;
    let (_mid, generation) = bootstrap_member(&p).await;

    let r = p
        .client
        .send(classic_sync_request(
            crate::support::classic::ClassicSyncSetup {
                group_id: (GROUP).into(),
                generation_id: crate::support::classic::GenerationId(generation),
                member_id: ("ghost-member").into(),
                protocol_type: Some(PROTOCOL_TYPE.into()),
                protocol_name: Some(PROTOCOL_NAME.into()),
                ..Default::default()
            },
        ))
        .await
        .expect("SyncGroup");
    check!(
        r == SyncGroupResponse {
            error_code: 25,
            protocol_type: None,
            protocol_name: None,
            assignment: bytes::Bytes::default(),
            ..Default::default()
        }
    );

    p.broker.shutdown().await;
}
