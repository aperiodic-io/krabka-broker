//! Client-side drivers for the classic (`JoinGroup`/`SyncGroup`) side of the
//! upgrade scenarios.
//!
//! A classic group is what both scenarios start from, and forming one takes the
//! `MEMBER_ID_REQUIRED` two-step plus a leader `SyncGroup`, so that sequence is
//! kept here apart from the streams-side heartbeat drivers.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
    sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
};
use krabka_units::{Time, convert::TimeExt, secs};

const ERR_MEMBER_ID_REQUIRED: i16 = 79;
const ERR_NONE: i16 = 0;

pub fn join_request(group_id: &str, member_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request(crate::support::classic::ClassicJoinSetup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![join_protocol("range".to_string(), Bytes::from_static(b""))],
            ..Default::default()
        })
    }
}

/// Drive the `JoinGroup` two-step (`MEMBER_ID_REQUIRED` + re-join) then `SyncGroup`.
/// Returns `(member_id, generation_id)`. The caller is the sole member so it
/// is also the leader and supplies a trivial self-assignment in `SyncGroup`.
pub async fn classic_join_sync(client: &Client, group_id: &str) -> (String, i32) {
    // Round 1: empty member_id → broker mints one and returns MEMBER_ID_REQUIRED.
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.send(join_request(group_id, "")),
    )
    .await
    .expect("JoinGroup1 timeout")
    .expect("JoinGroup1");
    assert!(
        r1.error_code == ERR_MEMBER_ID_REQUIRED,
        "expected MEMBER_ID_REQUIRED, got {r1:?}"
    );
    let member_id = r1.member_id.clone();
    assert!(!member_id.is_empty());

    // Round 2: rejoin with assigned member_id — broker blocks for the
    // initial-rebalance-delay then returns as sole leader.
    let r2 = tokio::time::timeout(
        Duration::from_secs(10),
        client.send(join_request(group_id, &member_id)),
    )
    .await
    .expect("JoinGroup2 timeout")
    .expect("JoinGroup2");
    assert!(
        r2.error_code == ERR_NONE,
        "second JoinGroup must succeed, got {r2:?}"
    );
    let generation_id = r2.generation_id;

    // SyncGroup: sole leader supplies its own assignment.
    let r3 = client
        .send(SyncGroupRequest {
            group_id: group_id.to_string(),
            generation_id,
            member_id: member_id.clone(),
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            assignments: vec![SyncGroupRequestAssignment {
                member_id: member_id.clone(),
                assignment: Bytes::from_static(b""),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("SyncGroup");
    assert!(
        r3.error_code == ERR_NONE,
        "SyncGroup must succeed, got {r3:?}"
    );

    (member_id, generation_id)
}

/// Preserve protocol preference order and arbitrary metadata bytes.
pub fn join_protocol(name: impl Into<String>, metadata: Bytes) -> JoinGroupRequestProtocol {
    JoinGroupRequestProtocol {
        name: name.into(),
        metadata,
        ..Default::default()
    }
}

/// Session and rebalance deadlines are distinct quantities, even when equal.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ClassicTimeouts {
    #[default(secs(30))]
    pub session: Time,
    #[default(secs(30))]
    pub rebalance: Time,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct GenerationId(pub i32);

impl Default for GenerationId {
    fn default() -> Self {
        Self(1)
    }
}

/// A classic join with explicit member identity, timing and protocol proposals.
#[derive(krabka_macros::FieldDefaults)]
pub struct ClassicJoinSetup {
    #[default("group".into())]
    pub group_id: String,
    pub member_id: String,
    pub timeouts: ClassicTimeouts,
    #[default("consumer".into())]
    pub protocol_type: String,
    #[default(vec![join_protocol("range", Bytes::new())])]
    pub protocols: Vec<JoinGroupRequestProtocol>,
}

pub fn classic_join_request(setup: ClassicJoinSetup) -> JoinGroupRequest {
    let ClassicJoinSetup {
        group_id,
        member_id,
        timeouts,
        protocol_type,
        protocols,
    } = setup;
    JoinGroupRequest {
        group_id,
        member_id,
        session_timeout_ms: timeouts.session.millis_i32(),
        rebalance_timeout_ms: timeouts.rebalance.millis_i32(),
        protocol_type,
        protocols,
        ..Default::default()
    }
}

pub fn sync_assignment(
    member_id: impl Into<String>,
    assignment: Bytes,
) -> SyncGroupRequestAssignment {
    SyncGroupRequestAssignment {
        member_id: member_id.into(),
        assignment,
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ClassicSyncSetup {
    #[default("group".into())]
    pub group_id: String,
    pub generation_id: GenerationId,
    pub member_id: String,
    #[default(Some("consumer".into()))]
    pub protocol_type: Option<String>,
    #[default(Some("range".into()))]
    pub protocol_name: Option<String>,
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

pub fn classic_sync_request(setup: ClassicSyncSetup) -> SyncGroupRequest {
    let ClassicSyncSetup {
        group_id,
        generation_id,
        member_id,
        protocol_type,
        protocol_name,
        assignments,
    } = setup;
    SyncGroupRequest {
        group_id,
        generation_id: generation_id.0,
        member_id,
        protocol_type,
        protocol_name,
        assignments,
        ..Default::default()
    }
}

/// Empty range metadata with the caller's original join deadlines.
pub fn empty_range_join(
    group: impl Into<String>,
    member: impl Into<String>,
    timeouts: ClassicTimeouts,
) -> JoinGroupRequest {
    classic_join_request(crate::support::classic::ClassicJoinSetup {
        group_id: (group).into(),
        member_id: (member).into(),
        timeouts,
        ..Default::default()
    })
}
