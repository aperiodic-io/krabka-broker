//! Typed request helpers for the unit tests: each one sends a single
//! [`GroupActorMessage`] to a live actor and awaits the reply, so a test reads
//! as the RPC it exercises rather than as channel plumbing.

use std::collections::HashMap;

use krabka_protocol::{
    owned::{
        consumer_group_heartbeat_request::{ConsumerGroupHeartbeatRequest, TopicPartitions},
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        heartbeat_request::HeartbeatRequest,
        join_group_request::JoinGroupRequest,
        leave_group_request::LeaveGroupRequest,
        leave_group_response::MemberResponse,
        sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
    },
    primitives::uuid::Uuid,
};

use super::subscription_blob;
use crate::{
    coordinator::unified::{
        actor::{
            ClassicView, CommitFence, CommitRequest, DescribeMember, DescribeView,
            GroupActorHandle, GroupActorMessage, JoinResult, LeaveResult, SyncResult,
        },
        classic_state::OffsetEntry,
        group::GroupOffsets,
    },
    task_util::ask,
};

/// Sends a request without awaiting its reply, so tests can retain the exact
/// receive point when a reply parks behind another actor action.
pub async fn begin<T>(
    handle: &GroupActorHandle,
    request: impl FnOnce(tokio::sync::oneshot::Sender<T>) -> GroupActorMessage,
) -> tokio::sync::oneshot::Receiver<T> {
    let (reply, response) = tokio::sync::oneshot::channel();
    handle.tx.send(request(reply)).await.unwrap();
    response
}

pub async fn describe(handle: &GroupActorHandle) -> DescribeView {
    ask(&handle.tx, |reply| GroupActorMessage::Describe { reply })
        .await
        .unwrap()
}

/// The live describe view of one member.
pub async fn describe_member(handle: &GroupActorHandle, member_id: &str) -> DescribeMember {
    describe(handle)
        .await
        .members
        .into_iter()
        .find(|member| member.member_id == member_id)
        .expect("member in the describe view")
}

pub async fn classic_leave_request(
    handle: &GroupActorHandle,
    req: LeaveGroupRequest,
    version: i16,
) -> LeaveResult {
    ask(&handle.tx, |reply| GroupActorMessage::ClassicLeave {
        req,
        version,
        reply,
    })
    .await
    .unwrap()
}

pub async fn classic_leave_member(handle: &GroupActorHandle, member_id: &str) -> LeaveResult {
    classic_leave_request(
        handle,
        LeaveGroupRequest {
            group_id: "g".into(),
            members: vec![
                krabka_protocol::owned::leave_group_request::MemberIdentity {
                    member_id: member_id.into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        3,
    )
    .await
}

pub fn check_successful_classic_leave(result: &LeaveResult) {
    assert2::check!(result.error_code == crate::codes::NONE);
    assert2::check!(result.members[0].error_code == crate::codes::NONE);
}

pub fn classic_join_message(
    member_id: &str,
    topic: &str,
    reply: tokio::sync::oneshot::Sender<JoinResult>,
) -> GroupActorMessage {
    GroupActorMessage::ClassicJoin {
        req: JoinGroupRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            protocol_type: "consumer".into(),
            protocols: vec![
                krabka_protocol::owned::join_group_request::JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: subscription_blob(&[topic]),
                    ..Default::default()
                },
            ],
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        },
        version: 4,
        client_id: "client-a".into(),
        client_host: "127.0.0.1".into(),
        regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
        reply,
    }
}

pub async fn classic_join(handle: &GroupActorHandle, member_id: &str, topic: &str) -> JoinResult {
    ask(&handle.tx, |reply| {
        classic_join_message(member_id, topic, reply)
    })
    .await
    .unwrap()
}

pub async fn begin_classic_sync(
    handle: &GroupActorHandle,
    req: SyncGroupRequest,
) -> tokio::sync::oneshot::Receiver<SyncResult> {
    begin(handle, |reply| GroupActorMessage::ClassicSync {
        req,
        reply,
    })
    .await
}

pub fn assignment_sync_request(
    generation: i32,
    member_id: &str,
    assignment: bytes::Bytes,
) -> SyncGroupRequest {
    SyncGroupRequest {
        group_id: "g".into(),
        generation_id: generation,
        member_id: member_id.into(),
        assignments: vec![SyncGroupRequestAssignment {
            member_id: member_id.into(),
            assignment,
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub async fn classic_sync(
    handle: &GroupActorHandle,
    member_id: &str,
    generation: i32,
) -> SyncResult {
    ask(&handle.tx, |reply| GroupActorMessage::ClassicSync {
        req: SyncGroupRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            generation_id: generation,
            ..Default::default()
        },
        reply,
    })
    .await
    .unwrap()
}

pub async fn classic_heartbeat(handle: &GroupActorHandle, member_id: &str, generation: i32) -> i16 {
    ask(&handle.tx, |reply| GroupActorMessage::ClassicHeartbeat {
        req: HeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            generation_id: generation,
            ..Default::default()
        },
        reply,
    })
    .await
    .unwrap()
}

pub fn consumer_heartbeat_request(
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: topic.map(|t| vec![t.into()]),
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    }
}

/// Sends a native consumer `Heartbeat` and returns the response. A
/// `member_id` of `""` with epoch 0 is a first-join. A first-join triggers
/// an upgrade when the group is a convertible classic group under a policy
/// that allows the upgrade.
pub async fn consumer_heartbeat(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
) -> ConsumerGroupHeartbeatResponse {
    consumer_request(
        handle,
        consumer_heartbeat_request(member_id, member_epoch, topic),
    )
    .await
}

pub async fn consumer_request(
    handle: &GroupActorHandle,
    request: ConsumerGroupHeartbeatRequest,
) -> ConsumerGroupHeartbeatResponse {
    consumer_request_from(handle, request, ("client-a", "")).await
}

pub async fn consumer_request_as_client(
    handle: &GroupActorHandle,
    request: ConsumerGroupHeartbeatRequest,
) -> ConsumerGroupHeartbeatResponse {
    consumer_request_from(handle, request, ("client", "host")).await
}

pub async fn consumer_request_from(
    handle: &GroupActorHandle,
    request: ConsumerGroupHeartbeatRequest,
    client: (&str, &str),
) -> ConsumerGroupHeartbeatResponse {
    ask(&handle.tx, |reply| GroupActorMessage::Heartbeat {
        request,
        client_id: client.0.into(),
        client_host: client.1.into(),
        regex_resolver: crate::coordinator::unified::regex_resolver::no_topic_regex_resolver(),
        reply,
    })
    .await
    .unwrap()
}

/// A native consumer heartbeat subscribed to `t` that reports `owned` as the
/// partitions the member holds.
pub async fn consumer_heartbeat_owning(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    owned: &[(Uuid, &[i32])],
) -> ConsumerGroupHeartbeatResponse {
    consumer_request(
        handle,
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            rebalance_timeout_ms: 60_000,
            topic_partitions: Some(
                owned
                    .iter()
                    .map(|(topic_id, partitions)| TopicPartitions {
                        topic_id: *topic_id,
                        partitions: partitions.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        },
    )
    .await
}

/// Reads the live `ClassicInspect` view. Only a classic-kind group
/// replies.
pub fn check_stable_classic_member(view: &ClassicView, member_id: &str) {
    assert2::check!(view.state == crate::coordinator::unified::classic_state::GroupState::Stable);
    assert2::check!(view.members.len() == 1);
    assert2::check!(view.members[0].member_id == member_id);
}

pub async fn classic_inspect(handle: &GroupActorHandle) -> ClassicView {
    ask(&handle.tx, |reply| GroupActorMessage::ClassicInspect {
        reply,
    })
    .await
    .unwrap()
}

/// A classic member leaves the group (v3 single-member leave list).
pub async fn classic_leave(handle: &GroupActorHandle, member_id: &str) -> Vec<MemberResponse> {
    classic_leave_member(handle, member_id).await.members
}

/// Validates an offset commit of `partitions` against the group's LIVE kind,
/// through the single `ValidateCommit` message.
pub async fn validate_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    fence: CommitFence,
    partitions: &[(Uuid, i32)],
) -> Result<(), i16> {
    validate_commit_request(
        handle,
        CommitRequest {
            member_id: member_id.into(),
            group_instance_id: None,
            generation_or_epoch,
            fence,
            partitions: partitions.to_vec(),
        },
    )
    .await
}

pub async fn validate_commit_request(
    handle: &GroupActorHandle,
    commit: CommitRequest,
) -> Result<(), i16> {
    ask(&handle.tx, |reply| GroupActorMessage::ValidateCommit {
        commit,
        reply,
    })
    .await
    .unwrap()
}

pub async fn shutdown(handle: &GroupActorHandle) {
    ask(&handle.tx, GroupActorMessage::Shutdown).await.unwrap();
}

/// Round-trips the kind-agnostic committed-offset store.
pub async fn fetch_committed(handle: &GroupActorHandle) -> HashMap<(String, i32), OffsetEntry> {
    fetch_offsets(handle).await.committed
}

/// Reads committed offsets together with every unresolved transactional key.
pub async fn fetch_offsets(handle: &GroupActorHandle) -> GroupOffsets {
    ask(&handle.tx, |reply| GroupActorMessage::FetchOffsets {
        reply,
    })
    .await
    .unwrap()
}
