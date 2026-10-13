//! Construction of the `ShareGroupHeartbeat` response. It keeps the wire
//! shaping of the success, error, and assignment replies out of the request
//! handlers that decide which of them to send.

use {
    crate::coordinator::unified::share::{config::ShareGroupConfig, state::ShareGroupState},
    krabka_protocol::owned::{
        common::share_group_heartbeat_response::topic_partitions::TopicPartitions,
        share_group_heartbeat_response::{
            Assignment as RespAssignment, ShareGroupHeartbeatResponse,
        },
    },
};

pub(super) fn base_resp(
    error_code: i16,
    member_epoch: i32,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        ..Default::default()
    }
}

pub(super) fn error_resp(
    error_code: i16,
    config: &ShareGroupConfig,
) -> ShareGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

/// The success response for `member_id`. It carries the member's assignment
/// only when `with_assignment` is set; Kafka leaves it null otherwise.
pub(super) fn build_assignment_resp(
    state: &ShareGroupState,
    member_id: &str,
    config: &ShareGroupConfig,
    with_assignment: bool,
) -> ShareGroupHeartbeatResponse {
    let m = state
        .members
        .get(member_id)
        .expect("member exists at build_assignment_resp");
    let assignment = with_assignment.then(|| RespAssignment {
        topic_partitions: m
            .assigned_partitions
            .iter()
            .map(|(tid, parts)| TopicPartitions {
                topic_id: *tid,
                partitions: parts.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    });
    ShareGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis())
            .unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}
