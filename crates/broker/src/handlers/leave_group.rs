//! `LeaveGroup` (`api_key=13`). It removes one or more members inside the
//! group's actor. It then opens a rebalance again, if the group is `Stable` or
//! `CompletingRebalance` and members remain.
//!
//! The checks run in Kafka's order: `Read` on the group in
//! `KafkaApis.handleLeaveGroupRequest`, then the empty group id in
//! `GroupCoordinatorService.leaveGroup`, then the coordinator routing. A group
//! the coordinator does not know is Kafka's `UnknownMemberIdException`, which
//! the service answers with one `UNKNOWN_MEMBER_ID` row per requested member
//! and a top-level `NONE`; `LeaveGroupResponse` folds that row into the
//! top-level code at v0-v2.

use {
    crate::{
        codes,
        coordinator::unified::actor::{GroupActorMessage, LeaveResult},
        handlers::{ErrorCodeResponse as _, group_read_denied},
        task_util::{AskError, ask},
    },
    krabka_protocol::owned::{
        leave_group_request::LeaveGroupRequest,
        leave_group_response::{LeaveGroupResponse, MemberResponse},
    },
};

#[cfg(test)]
mod tests;

context_handler! {
    LeaveGroupRequest => LeaveGroupResponse,
    (broker, req, version, ctx),
    {
        let coordinator = broker.group_coordinator.clone();

        // ── ACL preamble ────────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let image = broker.controller.current_image();
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return Ok(LeaveGroupResponse::error(codes::GROUP_AUTHORIZATION_FAILED));
        }

        // `GroupCoordinatorService.leaveGroup`'s `isGroupIdNotEmpty` check runs
        // before the request is routed to a coordinator shard.
        if req.group_id.is_empty() {
            return Ok(LeaveGroupResponse::error(codes::INVALID_GROUP_ID));
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return Ok(LeaveGroupResponse::error(error_code));
        }

        let result = match coordinator.find(&req.group_id) {
            None => unknown_group_result(&req, version),
            Some(handle) => {
                let asked = ask(&handle.tx, |reply| GroupActorMessage::ClassicLeave {
                    req: req.clone(),
                    version,
                    reply,
                })
                .await;
                match asked {
                    Err(AskError::Closed) => LeaveResult {
                        error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                        members: Vec::new(),
                    },
                    asked => match asked.unwrap_or_default() {
                        // The actor answers a group of a kind the classic leave
                        // cannot reach with `UNKNOWN_MEMBER_ID`, which Kafka
                        // shapes exactly as a missing group.
                        result
                            if result.error_code == codes::UNKNOWN_MEMBER_ID
                                && result.members.is_empty() =>
                        {
                            unknown_group_result(&req, version)
                        }
                        result => result,
                    },
                }
            }
        };

        Ok(LeaveGroupResponse {
            error_code: result.error_code,
            throttle_time_ms: 0,
            members: result.members,
            ..Default::default()
        })
    }
}

/// Kafka's answer for a group the coordinator does not hold: one
/// `UNKNOWN_MEMBER_ID` row per requested identity under a top-level `NONE`.
/// A v0-v2 request names one member in the top-level `member_id`, and
/// `LeaveGroupResponse` carries that row's code at the top level instead.
fn unknown_group_result(req: &LeaveGroupRequest, version: i16) -> LeaveResult {
    if version < 3 {
        return LeaveResult {
            error_code: codes::UNKNOWN_MEMBER_ID,
            members: Vec::new(),
        };
    }
    LeaveResult {
        error_code: codes::NONE,
        members: req
            .members
            .iter()
            .map(|member| MemberResponse {
                member_id: member.member_id.clone(),
                group_instance_id: member.group_instance_id.clone(),
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            })
            .collect(),
    }
}
