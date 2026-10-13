//! `Heartbeat` (`api_key=12`).
//!
//! This handler validates `(generation, member)`. It then refreshes the
//! member's `last_heartbeat` clock inside the group's actor.

use {
    crate::{
        codes, coordinator::unified::actor::GroupActorMessage, handlers::ErrorCodeResponse as _,
        task_util::ask,
    },
    krabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest, heartbeat_response::HeartbeatResponse,
    },
};

context_handler! {
    HeartbeatRequest => HeartbeatResponse,
    (broker, req, _version, ctx),
    {
        let coordinator = broker.group_coordinator.clone();

        // Kafka's `GroupCoordinatorService.heartbeat` answers an empty group
        // id before any group lookup.
        // `GroupCoordinatorService.heartbeat` answers `NONE` in place of
        // `COORDINATOR_LOAD_IN_PROGRESS`, so a member keeps its session while
        // the new coordinator loads.
        if let Some(error_code) =
            crate::handlers::acl_gates::classic_group_refusal(broker, ctx, &req.group_id)
            .map(|code| {
                if code == codes::COORDINATOR_LOAD_IN_PROGRESS {
                    codes::NONE
                } else {
                    code
                }
            })
        {
            return Ok(HeartbeatResponse::error(error_code));
        }

        let error_code = match coordinator.find(&req.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            // A closed mailbox and a dropped reply both read as a member the
            // group no longer knows.
            Some(handle) => ask(&handle.tx, |reply| GroupActorMessage::ClassicHeartbeat {
                req,
                reply,
            })
            .await
            .unwrap_or(codes::UNKNOWN_MEMBER_ID),
        };

        Ok(HeartbeatResponse::error(error_code))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{handlers::group_read_denied, test_support::peer},
        assert2::assert,
    };

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        empty_acl_fixture!(
            (authorizer, image),
            (principal, peer, ctx),
            crate::test_support::principal("ANONYMOUS"),
            client_id = "heartbeat-client"
        );

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        assert!(
            HeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED)
                == unthrottled_wire!(HeartbeatResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                })
        );
    }
}
