//! `SyncGroup` (`api_key=14`). The handler routes the request into the group's
//! unified actor as a `ClassicSync` message.
//!
//! The leader's call installs the assignments, and the actor then releases the
//! parked followers. The actor parks a follower that has no assignment yet
//! until that point, up to the broker's configured follower wait.
//!
//! From v5, KIP-559 makes the response carry `protocol_type` and
//! `protocol_name`, so an L7 proxy can route the call without remembering the
//! earlier `JoinGroup` exchange.

use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{ApiVersion, ErrorCodeResponse as _, RequestContext},
        task_util::ask,
    },
    krabka_protocol::owned::{
        sync_group_request::SyncGroupRequest, sync_group_response::SyncGroupResponse,
    },
    krabka_units::convert::TimeExt as _,
};

pub(crate) async fn handle(
    broker: &Broker,
    req: SyncGroupRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<SyncGroupResponse, BrokerError> {
    use crate::{
        coordinator::unified::actor::GroupActorMessage, handlers::acl_gates::classic_group_refusal,
    };

    let coordinator = broker.group_coordinator.clone();

    // Kafka's `KafkaApis.handleSyncGroupRequest` answers a v5+ request
    // without a protocol type or name before the ACL check
    // (`SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`).
    if !mandatory_protocol_type_and_name_present(&req, version) {
        return Ok(SyncGroupResponse::error(codes::INCONSISTENT_GROUP_PROTOCOL));
    }

    // Kafka's `GroupCoordinatorService.syncGroup` answers an empty group
    // id before any group lookup.
    if let Some(error_code) = classic_group_refusal(broker, ctx, &req.group_id) {
        return Ok(SyncGroupResponse::error(error_code));
    }

    let Some(handle) = coordinator.find(&req.group_id) else {
        return Ok(SyncGroupResponse::error(codes::UNKNOWN_MEMBER_ID));
    };

    // The leader and the already-Stable follower reply immediately; a
    // not-yet-synced follower is parked and resolved when the leader's
    // SyncGroup installs assignments, bounded by the configured follower wait.
    // A closed mailbox, a dropped reply and the wait running out all answer
    // REBALANCE_IN_PROGRESS.
    let asked = ask(&handle.tx, |reply| GroupActorMessage::ClassicSync {
        req,
        reply,
    });
    let Ok(Ok(result)) =
        tokio::time::timeout(broker.config.sync_group_follower_wait.to_std(), asked).await
    else {
        return Ok(SyncGroupResponse::error(codes::REBALANCE_IN_PROGRESS));
    };

    Ok(SyncGroupResponse {
        error_code: result.error_code,
        assignment: result.assignment,
        protocol_type: result.protocol_type,
        protocol_name: result.protocol_name,
        ..Default::default()
    })
}

/// Kafka's `SyncGroupRequest.areMandatoryProtocolTypeAndNamePresent`: from v5
/// the request must name both its protocol type and its protocol name.
fn mandatory_protocol_type_and_name_present(req: &SyncGroupRequest, version: ApiVersion) -> bool {
    version < 5 || (req.protocol_type.is_some() && req.protocol_name.is_some())
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            authorizer::AllowAllAuthorizer,
            broker::Broker,
            handlers::{RequestContext, join_group::handle as join_group_handle},
            test_support::{DenyAll, request_context, start_group_broker_no_audit},
        },
        assert2::assert,
        bytes::Bytes,
        krabka_protocol::owned::{
            join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
            join_group_response,
            sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment},
            sync_group_response::{self, SyncGroupResponse},
        },
        krabka_security::Principal,
        std::{net::SocketAddr, sync::Arc},
    };

    const GROUP: &str = "sync-group-unit";
    const PROTOCOL_TYPE: &str = "consumer";
    const PROTOCOL_NAME: &str = "range";

    #[bon::builder]
    fn sync_request(
        member_id: String,
        generation_id: i32,
        #[builder(default)] assignments: Vec<SyncGroupRequestAssignment>,
    ) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: GROUP.into(),
            member_id,
            generation_id,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignments,
            ..Default::default()
        }
    }

    fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        request_context(principal, peer, "sync-group-client")
    }

    /// A member that joined the group, and the generation it joined.
    struct JoinedMember {
        member_id: String,
        generation_id: i32,
    }

    async fn bootstrap_member(broker: &Broker, ctx: &RequestContext<'_>) -> JoinedMember {
        let version = join_group_response::MAX_VERSION;
        let join = |member_id: String| JoinGroupRequest {
            group_id: GROUP.into(),
            protocol_type: PROTOCOL_TYPE.into(),
            member_id,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: PROTOCOL_NAME.into(),
                metadata: Bytes::new(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let r1 = join_group_handle(broker, join(String::new()), version, ctx)
            .await
            .expect("JoinGroup bootstrap");
        assert!(r1.error_code == codes::MEMBER_ID_REQUIRED, "{r1:?}");
        assert!(!r1.member_id.is_empty());

        let r2 = join_group_handle(broker, join(r1.member_id.clone()), version, ctx)
            .await
            .expect("JoinGroup rejoin");
        assert!(
            (
                r2.error_code,
                r2.protocol_type.as_deref(),
                r2.protocol_name.as_deref()
            ) == (codes::NONE, Some(PROTOCOL_TYPE), Some(PROTOCOL_NAME)),
            "{r2:?}"
        );
        JoinedMember {
            member_id: r2.member_id,
            generation_id: r2.generation_id,
        }
    }

    use {
        super::*,
        crate::{
            handlers::group_read_denied,
            test_support::{peer, principal},
        },
    };

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        empty_acl_fixture!(
            (authorizer, image),
            (principal, peer, ctx),
            crate::test_support::principal("ANONYMOUS"),
            client_id = "sync-client"
        );

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));
    }

    /// #736 and #793: from v5 a missing protocol type or name answers
    /// `INCONSISTENT_GROUP_PROTOCOL` before the group ACL, and an empty group
    /// id answers `INVALID_GROUP_ID` after it. Every error reply carries only
    /// its `error_code`, as Kafka's `SyncGroupRequest.getErrorResponse`.
    #[tokio::test]
    async fn handle_checks_mandatory_protocol_fields_acl_and_group_id_in_kafka_order() {
        struct Row {
            name: &'static str,
            version: i16,
            group_id: &'static str,
            protocol_type: Option<&'static str>,
            protocol_name: Option<&'static str>,
            allowed: bool,
            want: i16,
        }
        let rows = [
            Row {
                name: "v5 null type, denied",
                version: 5,
                group_id: GROUP,
                protocol_type: None,
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::INCONSISTENT_GROUP_PROTOCOL,
            },
            Row {
                name: "v5 null name, allowed",
                version: 5,
                group_id: GROUP,
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: None,
                allowed: true,
                want: codes::INCONSISTENT_GROUP_PROTOCOL,
            },
            Row {
                name: "v4 null both, allowed: normal path to an unknown group",
                version: 4,
                group_id: GROUP,
                protocol_type: None,
                protocol_name: None,
                allowed: true,
                want: codes::UNKNOWN_MEMBER_ID,
            },
            Row {
                name: "v5 both present, denied",
                version: 5,
                group_id: GROUP,
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::GROUP_AUTHORIZATION_FAILED,
            },
            Row {
                name: "empty group id, allowed",
                version: 5,
                group_id: "",
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: true,
                want: codes::INVALID_GROUP_ID,
            },
            Row {
                name: "empty group id, denied",
                version: 5,
                group_id: "",
                protocol_type: Some(PROTOCOL_TYPE),
                protocol_name: Some(PROTOCOL_NAME),
                allowed: false,
                want: codes::GROUP_AUTHORIZATION_FAILED,
            },
        ];
        let (denied_handle, _denied_dir) = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let (allowed_handle, _allowed_dir) =
            start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
        request_identity!((principal, peer, ctx), principal("alice"), context);

        for r in rows {
            let broker = if r.allowed {
                allowed_handle.broker_arc_for_test()
            } else {
                denied_handle.broker_arc_for_test()
            };
            let req = SyncGroupRequest {
                group_id: r.group_id.into(),
                member_id: "member-a".into(),
                generation_id: 1,
                protocol_type: r.protocol_type.map(String::from),
                protocol_name: r.protocol_name.map(String::from),
                ..Default::default()
            };

            let resp = handle(&broker, req, r.version, &ctx)
                .await
                .expect("SyncGroup");

            let expected = SyncGroupResponse {
                error_code: r.want,
                ..Default::default()
            };
            assert!(resp == expected, "{}: {resp:?}", r.name);
        }
        denied_handle.shutdown().await;
        allowed_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_group_read_and_preserves_error_response_shape() {
        let version = sync_group_response::MAX_VERSION;
        let (broker_handle, _dir) = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((principal, peer, ctx), principal("alice"), context);
        let req = sync_request()
            .member_id("member-a".into())
            .generation_id(1)
            .call();

        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("SyncGroup");

        let expected = krabka_macros::unthrottled_wire!(SyncGroupResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            protocol_type: None,
            protocol_name: None,
            assignment: Bytes::new(),
        });
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_success_preserves_assignment_and_kip559_protocol_fields() {
        let version = sync_group_response::MAX_VERSION;
        broker_fixture!((broker_handle, _dir, broker), group_allow_all);
        request_identity!((principal, peer, ctx), principal("alice"), context);
        let JoinedMember {
            member_id,
            generation_id,
        } = bootstrap_member(&broker, &ctx).await;
        let assignment = Bytes::from_static(b"assignment-payload");
        let req = sync_request()
            .member_id(member_id.clone())
            .generation_id(generation_id)
            .assignments(vec![SyncGroupRequestAssignment {
                member_id,
                assignment: assignment.clone(),
                ..Default::default()
            }])
            .call();

        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("SyncGroup");

        let expected = krabka_macros::unthrottled_wire!(SyncGroupResponse {
            error_code: codes::NONE,
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some(PROTOCOL_NAME.into()),
            assignment,
        });
        assert!(resp == expected, "{resp:?}");
        broker_handle.shutdown().await;
    }
}
