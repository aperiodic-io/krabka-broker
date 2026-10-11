//! `ShareGroupHeartbeat` (`api_key` 76), KIP-932 share-group membership. The
//! handler routes the request to the per-group share actor in
//! `GroupCoordinator`.

use krabka_protocol::owned::{
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    share_group_heartbeat_response::ShareGroupHeartbeatResponse,
};

#[cfg(test)]
use crate::config::BrokerConfig;
use crate::{
    broker::Broker,
    codes,
    coordinator::unified::{GroupCoordinator, GroupType, share::actor::ShareGroupActorMessage},
    handlers::{ErrorResponse as _, group_coordinator_error},
    task_util::{AskError, ask},
};

/// Kafka's `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

context_handler! {
    ShareGroupHeartbeatRequest => ShareGroupHeartbeatResponse,
    (broker, req, _version, ctx),
    {
        let ng = broker.group_coordinator.clone();

        // ── Protocol gate ───────────────────────────────────────────
        // Kafka's `handleShareGroupHeartbeat` checks whether share groups are
        // enabled BEFORE any ACL check, so a disabled feature answers
        // `UNSUPPORTED_VERSION` even to a caller with no ACLs on the group at
        // all. They are enabled by a finalized `share.version` of 1.
        let image = broker.controller.current_image();

        // ── ACL preamble ────────────────────────────────────────────
        // KIP-932 share groups still gate membership on `Read` on
        // `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        if let Some(error_code) = crate::handlers::acl_gates::group_protocol_refusal(
            crate::features::share_groups_enabled(&image),
            broker,
            &image,
            ctx,
            &req.group_id,
        ) {
            return Ok(reply(error_code, None));
        }

        // Kafka's `KafkaApis.isMemberIdValid`: the member id must be set and
        // at most 36 characters long. The share consumer mints its own id, so
        // even a first join must carry one. `getErrorResponse` sets only the
        // code. This runs before the topic `Describe` check, so a malformed
        // request that names a denied topic answers `INVALID_REQUEST`.
        if !crate::handlers::share_fetch::member_id_is_valid(&req.member_id) {
            return Ok(reply(codes::INVALID_REQUEST, None));
        }

        // `Describe` on every distinct name in `subscribed_topic_names`
        // (Kafka's `filterByAuthorized(request.context, DESCRIBE, TOPIC,
        // subscribedTopicSet)`). Any denial fails the WHOLE heartbeat with
        // `TOPIC_AUTHORIZATION_FAILED` (29); the group is never touched, so an
        // unauthorized caller cannot learn a denied topic's id or partitions
        // by being admitted as a member. This runs before
        // `group_coordinator_error` -- Kafka authorizes the request before it
        // ever reaches coordinator routing, so an unauthorized subscription
        // must not be masked by `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`.
        crate::handlers::acl_gates::authorize_subscription!(
            (broker, &image, ctx), req.subscribed_topic_names.as_deref() => reply
        );

        // Kafka's `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`
        // runs before the operation reaches a coordinator shard.
        if let Some(message) = invalid_request_message(&req) {
            return Ok(reply(codes::INVALID_REQUEST, Some(message.to_owned())));
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return Ok(reply(error_code, None));
        }

        // Kafka creates a share group only on a join, and answers
        // GROUP_ID_NOT_FOUND for a missing group on any other epoch and for a
        // group of another type, without touching any group.
        if let Some(message) = share_group_lookup_error(&ng, &req.group_id, req.member_epoch) {
            return Ok(reply(codes::GROUP_ID_NOT_FOUND, Some(message)));
        }

        ng.mark_share(&req.group_id);
        let handle = ng.get_or_create_share(&req.group_id);
        let group_id = req.group_id.clone();
        let asked = ask(&handle.tx, |reply| ShareGroupActorMessage::Heartbeat {
            request: req,
            client_id: ctx.client_id.unwrap_or_default().to_owned(),
            client_host: ctx.client_host(),
            reply,
        })
        .await;
        Ok(match asked {
            Ok(resp) => resp,
            Err(AskError::Closed) => reply(codes::COORDINATOR_LOAD_IN_PROGRESS, None),
            Err(AskError::Dropped) => {
                ShareGroupHeartbeatResponse::error(stopped_actor_code(broker, &group_id), None)
            }
        })
    }
}

/// The `GROUP_ID_NOT_FOUND` message for a heartbeat that must not reach a
/// share group, or `None` when the heartbeat may go to the share actor.
///
/// It follows Kafka's `GroupMetadataManager`: `getOrMaybeCreateShareGroup`
/// creates a missing group only when `member_epoch == 0` and refuses a group of
/// another type, and `shareGroupLeave` looks the group up through
/// `shareGroup`, which refuses a missing group with the generic message.
///
/// A classic or consumer group lives in the `groups` registry with no type
/// lock, and a streams group keeps its offset home there too, so a hit in
/// either registry is a group of another type.
fn share_group_lookup_error(
    coordinator: &GroupCoordinator,
    group_id: &str,
    member_epoch: i32,
) -> Option<String> {
    let not_share = || Some(format!("Group {group_id} is not a share group."));
    match coordinator.group_type(group_id) {
        Some(GroupType::Share) => return None,
        Some(_) => return not_share(),
        None => {}
    }
    if coordinator.find(group_id).is_some() || coordinator.find_streams(group_id).is_some() {
        return not_share();
    }
    if member_epoch == 0 || coordinator.find_share(group_id).is_some() {
        return None;
    }
    Some(if member_epoch == LEAVE_GROUP_MEMBER_EPOCH {
        format!("Group {group_id} not found.")
    } else {
        format!("Share group {group_id} not found.")
    })
}

/// Kafka's `Utils.throwIfEmptyString` test: a set value that Java's
/// `String.trim` reduces to nothing, which strips every character at or below
/// U+0020 from both ends.
fn blank(value: &str) -> bool {
    value.chars().all(|c| c <= ' ')
}

/// The `INVALID_REQUEST` message of Kafka's
/// `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`, or
/// `None` for a well-formed request.
fn invalid_request_message(req: &ShareGroupHeartbeatRequest) -> Option<&'static str> {
    if blank(&req.member_id) {
        return Some("MemberId can't be empty.");
    }
    if blank(&req.group_id) {
        return Some("GroupId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(blank) {
        return Some("RackId can't be empty.");
    }
    if req.member_epoch == 0 {
        if req
            .subscribed_topic_names
            .as_ref()
            .is_none_or(Vec::is_empty)
        {
            return Some("SubscribedTopicNames must be set in first request.");
        }
    } else if req.member_epoch < LEAVE_GROUP_MEMBER_EPOCH {
        return Some("MemberEpoch is invalid.");
    }
    None
}

/// The early refusal: `code` carrying `message`.
fn reply(code: i16, message: Option<String>) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse::error(code, message)
}

/// The code of a heartbeat that the group's actor dropped unanswered.
///
/// The actor stops when this broker unloads the group's offsets partition,
/// and when a write fails, and the heartbeats still in its mailbox stop with
/// it. Kafka fails an operation on a shard that unloads with
/// `NOT_COORDINATOR`, so the routing check runs again: it answers
/// `NOT_COORDINATOR` once another broker leads the partition. A broker that
/// still serves the group answers `COORDINATOR_NOT_AVAILABLE`, which the
/// client retries, and the retry starts a new actor from the committed state
/// of the group.
fn stopped_actor_code(broker: &Broker, group_id: &str) -> i16 {
    group_coordinator_error(broker, group_id).unwrap_or(codes::COORDINATOR_NOT_AVAILABLE)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::MetadataImage;
    use krabka_protocol::{Decode, owned::share_group_heartbeat_response};

    use super::*;
    use crate::{
        authorizer::AllowAllAuthorizer,
        handlers::{
            encode_response,
            group_heartbeat_test_support::{
                GroupTopicSetup, acl_authorizer, alice, describe_acl, grant_group_read,
                group_read_acl, subscribed_names_describe_denied_table as denied_table_fixture,
                topic_with_partitions,
            },
            group_read_denied,
        },
        task_util::ask,
        test_support::{
            ControllerPeerAllowed, PartitionCount, finalize_share_version, peer, principal,
            request_context, start_broker_with_authorizer as start_broker, test_ctx,
        },
    };

    const VERSION: i16 = share_group_heartbeat_response::MAX_VERSION;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer = acl_authorizer();
        let image = MetadataImage::new(uuid::Uuid::nil());
        request_identity!(
            (principal, peer, ctx),
            crate::test_support::principal("ANONYMOUS"),
            client_id = "share-client"
        );

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));
        assert!(!group_read_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &ctx,
            "g"
        ));

        let bytes = encode_response(
            &ShareGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None),
            VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ShareGroupHeartbeatResponse::decode(&mut cur, VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(cur.is_empty(), "response decoder consumed all bytes");
    }

    crate::test_support::context_helper!(client_id = "client-a");

    macro_rules! share_group_view {
        (($actor:ident, $view:ident), $broker:ident, $group:expr) => {
            let $actor = $broker.group_coordinator.get_or_create_share($group);
            let $view = crate::task_util::ask(&$actor.tx, |reply| {
                ShareGroupActorMessage::Describe { reply }
            })
            .await
            .expect("share group view");
        };
    }

    fn request(group_id: &str, subscribed: Vec<&str>) -> ShareGroupHeartbeatRequest {
        ShareGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(subscribed.into_iter().map(String::from).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_unsupported_version() {
        let version = VERSION;
        let dir = tempfile::TempDir::new().expect("tempdir");
        let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        let broker_handle = Broker::start(cfg).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        finalize_share_version(&broker, 0).await;
        test_ctx!(ctx, "ANONYMOUS");
        let req = request("g1", vec!["t1"]);

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let expected = ShareGroupHeartbeatResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// The protocol gate -- `UNSUPPORTED_VERSION` when `share.version` is
    /// finalized at 0 -- runs BEFORE the group ACL check, matching Kafka's
    /// `handleShareGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(acl_authorizer()))
        );
        finalize_share_version(&broker, 0).await;
        test_ctx!(ctx, "ANONYMOUS");
        let req = request("denied-group", vec!["t1"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(acl_authorizer()))
        );
        test_ctx!(ctx, "ANONYMOUS");
        let req = request("denied-group", vec!["t1"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");

        assert!(
            resp.error_code == codes::GROUP_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// A non-zero `member_epoch` with an empty `member_id` is malformed
    /// (Kafka rejects it before topic filtering) and answers
    /// `INVALID_REQUEST` even when the request also names a Describe-denied
    /// topic -- the malformed-request check must win, not
    /// `TOPIC_AUTHORIZATION_FAILED`.
    #[tokio::test]
    async fn handle_malformed_member_id_precedes_topic_authorization() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(acl_authorizer()))
        );
        grant_group_read(&broker, "g").await;
        request_identity!(
            (principal, peer, ctx),
            alice(),
            client_id = "c",
            address = peer()
        );
        // No Describe grant for "topic-b" -- if the malformed-request check
        // did not run first, this would answer `TOPIC_AUTHORIZATION_FAILED`.
        let req = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["topic-b".into()]),
            ..Default::default()
        };

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");

        broker_handle.shutdown().await;
    }

    /// Kafka's `KafkaApis.isMemberIdValid` and
    /// `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`:
    /// each malformed heartbeat answers `INVALID_REQUEST`, with the
    /// coordinator's message where Kafka sets one, and creates no group.
    #[tokio::test]
    async fn handle_refuses_malformed_heartbeats_as_kafka_does() {
        let version = VERSION;
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "ANONYMOUS")
        );
        let valid = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        let invalid = |message: Option<&str>| ShareGroupHeartbeatResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: message.map(str::to_owned),
            ..Default::default()
        };
        // (row, request, expected response)
        let rows = [
            (
                "empty member id",
                ShareGroupHeartbeatRequest {
                    member_id: String::new(),
                    ..valid.clone()
                },
                invalid(None),
            ),
            (
                "member id of 37 characters",
                ShareGroupHeartbeatRequest {
                    member_id: "m".repeat(37),
                    ..valid.clone()
                },
                invalid(None),
            ),
            (
                "blank member id",
                ShareGroupHeartbeatRequest {
                    member_id: "  ".into(),
                    ..valid.clone()
                },
                invalid(Some("MemberId can't be empty.")),
            ),
            (
                "empty group id",
                ShareGroupHeartbeatRequest {
                    group_id: String::new(),
                    ..valid.clone()
                },
                invalid(Some("GroupId can't be empty.")),
            ),
            (
                "blank group id",
                ShareGroupHeartbeatRequest {
                    group_id: " \t".into(),
                    ..valid.clone()
                },
                invalid(Some("GroupId can't be empty.")),
            ),
            (
                "empty rack id",
                ShareGroupHeartbeatRequest {
                    rack_id: Some(String::new()),
                    ..valid.clone()
                },
                invalid(Some("RackId can't be empty.")),
            ),
            (
                "first heartbeat with no subscription",
                ShareGroupHeartbeatRequest {
                    subscribed_topic_names: None,
                    ..valid.clone()
                },
                invalid(Some("SubscribedTopicNames must be set in first request.")),
            ),
            (
                "first heartbeat with an empty subscription",
                ShareGroupHeartbeatRequest {
                    subscribed_topic_names: Some(Vec::new()),
                    ..valid.clone()
                },
                invalid(Some("SubscribedTopicNames must be set in first request.")),
            ),
            (
                "member epoch below -1",
                ShareGroupHeartbeatRequest {
                    member_epoch: -2,
                    ..valid.clone()
                },
                invalid(Some("MemberEpoch is invalid.")),
            ),
        ];

        for (row, req, expected) in rows {
            let resp = handle(&broker, req, version, &ctx)
                .await
                .expect("ShareGroupHeartbeat handler");
            assert!(resp == expected, "{row}");
        }
        assert!(broker.group_coordinator.share_group_ids().is_empty());

        broker_handle.shutdown().await;
    }

    #[test]
    fn subscribed_names_describe_denied_table() {
        denied_table_fixture();
    }

    // ── Explicit-name `Describe` checks (issue #720) ────────────────────

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member -- Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-b".
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(acl_authorizer()))
        );
        broker
            .controller
            .submit_change(vec![group_read_acl("g"), describe_acl("topic-a")])
            .await
            .expect("grant group Read and Describe(topic-a)");
        request_identity!(
            (principal, peer, ctx),
            alice(),
            client_id = "c",
            address = peer()
        );
        let req = request("g", vec!["topic-a", "topic-b"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        share_group_view!((actor, view), broker, "g");
        assert!(view.members.is_empty(), "{view:?}");

        broker_handle.shutdown().await;
    }

    /// The end-to-end authorized case: both subscribed topics are
    /// `Describe`-authorized and the group is `Read`-authorized, so the
    /// heartbeat succeeds and creates a member.
    #[tokio::test]
    async fn handle_all_authorized_creates_member() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(ControllerPeerAllowed(acl_authorizer()))).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        let allowed_id = uuid::Uuid::from_u128(1);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("topic-a")];
        records.extend(topic_with_partitions(GroupTopicSetup {
            name: "topic-a",
            topic_id: allowed_id,
            partitions: PartitionCount(1),
            node,
        }));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topic");
        request_identity!(
            (principal, peer, ctx),
            alice(),
            client_id = "c",
            address = peer()
        );
        let req = request("g", vec!["topic-a"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        share_group_view!((actor, view), broker, "g");
        assert!(view.members.len() == 1, "{view:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = VERSION;
        let (broker_handle, _dir) = start_broker(std::sync::Arc::new(AllowAllAuthorizer)).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((principal, peer, ctx), principal("ANONYMOUS"), test_context);
        let req = ShareGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };

        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_share("identity-group");
        let view = ask(&actor.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("share group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "client-a");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = request_context(&principal, &peer, "client-b");
        let req = ShareGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: view.members[0].member_id.clone(),
            member_epoch: view.members[0].member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        };
        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("ShareGroupHeartbeat identity refresh");
        assert!(resp.error_code == 0);

        let view = ask(&actor.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("refreshed share group view");
        assert!(view.members[0].client_id == "client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    /// Kafka's `getOrMaybeCreateShareGroup` and `shareGroupLeave`: only a join
    /// creates a share group, and a group of another type is refused. Each row
    /// sends one heartbeat for its own group id and compares the whole
    /// response; `None` expects an accepted join.
    #[tokio::test]
    async fn handle_creates_share_group_only_on_join_as_kafka_does() {
        use crate::coordinator::unified::actor::GroupKindTag;

        let version = VERSION;
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        let coordinator = &broker.group_coordinator;
        let _classic = coordinator.get_or_create_classic("classic");
        let _consumer = coordinator.get_or_create_group("consumer", GroupKindTag::Consumer);
        coordinator.mark_streams("streams");
        test_ctx!(ctx, "ANONYMOUS");
        let not_found = |message: &str| {
            Some(ShareGroupHeartbeatResponse {
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: Some(message.into()),
                ..Default::default()
            })
        };
        // (group id, member epoch, expected response)
        let rows = [
            (
                "absent-heartbeat",
                3,
                not_found("Share group absent-heartbeat not found."),
            ),
            (
                "absent-leave",
                -1,
                not_found("Group absent-leave not found."),
            ),
            (
                "classic",
                0,
                not_found("Group classic is not a share group."),
            ),
            (
                "consumer",
                3,
                not_found("Group consumer is not a share group."),
            ),
            (
                "streams",
                -1,
                not_found("Group streams is not a share group."),
            ),
            ("joined", 0, None),
        ];

        for (group_id, member_epoch, expected) in rows {
            let req = ShareGroupHeartbeatRequest {
                group_id: group_id.into(),
                member_id: "m1".into(),
                member_epoch,
                subscribed_topic_names: Some(vec!["t".into()]),
                ..Default::default()
            };
            let resp = handle(&broker, req, version, &ctx)
                .await
                .expect("ShareGroupHeartbeat handler");
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
        }

        assert!(coordinator.share_group_ids() == vec!["joined".to_string()]);
        broker_handle.shutdown().await;
    }
}
