//! `ConsumerGroupHeartbeat` (`api_key` 68), from the KIP-848 next-gen consumer
//! group protocol. It routes the request to the per-group actor in
//! `GroupCoordinator`.

use {
    crate::{
        codes,
        coordinator::unified::{
            actor::{GroupActorMessage, GroupKindTag},
            config::NextGenConfig,
            regex_resolver::ImageTopicRegexResolver,
        },
        handlers::{ErrorResponse as _, group_version_disabled},
        task_util::ask,
    },
    krabka_protocol::owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    },
};

context_handler! {
    ConsumerGroupHeartbeatRequest => ConsumerGroupHeartbeatResponse,
    (broker, req, version, ctx),
    {
        let coordinator = broker.group_coordinator.clone();
        // Read the offset BEFORE the image, not after: if a record commits in
        // between, `image` may reflect it while `metadata_offset` does not. The
        // regex resolution the actor may make from `image` is stamped with
        // `metadata_offset`, and an offset older than its image only makes a
        // later refresh happen once too often, never once too rarely.
        let metadata_offset = broker.controller.current_metadata_offset();
        let image = broker.controller.current_image();

        // ── Protocol gate ───────────────────────────────────────────
        // Kafka's `handleConsumerGroupHeartbeat` checks whether the
        // consumer-group protocol is available BEFORE any ACL check, so a
        // disabled protocol answers `UNSUPPORTED_VERSION` even to a caller
        // with no ACLs on the group at all. KIP-848 / KIP-584: the next-gen
        // protocol is gated on a finalized group.version >= 1. Below that —
        // including UNFINALIZED, which means disabled — reject so the client
        // falls back to the classic protocol.
        // `isConsumerGroupProtocolEnabled` also needs `consumer` among the
        // configured rebalance protocols, and answers the same way without it.

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        if let Some(error_code) = crate::handlers::acl_gates::group_protocol_refusal(
            !group_version_disabled(&image) && coordinator.config.next_gen_enabled(),
            broker,
            &image,
            ctx,
            &req.group_id,
        ) {
            return Ok(reply(error_code, None));
        }

        // `Describe` on every distinct name in `subscribed_topic_names`
        // (Kafka's `filterByAuthorized(request.context, DESCRIBE, TOPIC,
        // subscribedTopicSet)`). Any denial fails the WHOLE heartbeat with
        // `TOPIC_AUTHORIZATION_FAILED` (29); the group is never touched, so an
        // unauthorized caller cannot learn the denied topic's id or
        // partitions by being admitted as a member. This runs before
        // `group_coordinator_error` -- Kafka authorizes the request before it
        // ever reaches coordinator routing, so an unauthorized subscription
        // must not be masked by `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`.
        crate::handlers::acl_gates::authorize_subscription!(
            (broker, &image, ctx), req.subscribed_topic_names.as_deref() => reply
        );

        // `GroupCoordinatorService.consumerGroupHeartbeat` validates the
        // request before it routes it to a coordinator shard.
        if let Err(refused) = validate_request(&req, version, &coordinator.config) {
            return Ok(*refused);
        }

        // `subscribed_topic_regex` (KIP-848 v1+): the actor resolves the
        // group's patterns against the image read above, with this principal's
        // `Describe` decisions, when a heartbeat needs them resolved or
        // refreshed -- Kafka's `TopicRegexResolver.resolveRegularExpressions`
        // with the request context of the heartbeat. A pattern that fails to
        // compile is rejected by the actor's own
        // `check_subscribed_topic_regex` with `INVALID_REGULAR_EXPRESSION`
        // before any member state changes.
        let regex_resolver = std::sync::Arc::new(ImageTopicRegexResolver::new(
            image,
            metadata_offset,
            broker.config.authorizer.clone(),
            ctx.principal.clone(),
            *ctx.peer,
        ));

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return Ok(reply(error_code, None));
        }

        // Kafka creates a consumer group only on a join, and answers
        // GROUP_ID_NOT_FOUND for a missing group on any other epoch and for a
        // share or streams group, without touching any group.
        if let Some(message) = coordinator.consumer_group_lookup_error(&req.group_id, req.member_epoch)
        {
            return Ok(reply(codes::GROUP_ID_NOT_FOUND, Some(message)));
        }

        // Route to the one actor for this id, spawning a consumer-kind actor if
        // the id is brand-new. Both RPC families reach the same actor; a classic
        // group rejects a next-gen heartbeat from inside the actor's `Heartbeat`
        // arm (replying `GROUP_ID_NOT_FOUND`), which is where the per-group kind
        // lock now lives.
        let handle = coordinator.get_or_create_group(&req.group_id, GroupKindTag::Consumer);
        let asked = ask(&handle.tx, |reply| GroupActorMessage::Heartbeat {
            request: req,
            client_id: ctx.client_id.unwrap_or_default().to_owned(),
            client_host: ctx.client_host(),
            regex_resolver,
            reply,
        })
        .await;
        Ok(asked.unwrap_or_else(|error| {
            reply(
                crate::handlers::coordinator_routing::group_actor_error_code(error),
                None,
            )
        }))
    }
}

/// The early refusal: `code` carrying `message`.
fn reply(code: i16, message: Option<String>) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse::error(code, message)
}

/// The version from which a consumer must generate its own member id,
/// Kafka's `CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION`.
const CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION: i16 = 1;
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// Kafka's `GroupCoordinatorService.throwIfConsumerGroupHeartbeatRequestIsInvalid`.
///
/// A refusal carries only the error code and Kafka's message.
fn validate_request(
    req: &ConsumerGroupHeartbeatRequest,
    version: i16,
    config: &NextGenConfig,
) -> Result<(), Box<ConsumerGroupHeartbeatResponse>> {
    let invalid = |message: &str| {
        Box::new(ConsumerGroupHeartbeatResponse::error(
            codes::INVALID_REQUEST,
            Some(message.to_string()),
        ))
    };
    // `Utils.throwIfEmptyString`: a present value that trims to nothing.
    let blank = |value: Option<&str>| value.is_some_and(|value| value.trim().is_empty());

    if (version >= CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION
        || req.member_epoch > 0
        || req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH)
        && blank(Some(&req.member_id))
    {
        return Err(invalid("MemberId can't be empty."));
    }
    if blank(Some(&req.group_id)) {
        return Err(invalid("GroupId can't be empty."));
    }
    if blank(req.instance_id.as_deref()) {
        return Err(invalid("InstanceId can't be empty."));
    }
    if blank(req.rack_id.as_deref()) {
        return Err(invalid("RackId can't be empty."));
    }

    if req.member_epoch == 0 {
        if req.rebalance_timeout_ms == -1 {
            return Err(invalid(
                "RebalanceTimeoutMs must be provided in first request.",
            ));
        }
        if req
            .topic_partitions
            .as_ref()
            .is_none_or(|partitions| !partitions.is_empty())
        {
            return Err(invalid("TopicPartitions must be empty when (re-)joining."));
        }
        if req.subscribed_topic_names.is_none() && req.subscribed_topic_regex.is_none() {
            return Err(invalid(
                "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when \
                 (re-)joining.",
            ));
        }
    } else if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        if req.instance_id.is_none() {
            return Err(invalid("InstanceId can't be null."));
        }
    } else if req.member_epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        return Err(invalid("MemberEpoch is invalid."));
    }

    if let Some(name) = req
        .server_assignor
        .as_deref()
        .filter(|name| !config.assignor_enabled(name))
    {
        let supported: Vec<&str> = config
            .assignors
            .iter()
            .map(|assignor| assignor.name())
            .collect();
        return Err(Box::new(ConsumerGroupHeartbeatResponse {
            error_code: codes::UNSUPPORTED_ASSIGNOR,
            error_message: Some(format!(
                "ServerAssignor {name} is not supported. Supported assignors: {}.",
                supported.join(", ")
            )),
            ..Default::default()
        }));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            broker::{Broker, BrokerHandle},
            coordinator::unified::config::NextGenConfig,
            handlers::{
                encode_response,
                group_heartbeat_test_support::{
                    GroupFeatureLevel, acl_authorizer, alice, check_group_read_denied,
                    describe_acl, grant_group_read, group_read_acl, image_with_group_version,
                    set_group_version,
                    subscribed_names_describe_denied_table as subscribed_names_describe_denied_cases,
                    topic_with_partitions, versioned_group_broker,
                },
                group_read_denied,
            },
            task_util::ask,
            test_support::{
                KafkaErrorCode, peer, principal, request_context, start_group_broker, test_ctx,
            },
        },
        assert2::assert,
        krabka_metadata::{MetadataImage, MetadataRecord},
        krabka_protocol::Decode,
        std::sync::Arc,
    };

    const VERSION: i16 = krabka_protocol::owned::consumer_group_heartbeat_request::MAX_VERSION;

    fn request(group_id: &str) -> ConsumerGroupHeartbeatRequest {
        subscription_request(group_id, "member-a", 0)
    }

    fn subscription_request(
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
    ) -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_names: Some(vec!["topic-a".into()]),
            ..Default::default()
        }
    }

    crate::test_support::context_helper!(client_id = "consumer-group-heartbeat-test");

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        let fresh = MetadataImage::new(uuid::Uuid::nil());
        assert!(group_version_disabled(&fresh));

        let enabled = image_with_group_version(GroupFeatureLevel(1));
        assert!(!group_version_disabled(&enabled));

        let disabled = image_with_group_version(GroupFeatureLevel(0));
        assert!(group_version_disabled(&disabled));
    }

    /// Kafka's `throwIfConsumerGroupHeartbeatRequestIsInvalid`, row by row:
    /// (label, version, request, whole expected refusal or `None`).
    #[test]
    fn validate_request_follows_kafka() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

        let join = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            topic_partitions: Some(vec![]),
            ..Default::default()
        };
        let steady = ConsumerGroupHeartbeatRequest {
            member_epoch: 3,
            rebalance_timeout_ms: -1,
            subscribed_topic_names: None,
            topic_partitions: None,
            ..join.clone()
        };
        let invalid = |message: &str| {
            Some(ConsumerGroupHeartbeatResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(message.into()),
                ..Default::default()
            })
        };
        let rows = [
            ("valid join", 1, join.clone(), None),
            ("valid steady", 1, steady.clone(), None),
            (
                "v0 join mints the member id",
                0,
                ConsumerGroupHeartbeatRequest {
                    member_id: String::new(),
                    ..join.clone()
                },
                None,
            ),
            (
                "v1 empty member id",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_id: " ".into(),
                    ..join.clone()
                },
                invalid("MemberId can't be empty."),
            ),
            (
                "v0 leave with empty member id",
                0,
                ConsumerGroupHeartbeatRequest {
                    member_id: String::new(),
                    member_epoch: -1,
                    ..steady.clone()
                },
                invalid("MemberId can't be empty."),
            ),
            (
                "empty group id",
                1,
                ConsumerGroupHeartbeatRequest {
                    group_id: String::new(),
                    ..join.clone()
                },
                invalid("GroupId can't be empty."),
            ),
            (
                "empty instance id",
                1,
                ConsumerGroupHeartbeatRequest {
                    instance_id: Some(String::new()),
                    ..join.clone()
                },
                invalid("InstanceId can't be empty."),
            ),
            (
                "empty rack id",
                1,
                ConsumerGroupHeartbeatRequest {
                    rack_id: Some(String::new()),
                    ..join.clone()
                },
                invalid("RackId can't be empty."),
            ),
            (
                "join without rebalance timeout",
                1,
                ConsumerGroupHeartbeatRequest {
                    rebalance_timeout_ms: -1,
                    ..join.clone()
                },
                invalid("RebalanceTimeoutMs must be provided in first request."),
            ),
            (
                "join with null owned partitions",
                1,
                ConsumerGroupHeartbeatRequest {
                    topic_partitions: None,
                    ..join.clone()
                },
                invalid("TopicPartitions must be empty when (re-)joining."),
            ),
            (
                "join with owned partitions",
                1,
                ConsumerGroupHeartbeatRequest {
                    topic_partitions: Some(vec![TopicPartitions::default()]),
                    ..join.clone()
                },
                invalid("TopicPartitions must be empty when (re-)joining."),
            ),
            (
                "join without a subscription",
                1,
                ConsumerGroupHeartbeatRequest {
                    subscribed_topic_names: None,
                    ..join.clone()
                },
                invalid(
                    "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when \
                     (re-)joining.",
                ),
            ),
            (
                "static leave without instance id",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_epoch: -2,
                    ..steady.clone()
                },
                invalid("InstanceId can't be null."),
            ),
            (
                "epoch below -2",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_epoch: -3,
                    ..steady.clone()
                },
                invalid("MemberEpoch is invalid."),
            ),
            (
                "unknown assignor",
                1,
                ConsumerGroupHeartbeatRequest {
                    server_assignor: Some("sticky".into()),
                    ..join.clone()
                },
                Some(ConsumerGroupHeartbeatResponse {
                    error_code: codes::UNSUPPORTED_ASSIGNOR,
                    error_message: Some(
                        "ServerAssignor sticky is not supported. Supported assignors: uniform, \
                         range."
                            .into(),
                    ),
                    ..Default::default()
                }),
            ),
        ];
        let config = NextGenConfig::default();
        for (label, version, request, want) in rows {
            let got = validate_request(&request, version, &config)
                .err()
                .map(|e| *e);
            assert!(got == want, "{label}");
        }
    }

    #[test]
    fn error_response_preserves_error_code() {
        let resp = ConsumerGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None);
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        check_group_read_denied().call();

        let bytes = encode_response(
            &ConsumerGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None),
            VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ConsumerGroupHeartbeatResponse::decode(&mut cur, VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn group_read_denied_allows_allow_all_authorizer() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        request_identity!(
            (principal, peer, ctx),
            principal("ANONYMOUS"),
            client_id = "consumer-client"
        );

        assert!(!group_read_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &ctx,
            "g"
        ));
    }

    async fn check_denied_group_response(broker: &Broker, expected: KafkaErrorCode) {
        test_ctx!(ctx, "ANONYMOUS");
        let resp = handle(broker, request("denied-group"), VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == expected.0, "{resp:?}");
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        let authorizer = acl_authorizer();
        let (broker_handle, _dir, broker) =
            versioned_group_broker(Arc::new(authorizer), GroupFeatureLevel(1)).await;
        check_denied_group_response(&broker, KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED))
            .await;

        broker_handle.shutdown().await;
    }

    /// The protocol gate — `UNSUPPORTED_VERSION` when `group.version` is not
    /// finalized — runs BEFORE the group ACL check, matching Kafka's
    /// `handleConsumerGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        let authorizer = acl_authorizer();
        let (broker_handle, _dir) = start_group_broker(Arc::new(authorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        // `start_broker`'s bootstrap seeds every feature at its release
        // default for a modern metadata.version, which finalizes
        // group.version >= 1 automatically. Explicitly downgrade it back to
        // 0 (unfinalized/disabled) so this test observes the protocol gate.
        set_group_version(&broker, GroupFeatureLevel(0)).await;
        check_denied_group_response(&broker, KafkaErrorCode(codes::UNSUPPORTED_VERSION)).await;

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let (broker_handle, _dir, broker) =
            versioned_group_broker(Arc::new(AllowAllAuthorizer), GroupFeatureLevel(1)).await;
        request_identity!((principal, peer, ctx), principal("ANONYMOUS"), test_context);

        let resp = handle(&broker, request("identity-group"), VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_group("identity-group", GroupKindTag::Consumer);
        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("consumer group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "consumer-group-heartbeat-test");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let member_id = view.members[0].member_id.clone();
        let member_epoch = view.members[0].member_epoch;
        let peer = std::net::SocketAddr::from(([127, 0, 0, 2], 9093));
        let ctx = request_context(&principal, &peer, "consumer-client-b");
        let req = subscription_request("identity-group", &member_id, member_epoch);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat identity refresh");
        assert!(resp.error_code == 0);

        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("refreshed consumer group view");
        assert!(view.members[0].client_id == "consumer-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    /// Kafka's `getOrMaybeCreateConsumerGroup` and `consumerGroupLeave`: only a
    /// join creates a consumer group, a missing group answers
    /// `GROUP_ID_NOT_FOUND` on any other epoch, and a share or streams group is
    /// not a consumer group. Each row sends one heartbeat for its own group id
    /// and compares the whole response; `None` expects an accepted join.
    #[tokio::test]
    async fn handle_creates_consumer_group_only_on_join_as_kafka_does() {
        let (broker_handle, _dir, broker) =
            versioned_group_broker(Arc::new(AllowAllAuthorizer), GroupFeatureLevel(1)).await;
        let coordinator = &broker.group_coordinator;
        coordinator.mark_share("share");
        coordinator.mark_streams("streams");
        let _share_actor = coordinator.get_or_create_share("share-actor");
        test_ctx!(ctx, "ANONYMOUS");
        let not_found = |message: &str| {
            Some(ConsumerGroupHeartbeatResponse {
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
                not_found("Consumer group absent-heartbeat not found."),
            ),
            (
                "absent-leave",
                -1,
                not_found("Group absent-leave not found."),
            ),
            (
                "absent-static-leave",
                -2,
                not_found("Group absent-static-leave not found."),
            ),
            (
                "share",
                0,
                not_found("Group share is not a consumer group."),
            ),
            (
                "share-actor",
                0,
                not_found("Group share-actor is not a consumer group."),
            ),
            (
                "streams",
                3,
                not_found("Group streams is not a consumer group."),
            ),
            (
                "streams",
                -1,
                not_found("Group streams is not a consumer group."),
            ),
            ("joined", 0, None),
        ];

        for (group_id, member_epoch, expected) in rows {
            let req = ConsumerGroupHeartbeatRequest {
                group_id: group_id.into(),
                member_id: "m1".into(),
                instance_id: (member_epoch == -2).then(|| "i1".into()),
                member_epoch,
                rebalance_timeout_ms: if member_epoch == 0 { 30_000 } else { -1 },
                topic_partitions: (member_epoch == 0).then(Vec::new),
                subscribed_topic_names: Some(vec!["topic-a".into()]),
                ..Default::default()
            };
            let resp = handle(&broker, req, VERSION, &ctx)
                .await
                .expect("ConsumerGroupHeartbeat handler");
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}: {resp:?}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
        }

        // Only the join left a consumer group behind.
        assert!(coordinator.find("joined").is_some());
        for group_id in [
            "absent-heartbeat",
            "absent-leave",
            "absent-static-leave",
            "share",
        ] {
            assert!(coordinator.find(group_id).is_none(), "{group_id}");
        }
        broker_handle.shutdown().await;
    }

    #[test]
    fn subscribed_names_describe_denied_table() { subscribed_names_describe_denied_cases(); }

    // ── Explicit-name and regex `Describe` checks (issue #716) ─────────

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member — Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-a".
        let (broker_handle, _dir, broker) =
            versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        grant_group_read(&broker, "g").await;
        request_identity!(
            (principal, peer, ctx),
            alice(),
            client_id = "c",
            address = peer()
        );
        let req = subscription_request("g", "", 0);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        let actor = broker
            .group_coordinator
            .get_or_create_group("g", GroupKindTag::Consumer);
        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("consumer group view");
        assert!(view.members.is_empty(), "{view:?}");

        broker_handle.shutdown().await;
    }

    /// The security-fix regression case end to end: a regex subscription
    /// matches two topics, but the principal may only `Describe` one of
    /// them. The assignment the heartbeat returns holds only the authorized
    /// topic's partitions — Kafka's
    /// `TopicRegexResolver.filterTopicDescribeAuthorizedTopics`, exercised
    /// through the whole handler → actor → reconciler path.
    #[tokio::test]
    async fn handle_regex_subscription_assigns_only_describe_authorized_topics() {
        let (broker_handle, _dir, broker) =
            versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        let allowed_id = uuid::Uuid::from_u128(1);
        let denied_id = uuid::Uuid::from_u128(2);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("orders-eu")];
        records.extend(
            topic_with_partitions()
                .name("orders-eu")
                .topic_id(allowed_id)
                .node(node)
                .call(),
        );
        records.extend(
            topic_with_partitions()
                .name("orders-us")
                .topic_id(denied_id)
                .node(node)
                .call(),
        );
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        request_identity!(
            (principal, peer, ctx),
            alice(),
            client_id = "c",
            address = peer()
        );
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "regex-member".into(),
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_regex: Some("^orders-.*".into()),
            ..Default::default()
        };

        let joined = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(joined.error_code == codes::NONE, "{joined:?}");
        // Kafka writes the resolution after the join's batch, and the member's
        // next heartbeat computes the target that holds its topics.
        let resp = handle(
            &broker,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "regex-member".into(),
                member_epoch: joined.member_epoch,
                rebalance_timeout_ms: -1,
                ..Default::default()
            },
            VERSION,
            &ctx,
        )
        .await
        .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let assigned: std::collections::HashSet<uuid::Uuid> = resp
            .assignment
            .into_iter()
            .flat_map(|a| a.topic_partitions)
            .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
            .collect();
        assert!(assigned == std::collections::HashSet::from([allowed_id]));

        broker_handle.shutdown().await;
    }

    // ── Regex resolution that follows the metadata ─────────────────────

    /// The topics a member of group `g` holds, as its heartbeats report them.
    ///
    /// The response carries the assignment only when it changed, so the set
    /// keeps the last one.
    struct RegexMember {
        broker: Arc<Broker>,
        member_id: &'static str,
        member_epoch: i32,
        assigned: std::collections::HashSet<uuid::Uuid>,
    }

    impl RegexMember {
        fn new(broker: Arc<Broker>, member_id: &'static str) -> Self {
            Self {
                broker,
                member_id,
                member_epoch: 0,
                assigned: std::collections::HashSet::new(),
            }
        }

        /// Sends one heartbeat as `alice`, with `regex` only when it is the
        /// join or when the pattern changes, as the Java client does.
        async fn heartbeat(&mut self, regex: Option<&str>) {
            request_identity!(
                (principal, peer, ctx),
                alice(),
                client_id = "c",
                address = peer()
            );
            let joining = self.member_epoch == 0;
            let req = ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: self.member_id.into(),
                member_epoch: self.member_epoch,
                rebalance_timeout_ms: if joining { 30_000 } else { -1 },
                topic_partitions: joining.then(Vec::new),
                subscribed_topic_regex: regex.map(str::to_owned),
                ..Default::default()
            };
            let resp = handle(&self.broker, req, VERSION, &ctx)
                .await
                .expect("ConsumerGroupHeartbeat handler");
            assert!(resp.error_code == codes::NONE, "{resp:?}");
            self.member_epoch = resp.member_epoch;
            if let Some(assignment) = resp.assignment {
                self.assigned = assignment
                    .topic_partitions
                    .into_iter()
                    .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
                    .collect();
            }
        }

        /// Heartbeats without a pattern until the member holds exactly
        /// `expected`, or fails after ten seconds.
        async fn heartbeat_until_holding(&mut self, expected: &[uuid::Uuid]) {
            let expected: std::collections::HashSet<uuid::Uuid> =
                expected.iter().copied().collect();
            for _ in 0..200 {
                self.heartbeat(None).await;
                if self.assigned == expected {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!(
                "member {} holds {:?} and never held {expected:?}",
                self.member_id, self.assigned
            );
        }
    }

    /// A broker whose group `g` and topics `topics` (each with two partitions
    /// and a `Describe` grant for alice) exist, and the broker's handle.
    async fn broker_with_described_topics(
        topics: &[(&str, uuid::Uuid)],
    ) -> (BrokerHandle, tempfile::TempDir) {
        let (broker_handle, dir, broker) =
            versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g")];
        for (name, id) in topics {
            records.push(describe_acl(name));
            records.extend(
                topic_with_partitions()
                    .name(name)
                    .topic_id(*id)
                    .node(node)
                    .call(),
            );
        }
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        (broker_handle, dir)
    }

    /// Kafka refreshes the resolution of a group's regular expressions when the
    /// metadata image has new topics (`lastMetadataImageWithNewTopics`), at the
    /// next heartbeat of any member. A Java client sends its pattern only when
    /// it joins, so a topic created after the join reaches the member only
    /// through that refresh.
    macro_rules! joined_regex_member {
        (($handle:ident, $directory:ident, $broker:ident, $member:ident), $topics:expr) => {
            let ($handle, $directory) = broker_with_described_topics($topics).await;
            let $broker = $handle.broker_arc_for_test();
            let mut $member = RegexMember::new($broker.clone(), "m1");
            $member.heartbeat(Some("^orders-.*")).await;
        };
    }

    #[tokio::test]
    async fn a_topic_created_after_the_join_is_assigned_to_a_member_that_never_resends_its_pattern()
    {
        let first = uuid::Uuid::from_u128(1);
        let second = uuid::Uuid::from_u128(2);
        joined_regex_member!((broker_handle, _dir, broker, member), &[(
            "orders-eu",
            first
        )]);
        member.heartbeat_until_holding(&[first]).await;

        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![describe_acl("orders-us")];
        records.extend(
            topic_with_partitions()
                .name("orders-us")
                .topic_id(second)
                .node(node)
                .call(),
        );
        broker
            .controller
            .submit_change(records)
            .await
            .expect("create a second topic");

        member.heartbeat_until_holding(&[first, second]).await;
        broker_handle.shutdown().await;
    }

    /// The resolution is made with the `Describe` grants of the heartbeat that
    /// refreshes it, so a grant that is revoked takes the topic from the member
    /// at a later heartbeat, again without the pattern.
    #[tokio::test]
    async fn revoking_describe_on_a_resolved_topic_removes_it_at_a_later_heartbeat() {
        let allowed = uuid::Uuid::from_u128(1);
        let revoked = uuid::Uuid::from_u128(2);
        joined_regex_member!((broker_handle, _dir, broker, member), &[
            ("orders-eu", allowed),
            ("orders-us", revoked)
        ]);
        member.heartbeat_until_holding(&[allowed, revoked]).await;

        broker
            .controller
            .submit_change(vec![MetadataRecord::V1DeleteAccessControlEntry(
                krabka_metadata::AclEntryFilter {
                    resource_type: Some(krabka_metadata::ResourceType::Topic),
                    resource_name: Some("orders-us".into()),
                    ..Default::default()
                },
            )])
            .await
            .expect("revoke the grant");

        member.heartbeat_until_holding(&[allowed]).await;
        broker_handle.shutdown().await;
    }
}
