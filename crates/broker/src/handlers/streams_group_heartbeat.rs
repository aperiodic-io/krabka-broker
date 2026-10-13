//! `StreamsGroupHeartbeat` (`api_key` 88), the KIP-1071 streams rebalance
//! protocol. The handler routes the request to the per-group streams actor in
//! `GroupCoordinator`.
//!
//! It mirrors the KIP-932 share-group heartbeat handler
//! ([`super::share_group_heartbeat`]): decode, gate, `mark_streams` and
//! `get_or_create_streams`, send a `Heartbeat` actor message, await the
//! oneshot, then encode.
//!
//! Two gates gate it: the finalized `streams.version >= 1` feature, which is
//! KIP-1071 early access, AND the `streams_group.enable` config kill-switch.
//! BOTH must allow the request.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::streams::actor::{StreamsGroupActorMessage, response::error_resp},
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, acl_gates::group_protocol_refusal,
            any_topic_describe_denied, coordinator_routing::group_actor_error_code,
            group_coordinator_error_response, streams_protocol_enabled,
        },
        task_util::ask,
        time_util::now_ms,
    },
    krabka_protocol::owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};

mod creation;
pub(super) mod topic_authz;
mod validation;

pub(crate) async fn handle(
    broker: &Broker,
    req: StreamsGroupHeartbeatRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<StreamsGroupHeartbeatResponse, BrokerError> {
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();

    // KafkaApis answers UNSUPPORTED_VERSION before the group ACL when the
    // streams protocol is off: KIP-1071 gates it on a finalized
    // streams.version >= 1 (early access, default-disabled), and krabka
    // also on the `streams_group.enable` config kill-switch.

    // ── ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    if let Some(error_code) = group_protocol_refusal(
        streams_protocol_enabled(broker, &image),
        broker,
        &image,
        ctx,
        &req.group_id,
    ) {
        return Ok(reply(error_code, None));
    }

    // Kafka's `KafkaApis.handleStreamsGroupHeartbeat` reads the topology
    // straight off the wire, before the group coordinator ever sees the
    // request: a topology that names a Kafka internal topic or an
    // invalid topic name is `STREAMS_INVALID_TOPOLOGY`, and a required
    // topic (source, repartition sink, repartition source or changelog)
    // that `Describe` denies fails the whole request with
    // `TOPIC_AUTHORIZATION_FAILED` -- no partial disclosure, and the
    // group coordinator never runs.
    if let Some(topology) = req.topology.as_ref() {
        let required = topic_authz::required_topics(topology);
        if let Some(message) = topic_authz::invalid_topology_message(broker, &required) {
            return Ok(reply(codes::STREAMS_INVALID_TOPOLOGY, Some(message)));
        }
        if !required.is_empty()
            && any_topic_describe_denied(broker.config.authorizer.as_ref(), &image, ctx, &required)
        {
            return Ok(reply(codes::TOPIC_AUTHORIZATION_FAILED, None));
        }
    }

    // Kafka's `GroupCoordinatorService` checks the request before it
    // schedules the write on the coordinator, so a refused request changes
    // no group and never gets NOT_COORDINATOR.
    if let Some(validation::RequestError {
        code: error_code,
        message,
    }) = validation::request_error(&req, broker.config.features.unstable_api_versions)
    {
        return Ok(reply(error_code, Some(message)));
    }

    if let Some(response) = group_coordinator_error_response(broker, &req.group_id, reply) {
        return Ok(response);
    }

    // Kafka creates a streams group only on a join, in place of nothing or of
    // an empty classic group (a KIP-1071 cold upgrade converts it here), and
    // answers GROUP_ID_NOT_FOUND to anything else.
    if let Some(message) = ng
        .streams_group_lookup_error(&req.group_id, req.member_epoch, now_ms())
        .await?
    {
        return Ok(reply(codes::GROUP_ID_NOT_FOUND, Some(message)));
    }

    let group_id = req.group_id.clone();
    ng.mark_streams(&group_id);
    let handle = ng.get_or_create_streams(&group_id);
    let asked = ask(&handle.tx, |reply| StreamsGroupActorMessage::Heartbeat {
        request: Box::new(req),
        version,
        client_id: ctx.client_id.unwrap_or_default().to_owned(),
        client_host: ctx.client_host(),
        reply,
    })
    .await;
    let result = match asked {
        Ok(result) => result,
        Err(error) => {
            return Ok(reply(group_actor_error_code(error), None));
        }
    };
    let mut resp = result.response;
    // KafkaApis hands the internal topics that the coordinator asks for to
    // `AutoTopicCreationManager.createStreamsInternalTopics`, with the
    // principal of the caller.
    if !result.creatable_topics.is_empty() {
        creation::create_internal_topics(
            broker,
            &creation::Heartbeat {
                ctx,
                group_id: &group_id,
            },
            &mut resp,
            &result.creatable_topics,
        );
    }
    Ok(resp)
}

/// Kafka's error response: the code, message and generated defaults.
fn reply(code: i16, message: Option<String>) -> StreamsGroupHeartbeatResponse {
    error_resp(code, message)
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            authorizer::AllowAllAuthorizer,
            broker::{Broker, BrokerHandle},
            config::BrokerConfig,
            handlers::{
                RequestContext, encode_response,
                group_heartbeat_test_support::{
                    check_group_read_denied, streams_request, topic_with_partitions,
                },
                test_support::StartedBroker,
            },
            task_util::ask,
            test_support::{
                ControllerPeerAllowed, GrantsInPrincipalName, dispatch_wire, peer, principal,
                request_context, start_group_broker_with,
            },
        },
        assert2::assert,
        krabka_metadata::MetadataRecord,
        krabka_protocol::{Decode, owned::streams_group_heartbeat_response},
        std::{net::SocketAddr, sync::Arc},
    };

    /// Dispatch every version through its wire codec so omitted fields stay observable.
    async fn wire_heartbeat(
        broker: &Broker,
        ctx: &RequestContext<'_>,
        req: StreamsGroupHeartbeatRequest,
        version: i16,
    ) -> StreamsGroupHeartbeatResponse {
        dispatch_wire(
            broker,
            krabka_protocol::api_key::ApiKey::StreamsGroupHeartbeat as i16,
            version,
            &req,
            ctx,
        )
        .await
    }

    /// A valid join of member `m1` with a one-subtopology topology.
    fn request(group_id: &str) -> StreamsGroupHeartbeatRequest { streams_request(group_id, "m1") }

    /// Creates a one-partition topic on the test broker, so that a streams
    /// topology can read it.
    async fn create_source_topic(broker: &Broker, name: &str) {
        broker
            .controller
            .submit_change(
                topic_with_partitions()
                    .name(name)
                    .topic_id(uuid::Uuid::new_v4())
                    .partitions(1)
                    .node(broker.config.node_id)
                    .call(),
            )
            .await
            .expect("create the source topic");
    }

    /// Kafka creates the internal topics of a streams topology through
    /// `CreateTopics` with the principal of the caller, so the controller
    /// validates the configs and places the replicas. The creation runs in
    /// the background, so the heartbeat that starts it reports only the
    /// missing topic. A failure goes into the error cache, and the
    /// `MISSING_INTERNAL_TOPICS` status of the next heartbeat names it. Each
    /// row joins one member with a changelog topic on a one-broker cluster,
    /// waits until the creation ends, heartbeats again, and compares the
    /// created topic and the status details.
    ///
    /// A topology that sets no replication factor sends -1, as Kafka 4.3.1's
    /// `InternalTopicManager.toCreatableTopic` does, which `CreateTopics`
    /// resolves to `default.replication.factor` (1).
    #[tokio::test]
    async fn handle_creates_the_internal_topics_through_create_topics() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::{
            key_value::KeyValue, topic_info::TopicInfo,
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let ConfiguredBroker {
            handle: broker_handle,
            dir: _dir,
            broker,
        } = configured_streams_broker(|cfg| {
            cfg.streams_group.enable = true;
            cfg.default_replication_factor = 1;
        })
        .await;
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
        };
        let config = |name: &str, value: &str| KeyValue {
            key: name.into(),
            value: value.into(),
            ..Default::default()
        };
        // (group id, the changelog topic of the topology, the expected
        // (replication factor, configs) of the created topic or None, the
        // expected status details of the second heartbeat)
        create_source_topic(&broker, "in").await;
        let rows = [
            (
                "rf-unset",
                TopicInfo {
                    name: "rf-unset-changelog".into(),
                    topic_configs: vec![config("cleanup.policy", "compact")],
                    ..Default::default()
                },
                Some((1, vec![("cleanup.policy", "compact")])),
                Vec::<String>::new(),
            ),
            (
                "rf-too-high",
                TopicInfo {
                    name: "rf-too-high-changelog".into(),
                    replication_factor: 3,
                    ..Default::default()
                },
                None,
                vec![
                    "Internal topics are missing: rf-too-high-changelog; Creation failed: \
                     rf-too-high-changelog (Unable to replicate the partition 3 time(s): The \
                     target replication factor of 3 cannot be reached because only 1 broker(s) \
                     are registered or some brokers have all their log directories cordoned.)."
                        .to_string(),
                ],
            ),
            (
                "bad-config",
                TopicInfo {
                    name: "bad-config-changelog".into(),
                    topic_configs: vec![config("cleanup.policy", "bogus")],
                    ..Default::default()
                },
                None,
                vec![format!(
                    "Internal topics are missing: bad-config-changelog; Creation failed: \
                     bad-config-changelog ({}).",
                    crate::config_keys::validate_topic_config_map(&maplit::btreemap! {
                        "cleanup.policy".to_string() => "bogus".to_string()
                    })
                    .expect_err("an unknown cleanup policy is refused")
                )],
            ),
        ];

        for (group_id, changelog, created, second_details) in rows {
            let mut req = request(group_id);
            let topology = req.topology.as_mut().expect("the join carries a topology");
            topology.subtopologies[0].state_changelog_topics = vec![changelog.clone()];

            let first = handle(&broker, req.clone(), version, &ctx)
                .await
                .expect("handle");
            assert!(first.error_code == codes::NONE, "{group_id}: {first:?}");
            assert!(
                status_details(&first)
                    == vec![format!("Internal topics are missing: {}", changelog.name)],
                "{group_id}: {first:?}"
            );

            wait_until_creation_ends(&broker, &changelog.name).await;
            let next = StreamsGroupHeartbeatRequest {
                member_epoch: first.member_epoch,
                topology: None,
                ..request(group_id)
            };
            let second = handle(&broker, next.clone(), version, &ctx)
                .await
                .expect("handle");
            assert!(second.error_code == codes::NONE, "{group_id}: {second:?}");
            assert!(
                status_details(&second) == second_details,
                "{group_id}: {second:?}"
            );

            let image = broker.controller.current_image();
            let topic = image.topic(&changelog.name);
            match created {
                Some((replication_factor, configs)) => {
                    let topic = topic.expect("the topic was created");
                    assert!(topic.replication_factor == replication_factor, "{group_id}");
                    let expected: std::collections::BTreeMap<String, String> = configs
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect();
                    assert!(
                        image.topic_config(&changelog.name) == Some(&expected),
                        "{group_id}"
                    );
                }
                None => assert!(topic.is_none(), "{group_id}"),
            }
        }
        broker_handle.shutdown().await;
    }

    /// The status details of `response`, in order.
    fn status_details(response: &StreamsGroupHeartbeatResponse) -> Vec<String> {
        response
            .status
            .iter()
            .flatten()
            .map(|status| status.status_detail.clone())
            .collect()
    }

    /// Waits until no creation of `topic` is in flight on `broker`.
    use crate::handlers::test_support::wait_until_creation_ends;

    /// Kafka's `handleStreamsGroupHeartbeat` reads the topology straight off
    /// the wire before the group coordinator sees it: a required topic (here,
    /// the one source topic) that names a Kafka internal topic or an invalid
    /// name is refused with `STREAMS_INVALID_TOPOLOGY` and Kafka's message,
    /// and no group is created.
    #[tokio::test]
    async fn handle_refuses_a_topology_naming_a_prohibited_or_invalid_topic() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [join_ready_broker()];
            user ["alice"];
        );

        // (group id, source topic, the expected error message)
        let rows = [
            (
                "internal-topic",
                "__consumer_offsets",
                "Use of Kafka internal topics __consumer_offsets in a Kafka Streams topology is \
                 prohibited.",
            ),
            (
                "invalid-name",
                "bad topic",
                "Topic names bad topic are not valid topic names.",
            ),
        ];

        for (group_id, source_topic, message) in rows {
            let mut req = request(group_id);
            req.topology
                .as_mut()
                .expect("the join carries a topology")
                .subtopologies[0]
                .source_topics = vec![source_topic.into()];

            let resp = handle(&broker, req.clone(), version, &ctx)
                .await
                .expect("handle");

            assert!(
                resp == crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::STREAMS_INVALID_TOPOLOGY,
                    Some(message.into()),
                ),
                "{group_id}"
            );
            assert!(
                broker.group_coordinator.find_streams(group_id).is_none(),
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `filterByAuthorized(DESCRIBE, TOPIC, requiredTopics)`: a
    /// principal that can `Read` the group but not `Describe` one of the
    /// topology's required topics gets `TOPIC_AUTHORIZATION_FAILED` (29) for
    /// the whole request, and the group coordinator never runs, so no group
    /// is created.
    #[tokio::test]
    async fn handle_refuses_a_required_topic_denied_for_describe() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let StartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker_with_grants().await;
        let broker = broker_handle.broker_arc_for_test();
        set_streams_version(&broker, 1).await;
        let peer = peer();
        // `Group:Read` only: the source topic `in` gets no `Describe`.
        let principal = principal("Group:Read");
        let ctx = test_context(&principal, &peer);

        let resp = handle(&broker, request("describe-denied"), version, &ctx)
            .await
            .expect("handle");

        assert!(
            resp == crate::coordinator::unified::streams::actor::response::error_resp(
                codes::TOPIC_AUTHORIZATION_FAILED,
                None,
            )
        );
        assert!(
            broker
                .group_coordinator
                .find_streams("describe-denied")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `CREATE` gate before `createStreamsInternalTopics`: `Create`
    /// on the `Cluster` once, else `Create` on each topic. A principal with
    /// only `Read` on the group and `Describe` on the topics gets none of
    /// the internal topics created, and the `MISSING_INTERNAL_TOPICS` status
    /// names them as unauthorized instead -- unlike the no-ACL-check path
    /// this replaces, which let any principal with group `Read` make the
    /// broker create arbitrary topics.
    #[tokio::test]
    async fn handle_reports_topics_unauthorized_to_create_in_status() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let StartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker_with_grants().await;
        let broker = broker_handle.broker_arc_for_test();
        set_streams_version(&broker, 1).await;
        create_source_topic(&broker, "in").await;
        let peer = peer();
        // `Read` on the group and `Describe` on the topics, but no `Create`
        // anywhere.
        let principal = principal("Group:Read+Topic:Describe");
        let ctx = test_context(&principal, &peer);

        let mut req = request("no-create-grant");
        let topology = req.topology.as_mut().expect("the join carries a topology");
        topology.subtopologies[0].state_changelog_topics = vec![TopicInfo {
            name: "no-create-grant-changelog".into(),
            ..Default::default()
        }];

        let resp = handle(&broker, req.clone(), version, &ctx)
            .await
            .expect("handle");

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let status = resp.status.unwrap_or_default();
        assert!(
            status
                .iter()
                .map(|s| s.status_detail.clone())
                .collect::<Vec<_>>()
                == vec![
                    "Internal topics are missing: no-create-grant-changelog; Unauthorized to \
                     CREATE on topics no-create-grant-changelog."
                        .to_string()
                ],
            "{status:?}"
        );
        let image = broker.controller.current_image();
        assert!(image.topic("no-create-grant-changelog").is_none());
        broker_handle.shutdown().await;
    }

    /// Kafka creates a streams group only on a join, and answers
    /// `GROUP_ID_NOT_FOUND` to a heartbeat or a leave for a group that does not
    /// exist and to any heartbeat for a group of another type
    /// (`getOrCreateStreamsGroup`, `getStreamsGroupOrThrow`, `streamsGroup`).
    /// Each row sends one request for its own group id, compares the whole
    /// response, and checks whether a streams group exists afterwards.
    #[tokio::test]
    async fn handle_finds_or_creates_the_streams_group_as_kafka_does() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, streams::actor::response::error_resp,
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [join_ready_broker()];
            user ["alice"];
        );
        broker.group_coordinator.mark_share("share");
        let _consumer = broker
            .group_coordinator
            .get_or_create_group("consumer", GroupKindTag::Consumer);
        let heartbeat = |group_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch,
            ..Default::default()
        };
        let not_found =
            |message: String| Some(error_resp(codes::GROUP_ID_NOT_FOUND, Some(message)));
        // (group id, request, expected error response or None for success,
        // a streams group exists afterwards)
        let rows = [
            (
                "absent-heartbeat",
                heartbeat("absent-heartbeat", 3),
                not_found("Streams group absent-heartbeat not found.".into()),
                false,
            ),
            (
                "absent-leave",
                heartbeat("absent-leave", -1),
                not_found("Group absent-leave not found.".into()),
                false,
            ),
            (
                "share",
                request("share"),
                not_found("Group share is not a streams group.".into()),
                false,
            ),
            (
                "consumer",
                request("consumer"),
                not_found("Group consumer is not a streams group.".into()),
                false,
            ),
            ("absent-join", request("absent-join"), None, true),
        ];

        for (group_id, req, expected, exists) in rows {
            let resp = handle(&broker, req.clone(), version, &ctx)
                .await
                .expect("handle");
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
            assert!(
                broker.group_coordinator.find_streams(group_id).is_some() == exists,
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `GroupCoordinatorService` refuses an invalid request before the
    /// coordinator runs it, so the response carries only the error and the
    /// group is not created.
    #[tokio::test]
    async fn handle_refuses_an_invalid_request_and_creates_no_group() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [join_ready_broker()];
            user ["alice"];
        );
        let req = StreamsGroupHeartbeatRequest {
            member_id: String::new(),
            ..request("invalid-join")
        };

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(resp.error_message == Some("MemberId can't be empty.".into()));
        assert!(
            broker
                .group_coordinator
                .find_streams("invalid-join")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    /// Kafka writes no record for a join that its coordinator refuses, so
    /// `getOrCreateStreamsGroup` never materializes the group: neither
    /// `ListGroups` nor `StreamsGroupDescribe` sees it, and the id keeps no
    /// type lock. Each row sends a join that the coordinator refuses for its
    /// own group id and compares the whole response; a later valid join then
    /// creates the group.
    #[tokio::test]
    async fn handle_leaves_no_group_behind_a_join_the_coordinator_refuses() {
        use {
            crate::coordinator::unified::streams::actor::response::error_resp,
            krabka_protocol::owned::{
                common::streams_group_heartbeat_request::topic_info::TopicInfo,
                streams_group_heartbeat_request::Subtopology,
            },
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(true)];
        };
        set_streams_version(&broker, 1).await;
        create_source_topic(&broker, "in").await;
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
        };
        let with_subtopology = |group_id: &str, subtopology: Subtopology| {
            let mut req = request(group_id);
            req.topology
                .as_mut()
                .expect("the join carries a topology")
                .subtopologies = vec![subtopology];
            req
        };
        // (group id, the refused join, the expected response)
        let rows = [
            (
                "no-source-topics",
                with_subtopology("no-source-topics", Subtopology {
                    subtopology_id: "0".into(),
                    ..Default::default()
                }),
                error_resp(
                    codes::STREAMS_INVALID_TOPOLOGY,
                    Some("No source topics found for subtopology 0".into()),
                ),
            ),
            (
                "never-written-repartition",
                with_subtopology("never-written-repartition", Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in".into()],
                    repartition_source_topics: vec![TopicInfo {
                        name: "never-written-repartition-topic".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                error_resp(
                    codes::STREAMS_INVALID_TOPOLOGY,
                    Some(
                        "Failed to compute number of partitions for all repartition topics, \
                         because a repartition source topic is never used as a sink topic."
                            .into(),
                    ),
                ),
            ),
        ];

        for (group_id, req, expected) in rows {
            let resp = handle(&broker, req.clone(), version, &ctx)
                .await
                .expect("handle");
            assert!(resp == expected, "{group_id}");
            let coordinator = &broker.group_coordinator;
            assert!(coordinator.find_streams(group_id).is_none(), "{group_id}");
            assert!(
                !coordinator
                    .streams_group_ids()
                    .contains(&group_id.to_owned()),
                "{group_id}"
            );
            assert!(coordinator.group_type(group_id).is_none(), "{group_id}");

            let resp = handle(&broker, request(group_id), version, &ctx)
                .await
                .expect("handle");
            assert!(resp.error_code == codes::NONE, "{group_id}");
            assert!(coordinator.find_streams(group_id).is_some(), "{group_id}");
        }
        broker_handle.shutdown().await;
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["streams-client"];
    }

    /// A started streams broker, its directory and a shared reference to it.
    struct ConfiguredBroker {
        handle: BrokerHandle,
        dir: tempfile::TempDir,
        broker: Arc<Broker>,
    }

    async fn configured_streams_broker(
        configure: impl FnOnce(&mut BrokerConfig),
    ) -> ConfiguredBroker {
        let HandlerStartedBroker {
            broker: handle,
            dir: directory,
        } = start_group_broker_with(|cfg| {
            cfg.authorizer = Arc::new(AllowAllAuthorizer);
            configure(cfg);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        set_streams_version(&broker, 1).await;
        ConfiguredBroker {
            handle,
            dir: directory,
            broker,
        }
    }

    async fn check_protocol_disabled(enabled: bool, level: i16, group_id: &str) {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(enabled)];
        };
        set_streams_version(&broker, level).await;
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
        };
        let resp = handle(&broker, request(group_id), version, &ctx)
            .await
            .expect("handle");
        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    /// Finalize the streams feature before constructing the joining caller's identity.
    async fn join_ready_broker() -> HandlerStartedBroker {
        let started = start_broker(true).await;
        set_streams_version(&started.broker.broker_arc_for_test(), 1).await;
        started
    }
    async fn start_broker(streams_enabled: bool) -> HandlerStartedBroker {
        start_group_broker_with(|cfg| {
            cfg.authorizer = Arc::new(AllowAllAuthorizer);
            cfg.streams_group.enable = streams_enabled;
        })
        .await
    }

    /// A broker whose authorizer grants exactly the operations named in the
    /// caller's principal (see [`crate::test_support::GrantsInPrincipalName`]),
    /// for the tests that drive a specific ACL gate rather than allow
    /// everything.
    async fn start_broker_with_grants() -> StartedBroker {
        let HandlerStartedBroker { broker, dir } = start_group_broker_with(|cfg| {
            cfg.authorizer = Arc::new(ControllerPeerAllowed(GrantsInPrincipalName));
            cfg.streams_group.enable = true;
        })
        .await;
        StartedBroker { broker, dir }
    }

    /// Writes a `streams.version` `FeatureLevelRecord` at `level` and waits
    /// until the image shows it. Level 0 removes the feature, as in Kafka.
    use crate::handlers::test_support::set_streams_version;

    /// `StreamsGroupHeartbeat` v1 changes only the response. Trunk's
    /// `GroupMetadataManager` sets the int64 `AcceptableRecoveryLag` from the
    /// group config and leaves the v0 int32 legacy field at 0, and KIP-1331's
    /// `TopologyDescriptionRequired` stays false while no topology description
    /// plugin is configured, as on this broker. The same join at v0 and
    /// at v1 therefore answers the same response apart from the lag, which v0
    /// does not carry. A refused v1 heartbeat answers the generated defaults,
    /// as Kafka's `getErrorResponse` does: -1 for the lag, false for the flag.
    #[tokio::test]
    async fn handle_answers_v1_with_the_recovery_lag_and_no_topology_description_request() {
        use crate::coordinator::unified::streams::actor::response::error_resp;

        const LAG: i64 = 4_321;
        let ConfiguredBroker {
            handle: broker_handle,
            dir: _dir,
            broker,
        } = configured_streams_broker(|cfg| {
            cfg.streams_group.acceptable_recovery_lag = LAG;
        })
        .await;
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
        };
        // Through the dispatch registry: v0 drops the int64 lag on the wire.
        let heartbeat = |req: StreamsGroupHeartbeatRequest, version: i16| {
            wire_heartbeat(&broker, &ctx, req, version)
        };

        let v0 = heartbeat(request("streams-app-v0"), 0).await;
        let v1 = heartbeat(request("streams-app-v1"), 1).await;

        assert!(v1.error_code == codes::NONE, "{v1:?}");
        assert!(
            v1 == StreamsGroupHeartbeatResponse {
                acceptable_recovery_lag: LAG,
                topology_description_required: false,
                ..v0.clone()
            }
        );
        assert!(v0.acceptable_recovery_lag == -1 && v0.acceptable_recovery_lag_legacy == 0);

        let refused = heartbeat(
            StreamsGroupHeartbeatRequest {
                group_id: "streams-app-absent".into(),
                member_id: "m1".into(),
                member_epoch: 3,
                ..Default::default()
            },
            1,
        )
        .await;
        assert!(
            refused
                == error_resp(
                    codes::GROUP_ID_NOT_FOUND,
                    Some("Streams group streams-app-absent not found.".into()),
                )
        );
        assert!(refused.acceptable_recovery_lag == -1 && !refused.topology_description_required);
        broker_handle.shutdown().await;
    }

    /// #972: a member that sends none of the tag keys that
    /// `group.streams.rack.aware.assignment.tags` (or the group's
    /// `streams.rack.aware.assignment.tags` override) names gets Kafka trunk's
    /// `MISSING_CLIENT_TAGS` status at version 1, after every other status,
    /// and not at version 0. Each row joins one member of a fresh group and
    /// compares the whole response with the version 0 answer of the same join.
    #[tokio::test]
    async fn handle_sends_missing_client_tags_at_version_1_only() {
        use {
            crate::coordinator::unified::streams::{
                config::KEY_RACK_AWARE_ASSIGNMENT_TAGS, topology::status,
            },
            krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status,
        };

        let ConfiguredBroker {
            handle: broker_handle,
            dir: _dir,
            broker,
        } = configured_streams_broker(|cfg| {
            cfg.streams_group.rack_aware_assignment_tags = vec!["zone".into()];
        })
        .await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1GroupConfig(
                krabka_metadata::GroupConfigRecord {
                    group_id: "overridden-v1".into(),
                    configs: maplit::btreemap! {
                        KEY_RACK_AWARE_ASSIGNMENT_TAGS.to_owned() => "rack, zone".to_owned(),
                    },
                },
            )])
            .await
            .expect("store the group override");
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["alice"];
        };
        // Through the dispatch registry: v0 drops the status the v1 adds.
        let heartbeat = |group_id: &str, version: i16| {
            let req = request(group_id);
            wire_heartbeat(&broker, &ctx, req, version)
        };
        let v0 = heartbeat("baseline-v0", 0).await;
        assert!(v0.error_code == codes::NONE, "{v0:?}");

        for (group_id, missing) in [
            ("broker-default-v1", "[zone]"),
            ("overridden-v1", "[rack, zone]"),
        ] {
            let v1 = heartbeat(group_id, 1).await;
            let mut statuses = v0.status.clone().unwrap_or_default();
            statuses.push(Status {
                status_code: status::MISSING_CLIENT_TAGS,
                status_detail: format!(
                    "Missing required client tags for rack-aware standby assignment: {missing}. \
                     Configure them via 'client.tag.<tagKey>' in your Streams config."
                ),
                ..Default::default()
            });
            assert!(
                v1 == StreamsGroupHeartbeatResponse {
                    acceptable_recovery_lag: broker.config.streams_group.acceptable_recovery_lag,
                    status: Some(statuses),
                    ..v0.clone()
                },
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_unfinalized_feature_returns_unsupported_version_with_read_allowed() {
        check_protocol_disabled(true, 0, "streams-app-disabled-feature").await;
    }

    #[tokio::test]
    async fn handle_disabled_config_returns_unsupported_version_when_feature_finalized() {
        check_protocol_disabled(false, 1, "streams-app-disabled-config").await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [join_ready_broker()];
            user ["alice"];
        );

        let resp = handle(&broker, request("identity-group"), version, &ctx)
            .await
            .expect("StreamsGroupHeartbeat handler");
        assert!(resp.error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_streams("identity-group");
        let view = ask(&actor.tx, |reply| StreamsGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("streams group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "streams-client");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = request_context(&principal, &peer, "streams-client-b");
        let req = StreamsGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: view.members[0].member_id.clone(),
            member_epoch: view.members[0].member_epoch,
            ..Default::default()
        };
        let resp = handle(&broker, req.clone(), version, &ctx)
            .await
            .expect("StreamsGroupHeartbeat identity refresh");
        assert!(resp.error_code == 0);

        let view = ask(&actor.tx, |reply| StreamsGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("refreshed streams group view");
        assert!(view.members[0].client_id == "streams-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse;

        check_group_read_denied().client_id("streams-client").call();

        let bytes = encode_response(
            &reply(codes::GROUP_AUTHORIZATION_FAILED, None),
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = StreamsGroupHeartbeatResponse::decode(
            &mut cur,
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
