//! `AlterShareGroupOffsets` (`api_key` 91), from KIP-932.
//!
//! The handler resets the share-partition start offset (SPSO) for the
//! requested partitions of an *empty* share group. As Kafka's
//! `GroupMetadataManager.alterShareGroupOffsets` does, it leaves the group
//! epoch alone: the group records the partitions as initializing, the
//! persister initializes them at the group's current epoch, and the group
//! records them as initialized. It rejects a non-empty group at the top level
//! with `NON_EMPTY_GROUP`.
//!
//! Authorization, per Kafka's `KafkaApis.handleAlterShareGroupOffsetsRequest`:
//!   - `Read` on `Group(group_id)` for the whole response.
//!   - `Read` on `Topic(name)` for each topic, checked BEFORE the
//!     unknown-topic lookup. On Deny, every requested partition of that topic
//!     gets `TOPIC_AUTHORIZATION_FAILED` (29) and its offset is left
//!     untouched.
//!
//! `network::dispatch` intercepts this RPC inline for the per-group `Read`
//! ACL gate, which needs the principal and the peer `SocketAddr`.

use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{
            GroupType,
            share::actor::{ResetPartition, ShareGroupActorMessage},
        },
        error::BrokerError,
        features::share_groups_enabled,
        handlers::{
            ApiVersion, ErrorResponse as _, RequestContext, denied_topics, group_coordinator_error,
            group_read_denied,
        },
        share_coordinator::coordinator::message::UNKNOWN_TOPIC_OR_PARTITION,
        task_util::{AskError, ask},
    },
    krabka_metadata::AclOperation,
    krabka_protocol::{
        owned::{
            alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
            alter_share_group_offsets_response::{
                AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
                AlterShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    },
};

pub(crate) async fn handle(
    broker: &Broker,
    req: AlterShareGroupOffsetsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<AlterShareGroupOffsetsResponse, BrokerError> {
    // Feature gate: share groups are on from a finalized `share.version` of 1,
    // and below it the RPC is unsupported.
    let image = broker.controller.current_image();
    if !share_groups_enabled(&image) {
        return Ok(AlterShareGroupOffsetsResponse::error(
            codes::UNSUPPORTED_VERSION,
            None,
        ));
    }

    let ng_opt = Some(broker.group_coordinator.clone());
    let gid = req.group_id;

    // ── ACL preamble ────────────────────────────────────
    // Per-group `Read` check, per Kafka's `handleAlterShareGroupOffsetsRequest`
    // (krabka previously checked `Alter`, which neither refuses a plain
    // `Alter` grant nor accepts the normal `Read`-only share-consumer grant).
    // On Deny → top-level `error_code = 30`.
    if group_read_denied(broker.config.authorizer.as_ref(), &image, ctx, &gid) {
        return Ok(AlterShareGroupOffsetsResponse::error(
            codes::GROUP_AUTHORIZATION_FAILED,
            None,
        ));
    }
    // Kafka's `GroupCoordinatorService.alterShareGroupOffsets` refuses the
    // empty group id before any group lookup. This structural check runs
    // before coordinator routing: an empty id hashes to some partition, and a
    // broker that does not lead it would otherwise answer `NOT_COORDINATOR`
    // instead of `INVALID_GROUP_ID`, so the error a client sees would depend
    // on which broker happened to receive the request.
    if gid.is_empty() {
        return Ok(AlterShareGroupOffsetsResponse::error(
            codes::INVALID_GROUP_ID,
            None,
        ));
    }
    if let Some(error_code) = group_coordinator_error(broker, &gid) {
        return Ok(AlterShareGroupOffsetsResponse::error(error_code, None));
    }
    // `GroupMetadataManager.getOrMaybeCreateShareGroup` throws
    // `GroupIdNotFoundException` for a group id already locked to another
    // protocol type. A share group not yet created (`None`) is fine: the
    // actor below creates and persists it.
    let existing_type = broker.group_coordinator.group_type(&gid);
    if let Some(existing_type) = existing_type
        && existing_type != GroupType::Share
    {
        return Ok(AlterShareGroupOffsetsResponse::error(
            codes::GROUP_ID_NOT_FOUND,
            None,
        ));
    }
    let group_already_exists = existing_type.is_some();

    // Per-topic `Read` ACL — per-partition `TOPIC_AUTHORIZATION_FAILED` on
    // Deny, checked BEFORE the unknown-topic lookup below. Decided up front
    // (borrowing `req.topics`, not owning it) because the loop below moves
    // each topic's partitions out of `req.topics`.
    let denied_topics = denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        AclOperation::Read,
        req.topics.iter().map(|t| t.topic_name.as_str()),
    );

    let mut responses: Vec<AlterShareGroupOffsetsResponseTopic> =
        Vec::with_capacity(req.topics.len());
    let mut actor_requests = Vec::new();
    let mut actor_response_slots = Vec::new();

    for rt in req.topics {
        let topic_name = rt.topic_name;
        let topic_denied = denied_topics.contains(&topic_name);

        if topic_denied {
            let partitions = rt
                .partitions
                .into_iter()
                .map(|rp| AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                })
                .collect();
            responses.push(AlterShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id: Uuid::default(),
                partitions,
                ..Default::default()
            });
            continue;
        }

        let topic_id = image.topic(&topic_name).map(|t| t.topic_id);

        let mut partitions: Vec<AlterShareGroupOffsetsResponsePartition> =
            Vec::with_capacity(rt.partitions.len());

        for rp in rt.partitions {
            let partition_exists = image.partition(&topic_name, rp.partition_index).is_some();
            let Some(topic_id) = topic_id.filter(|_| partition_exists) else {
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index: rp.partition_index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some(UNKNOWN_TOPIC_OR_PARTITION.to_owned()),
                    ..Default::default()
                });
                continue;
            };

            actor_response_slots.push((responses.len(), partitions.len()));
            actor_requests.push(ResetPartition {
                topic_id,
                topic_name: topic_name.clone(),
                partition: rp.partition_index,
                start_offset: rp.start_offset,
            });
            partitions.push(AlterShareGroupOffsetsResponsePartition {
                partition_index: rp.partition_index,
                error_code: codes::NONE,
                ..Default::default()
            });
        }

        responses.push(AlterShareGroupOffsetsResponseTopic {
            topic_name,
            topic_id: topic_id.map_or_else(Uuid::default, |id| Uuid(*id.as_bytes())),
            partitions,
            ..Default::default()
        });
    }

    // No partition survived denial/unknown-topic filtering, and the group id
    // does not exist yet: every response row already carries its final error
    // code, and there is nothing to send an actor. Answer now, without
    // claiming the group id as `Share` -- locking a not-yet-existing id for a
    // request that mutates nothing would block classic/consumer/streams use
    // of that id for the broker's lifetime while leaving no persisted trace
    // of the group (a phantom that vanishes on restart). An id that already
    // exists as a share group still goes to the actor below even with an
    // empty batch, so its own emptiness/existence checks (e.g.
    // `NON_EMPTY_GROUP`) still run.
    if actor_requests.is_empty() && !group_already_exists {
        let resp = AlterShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            responses,
            ..Default::default()
        };
        return Ok(resp);
    }

    // The actor checks emptiness and applies the complete requested batch in
    // one mailbox turn, so a heartbeat cannot join between the gate and a
    // reset. Its seed message is queued first when this is a recovered group.
    //
    // `mark_share` + `get_or_create_share` is Kafka's
    // `getOrMaybeCreateShareGroup(groupId, true)`: a group id with no prior
    // type lock is created as a share group here, exactly as the first
    // `ShareGroupHeartbeat` would create it.
    let ng = ng_opt.as_ref().expect("group coordinator is installed");
    ng.mark_share(&gid);
    let actor = ng.get_or_create_share(&gid);
    let asked = ask(&actor.tx, |reply| ShareGroupActorMessage::ResetOffsets {
        requests: actor_requests,
        reply,
    })
    .await;
    let actor_result = match asked {
        Ok(actor_result) => actor_result,
        Err(AskError::Closed) => {
            return Ok(AlterShareGroupOffsetsResponse::error(
                codes::COORDINATOR_NOT_AVAILABLE,
                None,
            ));
        }
        Err(AskError::Dropped) => {
            return Err(BrokerError::Share("share-group reset actor stopped".into()));
        }
    };
    let result_codes = match actor_result {
        Ok(result_codes) => result_codes,
        Err(error_code) => return Ok(AlterShareGroupOffsetsResponse::error(error_code, None)),
    };
    if result_codes.len() != actor_response_slots.len() {
        return Ok(AlterShareGroupOffsetsResponse::error(
            codes::COORDINATOR_NOT_AVAILABLE,
            None,
        ));
    }
    for ((topic_slot, partition_slot), error_code) in
        actor_response_slots.into_iter().zip(result_codes)
    {
        let topic_id = uuid::Uuid::from_bytes(responses[topic_slot].topic_id.0);
        let partition = &mut responses[topic_slot].partitions[partition_slot];
        partition.error_code = error_code;
        if error_code == codes::NONE {
            broker
                .share_partition_leaders
                .invalidate(&gid, topic_id, partition.partition_index);
        }
    }

    let resp = AlterShareGroupOffsetsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        responses,
        ..Default::default()
    };
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use {
        super::handle as alter_share_group_offsets_handle,
        crate::{
            authorizer::AuthorizationResult,
            broker::BrokerHandle,
            codes,
            coordinator::unified::{
                GroupType, ShareGroupSeed,
                share::actor::{ShareGroupActorHandle, ShareGroupActorMessage},
            },
            handlers::{
                RequestContext,
                create_topics::handle as create_topics_handle,
                test_support::{
                    self as handler_fixture_support, CreateTopicSetup, ShareRefusalCase,
                    ShareRefusalPolicy, configured_topic_request_from,
                },
            },
            share_coordinator::{
                coordinator::message::UNKNOWN_TOPIC_OR_PARTITION,
                handlers::test_support::lead_share_state_partitions,
            },
            test_support::{
                ShareApiSupport, StartedBroker as HandlerStartedBroker, start_share_broker,
            },
        },
        assert2::assert,
        krabka_protocol::{
            owned::{
                alter_share_group_offsets_request::{
                    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
                    AlterShareGroupOffsetsRequestTopic,
                },
                alter_share_group_offsets_response::{
                    self, AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
                    AlterShareGroupOffsetsResponseTopic,
                },
                create_topics_response,
                share_group_heartbeat_request::ShareGroupHeartbeatRequest,
                share_group_heartbeat_response::ShareGroupHeartbeatResponse,
            },
            primitives::uuid::Uuid,
        },
        std::sync::Arc,
    };

    /// Join the actor directly with the client identity used by the offset-reset cases.
    async fn joining_share_member(
        actor: &ShareGroupActorHandle,
        group: &str,
        member: &str,
        topics: Vec<String>,
    ) -> ShareGroupHeartbeatResponse {
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: ShareGroupHeartbeatRequest {
                    group_id: group.into(),
                    member_id: member.into(),
                    member_epoch: 0,
                    subscribed_topic_names: Some(topics),
                    ..Default::default()
                },
                client_id: "client-a".into(),
                client_host: "127.0.0.1".into(),
                reply: tx,
            })
            .await
            .expect("send heartbeat");
        rx.await.expect("heartbeat response")
    }
    const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str = UNKNOWN_TOPIC_OR_PARTITION;

    /// An authorizer with independently controllable answers for `Group`
    /// `Read`/`Alter` and per-topic `Read`, for the KIP-932
    /// `AlterShareGroupOffsets` ACL table (issue #727).
    #[derive(Debug, Default)]
    struct ScenarioAuthorizer {
        group_read: bool,
        group_alter: bool,
        denied_topics: Vec<String>,
    }

    krabka_macros::test_authorizer!(ScenarioAuthorizer, req; {
        use krabka_metadata::{AclOperation, ResourceType};
        let allowed = match (req.resource_type, req.operation) {
            (ResourceType::Group, AclOperation::Read) => self.group_read,
            (ResourceType::Group, AclOperation::Alter) => self.group_alter,
            (ResourceType::Topic, AclOperation::Read) => !self
                .denied_topics
                .iter()
                .any(|t| t.as_str() == req.resource_name),
            _ => true,
        };
        if allowed {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    });

    fn request(
        group_id: &str,
        topic_name: &str,
        partitions: &[i32],
    ) -> AlterShareGroupOffsetsRequest {
        AlterShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: topic_name.into(),
                partitions: partitions
                    .iter()
                    .map(|partition_index| AlterShareGroupOffsetsRequestPartition {
                        partition_index: *partition_index,
                        start_offset: 42,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    async fn create_topic(
        broker_handle: &BrokerHandle,
        ctx: &RequestContext<'_>,
        setup: CreateTopicSetup<'_>,
    ) {
        let broker = broker_handle.broker_arc_for_test();
        let topic_name = setup.topic;
        let version = create_topics_response::MAX_VERSION;
        let request = configured_topic_request_from(setup);
        let response = create_topics_handle(&broker, request, version, ctx)
            .await
            .expect("create topic");
        assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
        broker_handle
            .wait_until_partition_present(topic_name, 0)
            .await;
    }

    async fn create_default_topic(
        handle: &BrokerHandle,
        context: &RequestContext<'_>,
        topic: &str,
    ) {
        create_topic(handle, context, CreateTopicSetup {
            topic,
            ..Default::default()
        })
        .await;
    }

    #[tokio::test]
    async fn handle_error_scenarios_preserve_expected_rows() {
        struct AlterOffsetsInput {
            topic_name: &'static str,
            partitions: Vec<i32>,
        }
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<ShareRefusalCase<AlterOffsetsInput, AlterShareGroupOffsetsResponse>> = vec![
            ShareRefusalPolicy::unsupported(
                "disabled feature returns top-level unsupported version",
            )
            .with_input(
                AlterOffsetsInput {
                    topic_name: "missing",
                    partitions: vec![0],
                },
                krabka_macros::unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    responses: Vec::new(),
                }),
            ),
            ShareRefusalPolicy::group_denied(
                "denied group returns top-level authorization failure",
            )
            .with_input(
                AlterOffsetsInput {
                    topic_name: "missing",
                    partitions: vec![0],
                },
                krabka_macros::unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    responses: Vec::new(),
                }),
            ),
            ShareRefusalCase {
                name: "unknown topic preserves topic and partition fields",
                authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
                enabled: ShareApiSupport::Enabled,
                input: AlterOffsetsInput {
                    topic_name: "missing-topic",
                    partitions: vec![3, 5],
                },
                expected: krabka_macros::unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::NONE,
                    error_message: None,
                    responses: vec![krabka_macros::tagged_wire!(
                        AlterShareGroupOffsetsResponseTopic {
                            topic_name: "missing-topic".into(),
                            topic_id: Uuid::default(),
                            partitions: vec![
                                krabka_macros::tagged_wire!(
                                    AlterShareGroupOffsetsResponsePartition {
                                        partition_index: 3,
                                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                        error_message: Some(
                                            UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()
                                        ),
                                    }
                                ),
                                krabka_macros::tagged_wire!(
                                    AlterShareGroupOffsetsResponsePartition {
                                        partition_index: 5,
                                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                        error_message: Some(
                                            UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()
                                        ),
                                    }
                                ),
                            ],
                        }
                    )],
                }),
            },
        ];
        krabka_macros::share_refusal_matrix! {
            case(row) [cases];
            bind(broker_handle, _dir, broker, ctx, resp);
            invoke [alter_share_group_offsets_handle(&broker, request("g1", row.input.topic_name, &row.input.partitions), version, &ctx)];
        };
    }

    /// Issue #727: krabka checked `Alter` on the group and never checked
    /// topic `Read`. Kafka checks `Read` on the group and `Read` on each
    /// topic, the topic check running BEFORE the unknown-topic lookup. Every
    /// row uses a fresh broker and group id.
    ///
    /// `(group Read granted, group Alter granted, topic name is denied,
    /// topic exists, expected top-level code, expected partition code)`.
    #[tokio::test]
    async fn acl_scenarios_match_kafka_semantics() {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum GroupGrant {
            ReadOnly,
            AlterOnly,
        }
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum TopicState {
            Granted,
            Denied,
            Unknown,
        }
        struct AclRow {
            case: &'static str,
            group: GroupGrant,
            topic: TopicState,
            top: i16,
            partition: Option<i16>,
        }
        let rows: [AclRow; 4] = [
            AclRow {
                case: "group Read only is allowed (the normal share-consumer grant)",
                group: GroupGrant::ReadOnly,
                topic: TopicState::Granted,
                top: codes::NONE,
                partition: Some(codes::NONE),
            },
            AclRow {
                case: "group Alter only, with no Read, is refused",
                group: GroupGrant::AlterOnly,
                topic: TopicState::Granted,
                top: codes::GROUP_AUTHORIZATION_FAILED,
                partition: None,
            },
            AclRow {
                case: "group Read granted but the topic is denied",
                group: GroupGrant::ReadOnly,
                topic: TopicState::Denied,
                top: codes::NONE,
                partition: Some(codes::TOPIC_AUTHORIZATION_FAILED),
            },
            AclRow {
                case: "group Read granted, topic unknown to the cluster",
                group: GroupGrant::ReadOnly,
                topic: TopicState::Unknown,
                top: codes::NONE,
                partition: Some(codes::UNKNOWN_TOPIC_OR_PARTITION),
            },
        ];

        for (
            index,
            AclRow {
                case,
                group,
                topic,
                top,
                partition,
            },
        ) in rows.into_iter().enumerate()
        {
            let topic_name = "acl-topic";
            let authorizer = Arc::new(ScenarioAuthorizer {
                group_read: group == GroupGrant::ReadOnly,
                group_alter: group == GroupGrant::AlterOnly,
                denied_topics: if topic == TopicState::Denied {
                    vec![topic_name.to_string()]
                } else {
                    Vec::new()
                },
            });
            let HandlerStartedBroker {
                broker: broker_handle,
                dir: _dir,
            } = start_share_broker().authorizer(authorizer).call().await;
            let broker = broker_handle.broker_arc_for_test();
            krabka_macros::request_test_identity! {
                bind(ctx);
                user ["alice"];
            };
            let topic_exists = topic != TopicState::Unknown;
            if topic_exists {
                create_topic(&broker_handle, &ctx, CreateTopicSetup {
                    topic: topic_name,
                    ..Default::default()
                })
                .await;
                lead_share_state_partitions(&broker).await;
            }
            let topic_id_before = if topic_exists {
                let topic_id = broker
                    .controller
                    .current_image()
                    .topic(topic_name)
                    .expect("topic metadata")
                    .topic_id;
                broker
                    .group_coordinator
                    .share_persister()
                    .expect("share persister")
                    .read_summary("g-acl", topic_id, 0)
                    .await
                    .expect("read state")
            } else {
                None
            };

            let resp = alter_share_group_offsets_handle(
                &broker,
                request("g-acl", topic_name, &[0]),
                alter_share_group_offsets_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle");

            assert!(resp.error_code == top, "row {index} ({case}): {resp:?}");
            if let Some(expected_partition_code) = partition {
                assert!(
                    resp.responses[0].partitions[0].error_code == expected_partition_code,
                    "row {index} ({case}): {resp:?}"
                );
            }
            if topic_exists && partition != Some(codes::NONE) {
                // A denied or otherwise-refused topic must not have its
                // start offset reset.
                let topic_id = broker
                    .controller
                    .current_image()
                    .topic(topic_name)
                    .expect("topic metadata")
                    .topic_id;
                let after = broker
                    .group_coordinator
                    .share_persister()
                    .expect("share persister")
                    .read_summary("g-acl", topic_id, 0)
                    .await
                    .expect("read state");
                assert!(after == topic_id_before, "row {index} ({case})");
            }
            broker_handle.shutdown().await;
        }
    }

    /// Issue #944: an empty `group_id` is refused with `INVALID_GROUP_ID`
    /// (24), and a `group_id` already locked to another protocol type is
    /// refused with `GROUP_ID_NOT_FOUND` (69), before any actor is touched.
    #[tokio::test]
    async fn invalid_and_wrong_type_group_ids_are_refused() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [handler_fixture_support::start_share_allow_all()];
            user ["alice"];
        };

        let empty_id_resp = alter_share_group_offsets_handle(
            &broker,
            request("", "t", &[0]),
            alter_share_group_offsets_response::MAX_VERSION,
            &ctx,
        )
        .await
        .expect("handle empty id");
        assert!(empty_id_resp.error_code == codes::INVALID_GROUP_ID);

        let _ = broker.group_coordinator.get_or_create_classic("classic-g");
        broker.group_coordinator.mark_classic("classic-g");
        assert!(broker.group_coordinator.group_type("classic-g") == Some(GroupType::Classic));
        let wrong_type_resp = alter_share_group_offsets_handle(
            &broker,
            request("classic-g", "t", &[0]),
            alter_share_group_offsets_response::MAX_VERSION,
            &ctx,
        )
        .await
        .expect("handle wrong-type id");
        assert!(wrong_type_resp.error_code == codes::GROUP_ID_NOT_FOUND);
        // The classic lock must not have been disturbed.
        assert!(broker.group_coordinator.group_type("classic-g") == Some(GroupType::Classic));
        broker_handle.shutdown().await;
    }

    /// Issue #944: a share group that does not exist yet is created and
    /// persisted by `AlterShareGroupOffsets` — matching Kafka's
    /// `getOrMaybeCreateShareGroup(groupId, true)` — through its
    /// `ShareGroupStatePartitionMetadata` record alone, at the initial group
    /// epoch. The regression guard: once a partition is
    /// initialized this way, later member joins must not re-initialize it and
    /// change its start offset back.
    #[tokio::test]
    async fn alter_creates_and_persists_a_new_share_group() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [handler_fixture_support::start_share_allow_all()];
            user ["alice"];
        };
        create_default_topic(&broker_handle, &ctx, "new-topic").await;
        lead_share_state_partitions(&broker).await;
        let topic_id = broker
            .controller
            .current_image()
            .topic("new-topic")
            .expect("topic metadata")
            .topic_id;

        assert!(broker.group_coordinator.group_type("g-new").is_none());

        let response = alter_share_group_offsets_handle(
            &broker,
            request("g-new", "new-topic", &[0]),
            alter_share_group_offsets_response::MAX_VERSION,
            &ctx,
        )
        .await
        .expect("handle alter");
        assert!(response.error_code == codes::NONE, "{response:?}");
        assert!(response.responses[0].partitions[0].error_code == codes::NONE);

        assert!(broker.group_coordinator.group_type("g-new") == Some(GroupType::Share));
        let seed = broker
            .group_coordinator
            .cached_share_seed("g-new")
            .expect("share group persisted");
        assert!(seed.group_epoch == 1, "alter leaves the group epoch alone");
        assert!(
            seed.state_partition_metadata
                .initialized
                .iter()
                .any(|t| t.topic_id == topic_id && t.partitions == vec![0]),
            "the partition lands in the persisted initialized set: {:?}",
            seed.state_partition_metadata
        );

        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let (_, _, start_offset, _) = persister
            .read_summary("g-new", topic_id, 0)
            .await
            .expect("read state")
            .expect("state present");
        assert!(start_offset == krabka_log::Offset(42));

        // A member joining afterwards must not re-initialize the partition
        // and reset its start offset: `reconcile_share_state` skips any
        // `(topic_id, partition)` already in `state.initialized`.
        let actor = broker.group_coordinator.get_or_create_share("g-new");
        for member_id in ["m1", "m2"] {
            let resp =
                joining_share_member(&actor, "g-new", member_id, vec!["new-topic".into()]).await;
            assert!(resp.error_code == codes::NONE, "{resp:?}");
        }
        let (_, _, start_offset_after, _) = persister
            .read_summary("g-new", topic_id, 0)
            .await
            .expect("read state")
            .expect("state present");
        assert!(
            start_offset_after == krabka_log::Offset(42),
            "member joins must not move the start offset back"
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn active_group_rejects_the_whole_reset_batch() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [handler_fixture_support::start_share_allow_all()];
        };
        let coordinator = broker.group_coordinator.clone();

        coordinator.mark_share("busy");
        let actor = coordinator.get_or_create_share("busy");
        let resp = joining_share_member(&actor, "busy", "member-1", Vec::new()).await;
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };
        let response = alter_share_group_offsets_handle(
            &broker,
            request("busy", "missing", &[0]),
            alter_share_group_offsets_response::MAX_VERSION,
            &ctx,
        )
        .await
        .expect("handle reset");
        assert!(
            response.error_code == codes::NON_EMPTY_GROUP,
            "{response:?}"
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `alterShareGroupOffsets` writes no `ShareGroupMetadata` record
    /// and initializes every requested partition at the group's current
    /// epoch, which the share coordinator accepts again on a repeat because
    /// it fences only an older epoch. A partition the topic does not have
    /// answers `UNKNOWN_TOPIC_OR_PARTITION` and is never initialized.
    #[tokio::test]
    async fn reset_initializes_requested_valid_partitions_at_the_group_epoch() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [handler_fixture_support::start_share_allow_all()];
            user ["alice"];
        };
        create_default_topic(&broker_handle, &ctx, "reset-topic").await;
        lead_share_state_partitions(&broker).await;
        let persister = broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister");
        let topic_id = broker
            .controller
            .current_image()
            .topic("reset-topic")
            .expect("topic metadata")
            .topic_id;

        broker.group_coordinator.mark_share("g-reset");
        let actor = broker.group_coordinator.get_or_create_share("g-reset");
        let (seed_tx, seed_rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                group_epoch: 7,
                ..Default::default()
            }))
            .await
            .expect("send seed");
        // `Seed` has no reply; round-trip a `Describe` to know it was applied
        // before the alter request below is sent.
        actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: seed_tx })
            .await
            .expect("send describe");
        seed_rx.await.expect("describe reply");

        let reset_request = request("g-reset", "reset-topic", &[0, 9]);
        for _ in 0..2 {
            let response = alter_share_group_offsets_handle(
                &broker,
                reset_request.clone(),
                alter_share_group_offsets_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle reset");
            assert!(response.error_code == codes::NONE, "{response:?}");
            assert!(response.responses[0].partitions[0].error_code == codes::NONE);
            assert!(
                response.responses[0].partitions[1].error_code == codes::UNKNOWN_TOPIC_OR_PARTITION
            );

            let (state_epoch, _, start_offset, _) = persister
                .read_summary("g-reset", topic_id, 0)
                .await
                .expect("read state")
                .expect("state present");
            assert!(state_epoch == 7);
            assert!(start_offset == krabka_log::Offset(42));
            let seed = broker
                .group_coordinator
                .cached_share_seed("g-reset")
                .expect("share group persisted");
            assert!(seed.group_epoch == 7);
            assert!(
                seed.state_partition_metadata
                    == crate::coordinator::unified::share::persistence::ShareGroupStatePartitionMetadataValue {
                        initialized: vec![
                            crate::coordinator::unified::share::persistence::TopicPartitionsInfo {
                                topic_id,
                                topic_name: "reset-topic".to_owned(),
                                partitions: vec![0],
                            }
                        ],
                        ..Default::default()
                    }
            );
        }

        let state = persister
            .read_summary("g-reset", topic_id, 9)
            .await
            .expect("read unrequested state");
        assert!(state.is_none());
        broker_handle.shutdown().await;
    }
}
