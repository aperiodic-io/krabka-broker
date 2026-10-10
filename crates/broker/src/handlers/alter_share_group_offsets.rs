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

use krabka_metadata::AclOperation;
use krabka_protocol::{
    owned::{
        alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
        alter_share_group_offsets_response::{
            AlterShareGroupOffsetsResponse, AlterShareGroupOffsetsResponsePartition,
            AlterShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    codes,
    coordinator::unified::{GroupType, share::actor::ShareGroupActorMessage},
    error::BrokerError,
    handlers::ErrorResponse as _,
    task_util::{AskError, ask},
};

context_handler! {
    AlterShareGroupOffsetsRequest => AlterShareGroupOffsetsResponse,
    (broker, req, _version, ctx),
    {
        // Feature gate: share groups are on from a finalized `share.version` of 1,
        // and below it the RPC is unsupported.
        let image = broker.controller.current_image();
        if !crate::features::share_groups_enabled(&image) {
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
        if crate::handlers::group_read_denied(broker.config.authorizer.as_ref(), &image, ctx, &gid) {
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
        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
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
        let denied_topics = crate::handlers::denied_topics(
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
                        error_message: Some(
                            crate::share_coordinator::coordinator::message::UNKNOWN_TOPIC_OR_PARTITION
                                .to_owned(),
                        ),
                        ..Default::default()
                    });
                    continue;
                };

                actor_response_slots.push((responses.len(), partitions.len()));
                actor_requests.push(crate::coordinator::unified::share::actor::ResetPartition {
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::{
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
        },
        primitives::uuid::Uuid,
    };

    use super::handle;
    use crate::{
        authorizer::{AuthorizationResult, Authorizer},
        codes,
        coordinator::unified::{GroupType, ShareGroupSeed, share::actor::ShareGroupActorMessage},
        handlers::test_support::CreateTopicSetup,
        test_support::{DenyAll, test_ctx},
    };

    macro_rules! joining_share_member {
        (($tx:ident, $rx:ident, $response:ident), $actor:expr, $group:expr, $member:expr, $topics:expr) => {
            let ($tx, $rx) = tokio::sync::oneshot::channel();
            $actor
                .tx
                .send(ShareGroupActorMessage::Heartbeat {
                    request: ShareGroupHeartbeatRequest {
                        group_id: $group.into(),
                        member_id: $member.into(),
                        member_epoch: 0,
                        subscribed_topic_names: Some($topics),
                        ..Default::default()
                    },
                    client_id: "client-a".into(),
                    client_host: "127.0.0.1".into(),
                    reply: $tx,
                })
                .await
                .expect("send heartbeat");
            let $response = $rx.await.expect("heartbeat response");
        };
    }

    const UNKNOWN_TOPIC_OR_PARTITION_MESSAGE: &str =
        crate::share_coordinator::coordinator::message::UNKNOWN_TOPIC_OR_PARTITION;

    /// An authorizer with independently controllable answers for `Group`
    /// `Read`/`Alter` and per-topic `Read`, for the KIP-932
    /// `AlterShareGroupOffsets` ACL table (issue #727).
    #[derive(Debug, Default)]
    struct ScenarioAuthorizer {
        group_read: bool,
        group_alter: bool,
        denied_topics: Vec<String>,
    }

    test_authorizer!(ScenarioAuthorizer, (self, _source, req), {
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

    crate::test_support::context_helper!(client_id = "admin-client");

    async fn create_topic(
        broker_handle: &crate::broker::BrokerHandle,
        ctx: &crate::handlers::RequestContext<'_>,
        setup: CreateTopicSetup<'_>,
    ) {
        let broker = broker_handle.broker_arc_for_test();
        let topic_name = setup.topic;
        let version = create_topics_response::MAX_VERSION;
        let request = crate::handlers::test_support::configured_topic_request(setup);
        let response = crate::handlers::create_topics::handle(&broker, request, version, ctx)
            .await
            .expect("create topic");
        assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
        broker_handle
            .wait_until_partition_present(topic_name, 0)
            .await;
    }

    #[tokio::test]
    async fn handle_error_scenarios_preserve_expected_rows() {
        type Case<'a> = (
            &'a str,
            Arc<dyn Authorizer>,
            crate::test_support::ShareApiSupport,
            &'a str,
            Vec<i32>,
            AlterShareGroupOffsetsResponse,
        );
        let version = alter_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature returns top-level unsupported version",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                crate::test_support::ShareApiSupport::Disabled,
                "missing",
                vec![0],
                unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: None,
                    responses: Vec::new(),
                }),
            ),
            (
                "denied group returns top-level authorization failure",
                Arc::new(crate::test_support::ControllerPeerAllowed(DenyAll)),
                crate::test_support::ShareApiSupport::Enabled,
                "missing",
                vec![0],
                unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                    responses: Vec::new(),
                }),
            ),
            (
                "unknown topic preserves topic and partition fields",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                crate::test_support::ShareApiSupport::Enabled,
                "missing-topic",
                vec![3, 5],
                unthrottled_wire!(AlterShareGroupOffsetsResponse {
                    error_code: codes::NONE,
                    error_message: None,
                    responses: vec![tagged_wire!(AlterShareGroupOffsetsResponseTopic {
                        topic_name: "missing-topic".into(),
                        topic_id: Uuid::default(),
                        partitions: vec![
                            tagged_wire!(AlterShareGroupOffsetsResponsePartition {
                                partition_index: 3,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
                            }),
                            tagged_wire!(AlterShareGroupOffsetsResponsePartition {
                                partition_index: 5,
                                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                                error_message: Some(UNKNOWN_TOPIC_OR_PARTITION_MESSAGE.into()),
                            }),
                        ],
                    })],
                }),
            ),
        ];
        share_refusal_cases!(
            (case, authorizer, share_enabled, [topic_name, partitions], expected) in cases;
            (broker_handle, _dir, broker, ctx, resp);
            handle(request("g1", topic_name, &partitions), version)
        );
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
        type Row = (&'static str, bool, bool, bool, bool, i16, Option<i16>);
        let rows: [Row; 4] = [
            (
                "group Read only is allowed (the normal share-consumer grant)",
                true,
                false,
                false,
                true,
                codes::NONE,
                Some(codes::NONE),
            ),
            (
                "group Alter only, with no Read, is refused",
                false,
                true,
                false,
                true,
                codes::GROUP_AUTHORIZATION_FAILED,
                None,
            ),
            (
                "group Read granted but the topic is denied",
                true,
                false,
                true,
                true,
                codes::NONE,
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                "group Read granted, topic unknown to the cluster",
                true,
                false,
                false,
                false,
                codes::NONE,
                Some(codes::UNKNOWN_TOPIC_OR_PARTITION),
            ),
        ];

        for (index, (case, group_read, group_alter, topic_denied, topic_exists, top, partition)) in
            rows.into_iter().enumerate()
        {
            let topic_name = "acl-topic";
            let authorizer = Arc::new(ScenarioAuthorizer {
                group_read,
                group_alter,
                denied_topics: if topic_denied {
                    vec![topic_name.to_string()]
                } else {
                    Vec::new()
                },
            });
            let (broker_handle, _dir) = crate::test_support::start_share_broker(
                authorizer,
                crate::test_support::ShareBrokerSetup::default(),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            test_ctx!(ctx, "alice");
            if topic_exists {
                create_topic(
                    &broker_handle,
                    &ctx,
                    CreateTopicSetup {
                        topic: topic_name,
                        ..Default::default()
                    },
                )
                .await;
                crate::share_coordinator::handlers::test_support::lead_share_state_partitions(
                    &broker,
                )
                .await;
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

            let resp = handle(
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
        broker_fixture!(
            (broker_handle, _dir, broker),
            share_allow_all,
            context(ctx, "alice")
        );

        let empty_id_resp = handle(
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
        let wrong_type_resp = handle(
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
        broker_fixture!(
            (broker_handle, _dir, broker),
            share_allow_all,
            context(ctx, "alice")
        );
        create_topic(
            &broker_handle,
            &ctx,
            CreateTopicSetup {
                topic: "new-topic",
                ..Default::default()
            },
        )
        .await;
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
            .await;
        let topic_id = broker
            .controller
            .current_image()
            .topic("new-topic")
            .expect("topic metadata")
            .topic_id;

        assert!(broker.group_coordinator.group_type("g-new").is_none());

        let response = handle(
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
            joining_share_member!(
                (tx, rx, resp),
                actor,
                "g-new",
                member_id,
                vec!["new-topic".into()]
            );
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
        broker_fixture!((broker_handle, _dir, broker), share_allow_all);
        let coordinator = broker.group_coordinator.clone();

        coordinator.mark_share("busy");
        let actor = coordinator.get_or_create_share("busy");
        joining_share_member!((tx, rx, resp), actor, "busy", "member-1", Vec::new());
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        test_ctx!(ctx, "alice");
        let response = handle(
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
        broker_fixture!(
            (broker_handle, _dir, broker),
            share_allow_all,
            context(ctx, "alice")
        );
        create_topic(
            &broker_handle,
            &ctx,
            CreateTopicSetup {
                topic: "reset-topic",
                ..Default::default()
            },
        )
        .await;
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
            .await;
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
            let response = handle(
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
