//! `DeleteShareGroupOffsets` (`api_key` 92), from KIP-932.
//!
//! It deletes the durable share state for every initialized partition of the
//! requested topics, in an *empty* share group. A non-empty group gets a
//! top-level `NON_EMPTY_GROUP` rejection.
//!
//! The request carries only `topic_name` for each topic, and no partition
//! list. The group's actor lists the initialized partitions of each topic
//! from its `ShareGroupStatePartitionMetadata`, records the topic as deleting,
//! and takes it out once the persister deleted its state, as Kafka's
//! `GroupCoordinatorService.deleteShareGroupOffsets` does.
//!
//! `network::dispatch` intercepts this request inline for the per-group
//! `Delete` ACL gate, which needs the principal and the peer `SocketAddr`.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    owned::{
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
        delete_share_group_offsets_response::{
            DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
        },
    },
    primitives::uuid::Uuid,
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    codes,
    coordinator::unified::{
        GroupType,
        share::actor::{DeleteTopicOutcome, ShareGroupActorMessage},
    },
    error::BrokerError,
    handlers::ErrorResponse as _,
    task_util::{AskError, ask},
};

/// Kafka's message for `TOPIC_AUTHORIZATION_FAILED`, which
/// `handleDeleteShareGroupOffsetsRequest` puts on every denied topic row.
const TOPIC_AUTHORIZATION_FAILED_MESSAGE: &str = "Topic authorization failed.";

context_handler! {
    DeleteShareGroupOffsetsRequest => DeleteShareGroupOffsetsResponse,
    (broker, req, _version, ctx),
    {
        // Feature gate: share groups are on from a finalized `share.version` of 1,
        // and below it the RPC is unsupported.
        let image = broker.controller.current_image();
        if !crate::features::share_groups_enabled(&image) {
            return Ok(top_level(codes::UNSUPPORTED_VERSION, None));
        }

        let coordinator = &broker.group_coordinator;
        let gid = req.group_id;

        // ── ACL preamble ────────────────────────────────────
        // Per-group `Delete` check. On Deny → top-level `error_code = 30`.
        if crate::handlers::acl_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            ResourceType::Group,
            gid.as_str(),
            AclOperation::Delete,
        ) {
            return Ok(top_level(codes::GROUP_AUTHORIZATION_FAILED, None));
        }

        // Per-topic `Read` ACL. Kafka's `handleDeleteShareGroupOffsetsRequest`
        // authorizes `READ` on each requested topic after the group `Delete`
        // check; a denied topic gets `TOPIC_AUTHORIZATION_FAILED` and never
        // reaches the coordinator. Denied rows come before the coordinator's.
        let topic_names: Vec<String> = req.topics.iter().map(|rt| rt.topic_name.clone()).collect();
        let topic_decisions = authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Read,
            topic_names.iter().map(String::as_str),
        );
        let (denied_topics, allowed_topics): (Vec<_>, Vec<_>) =
            req.topics.into_iter().partition(|rt| {
                topic_decisions.get(rt.topic_name.as_str()) == Some(&AuthorizationResult::Deny)
            });

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &gid) {
            return Ok(top_level(error_code, None));
        }
        // GroupCoordinatorService.deleteShareGroupOffsets rejects an empty id
        // before it routes the group to a shard.
        if gid.is_empty() {
            return Ok(top_level(codes::INVALID_GROUP_ID, None));
        }
        // GroupCoordinatorShard.initiateDeleteShareGroupOffsets looks the group
        // up through `shareGroup`, which refuses a missing group and a group of
        // another type. No share actor is created for either.
        let actor = match coordinator.group_type(&gid) {
            Some(GroupType::Share) => Some(coordinator.get_or_create_share(&gid)),
            Some(_) => None,
            None => coordinator.find_share(&gid),
        };
        let Some(actor) = actor else {
            return Ok(top_level(
                codes::GROUP_ID_NOT_FOUND,
                Some(crate::handlers::share_group_not_found_message(
                    coordinator,
                    &gid,
                )),
            ));
        };

        let mut responses: Vec<DeleteShareGroupOffsetsResponseTopic> = denied_topics
            .into_iter()
            .map(|rt| DeleteShareGroupOffsetsResponseTopic {
                topic_name: rt.topic_name,
                topic_id: Uuid::default(),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.to_string()),
                ..Default::default()
            })
            .collect();
        let topic_names: Vec<String> = allowed_topics.into_iter().map(|rt| rt.topic_name).collect();

        let asked = ask(&actor.tx, |reply| ShareGroupActorMessage::DeleteOffsets {
            topic_names,
            reply,
        })
        .await;
        let actor_result = match asked {
            Ok(actor_result) => actor_result,
            Err(AskError::Closed) => return Ok(top_level(codes::COORDINATOR_NOT_AVAILABLE, None)),
            Err(AskError::Dropped) => {
                return Err(BrokerError::Share(
                    "share-group delete actor stopped".into(),
                ));
            }
        };
        let outcomes = match actor_result {
            Ok(outcomes) => outcomes,
            Err(error_code) => return Ok(top_level(error_code, None)),
        };
        // The actor answers in Kafka's row order: the deleted topics, the topics
        // `sharePartitionsEligibleForOffsetDeletion` refused, then the topics
        // whose state delete failed.
        for (topic_name, outcome) in outcomes {
            responses.push(match outcome {
                DeleteTopicOutcome::Deleted {
                    topic_id,
                    partitions,
                } => {
                    for partition in partitions {
                        broker
                            .share_partition_leaders
                            .invalidate(&gid, topic_id, partition);
                    }
                    DeleteShareGroupOffsetsResponseTopic {
                        topic_name,
                        topic_id: Uuid(*topic_id.as_bytes()),
                        error_code: codes::NONE,
                        ..Default::default()
                    }
                }
                DeleteTopicOutcome::UnknownTopic => DeleteShareGroupOffsetsResponseTopic {
                    topic_name,
                    topic_id: Uuid::default(),
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: kafka_message(codes::UNKNOWN_TOPIC_OR_PARTITION).map(str::to_owned),
                    ..Default::default()
                },
                DeleteTopicOutcome::NoState => DeleteShareGroupOffsetsResponseTopic {
                    topic_name,
                    topic_id: Uuid::default(),
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some(NO_OFFSETS_MESSAGE.to_owned()),
                    ..Default::default()
                },
                DeleteTopicOutcome::Failed {
                    topic_id,
                    error_code,
                } => DeleteShareGroupOffsetsResponseTopic {
                    topic_name,
                    topic_id: Uuid(*topic_id.as_bytes()),
                    error_code,
                    error_message: kafka_message(error_code).map(str::to_owned),
                    ..Default::default()
                },
            });
        }

        let resp = DeleteShareGroupOffsetsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            responses,
            ..Default::default()
        };
        Ok(resp)
    }
}

/// Kafka's row message for a topic the group holds no share state for.
const NO_OFFSETS_MESSAGE: &str = "There is no offset information to delete.";

/// The default message of Kafka's `Errors` for the codes this RPC answers,
/// which `DeleteShareGroupOffsetsRequest.getErrorDeleteResponseData` and the
/// failed-topic rows carry.
fn kafka_message(error_code: i16) -> Option<&'static str> {
    Some(match error_code {
        codes::UNKNOWN_SERVER_ERROR => {
            "The server experienced an unexpected error when processing the request."
        }
        codes::UNKNOWN_TOPIC_OR_PARTITION => "This server does not host this topic-partition.",
        codes::COORDINATOR_LOAD_IN_PROGRESS => {
            "The coordinator is loading and hence can't process requests."
        }
        codes::COORDINATOR_NOT_AVAILABLE => "The coordinator is not available.",
        codes::NOT_COORDINATOR => "This is not the correct coordinator.",
        codes::INVALID_GROUP_ID => "The group id is invalid.",
        codes::GROUP_AUTHORIZATION_FAILED => "Group authorization failed.",
        codes::UNSUPPORTED_VERSION => "The version of API is not supported.",
        codes::NON_EMPTY_GROUP => "The group is not empty.",
        codes::GROUP_ID_NOT_FOUND => "The group id does not exist.",
        codes::FENCED_LEADER_EPOCH => {
            "The leader epoch in the request is older than the epoch on the broker."
        }
        _ => return None,
    })
}

/// A top-level error response. Kafka's `getErrorResponse` sets the message
/// to `message`, or to the error's default message.
fn top_level(error_code: i16, message: Option<String>) -> DeleteShareGroupOffsetsResponse {
    DeleteShareGroupOffsetsResponse::error(
        error_code,
        message.or_else(|| kafka_message(error_code).map(str::to_owned)),
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert2::assert;
    use krabka_metadata::{AclOperation, ResourceType};
    use krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            create_topics_response,
            delete_share_group_offsets_request::{
                DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
            },
            delete_share_group_offsets_response::{
                self, DeleteShareGroupOffsetsResponse, DeleteShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    };

    use super::{TOPIC_AUTHORIZATION_FAILED_MESSAGE, handle, top_level};
    use crate::{
        authorizer::{AuthorizationResult, Authorizer},
        codes,
        coordinator::unified::{
            ShareGroupSeed,
            share::{
                actor::ShareGroupActorMessage,
                persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
            },
        },
        test_support::{DenyAll, test_ctx},
    };

    /// Denies `Read` on the named topics and allows everything else, so group
    /// `Delete` and topic creation still succeed and only the per-topic gate
    /// refuses.
    #[derive(Debug)]
    struct DenyReadOnTopics(HashSet<&'static str>);

    test_authorizer!(DenyReadOnTopics, (self, _source, request), {
        if request.resource_type == ResourceType::Topic
            && request.operation == AclOperation::Read
            && self.0.contains(request.resource_name)
        {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    });

    fn request(group_id: &str, topics: &[&str]) -> DeleteShareGroupOffsetsRequest {
        DeleteShareGroupOffsetsRequest {
            group_id: group_id.into(),
            topics: topics
                .iter()
                .map(|topic_name| DeleteShareGroupOffsetsRequestTopic {
                    topic_name: (*topic_name).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    crate::test_support::context_helper!(client_id = "admin-client");

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ShareTopicsSetup<'a> {
        #[default(&["t"])]
        names: &'a [&'a str],
    }

    async fn create_topics(
        broker_handle: &crate::broker::BrokerHandle,
        ctx: &crate::handlers::RequestContext<'_>,
        setup: ShareTopicsSetup<'_>,
    ) {
        let broker = broker_handle.broker_arc_for_test();
        let topic_names = setup.names;
        let version = create_topics_response::MAX_VERSION;
        let request = CreateTopicsRequest {
            topics: topic_names
                .iter()
                .map(|topic_name| CreatableTopic {
                    name: (*topic_name).into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 5_000,
            ..Default::default()
        };
        let response = crate::handlers::create_topics::handle(&broker, request, version, ctx)
            .await
            .expect("create topics");
        assert!(
            response
                .topics
                .iter()
                .all(|topic| topic.error_code == codes::NONE),
            "{response:?}"
        );
        for topic_name in topic_names {
            broker_handle
                .wait_until_partition_present(topic_name, 0)
                .await;
        }
    }

    #[test]
    fn top_level_preserves_error_fields() {
        let resp = top_level(codes::UNSUPPORTED_VERSION, None);

        let expected = unthrottled_wire!(DeleteShareGroupOffsetsResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: Some("The version of API is not supported.".into()),
            responses: Vec::new(),
        });
        assert!(resp == expected);
    }

    async fn create_share_topics(
        handle: &crate::broker::BrokerHandle,
        ctx: &crate::handlers::RequestContext<'_>,
        setup: ShareTopicsSetup<'_>,
    ) -> Arc<crate::share_coordinator::persister_client::SharePersister> {
        let broker = handle.broker_arc_for_test();
        create_topics(handle, ctx, setup).await;
        crate::share_coordinator::handlers::test_support::lead_share_state_partitions(&broker)
            .await;
        broker
            .group_coordinator
            .share_persister()
            .cloned()
            .expect("share persister")
    }

    #[tokio::test]
    async fn handle_error_scenarios_preserve_expected_rows() {
        type Case<'a> = (
            &'a str,
            Arc<dyn Authorizer>,
            crate::test_support::ShareApiSupport,
            Vec<&'a str>,
            DeleteShareGroupOffsetsResponse,
        );
        let version = delete_share_group_offsets_response::MAX_VERSION;
        let cases: Vec<Case<'_>> = vec![
            (
                "disabled feature returns top-level unsupported version",
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                crate::test_support::ShareApiSupport::Disabled,
                vec!["missing"],
                unthrottled_wire!(DeleteShareGroupOffsetsResponse {
                    error_code: codes::UNSUPPORTED_VERSION,
                    error_message: Some("The version of API is not supported.".into()),
                    responses: Vec::new(),
                }),
            ),
            (
                "denied group returns top-level authorization failure",
                Arc::new(crate::test_support::ControllerPeerAllowed(DenyAll)),
                crate::test_support::ShareApiSupport::Enabled,
                vec!["missing"],
                unthrottled_wire!(DeleteShareGroupOffsetsResponse {
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: Some("Group authorization failed.".into()),
                    responses: Vec::new(),
                }),
            ),
        ];
        share_refusal_cases!(
            (case, authorizer, share_enabled, [topics], expected) in cases;
            (broker_handle, _dir, broker, ctx, resp);
            handle(request("g1", &topics), version)
        );
    }

    /// Kafka's `deleteShareGroupOffsets` and `initiateDeleteShareGroupOffsets`:
    /// an empty id is invalid, a missing group or one of another type is not
    /// found (and no share group is created), a group with no share-state
    /// partition metadata answers no row at all, and in a group with some, a
    /// topic the image lacks or the group holds no state for answers
    /// `UNKNOWN_TOPIC_OR_PARTITION` with Kafka's message, in request order.
    #[tokio::test]
    async fn handle_refuses_what_kafka_refuses() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            share_allow_all,
            context(ctx, "alice")
        );
        create_topics(
            &broker_handle,
            &ctx,
            ShareTopicsSetup {
                names: &["t", "elsewhere"],
            },
        )
        .await;
        // `share-held` holds state for a topic the image has. The actor's
        // session tick drops the state of every topic the image lacks, as
        // Kafka's `maybeCleanupShareGroupState` does, and that tick can run
        // between the seed and the request.
        let elsewhere_id = broker
            .controller
            .current_image()
            .topic("elsewhere")
            .expect("elsewhere topic metadata")
            .topic_id;
        let coordinator = &broker.group_coordinator;
        let _classic = coordinator.get_or_create_classic("classic");
        coordinator.mark_share("share-empty");
        let _share = coordinator.get_or_create_share("share-empty");
        coordinator.mark_share("share-held");
        coordinator
            .get_or_create_share("share-held")
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                    initialized: vec![TopicPartitionsInfo {
                        topic_id: elsewhere_id,
                        topic_name: "elsewhere".into(),
                        partitions: vec![0],
                    }],
                    ..Default::default()
                },
                ..Default::default()
            }))
            .await
            .expect("seed share-held");
        let top_level = |error_code, message: &str| DeleteShareGroupOffsetsResponse {
            error_code,
            error_message: Some(message.into()),
            ..Default::default()
        };
        let row = |topic_name: &str, message: &str| DeleteShareGroupOffsetsResponseTopic {
            topic_name: topic_name.into(),
            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some(message.into()),
            ..Default::default()
        };
        // (group id, topics, expected response)
        let rows = [
            (
                "",
                vec!["t"],
                top_level(codes::INVALID_GROUP_ID, "The group id is invalid."),
            ),
            (
                "absent",
                vec!["t"],
                top_level(codes::GROUP_ID_NOT_FOUND, "Group absent not found."),
            ),
            (
                "classic",
                vec!["t"],
                top_level(
                    codes::GROUP_ID_NOT_FOUND,
                    "Group classic is not a share group.",
                ),
            ),
            (
                "share-empty",
                vec!["t", "missing-topic"],
                DeleteShareGroupOffsetsResponse::default(),
            ),
            (
                "share-held",
                vec!["t", "missing-topic"],
                DeleteShareGroupOffsetsResponse {
                    responses: vec![
                        row("t", "There is no offset information to delete."),
                        row(
                            "missing-topic",
                            "This server does not host this topic-partition.",
                        ),
                    ],
                    ..Default::default()
                },
            ),
        ];
        for (group_id, topics, expected) in rows {
            let response = handle(
                &broker,
                request(group_id, &topics),
                delete_share_group_offsets_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle delete");
            assert!(response == expected, "group {group_id:?}");
        }
        let mut share_groups = coordinator.share_group_ids();
        share_groups.sort_unstable();
        assert!(share_groups == vec!["share-empty".to_owned(), "share-held".to_owned()]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn delete_removes_only_requested_state_and_a_retry_finds_none() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            share_allow_all,
            context(ctx, "alice")
        );
        let persister = create_share_topics(
            &broker_handle,
            &ctx,
            ShareTopicsSetup {
                names: &["delete-topic", "kept-topic"],
            },
        )
        .await;
        let image = broker.controller.current_image();
        let deleted_id = image
            .topic("delete-topic")
            .expect("delete topic metadata")
            .topic_id;
        let kept_id = image
            .topic("kept-topic")
            .expect("kept topic metadata")
            .topic_id;
        drop(image);
        persister
            .initialize("g-delete", deleted_id, 0, 4, krabka_log::Offset(10))
            .await
            .expect("seed deleted state");
        persister
            .initialize("g-delete", kept_id, 0, 6, krabka_log::Offset(20))
            .await
            .expect("seed kept state");

        let actor = broker.group_coordinator.get_or_create_share("g-delete");
        actor
            .tx
            .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                    initializing: Vec::new(),
                    initialized: vec![
                        TopicPartitionsInfo {
                            topic_id: deleted_id,
                            topic_name: "deleted".into(),
                            partitions: vec![0],
                        },
                        TopicPartitionsInfo {
                            topic_id: kept_id,
                            topic_name: "kept".into(),
                            partitions: vec![0],
                        },
                    ],
                    ..Default::default()
                },
                ..Default::default()
            }))
            .await
            .expect("seed share actor");

        // The first delete removes the state, and
        // `completeDeleteShareGroupOffsets` takes the topic out of the
        // deleting set. The retry finds the topic neither initialized nor
        // deleting, so Kafka's `sharePartitionsEligibleForOffsetDeletion`
        // answers the no-offsets row.
        let deleted_row = DeleteShareGroupOffsetsResponseTopic {
            topic_name: "delete-topic".into(),
            topic_id: Uuid(*deleted_id.as_bytes()),
            ..Default::default()
        };
        let no_state_row = DeleteShareGroupOffsetsResponseTopic {
            topic_name: "delete-topic".into(),
            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("There is no offset information to delete.".into()),
            ..Default::default()
        };
        for expected_row in [deleted_row, no_state_row] {
            let response = handle(
                &broker,
                request("g-delete", &["delete-topic"]),
                delete_share_group_offsets_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle delete");
            assert!(
                response
                    == DeleteShareGroupOffsetsResponse {
                        responses: vec![expected_row],
                        ..Default::default()
                    }
            );

            let deleted_state = persister
                .read_summary("g-delete", deleted_id, 0)
                .await
                .expect("read deleted state");
            assert!(deleted_state.is_none());
        }
        let (kept_state_epoch, _, kept_start_offset, _) = persister
            .read_summary("g-delete", kept_id, 0)
            .await
            .expect("read kept state")
            .expect("kept state");
        assert!(kept_state_epoch == 6);
        assert!(kept_start_offset == krabka_log::Offset(20));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_unauthorized_topics_and_orders_denied_rows_first() {
        // Each case denies `Read` on the listed topics and requests both
        // fixture topics in `["allow-topic", "deny-topic"]` order. Kafka's
        // `handleDeleteShareGroupOffsetsRequest` puts every denied-topic row
        // ahead of the coordinator-returned rows, regardless of request
        // order, and never routes a denied topic to the coordinator.
        type Case<'a> = (&'a str, &'a [&'a str]);
        let cases: Vec<Case<'_>> = vec![
            (
                "one allowed, one denied: denied row comes first",
                &["deny-topic"],
            ),
            ("all topics denied", &["allow-topic", "deny-topic"]),
            ("all topics allowed", &[]),
        ];

        for (case, denied_names) in cases {
            let denied: HashSet<&'static str> = denied_names.iter().copied().collect();
            let (broker_handle, _dir) = crate::test_support::start_share_broker(
                Arc::new(DenyReadOnTopics(denied.clone())),
                crate::test_support::ShareBrokerSetup::default(),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            test_ctx!(ctx, "alice");
            let persister = create_share_topics(
                &broker_handle,
                &ctx,
                ShareTopicsSetup {
                    names: &["allow-topic", "deny-topic"],
                },
            )
            .await;
            let image = broker.controller.current_image();
            let allow_id = image.topic("allow-topic").expect("allow topic").topic_id;
            let deny_id = image.topic("deny-topic").expect("deny topic").topic_id;
            drop(image);

            persister
                .initialize("g-authz", allow_id, 0, 4, krabka_log::Offset(10))
                .await
                .expect("seed allow-topic state");
            persister
                .initialize("g-authz", deny_id, 0, 4, krabka_log::Offset(10))
                .await
                .expect("seed deny-topic state");

            let actor = broker.group_coordinator.get_or_create_share("g-authz");
            actor
                .tx
                .send(ShareGroupActorMessage::Seed(ShareGroupSeed {
                    state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                        initializing: Vec::new(),
                        initialized: vec![
                            TopicPartitionsInfo {
                                topic_id: allow_id,
                                topic_name: "allow-topic".into(),
                                partitions: vec![0],
                            },
                            TopicPartitionsInfo {
                                topic_id: deny_id,
                                topic_name: "deny-topic".into(),
                                partitions: vec![0],
                            },
                        ],
                        ..Default::default()
                    },
                    ..Default::default()
                }))
                .await
                .expect("seed share actor");

            let response = handle(
                &broker,
                request("g-authz", &["allow-topic", "deny-topic"]),
                delete_share_group_offsets_response::MAX_VERSION,
                &ctx,
            )
            .await
            .expect("handle delete");

            let denied_row = |name: &str| {
                tagged_wire!(DeleteShareGroupOffsetsResponseTopic {
                    topic_name: name.into(),
                    topic_id: Uuid::default(),
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: Some(TOPIC_AUTHORIZATION_FAILED_MESSAGE.to_string()),
                })
            };
            let allowed_row = |name: &str, topic_id: uuid::Uuid| {
                tagged_wire!(DeleteShareGroupOffsetsResponseTopic {
                    topic_name: name.into(),
                    topic_id: Uuid(*topic_id.as_bytes()),
                    error_code: codes::NONE,
                    error_message: None,
                })
            };

            // Denied rows come first, in request order; allowed rows follow,
            // also in request order.
            let mut expected_responses = Vec::new();
            for name in ["allow-topic", "deny-topic"] {
                if denied.contains(name) {
                    expected_responses.push(denied_row(name));
                }
            }
            for name in ["allow-topic", "deny-topic"] {
                if !denied.contains(name) {
                    let topic_id = if name == "allow-topic" {
                        allow_id
                    } else {
                        deny_id
                    };
                    expected_responses.push(allowed_row(name, topic_id));
                }
            }

            let expected = unthrottled_wire!(DeleteShareGroupOffsetsResponse {
                error_code: codes::NONE,
                error_message: None,
                responses: expected_responses,
            });
            assert!(response == expected, "case: {case}");

            // A denied topic's durable state must be untouched; an allowed
            // topic's must be deleted.
            for (name, topic_id) in [("allow-topic", allow_id), ("deny-topic", deny_id)] {
                let state = persister
                    .read_summary("g-authz", topic_id, 0)
                    .await
                    .expect("read state")
                    .map(|(state_epoch, _, start_offset, _)| (state_epoch, start_offset));
                let expected = denied.contains(name).then_some((4, krabka_log::Offset(10)));
                assert!(state == expected, "case: {case}, topic: {name}");
            }

            broker_handle.shutdown().await;
        }
    }
}
