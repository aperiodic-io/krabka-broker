//! `ShareAcknowledge` (`api_key` 79), from KIP-932.
//!
//! This is the acknowledge-only counterpart of `ShareFetch`. It acknowledges
//! records that a member acquired earlier, and acquires no new ones.
//!
//! For every requested partition that this broker leads, the handler applies
//! each acknowledgement batch to the `(group, topic, partition)`
//! [`AcquisitionState`] machine, and persists the result. Accept advances the
//! SPSO, Release offers the records again, and Reject and Gap archive them.
//!
//! A partition that this broker does not lead gets
//! `UNKNOWN_TOPIC_OR_PARTITION` and no leader hint, as Kafka answers for a
//! partition with no share partition in its cache.
//! An acknowledge that targets records the member does not currently hold
//! fails that partition row with `INVALID_RECORD_STATE`.
//!
//! The handler receives the per-connection principal and the peer
//! `SocketAddr` in its `RequestContext`, for the group `Read` and per-topic
//! `Read` ACL gates.

use std::time::Instant;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::{
    share_acknowledge_request::ShareAcknowledgeRequest,
    share_acknowledge_response::{
        LeaderIdAndEpoch, NodeEndpoint, PartitionData, ShareAcknowledgeResponse,
        ShareAcknowledgeTopicResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    handlers::{
        ErrorResponse as _,
        share_fetch::{
            AckApplication, Renewal, acknowledgement_batches_are_valid, apply_acknowledgements,
            current_leader, names_the_leader, share_request_identity,
        },
    },
    share_partition::group_settings::GroupShareSettings,
};

context_handler! {
    ShareAcknowledgeRequest => ShareAcknowledgeResponse,
    (broker, req, version, ctx),
    {
        share_request_identity!(
            (cfg, image, group, settings, lock_timeout_ms, member),
            ShareAcknowledgeResponse,
            broker,
            req,
            ctx
        );

        let released = match broker.share_partition_leaders.update_acknowledge_session(
            &group,
            &member,
            req.share_session_epoch,
        ) {
            Ok(released) => released,
            Err(code) => return Ok(ShareAcknowledgeResponse::error(code, None)),
        };

        let now = Instant::now();
        let mut responses = process_topics(&AcknowledgeContext {
            broker,
            version,
            req: &req,
            ctx,
            settings,
            group: &group,
            member: &member,
            now,
        })
        .await;
        broker
            .share_partition_leaders
            .release_session_partitions(&group, &member, &released)
            .await;

        let node_endpoints = hint_current_leaders(broker, ctx, &mut responses);
        Ok(ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            acquisition_lock_timeout_ms: lock_timeout_ms,
            responses,
            node_endpoints,
            ..Default::default()
        })
    }
}

/// Kafka's `processShareAcknowledgeResponse`: sets the current leader on
/// every row whose error names another leader, and returns the endpoint of
/// each such leader on the request's listener.
fn hint_current_leaders(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    responses: &mut [ShareAcknowledgeTopicResponse],
) -> Vec<NodeEndpoint> {
    let mgr = &broker.share_partition_leaders;
    let mut leader_ids = Vec::new();
    for topic in responses.iter_mut() {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        for partition in &mut topic.partitions {
            if names_the_leader(partition.error_code) {
                let (leader_id, leader_epoch) =
                    current_leader(mgr, topic_id, partition.partition_index);
                partition.current_leader = LeaderIdAndEpoch {
                    leader_id,
                    leader_epoch,
                    ..Default::default()
                };
                leader_ids.push(leader_id);
            }
        }
    }
    crate::handlers::leader_endpoints(
        &broker.controller.current_image(),
        ctx.connection_listener_name,
        &broker.config.inter_broker_listener_name,
        leader_ids,
    )
}

/// The request-wide inputs of [`process_topics`].
struct AcknowledgeContext<'a> {
    broker: &'a Broker,
    version: i16,
    req: &'a ShareAcknowledgeRequest,
    ctx: &'a crate::handlers::RequestContext<'a>,
    settings: GroupShareSettings,
    group: &'a str,
    member: &'a str,
    now: Instant,
}

async fn process_topics(context: &AcknowledgeContext<'_>) -> Vec<ShareAcknowledgeTopicResponse> {
    let &AcknowledgeContext {
        broker,
        version,
        req,
        ctx,
        settings,
        group,
        member,
        now,
    } = context;
    let mgr = &broker.share_partition_leaders;
    let image = broker.controller.current_image();
    let mut responses = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        // Kafka's `KafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest`
        // answers UNKNOWN_TOPIC_ID on every partition of a topic id that
        // `metadataCache.topicIdsToNames()` does not hold, the zero id
        // included. `handleAcknowledgements` checks `Read` only for the
        // partitions that resolved.
        let Some(topic_name) = mgr.topic_name_for(topic_id) else {
            responses.push(ShareAcknowledgeTopicResponse {
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|ap| PartitionData {
                        partition_index: ap.partition_index,
                        error_code: codes::UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
            continue;
        };

        let denied = crate::handlers::acl_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            ResourceType::Topic,
            &topic_name,
            AclOperation::Read,
        );

        let renewal = Renewal {
            requested: req.is_renew_ack,
            enabled: settings.renew_acknowledge_enabled,
            lock_duration: settings.record_lock_duration,
        };
        let mut parts: Vec<PartitionData> = Vec::with_capacity(topic.partitions.len());
        for ap in &topic.partitions {
            let mut out = PartitionData {
                partition_index: ap.partition_index,
                ..Default::default()
            };

            // Kafka's `KafkaApis.handleAcknowledgements` validates the batches
            // first, then checks the topic `Read`, then asks the metadata
            // cache for the partition. A partition with no batch then answers
            // NONE without reaching the share partition.
            let batches_are_valid = acknowledgement_batches_are_valid(
                ap.acknowledgement_batches.iter().map(|batch| {
                    (
                        batch.first_offset,
                        batch.last_offset,
                        batch.acknowledge_types.as_slice(),
                    )
                }),
                version >= 2,
                req.is_renew_ack,
            );
            let error = if !batches_are_valid {
                Some(codes::INVALID_REQUEST)
            } else if denied {
                Some(codes::TOPIC_AUTHORIZATION_FAILED)
            } else if image.partition(&topic_name, ap.partition_index).is_none() {
                Some(codes::UNKNOWN_TOPIC_OR_PARTITION)
            } else if ap.acknowledgement_batches.is_empty() {
                Some(codes::NONE)
            } else {
                None
            };
            if let Some(code) = error {
                out.error_code = code;
                parts.push(out);
                continue;
            }

            // Kafka's `SharePartitionManager.acknowledge` has no leadership
            // check: a broker that does not lead the partition holds no share
            // partition for it, so the row answers UNKNOWN_TOPIC_OR_PARTITION
            // with no leader hint, exactly as for a share partition that no
            // fetch on this broker loaded. It reads no state for it.
            let cell = if mgr.topic_leader_is_self(topic_id, ap.partition_index) {
                mgr.cached(group, topic_id, ap.partition_index)
            } else {
                None
            };
            let Some(cell) = cell else {
                out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                parts.push(out);
                continue;
            };
            let mut st = cell.lock().await;
            st.set_dlq_enabled(settings.dlq_enabled);
            // The batches apply as one unit. The acknowledgement is durable
            // before the answer, or it is rolled back and the write error is
            // the partition error, as Kafka's
            // `SharePartition.rollbackOrProcessStateUpdates` does.
            let application = AckApplication {
                member,
                now,
                renewal,
                max_attempts: settings.delivery_count_limit,
            };
            let batches = ap.acknowledgement_batches.iter().map(|batch| {
                (
                    batch.first_offset,
                    batch.last_offset,
                    batch.acknowledge_types.as_slice(),
                )
            });
            out.error_code = mgr
                .apply_durably(group, topic_id, ap.partition_index, &cell, &mut st, |st| {
                    apply_acknowledgements(st, &application, batches)
                })
                .await;
            parts.push(out);
        }

        responses.push(ShareAcknowledgeTopicResponse {
            topic_id: topic.topic_id,
            partitions: parts,
            ..Default::default()
        });
    }
    responses
}

#[cfg(test)]
mod tests {

    use assert2::assert;
    use krabka_protocol::{
        owned::{
            share_acknowledge_request::{AcknowledgePartition, AcknowledgeTopic},
            share_acknowledge_response,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;
    use crate::test_support::{peer, principal, test_ctx};

    crate::test_support::context_helper!(client_id = "client-a");

    fn request(topic_id: ProtoUuid, partitions: &[i32]) -> ShareAcknowledgeRequest {
        ShareAcknowledgeRequest {
            group_id: Some("g1".into()),
            member_id: Some("member-1".into()),
            share_session_epoch: 0,
            topics: vec![AcknowledgeTopic {
                topic_id,
                partitions: partitions
                    .iter()
                    .map(|partition_index| AcknowledgePartition {
                        partition_index: *partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_top_level_unsupported_version() {
        let version = share_acknowledge_response::MAX_VERSION;
        let (broker_handle, _dir) = crate::test_support::start_share_broker(
            std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
            crate::test_support::ShareBrokerSetup {
                support: crate::test_support::ShareApiSupport::Disabled,
            },
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "alice");

        let resp = handle(&broker, request(ProtoUuid([7; 16]), &[0]), version, &ctx)
            .await
            .expect("handle");

        let expected = unthrottled_wire!(ShareAcknowledgeResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    use crate::handlers::test_support::{DenyTopicRead, TopicIdRef as TopicRef};

    async fn create_topic(broker: &crate::broker::BrokerHandle, name: &str) -> ProtoUuid {
        crate::handlers::test_support::create_topic(
            broker,
            crate::handlers::test_support::ClientTopicSetup {
                client_id: "share-acknowledge-resolution-test",
                name,
                ..Default::default()
            },
        )
        .await
    }

    /// Open a share session for `member` on partition 0 of `topic_id`,
    /// acknowledge it at epoch 1 with no batch, and return the decoded
    /// response. Kafka answers `NONE` for a partition that it may acknowledge
    /// and that carries no batch.
    async fn acknowledge(
        broker: &crate::broker::BrokerHandle,
        version: i16,
        member: &str,
        topic_id: ProtoUuid,
    ) -> ShareAcknowledgeResponse {
        let shared = broker.broker_arc_for_test();
        request_identity!(
            (principal, peer, ctx),
            principal("alice"),
            client_id = "client-a"
        );
        let id = uuid::Uuid::from_bytes(topic_id.0);
        shared
            .share_partition_leaders
            .update_fetch_session(
                ("g1", member),
                ctx.connection_id,
                0,
                crate::share_partition::session::FetchPartitions {
                    requested: &[(id, 0)],
                    forgotten: &std::collections::HashSet::new(),
                },
                false,
            )
            .expect("open share session");
        let mut request = request(topic_id, &[0]);
        request.member_id = Some(member.into());
        request.share_session_epoch = 1;
        // Both ends of the version range: the response is read off the wire.
        crate::test_support::dispatch_wire(
            &shared,
            krabka_protocol::owned::share_acknowledge_request::API_KEY,
            version,
            &request,
            &ctx,
        )
        .await
    }

    /// Run one case per (version, topic reference) on `broker`, each in its
    /// own share session. `known_error` is the code for a topic that exists.
    async fn drive(
        broker: &crate::broker::BrokerHandle,
        known: ProtoUuid,
        known_error: i16,
    ) -> (
        Vec<(i16, TopicRef, ShareAcknowledgeResponse)>,
        Vec<(i16, TopicRef, ShareAcknowledgeResponse)>,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for version in [
            share_acknowledge_response::MIN_VERSION,
            share_acknowledge_response::MAX_VERSION,
        ] {
            for (topic, topic_id, error_code) in [
                (TopicRef::Known, known, known_error),
                (
                    TopicRef::Unknown,
                    ProtoUuid([8; 16]),
                    codes::UNKNOWN_TOPIC_ID,
                ),
                (TopicRef::Zero, ProtoUuid::ZERO, codes::UNKNOWN_TOPIC_ID),
            ] {
                let member = format!("member-{version}-{topic:?}");
                let response = acknowledge(broker, version, &member, topic_id).await;
                actual.push((version, topic, response));
                let row = PartitionData {
                    partition_index: 0,
                    error_code,
                    ..Default::default()
                };
                expected.push((
                    version,
                    topic,
                    ShareAcknowledgeResponse {
                        // The wire carries the lock timeout from v2 on. An
                        // older version decodes the field's default, 0.
                        acquisition_lock_timeout_ms: if version >= 2 { 30_000 } else { 0 },
                        responses: vec![ShareAcknowledgeTopicResponse {
                            topic_id,
                            partitions: vec![row],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ));
            }
        }
        (actual, expected)
    }

    #[tokio::test]
    async fn partition_row_error_follows_topic_id() {
        initialized_share_topic!(
            (broker_handle, _dir, known),
            crate::test_support::start_share_broker(
                std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
                crate::test_support::ShareBrokerSetup::default()
            ),
            "ack-resolution",
            "g1"
        );

        let (actual, expected) = drive(&broker_handle, known, codes::NONE).await;

        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
    /// principal with no topic `Read` grant sees 29 for a topic that exists and
    /// 100 for an id that does not resolve.
    #[tokio::test]
    async fn unresolved_id_answers_before_topic_authorization() {
        initialized_share_topic!(
            (broker_handle, _dir, known),
            crate::test_support::start_broker_with(|cfg| {
                cfg.authorizer = std::sync::Arc::new(DenyTopicRead);
            }),
            "ack-resolution",
            "g1"
        );

        let (actual, expected) =
            drive(&broker_handle, known, codes::TOPIC_AUTHORIZATION_FAILED).await;

        assert!(actual == expected);
        broker_handle.shutdown().await;
    }
}
