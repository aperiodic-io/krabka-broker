//! `DescribeTopicPartitions` (`api_key=75`, KIP-966) lists topics and their
//! partitions in pages.
//!
//! The JVM admin client uses this API for `kafka-topics --describe` against
//! Kafka 3.7+ brokers. It replaces the Metadata fan-out that the older admin
//! client used for the same job.
//!
//! ## Request shape
//!
//! - `topics`: if empty, the broker returns all topics in alphabetical order.
//!   If not empty, the broker returns exactly those topics, deduplicated and
//!   sorted by name.
//! - `response_partition_limit`: the maximum number of partition rows in the
//!   response. Default 2000.
//! - `cursor`: an optional resume point `(topic_name, partition_index)`. If
//!   set, the response starts at that topic's partition and the broker skips
//!   all earlier topics.
//!
//! ## ACL semantics
//!
//! The broker checks `Describe` on `Topic(name)` for each topic. For a *named*
//! request, a Deny gives a topic row with
//! `error_code = TOPIC_AUTHORIZATION_FAILED (29)`. For a *fetch-all* request, a
//! Deny makes the broker omit the topic. This matches `Metadata` fetch-all, so
//! the broker does not leak topic names to unauthorized clients.
//!
//! Every Deny row for a named request is collected up front, for every
//! requested name, and appended after the (possibly truncated) paginated
//! rows -- regardless of where the partition budget ran out. Only the
//! Allow rows are deduplicated, sorted by name, and walked under the
//! partition budget; see
//! `KRaftMetadataCache.describeTopicResponse`/`KafkaApis.handleDescribeTopicPartitionsRequest`.
//!
//! ## Cursor validation
//!
//! A cursor naming a topic absent from a non-empty requested-topic list, or
//! carrying a negative `partition_index`, fails the whole request with
//! `INVALID_REQUEST (42)` before any authorization or pagination happens.
//!
//! ## Partition budget exhaustion at a topic boundary
//!
//! `KRaftMetadataCache.describeTopicResponse` checks the remaining partition
//! budget at the top of every topic's turn, before resolving the topic or
//! writing a row for it. When the budget already hit zero, that topic gets
//! no row at all: the response's `next_cursor` points at
//! `(topic_name, 0)` and the walk stops. This is also why a cursor can point
//! at a requested name that turns out not to exist in the image -- the
//! budget check runs before the existence check ever would.
//!
//! ## KIP-430 integration
//!
//! Every Allow row carries `topic_authorized_operations`. The v0 schema always
//! encodes this field. Metadata has an opt-in flag for it, but this API does
//! not.
//!
//! ## KIP-966 integration
//!
//! Every partition row carries `eligible_leader_replicas` and `last_known_elr`
//! from what the metadata image holds for the topic, always as a list and
//! never as null. This is the only API that reports ELR; Kafka's Metadata
//! schema has no field for it in any version. See [`crate::elr`].

use krabka_log::topic_name::validate_topic_name;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    owned::{
        describe_topic_partitions_request::DescribeTopicPartitionsRequest,
        describe_topic_partitions_response::{
            Cursor as ResponseCursor, DescribeTopicPartitionsResponse,
            DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    elr::TopicElr,
    handlers::authorized_operations::authorized_operations_bits,
    internal_topics::is_internal_topic,
};

context_handler! {
    // The `async fn` shape matches the other inline-intercept handlers
    // (DescribeCluster, DescribeGroups) so dispatch.rs can call it through one
    // `await`. The single suspension point is the fenced-broker snapshot the
    // `offline_replicas` projection needs.
    // ACL preamble + pagination + cursor logic
    DescribeTopicPartitionsRequest => DescribeTopicPartitionsResponse,
    (broker, req, _version, ctx),
    {
        // ── 0. Cursor validation. ───────────────────────────────────────────
        // `KafkaApis.handleDescribeTopicPartitionsRequest` checks this before
        // anything else -- before authorization, before pagination -- and
        // fails the whole request with INVALID_REQUEST when it does not hold.
        if let Some(cursor) = &req.cursor {
            let named = !req.topics.is_empty();
            let cursor_topic_missing = named
                && !req
                    .topics
                    .iter()
                    .any(|topic| topic.name == cursor.topic_name);
            if cursor_topic_missing || cursor.partition_index < 0 {
                return Ok(invalid_request_response(&req));
            }
        }

        let image = broker.controller.current_image();
        // KIP-112 / KIP-858 `offline_replicas` needs the fenced-broker set as well
        // as the image; see `handlers::offline_replicas`.
        let unavailable = crate::handlers::offline_replicas::unavailable_brokers(broker, &image).await;

        // ── 1. Resolve the topic-name iteration order ──────────────────────
        // Named request: every requested name, deduplicated and sorted, even
        // if some don't exist (those rows carry UNKNOWN_TOPIC_OR_PARTITION, or
        // INVALID_TOPIC_EXCEPTION for a name `Topic.validate` refuses).
        // Fetch-all (empty `topics`): walk every topic from the image,
        // alphabetical for deterministic pagination.
        let (named, ordered_names, cursor_partition) = resolve_names(&image, &req);

        // ── 3. Batch-authorize Describe on all candidate topics. ───────────
        let acl_by_name = authorize_topics(
            broker.config.authorizer.as_ref(),
            &*image,
            ctx.principal,
            ctx.peer,
            AclOperation::Describe,
            ordered_names.iter().map(String::as_str),
        );

        // Split by authorization result up front. Every Deny row for a named
        // request is collected here, unconditionally, so it survives even when
        // the partition budget runs out before pagination reaches it -- Kafka
        // appends the whole Deny set after the (possibly truncated) paginated
        // rows, not in request/iteration position. A fetch-all Deny is silently
        // omitted, matching `Metadata` fetch-all, so the broker doesn't leak
        // topic existence to unauthorized clients.
        let mut authorized_names: Vec<&String> = Vec::with_capacity(ordered_names.len());
        let mut denied_out: Vec<DescribeTopicPartitionsResponseTopic> = Vec::new();
        for name in &ordered_names {
            let allowed = acl_by_name
                .get(name.as_str())
                .copied()
                .unwrap_or(AuthorizationResult::Deny)
                == AuthorizationResult::Allow;
            if allowed {
                authorized_names.push(name);
            } else if named {
                denied_out.push(error_topic(name, codes::TOPIC_AUTHORIZATION_FAILED));
            }
        }

        // ── 4. Walk the authorized topics, building rows under the
        // partition-limit budget. Kafka's clamp: `max(min(
        // max.request.partition.size.limit, response_partition_limit), 1)`.
        let partition_limit = broker
            .config
            .max_request_partition_size_limit
            .min(req.response_partition_limit)
            .max(1);
        let mut emitted_partitions: i32 = 0;
        let mut topics_out: Vec<DescribeTopicPartitionsResponseTopic> =
            Vec::with_capacity(authorized_names.len());
        let mut next_cursor: Option<ResponseCursor> = None;

        // Apply the request cursor's partition_index only to the topic it
        // actually names, wherever that topic lands after authorization
        // filtering -- not just the first topic this loop happens to process.
        // A cursor naming a topic that is now Deny-filtered out must not leak
        // its offset onto the next authorized topic.
        let cursor_topic_name = req.cursor.as_ref().map(|cursor| cursor.topic_name.as_str());

        for name in &authorized_names {
            // Kafka checks the remaining budget at the top of every topic's
            // turn, before resolving it or writing a row: when the budget is
            // already exhausted, this topic (existent or not) gets no row at
            // all, and the cursor just points at it.
            if emitted_partitions >= partition_limit {
                next_cursor = Some(ResponseCursor {
                    topic_name: (*name).clone(),
                    partition_index: 0,
                    ..Default::default()
                });
                break;
            }

            let topic = image.topic(name.as_str());
            let Some(t) = topic else {
                topics_out.push(unknown_topic_row(broker, &image, ctx, name.as_str()));
                continue;
            };

            // `partitions_of` yields ascending partition-index order — the
            // order the cursor pagination below depends on.
            let mut sorted_parts: Vec<_> = image.partitions_of(name.as_str()).collect();

            // Skip partitions before the cursor's `partition_index`, but only on
            // the topic the cursor actually names -- not just the first topic
            // this loop happens to reach, which may differ once Deny-filtering
            // and pagination truncation are applied.
            if cursor_topic_name == Some(name.as_str()) {
                sorted_parts.retain(|p| p.partition >= cursor_partition);
            }

            // KIP-966: one read of the topic's published ELR state feeds every
            // partition row below; see `crate::elr`.
            let topic_elr = TopicElr::of_topic(&image, name.as_str());

            let mut row_partitions: Vec<DescribeTopicPartitionsResponsePartition> =
                Vec::with_capacity(sorted_parts.len());
            let mut truncated = false;
            let mut next_partition_index: i32 = 0;
            for p in &sorted_parts {
                if emitted_partitions >= partition_limit {
                    truncated = true;
                    next_partition_index = p.partition;
                    break;
                }
                row_partitions.push(partition_response(
                    &image,
                    p,
                    &unavailable,
                    ctx.connection_listener_name,
                    &topic_elr,
                ));
                emitted_partitions += 1;
            }

            // KIP-430: the v0 schema always encodes the bitfield, no opt-in
            // flag exists for this API. Always populate via the shared helper.
            let topic_authorized_operations = authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                ResourceType::Topic,
                name.as_str(),
            );

            topics_out.push(DescribeTopicPartitionsResponseTopic {
                error_code: codes::NONE,
                name: Some((*name).clone()),
                topic_id: WireUuid(t.topic_id.into_bytes()),
                is_internal: is_internal_topic(&broker.config, name.as_str()),
                partitions: row_partitions,
                topic_authorized_operations,
                ..Default::default()
            });

            if truncated {
                next_cursor = Some(ResponseCursor {
                    topic_name: (*name).clone(),
                    partition_index: next_partition_index,
                    ..Default::default()
                });
                break;
            }
        }

        // Deny rows are appended last, after the (possibly truncated) paginated
        // Allow rows -- see the module docs.
        topics_out.extend(denied_out);

        let resp = DescribeTopicPartitionsResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            next_cursor,
            ..Default::default()
        };
        Ok(resp)
    }
}

fn partition_response(
    image: &krabka_metadata::MetadataImage,
    partition: &krabka_metadata::PartitionRecord,
    unavailable: &std::collections::HashSet<u64>,
    listener: &str,
    topic_elr: &TopicElr,
) -> DescribeTopicPartitionsResponsePartition {
    let elr = topic_elr.partition(partition.partition);
    // Leader, ISR and `offlineReplicas` are one answer: see
    // `crate::handlers::offline_replicas::partition_availability`. Kafka's
    // `KRaftMetadataCache.partitionMetadataForDescribeTopicResponse` leaves
    // `error_code` alone for a `-1` leader here -- only `Metadata` carries
    // `LEADER_NOT_AVAILABLE` beside it -- so this row stays `NONE`. A leader
    // with no endpoint on the request's listener is `-1` as well.
    let availability = crate::handlers::offline_replicas::partition_availability(
        image,
        partition,
        unavailable,
        listener,
    );
    DescribeTopicPartitionsResponsePartition {
        error_code: codes::NONE,
        partition_index: partition.partition,
        leader_id: availability.leader_id,
        leader_epoch: partition.leader_epoch.0,
        replica_nodes: partition
            .replicas
            .iter()
            .copied()
            .map(crate::handlers::metadata::wire_id)
            .collect(),
        isr_nodes: availability.isr_nodes,
        // KIP-966. Both fields are nullable in the schema, but a real broker
        // never sends null: `Replicas.toList` gives an empty list for a
        // partition with no ELR, and `kafka-topics --describe` renders a null
        // as `N/A` -- "this broker does not know" -- rather than as "none".
        // See `crate::elr`.
        eligible_leader_replicas: Some(elr.eligible_leader_replicas),
        last_known_elr: Some(elr.last_known_elr),
        offline_replicas: availability.offline_replicas,
        ..Default::default()
    }
}

fn error_topic(name: &str, error_code: i16) -> DescribeTopicPartitionsResponseTopic {
    DescribeTopicPartitionsResponseTopic {
        error_code,
        name: Some(name.to_string()),
        topic_id: WireUuid::ZERO,
        is_internal: false,
        partitions: Vec::new(),
        topic_authorized_operations: i32::MIN,
        ..Default::default()
    }
}

/// The error row for a named topic the image doesn't hold.
///
/// Kafka's `describeTopicResponse` runs `Topic.validate(name)` here: a name it
/// refuses answers `INVALID_TOPIC_EXCEPTION` (17), any other name answers
/// `UNKNOWN_TOPIC_OR_PARTITION` (3). It still answers `Topic.isInternal(name)`
/// and `topic_authorized_operations` on this row, computed from the name
/// alone -- so a request for a missing `__consumer_offsets` still reports
/// `is_internal: true`, and the KIP-430 bitfield is never left unset.
fn unknown_topic_row(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    name: &str,
) -> DescribeTopicPartitionsResponseTopic {
    let error_code = if validate_topic_name(name).is_ok() {
        codes::UNKNOWN_TOPIC_OR_PARTITION
    } else {
        codes::INVALID_TOPIC_EXCEPTION
    };
    DescribeTopicPartitionsResponseTopic {
        error_code,
        name: Some(name.to_string()),
        topic_id: WireUuid::ZERO,
        is_internal: is_internal_topic(&broker.config, name),
        partitions: Vec::new(),
        topic_authorized_operations: authorized_operations_bits(
            broker.config.authorizer.as_ref(),
            image,
            ctx,
            ResourceType::Topic,
            name,
        ),
        ..Default::default()
    }
}

/// The whole-request `INVALID_REQUEST` response for a bad cursor: one row per
/// requested topic (in request order, not deduplicated), each carrying the
/// error code and nothing else -- the same shape `MetadataResponse`'s
/// equivalent error response uses, since neither schema has a top-level error
/// code field to carry it instead.
fn invalid_request_response(
    req: &DescribeTopicPartitionsRequest,
) -> DescribeTopicPartitionsResponse {
    DescribeTopicPartitionsResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| error_topic(&topic.name, codes::INVALID_REQUEST))
            .collect(),
        ..Default::default()
    }
}

fn resolve_names(
    image: &krabka_metadata::MetadataImage,
    req: &DescribeTopicPartitionsRequest,
) -> (bool, Vec<String>, i32) {
    let named = !req.topics.is_empty();
    let mut ordered_names: Vec<String> = if named {
        let mut deduped: Vec<String> = req.topics.iter().map(|topic| topic.name.clone()).collect();
        deduped.sort();
        deduped.dedup();
        deduped
    } else {
        let mut all_topics: Vec<_> = image.topics().map(|topic| topic.name.clone()).collect();
        all_topics.sort();
        all_topics
    };
    let cursor_partition = req.cursor.as_ref().map_or(0, |cursor| {
        ordered_names.retain(|candidate| candidate.as_str() >= cursor.topic_name.as_str());
        cursor.partition_index
    });
    (named, ordered_names, cursor_partition)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_ids::{LeaderEpoch, PartitionIndex};
    use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
    use krabka_protocol::owned::describe_topic_partitions_request::{
        Cursor as RequestCursor, TopicRequest,
    };

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{PartitionCount, TopicSetup},
    };

    const VERSION: i16 = krabka_protocol::owned::describe_topic_partitions_response::MAX_VERSION;

    crate::test_support::context_helper!(client_id = "admin-client");

    use crate::test_support::{start_broker_with_authorizer_no_audit as start_broker, test_ctx};

    async fn seed_topic_with_epoch(handle: &BrokerHandle, leader_epoch: LeaderEpoch) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: uuid::Uuid::from_u128(1),
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    leader_epoch,
                    directories: vec![uuid::Uuid::nil()],
                    partition_epoch: 3,
                    ..crate::handlers::test_support::replicated_partition(
                        crate::handlers::test_support::ReplicatedPartitionSetup {
                            leader: NodeId(1),
                            replicas: &[NodeId(1)],
                            ..Default::default()
                        },
                    )
                }),
            ])
            .await
            .expect("seed topic + partition");
    }

    /// Seed a topic with `partitions` single-replica partitions, leader node
    /// 1, no ELR. `id_seed` gives each seeded topic a distinct `topic_id` so
    /// row comparisons can pin it.
    async fn seed_topic(handle: &BrokerHandle, setup: TopicSetup<'_>) {
        let TopicSetup {
            topic: name,
            topic_id,
            partitions,
            replication_factor,
        } = setup;
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: name.to_string(),
            topic_id,
            partitions: partitions.0,
            replication_factor: replication_factor.0,
        })];
        for index in 0..partitions.0 {
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                directories: vec![uuid::Uuid::nil()],
                ..crate::handlers::test_support::single_replica_partition(
                    name,
                    krabka_ids::PartitionIndex(index),
                    NodeId(1),
                )
            }));
        }
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(records)
            .await
            .expect("seed topic + partitions");
    }

    /// Denies `Describe` on `Topic(name)` for every name in the list, allows
    /// everything else.
    #[derive(Debug)]
    struct DenyTopics(&'static [&'static str]);

    test_authorizer!(DenyTopics, (self, _source, request), {
        if request.resource_type == ResourceType::Topic
            && request.operation == AclOperation::Describe
            && self.0.contains(&request.resource_name)
        {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    });

    #[derive(Clone, Copy)]
    struct ResponsePartitionLimit(i32);

    #[derive(krabka_macros::FieldDefaults)]
    struct DescribeTopicsSetup {
        #[default(vec!["a".to_owned()])]
        topics: Vec<String>,
        #[default(ResponsePartitionLimit(2000))]
        response_partition_limit: ResponsePartitionLimit,
        cursor: Option<RequestCursor>,
    }

    impl DescribeTopicsSetup {
        /// A one-partition pagination budget over the caller's ordered topic names.
        fn one_partition(topics: &[&str]) -> Self {
            Self {
                topics: topics.iter().map(|name| (*name).to_owned()).collect(),
                response_partition_limit: ResponsePartitionLimit(1),
                ..Default::default()
            }
        }
    }

    fn request(setup: DescribeTopicsSetup) -> DescribeTopicPartitionsRequest {
        let DescribeTopicsSetup {
            topics,
            response_partition_limit,
            cursor,
        } = setup;
        DescribeTopicPartitionsRequest {
            topics: topics
                .into_iter()
                .map(|name| TopicRequest {
                    name,
                    ..Default::default()
                })
                .collect(),
            response_partition_limit: response_partition_limit.0,
            cursor,
            ..Default::default()
        }
    }

    fn partition_row(index: PartitionIndex) -> DescribeTopicPartitionsResponsePartition {
        DescribeTopicPartitionsResponsePartition {
            error_code: codes::NONE,
            partition_index: index.0,
            leader_id: 1,
            leader_epoch: 0,
            replica_nodes: vec![1],
            isr_nodes: vec![1],
            eligible_leader_replicas: Some(vec![]),
            last_known_elr: Some(vec![]),
            offline_replicas: vec![],
            ..Default::default()
        }
    }

    /// A denied topic that would fall after the point pagination truncates
    /// at still gets its `TOPIC_AUTHORIZATION_FAILED` row: Kafka collects
    /// every Deny row up front and appends the whole set after the
    /// (possibly truncated) paginated Allow rows, regardless of where the
    /// partition budget ran out.
    async fn describe(
        broker: &Broker,
        request: DescribeTopicPartitionsRequest,
        ctx: &crate::handlers::RequestContext<'_>,
    ) -> DescribeTopicPartitionsResponse {
        handle(broker, request, VERSION, ctx).await.expect("handle")
    }

    fn response_topic_names(response: &DescribeTopicPartitionsResponse) -> Vec<&str> {
        response
            .topics
            .iter()
            .map(|topic| topic.name.as_deref().expect("named topic"))
            .collect()
    }

    fn ordered_a_b_topics() -> [TopicSetup<'static>; 2] {
        [
            TopicSetup {
                topic: "a",
                ..TopicSetup::single_replica()
            },
            TopicSetup {
                topic: "b",
                topic_id: uuid::Uuid::from_u128(2),
                ..TopicSetup::single_replica()
            },
        ]
    }

    macro_rules! seeded_topics_fixture {
        (($handle:ident, $dir:ident, $broker:ident, $ctx:ident), $authorizer:expr,
            $topics:expr) => {
            let ($handle, $dir) = start_broker($authorizer).await;
            for setup in $topics {
                seed_topic(&$handle, setup).await;
            }
            let $broker = $handle.broker_arc_for_test();
            test_ctx!($ctx, "admin");
        };
    }

    #[tokio::test]
    async fn denied_topic_survives_truncation() {
        seeded_topics_fixture!(
            (broker_handle, _dir, broker, ctx),
            Arc::new(DenyTopics(&["c"])),
            ordered_a_b_topics()
        );
        // Budget of 1: "a" fills it, "b" is truncated with no row (the
        // partition-budget-at-topic-boundary rule), "c" is denied. Without
        // the fix, "c" would vanish instead of appearing after "b".
        let req = request(DescribeTopicsSetup::one_partition(&["a", "b", "c"]));

        let resp = describe(&broker, req, &ctx).await;

        assert!(
            resp == DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics: vec![
                    DescribeTopicPartitionsResponseTopic {
                        error_code: codes::NONE,
                        name: Some("a".into()),
                        topic_id: WireUuid(uuid::Uuid::from_u128(1).into_bytes()),
                        is_internal: false,
                        partitions: vec![partition_row(PartitionIndex(0))],
                        topic_authorized_operations: authorized_operations_bits(
                            broker.config.authorizer.as_ref(),
                            &broker.controller.current_image(),
                            &ctx,
                            ResourceType::Topic,
                            "a",
                        ),
                        ..Default::default()
                    },
                    error_topic("c", codes::TOPIC_AUTHORIZATION_FAILED),
                ],
                next_cursor: Some(ResponseCursor {
                    topic_name: "b".into(),
                    partition_index: 0,
                    ..Default::default()
                }),
                ..Default::default()
            }
        );

        broker_handle.shutdown().await;
    }

    /// A cursor naming a topic that authorization then filters out must not
    /// leak its `partition_index` onto whichever authorized topic the loop
    /// reaches first. `a` is Deny (and thus never appears in
    /// `authorized_names`), so `b`'s partitions must start at 0, not at the
    /// cursor's offset into `a`.
    #[tokio::test]
    async fn cursor_offset_on_a_denied_topic_does_not_leak_onto_the_next_topic() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyTopics(&["a"]))).await;
        seed_topic(
            &broker_handle,
            TopicSetup {
                topic: "a",
                ..TopicSetup::single_replica()
            },
        )
        .await;
        seed_topic(
            &broker_handle,
            TopicSetup {
                topic: "b",
                topic_id: uuid::Uuid::from_u128(2),
                partitions: PartitionCount(3),
                ..TopicSetup::single_replica()
            },
        )
        .await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "admin");
        let cursor = Some(RequestCursor {
            topic_name: "a".into(),
            partition_index: 2,
            ..Default::default()
        });
        let req = request(DescribeTopicsSetup {
            topics: vec!["a", "b"].into_iter().map(str::to_owned).collect(),
            cursor,
            ..Default::default()
        });

        let resp = describe(&broker, req, &ctx).await;

        assert!(
            resp == DescribeTopicPartitionsResponse {
                throttle_time_ms: 0,
                topics: vec![
                    DescribeTopicPartitionsResponseTopic {
                        error_code: codes::NONE,
                        name: Some("b".into()),
                        topic_id: WireUuid(uuid::Uuid::from_u128(2).into_bytes()),
                        is_internal: false,
                        partitions: vec![
                            partition_row(PartitionIndex(0)),
                            partition_row(PartitionIndex(1)),
                            partition_row(PartitionIndex(2)),
                        ],
                        topic_authorized_operations: authorized_operations_bits(
                            broker.config.authorizer.as_ref(),
                            &broker.controller.current_image(),
                            &ctx,
                            ResourceType::Topic,
                            "b",
                        ),
                        ..Default::default()
                    },
                    error_topic("a", codes::TOPIC_AUTHORIZATION_FAILED),
                ],
                next_cursor: None,
                ..Default::default()
            }
        );

        broker_handle.shutdown().await;
    }

    /// Duplicate requested names in reverse alphabetical order are
    /// deduplicated and the surviving (Allow) names are sorted before
    /// pagination walks them.
    #[tokio::test]
    async fn duplicate_requested_names_dedupe_and_sort() {
        seeded_topics_fixture!(
            (broker_handle, _dir, broker, ctx),
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            ordered_a_b_topics()
        );
        let req = request(DescribeTopicsSetup {
            topics: vec!["b", "a", "b", "a"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ..Default::default()
        });

        let resp = describe(&broker, req, &ctx).await;

        let names = response_topic_names(&resp);
        assert!(names == vec!["a", "b"]);

        broker_handle.shutdown().await;
    }

    /// The cursor's topic name must be present in a non-empty requested-topic
    /// list, and its partition index must not be negative. Either failure
    /// answers `INVALID_REQUEST` for the whole request: one row per
    /// requested topic, in request order, none of them paginated.
    #[tokio::test]
    async fn invalid_cursor_fails_the_whole_request() {
        let cases: [(&str, Option<RequestCursor>); 2] = [
            (
                "cursor topic absent from a non-empty topic list",
                Some(RequestCursor {
                    topic_name: "missing".into(),
                    partition_index: 0,
                    ..Default::default()
                }),
            ),
            (
                "negative cursor partition index",
                Some(RequestCursor {
                    topic_name: "a".into(),
                    partition_index: -1,
                    ..Default::default()
                }),
            ),
        ];

        for (name, cursor) in cases {
            seeded_topics_fixture!(
                (broker_handle, _dir, broker, ctx),
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                [TopicSetup {
                    topic: "a",
                    ..TopicSetup::single_replica()
                }]
            );
            let req = request(DescribeTopicsSetup {
                cursor,
                ..Default::default()
            });

            let resp = describe(&broker, req, &ctx).await;

            assert!(
                resp == DescribeTopicPartitionsResponse {
                    throttle_time_ms: 0,
                    topics: vec![error_topic("a", codes::INVALID_REQUEST)],
                    next_cursor: None,
                    ..Default::default()
                },
                "case: {name}"
            );

            broker_handle.shutdown().await;
        }
    }

    /// `response_partition_limit = 0` floors to 1 partition, not 0 --
    /// Kafka's clamp is `max(min(max.request.partition.size.limit,
    /// response_partition_limit), 1)`.
    #[tokio::test]
    async fn zero_partition_limit_floors_to_one() {
        seeded_topics_fixture!(
            (broker_handle, _dir, broker, ctx),
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            [TopicSetup {
                topic: "a",
                partitions: PartitionCount(2),
                ..TopicSetup::single_replica()
            }]
        );
        let req = request(DescribeTopicsSetup {
            response_partition_limit: ResponsePartitionLimit(0),
            ..Default::default()
        });

        let resp = describe(&broker, req, &ctx).await;

        let topic = resp
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some("a"))
            .expect("topic a row");
        assert!(topic.partitions == vec![partition_row(PartitionIndex(0))]);
        assert!(
            resp.next_cursor
                == Some(ResponseCursor {
                    topic_name: "a".into(),
                    partition_index: 1,
                    ..Default::default()
                })
        );

        broker_handle.shutdown().await;
    }

    /// When the partition budget runs out exactly at a topic boundary, Kafka
    /// writes no row for the next topic at all -- it just points the cursor
    /// at it, `(name, 0)`. This holds whether that next topic exists in the
    /// image or not: the budget check runs before the existence check ever
    /// would, so a cursor can legitimately name a topic the image doesn't
    /// hold without krabka resolving it to an `UNKNOWN_TOPIC_OR_PARTITION`
    /// row.
    #[tokio::test]
    async fn budget_exhausted_at_topic_boundary_emits_no_row_for_the_next_topic() {
        for next_topic_exists in [true, false] {
            let (broker_handle, _dir) =
                start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
            seed_topic(
                &broker_handle,
                TopicSetup {
                    topic: "a",
                    ..TopicSetup::single_replica()
                },
            )
            .await;
            if next_topic_exists {
                seed_topic(
                    &broker_handle,
                    TopicSetup {
                        topic: "b",
                        topic_id: uuid::Uuid::from_u128(2),
                        ..TopicSetup::single_replica()
                    },
                )
                .await;
            }
            let broker = broker_handle.broker_arc_for_test();
            test_ctx!(ctx, "admin");
            let req = request(DescribeTopicsSetup::one_partition(&["a", "b"]));

            let resp = describe(&broker, req, &ctx).await;

            let names = response_topic_names(&resp);
            assert!(
                names == vec!["a"],
                "next_topic_exists = {next_topic_exists}"
            );
            assert!(
                resp.next_cursor
                    == Some(ResponseCursor {
                        topic_name: "b".into(),
                        partition_index: 0,
                        ..Default::default()
                    }),
                "next_topic_exists = {next_topic_exists}"
            );

            broker_handle.shutdown().await;
        }
    }

    /// A missing named topic's row answers `is_internal` (by name, since the
    /// image has no record of the topic to look it up on) and
    /// `topic_authorized_operations` -- neither field is left at its unset
    /// default the way a Deny row's is -- and its error code follows
    /// `Topic.validate`: 17 for a name it refuses, 3 otherwise.
    ///
    /// `__transaction_state` is internal (`INTERNAL_TOPICS`), and no broker
    /// creates it at startup, so it is `UNKNOWN_TOPIC_OR_PARTITION` here.
    #[tokio::test]
    async fn missing_topic_row_follows_topic_validate() {
        let too_long = "a".repeat(250);
        let cases: [(&str, i16, bool); 5] = [
            (
                "__transaction_state",
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                true,
            ),
            ("no-such-topic", codes::UNKNOWN_TOPIC_OR_PARTITION, false),
            ("bad/name", codes::INVALID_TOPIC_EXCEPTION, false),
            ("..", codes::INVALID_TOPIC_EXCEPTION, false),
            (&too_long, codes::INVALID_TOPIC_EXCEPTION, false),
        ];
        broker_fixture!(
            (broker_handle, _dir, broker),
            allow_all,
            context(ctx, "admin")
        );

        for (name, error_code, is_internal) in cases {
            let req = request(DescribeTopicsSetup {
                topics: vec![name].into_iter().map(str::to_owned).collect(),
                ..Default::default()
            });
            let resp = describe(&broker, req, &ctx).await;

            let expected_ops = authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &broker.controller.current_image(),
                &ctx,
                ResourceType::Topic,
                name,
            );
            assert!(expected_ops != i32::MIN);
            assert!(
                resp == DescribeTopicPartitionsResponse {
                    throttle_time_ms: 0,
                    topics: vec![DescribeTopicPartitionsResponseTopic {
                        error_code,
                        name: Some(name.into()),
                        topic_id: WireUuid::ZERO,
                        is_internal,
                        partitions: Vec::new(),
                        topic_authorized_operations: expected_ops,
                        ..Default::default()
                    }],
                    next_cursor: None,
                    ..Default::default()
                },
                "case {name}"
            );
        }

        broker_handle.shutdown().await;
    }

    /// Seed a two-partition RF=3 topic whose partition 0 carries the KIP-966
    /// ELR state `elr_config` and whose partition 1 carries none.
    ///
    /// Only node 1 is registered, so nodes 2 and 3 are offline replicas: this
    /// is the shape a partition has when it *has* an ELR, because a replica
    /// only becomes eligible-but-not-in-ISR when its broker stops keeping up.
    async fn seed_topic_with_elr(handle: &BrokerHandle, elr_config: &str) {
        let partition = |index: i32| {
            MetadataRecord::V1Partition(PartitionRecord {
                isr: vec![NodeId(1)],
                leader_epoch: krabka_metadata::LeaderEpoch(7),
                directories: vec![uuid::Uuid::nil(); 3],
                partition_epoch: 4,
                ..crate::handlers::test_support::replicated_partition(
                    crate::handlers::test_support::ReplicatedPartitionSetup {
                        partition: krabka_ids::PartitionIndex(index),
                        leader: NodeId(1),
                        replicas: &[NodeId(1), NodeId(2), NodeId(3)],
                        ..Default::default()
                    },
                )
            })
        };
        let mut records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".into(),
                topic_id: uuid::Uuid::from_u128(1),
                partitions: 2,
                replication_factor: 3,
            }),
            partition(0),
            partition(1),
        ];
        records.extend(crate::elr::state::test_records("orders", elr_config));
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(records)
            .await
            .expect("seed topic + partitions + ELR state");
    }

    /// The whole partition row, for a partition with a populated ELR set and
    /// for a sibling partition with none.
    ///
    /// Comparing the entire struct is what pins the nullable-vs-empty
    /// encoding: a partition with no ELR must answer with two empty lists and
    /// not with null, because `kafka-topics --describe` renders a null as
    /// `Elr: N/A` -- "this broker does not know" -- while a real Kafka broker
    /// renders `Elr: `.
    #[tokio::test]
    async fn response_partition_carries_the_elr_the_image_holds() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic_with_elr(&broker_handle, "0:2:3").await;
        let broker = broker_handle.broker_arc_for_test();
        let topic = orders_topic(&broker).await;
        let row = |index: PartitionIndex| DescribeTopicPartitionsResponsePartition {
            error_code: codes::NONE,
            partition_index: index.0,
            leader_id: 1,
            leader_epoch: 7,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1],
            eligible_leader_replicas: Some(if index == PartitionIndex(0) {
                vec![2]
            } else {
                vec![]
            }),
            last_known_elr: Some(if index == PartitionIndex(0) {
                vec![3]
            } else {
                vec![]
            }),
            offline_replicas: vec![2, 3],
            ..Default::default()
        };
        assert!(topic.partitions == vec![row(PartitionIndex(0)), row(PartitionIndex(1))]);

        broker_handle.shutdown().await;
    }

    /// The response partition echoes the `leader_epoch` of the metadata image
    /// exactly (KIP-320). A non-zero epoch pins the field against the
    /// struct-field-deletion mutant, which would set it to 0.
    #[tokio::test]
    async fn response_partition_carries_leader_epoch() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic_with_epoch(&broker_handle, LeaderEpoch(9)).await;
        let broker = broker_handle.broker_arc_for_test();
        let topic = orders_topic(&broker).await;
        let part = topic
            .partitions
            .iter()
            .find(|p| p.partition_index == 0)
            .expect("partition 0 row");
        assert!(
            part.leader_epoch == 9,
            "response must echo the image leader_epoch (9), got {}",
            part.leader_epoch
        );
        broker_handle.shutdown().await;
    }
    async fn orders_topic(broker: &Broker) -> DescribeTopicPartitionsResponseTopic {
        test_ctx!(ctx, "admin");
        let req = DescribeTopicPartitionsRequest {
            topics: vec![
                krabka_protocol::owned::describe_topic_partitions_request::TopicRequest {
                    name: "orders".into(),
                    ..Default::default()
                },
            ],
            response_partition_limit: 2000,
            ..Default::default()
        };

        let resp = handle(broker, req, VERSION, &ctx).await.expect("handle");

        resp.topics
            .into_iter()
            .find(|t| t.name.as_deref() == Some("orders"))
            .expect("orders topic row")
    }
}
