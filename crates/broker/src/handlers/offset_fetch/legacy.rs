//! The legacy single-group `OffsetFetch` shape, versions 0 through 7.
//!
//! Before KIP-516 the request carried one `group_id` and one optional
//! `topics` list, and the response carried one flat `topics` array. This
//! module keeps that shape whole: the group gate, the coordinator check, the
//! fetch-all sentinel, and the named-topic rows the client asked for. The v8
//! and above `groups[]` shape lives in `groups` and shares nothing but the
//! group gate and the offset read.

use {
    super::{
        authz::{group_error, topic_decisions, visible_topics},
        committed::{
            committed_topics, fetch_offsets, missing_legacy_row as missing_offset_row,
            stable_legacy_row,
        },
        unstable,
    },
    crate::{
        authorizer::AuthorizationResult, broker::Broker, codes,
        coordinator::unified::group::GroupOffsets, handlers::RequestContext,
    },
    krabka_protocol::owned::{
        offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
        },
    },
};

/// Serves an `OffsetFetch` request in the pre-KIP-516 single-group shape.
///
/// It gates the group, resolves the group's committed offsets, and fills
/// `resp.topics`, which the encoder writes only for versions below 8.
///
/// `req.require_stable` decodes as `false` below v7, so a pre-KIP-447 client
/// keeps seeing the stable offset for a partition an open transaction has
/// written.
// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_legacy(
    broker: &Broker,
    req: &OffsetFetchRequest,
    ctx: &RequestContext<'_>,
) -> OffsetFetchResponse {
    if let Some(error_code) = group_error(broker, ctx, &req.group_id) {
        return OffsetFetchResponse {
            topics: Vec::new(),
            error_code,
            throttle_time_ms: 0,
            ..Default::default()
        };
    }

    // Fetch the group's offset state from its actor. An unknown id reads as a
    // group with no offsets and creates nothing. The legacy shape carries no
    // member fields, and Kafka validates it as a fetch with no member id and
    // epoch -1, which every group accepts.
    let offsets = fetch_offsets(broker, &req.group_id, None, -1)
        .await
        .unwrap_or_default();

    // A `None` `topics` field (v ≥ 2) is the "fetch all" sentinel:
    // return every committed offset stored for this group.
    let topics_out: Vec<OffsetFetchResponseTopic> = if req.topics.is_none() {
        legacy_fetch_all(broker, ctx, &offsets, req.require_stable)
    } else {
        legacy_named_topics(
            broker,
            ctx,
            req.topics.as_deref().unwrap_or(&[]),
            &offsets,
            req.require_stable,
        )
    };

    OffsetFetchResponse {
        topics: topics_out,
        error_code: codes::NONE,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

/// Builds the response rows for an explicit topic list on the legacy shape.
///
/// Kafka serves the legacy shape through the same `fetchOffsetsForGroup` as
/// the KIP-516 shape and flattens the one group's rows. Each requested topic
/// is gated with `Describe`. A refused topic answers every partition with
/// `TOPIC_AUTHORIZATION_FAILED`, and its row follows every allowed row, as
/// Kafka appends `errorTopics` after the coordinator's rows. An offset the
/// group never committed reports `-1` with no error, which is what the JVM
/// consumer expects for an unset partition. Under `require_stable`, a
/// partition an unresolved transaction has written reports
/// `UNSTABLE_OFFSET_COMMIT` ahead of either of those.
fn legacy_named_topics(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req_topics: &[OffsetFetchRequestTopic],
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopic> {
    let topic_decisions = {
        let image = broker.controller.current_image();
        topic_decisions(
            broker,
            &image,
            ctx,
            req_topics.iter().map(|t| t.name.as_str()),
        )
    };

    let mut allowed = Vec::with_capacity(req_topics.len());
    let mut refused = Vec::new();
    for topic in req_topics {
        let authorized =
            topic_decisions.get(topic.name.as_str()).copied() == Some(AuthorizationResult::Allow);
        let partitions = topic
            .partition_indexes
            .iter()
            .map(|&partition| {
                if authorized {
                    committed_row(&topic.name, partition, offsets, require_stable)
                } else {
                    missing_offset_row(partition, codes::TOPIC_AUTHORIZATION_FAILED)
                }
            })
            .collect();
        let row = OffsetFetchResponseTopic {
            name: topic.name.clone(),
            partitions,
            ..Default::default()
        };
        if authorized {
            allowed.push(row);
        } else {
            refused.push(row);
        }
    }
    allowed.extend(refused);
    allowed
}

/// Builds the response rows for the fetch-all sentinel on the legacy shape.
///
/// The rows come from the group's stable offsets, so a partition that an open
/// transaction has written but that has no earlier committed offset is absent
/// here, exactly as it is in Kafka. `require_stable` still applies to the rows
/// that are present. Kafka's `fetchAllOffsetsForGroup` leaves out every topic
/// that the principal may not `Describe`, rather than answering it with an
/// error. The topics come in name order.
fn legacy_fetch_all(
    broker: &Broker,
    context: &RequestContext<'_>,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopic> {
    let by_topic = committed_topics(offsets, |topic, partition| {
        committed_row(topic, partition, offsets, require_stable)
    });
    let image = broker.controller.current_image();
    visible_topics(broker, &image, context, by_topic)
        .map(|(name, mut partitions)| {
            partitions.sort_by_key(|p| p.partition_index);
            OffsetFetchResponseTopic {
                name: name.to_string(),
                partitions,
                ..Default::default()
            }
        })
        .collect()
}

/// The row of one partition of an allowed topic.
fn committed_row(
    topic: &str,
    partition_index: i32,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> OffsetFetchResponsePartition {
    let key = (topic.to_string(), partition_index);
    if require_stable && offsets.pending_txn.contains(&key) {
        return unstable::legacy_row(partition_index);
    }
    offsets.committed.get(&key).map_or_else(
        || missing_offset_row(partition_index, codes::NONE),
        |entry| stable_legacy_row(partition_index, entry),
    )
}
