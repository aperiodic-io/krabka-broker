//! The KIP-516 batched `groups[]` `OffsetFetch` shape, version 8 and above.
//!
//! From version 8 one request carries several groups, each with its own topic
//! list and its own error code, and from version 10 the topics are keyed by
//! `topic_id` rather than by name. Internal offset storage stays keyed by
//! name, so this module resolves each id to a name at the wire boundary and
//! echoes the id back on the response. The legacy single-group shape lives in
//! `legacy`.

use krabka_protocol::{
    owned::{
        offset_fetch_request::OffsetFetchRequest,
        offset_fetch_response::{
            OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
            OffsetFetchResponseTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use super::{
    authz::{group_error, topic_decisions, visible_topics},
    committed::{
        committed_topics, fetch_offsets, missing_group_row as missing_offset_row, stable_group_row,
    },
    unstable,
};
use crate::{
    authorizer::AuthorizationResult,
    broker::Broker,
    codes,
    coordinator::unified::group::GroupOffsets,
    handlers::{RequestContext, requested_topic_name},
};

/// Per-group fetch for v8 and above.
///
/// It processes `req.groups` into `resp.groups` and leaves `resp.topics`
/// empty, because the encoder writes `resp.topics` only for v < 8.
///
/// The offset storage keys by name, so at v10 this function resolves each
/// requested `topic_id` to a name and echoes the id back. An unknown id gives
/// `UNKNOWN_TOPIC_ID` for each partition.
///
/// `require_stable` is a top-level request field, not a per-group one, so one
/// request either asks every group it names for stable offsets or none of
/// them.
// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_groups(
    broker: &Broker,
    version: i16,
    req: &OffsetFetchRequest,
    ctx: &RequestContext<'_>,
) -> OffsetFetchResponse {
    let mut groups_out: Vec<OffsetFetchResponseGroup> = Vec::with_capacity(req.groups.len());

    for grp in &req.groups {
        if let Some(error_code) = group_error(broker, ctx, &grp.group_id) {
            groups_out.push(refused_group(&grp.group_id, error_code));
            continue;
        }

        // Fetch the group's offset state from its actor. An unknown id reads
        // as a group with no offsets and creates nothing. A consumer group
        // checks the v9+ member id and epoch first, and a refusal is the
        // group's error code with no topics (Kafka's
        // `OffsetFetchResponse.groupError`).
        let offsets = match fetch_offsets(
            broker,
            &grp.group_id,
            grp.member_id.as_deref(),
            grp.member_epoch,
        )
        .await
        {
            Ok(offsets) => offsets,
            Err(error_code) => {
                groups_out.push(refused_group(&grp.group_id, error_code));
                continue;
            }
        };
        let image = broker.controller.current_image();

        // Named/id'd topics: resolve id→name (v10) and read each requested
        // partition from the name-keyed store. `None` topics → fetch-all.
        let topics_out: Vec<OffsetFetchResponseTopics> =
            if let Some(req_topics) = grp.topics.as_deref() {
                group_named_topics(
                    broker,
                    ctx,
                    version,
                    &image,
                    req_topics,
                    &offsets,
                    req.require_stable,
                )
            } else {
                group_fetch_all(broker, ctx, version, &image, &offsets, req.require_stable)
            };

        groups_out.push(OffsetFetchResponseGroup {
            group_id: grp.group_id.clone(),
            topics: topics_out,
            error_code: codes::NONE,
            ..Default::default()
        });
    }

    OffsetFetchResponse {
        topics: Vec::new(),
        error_code: codes::NONE,
        throttle_time_ms: 0,
        groups: groups_out,
        ..Default::default()
    }
}

fn refused_group(group_id: &str, error_code: i16) -> OffsetFetchResponseGroup {
    OffsetFetchResponseGroup {
        group_id: group_id.to_owned(),
        topics: Vec::new(),
        error_code,
        ..Default::default()
    }
}

/// The first `OffsetFetch` version that names each topic by `topic_id` only.
/// The request schema carries `name` at versions 8-9 and `topic_id` from this
/// version on (Kafka's `OffsetFetchRequest.TOPIC_ID_MIN_VERSION`).
const FIRST_TOPIC_ID_VERSION: i16 = 10;

/// Builds one group's rows for an explicit topic list on the KIP-516 shape.
///
/// The precedence is the one in Kafka's `KafkaApis.fetchOffsetsForGroup`. At
/// v10 each `topic_id` resolves to a name, and a row whose name stays empty
/// answers `UNKNOWN_TOPIC_ID` on every partition. The zero id is such a row.
/// A topic without a `Describe` grant then answers
/// `TOPIC_AUTHORIZATION_FAILED`.
/// Within an allowed topic, a partition that an unresolved transaction has
/// written reports `UNSTABLE_OFFSET_COMMIT` under `require_stable` before any
/// offset is read.
///
/// The allowed topics come first, in request order, and the refused topics
/// follow them, as Kafka appends `errorTopics` after the coordinator's rows.
fn group_named_topics(
    broker: &Broker,
    context: &RequestContext<'_>,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    requested: &[krabka_protocol::owned::offset_fetch_request::OffsetFetchRequestTopics],
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopics> {
    let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    let resolved: Vec<_> = requested
        .iter()
        // Below v10 a row carries only `name`, from v10 only `topic_id`.
        .map(|topic| {
            let name = requested_topic_name(image, &topic.name, topic.topic_id);
            (topic, name)
        })
        .collect();
    let decisions = topic_decisions(
        broker,
        image,
        context,
        resolved
            .iter()
            .filter(|(_, name)| !(use_topic_ids && name.is_empty()))
            .map(|(_, name)| name.as_str()),
    );

    let mut allowed = Vec::with_capacity(resolved.len());
    let mut refused = Vec::new();
    for (topic, name) in &resolved {
        let refusal = if use_topic_ids && name.is_empty() {
            Some(codes::UNKNOWN_TOPIC_ID)
        } else if decisions.get(name.as_str()).copied() == Some(AuthorizationResult::Allow) {
            None
        } else {
            Some(codes::TOPIC_AUTHORIZATION_FAILED)
        };
        let partitions = topic
            .partition_indexes
            .iter()
            .map(|&partition| match refusal {
                Some(error_code) => missing_offset_row(partition, error_code),
                None => committed_row(
                    name,
                    partition,
                    use_topic_ids.then(|| uuid::Uuid::from_bytes(topic.topic_id.0)),
                    offsets,
                    require_stable,
                ),
            });
        let row = OffsetFetchResponseTopics {
            name: name.clone(),
            topic_id: topic.topic_id,
            partitions: partitions.collect(),
            ..Default::default()
        };
        if refusal.is_some() {
            refused.push(row);
        } else {
            allowed.push(row);
        }
    }
    allowed.extend(refused);
    allowed
}

/// Builds one group's rows for the fetch-all sentinel (a null topic list) on
/// the KIP-516 shape.
///
/// Kafka's `KafkaApis.fetchAllOffsetsForGroup` keeps only the topics that the
/// principal may `Describe`, and leaves every other topic out of the response
/// rather than answering it with an error. The coordinator sends no topic id,
/// so the id comes from the metadata image, and at v10, where the wire carries
/// only the id, a topic that the image does not hold is left out too because
/// it cannot be written without one. The topics come in name order.
fn group_fetch_all(
    broker: &Broker,
    context: &RequestContext<'_>,
    version: i16,
    image: &krabka_metadata::MetadataImage,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> Vec<OffsetFetchResponseTopics> {
    let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
    let by_topic = committed_topics(offsets, |topic, partition| {
        committed_row(topic, partition, None, offsets, require_stable)
    });
    visible_topics(broker, image, context, by_topic)
        .filter_map(|(name, mut partitions)| {
            let topic_id = image
                .topic(name)
                .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));
            if use_topic_ids && topic_id == WireUuid::ZERO {
                return None;
            }
            partitions.sort_by_key(|p| p.partition_index);
            Some(OffsetFetchResponseTopics {
                name: name.to_string(),
                topic_id,
                partitions,
                ..Default::default()
            })
        })
        .collect()
}

/// One partition row of a named topic, as Kafka's
/// `OffsetMetadataManager.fetchOffsets` builds it: `UNSTABLE_OFFSET_COMMIT`
/// under `require_stable` while a transaction holds the partition, then the
/// committed offset, or a -1 row when there is none or when it belongs to
/// another incarnation of the topic.
///
/// `requested_topic_id` is the id a v10+ request named the topic by; earlier
/// versions name it by name only, and so does the fetch-all path, as Kafka's
/// request carries the zero id there.
fn committed_row(
    topic: &str,
    partition_index: i32,
    requested_topic_id: Option<uuid::Uuid>,
    offsets: &GroupOffsets,
    require_stable: bool,
) -> OffsetFetchResponsePartitions {
    let key = (topic.to_string(), partition_index);
    if require_stable && offsets.pending_txn.contains(&key) {
        return unstable::group_row(partition_index);
    }
    offsets
        .committed
        .get(&key)
        .filter(|entry| !is_mismatched_topic_id(entry.topic_id, requested_topic_id))
        .map_or_else(
            || missing_offset_row(partition_index, codes::NONE),
            |entry| stable_group_row(partition_index, entry),
        )
}

/// Kafka's `OffsetMetadataManager.isMismatchedTopicId`: an offset stored for
/// one topic id is not the offset of a topic named by another. A zero id on
/// either side, `None` here, matches anything.
fn is_mismatched_topic_id(stored: Option<uuid::Uuid>, requested: Option<uuid::Uuid>) -> bool {
    matches!((stored, requested), (Some(stored), Some(requested)) if stored != requested)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::classic_state::OffsetEntry;

    /// #987, Kafka's `isOffsetInvalid`: an `OffsetFetch` that names the topic
    /// by an id other than the one the offset was committed under reads no
    /// offset, as for a topic created again under the same name. A zero id on
    /// either side, a pre-v10 request or a record from before version 4,
    /// never mismatches.
    #[test]
    fn an_offset_of_another_topic_id_reads_as_absent() {
        let committed_id = uuid::Uuid::from_u128(1);
        let other_id = uuid::Uuid::from_u128(2);
        let offsets = |topic_id| GroupOffsets {
            committed: std::collections::HashMap::from([(
                ("orders".to_string(), 0),
                OffsetEntry {
                    offset: krabka_log::Offset(7),
                    leader_epoch: 3,
                    metadata: "m".into(),
                    commit_timestamp_ms: 0,
                    expire_timestamp_ms: None,
                    topic_id,
                },
            )]),
            pending_txn: std::collections::HashSet::new(),
        };
        let found = OffsetFetchResponsePartitions {
            partition_index: 0,
            committed_offset: 7,
            committed_leader_epoch: 3,
            metadata: Some("m".into()),
            error_code: codes::NONE,
            ..Default::default()
        };
        let absent = missing_offset_row(0, codes::NONE);
        for (stored, requested, want) in [
            (Some(committed_id), Some(committed_id), &found),
            (Some(committed_id), Some(other_id), &absent),
            (Some(committed_id), None, &found),
            (None, Some(other_id), &found),
            (None, None, &found),
        ] {
            check!(
                committed_row("orders", 0, requested, &offsets(stored), false) == *want,
                "stored {stored:?}, requested {requested:?}"
            );
        }
    }
}
