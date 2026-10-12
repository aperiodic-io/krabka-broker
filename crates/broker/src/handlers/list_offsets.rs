//! `ListOffsets` (`api_key=2`). The handler resolves the EARLIEST / LATEST
//! sentinels with each partition's log. For tiered topics (KIP-405),
//! EARLIEST and by-timestamp lookups consult the
//! [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager).
//! Local-retention deletes some offsets locally, but they still live in the
//! remote tier, and this keeps them visible. KIP-1005's latest-tiered (`-5`)
//! and KIP-1023's earliest-pending-upload (`-6`) sentinels read the same
//! metadata asynchronously. KIP-1075 bounds that remote work by the request
//! timeout and resolves all requested partitions concurrently.
//!
//! Positive-timestamp lookups resolve against the remote tier first, because
//! it holds the oldest records. They then fall back to the local log's
//! time index (KIP-405/734). The handler resolves the `MAX_TIMESTAMP` (-3) and
//! `EARLIEST_LOCAL_TIMESTAMP` (-4) sentinels against the local log.
//!
//! KFC-1 changes one sentinel and no other: on a topic that schedules
//! delivery, LATEST reports the partition's delivery watermark instead of its
//! log end offset. See [`scheduled_latest_offset`](self::local::scheduled_latest_offset).
//!
//! KIP-320 fences the whole request the way Fetch does: a partition row that
//! carries a `current_leader_epoch` (v4 and up) is answered with
//! `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH` when that epoch is not the
//! partition's live one, so a consumer with stale metadata refreshes instead of
//! receiving an offset its next Fetch would refuse. See
//! [`resolve_partition`](self::resolve::resolve_partition).
//!
//! Every other answer is decided by one bound, Kafka's `lastFetchableOffset`:
//! the log end offset for a request that is not a client's, the high watermark
//! for a `read_uncommitted` client, and the last stable offset (KIP-98) for a
//! `read_committed` one. `Partition.fetchOffsetForTimestamp` chooses it once
//! and then uses it twice over. LATEST *is* the bound, so a `read_committed`
//! consumer that seeks to end stops in front of the records of a transaction
//! that is still open instead of stepping over them. Every sentinel that
//! resolves against record data -- `MAX_TIMESTAMP`, the two tiered sentinels,
//! and a positive timestamp -- is refused with `UNKNOWN_OFFSET` when it lands
//! at or above the bound, so no client can read an offset past its own end of
//! partition by asking for it a different way. EARLIEST and
//! `EARLIEST_LOCAL_TIMESTAMP` are the exceptions Kafka leaves unmeasured:
//! both resolve from the start of the log, which is never above the bound. See
//! [`FetchBound`](self::bound::FetchBound),
//! [`fetch_bound`](self::bound::fetch_bound) and
//! [`last_fetchable_offset`](self::bound::last_fetchable_offset).

use std::collections::HashSet;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        list_offsets_request::ListOffsetsRequest,
        list_offsets_response::{ListOffsetsResponse, ListOffsetsTopicResponse},
    },
};

mod bound;
mod diskless;
mod leadership;
mod local;
mod remote;
mod resolve;
mod response;
mod sentinels;
pub(crate) mod timestamp;
mod v0;

#[cfg(test)]
mod record_limit_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    bound::fetch_bound,
    remote::{concurrently, remote_timeout},
    resolve::resolve_partition,
    response::error_response,
};
use crate::{
    broker::Broker,
    codes,
    handlers::{RequestContext, acl_denied},
};

/// The `(topic_index, partition)` rows whose `(name, partition)` key appears
/// more than once in `topics`.
///
/// Kafka collects these once over the whole request, before authorization or
/// resolution runs, so a topic-partition named twice -- whether within one
/// topic entry or across two entries for the same topic -- is answered
/// `INVALID_REQUEST` on every row that names it. The result is keyed by
/// `topic_index` rather than a cloned topic name so a request with a large
/// name and many partition rows can't multiply that name into an
/// unbounded amount of retained memory.
fn duplicate_partition_keys<'a, P: IntoIterator<Item = i32>>(
    topics: impl Iterator<Item = (&'a str, P)> + Clone,
) -> HashSet<(usize, i32)> {
    let mut counts: std::collections::HashMap<(&str, i32), usize> =
        std::collections::HashMap::new();
    for (name, partitions) in topics.clone() {
        for partition in partitions {
            *counts.entry((name, partition)).or_insert(0) += 1;
        }
    }
    let mut duplicates = HashSet::new();
    for (topic_index, (name, partitions)) in topics.enumerate() {
        for partition in partitions {
            if counts[&(name, partition)] > 1 {
                duplicates.insert((topic_index, partition));
            }
        }
    }
    duplicates
}

type IndexedTopics<T> = Vec<(usize, T)>;

/// Retain request indexes while moving Describe-authorized rows ahead of denials.
fn partition_topics_by_describe<T>(
    topics: Vec<T>,
    name: fn(&T) -> &str,
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
) -> (IndexedTopics<T>, IndexedTopics<T>) {
    topics.into_iter().enumerate().partition(|(_, topic)| {
        !acl_denied(
            broker.config.authorizer.as_ref(),
            image,
            ctx,
            ResourceType::Topic,
            name(topic),
            AclOperation::Describe,
        )
    })
}

wire_handler! {
    async (broker, version, req_bytes, ctx), {
        if version == 0 {
            return v0::handle(broker, req_bytes, ctx).await;
        }

        let controller = broker.controller.clone();
        {
            let req = ListOffsetsRequest::decode(&mut &*req_bytes, version)?;
            // `isolation_level` decodes only from v2 up; v1 leaves it at 0, which
            // is `read_uncommitted`, exactly as Kafka treats a v1 request.
            let bound = fetch_bound(req.replica_id, req.isolation_level);

            // ── ACL preamble ────────────────────────────────────────────
            // Per-topic `Describe` on `Topic(name)`. A denied topic gets
            // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
            // requested. Kafka's `handleListOffsetRequest` splits topics into
            // authorized and unauthorized up front (`AuthHelper.
            // partitionMapToAuthorizedPartitionsAndErrors`), processes only the
            // authorized ones, and appends the unauthorized rows after them
            // (`mergedResponses.addAll(unauthorizedResponseStatus)`) rather than
            // interleaving them in request order.
            let acl_image = controller.current_image();

            let timeout = remote_timeout(
                version,
                req.timeout_ms,
                crate::config_keys::resolve_remote_list_offsets_timeout(
                    &acl_image,
                    broker.config.node_id,
                ),
            );
            // Kafka's `ListOffsetRequest.duplicatePartitions()` is computed once
            // over the whole request, ahead of authorization and resolution, and
            // every row of a topic-partition it names more than once is answered
            // `INVALID_REQUEST` instead of being resolved. See
            // `ReplicaManager.scala:1473-1478`.
            let duplicates = duplicate_partition_keys(req.topics.iter().map(|topic| {
                (
                    topic.name.as_str(),
                    topic.partitions.iter().map(|part| part.partition_index),
                )
            }));

            let (authorized_topics, denied_topics) =
                partition_topics_by_describe(req.topics, |topic| &topic.name, broker, &acl_image, ctx);

            let mut topics_out =
                concurrently(authorized_topics.into_iter().map(|(topic_index, topic)| {
                    let duplicates = &duplicates;
                    async move {
                        let name = topic.name;
                        let partitions = concurrently(topic.partitions.into_iter().map(|part| {
                            let is_duplicate =
                                duplicates.contains(&(topic_index, part.partition_index));
                            let name = name.clone();
                            async move {
                                if is_duplicate {
                                    error_response(part.partition_index, codes::INVALID_REQUEST)
                                } else {
                                    resolve_partition(broker, &name, part, version, timeout, bound)
                                        .await
                                }
                            }
                        }))
                        .await;
                        ListOffsetsTopicResponse {
                            name,
                            partitions,
                            ..Default::default()
                        }
                    }
                }))
                .await;

            topics_out.extend(denied_topics.into_iter().map(|(_, topic)| {
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|part| error_response(part.partition_index, codes::TOPIC_AUTHORIZATION_FAILED))
                    .collect();
                ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions,
                    ..Default::default()
                }
            }));

            let resp = ListOffsetsResponse {
                throttle_time_ms: 0,
                topics: topics_out,
                ..Default::default()
            };
            crate::handlers::encode_response(&resp, version)
        }
    }
}
