//! `OffsetCommit` (`api_key=8`). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to the group-id's
//! `__consumer_offsets` partition
//! via the partition writer, then updates the group's committed offsets
//! through its actor.
//!
//! KIP-211: a v2-v4 request may carry `retention_time_ms`, which overrides the
//! broker's `offsets.retention.minutes` for the offsets in that one request.
//! The handler turns it into the absolute `expire_timestamp_ms` that the
//! record and the in-memory entry both carry, and
//! `coordinator::retention` honours it. The field was removed at v5,
//! where the decoder leaves it at its `-1` default, and `-1` at v2-v4 means
//! the same thing: take the broker-wide retention.

use std::sync::Arc;

use krabka_metadata::AclOperation;
use krabka_protocol::{
    owned::{
        offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestTopic},
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use self::response::ResponseBuilder;
use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::{
        persistence::OffsetCommitValue,
        unified::{
            actor::{
                CommitFence, CommitRequest, GroupActorHandle, GroupActorMessage, GroupKindTag,
                validate_commit,
            },
            classic_state::OffsetEntry,
            streams::actor::validate_streams_group_offset_commit,
        },
    },
    error::BrokerError,
    task_util::ask,
};

mod response;

#[cfg(test)]
mod group_validation_tests;
#[cfg(test)]
mod topic_resolution_tests;

/// The first `OffsetCommit` version that names each topic by `topic_id` only
/// (KIP-848). The request schema carries `name` at versions 0-9 and
/// `topic_id` from this version on.
const FIRST_TOPIC_ID_VERSION: i16 = 10;

/// The first `OffsetCommit` version that answers `GROUP_ID_NOT_FOUND` for a
/// group that does not exist. Earlier versions answer `ILLEGAL_GENERATION`.
const FIRST_GROUP_ID_NOT_FOUND_VERSION: i16 = 9;

context_handler! {
    /// Serves one `OffsetCommit` request.
    ///
    /// The order of the checks is the order of Kafka's
    /// `KafkaApis.handleOffsetCommitRequest`:
    ///
    /// 1. `Read` on `Group(group_id)`. A denial answers
    ///    `GROUP_AUTHORIZATION_FAILED` on every partition row.
    /// 2. At v10 and later, each `topic_id` resolves to a name. A row whose name
    ///    stays empty answers `UNKNOWN_TOPIC_ID` on every partition row. The zero
    ///    id is such a row.
    /// 3. `Read` on each `Topic(name)`. A denied topic answers
    ///    `TOPIC_AUTHORIZATION_FAILED` on every partition row.
    /// 4. A topic or a partition that the image does not hold answers
    ///    `UNKNOWN_TOPIC_OR_PARTITION` on its partition rows.
    /// 5. The group coordinator commits the rows that remain, and a coordinator
    ///    error goes on those rows only.
    ///
    /// The error rows come first in the response and the committed rows follow,
    /// as `OffsetCommitResponse.Builder.merge` puts them. The handler writes no
    /// offset for a row that steps 2 to 4 refuse.
    OffsetCommitRequest => OffsetCommitResponse,
    (broker, mut req, version, ctx),
    {
        let image = broker.controller.current_image();

        if crate::handlers::group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            req.group_id.as_str(),
        ) {
            return Ok(build_response_all(&req, codes::GROUP_AUTHORIZATION_FAILED));
        }

        let use_topic_ids = version >= FIRST_TOPIC_ID_VERSION;
        if use_topic_ids {
            // v10+ rows carry only `topic_id`; a zero or unknown id keeps the
            // empty name.
            for topic in &mut req.topics {
                topic.name = crate::handlers::requested_topic_name(&image, &topic.name, topic.topic_id);
            }
        }

        let allowed: Vec<bool> = {
            let decisions = authorize_topics(
                broker.config.authorizer.as_ref(),
                &*image,
                ctx.principal,
                ctx.peer,
                AclOperation::Read,
                req.topics
                    .iter()
                    .filter(|topic| !(use_topic_ids && topic.name.is_empty()))
                    .map(|topic| topic.name.as_str()),
            );
            req.topics
                .iter()
                .map(|topic| {
                    decisions.get(topic.name.as_str()).copied() == Some(AuthorizationResult::Allow)
                })
                .collect()
        };

        let mut response = ResponseBuilder::new(use_topic_ids);
        let mut accepted = Vec::with_capacity(req.topics.len());
        for (topic, allowed) in std::mem::take(&mut req.topics).into_iter().zip(allowed) {
            if use_topic_ids && topic.name.is_empty() {
                response.add_topic(&topic, codes::UNKNOWN_TOPIC_ID);
            } else if !allowed {
                response.add_topic(&topic, codes::TOPIC_AUTHORIZATION_FAILED);
            } else if let Some(topic) = existing_partitions(topic, &image, &mut response) {
                accepted.push(topic);
            }
        }
        if accepted.is_empty() {
            return Ok(response.build());
        }

        req.topics = accepted;
        response.merge(commit(broker, &req, version).await);
        Ok(response.build())
    }
}

/// Keeps the partitions of an authorized `topic` that the image holds.
///
/// A topic that the image does not hold answers `UNKNOWN_TOPIC_OR_PARTITION`
/// on every partition row, with the zero id. A partition that the image does
/// not hold answers `UNKNOWN_TOPIC_OR_PARTITION` on its own row. This is the
/// existence check of Kafka's `KafkaApis.handleOffsetCommitRequest`, which
/// runs after the topic `Read` check and keeps every refused row away from
/// the group coordinator. It returns `None` when no partition remains.
fn existing_partitions(
    mut topic: OffsetCommitRequestTopic,
    image: &krabka_metadata::MetadataImage,
    response: &mut ResponseBuilder,
) -> Option<OffsetCommitRequestTopic> {
    if image.topic(&topic.name).is_none() {
        topic.topic_id = WireUuid::ZERO;
        response.add_topic(&topic, codes::UNKNOWN_TOPIC_OR_PARTITION);
        return None;
    }
    let (present, missing): (Vec<_>, Vec<_>) = std::mem::take(&mut topic.partitions)
        .into_iter()
        .partition(|partition| {
            image
                .partition(&topic.name, partition.partition_index)
                .is_some()
        });
    for partition in missing {
        response.add_partition(
            topic.topic_id,
            &topic.name,
            partition.partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
        );
    }
    topic.partitions = present;
    (!topic.partitions.is_empty()).then_some(topic)
}

/// Commits the rows of `req` through the group coordinator, and returns the
/// coordinator's topic rows.
///
/// The group routing, the membership and epoch check, and the append each
/// answer with one code for the whole commit, as Kafka's
/// `GroupCoordinatorService.commitOffsets` does with
/// `OffsetCommitRequest.getErrorResponse`. Past those, a partition whose
/// metadata is too large answers `OFFSET_METADATA_TOO_LARGE` on its own row,
/// and the others commit, as `OffsetMetadataManager.commitOffset` does.
async fn commit(
    broker: &Broker,
    req: &OffsetCommitRequest,
    version: i16,
) -> Vec<OffsetCommitResponseTopic> {
    match commit_rows(broker, req, version).await {
        Ok(topics) => topics,
        Err(code) => build_response_all(req, code).topics,
    }
}

async fn commit_rows(
    broker: &Broker,
    req: &OffsetCommitRequest,
    version: i16,
) -> Result<Vec<OffsetCommitResponseTopic>, i16> {
    let image = broker.controller.current_image();
    if let Some(code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
        return Err(code);
    }

    let now_ms = crate::time_util::now_ms();
    let expire_timestamp_ms = expire_timestamp_ms(req.retention_time_ms, now_ms);
    // Kafka's `commitOffset` runs the per-partition validator only on the
    // partitions whose metadata fits, so split them out first.
    let (valid, rows) = split_oversized_metadata(req, broker.config.offset_metadata_max_bytes);
    let handle = validate(broker, &valid, &image, version).await?;

    if valid
        .topics
        .iter()
        .any(|topic| !topic.partitions.is_empty())
    {
        let commit = Commit {
            now_ms,
            expire_timestamp_ms,
            image: &image,
        };
        commit_through_actor(&handle, &valid, commit).await?;
    }
    Ok(rows)
}

/// Whether `metadata` is longer than `max_bytes`, as Kafka's
/// `OffsetMetadataManager.isMetadataInvalid` measures it: in UTF-16 code
/// units, which is Java's `String.length()`. A null metadata is valid.
fn metadata_too_large(metadata: Option<&str>, max_bytes: i32) -> bool {
    metadata.is_some_and(|metadata| {
        i64::try_from(metadata.encode_utf16().count()).unwrap_or(i64::MAX) > i64::from(max_bytes)
    })
}

/// Splits `req` into the request that holds the partitions whose metadata
/// fits in `max_bytes`, and the coordinator's topic rows in request order: a
/// partition whose metadata is too large answers `OFFSET_METADATA_TOO_LARGE`,
/// and every other partition answers `NONE`.
fn split_oversized_metadata(
    req: &OffsetCommitRequest,
    max_bytes: i32,
) -> (OffsetCommitRequest, Vec<OffsetCommitResponseTopic>) {
    let mut valid = OffsetCommitRequest {
        topics: Vec::with_capacity(req.topics.len()),
        ..req.clone()
    };
    let mut rows = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let mut row = OffsetCommitResponseTopic {
            name: topic.name.clone(),
            topic_id: topic.topic_id,
            ..Default::default()
        };
        let mut kept = OffsetCommitRequestTopic {
            partitions: Vec::with_capacity(topic.partitions.len()),
            ..topic.clone()
        };
        for partition in &topic.partitions {
            let error_code =
                if metadata_too_large(partition.committed_metadata.as_deref(), max_bytes) {
                    codes::OFFSET_METADATA_TOO_LARGE
                } else {
                    kept.partitions.push(partition.clone());
                    codes::NONE
                };
            row.partitions.push(OffsetCommitResponsePartition {
                partition_index: partition.partition_index,
                error_code,
                ..Default::default()
            });
        }
        valid.topics.push(kept);
        rows.push(row);
    }
    (valid, rows)
}

/// The wire value of `retention_time_ms` that asks for the broker's own
/// `offsets.retention.minutes`.
const DEFAULT_RETENTION_TIME_MS: i64 = -1;

/// The values every record of one commit shares.
#[derive(Clone, Copy)]
struct Commit<'a> {
    /// Commit time, stamped on the batch and on every `OffsetCommitValue`.
    now_ms: i64,
    /// The KIP-211 per-commit expiry, when the request asked for one.
    expire_timestamp_ms: Option<i64>,
    /// The image the commit was validated against. It gives each offset the
    /// topic id of its topic name, as `KafkaApis.handleOffsetCommitRequest`
    /// sets it from the metadata cache.
    image: &'a krabka_metadata::MetadataImage,
}

/// KIP-211: resolve the absolute expiry that this commit asked for.
///
/// `OffsetCommitRequest` carries `retention_time_ms` at v2-v4 only. The
/// decoder leaves the field at its schema default of `-1` for every other
/// version, and `-1` is also how a v2-v4 client says "use the broker's
/// `offsets.retention.minutes`". This mirrors
/// `OffsetMetadataManager.expireTimestampMs`.
fn expire_timestamp_ms(retention_time_ms: i64, now_ms: i64) -> Option<i64> {
    (retention_time_ms != DEFAULT_RETENTION_TIME_MS)
        .then(|| now_ms.saturating_add(retention_time_ms))
}

/// Finds the group of `req` and validates the commit against its membership,
/// as Kafka's `OffsetMetadataManager.validateOffsetCommit` does. It returns
/// the actor that holds the group's offsets, or the error code for every row.
///
/// A group that does not exist is created as a simple group when the
/// generation is negative, which is the admin client or a consumer that does
/// not use group management. Otherwise it answers `GROUP_ID_NOT_FOUND` from
/// v9 on and `ILLEGAL_GENERATION` before.
///
/// A classic or consumer group validates inside its actor, on the actor's
/// LIVE protocol: a KIP-848 migration may have flipped the protocol in place
/// after spawn. A streams group (KIP-1071) validates in its streams actor, and
/// its offsets live in a group actor of the same id, as `TxnOffsetCommit`
/// keeps them.
///
/// `req` holds the partitions that Kafka's `commitOffset` runs the
/// per-partition validator on: a consumer group checks them by topic id, and
/// a streams group by topic name.
async fn validate(
    broker: &Broker,
    req: &OffsetCommitRequest,
    image: &krabka_metadata::MetadataImage,
    version: i16,
) -> Result<Arc<GroupActorHandle>, i16> {
    let coordinator = &broker.group_coordinator;
    let generation = req.generation_id_or_member_epoch;
    let code = if let Some(streams) = coordinator.find_streams(&req.group_id) {
        validate_streams_group_offset_commit(
            &streams,
            &req.member_id,
            generation,
            version,
            named_partitions(req),
        )
        .await
    } else if let Some(handle) = coordinator.find(&req.group_id) {
        let code = validate_commit(
            &handle,
            CommitRequest {
                member_id: req.member_id.clone(),
                group_instance_id: req.group_instance_id.clone(),
                generation_or_epoch: generation,
                fence: CommitFence::Offset {
                    api_version: version,
                },
                partitions: committed_partitions(req, image),
            },
        )
        .await;
        return code.map_or(Ok(handle), Err);
    } else if generation < 0 {
        None
    } else if version >= FIRST_GROUP_ID_NOT_FOUND_VERSION {
        Some(codes::GROUP_ID_NOT_FOUND)
    } else {
        Some(codes::ILLEGAL_GENERATION)
    };
    match code {
        Some(code) => Err(code),
        None => Ok(coordinator.get_or_create_group(&req.group_id, GroupKindTag::Classic)),
    }
}

/// The `(topic id, partition)` of every partition `req` commits, with the
/// topic id the image holds for the topic name. The handler has already
/// dropped every topic the image does not hold, and Kafka's
/// `KafkaApis.handleOffsetCommitRequest` resolves the same id before the
/// group coordinator validates each partition.
fn committed_partitions(
    req: &OffsetCommitRequest,
    image: &krabka_metadata::MetadataImage,
) -> Vec<(WireUuid, i32)> {
    req.topics
        .iter()
        .flat_map(|topic| {
            let topic_id = image
                .topic(&topic.name)
                .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));
            topic
                .partitions
                .iter()
                .map(move |partition| (topic_id, partition.partition_index))
        })
        .collect()
}

/// The `(topic name, partition)` of every partition `req` commits, which a
/// streams group checks against its topology's source topics.
fn named_partitions(req: &OffsetCommitRequest) -> Vec<(String, i32)> {
    req.topics
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(|partition| (topic.name.clone(), partition.partition_index))
        })
        .collect()
}

/// One commit's two halves: the `__consumer_offsets` records for every
/// `(topic, partition)` in the request, and the in-memory entries that mirror
/// them.
///
/// They are built together and travel to the group's actor together, because
/// the actor is what orders them against the KIP-211 retention sweep.
struct CommitRecords {
    batch: RecordBatch,
    entries: Vec<((String, i32), OffsetEntry)>,
}

/// Build both halves of one commit.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the group id or a topic name is longer
/// than 32767 bytes, which a non-flexible record key cannot carry.
fn commit_records(
    req: &OffsetCommitRequest,
    commit: Commit<'_>,
) -> Result<CommitRecords, BrokerError> {
    let mut batch = RecordBatch {
        base_timestamp: commit.now_ms,
        max_timestamp: commit.now_ms,
        ..RecordBatch::default()
    };
    let mut entries = Vec::new();
    let mut delta: i32 = 0;
    for topic in &req.topics {
        for part in &topic.partitions {
            // Kafka's `OffsetAndMetadata.fromRequest` keeps the topic id the
            // request resolved to, and the record carries it (#987).
            let entry = OffsetEntry {
                offset: krabka_log::Offset(part.committed_offset),
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: commit.now_ms,
                expire_timestamp_ms: commit.expire_timestamp_ms,
                topic_id: commit.image.topic(&topic.name).map(|t| t.topic_id),
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )?),
                value: Some(OffsetCommitValue::from(&entry).encode_value()),
                ..Default::default()
            });
            entries.push(((topic.name.clone(), part.partition_index), entry));
            delta += 1;
        }
    }
    batch.last_offset_delta = (delta - 1).max(0);
    Ok(CommitRecords { batch, entries })
}

/// Append the commit and apply it to the group, both inside the group's actor.
///
/// The append cannot run outside the mailbox. `coordinator::retention` decides
/// what to tombstone from the actor's in-memory offsets and writes the
/// tombstones in the same turn, so a commit that appended its record first and
/// queued the in-memory update afterwards could be acknowledged and then
/// deleted by a sweep that never saw it — the sweep would read the stale map,
/// tombstone the offset behind the newer record, and stop the actor with the
/// queued update still in flight. Sending both halves as one message puts the
/// commit and the sweep in one order.
///
/// Returns `Err(error_code)` when the append failed or the actor is gone.
async fn commit_through_actor(
    handle: &Arc<GroupActorHandle>,
    req: &OffsetCommitRequest,
    commit: Commit<'_>,
) -> Result<(), i16> {
    let CommitRecords { batch, entries } = commit_records(req, commit).map_err(|error| {
        tracing::warn!(group_id = %req.group_id, %error, "offset commit records are not encodable");
        codes::UNKNOWN_SERVER_ERROR
    })?;
    ask(&handle.tx, |reply| GroupActorMessage::CommitOffsets {
        batch,
        entries,
        reply,
    })
    .await
    .unwrap_or(Err(codes::UNKNOWN_SERVER_ERROR))
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
            topic_id: t.topic_id,
            partitions: t
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    OffsetCommitResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::MetadataImage;
    use krabka_protocol::owned::offset_commit_request::OffsetCommitRequestPartition;

    use super::*;

    #[test]
    fn offset_commits_roll_only_after_the_segment_age_limit() {
        let mut fixture = crate::test_support::segment_age_fixture();
        let image = MetadataImage::new(uuid::Uuid::nil());
        let request = OffsetCommitRequest {
            group_id: "group".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: "topic".into(),
                partitions: vec![OffsetCommitRequestPartition {
                    committed_offset: 42,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        for elapsed_ms in [0, 1, 2, 3, fixture.roll_ms, fixture.roll_ms + 1] {
            let commit = Commit {
                now_ms: fixture.first_ms + elapsed_ms,
                expire_timestamp_ms: None,
                image: &image,
            };
            fixture
                .log
                .append(&mut commit_records(&request, commit).unwrap().batch)
                .unwrap();
            fixture.log.sync().unwrap();
            check!(
                fixture.log.tierable_segments().len() == usize::from(elapsed_ms > fixture.roll_ms)
            );
        }
    }

    /// KIP-211: `-1` is the "use the broker's own retention" sentinel, and it
    /// is also what the decoder leaves behind for a version that does not
    /// carry the field at all. Every other value becomes an absolute deadline.
    #[test]
    fn per_commit_retention_becomes_an_absolute_deadline_unless_it_is_the_sentinel() {
        let cases = [
            (DEFAULT_RETENTION_TIME_MS, 1_000, None),
            (0, 1_000, Some(1_000)),
            (86_400_000, 1_000, Some(86_401_000)),
            // A nonsense value still becomes a deadline rather than being read
            // as the sentinel, which is what Kafka's `== DEFAULT_RETENTION_TIME`
            // check does.
            (-5, 1_000, Some(995)),
            // Arithmetic that would overflow saturates rather than panicking.
            (i64::MAX, 1_000, Some(i64::MAX)),
        ];
        for (retention_time_ms, now_ms, want) in cases {
            check!(
                expire_timestamp_ms(retention_time_ms, now_ms) == want,
                "retention_time_ms={retention_time_ms} now_ms={now_ms}"
            );
        }
    }

    /// The key of a committed offset writes the group id and the topic name
    /// with an `INT16` length. A string of 32767 bytes encodes, and one of
    /// 32768 bytes is an error that the commit answers `UNKNOWN_SERVER_ERROR`,
    /// not a panic.
    #[test]
    fn a_commit_key_string_over_32767_bytes_does_not_encode() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
        let rows = [
            ("group id", limit, 1, true),
            ("group id", limit + 1, 1, false),
            ("topic name", 1, limit, true),
            ("topic name", 1, limit + 1, false),
        ];
        for (name, group_length, topic_length, encodes) in rows {
            let request = OffsetCommitRequest {
                group_id: "g".repeat(group_length),
                topics: vec![OffsetCommitRequestTopic {
                    name: "t".repeat(topic_length),
                    partitions: vec![OffsetCommitRequestPartition::default()],
                    ..Default::default()
                }],
                ..Default::default()
            };
            let commit = Commit {
                now_ms: 1,
                expire_timestamp_ms: None,
                image: &image,
            };

            let outcome = commit_records(&request, commit);

            check!(
                outcome.is_ok() == encodes,
                "{name} of {} bytes",
                group_length.max(topic_length)
            );
            check!(
                encodes || matches!(outcome, Err(BrokerError::Protocol(_))),
                "{name} is a protocol error"
            );
        }
    }

    /// `OffsetMetadataManager.isMetadataInvalid`: Java's `String.length()`
    /// against `offset.metadata.max.bytes`. A character outside the Basic
    /// Multilingual Plane is two UTF-16 code units.
    #[test]
    fn oversized_metadata_answers_on_its_own_row_and_is_not_committed() {
        const LIMIT: i32 = 4096;
        let supplementary = format!("{}\u{1F980}", "a".repeat(4095));
        let cases: [(Option<String>, i16); 6] = [
            (None, codes::NONE),
            (Some(String::new()), codes::NONE),
            (Some("a".repeat(4096)), codes::NONE),
            (Some("a".repeat(4097)), codes::OFFSET_METADATA_TOO_LARGE),
            // 4095 + 2 code units, though 4096 characters.
            (Some(supplementary), codes::OFFSET_METADATA_TOO_LARGE),
            // 4096 characters of two UTF-8 bytes each are 4096 code units.
            (Some("\u{e9}".repeat(4096)), codes::NONE),
        ];
        let partition = |index: usize, metadata: &Option<String>| OffsetCommitRequestPartition {
            partition_index: i32::try_from(index).unwrap(),
            committed_offset: 42,
            committed_metadata: metadata.clone(),
            ..Default::default()
        };
        let request = OffsetCommitRequest {
            group_id: "g".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: "t".into(),
                partitions: cases
                    .iter()
                    .enumerate()
                    .map(|(index, (metadata, _))| partition(index, metadata))
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let expected_valid = OffsetCommitRequest {
            topics: vec![OffsetCommitRequestTopic {
                name: "t".into(),
                partitions: cases
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, code))| *code == codes::NONE)
                    .map(|(index, (metadata, _))| partition(index, metadata))
                    .collect(),
                ..Default::default()
            }],
            ..request.clone()
        };
        let expected_rows = vec![OffsetCommitResponseTopic {
            name: "t".into(),
            partitions: cases
                .iter()
                .enumerate()
                .map(|(index, (_, error_code))| OffsetCommitResponsePartition {
                    partition_index: i32::try_from(index).unwrap(),
                    error_code: *error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }];
        check!(split_oversized_metadata(&request, LIMIT) == (expected_valid, expected_rows));
    }
}
