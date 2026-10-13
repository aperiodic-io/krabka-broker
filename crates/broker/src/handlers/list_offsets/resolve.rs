//! The per-partition offset resolution: one row of a `ListOffsets` request
//! turned into one row of the response.
//!
//! The version gate runs first, then the partition lookup, then KIP-320's
//! leader-epoch fence, and then the match that sends each sentinel to the log,
//! the remote tier, or the diskless index that answers it. A partition that
//! fails any of those steps carries its own error code, because Kafka reports
//! per-partition failures in the row rather than at the top level.
//!
//! The fence sits exactly where Kafka's does. `Partition.fetchOffsetForTimestamp`
//! resolves nothing until `localLogWithEpochOrThrow` has compared the request's
//! `current_leader_epoch` against the live one, so a consumer holding stale
//! metadata is told to refresh rather than handed an offset resolved against a
//! leader it no longer believes in -- and then refused by the very next Fetch,
//! which applies the same comparison. The two APIs share the comparison but
//! not the sentinel that skips it: only `-1` means "no epoch asserted" here,
//! whereas Fetch reads every negative epoch that way. See
//! `Partition::list_offsets_leader_epoch_fence`.

use {
    super::{
        bound::{FetchBound, fetchable_offsets},
        diskless::diskless_earliest_candidate,
        leadership::resolve_for_broker,
        local::{leader_epoch_for_offset, scheduled_latest_offset},
        remote::await_remote,
        response::error_response,
        sentinels::{UNKNOWN_EPOCH, UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP, timestamp_kind},
        timestamp::resolve_timestamp_offset,
    },
    crate::{broker::Broker, codes, partition::Partition},
    krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse,
    krabka_verified::{
        ListOffsetsEarliestFacts, ListOffsetsKind, ListOffsetsSelectionDecision,
        ListOffsetsSelectionFacts, list_offsets_earliest, list_offsets_selection_decision,
    },
    std::{sync::atomic::Ordering, time::Duration},
};

fn earliest_pending_upload_offset(tiered_offset: i64) -> Option<i64> {
    tiered_offset.checked_add(1)
}

/// The values [`resolve_earliest`] needs, gathered so `resolve_partition`
/// passes them as one handle rather than one parameter each.
struct EarliestContext<'a> {
    broker: &'a Broker,
    topic_name: &'a str,
    index: i32,
    partition: &'a Partition,
    remote_timeout: Duration,
    remote_topic_id: Option<uuid::Uuid>,
    topic_id: Option<uuid::Uuid>,
    local_start: i64,
    deleted_below: Option<krabka_log::Offset>,
    local_log_start: i64,
}

/// An offset with the leader epoch that goes with it.
struct ResolvedOffset {
    offset: i64,
    leader_epoch: i32,
}

/// Resolve the EARLIEST sentinel's offset across the local log, the remote
/// tier and the diskless index, and the leader epoch that goes with it.
///
/// Split out of [`resolve_partition`] purely to keep that function's line
/// count down.
async fn resolve_earliest(ctx: EarliestContext<'_>) -> Result<ResolvedOffset, i16> {
    let mut remote_candidate = None;
    if let (Some(reader), Some(id)) = (ctx.broker.remote_reader.as_ref(), ctx.remote_topic_id) {
        let topic_partition =
            krabka_remote_storage::TopicIdPartition::new(id, ctx.topic_name.to_string(), ctx.index);
        match await_remote(ctx.remote_timeout, reader.earliest_offset(&topic_partition)).await {
            None => return Err(codes::REQUEST_TIMED_OUT),
            // KIP-405: the global log start bounds the archive too. A
            // `DeleteRecords` moves the floor at once and the expiration
            // pass removes the breached segments on its own tick, so in
            // between the RLMM still lists a segment that starts below the
            // floor. Reporting its start as EARLIEST would name an offset
            // the fetch path refuses.
            //
            // Only a floor someone deleted up to bounds it. The one
            // `Log::open` infers from the segments left on disk sits above
            // the whole archive on a partition whose local segments were
            // evicted, and clamping to that would report an EARLIEST past
            // every record the tier still holds -- a `--from-beginning`
            // consumer would skip them all.
            Some(Ok(Some(remote_start))) => {
                remote_candidate = Some(match ctx.deleted_below {
                    Some(floor) => remote_start.max(floor.0),
                    None => remote_start,
                });
            }
            Some(Ok(None)) => {}
            Some(Err(error)) => tracing::warn!(topic = ctx.topic_name, partition = ctx.index,
                error = %error, "list_offsets: remote earliest_offset failed"),
        }
    }
    let diskless_candidate =
        diskless_earliest_candidate(ctx.broker.diskless_read.as_deref(), ctx.topic_id, ctx.index)
            .await;
    let facts = ListOffsetsEarliestFacts {
        local: ctx.local_start,
        has_remote: remote_candidate.is_some(),
        remote: remote_candidate.unwrap_or(0),
        has_diskless: diskless_candidate.is_some(),
        diskless: diskless_candidate.unwrap_or(0),
    };
    let Some(earliest) = list_offsets_earliest(facts) else {
        return Err(codes::KAFKA_STORAGE_ERROR);
    };
    // Kafka fills the epoch for EARLIEST when its start offset sits at or
    // below the log start, which is always true of the offset just
    // selected: it is by definition the earliest one this broker can name.
    // `UnifiedLog.java:1690-1700`.
    Ok(ResolvedOffset {
        offset: earliest,
        leader_epoch: leader_epoch_for_offset(ctx.partition, earliest),
    })
}

/// Resolve the KIP-1023 `EARLIEST_PENDING_UPLOAD` sentinel: the first offset
/// this leader has not yet copied to the remote tier, or `None` when there
/// is no remote reader configured for this topic.
///
/// Split out of [`resolve_partition`] purely to keep that function's line
/// count down.
async fn resolve_earliest_pending_upload(
    ctx: EarliestContext<'_>,
) -> Result<Option<ResolvedOffset>, i16> {
    let Some((reader, id)) = ctx.broker.remote_reader.as_ref().zip(ctx.remote_topic_id) else {
        return Ok(None);
    };
    let topic_partition =
        krabka_remote_storage::TopicIdPartition::new(id, ctx.topic_name.to_string(), ctx.index);
    match await_remote(
        ctx.remote_timeout,
        reader.latest_tiered_offset(&topic_partition),
    )
    .await
    {
        None => Err(codes::REQUEST_TIMED_OUT),
        Some(Ok(Some(tiered))) => {
            // `UnifiedLog.fetchEarliestPendingUploadOffset` clamps the raw
            // remote frontier with the log start offset:
            // `Math.max(curHighestRemoteOffset + 1, logStartOffset())`.
            // Without the clamp, a `DeleteRecords` call that moves the log
            // start past the last finished upload would report an offset
            // the fetch path refuses, the same failure mode EARLIEST guards
            // against above.
            let Some(raw_offset) = earliest_pending_upload_offset(tiered.offset) else {
                return Err(codes::KAFKA_STORAGE_ERROR);
            };
            let offset = raw_offset.max(ctx.local_start);
            let local_epoch = leader_epoch_for_offset(ctx.partition, offset);
            let leader_epoch = if local_epoch < 0 {
                tiered.leader_epoch.0
            } else {
                local_epoch
            };
            Ok(Some(ResolvedOffset {
                offset,
                leader_epoch,
            }))
        }
        // The RLMM lists no segment. When the local log still holds
        // everything from the log start, nothing has ever been tiered and
        // Kafka falls back to the EARLIEST answer. When the local log start
        // has moved ahead of it, segments were evicted on the strength of a
        // tier upload this leader cannot currently confirm, and Kafka
        // reports -1 rather than naming an offset it cannot stand behind.
        Some(Ok(None)) if ctx.local_log_start == ctx.local_start => Ok(Some(ResolvedOffset {
            offset: ctx.local_start,
            leader_epoch: leader_epoch_for_offset(ctx.partition, ctx.local_start),
        })),
        Some(Ok(None)) => Ok(None),
        Some(Err(error)) => {
            tracing::warn!(topic = ctx.topic_name, partition = ctx.index,
                error = %error, "list_offsets: remote earliest_pending_upload failed");
            Ok(None)
        }
    }
}

/// The local snapshot and archive lookup needed to resolve scheduled LATEST.
pub(super) struct LatestContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) partition: &'a Partition,
    pub(super) topic: TopicPartitionName<'a>,
    pub(super) remote_timeout: Duration,
    pub(super) remote_topic_id: Option<uuid::Uuid>,
    /// Config, log end, local floor and established deletion floor, all read
    /// under the same log mutex hold.
    pub(super) snapshot: LatestSnapshot<'a>,
}

/// A topic-partition by topic name.
#[derive(Clone, Copy)]
pub(super) struct TopicPartitionName<'a> {
    pub(super) name: &'a str,
    pub(super) partition: i32,
}

/// The log facts that LATEST resolution reads under one log mutex hold.
pub(super) struct LatestSnapshot<'a> {
    pub(super) config: &'a krabka_log::LogConfig,
    pub(super) local_end: i64,
    pub(super) local_log_start: i64,
    pub(super) deleted_below: Option<krabka_log::Offset>,
}

pub(super) async fn resolve_latest(ctx: LatestContext<'_>) -> Result<i64, i16> {
    let LatestSnapshot {
        config: log_config,
        local_end,
        local_log_start,
        deleted_below,
    } = ctx.snapshot;
    let scheduled = scheduled_latest_offset(ctx.partition, log_config.delivery_policy);
    let latest = scheduled.unwrap_or(local_end);
    let Some((reader, id)) = ctx.broker.remote_reader.as_ref().zip(ctx.remote_topic_id) else {
        return Ok(latest);
    };
    if scheduled.is_none() || local_log_start <= deleted_below.map_or(0, |floor| floor.0) {
        return Ok(latest);
    }
    // Older brokers, or an immediate topic switched to scheduled, may already
    // have evicted a pending batch. LATEST must stop at that archived batch.
    let TopicPartitionName {
        name: topic_name,
        partition: index,
    } = ctx.topic;
    let tp = krabka_remote_storage::TopicIdPartition::new(id, topic_name, index);
    match await_remote(
        ctx.remote_timeout,
        reader.delivery_watermark(
            &tp,
            deleted_below.unwrap_or(krabka_log::Offset(0)),
            krabka_log::Offset(local_log_start),
            log_config.delivery_clock_uncertainty,
            ctx.partition.delivery.now_ms(),
        ),
    )
    .await
    {
        None => Err(codes::REQUEST_TIMED_OUT),
        Some(Ok(frontier)) => Ok(frontier.map_or(latest, |pending| latest.min(pending.0))),
        Some(Err(error)) => {
            tracing::warn!(topic = topic_name, partition = index, %error,
                "list_offsets: archived delivery frontier unavailable");
            Err(codes::KAFKA_STORAGE_ERROR)
        }
    }
}

/// KIP-207's `maybeOffsetsError` from `Partition.fetchOffsetForTimestamp`,
/// as the code `ReplicaManager.fetchOffset` answers it with, or `None` when
/// there is no error to raise.
///
/// Right after a leader election the new leader's high watermark can still
/// sit below the start offset of its own epoch, and an end offset read in
/// that window could later move backwards. Kafka raises the error only for a
/// client request (`replica_id == -1`, the one shape that passes an isolation
/// level down) and only while the epoch start is above the high watermark.
/// The comparison reads the high watermark whatever the isolation level: a
/// `read_committed` client whose last stable offset an open transaction holds
/// below the epoch start is not fenced by it.
///
/// `ReplicaManager.scala:1543-1551` answers `OFFSET_NOT_AVAILABLE` from v5 and
/// `LEADER_NOT_AVAILABLE` for v1-v4, where the dedicated code did not exist.
fn lagging_high_watermark_error(
    replica_id: i32,
    epoch_start: Option<i64>,
    high_watermark: i64,
    version: i16,
) -> Option<i16> {
    let epoch_start = epoch_start?;
    if replica_id != -1 || epoch_start <= high_watermark {
        return None;
    }
    Some(if version >= 5 {
        codes::OFFSET_NOT_AVAILABLE
    } else {
        codes::LEADER_NOT_AVAILABLE
    })
}

/// Whether a raised [`lagging_high_watermark_error`] fences the lookup that
/// resolved `candidate_offset`, as `Partition.fetchOffsetForTimestamp` and
/// `ReplicaManager.fetchOffset` apply it.
///
/// LATEST is fenced outright: it is the live end of the log the window puts
/// at risk. EARLIEST and `EARLIEST_LOCAL` never are, because they resolve from
/// the start of the log. Every other kind resolves against record data, and
/// Kafka fences it when the lookup found nothing or when what it found sits
/// at or above `last_fetchable`, the bound the request's isolation level
/// selects. A lookup that resolves to an older, already fetchable offset is
/// answered even while the window is open.
///
/// A by-timestamp or `MAX_TIMESTAMP` lookup that found nothing arrives here as
/// `UNKNOWN_OFFSET`, Kafka's empty `OffsetResultHolder`. The two tiered
/// sentinels answer `-1` as a found offset instead, and `-1` is below every
/// bound, so it is never fenced.
fn lagging_high_watermark_fences(
    kind: ListOffsetsKind,
    candidate_offset: i64,
    last_fetchable: i64,
) -> bool {
    match kind {
        ListOffsetsKind::Latest => true,
        ListOffsetsKind::Earliest
        | ListOffsetsKind::EarliestLocal
        | ListOffsetsKind::Unsupported => false,
        ListOffsetsKind::Timestamp | ListOffsetsKind::MaxTimestamp
            if candidate_offset == UNKNOWN_OFFSET =>
        {
            true
        }
        ListOffsetsKind::Timestamp
        | ListOffsetsKind::MaxTimestamp
        | ListOffsetsKind::LatestTiered
        | ListOffsetsKind::EarliestPendingUpload => candidate_offset >= last_fetchable,
    }
}

/// The bound a request's answer is measured against and KIP-207's
/// `maybeOffsetsError`, as `Partition.fetchOffsetForTimestamp` computes them
/// before it resolves anything. `Err` carries the code of a partition that
/// is refused before resolution: a malformed watermark, or LATEST fenced
/// while the high watermark lags.
///
/// Kafka reads `lastFetchableOffset` for every request but EARLIEST and
/// `EARLIEST_LOCAL`, which it answers from the start of the log without
/// measuring them. Skipping the read for those two also skips the high
/// watermark's async mutex and the epoch-start lookup, which the fence never
/// needs for them.
/// What [`measure`] read: the last fetchable offset of the bound and the
/// error a lagging high watermark would raise.
struct Measured {
    last_fetchable: Option<i64>,
    offsets_error: Option<i16>,
}

async fn measure(
    partition: &Partition,
    kind: ListOffsetsKind,
    bound: FetchBound,
    version: i16,
) -> Result<Measured, i16> {
    if matches!(
        kind,
        ListOffsetsKind::Earliest | ListOffsetsKind::EarliestLocal
    ) {
        return Ok(Measured {
            last_fetchable: None,
            offsets_error: None,
        });
    }
    let Some(offsets) = fetchable_offsets(partition, bound).await else {
        return Err(codes::KAFKA_STORAGE_ERROR);
    };
    let offsets_error = lagging_high_watermark_error(
        bound.replica_id(),
        super::local::epoch_start_offset(partition),
        offsets.high_watermark,
        version,
    );
    match offsets_error {
        Some(error_code) if kind == ListOffsetsKind::Latest => Err(error_code),
        _ => Ok(Measured {
            last_fetchable: Some(offsets.last_fetchable),
            offsets_error,
        }),
    }
}

fn apply_selection(
    response: &mut ListOffsetsPartitionResponse,
    kind: ListOffsetsKind,
    offset: i64,
    timestamp: i64,
    last_fetchable: Option<i64>,
) -> bool {
    match list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind,
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch: response.leader_epoch,
        last_fetchable: last_fetchable.unwrap_or(0),
    }) {
        ListOffsetsSelectionDecision::RejectMalformed => false,
        ListOffsetsSelectionDecision::Unknown => {
            response.leader_epoch = UNKNOWN_EPOCH;
            response.offset = UNKNOWN_OFFSET;
            response.timestamp = UNKNOWN_TIMESTAMP;
            true
        }
        ListOffsetsSelectionDecision::Resolved {
            offset,
            timestamp,
            leader_epoch,
        } => {
            response.leader_epoch = leader_epoch;
            response.offset = offset;
            response.timestamp = timestamp;
            true
        }
    }
}

/// Log coordinates and policy read together before any remote lookup awaits.
struct PartitionLogSnapshot {
    local_start: i64,
    deleted_below: Option<krabka_log::Offset>,
    local_end: i64,
    local_log_start: i64,
    log_config: krabka_log::LogConfig,
}

fn partition_log_snapshot(partition: &Partition) -> PartitionLogSnapshot {
    let log = partition.log.lock().expect("log mutex poisoned");
    PartitionLogSnapshot {
        local_start: log.log_start_offset().0,
        deleted_below: log.established_log_start(),
        local_end: log.log_end_offset().0,
        local_log_start: log.local_log_start_offset().0,
        log_config: log.config_snapshot(),
    }
}

pub(super) async fn resolve_partition(
    broker: &Broker,
    topic_name: &str,
    request: krabka_protocol::owned::list_offsets_request::ListOffsetsPartition,
    version: i16,
    remote_timeout: Duration,
    bound: FetchBound,
) -> ListOffsetsPartitionResponse {
    let index = request.partition_index;
    let mut response = ListOffsetsPartitionResponse {
        partition_index: index,
        timestamp: UNKNOWN_TIMESTAMP,
        ..Default::default()
    };
    let kind = timestamp_kind(request.timestamp, version);
    if kind == ListOffsetsKind::Unsupported {
        response.error_code = codes::UNSUPPORTED_VERSION;
        response.offset = UNKNOWN_OFFSET;
        return response;
    }
    // Kafka answers from whatever this node holds only when it leads the
    // partition, or the request carries the offline-debugging sentinel
    // `replica_id == -2` and this node holds it as a follower. KIP-320's
    // epoch fence -- the `current_leader_epoch` field decodes from v4 up and
    // holds the `-1` sentinel below it, so a v1-v3 request never trips it --
    // runs ahead of that leadership check, matching Kafka's order. Anyone
    // refused leadership gets `NOT_LEADER_OR_FOLLOWER` when the metadata
    // image and the installed local role do not both name this node leader,
    // `UNKNOWN_TOPIC_OR_PARTITION` when the image does not know the
    // partition at all, and `KAFKA_STORAGE_ERROR` when the partition's log
    // directory is offline. See
    // [`resolve_leadership`](super::leadership::resolve_leadership).
    let partition = match resolve_for_broker(
        broker,
        topic_name,
        index,
        bound.replica_id(),
        request.current_leader_epoch,
    ) {
        Ok(partition) => partition,
        Err(error_code) => return error_response(index, error_code),
    };
    let PartitionLogSnapshot {
        local_start,
        deleted_below,
        local_end,
        local_log_start,
        log_config,
    } = partition_log_snapshot(&partition);
    let remote_enabled = log_config.remote_storage_enable;
    let diskless = partition.diskless && broker.diskless_read.is_some();
    let topic_id = if (remote_enabled && broker.remote_reader.is_some()) || diskless {
        broker
            .controller
            .current_image()
            .topic(topic_name)
            .map(|topic| topic.topic_id)
    } else {
        None
    };
    let remote_topic_id = if remote_enabled { topic_id } else { None };
    // Read here, ahead of the match, because several arms below hold the log
    // mutex and the high watermark must not be awaited under it.
    let Measured {
        last_fetchable,
        offsets_error,
    } = match measure(&partition, kind, bound, version).await {
        Ok(measured) => measured,
        Err(error_code) => return error_response(index, error_code),
    };
    let earliest_context = || EarliestContext {
        broker,
        topic_name,
        index,
        partition: &partition,
        remote_timeout,
        remote_topic_id,
        topic_id,
        local_start,
        deleted_below,
        local_log_start,
    };
    let (offset, timestamp) = match kind {
        ListOffsetsKind::Earliest => match resolve_earliest(earliest_context()).await {
            Ok(ResolvedOffset {
                offset: earliest,
                leader_epoch,
            }) => {
                response.leader_epoch = leader_epoch;
                (earliest, UNKNOWN_TIMESTAMP)
            }
            Err(error_code) => return error_response(index, error_code),
        },
        ListOffsetsKind::Latest => {
            // Kafka answers LATEST with the partition's live leader epoch,
            // not the epoch recorded for whatever offset the log end
            // happens to sit at -- the two agree once a record lands in the
            // new epoch, but LATEST must not wait for that.
            // `Partition.scala:1473-1475`.
            response.leader_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
            match resolve_latest(LatestContext {
                broker,
                partition: &partition,
                topic: TopicPartitionName {
                    name: topic_name,
                    partition: index,
                },
                remote_timeout,
                remote_topic_id,
                snapshot: LatestSnapshot {
                    config: &log_config,
                    local_end,
                    local_log_start,
                    deleted_below,
                },
            })
            .await
            {
                Ok(latest) => (latest, UNKNOWN_TIMESTAMP),
                Err(error_code) => return error_response(index, error_code),
            }
        }
        ListOffsetsKind::EarliestLocal => {
            let offset = if remote_enabled {
                local_log_start
            } else {
                local_start
            };
            response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            (offset, UNKNOWN_TIMESTAMP)
        }
        ListOffsetsKind::LatestTiered => {
            if let Some((reader, id)) = broker.remote_reader.as_ref().zip(remote_topic_id) {
                let topic_partition =
                    krabka_remote_storage::TopicIdPartition::new(id, topic_name.to_string(), index);
                match await_remote(
                    remote_timeout,
                    reader.latest_tiered_offset(&topic_partition),
                )
                .await
                {
                    None => return error_response(index, codes::REQUEST_TIMED_OUT),
                    Some(Ok(Some(tiered))) => {
                        response.leader_epoch = tiered.leader_epoch.0;
                        (tiered.offset, UNKNOWN_TIMESTAMP)
                    }
                    Some(Ok(None)) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    Some(Err(error)) => {
                        tracing::warn!(topic = topic_name, partition = index,
                            error = %error, "list_offsets: remote latest_tiered_offset failed");
                        (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
                    }
                }
            } else {
                (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP)
            }
        }
        ListOffsetsKind::EarliestPendingUpload => {
            match resolve_earliest_pending_upload(earliest_context()).await {
                Ok(None) => (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                Ok(Some(ResolvedOffset {
                    offset,
                    leader_epoch,
                })) => {
                    response.leader_epoch = leader_epoch;
                    (offset, UNKNOWN_TIMESTAMP)
                }
                Err(error_code) => return error_response(index, error_code),
            }
        }
        ListOffsetsKind::MaxTimestamp => {
            // An empty log has no record to name, and Kafka's
            // `UnifiedLog.fetchOffsetByTimestamp` answers it with an empty
            // result rather than the log start: KIP-207 fences that empty
            // result, and an unfenced one is the unknown row.
            let found = partition
                .log
                .lock()
                .expect("log mutex poisoned")
                .max_timestamp_offset_and_ts();
            // A compressed record above the topic's trunk
            // `max.decompressed.message.bytes` in the batch Kafka reads to
            // resolve the maximum fails the row, as an exception out of
            // `UnifiedLog.fetchOffsetByTimestamp` does.
            let (offset, timestamp) = match found {
                Ok(found) => found.map_or(
                    (UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP),
                    |(offset, timestamp)| (offset.0, timestamp),
                ),
                Err(error) => {
                    tracing::warn!(topic = topic_name, partition = index, error = %error,
                        "list_offsets: max timestamp lookup refused a record");
                    return error_response(index, codes::from_broker_error(&error.into()));
                }
            };
            // Kafka fills the epoch with the resolved batch's own
            // `partitionLeaderEpoch`. `UnifiedLog.java:1742-1749`.
            if offset != UNKNOWN_OFFSET {
                response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            }
            (offset, timestamp)
        }
        ListOffsetsKind::Timestamp => {
            let (offset, timestamp) = match resolve_timestamp_offset(
                broker,
                &partition,
                topic_name,
                index,
                topic_id,
                request.timestamp,
                remote_timeout,
            )
            .await
            {
                Ok(found) => found,
                Err(error_code) => return error_response(index, error_code),
            };
            // Kafka fills the epoch with the matched batch's own epoch.
            // `FileRecords.java:364-381`.
            if offset != UNKNOWN_OFFSET {
                response.leader_epoch = leader_epoch_for_offset(&partition, offset);
            }
            (offset, timestamp)
        }
        ListOffsetsKind::Unsupported => unreachable!("unsupported timestamp returned above"),
    };
    // KIP-207 for a data-resolved lookup: fenced only when it found nothing
    // or found an offset the request's bound does not yet cover.
    if let (Some(error_code), Some(last_fetchable)) = (offsets_error, last_fetchable)
        && lagging_high_watermark_fences(kind, offset, last_fetchable)
    {
        return error_response(index, error_code);
    }
    // One bound, applied the two ways `Partition.fetchOffsetForTimestamp`
    // applies it. EARLIEST and `EARLIEST_LOCAL` are absent from both arms
    // because they resolve from the start of the log, which is never above the
    // bound, and Kafka returns them unmeasured.
    if !apply_selection(&mut response, kind, offset, timestamp, last_fetchable) {
        return error_response(index, codes::KAFKA_STORAGE_ERROR);
    }
    response.error_code = codes::NONE;
    response
}

#[cfg(test)]
mod tests {
    use crate::{
        broker::BrokerHandle,
        handlers::list_offsets::test_support::{ClientBroker, remote_topic, test_context},
        partition::Partition,
    };
    mod timestamp_visibility;

    use {
        super::*,
        crate::{
            handlers::list_offsets::{
                handle,
                sentinels::{
                    EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP,
                    EARLIEST_TIMESTAMP, LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP,
                },
                test_support::{
                    client_for, create_topic, decode_response, encode_request, list_one,
                    list_one_at_epoch,
                },
            },
            test_support::test_ctx,
        },
        assert2::assert,
        krabka_protocol::owned::create_topics_request::CreatableTopicConfig,
    };

    #[test]
    fn pending_upload_offset_rejects_overflow() {
        assert!(earliest_pending_upload_offset(4) == Some(5));
        assert!(earliest_pending_upload_offset(i64::MAX).is_none());
    }

    #[tokio::test]
    async fn request_leader_epoch_is_fenced_the_way_the_fetch_path_fences_it() {
        const TOPIC: &str = "list-offsets-epoch";
        const CURRENT_EPOCH: i32 = 3;
        const RECORDS: usize = 4;

        list_offsets_topic_fixture!((broker, _dir, client), TOPIC);
        broker
            .produce_records_for_test(TOPIC, 0, RECORDS)
            .await
            .expect("produce");
        broker.test_set_leader_epoch(TOPIC, 0, CURRENT_EPOCH);

        let resolved = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp: UNKNOWN_TIMESTAMP,
            offset: i64::try_from(RECORDS).expect("record count fits an offset"),
            // LATEST reports the partition's live leader epoch, which the
            // `test_set_leader_epoch` call above bumped to `CURRENT_EPOCH`.
            leader_epoch: CURRENT_EPOCH,
            ..Default::default()
        };
        let fenced = |error_code| ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code,
            timestamp: UNKNOWN_TIMESTAMP,
            offset: UNKNOWN_OFFSET,
            leader_epoch: UNKNOWN_EPOCH,
            ..Default::default()
        };
        let cases = [
            (
                "below the current epoch",
                CURRENT_EPOCH - 1,
                fenced(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "equal to the current epoch",
                CURRENT_EPOCH,
                resolved.clone(),
            ),
            (
                "above the current epoch",
                CURRENT_EPOCH + 1,
                fenced(codes::UNKNOWN_LEADER_EPOCH),
            ),
            ("the unknown-epoch sentinel", UNKNOWN_EPOCH, resolved),
            // Kafka's `RequestUtils.getLeaderEpoch` reads only `-1` as "no
            // epoch asserted", so a `ListOffsets` row carrying any other
            // negative epoch is compared and fenced. Fetch does not: its
            // `FetchRequest.optionalEpoch` reads every negative epoch that
            // way. Confirmed on apache/kafka:4.3.1.
            (
                "a negative epoch that is not the sentinel",
                UNKNOWN_EPOCH - 1,
                fenced(codes::FENCED_LEADER_EPOCH),
            ),
        ];
        for (name, request_epoch, expected) in cases {
            assert!(
                list_one_at_epoch(&client, TOPIC, LATEST_TIMESTAMP, request_epoch).await
                    == expected,
                "{name}"
            );
        }

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn non_tiered_sentinels_use_ordinary_earliest_and_unknown_remote_offsets() {
        const TOPIC: &str = "list-offsets-local";

        list_offsets_topic_fixture!((broker, _dir, client), TOPIC);
        broker
            .produce_records_for_test(TOPIC, 0, 8)
            .await
            .expect("produce");
        broker
            .test_advance_log_start(TOPIC, 0, 5)
            .await
            .expect("advance log start");

        assert!(
            list_one(&client, TOPIC, EARLIEST_LOCAL_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        for timestamp in [LATEST_TIERED_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP] {
            assert!(
                list_one(&client, TOPIC, timestamp).await
                    == ListOffsetsPartitionResponse {
                        partition_index: 0,
                        error_code: codes::NONE,
                        timestamp: UNKNOWN_TIMESTAMP,
                        offset: UNKNOWN_OFFSET,
                        leader_epoch: -1,
                        ..Default::default()
                    }
            );
        }

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn tiered_sentinels_return_finished_remote_frontier_and_pending_epoch() {
        use {
            krabka_ids::LeaderEpoch,
            krabka_remote_storage::{
                RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentMetadata,
                RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, TopicIdPartition,
            },
        };

        const TOPIC: &str = "list-offsets-tiered";

        let ClientBroker {
            broker,
            client,
            dirs: _dirs,
        } = remote_topic(TOPIC, vec![]).await;
        broker
            .produce_records_for_test(TOPIC, 0, 10)
            .await
            .expect("produce");

        let broker_arc = broker.broker_arc_for_test();
        let topic_id = broker_arc
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic metadata")
            .topic_id;
        let topic_partition = TopicIdPartition::new(topic_id, TOPIC, 0);
        let rlmm = broker_arc
            .remote_reader
            .as_ref()
            .expect("remote reader")
            .rlmm
            .clone();
        let finished_id = RemoteLogSegmentId::new(topic_partition.clone(), uuid::Uuid::new_v4());
        let finished = RemoteLogSegmentMetadata::new(
            finished_id.clone(),
            0,
            4,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .expect("finished metadata");
        rlmm.add_remote_log_segment_metadata(finished)
            .expect("add finished segment");
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: finished_id,
            event_timestamp_ms: 0,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .expect("finish segment");
        rlmm.add_remote_log_segment_metadata(
            RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(topic_partition, uuid::Uuid::new_v4()),
                5,
                8,
                0,
                1,
                0,
                RemoteLogSegmentDetails::new(
                    1,
                    RemoteLogSegmentState::CopySegmentStarted,
                    maplit::btreemap! {LeaderEpoch(0) => 5},
                ),
            )
            .expect("started metadata"),
        )
        .expect("add in-progress segment");

        assert!(
            list_one(&client, TOPIC, LATEST_TIERED_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 4,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 5,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        // `UnifiedLog.fetchEarliestPendingUploadOffset` clamps the raw remote
        // frontier (5) with the log start offset, so once `DeleteRecords`
        // moves the log start past it, the log start wins: naming the raw
        // frontier here would report an offset the fetch path refuses.
        broker
            .test_advance_log_start(TOPIC, 0, 7)
            .await
            .expect("advance leader log start");
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 7,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn earliest_pending_upload_falls_back_to_earliest_when_nothing_is_tiered() {
        const TOPIC: &str = "list-offsets-pending-upload-empty-tier";

        let ClientBroker {
            broker,
            client,
            dirs: _dirs,
        } = remote_topic(TOPIC, vec![]).await;
        broker
            .produce_records_for_test(TOPIC, 0, 4)
            .await
            .expect("produce");

        // The RLMM has no segment for this partition at all, and the local
        // log still holds everything from its own start: nothing has ever
        // reached the remote tier. Kafka answers EARLIEST rather than -1 in
        // this state.
        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: 0,
                    leader_epoch: 0,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn earliest_pending_upload_stays_unknown_when_local_segments_outrun_the_rlmm() {
        const TOPIC: &str = "list-offsets-pending-upload-unconfirmed";

        let ClientBroker {
            broker,
            client,
            dirs: _dirs,
        } = remote_topic(TOPIC, vec![CreatableTopicConfig {
            name: "internal.segment.bytes".into(),
            value: Some("1".into()),
            ..Default::default()
        }])
        .await;
        broker
            .produce_records_for_test(TOPIC, 0, 6)
            .await
            .expect("produce");

        // Drop the local copies of the sealed segments directly, without
        // going through the RLMM upload path and without moving the global
        // log start. This is the state a leader that lost its RLMM cache
        // memory would observe: files gone from disk, but no record of
        // what -- if anything -- ever reached the tier. Kafka reports -1
        // here rather than guessing.
        let broker_arc = broker.broker_arc_for_test();
        let partition = broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition");
        {
            let mut log = partition.log.lock().expect("log mutex poisoned");
            let removed = log
                .delete_local_segments_through(krabka_log::Offset(3))
                .expect("evict sealed segments");
            assert!(removed > 0, "the tiny segment size must have sealed some");
        }

        assert!(
            list_one(&client, TOPIC, EARLIEST_PENDING_UPLOAD_TIMESTAMP).await
                == ListOffsetsPartitionResponse {
                    partition_index: 0,
                    error_code: codes::NONE,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: UNKNOWN_OFFSET,
                    leader_epoch: UNKNOWN_EPOCH,
                    ..Default::default()
                }
        );

        drop(client);
        broker.shutdown().await;
    }

    /// One `ListOffsets` for partition 0 of `topic`, at a chosen wire
    /// `version`, bypassing the client's own version negotiation. KIP-207's
    /// error code depends on the version (`OFFSET_NOT_AVAILABLE` from v5,
    /// `LEADER_NOT_AVAILABLE` below it), so this suite needs to pick the
    /// version a case tests rather than always sending the client's max.
    async fn list_one_at_version(
        broker: &BrokerHandle,
        topic: &str,
        (timestamp, isolation_level): (i64, i8),
        version: i16,
    ) -> ListOffsetsPartitionResponse {
        let broker_arc = broker.broker_arc_for_test();
        test_ctx!(ctx, "admin");
        let req = encode_request(
            &krabka_protocol::owned::list_offsets_request::ListOffsetsRequest {
                replica_id: -1,
                topics: vec![
                    krabka_protocol::owned::list_offsets_request::ListOffsetsTopic {
                        name: topic.to_string(),
                        partitions: vec![
                            krabka_protocol::owned::list_offsets_request::ListOffsetsPartition {
                                partition_index: 0,
                                current_leader_epoch: -1,
                                timestamp,
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    },
                ],
                isolation_level,
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let bytes = handle(&broker_arc, version, &req, &ctx)
            .await
            .expect("handle");
        let mut response = decode_response(&bytes, version);
        response.topics.remove(0).partitions.remove(0)
    }

    /// One record, appended straight through the partition's writer with a
    /// chosen create timestamp and leader epoch, bypassing the ordinary
    /// Produce validation path (which would otherwise stamp its own
    /// timestamp). With `transactional_producer`, the record opens a
    /// transaction that producer leaves open, which pins the last stable
    /// offset at the offset it lands at. Returns that offset.
    async fn produce_one_at_timestamp(
        partition: &Partition,
        leader_epoch: i32,
        timestamp: i64,
        transactional_producer: Option<i64>,
    ) -> i64 {
        let mut batch = krabka_protocol::records::RecordBatch {
            partition_leader_epoch: leader_epoch,
            base_timestamp: timestamp,
            max_timestamp: timestamp,
            records: vec![krabka_protocol::records::Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Default::default()
            }],
            ..Default::default()
        };
        if let Some(producer_id) = transactional_producer {
            batch.producer_id = producer_id;
            batch.producer_epoch = 0;
            batch.base_sequence = 0;
            batch.attributes = batch.attributes.with_transactional(true);
        }
        partition.produce_batch(batch).await.expect("produce").0
    }

    #[test]
    fn lagging_high_watermark_error_follows_the_high_watermark_and_the_version() {
        // (label, replica id, epoch start, high watermark, version, error)
        let cases = [
            ("no epoch start", -1, None, 3, 11, None),
            ("caught up exactly", -1, Some(4), 4, 11, None),
            ("caught up past", -1, Some(4), 6, 11, None),
            (
                "lagging, v11",
                -1,
                Some(4),
                3,
                11,
                Some(codes::OFFSET_NOT_AVAILABLE),
            ),
            (
                "lagging, v5",
                -1,
                Some(4),
                3,
                5,
                Some(codes::OFFSET_NOT_AVAILABLE),
            ),
            (
                "lagging, v4",
                -1,
                Some(4),
                3,
                4,
                Some(codes::LEADER_NOT_AVAILABLE),
            ),
            (
                "lagging, v1",
                -1,
                Some(4),
                3,
                1,
                Some(codes::LEADER_NOT_AVAILABLE),
            ),
            ("lagging, follower", 3, Some(4), 3, 11, None),
            ("lagging, debugging replica", -2, Some(4), 3, 11, None),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, replica_id, epoch_start, high_watermark, version, error) in cases {
            actual.push((
                label,
                lagging_high_watermark_error(replica_id, epoch_start, high_watermark, version),
            ));
            expected.push((label, error));
        }
        assert!(actual == expected);
    }

    #[test]
    fn a_raised_lagging_high_watermark_error_fences_what_kafka_fences() {
        const LAST_FETCHABLE: i64 = 3;
        // (kind, candidate offset, fenced)
        let cases = [
            (ListOffsetsKind::Latest, 7, true),
            (ListOffsetsKind::Latest, 0, true),
            (ListOffsetsKind::Earliest, 0, false),
            (ListOffsetsKind::EarliestLocal, 0, false),
            (ListOffsetsKind::Timestamp, 2, false),
            (ListOffsetsKind::Timestamp, LAST_FETCHABLE, true),
            (ListOffsetsKind::Timestamp, 5, true),
            (ListOffsetsKind::Timestamp, UNKNOWN_OFFSET, true),
            (ListOffsetsKind::MaxTimestamp, 2, false),
            (ListOffsetsKind::MaxTimestamp, LAST_FETCHABLE, true),
            (ListOffsetsKind::MaxTimestamp, UNKNOWN_OFFSET, true),
            (ListOffsetsKind::LatestTiered, 2, false),
            (ListOffsetsKind::LatestTiered, LAST_FETCHABLE, true),
            (ListOffsetsKind::LatestTiered, UNKNOWN_OFFSET, false),
            (ListOffsetsKind::EarliestPendingUpload, 2, false),
            (ListOffsetsKind::EarliestPendingUpload, 4, true),
            (
                ListOffsetsKind::EarliestPendingUpload,
                UNKNOWN_OFFSET,
                false,
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (kind, candidate, fenced) in cases {
            actual.push((
                kind,
                candidate,
                lagging_high_watermark_fences(kind, candidate, LAST_FETCHABLE),
            ));
            expected.push((kind, candidate, fenced));
        }
        assert!(actual == expected);
    }

    /// KIP-207 over the wire, one row per request shape, in the two states
    /// `Partition.fetchOffsetForTimestamp` distinguishes: the high watermark
    /// lagging the epoch start and caught up to it, with an open transaction
    /// holding the last stable offset below the epoch start throughout.
    ///
    /// While the high watermark lags, LATEST is fenced outright, and a lookup
    /// that resolves against record data is fenced when it found nothing or
    /// found an offset at or above the request's bound -- even one below the
    /// epoch start. An offset below the bound is answered. The lag itself is
    /// measured against the high watermark at every isolation level, so a
    /// `read_committed` client is not fenced once the watermark has caught up.
    #[tokio::test]
    async fn offset_not_available_follows_partition_fetch_offset_for_timestamp() {
        const TOPIC: &str = "list-offsets-kip-207";
        const OLD_TIMESTAMP: i64 = 1_000;
        const NEW_TIMESTAMP: i64 = 5_000;
        const PAST_EVERY_RECORD: i64 = 9_000;
        const PRODUCER_ID: i64 = 77;
        const READ_UNCOMMITTED: i8 = 0;
        const READ_COMMITTED: i8 = 1;

        list_offsets_topic_fixture!((broker, _dir, client), TOPIC);
        let broker_arc = broker.broker_arc_for_test();
        let partition = broker_arc
            .partitions
            .get(TOPIC, krabka_ids::PartitionIndex(0))
            .expect("partition");

        // Epoch 0 holds offsets 0-3 at ascending old timestamps, and offset 2
        // opens a transaction that never resolves, so the last stable offset
        // stays at 2. Epoch 1 starts at offset 4 with later timestamps.
        for delta in 0..4 {
            let transactional = (delta == 2).then_some(PRODUCER_ID);
            produce_one_at_timestamp(&partition, 0, OLD_TIMESTAMP + delta, transactional).await;
        }
        broker.test_set_leader_epoch(TOPIC, 0, 1);
        for delta in 0..2 {
            produce_one_at_timestamp(&partition, 1, NEW_TIMESTAMP + delta, None).await;
        }

        let found = |offset: i64, timestamp: i64, leader_epoch: i32| ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp,
            offset,
            leader_epoch,
            ..Default::default()
        };
        let not_found = found(UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP, UNKNOWN_EPOCH);

        // Force the high watermark back below the epoch-1 start, the way a
        // fresh leader's sits before replication catches it up.
        partition.replica_state.lock().await.hw = krabka_log::Offset(3);
        // `None` stands for the KIP-207 error row, whose code depends on the
        // version. MAX_TIMESTAMP (KIP-734) only decodes from v7, above the v5
        // floor `OFFSET_NOT_AVAILABLE` needs.
        // (label, timestamp, isolation level, versions, expected row)
        let lagging = [
            (
                "LATEST",
                LATEST_TIMESTAMP,
                READ_UNCOMMITTED,
                &[4, 5, 11][..],
                None,
            ),
            (
                "a timestamp resolving into the new epoch",
                NEW_TIMESTAMP,
                READ_UNCOMMITTED,
                &[4, 5, 11],
                None,
            ),
            (
                "MAX_TIMESTAMP",
                MAX_TIMESTAMP,
                READ_UNCOMMITTED,
                &[7, 11],
                None,
            ),
            // Below the epoch start but at the high watermark: Kafka compares
            // the resolved offset with the bound, not with the epoch start.
            (
                "a timestamp resolving at the bound, below the epoch start",
                OLD_TIMESTAMP + 3,
                READ_UNCOMMITTED,
                &[4, 5, 11],
                None,
            ),
            // An empty lookup result is fenced too.
            (
                "a timestamp past every record",
                PAST_EVERY_RECORD,
                READ_UNCOMMITTED,
                &[4, 5, 11],
                None,
            ),
            (
                "a timestamp resolving below the bound",
                OLD_TIMESTAMP + 1,
                READ_UNCOMMITTED,
                &[4, 11],
                Some(found(1, OLD_TIMESTAMP + 1, 0)),
            ),
            // The last stable offset (2) is the bound at read_committed.
            (
                "a timestamp resolving at the last stable offset",
                OLD_TIMESTAMP + 2,
                READ_COMMITTED,
                &[4, 11],
                None,
            ),
            (
                "EARLIEST",
                EARLIEST_TIMESTAMP,
                READ_UNCOMMITTED,
                &[4, 11],
                Some(found(0, UNKNOWN_TIMESTAMP, 0)),
            ),
        ];
        for (label, timestamp, isolation_level, versions, expected) in lagging {
            for &version in versions {
                let expected = expected.clone().unwrap_or_else(|| {
                    error_response(
                        0,
                        if version >= 5 {
                            codes::OFFSET_NOT_AVAILABLE
                        } else {
                            codes::LEADER_NOT_AVAILABLE
                        },
                    )
                });
                assert!(
                    list_one_at_version(&broker, TOPIC, (timestamp, isolation_level), version,)
                        .await
                        == expected,
                    "lagging: {label} at v{version}"
                );
            }
        }

        // Caught up: nothing is fenced, and each lookup is measured against
        // its own bound. The open transaction keeps the read_committed bound
        // at 2, below the epoch start, and Kafka still does not fence it
        // because the lag is read from the high watermark.
        partition.replica_state.lock().await.hw = krabka_log::Offset(6);
        let caught_up = [
            (
                "LATEST, read_uncommitted",
                LATEST_TIMESTAMP,
                READ_UNCOMMITTED,
                found(6, UNKNOWN_TIMESTAMP, 1),
            ),
            (
                "LATEST, read_committed",
                LATEST_TIMESTAMP,
                READ_COMMITTED,
                found(2, UNKNOWN_TIMESTAMP, 1),
            ),
            (
                "a timestamp resolving into the new epoch, read_committed",
                NEW_TIMESTAMP,
                READ_COMMITTED,
                not_found.clone(),
            ),
            (
                "a timestamp past every record",
                PAST_EVERY_RECORD,
                READ_UNCOMMITTED,
                not_found,
            ),
            (
                "a timestamp resolving into the new epoch",
                NEW_TIMESTAMP,
                READ_UNCOMMITTED,
                found(4, NEW_TIMESTAMP, 1),
            ),
        ];
        for (label, timestamp, isolation_level, expected) in caught_up {
            assert!(
                list_one_at_version(&broker, TOPIC, (timestamp, isolation_level), 11,).await
                    == expected,
                "caught up: {label}"
            );
        }

        drop(client);
        broker.shutdown().await;
    }
}
