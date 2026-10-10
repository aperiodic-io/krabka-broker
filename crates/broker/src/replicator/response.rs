//! Interpretation of one partition row of a `Fetch` response.
//!
//! One request now covers every partition a fetcher follows on one leader, so
//! the response carries a row per partition rather than exactly one row. This
//! module handles one row: it applies the identity, epoch and target checks
//! that the single-row path always applied, appends the records the row
//! carries, and maps every Kafka error code to what the fetcher should do with
//! that partition -- including the KIP-320 in-band divergence signal.
//!
//! A row never sleeps. A backoff is the fetcher's, not one partition's: a
//! sleep taken here would stall every other partition sharing the round, which
//! is the cost the batching exists to remove. A row that wants one says so
//! with [`RowAction::Backoff`] and the loop applies the longest of them once.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Buf as _;
use krabka_ids::PartitionIndex;
use krabka_log::{Offset, VerbatimBatch};
use krabka_protocol::{
    owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, RecordBatch, RecordsError, RecordsPayload, validate_one_v2_batch},
};
use krabka_units::Time;
use krabka_verified::{ReplicaFetchMutation, broker::ReplicaFetchFacts};
use tracing::{info, warn};

use super::{
    Config, FollowedKey,
    follower_throttle::record_replicated,
    replication_target_changed,
    truncation::{
        diverging_epoch_truncation_target, handle_epoch_fence,
        handle_offset_moved_to_tiered_storage, handle_offset_out_of_range,
    },
};
use crate::codes;

/// What the fetcher should do with one partition after reading its row.
///
/// It is not `Eq`: [`RowAction::Backoff`] carries a [`Time`], whose `f64`
/// storage is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum RowAction {
    /// Keep following the partition and fetch it again next round.
    Continue,
    /// Stop following the partition. The leader disowned it, or the response
    /// was fenced against a target this follower no longer has. The next
    /// supervisor reconcile decides where it belongs.
    Drop,
    /// Keep following the partition, but hold the whole round back first, so a
    /// persistent error does not hot-spin the fetch loop.
    Backoff(Time),
    /// Stop following the partition, because an append to it failed on a log
    /// directory that is now offline. The supervisor does not follow it again,
    /// as Kafka's `ReplicaManager.handleLogDirFailure` removes the fetcher of
    /// every partition in a failed directory.
    Offline,
}

/// The action for a partition whose append just failed: [`RowAction::Offline`]
/// when the failure took its log directory offline, and `None` otherwise.
fn offline_after_failed_append(
    part: &crate::partition::Partition,
    cfg: &Config,
) -> Option<RowAction> {
    cfg.log_dir_status
        .is_offline(&part.log_dir.load())
        .then_some(RowAction::Offline)
}

/// Raise this follower's log start offset to the leader's, as Kafka's
/// `ReplicaFetcherThread.processPartitionData` does with
/// `maybeIncrementLogStartOffset(leaderLogStartOffset)`.
///
/// The records below the leader's log start are deleted on the leader, by a
/// `DeleteRecords` or by retention, so a follower that kept them would serve
/// them again after a leader change. The new start never passes this
/// follower's high watermark, which is the bound Kafka's
/// `UnifiedLog.maybeIncrementLogStartOffset` enforces.
///
/// A diskless partition's local log start is the flusher's trim frontier, and
/// its delete floor travels through the WAL index instead, so it is left
/// alone here.
async fn follow_leader_log_start(
    part: &crate::partition::Partition,
    cfg: &Config,
    leader_log_start: i64,
) {
    if part.diskless {
        return;
    }
    let target = Offset(leader_log_start).min(part.high_watermark().await);
    if target <= part.log_start_offset() {
        return;
    }
    if let Err(error) = part.trim_to_offset(target).await {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
            leader_log_start, "replicator: raising the log start to the leader's failed");
    }
}

/// Applies one partition row of a response against the partition it names.
///
/// `part_resp` is taken by *mutable* reference so the record batches can be
/// moved out of it (via `records.take()`) and handed to the writer without a
/// deep clone per batch.
///
/// `request_leader_epoch` is the epoch this follower sent for *this* partition
/// in the request the row answers. The metadata image may advance while a
/// request is in flight, and a batched request carries a different epoch per
/// row, so the fence is per row and not per response.
// KIP-320 in-band truncation + KIP-101 epoch fence add match arms
pub(super) async fn handle_partition_response(
    part_resp: &mut PartitionData,
    cfg: &Config,
    request_leader_epoch: i32,
) -> RowAction {
    let target_matches = !replication_target_changed(cfg);
    let reported_leader = &part_resp.current_leader;
    let reported_leader_absent =
        reported_leader.leader_id == -1 && reported_leader.leader_epoch == -1;
    let reported_leader_exact = u64::try_from(reported_leader.leader_id).ok()
        == Some(cfg.leader_node_id.0)
        && reported_leader.leader_epoch == cfg.leader_epoch.0;
    let reported_target_matches = reported_leader_absent || reported_leader_exact;
    // The row's topic and partition identity is already fenced: the caller
    // hands over only the one row `ResponseIndex::locate` attributes to this
    // partition.
    let mutation = krabka_verified::replica_fetch_mutation(ReplicaFetchFacts {
        request_leader_epoch,
        current_leader_epoch: cfg.leader_epoch.0,
        target_matches,
        reported_target_matches,
        error_code: part_resp.error_code,
        diverging_epoch: part_resp.diverging_epoch.epoch,
    });

    if mutation == ReplicaFetchMutation::Reject {
        warn!(
            topic = %cfg.topic,
            partition = cfg.partition.get(),
            leader_node_id = cfg.leader_node_id.0,
            leader_epoch = cfg.leader_epoch.0,
            request_leader_epoch,
            reported_leader_id = reported_leader.leader_id,
            reported_leader_epoch = reported_leader.leader_epoch,
            "replicator: discarding fenced Fetch response"
        );
        return RowAction::Drop;
    }

    match mutation {
        ReplicaFetchMutation::Truncate => {
            // KIP-320: an in-band divergence signal. The leader served no
            // records and named the largest epoch it has that is at or below
            // our last fetched epoch, and where that epoch ends. That is
            // the leader's history, not our truncation point: Kafka's
            // `getOffsetTruncationState` intersects it with our own epoch
            // history first. `EpochEndOffset` defaults to (epoch:-1,
            // end_offset:-1); a populated `epoch >= 0` marks a divergence.
            // Recheck immediately before mutating: metadata may have
            // changed since the response-level guard above.
            if replication_target_changed(cfg) {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: skipping diverging_epoch truncation from stale target");
                return RowAction::Drop;
            }
            if let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) {
                let _target_guard = lock_local_target!(
                    part,
                    cfg,
                    "replicator: skipping diverging_epoch truncation from stale local target"
                );
                let leader_end_offset = part_resp.diverging_epoch.end_offset;
                let end_offset =
                    diverging_epoch_truncation_target(&part, &part_resp.diverging_epoch).0;
                match part.truncate_to(Offset(end_offset)).await {
                    Ok(()) => {
                        // Drop idempotent-producer dedup entries for the
                        // truncated tail, or a retried batch deduplicates
                        // against an offset the log no longer holds and its
                        // acks=all HW gate stalls forever (failover stall).
                        cfg.producer_state
                            .truncate(&cfg.topic, cfg.partition, end_offset)
                            .await;
                        info!(
                            topic = %cfg.topic,
                            partition = cfg.partition.get(),
                            leader_epoch = part_resp.diverging_epoch.epoch,
                            leader_end_offset,
                            end_offset,
                            "replicator: truncated to diverging_epoch (KIP-320 in-band)"
                        );
                    }
                    Err(e) => warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        end_offset,
                        error = %e,
                        "replicator: truncate_to(diverging_epoch) failed"
                    ),
                }
            }
            RowAction::Continue
        }

        ReplicaFetchMutation::Append => {
            let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
                warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: local partition vanished between fetches");
                return RowAction::Continue;
            };
            let _target_guard = lock_local_target!(
                part,
                cfg,
                "replicator: discarding response for stale local target"
            );
            match part_resp.records.take() {
                Some(RecordsPayload::Raw(bytes)) => {
                    let action = replicate_raw_batches(&part, cfg, bytes).await;
                    if action != RowAction::Continue {
                        return action;
                    }
                }
                Some(RecordsPayload::V2(batches)) => {
                    for batch in batches {
                        if replication_target_changed(cfg) {
                            return RowAction::Drop;
                        }
                        // Capture byte count before the move into replicate_batch
                        // so the metrics update only fires on a successful append.
                        // PERF — measured; decision: KEEP. `encoded_len()` is
                        // computed here for the metric and again inside the append
                        // path; threading a single computation through would save
                        // the re-walk, but that changes the writer API
                        // (cross-file). `benches/perf_deferrals.rs` times the walk
                        // against the `replicate_batch` it precedes, over the
                        // batch shapes a producer actually writes: 0.01% of the
                        // append for one 100 KiB record, ~1% for 100 x 1 KiB, ~4%
                        // for 1000 x 100 B. Even that worst shape's ~4% sits
                        // inside the append's own run-to-run spread, so the
                        // re-walk is not separable from noise in production.
                        // Those three figures are what the `bench` job of the `ci`
                        // workflow measures on the nightly schedule and prints in
                        // its job summary; the latest scheduled `ci` run in the
                        // Actions tab is the reproducible reading, and its
                        // `criterion-baseline` artifact holds the samples.
                        // Revisit only if the replicator's shape mix moves to very
                        // wide batches of tiny records, where the walk is a
                        // per-record cost and the append is not.
                        let batch_bytes = batch.encoded_len();
                        if let Err(e) = part.replicate_batch(batch).await {
                            warn!(error = %e, topic = %cfg.topic, partition = cfg.partition.get(),
                            "replicator: replicate_batch failed");
                            if let Some(offline) = offline_after_failed_append(&part, cfg) {
                                return offline;
                            }
                            break;
                        }
                        record_replicated(cfg, u64::try_from(batch_bytes).unwrap_or(0));
                    }
                }
                _ => {}
            }
            // KIP-392: record the leader's high watermark so consumer reads
            // served from this follower are bounded correctly. Done on every
            // successful response, including empty ones. Wrap the wire `i64`.
            if replication_target_changed(cfg) {
                return RowAction::Drop;
            }
            part.set_follower_hw(Offset(part_resp.high_watermark)).await;
            // KIP-73: the lag behind the leader's high watermark decides
            // whether the follower throttle applies to this partition.
            cfg.lag
                .update(part_resp.high_watermark, part.log_end_offset().0);
            follow_leader_log_start(&part, cfg, part_resp.log_start_offset).await;
            RowAction::Continue
        }

        ReplicaFetchMutation::Retry => match part_resp.error_code {
            codes::OFFSET_OUT_OF_RANGE => handle_offset_out_of_range(cfg).await,
            // KIP-405: the leader still holds this offset, but only in the
            // remote tier, and it will not stream the archive down the
            // replication path. The follower starts again at the leader's
            // local log start instead.
            codes::OFFSET_MOVED_TO_TIERED_STORAGE => {
                handle_offset_moved_to_tiered_storage(cfg).await
            }
            codes::UNKNOWN_TOPIC_OR_PARTITION => {
                // Leader hasn't materialized its side yet
                // (CreateTopics-vs-replicator race).
                RowAction::Backoff(cfg.connection.replication.unknown_topic_retry_delay)
            }
            codes::NOT_LEADER_OR_FOLLOWER => RowAction::Drop,
            codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
                // Stale-response guard: if the target changed, this
                // follower-replicator is stale — STOP it. Without this it neither
                // truncates (the `replication_target_changed` guard in
                // `handle_epoch_fence` skips that) nor stops, so it hot-loops the
                // Fetch at ~full CPU, starving metadata propagation and the
                // cooperative cancellation that would otherwise retire it — the
                // broker then never becomes ready and crashloops.
                if replication_target_changed(cfg) {
                    warn!(topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: stopping on fenced epoch from stale target");
                    return RowAction::Drop;
                }
                if part_resp.error_code == codes::FENCED_LEADER_EPOCH {
                    warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        "replicator: fenced leader epoch; calling OffsetForLeaderEpoch"
                    );
                    if let Err(error) = handle_epoch_fence(cfg).await {
                        warn!(
                            topic = %cfg.topic,
                            partition = cfg.partition.get(),
                            %error,
                            "replicator: OffsetForLeaderEpoch recovery failed"
                        );
                    }
                } else {
                    // The leader is behind our metadata image. It cannot answer an
                    // epoch lookup yet, so leave the log untouched and retry Fetch.
                    warn!(
                        topic = %cfg.topic,
                        partition = cfg.partition.get(),
                        "replicator: leader does not know current epoch; retrying Fetch"
                    );
                }
                // Back off before re-fetching so a persistent fence (e.g. our
                // leader_epoch hasn't caught up to the new leader's yet) doesn't
                // hot-spin the CPU between fetch and fence.
                RowAction::Backoff(cfg.connection.replication.epoch_fence_backoff)
            }
            other => {
                warn!(
                    error_code = other,
                    "replicator: unexpected fetch error_code"
                );
                RowAction::Backoff(cfg.connection.replication.unexpected_error_backoff)
            }
        },

        ReplicaFetchMutation::Reject => unreachable!("rejected responses return above"),
    }
}

async fn replicate_raw_batches(
    part: &crate::partition::Partition,
    cfg: &Config,
    mut remaining: bytes::Bytes,
) -> RowAction {
    while !remaining.is_empty() {
        if replication_target_changed(cfg) {
            return RowAction::Drop;
        }
        let validated = match validate_one_v2_batch(&remaining) {
            Ok(batch) => batch,
            Err(RecordsError::HeaderTooShort { .. } | RecordsError::BodyTooShort { .. }) => break,
            Err(error) => {
                warn!(%error, topic = %cfg.topic, partition = cfg.partition.get(),
                    "replicator: invalid raw record batch");
                break;
            }
        };
        let batch_len = validated.total_len;
        let batch_bytes = remaining.slice(..batch_len);
        let header = validated.header;
        let attributes = Attributes(header.attributes.get());
        let base_offset = Offset(header.base_offset.get());
        let result = if attributes.is_control_batch() {
            let mut cursor: &[u8] = &batch_bytes;
            match RecordBatch::decode(&mut cursor) {
                Ok(batch) => part.replicate_batch(batch).await,
                Err(error) => {
                    warn!(%error, topic = %cfg.topic, partition = cfg.partition.get(),
                        "replicator: invalid control batch");
                    break;
                }
            }
        } else {
            let batch = VerbatimBatch::from_header(header, batch_bytes);
            part.replicate_verbatim(batch, base_offset).await
        };
        if let Err(error) = result {
            warn!(%error, topic = %cfg.topic, partition = cfg.partition.get(),
                "replicator: append failed");
            if let Some(offline) = offline_after_failed_append(part, cfg) {
                return offline;
            }
            break;
        }
        record_replicated(cfg, u64::try_from(batch_len).unwrap_or(0));
        remaining.advance(batch_len);
    }
    RowAction::Continue
}

/// One partition this round asked about, and the epoch it asked under.
pub(super) struct RoundEntry {
    /// The `current_leader_epoch` this follower sent for this partition. The
    /// metadata image can advance while the request is in flight, so the fence
    /// is against what was sent and not against what is current.
    pub(super) request_leader_epoch: i32,
}

/// Stable lookup built only when a fetcher's metadata membership changes.
pub(super) struct FollowedIndex {
    by_id: std::collections::HashMap<(WireUuid, i32), Arc<Config>>,
    by_name: std::collections::HashMap<Arc<str>, std::collections::HashMap<i32, Arc<Config>>>,
}

impl FollowedIndex {
    pub(super) fn new(followed: &BTreeMap<FollowedKey, Arc<Config>>) -> Self {
        let mut out = Self {
            by_id: std::collections::HashMap::with_capacity(followed.len()),
            by_name: std::collections::HashMap::with_capacity(followed.len()),
        };
        for cfg in followed.values() {
            out.by_id
                .insert((cfg.topic_id, cfg.partition.get()), Arc::clone(cfg));
            out.by_name
                .entry(Arc::clone(&cfg.topic))
                .or_default()
                .insert(cfg.partition.get(), Arc::clone(cfg));
        }
        out
    }

    fn config(&self, topic: &FetchableTopicResponse, partition: i32) -> Option<Arc<Config>> {
        let by_id = (topic.topic_id != WireUuid::ZERO)
            .then(|| self.by_id.get(&(topic.topic_id, partition)))
            .flatten();
        let by_name = if topic.topic.is_empty() {
            None
        } else {
            self.by_name
                .get(topic.topic.as_str())
                .and_then(|partitions| partitions.get(&partition))
        };
        match (by_id, by_name) {
            (Some(a), Some(b)) if Arc::ptr_eq(a, b) => Some(Arc::clone(a)),
            (Some(cfg), None) | (None, Some(cfg)) => Some(Arc::clone(cfg)),
            _ => None,
        }
    }
}

/// Applies only rows the leader returned. Incremental Fetch responses omit
/// caught-up partitions, so this avoids walking every followed partition on
/// every long-poll completion.
pub(super) async fn apply_response_indexed(
    response: &mut FetchResponse,
    followed: &FollowedIndex,
    request_epochs: &std::collections::HashMap<FollowedKey, i32>,
) -> Vec<(FollowedKey, RowAction)> {
    let response_index = ResponseIndex::of(response);
    let mut rows = Vec::new();
    for topic_index in 0..response.responses.len() {
        for partition_index in 0..response.responses[topic_index].partitions.len() {
            let topic = &response.responses[topic_index];
            let partition = topic.partitions[partition_index].partition_index;
            let Some(cfg) = followed.config(topic, partition) else {
                continue;
            };
            if response_index.locate(response, &cfg) != Some((topic_index, partition_index)) {
                continue;
            }
            let key = (Arc::clone(&cfg.topic), PartitionIndex(partition));
            let request_epoch = request_epochs
                .get(&key)
                .copied()
                .unwrap_or(cfg.leader_epoch.0);
            let action = handle_partition_response(
                &mut response.responses[topic_index].partitions[partition_index],
                &cfg,
                request_epoch,
            )
            .await;
            rows.push((key, action));
        }
    }
    rows
}

/// Where each `(topic identity, partition)` sits in one response.
///
/// A key that two rows claim maps to `None`, so the partition it names is left
/// untouched.
#[derive(Default)]
struct ResponseIndex {
    by_id: std::collections::HashMap<(WireUuid, i32), Option<(usize, usize)>>,
    by_name: std::collections::HashMap<(String, i32), Option<(usize, usize)>>,
}

impl ResponseIndex {
    fn of(response: &FetchResponse) -> Self {
        let mut index = Self::default();
        for (topic_index, topic) in response.responses.iter().enumerate() {
            for (partition_index, partition) in topic.partitions.iter().enumerate() {
                let at = (topic_index, partition_index);
                if topic.topic_id != WireUuid::ZERO {
                    index
                        .by_id
                        .entry((topic.topic_id, partition.partition_index))
                        .and_modify(|slot| *slot = None)
                        .or_insert(Some(at));
                }
                if !topic.topic.is_empty() {
                    index
                        .by_name
                        .entry((topic.topic.clone(), partition.partition_index))
                        .and_modify(|slot| *slot = None)
                        .or_insert(Some(at));
                }
            }
        }
        index
    }

    /// The row that answers `cfg`, if exactly one does and its identity
    /// matches on every field the negotiated version populated.
    fn locate(&self, response: &FetchResponse, cfg: &Config) -> Option<(usize, usize)> {
        let by_id = (cfg.topic_id != WireUuid::ZERO)
            .then(|| self.by_id.get(&(cfg.topic_id, cfg.partition.get())))
            .flatten();
        let by_name = self
            .by_name
            .get(&(cfg.topic.to_string(), cfg.partition.get()));
        // A key claimed twice under either identity is ambiguous under both:
        // the row it would resolve to is one of a contradictory pair.
        let at = match (by_id, by_name) {
            (Some(Some(at)), None) | (None, Some(Some(at))) => *at,
            (Some(Some(id_at)), Some(Some(name_at))) if id_at == name_at => *id_at,
            // Either identity claimed twice, the two identities disagreeing
            // about which row answers, or no row at all: none of them names
            // one unambiguous row, and none of them may mutate anything.
            _ => return None,
        };
        let topic = response.responses.get(at.0)?;
        fetch_topic_identity_matches(topic, cfg).then_some(at)
    }
}

fn fetch_topic_identity_matches(topic: &FetchableTopicResponse, cfg: &Config) -> bool {
    let name_present = !topic.topic.is_empty();
    let id_present = topic.topic_id != WireUuid::ZERO;
    (name_present || id_present)
        && (!name_present || topic.topic.as_str() == &*cfg.topic)
        && (!id_present || topic.topic_id == cfg.topic_id)
}

/// Applies one response to one partition, as the single-partition path used
/// to. It is the batched path with a round of one, so the tests below still
/// exercise the identity, epoch and target checks exactly as they run in
/// production.
#[cfg(test)]
pub(super) async fn handle_response(
    mut resp: FetchResponse,
    cfg: &Config,
    request_leader_epoch: i32,
) -> RowAction {
    let index = ResponseIndex::of(&resp);
    let Some((topic, partition)) = index.locate(&resp, cfg) else {
        return RowAction::Continue;
    };
    handle_partition_response(
        &mut resp.responses[topic].partitions[partition],
        cfg,
        request_leader_epoch,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_compression::CompressionType;
    use krabka_protocol::{
        owned::fetch_response::{EpochEndOffset, LeaderIdAndEpoch},
        records::{Record, RecordBatch},
    };
    use krabka_raft::NodeId;
    use krabka_units::{mebibytes, secs};

    use super::*;
    use crate::replicator::{
        ensure_local_partition,
        test_support::{
            LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, fetch_response, image_with_leader,
            partition_response, test_config,
        },
    };

    fn local_partition_fixture() -> (Config, tempfile::TempDir, Arc<crate::partition::Partition>) {
        let (cfg, log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let part = cfg.partitions.get(&cfg.topic, cfg.partition).unwrap();
        (cfg, log_dir, part)
    }

    fn one_record_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 4,
            last_offset_delta: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        }
    }

    /// A row that wants a backoff says so and returns; the sleep is the
    /// fetcher's, because a sleep taken here would hold back every other
    /// partition sharing the round.
    #[tokio::test(start_paused = true)]
    async fn unexpected_error_asks_for_the_configured_backoff() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.connection.replication.unexpected_error_backoff = secs(37);
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::INVALID_REQUEST),
        );

        let started = tokio::time::Instant::now();
        let action = handle_response(response, &cfg, cfg.leader_epoch.0).await;

        assert!(action == RowAction::Backoff(secs(37)));
        assert!(
            started.elapsed() == Duration::ZERO,
            "the row must not sleep on behalf of the round"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_leader_epoch_retries_without_epoch_lookup() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        cfg.connection.replication.epoch_fence_backoff = secs(37);
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::UNKNOWN_LEADER_EPOCH),
        );

        let action = handle_response(response, &cfg, cfg.leader_epoch.0).await;

        assert!(action == RowAction::Backoff(secs(37)));
        assert!(listener.accept().is_err(), "unexpected epoch lookup");
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_name() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WireUuid::ZERO,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }

    #[tokio::test]
    async fn handle_response_matches_fetch_topic_by_topic_id() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            "",
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }

    #[tokio::test]
    async fn handle_response_rejects_contradictory_or_duplicate_identity() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));

        for resp in [
            fetch_response(
                TOPIC,
                WireUuid([8; 16]),
                partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
            ),
            fetch_response(
                "other",
                WIRE_TOPIC_ID,
                partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
            ),
        ] {
            assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        }

        let mut duplicate = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER),
        );
        duplicate.responses[0]
            .partitions
            .push(partition_response(PARTITION, codes::NOT_LEADER_OR_FOLLOWER));
        assert!(handle_response(duplicate, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
    }

    #[tokio::test]
    async fn handle_response_rejects_mismatched_reported_leader() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        for current_leader in [
            LeaderIdAndEpoch {
                leader_id: 99,
                leader_epoch: cfg.leader_epoch.0,
                ..LeaderIdAndEpoch::default()
            },
            LeaderIdAndEpoch {
                leader_id: -2,
                leader_epoch: -2,
                ..LeaderIdAndEpoch::default()
            },
            LeaderIdAndEpoch {
                leader_id: -1,
                leader_epoch: cfg.leader_epoch.0,
                ..LeaderIdAndEpoch::default()
            },
        ] {
            let resp = fetch_response(
                TOPIC,
                WIRE_TOPIC_ID,
                PartitionData {
                    partition_index: PARTITION,
                    error_code: codes::NONE,
                    current_leader,
                    ..PartitionData::default()
                },
            );

            assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
        }
    }

    #[tokio::test]
    async fn stale_request_epoch_cannot_truncate_the_follower_log() {
        let (cfg, _log_dir, part) = local_partition_fixture();
        part.replicate_batch(one_record_batch(0)).await.unwrap();
        assert!(part.log_end_offset() == Offset(1));

        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                diverging_epoch: EpochEndOffset {
                    epoch: cfg.leader_epoch.0,
                    end_offset: 0,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            },
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0 - 1).await == RowAction::Drop);
        assert!(part.log_end_offset() == Offset(1));
    }

    /// KIP-320 follower side, `AbstractFetcherThread.getOffsetTruncationState`:
    /// the follower intersects the leader's `(epoch, end offset)` with its own
    /// epoch history. Each case is a follower log, written as runs of
    /// `(leader epoch, record count)`, and the diverging row the leader
    /// answered its Fetch with.
    #[tokio::test]
    async fn diverging_epoch_truncates_to_the_intersection_with_the_local_epochs() {
        struct Case {
            name: &'static str,
            /// The follower log as runs of `(leader epoch, record count)`.
            runs: &'static [(i32, usize)],
            /// The leader's diverging epoch and its end offset.
            diverging: (i32, i64),
            /// The log end offset after the truncation.
            expected_end: i64,
            /// The `(epoch, start offset)` entries left in the epoch history.
            expected_epochs: &'static [(i32, i64)],
        }
        let case = |name, runs, diverging, expected_end, expected_epochs| Case {
            name,
            runs,
            diverging,
            expected_end,
            expected_epochs,
        };
        let cases = [
            case(
                // The leader went 4 -> 5 -> 7 while this follower went 4 -> 6:
                // it does not know epoch 5, so it keeps only what epoch 4
                // ended with, not the leader's end for epoch 5.
                "two elections: the follower does not know the leader's epoch",
                &[(4, 10), (6, 5)],
                (5, 12),
                10,
                &[(4, 0)],
            ),
            case(
                // The leader's epoch 5 is longer than this whole log: the
                // leader's end offset cannot be a truncation point, or the
                // same row would come back on every Fetch.
                "the leader's end offset is past the log end",
                &[(4, 10), (6, 2)],
                (5, 20),
                10,
                &[(4, 0)],
            ),
            case(
                "a known epoch that the leader ended earlier than the follower",
                &[(4, 13)],
                (4, 8),
                8,
                &[(4, 0)],
            ),
            case(
                "a known epoch that the leader ended later than the follower",
                &[(4, 10), (6, 5)],
                (4, 12),
                10,
                &[(4, 0)],
            ),
            case(
                "the leader's epoch is below every epoch the follower recorded",
                &[(6, 5)],
                (5, 3),
                0,
                &[],
            ),
            case(
                "no leader end offset leaves the log alone",
                &[(4, 5)],
                (4, -1),
                5,
                &[(4, 0)],
            ),
        ];
        for Case {
            name,
            runs,
            diverging: (epoch, end_offset),
            expected_end,
            expected_epochs,
        } in cases
        {
            let (cfg, _log_dir, part) = local_partition_fixture();
            for &(leader_epoch, count) in runs {
                for _ in 0..count {
                    let mut batch = RecordBatch {
                        partition_leader_epoch: leader_epoch,
                        records: vec![Record::default()],
                        ..RecordBatch::default()
                    };
                    part.log.lock().unwrap().append(&mut batch).unwrap();
                }
            }
            let resp = fetch_response(
                TOPIC,
                WIRE_TOPIC_ID,
                PartitionData {
                    partition_index: PARTITION,
                    error_code: codes::NONE,
                    diverging_epoch: EpochEndOffset {
                        epoch,
                        end_offset,
                        ..EpochEndOffset::default()
                    },
                    ..PartitionData::default()
                },
            );

            let action = handle_response(resp, &cfg, cfg.leader_epoch.0).await;

            assert!(action == RowAction::Continue, "{name}");
            assert!(part.log_end_offset() == Offset(expected_end), "{name}");
            let epochs = part.epoch_history();
            assert!(epochs == expected_epochs, "{name}");
        }
    }

    #[tokio::test]
    async fn failed_append_leaves_log_retryable() {
        let (cfg, _log_dir, part) = local_partition_fixture();
        let respond = |base_offset| {
            fetch_response(
                TOPIC,
                WIRE_TOPIC_ID,
                PartitionData {
                    partition_index: PARTITION,
                    error_code: codes::NONE,
                    records: Some(RecordsPayload::V2(vec![one_record_batch(base_offset)])),
                    ..PartitionData::default()
                },
            )
        };
        assert!(handle_response(respond(0), &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        assert!(part.log_end_offset() == Offset(1));

        // A batch that starts below the log end offset is a duplicate. The
        // append is refused and leaves the log as it was.
        let bad = respond(0);
        assert!(handle_response(bad, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        assert!(part.log_end_offset() == Offset(1));

        let retry = respond(1);
        assert!(handle_response(retry, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        assert!(part.log_end_offset() == Offset(2));
    }

    fn one_batch_response(cfg: &Config, offset: i64, raw: bool) -> FetchResponse {
        let mut batch = one_record_batch(offset);
        batch.partition_leader_epoch = cfg.leader_epoch.0;
        let records = if raw {
            let mut encoded = BytesMut::new();
            batch.encode(&mut encoded).unwrap();
            RecordsPayload::Raw(encoded.freeze())
        } else {
            RecordsPayload::V2(vec![batch])
        };
        fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                records: Some(records),
                ..PartitionData::default()
            },
        )
    }

    /// A disk that refuses every write to a segment's `.log` file.
    #[derive(Debug)]
    struct LogWritesFail;

    impl krabka_log::LogIo for LogWritesFail {
        fn write(&self, _file: &std::fs::File, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::PermissionDenied.into())
        }

        fn write_vectored(
            &self,
            _file: &std::fs::File,
            _bufs: &[std::io::IoSlice<'_>],
        ) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::PermissionDenied.into())
        }
    }

    /// Kafka's `ReplicaManager.handleLogDirFailure`: an append that fails on
    /// the partition's log directory takes the directory offline, and the
    /// fetcher stops following the partition. It does not fetch the records
    /// again only to fail on them again. Decoded and raw records take the
    /// same path.
    #[tokio::test]
    async fn an_append_that_fails_on_its_log_directory_stops_following_the_partition() {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for raw in [false, true] {
            let (cfg, log_dir, part) = local_partition_fixture();
            part.log
                .lock()
                .unwrap()
                .test_set_io(Arc::new(LogWritesFail));
            let response = one_batch_response(&cfg, 0, raw);

            let action = handle_response(response, &cfg, cfg.leader_epoch.0).await;

            let offline: Vec<std::path::PathBuf> = cfg
                .log_dir_status
                .offline()
                .into_iter()
                .map(|(dir, _reason)| dir)
                .collect();
            actual.push((raw, action, offline, part.log_end_offset()));
            expected.push((
                raw,
                RowAction::Offline,
                vec![log_dir.path().to_path_buf()],
                Offset(0),
            ));
        }
        assert!(actual == expected);
    }

    /// A compacted leader's log has holes, so the batch after one starts past
    /// the follower's log end offset. Kafka's `appendAsFollower` takes it, and
    /// the follower's log end offset moves to the end of that batch, whether
    /// the records arrive as decoded batches or as raw bytes.
    #[tokio::test]
    async fn a_batch_past_the_log_end_offset_is_replicated_across_the_hole() {
        for raw in [false, true] {
            let (cfg, _log_dir, part) = local_partition_fixture();

            // Offsets 1 to 4 were compacted away on the leader.
            for base_offset in [0, 5] {
                let response = one_batch_response(&cfg, base_offset, raw);
                assert!(
                    handle_response(response, &cfg, cfg.leader_epoch.0).await
                        == RowAction::Continue
                );
            }

            assert!(part.log_end_offset() == Offset(6), "raw: {raw}");
        }
    }

    #[tokio::test]
    async fn raw_compressed_replication_stays_byte_exact() {
        let (cfg, _log_dir, part) = local_partition_fixture();
        let mut batch = one_record_batch(0);
        batch.partition_leader_epoch = cfg.leader_epoch.0;
        batch.attributes = batch.attributes.with_compression(CompressionType::Lz4);
        let mut encoded = BytesMut::new();
        batch.encode(&mut encoded).unwrap();
        let encoded = encoded.freeze();
        let response = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                records: Some(RecordsPayload::Raw(encoded.clone())),
                ..PartitionData::default()
            },
        );

        assert!(handle_response(response, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
        let stored = part
            .log
            .lock()
            .unwrap()
            .read_raw(Offset(0), Offset(1), mebibytes(1))
            .unwrap()
            .bytes;
        assert!(stored == encoded);
    }

    #[tokio::test]
    async fn handle_response_ignores_other_partition_index() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION + 1, codes::NOT_LEADER_OR_FOLLOWER),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Continue);
    }

    #[tokio::test]
    async fn handle_response_stops_on_diverging_epoch_after_local_promotion() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            PartitionData {
                partition_index: PARTITION,
                error_code: codes::NONE,
                diverging_epoch: EpochEndOffset {
                    epoch: 4,
                    end_offset: 0,
                    ..EpochEndOffset::default()
                },
                ..PartitionData::default()
            },
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }

    #[tokio::test]
    async fn handle_response_stops_on_out_of_range_after_remote_target_change() {
        let (cfg, _log_dir) = test_config(image_with_leader(NodeId(99)));
        let resp = fetch_response(
            TOPIC,
            WIRE_TOPIC_ID,
            partition_response(PARTITION, codes::OFFSET_OUT_OF_RANGE),
        );

        assert!(handle_response(resp, &cfg, cfg.leader_epoch.0).await == RowAction::Drop);
    }
}
