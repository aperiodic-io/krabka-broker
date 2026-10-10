//! Recovery paths that rewrite the local log after the follower and the leader
//! disagree.
//!
//! `OFFSET_OUT_OF_RANGE` asks the leader for its log end and log start with
//! `ListOffsets` and truncates to the one this replica has overrun.
//! `FENCED_LEADER_EPOCH` runs the KIP-101 `OffsetForLeaderEpoch` lookup and
//! truncates to the epoch boundary the leader reports.
//! A KIP-320 `diverging_epoch` row intersects the leader's `(epoch, end
//! offset)` with this replica's own epoch history before it truncates.
//! `OFFSET_MOVED_TO_TIERED_STORAGE` runs the KIP-405
//! `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)` lookup and restarts the log at the
//! leader's local log start.

use krabka_client_core::Connection;
use krabka_ids::LeaderEpoch;
use krabka_log::Offset;
use krabka_protocol::owned::{
    fetch_response::EpochEndOffset,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
    offset_for_leader_epoch_request::{
        OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
    },
};
use tracing::{info, warn};

use super::{
    Config, connection::connection_options, replication_target_changed, response::RowAction,
    task_replication_target,
};
use crate::{codes, network::client::InterBrokerError};

/// Where this follower truncates for a KIP-320 `diverging_epoch` row, as
/// `AbstractFetcherThread.getOffsetTruncationState` computes it.
///
/// `follower` is this replica's own `LeaderEpochFileCache.endOffsetFor`
/// answer for the leader's epoch: the largest local epoch at or below it and
/// where that epoch ends, or `(-1, -1)` when the history cannot place it.
///
/// - The leader's end offset is undefined: keep the log, and fetch on from the
///   log end.
/// - The local history cannot place the epoch: the leader's end offset, capped
///   at the log end.
/// - This replica does not know the leader's epoch (`follower.0` is a smaller
///   one): only the end of that smaller epoch, capped at the log end. Kafka
///   truncates there and asks `OffsetsForLeaderEpoch` again. The next `Fetch`
///   asks the same question in band, from the epoch this replica now ends in,
///   so the loop that follows needs no separate round trip.
/// - Otherwise the lower of this replica's end for the epoch and the leader's,
///   capped at the log end.
fn diverging_truncation_offset(
    follower: (LeaderEpoch, Offset),
    leader_epoch: LeaderEpoch,
    leader_end: Offset,
    log_end: Offset,
) -> Offset {
    let (follower_epoch, follower_end) = follower;
    if leader_end < Offset(0) {
        log_end
    } else if follower_end < Offset(0) {
        leader_end.min(log_end)
    } else if follower_epoch != leader_epoch {
        follower_end.min(log_end)
    } else {
        follower_end.min(leader_end).min(log_end)
    }
}

/// Where the local log of `part` truncates for the leader's `diverging`
/// epoch and end offset. See [`diverging_truncation_offset`].
pub(super) fn diverging_epoch_truncation_target(
    part: &crate::partition::Partition,
    diverging: &EpochEndOffset,
) -> Offset {
    let leader_epoch = LeaderEpoch(diverging.epoch);
    let log = part.log.lock().expect("log mutex poisoned");
    let log_end = log.log_end_offset();
    let follower = log
        .epoch_checkpoint()
        .epoch_and_offset_for(leader_epoch, log_end);
    diverging_truncation_offset(
        follower,
        leader_epoch,
        Offset(diverging.end_offset),
        log_end,
    )
}

/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`: a follower's `ListOffsets`
/// is answered with the leader's log end offset.
const LATEST_TIMESTAMP: i64 = -1;

/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`: the leader's (global) log
/// start offset.
const EARLIEST_TIMESTAMP: i64 = -2;

/// What `AbstractFetcherThread.fetchOffsetAndTruncate` does with this
/// replica's log once it knows the leader's log end offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeaderEndRecovery {
    /// The unclean-election case: the leader's log ends below this replica's,
    /// so the replica truncates to the leader's end and fetches from there.
    TruncateTo(i64),
    /// The leader's log reaches at least as far as this replica's; what to do
    /// next depends on the leader's log start.
    AskLeaderStart,
}

/// `fetchOffsetAndTruncate`'s first test, `leaderEndOffset <
/// replicaEndOffset`.
///
/// Its KIP-1023 branch, which restarts an empty replica at the last tiered
/// offset, needs `follower.fetch.last.tiered.offset.enable`, which defaults
/// to `false` and which this broker does not offer, so it is never taken.
fn recovery_for_leader_end(replica_end: i64, leader_end: i64) -> LeaderEndRecovery {
    if leader_end < replica_end {
        LeaderEndRecovery::TruncateTo(leader_end)
    } else {
        LeaderEndRecovery::AskLeaderStart
    }
}

/// `fetchOffsetAndTruncate`'s second test: the offset to restart an emptied
/// log at (`truncateFullyAndStartAt`) when the leader's log start lies above
/// this replica's end, or `None` when the replica keeps its log and fetches
/// on from its own end.
///
/// The second case covers a tiered leader whose remote tier was momentarily
/// unreachable, and an unclean election whose new leader has since written
/// past this replica: neither is evidence that this replica's log is stale.
fn reset_for_leader_start(replica_end: i64, leader_start: i64) -> Option<i64> {
    (leader_start > replica_end).then_some(leader_start)
}

/// Recovers from `OFFSET_OUT_OF_RANGE` the way Kafka's
/// `AbstractFetcherThread.fetchOffsetAndTruncate` does: ask the leader for
/// its log end offset with `ListOffsets(LATEST)`, truncate to it when it lies
/// below this replica's log end, and otherwise ask for its log start with
/// `ListOffsets(EARLIEST)` and restart an emptied log there when the log
/// start lies above this replica's end.
///
/// The error row's own offsets are not read: Kafka answers this error with
/// `LogReadResult(Errors)`, whose offsets are all -1. A lookup that fails
/// deletes nothing; the row backs off and fetches again, as Kafka's
/// `handleOutOfRangeError` delays a partition whose offset lookup failed.
// cargo-mutants: an IO-only wrapper. It connects to the leader and hands the
// `ListOffsets` round trip to `recover_offset_out_of_range`, which the tests
// below drive with a scripted leader.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_offset_out_of_range(cfg: &Config) -> RowAction {
    // Kafka asks under the fetch state's `currentLeaderEpoch`: the epoch of
    // the leader this task replicates from.
    let our_epoch = cfg.leader_epoch.0;
    recover_offset_out_of_range(cfg, |timestamp| {
        leader_list_offset(cfg, our_epoch, timestamp)
    })
    .await
}

/// [`handle_offset_out_of_range`] over an injectable `ListOffsets` lookup,
/// which maps a sentinel timestamp to the leader's offset for it.
async fn recover_offset_out_of_range<F, Fut>(cfg: &Config, mut leader_offset: F) -> RowAction
where
    F: FnMut(i64) -> Fut,
    Fut: std::future::Future<Output = Result<i64, String>>,
{
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping out_of_range recovery from stale target");
        return RowAction::Drop;
    }
    let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return RowAction::Continue;
    };
    let backoff = |error: String| {
        warn!(
            topic = %cfg.topic,
            partition = cfg.partition.get(),
            %error,
            "replicator: could not read the leader's offsets after out_of_range; retrying"
        );
        RowAction::Backoff(cfg.connection.replication.unexpected_error_backoff)
    };
    let replica_end = partition.log_end_offset().0;
    let leader_end = match leader_offset(LATEST_TIMESTAMP).await {
        Ok(offset) => offset,
        Err(error) => return backoff(error),
    };
    let (target, reset) = match recovery_for_leader_end(replica_end, leader_end) {
        LeaderEndRecovery::TruncateTo(leader_end) => (leader_end, false),
        LeaderEndRecovery::AskLeaderStart => {
            let leader_start = match leader_offset(EARLIEST_TIMESTAMP).await {
                Ok(offset) => offset,
                Err(error) => return backoff(error),
            };
            let Some(leader_start) = reset_for_leader_start(replica_end, leader_start) else {
                info!(
                    topic = %cfg.topic,
                    partition = cfg.partition.get(),
                    replica_end,
                    leader_start,
                    leader_end,
                    "replicator.out_of_range; fetching on from the local log end"
                );
                return RowAction::Continue;
            };
            (leader_start, true)
        }
    };
    // Stale-response guard, as on every other recovery path: the metadata
    // image can have chosen another target while the lookups were in flight.
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping out_of_range recovery from stale target");
        return RowAction::Drop;
    }
    let _target_guard = lock_local_target!(
        partition,
        cfg,
        "replicator: skipping out_of_range recovery from stale local target"
    );
    warn!(
        topic = %cfg.topic,
        partition = cfg.partition.get(),
        replica_end,
        target,
        reset,
        "replicator.out_of_range; {}",
        if reset {
            "truncating fully and restarting at the leader's log start"
        } else {
            "truncating to the leader's log end"
        }
    );
    let result = if reset {
        partition.reset_to(Offset(target)).await
    } else {
        partition.truncate_to(Offset(target)).await
    };
    match result {
        Ok(()) => {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, target)
                .await;
        }
        Err(error) => {
            warn!(topic = %cfg.topic, partition = cfg.partition.get(), %error,
                target, reset, "replicator: out_of_range recovery failed");
        }
    }
    RowAction::Continue
}

/// Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP`: the sentinel
/// timestamp whose answer is the first offset the leader still holds on local
/// disk. It is the one question that separates a tiered leader's two floors
/// over the wire -- `EARLIEST` answers the global one, which on a tiered
/// partition names an offset that lives only in the archive.
const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// KIP-405: restarts this follower's log at the leader's local log start after
/// the leader answered `OFFSET_MOVED_TO_TIERED_STORAGE`.
///
/// The leader raises that code for a fetch offset in
/// `[log_start, local_log_start)`: the offset exists, but only in the archive,
/// and copying the archive back down the replication path is exactly what
/// tiered storage exists to avoid. The `Fetch` response cannot carry the
/// leader's *local* log start -- its `log_start_offset` field is the global
/// one, and Kafka adds no field for the other -- so the follower asks for it,
/// with `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)`, exactly as
/// `ReplicaFetcherThread.fetchEarliestLocalOffset` does before
/// `truncateFullyAndStartAt`.
///
/// This is a reset and not a truncation: every offset below the answer is in
/// the archive and none of it belongs on this replica's disk, and the leader
/// named this band itself.
///
/// The lookup failing is not evidence about the log, so nothing is deleted:
/// the row backs off and fetches again.
// cargo-mutants: an IO-only wrapper. Every step is inter-broker IO -- connect,
// send `ListOffsets`, then `reset_to` -- so no in-process seam distinguishes a
// mutant. The request it sends and the answer it reads are computed by
// `build_leader_offsets_request` and `leader_offset_from`, which are
// unit-tested below.
#[cfg_attr(test, mutants::skip)]
pub(super) async fn handle_offset_moved_to_tiered_storage(cfg: &Config) -> RowAction {
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping tiered-storage restart from stale target");
        return RowAction::Drop;
    }
    let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return RowAction::Continue;
    };
    let our_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(partition);

    let local_log_start = match leader_list_offset(cfg, our_epoch, EARLIEST_LOCAL_TIMESTAMP).await {
        Ok(offset) => offset,
        Err(error) => {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                %error,
                "replicator: could not read the leader's local log start; retrying"
            );
            return RowAction::Backoff(cfg.connection.replication.unexpected_error_backoff);
        }
    };

    let Some(partition) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return RowAction::Continue;
    };
    // Stale-response guard, as on every other recovery path: the metadata
    // image can have chosen another target while the lookup was in flight.
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping tiered-storage restart from stale target");
        return RowAction::Drop;
    }
    let _target_guard = lock_local_target!(
        partition,
        cfg,
        "replicator: skipping tiered-storage restart from stale local target"
    );
    match partition.reset_to(Offset(local_log_start)).await {
        Ok(()) => {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, local_log_start)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                local_log_start,
                "replicator: offset moved to tiered storage; restarting at the leader's \
                 local log start"
            );
        }
        Err(error) => {
            warn!(error = %error, "replicator: reset_to(leader local log start) failed");
        }
    }
    RowAction::Continue
}

/// Dial with the partition's unchanged identity and outbound policy, before its RPC.
async fn leader_connection(cfg: &Config) -> Result<Connection, InterBrokerError> {
    let opts = connection_options(&cfg.client_id);
    cfg.connection
        .inter_broker_client
        .connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.connection.inter_broker_listener_protocol,
            &cfg.connection.inter_broker_server_name,
            opts,
        )
        .await
}

/// Asks the leader for its offset at a `ListOffsets` sentinel `timestamp`,
/// Kafka's `RemoteLeaderEndPoint.fetchOffset`.
async fn leader_list_offset(cfg: &Config, our_epoch: i32, timestamp: i64) -> Result<i64, String> {
    let client = leader_connection(cfg)
        .await
        .map_err(|e| format!("list_offsets({timestamp}): connect: {e}"))?;
    let response = client
        .send(build_leader_offsets_request(cfg, our_epoch, timestamp))
        .await
        .map_err(|e| format!("list_offsets({timestamp}): send: {e}"))?;
    leader_offset_from(&response, &cfg.topic, cfg.partition.get())
}

/// The `ListOffsets` a follower sends to learn one of the leader's offsets.
///
/// `replica_id` carries this broker's id, as every inter-broker `ListOffsets`
/// does, so the leader answers LATEST with its log end rather than its high
/// watermark, and `current_leader_epoch` carries the epoch this follower is
/// replicating under so the leader can fence a stale question (KIP-320).
fn build_leader_offsets_request(
    cfg: &Config,
    our_epoch: i32,
    timestamp: i64,
) -> ListOffsetsRequest {
    ListOffsetsRequest {
        replica_id: i32::try_from(cfg.node_id.0).unwrap_or(-1),
        topics: vec![ListOffsetsTopic {
            name: cfg.topic.to_string(),
            partitions: vec![ListOffsetsPartition {
                partition_index: cfg.partition.get(),
                current_leader_epoch: our_epoch,
                timestamp,
                ..ListOffsetsPartition::default()
            }],
            ..ListOffsetsTopic::default()
        }],
        timeout_ms: 0,
        ..ListOffsetsRequest::default()
    }
}

/// Reads the answer for one partition out of a `ListOffsets` response.
///
/// A row the leader answered with an error, a row it did not answer at all,
/// and a negative offset are all failures rather than a truncation point: none
/// of them says where this follower's log should begin or end, and acting on
/// one would delete a log on no evidence.
fn leader_offset_from(
    response: &ListOffsetsResponse,
    topic: &str,
    partition: i32,
) -> Result<i64, String> {
    let Some(row) = response
        .topics
        .iter()
        .find(|t| t.name == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition_index == partition))
    else {
        return Err("list_offsets: the leader answered no row for this partition".into());
    };
    if row.error_code != codes::NONE {
        return Err(format!("list_offsets: error {}", row.error_code));
    }
    if row.offset < 0 {
        return Err(format!(
            "list_offsets: the leader reported offset {}",
            row.offset
        ));
    }
    Ok(row.offset)
}

/// Aligns the local log with the epoch history of the leader after an epoch
/// fence.
///
/// On `FENCED_LEADER_EPOCH`, this function calls `OffsetForLeaderEpoch`
/// against the leader to find the truncation point. It then truncates the local
/// log to that point.
///
/// KIP-101: the follower sends its current `leader_epoch`. The leader replies
/// with `end_offset`, the first offset of the next epoch, which is the safe
/// truncation point.
// cargo-mutants: an I/O-only wrapper with no in-process signal. Every step is
// inter-broker IO -- connect, send `OffsetForLeaderEpoch`, then `part.truncate_to`
// / `part.reset_to` -- and the `end_offset >= 0` truncate-vs-reset branch is only
// reachable once a live leader connection has answered, so no in-process seam
// distinguishes a mutant. The truncation point itself is computed by
// `truncation_offset`, which is mutation-tested.
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    name = "replicator_handle_epoch_fence",
    level = "info",
    skip_all,
    fields(topic = %cfg.topic, partition = cfg.partition.get()),
    err,
)]
pub(super) async fn handle_epoch_fence(cfg: &Config) -> Result<(), String> {
    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };
    let our_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    drop(part);

    let client = leader_connection(cfg)
        .await
        .map_err(|e| format!("handle_epoch_fence: connect: {e}"))?;

    let req = build_offset_for_leader_epoch_request(cfg, our_epoch);

    let resp = client
        .send(req)
        .await
        .map_err(|e| format!("handle_epoch_fence: send: {e}"))?;

    // Find our (topic, partition) in the response.
    let Some(epoch_result) = resp
        .topics
        .iter()
        .find(|t| t.topic.as_str() == &*cfg.topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == cfg.partition))
    else {
        return Ok(());
    };
    if epoch_result.error_code != codes::NONE {
        return Err(format!(
            "handle_epoch_fence: OffsetForLeaderEpoch error {}",
            epoch_result.error_code
        ));
    }
    let end_offset = epoch_result.end_offset;

    let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) else {
        return Ok(());
    };

    // Stale-response guard: never truncate/reset from an OffsetForLeaderEpoch
    // response if metadata has since selected another target (see
    // `replication_target_changed`).
    if replication_target_changed(cfg) {
        warn!(topic = %cfg.topic, partition = cfg.partition.get(),
            "replicator: skipping epoch-fence truncation from stale target");
        return Ok(());
    }

    let _target_guard = part
        .lock_replication_target(task_replication_target(cfg))
        .await
        .map_err(|error| format!("handle_epoch_fence: stale local target: {error}"))?;

    if end_offset >= 0 {
        // Truncate to the epoch boundary. Wrap the wire `i64` into `Offset`.
        if let Err(e) = part.truncate_to(Offset(end_offset)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                error = %e,
                "handle_epoch_fence: truncate_to failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, end_offset)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                end_offset,
                "handle_epoch_fence: truncated to epoch boundary"
            );
        }
    } else {
        // end_offset == -1 (UNDEFINED_OFFSET): no epoch info available;
        // reset to 0 as a safe fallback.
        if let Err(e) = part.reset_to(Offset(0)).await {
            warn!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                error = %e,
                "handle_epoch_fence: reset_to(0) failed"
            );
        } else {
            cfg.producer_state
                .truncate(&cfg.topic, cfg.partition, 0)
                .await;
            info!(
                topic = %cfg.topic,
                partition = cfg.partition.get(),
                "handle_epoch_fence: reset to 0 (undefined epoch boundary)"
            );
        }
    }

    Ok(())
}

fn build_offset_for_leader_epoch_request(
    cfg: &Config,
    our_epoch: i32,
) -> OffsetForLeaderEpochRequest {
    OffsetForLeaderEpochRequest {
        replica_id: i32::try_from(cfg.node_id.0).unwrap_or(-1),
        topics: vec![OffsetForLeaderTopic {
            topic: cfg.topic.to_string(),
            partitions: vec![OffsetForLeaderPartition {
                partition: cfg.partition.get(),
                current_leader_epoch: our_epoch,
                leader_epoch: our_epoch,
                ..OffsetForLeaderPartition::default()
            }],
            ..OffsetForLeaderTopic::default()
        }],
        ..OffsetForLeaderEpochRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_raft::NodeId;

    use super::*;
    use crate::replicator::{
        ensure_local_partition,
        test_support::{LEADER_ID, NODE_ID, PARTITION, TOPIC, image_with_leader, test_config},
    };

    /// One row per branch of `getOffsetTruncationState`.
    #[test]
    fn diverging_truncation_offset_follows_kafka_get_offset_truncation_state() {
        let unplaced = (LeaderEpoch(-1), Offset(-1));
        // (name, this replica's endOffsetFor, leader epoch, leader end, log end,
        // expected offset)
        for (name, follower, leader_epoch, leader_end, log_end, expected) in [
            (
                "the leader's end offset is undefined",
                (LeaderEpoch(4), Offset(8)),
                4,
                -1,
                9,
                9,
            ),
            (
                "the local epochs cannot place the epoch",
                unplaced,
                6,
                12,
                15,
                12,
            ),
            (
                "the local epochs cannot place the epoch, past the log end",
                unplaced,
                6,
                20,
                15,
                15,
            ),
            (
                "the epoch is unknown here: only the end of the smaller epoch",
                (LeaderEpoch(4), Offset(10)),
                5,
                12,
                15,
                10,
            ),
            (
                "the epoch is unknown here, and that end is past the log end",
                (LeaderEpoch(4), Offset(10)),
                5,
                12,
                8,
                8,
            ),
            (
                "a known epoch: the leader ends first",
                (LeaderEpoch(5), Offset(12)),
                5,
                10,
                15,
                10,
            ),
            (
                "a known epoch: this replica ends first",
                (LeaderEpoch(5), Offset(10)),
                5,
                12,
                15,
                10,
            ),
            (
                "a known epoch: the log ends first",
                (LeaderEpoch(5), Offset(12)),
                5,
                14,
                11,
                11,
            ),
        ] {
            assert!(
                diverging_truncation_offset(
                    follower,
                    LeaderEpoch(leader_epoch),
                    Offset(leader_end),
                    Offset(log_end),
                ) == Offset(expected),
                "{name}"
            );
        }
    }

    #[test]
    fn offset_epoch_request_and_connection_options_preserve_identity_fields() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let opts = connection_options(&cfg.client_id);
        assert!(opts.client_id == "replica-test");

        let req = build_offset_for_leader_epoch_request(&cfg, 7);
        let expected = OffsetForLeaderEpochRequest {
            replica_id: i32::try_from(NODE_ID.0).unwrap(),
            topics: vec![OffsetForLeaderTopic {
                topic: TOPIC.into(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: PARTITION,
                    current_leader_epoch: 7,
                    leader_epoch: 7,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(req == expected);
    }

    #[test]
    fn offset_epoch_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_offset_for_leader_epoch_request(&cfg, 7);

        assert!(req.replica_id == -1);
    }

    /// Every offset a follower asks its leader for: LATEST and EARLIEST for
    /// `fetchOffsetAndTruncate`, and `EARLIEST_LOCAL`, the one question that
    /// separates a tiered leader's two floors over the wire. `EARLIEST`
    /// answers the global log start, which on a tiered partition names an
    /// offset that lives only in the archive.
    #[test]
    fn leader_offsets_request_names_this_replica_and_the_sentinel() {
        use krabka_protocol::owned::list_offsets_request::{
            ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
        };

        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));

        for (sentinel, timestamp) in [
            (LATEST_TIMESTAMP, -1),
            (EARLIEST_TIMESTAMP, -2),
            (EARLIEST_LOCAL_TIMESTAMP, -4),
        ] {
            let req = build_leader_offsets_request(&cfg, 7, sentinel);

            let expected = ListOffsetsRequest {
                replica_id: i32::try_from(NODE_ID.0).unwrap(),
                isolation_level: 0,
                topics: vec![ListOffsetsTopic {
                    name: TOPIC.into(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: PARTITION,
                        current_leader_epoch: 7,
                        timestamp,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                    }],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                }],
                timeout_ms: 0,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            };
            assert!(req == expected, "timestamp {timestamp}");
        }
    }

    #[test]
    fn leader_offsets_request_uses_negative_replica_sentinel_when_node_id_overflows() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);

        let req = build_leader_offsets_request(&cfg, 7, EARLIEST_LOCAL_TIMESTAMP);

        assert!(req.replica_id == -1);
    }

    /// Only a row this leader answered, for this partition, without an error
    /// and with a real offset, says where the log should begin. Everything
    /// else is a failure: acting on one would delete a follower's log on no
    /// evidence at all.
    #[test]
    fn only_a_clean_row_for_this_partition_names_a_restart_point() {
        use krabka_protocol::owned::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        };

        let row = |partition_index, error_code, offset| ListOffsetsPartitionResponse {
            partition_index,
            error_code,
            timestamp: -1,
            offset,
            leader_epoch: 4,
            ..ListOffsetsPartitionResponse::default()
        };
        let response = |name: &str, partitions| ListOffsetsResponse {
            topics: vec![ListOffsetsTopicResponse {
                name: name.into(),
                partitions,
                ..ListOffsetsTopicResponse::default()
            }],
            ..ListOffsetsResponse::default()
        };

        let answered = response(TOPIC, vec![row(PARTITION, codes::NONE, 4_096)]);
        assert!(leader_offset_from(&answered, TOPIC, PARTITION) == Ok(4_096));

        for (what, resp) in [
            (
                "another topic",
                response("other", vec![row(PARTITION, codes::NONE, 4_096)]),
            ),
            (
                "another partition",
                response(TOPIC, vec![row(PARTITION + 1, codes::NONE, 4_096)]),
            ),
            ("no row at all", response(TOPIC, Vec::new())),
            (
                "an error row",
                response(
                    TOPIC,
                    vec![row(PARTITION, codes::UNKNOWN_LEADER_EPOCH, 4_096)],
                ),
            ),
            (
                "an undefined offset",
                response(TOPIC, vec![row(PARTITION, codes::NONE, -1)]),
            ),
        ] {
            assert!(
                leader_offset_from(&resp, TOPIC, PARTITION).is_err(),
                "{what} named a restart point"
            );
        }
    }

    fn unreachable_local_config() -> (Config, tempfile::TempDir) {
        let (mut cfg, log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        cfg.leader_port = listener.local_addr().unwrap().port();
        drop(listener);
        (cfg, log_dir)
    }

    /// A lookup that never reached the leader says nothing about the log, so
    /// nothing is deleted: the row backs off and asks again. This is the case
    /// that separates the tiered restart from a reset on a transient failure.
    #[tokio::test]
    async fn a_failed_earliest_local_lookup_backs_off_without_touching_the_log() {
        let (cfg, _log_dir) = unreachable_local_config();
        let before = cfg
            .partitions
            .get(&cfg.topic, cfg.partition)
            .expect("local partition")
            .log_end_offset();

        let action = handle_offset_moved_to_tiered_storage(&cfg).await;

        assert!(action == RowAction::Backoff(cfg.connection.replication.unexpected_error_backoff));
        let after = cfg
            .partitions
            .get(&cfg.topic, cfg.partition)
            .expect("local partition")
            .log_end_offset();
        assert!(after == before);
    }

    /// What one out-of-range recovery did: the row action, the `ListOffsets`
    /// sentinels it asked the leader for in order, and this replica's log
    /// bounds afterwards.
    #[derive(Debug, PartialEq)]
    struct Recovery {
        action: RowAction,
        asked: Vec<i64>,
        log_start: i64,
        log_end: i64,
    }

    /// Kafka's `AbstractFetcherThread.fetchOffsetAndTruncate`, over a
    /// replica holding offsets 0..6 and a leader scripted per sentinel.
    #[tokio::test]
    async fn out_of_range_recovery_follows_kafka_fetch_offset_and_truncate() {
        let backoff = RowAction::Backoff(
            crate::config::ReplicationRuntimeConfig::default().unexpected_error_backoff,
        );
        let lookup_failed = || Err::<i64, _>("scripted lookup failure".to_owned());
        let recovery = |action, asked: &[i64], log_start, log_end| Recovery {
            action,
            asked: asked.to_vec(),
            log_start,
            log_end,
        };
        // (name, leader LATEST, leader EARLIEST, expected recovery)
        let cases = [
            (
                "unclean election: the leader ends below this replica",
                Ok(4),
                Ok(0),
                recovery(RowAction::Continue, &[-1], 0, 4),
            ),
            (
                "the leader's log start is above this replica's end",
                Ok(20),
                Ok(12),
                recovery(RowAction::Continue, &[-1, -2], 12, 12),
            ),
            (
                "the leader's log start is inside this replica's log",
                Ok(20),
                Ok(3),
                recovery(RowAction::Continue, &[-1, -2], 0, 6),
            ),
            (
                "the leader's log start is this replica's end",
                Ok(20),
                Ok(6),
                recovery(RowAction::Continue, &[-1, -2], 0, 6),
            ),
            (
                "the leader ends where this replica ends",
                Ok(6),
                Ok(0),
                recovery(RowAction::Continue, &[-1, -2], 0, 6),
            ),
            (
                "the log end lookup fails",
                lookup_failed(),
                Ok(0),
                recovery(backoff, &[-1], 0, 6),
            ),
            (
                "the log start lookup fails",
                Ok(20),
                lookup_failed(),
                recovery(backoff, &[-1, -2], 0, 6),
            ),
        ];
        for (name, latest, earliest, expected) in cases {
            let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
            ensure_local_partition(&cfg).unwrap();
            let partition = cfg.partitions.get(&cfg.topic, cfg.partition).unwrap();
            {
                let mut log = partition.log.lock().unwrap();
                for _ in 0..6 {
                    let mut batch = krabka_protocol::records::RecordBatch {
                        partition_leader_epoch: 4,
                        records: vec![krabka_protocol::records::Record::default()],
                        ..Default::default()
                    };
                    log.append(&mut batch).unwrap();
                }
            }
            let asked = std::cell::RefCell::new(Vec::new());

            let action = recover_offset_out_of_range(&cfg, |timestamp| {
                asked.borrow_mut().push(timestamp);
                let answer = match timestamp {
                    LATEST_TIMESTAMP => latest.clone(),
                    EARLIEST_TIMESTAMP => earliest.clone(),
                    other => Err(format!("unexpected sentinel {other}")),
                };
                std::future::ready(answer)
            })
            .await;

            let got = Recovery {
                action,
                asked: asked.into_inner(),
                log_start: partition.log_start_offset().0,
                log_end: partition.log_end_offset().0,
            };
            assert!(got == expected, "{name}");
        }
    }

    /// A recovery for a leader the metadata image no longer names asks the
    /// old leader nothing and touches nothing.
    #[tokio::test]
    async fn out_of_range_recovery_from_a_stale_target_asks_nothing() {
        let (cfg, _log_dir) = test_config(image_with_leader(NodeId(99)));
        let mut asked = Vec::new();

        let action = recover_offset_out_of_range(&cfg, |timestamp| {
            asked.push(timestamp);
            std::future::ready(Ok(0))
        })
        .await;

        assert!(action == RowAction::Drop);
        assert!(asked.is_empty());
    }

    #[tokio::test]
    async fn handle_epoch_fence_surfaces_connection_failure_for_local_partition() {
        let (cfg, _log_dir) = unreachable_local_config();

        let err = handle_epoch_fence(&cfg).await.unwrap_err();

        assert!(
            err.contains("handle_epoch_fence: connect"),
            "unexpected error: {err}"
        );
    }
}
