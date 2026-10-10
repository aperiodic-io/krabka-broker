//! The follower fetch loop.
//!
//! A full or metadata-changed round snapshots every followed partition. Later
//! KIP-227 rounds replan only rows named by the leader's last incremental
//! response, then apply only the rows the next response carries. The loop
//! reconnects on transport failure and returns when the fetcher is cancelled.
//!
//! A round costs one request and one response however many partitions the
//! fetcher follows, and after the first round the request names only the
//! partitions whose desired state moved.

use std::{collections::BTreeMap, sync::Arc};

use krabka_client_core::{ClientError, Connection};
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, ReplicaState},
    fetch_response::FetchResponse,
};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt},
};
use tracing::{info, warn};

use super::{
    Config, FetcherConfig, FollowedKey,
    connection::{connect_with_backoff, sleep_or_cancel},
    ensure_local_partition,
    follower_throttle::{FetchThrottleDecision, follower_partition_fetch_cap},
    raw_fetch::RawFetchRequest,
    replication_target_changed,
    response::{FollowedIndex, RoundEntry, RowAction, apply_response_indexed},
    session::{FollowerFetchSession, SessionKey, SessionOutcome, WantedRows},
};

/// When each partition may next be asked about.
///
/// A leader that answers `NOT_LEADER_OR_FOLLOWER`, or a response fenced
/// against a target this follower no longer has, means the fetcher and the
/// metadata image disagree about who leads the partition. Removing it from the
/// fetcher would not settle that: the supervisor owns the membership and puts
/// the partition straight back on the next reconcile, so the pair would churn
/// -- rebuilding the partition's configuration and re-asking the leader -- for
/// as long as the disagreement lasts. Holding it back instead leaves the
/// membership where it belongs and stops the churn.
type DelayedUntil = std::collections::HashMap<FollowedKey, tokio::time::Instant>;

/// Catch out-of-band local log mutations without putting a full partition
/// scan back into every incremental Fetch round.
const PARTITION_STATE_AUDIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// One round's plan: what goes on the wire, and which partition each row
/// belongs to once the answer comes back.
struct Round {
    wanted: WantedRows,
    entries: Vec<RoundEntry>,
    keys: Vec<FollowedKey>,
    /// Every partition wanted a fetch this round but no budget was left for
    /// any of them, so the round has nothing to send and should wait for the
    /// throttle rather than spin.
    all_throttled: bool,
}

pub(super) async fn run_fetcher_loop(fetcher: &FetcherConfig) -> Result<(), String> {
    let replication = &fetcher.connection.replication;
    let mut session = FollowerFetchSession::default();
    // The partitions whose on-disk log this fetcher has already opened. A
    // reconcile adds a partition to a running fetcher, and the first round
    // that sees it is what opens it; re-checking every partition every round
    // would put the registry lookup back on the per-partition path this
    // batching exists to leave.
    let mut materialized: std::collections::HashSet<FollowedKey> = std::collections::HashSet::new();
    let mut followed = BTreeMap::new();
    let mut followed_generation = 0;
    let mut followed_index = FollowedIndex::new(&followed);
    let mut dirty = std::collections::BTreeSet::new();
    let mut request_epochs = std::collections::HashMap::new();
    let mut forgotten = Vec::new();
    let mut delayed: DelayedUntil = DelayedUntil::new();
    let mut next_partition_audit = tokio::time::Instant::now() + PARTITION_STATE_AUDIT_INTERVAL;
    // The leader connection, dialled lazily. Opening the local logs must not
    // wait behind the dial: a follower whose leader is not accepting yet still
    // has to have its replicas on disk, because the ISR report, a later
    // election and a consumer's metadata are all keyed off the partition
    // existing. One task per partition used to guarantee that by construction,
    // since each dialled only after opening its own log.
    let mut client: Option<Connection> = None;

    loop {
        if fetcher.shutdown.is_cancelled() {
            return Ok(());
        }

        if let Some((generation, snapshot)) =
            fetcher.followed.snapshot_if_changed(followed_generation)
        {
            followed_generation = generation;
            followed = snapshot;
            followed_index = FollowedIndex::new(&followed);
            dirty = followed.keys().cloned().collect();
            request_epochs.retain(|key, _| followed.contains_key(key));
            forgotten.clear();
            session.reset();
            materialized.retain(|key| followed.contains_key(key));
            for (key, cfg) in &followed {
                if materialized.contains(key) {
                    continue;
                }
                if let Err(error) = ensure_local_partition(cfg) {
                    warn!(error = %error, topic = %cfg.topic, partition = cfg.partition.get(),
                        "replicator failed to open local partition; not following it");
                    drop_partition(fetcher, key);
                    continue;
                }
                materialized.insert(key.clone());
            }
        }

        let now = tokio::time::Instant::now();
        if now >= next_partition_audit {
            dirty.extend(followed.keys().cloned());
            next_partition_audit = now + PARTITION_STATE_AUDIT_INTERVAL;
        }
        dirty.extend(
            delayed
                .iter()
                .filter(|(_, until)| **until <= now)
                .map(|(key, _)| key.clone()),
        );
        delayed.retain(|key, until| {
            followed.contains_key(key) && *until > tokio::time::Instant::now()
        });
        let round = plan_changed(&followed, &delayed, &dirty);
        if round.entries.is_empty()
            && (session.is_full() || followed.is_empty() || round.all_throttled)
        {
            // Nothing to ask for: either this fetcher follows nothing yet, or
            // every partition it follows is out of throttle budget. Wait, and
            // let the next reconcile or the next refill decide.
            let idle = if round.all_throttled {
                replication.throttle_exhausted_backoff
            } else {
                replication.fetch_max_wait
            };
            if sleep_or_cancel(&fetcher.shutdown, idle).await.is_err() {
                return Ok(());
            }
            continue;
        }

        let connection = match client {
            Some(ref mut connection) => connection,
            None => client.insert(connect_with_backoff(fetcher).await?),
        };
        for (key, entry) in round.keys.iter().zip(&round.entries) {
            request_epochs.insert(key.clone(), entry.request_leader_epoch);
        }
        let removed = std::mem::take(&mut forgotten);
        let request = RawFetchRequest(build_fetch_request(
            fetcher,
            &mut session,
            round.wanted,
            &removed,
        ));
        let send = tokio::select! {
            () = fetcher.shutdown.cancelled() => return Ok(()),
            r = connection.send(request) => r,
        };

        let mut resp: FetchResponse = match send {
            Ok(r) => r.0,
            // Transport / framing failure: the leader may or may not have seen
            // the request, so what it holds for this session is unknowable.
            // Drop the session with the connection; the next round is full.
            Err(ClientError::Disconnected | ClientError::Io(_)) => {
                client = None;
                session.reset();
                dirty = followed.keys().cloned().collect();
                continue;
            }
            Err(e) => {
                warn!(error = %e,
                    "replicator: client.send unexpected error; retrying after backoff");
                client = None;
                session.reset();
                dirty = followed.keys().cloned().collect();
                if sleep_or_cancel(&fetcher.shutdown, replication.send_error_backoff)
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                continue;
            }
        };

        if session.handle_response(resp.error_code, resp.session_id) == SessionOutcome::SessionLost
        {
            dirty = followed.keys().cloned().collect();
            info!(
                leader_node_id = fetcher.leader_node_id.0,
                error_code = resp.error_code,
                "replicator: leader dropped the fetch session; re-opening it"
            );
            continue;
        }

        let actions = apply_response_indexed(&mut resp, &followed_index, &request_epochs).await;
        for key in &round.keys {
            dirty.remove(key);
        }
        let mut backoff: Option<Time> = None;
        for (key, action) in actions {
            match action {
                RowAction::Continue => {
                    dirty.insert(key);
                }
                RowAction::Drop => {
                    dirty.remove(&key);
                    info!(topic = %key.0, partition = key.1.get(),
                        "replicator.not_leader; holding the partition back until \
                         the metadata image settles");
                    delayed.insert(
                        key.clone(),
                        tokio::time::Instant::now() + replication.epoch_fence_backoff.to_std(),
                    );
                    if let Some(cfg) = followed.get(&key) {
                        forgotten.push(session_key(cfg));
                    }
                    // The leader's cached set still holds it; the next round
                    // forgets it, which is what the session handler does with
                    // a key that left the wanted set.
                }
                RowAction::Backoff(delay) => {
                    dirty.insert(key);
                    backoff = Some(match backoff {
                        Some(current) if current > delay => current,
                        _ => delay,
                    });
                }
                RowAction::Offline => {
                    // The next round sees the smaller membership, re-opens the
                    // session without the partition, and the supervisor does
                    // not hand it back while its log directory is offline.
                    dirty.remove(&key);
                    warn!(topic = %key.0, partition = key.1.get(),
                        "replicator: log directory offline; no longer following the partition");
                    drop_partition(fetcher, &key);
                }
            }
        }
        if session.is_full() {
            dirty = followed.keys().cloned().collect();
        }
        if let Some(delay) = backoff
            && sleep_or_cancel(&fetcher.shutdown, delay).await.is_err()
        {
            return Ok(());
        }
    }
}

/// Stops following one partition. The supervisor re-adds it if the next image
/// still says this broker follows it from this leader.
fn drop_partition(fetcher: &FetcherConfig, key: &FollowedKey) {
    fetcher.followed.remove(key);
}

/// Decides what this round asks for, partition by partition.
///
/// A partition whose replication target has moved is skipped rather than
/// dropped: the supervisor owns that decision, and the next reconcile makes
/// it. A partition with no throttle budget left is skipped for this round
/// only, which is how Kafka's follower quota holds one partition back without
/// holding back the ones sharing its fetcher. A partition the leader disowned
/// is held back by `delayed` for the same reason.
#[cfg(test)]
fn plan_round(followed: &BTreeMap<FollowedKey, Arc<Config>>, delayed: &DelayedUntil) -> Round {
    let dirty = followed.keys().cloned().collect();
    plan_changed(followed, delayed, &dirty)
}

fn plan_changed(
    followed: &BTreeMap<FollowedKey, Arc<Config>>,
    delayed: &DelayedUntil,
    dirty: &std::collections::BTreeSet<FollowedKey>,
) -> Round {
    let mut round = Round {
        wanted: WantedRows::new(),
        entries: Vec::new(),
        keys: Vec::new(),
        all_throttled: false,
    };
    let mut wanted_a_fetch = 0_usize;
    let mut throttled_out = 0_usize;
    for key in dirty {
        let Some(cfg) = followed.get(key) else {
            continue;
        };
        if delayed.contains_key(key) || replication_target_changed(cfg) {
            continue;
        }
        wanted_a_fetch += 1;
        let partition_max_cap = match follower_partition_fetch_cap(cfg) {
            FetchThrottleDecision::Fetch(cap) => cap,
            FetchThrottleDecision::Sleep => {
                tracing::debug!(
                    topic = %cfg.topic,
                    partition = cfg.partition.get(),
                    "follower throttle: skip fetch this round (bucket exhausted)"
                );
                throttled_out += 1;
                continue;
            }
        };
        let Some(row) = partition_row(cfg, partition_max_cap) else {
            continue;
        };
        round.wanted.insert(session_key(cfg), row.request);
        round.entries.push(RoundEntry {
            request_leader_epoch: row.request_leader_epoch,
        });
        round.keys.push(key.clone());
    }
    round.all_throttled = throttled_out > 0 && throttled_out == wanted_a_fetch;
    round
}

fn session_key(cfg: &Config) -> SessionKey {
    SessionKey {
        topic: cfg.topic.to_string(),
        topic_id: cfg.topic_id,
        partition: cfg.partition.get(),
    }
}

/// One partition's request row, and the epoch its response is fenced against.
struct PartitionRow {
    request: FetchPartition,
    request_leader_epoch: i32,
}

/// Builds the `FetchPartition` for one partition of this round.
///
/// KIP-101: the row carries `current_leader_epoch`, so the leader can detect a
/// stale or fenced replica and answer `FENCED_LEADER_EPOCH` or
/// `UNKNOWN_LEADER_EPOCH`. KIP-320: it carries `last_fetched_epoch`, the epoch
/// of the follower's last appended record, so the leader can report divergence
/// in-band.
///
/// `partition_max_cap` is the KIP-73 follower-throttle budget. An unthrottled
/// partition gets the configured fetch maximum.
fn partition_row(cfg: &Config, partition_max_cap: ByteSize) -> Option<PartitionRow> {
    let entry = cfg.partitions.get(&cfg.topic, cfg.partition)?;
    let fetch_offset = entry.log_end_offset();
    let leader_epoch = entry
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let last_fetched_epoch = {
        let log = entry.log.lock().expect("log mutex poisoned");
        log.epoch_checkpoint()
            .latest_epoch()
            // Unwrap the log-layer `LeaderEpoch` into the raw wire field.
            .map_or(-1, |epoch| epoch.0)
    };
    Some(PartitionRow {
        request: FetchPartition {
            partition: cfg.partition.get(),
            // Unwrap the `Offset` into the wire `i64` field.
            fetch_offset: fetch_offset.0,
            current_leader_epoch: leader_epoch,
            last_fetched_epoch,
            // The leader's `DeleteRecords` waits until every live follower
            // reports a log start at or above the trim point.
            log_start_offset: entry.log_start_offset().0,
            partition_max_bytes: partition_max_cap.bytes_i32(),
            ..FetchPartition::default()
        },
        request_leader_epoch: leader_epoch,
    })
}

/// Wraps one round's rows in the request envelope the leader reads.
///
/// `replica_id` holds the local broker, so the leader treats the request as a
/// follower fetch and not as a consumer fetch. The high-watermark semantics of
/// Kafka differ between the two.
fn build_fetch_request(
    fetcher: &FetcherConfig,
    session: &mut FollowerFetchSession,
    wanted: WantedRows,
    forgotten: &[SessionKey],
) -> FetchRequest {
    let replication = &fetcher.connection.replication;
    let session_request = if session.is_full() {
        session.build(wanted)
    } else {
        session.build_changes(wanted, forgotten)
    };
    // `replica_id` is the wire field on Fetch v0-14. KIP-903 (Kafka 3.5) moved
    // it into a tagged `replica_state` struct on v15+; the codegen serializes
    // whichever the negotiated version requires. Populate BOTH so the request
    // is correct regardless of which version the leader negotiates.
    let rid = i32::try_from(fetcher.node_id.0).unwrap_or(-1);
    // Truncate rather than round: `max_wait_ms` is a wire field, and a
    // fractional millisecond rounded up would ask the leader to hold the Fetch
    // open past the configured budget. A negative budget means "do not wait".
    let max_wait_ms =
        i32::try_from(replication.fetch_max_wait.millis_i64_trunc().max(0)).unwrap_or(i32::MAX);
    FetchRequest {
        replica_id: rid,
        replica_state: ReplicaState {
            replica_id: rid,
            ..ReplicaState::default()
        },
        max_wait_ms,
        min_bytes: replication.fetch_min.bytes_i32(),
        max_bytes: replication.fetch_max.bytes_i32(),
        session_id: session_request.session_id,
        session_epoch: session_request.session_epoch,
        topics: session_request.topics,
        forgotten_topics_data: session_request.forgotten_topics_data,
        ..FetchRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;
    use krabka_raft::NodeId;
    use krabka_units::{bytes, millis};

    use super::*;
    use crate::replicator::{
        FollowedPartitions,
        test_support::{
            LEADER_ID, NODE_ID, PARTITION, TOPIC, WIRE_TOPIC_ID, image_with_follower_throttle,
            image_with_leader, test_config,
        },
    };

    fn followed_partition(
        cfg: Config,
    ) -> BTreeMap<(Arc<str>, krabka_ids::PartitionIndex), Arc<Config>> {
        [((Arc::clone(&cfg.topic), cfg.partition), Arc::new(cfg))]
            .into_iter()
            .collect()
    }

    /// A throttled fetcher whose follower-in bucket is empty, so every
    /// partition it follows wants a fetch this round and none may have one.
    fn throttled_out() -> (Config, tempfile::TempDir) {
        let (cfg, log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(krabka_units::bytes_per_sec(1024), bytes(0));
        (cfg, log_dir)
    }

    /// A fetcher over `followed`, dialling nowhere. Every test here drives the
    /// request-building half, which never touches the connection.
    fn test_fetcher(cfg: &Config, followed: FollowedPartitions) -> FetcherConfig {
        FetcherConfig {
            node_id: cfg.node_id,
            leader_node_id: cfg.leader_node_id,
            leader_host: cfg.leader_host.clone(),
            leader_port: cfg.leader_port,
            client_id: cfg.client_id.clone(),
            shutdown: tokio_util::sync::CancellationToken::new(),
            connection: cfg.connection.clone(),
            followed,
        }
    }

    /// One `wanted` row per partition, at `fetch_offset`, as `plan_round`
    /// would have produced it.
    fn wanted_row(
        partition: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> (SessionKey, FetchPartition) {
        (
            SessionKey {
                topic: TOPIC.to_string(),
                topic_id: WIRE_TOPIC_ID,
                partition,
            },
            FetchPartition {
                partition,
                fetch_offset,
                current_leader_epoch: -1,
                last_fetched_epoch: -1,
                partition_max_bytes: max_bytes,
                ..FetchPartition::default()
            },
        )
    }

    #[test]
    fn the_first_request_carries_every_partition_and_the_configured_envelope() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.connection.replication.fetch_max = bytes(2_345_678);
        cfg.connection.replication.fetch_max_wait = millis(321);
        cfg.connection.replication.fetch_min = bytes(17);
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();
        let wanted: WantedRows = [
            wanted_row(PARTITION, 123, 456),
            wanted_row(PARTITION + 1, 7, 456),
        ]
        .into_iter()
        .collect();

        let req = build_fetch_request(&fetcher, &mut session, wanted, &[]);

        let rid = i32::try_from(NODE_ID.0).unwrap();
        check!(req.replica_id == rid);
        check!(req.replica_state.replica_id == rid);
        check!(req.max_wait_ms == 321);
        check!(req.min_bytes == 17);
        check!(req.max_bytes == 2_345_678);
        // KIP-227: the first request opens a session rather than declaring
        // itself sessionless, which is what `(0, -1)` used to say.
        check!(req.session_id == 0);
        check!(req.session_epoch == 0);
        check!(req.forgotten_topics_data.is_empty());
        // One `FetchTopic` for both partitions, not one request each.
        check!(req.topics.len() == 1);
        check!(req.topics[0].topic.as_str() == TOPIC);
        check!(req.topics[0].topic_id == WIRE_TOPIC_ID);
        check!(
            req.topics[0]
                .partitions
                .iter()
                .map(|p| (p.partition, p.fetch_offset, p.partition_max_bytes))
                .collect::<Vec<_>>()
                == vec![(PARTITION, 123, 456), (PARTITION + 1, 7, 456)]
        );
    }

    #[test]
    fn a_later_request_names_only_the_partition_that_moved() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();
        let first: WantedRows = [
            wanted_row(PARTITION, 0, 99),
            wanted_row(PARTITION + 1, 0, 99),
        ]
        .into_iter()
        .collect();
        build_fetch_request(&fetcher, &mut session, first, &[]);
        session.handle_response(crate::codes::NONE, 77);

        let second: WantedRows = [wanted_row(PARTITION, 5, 99)].into_iter().collect();
        let req = build_fetch_request(&fetcher, &mut session, second, &[]);

        check!(req.session_id == 77);
        check!(req.session_epoch == 1);
        check!(req.topics.len() == 1);
        check!(
            req.topics[0]
                .partitions
                .iter()
                .map(|p| p.partition)
                .collect::<Vec<_>>()
                == vec![PARTITION]
        );
    }

    #[test]
    fn a_node_id_that_overflows_the_wire_field_sends_the_negative_sentinel() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.node_id = NodeId(i32::MAX as u64 + 1);
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        let mut session = FollowerFetchSession::default();

        let req = build_fetch_request(
            &fetcher,
            &mut session,
            [wanted_row(PARTITION, 0, 1)].into_iter().collect(),
            &[],
        );

        check!(req.replica_id == -1);
        check!(req.replica_state.replica_id == -1);
    }

    /// A partition whose replication target has moved is not asked about, and
    /// not dropped either: the supervisor owns that decision and makes it on
    /// the next reconcile.
    #[test]
    fn a_partition_whose_target_moved_is_skipped_for_the_round() {
        let (cfg, _log_dir) = test_config(image_with_leader(NODE_ID));
        let followed = followed_partition(cfg);

        let round = plan_round(&followed, &DelayedUntil::new());

        check!(round.entries.is_empty());
        check!(round.wanted.is_empty());
        check!(!round.all_throttled);
    }

    /// A round in which every partition wanted a fetch and the KIP-73 bucket
    /// granted none says so, which is what makes the loop wait for the refill
    /// rather than spin through an empty round at the fetch interval.
    #[test]
    fn a_round_the_throttle_emptied_reports_itself_as_throttled() {
        let (cfg, _log_dir) = throttled_out();
        let followed = followed_partition(cfg);

        let round = plan_round(&followed, &DelayedUntil::new());

        check!(round.entries.is_empty());
        check!(round.all_throttled);
    }

    /// A partition the leader disowned is held back before the throttle is
    /// consulted at all: it is not asking for bytes it was refused, it is not
    /// asking at all. The same round without the hold-back reports itself
    /// throttled, so this pins the order of the two and not merely that the
    /// round came back empty.
    #[test]
    fn a_delayed_partition_is_skipped_before_the_throttle_is_consulted() {
        let (cfg, _log_dir) = throttled_out();
        let key = (Arc::clone(&cfg.topic), cfg.partition);
        let mut followed = BTreeMap::new();
        followed.insert(key.clone(), Arc::new(cfg));
        let mut delayed = DelayedUntil::new();
        delayed.insert(
            key,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        );

        let round = plan_round(&followed, &delayed);

        check!(round.entries.is_empty());
        check!(!round.all_throttled);
    }

    /// A partition with no local runtime yet contributes no row rather than
    /// putting a bogus offset on the wire.
    #[test]
    fn a_partition_with_no_local_log_contributes_no_row() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let followed = followed_partition(cfg);

        let round = plan_round(&followed, &DelayedUntil::new());

        check!(round.entries.is_empty());
    }

    #[tokio::test]
    async fn an_incremental_round_does_not_replan_caught_up_partitions() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        ensure_local_partition(&cfg).unwrap();
        let key = (Arc::clone(&cfg.topic), cfg.partition);
        let followed = [(key, Arc::new(cfg))].into_iter().collect();

        let round = plan_changed(
            &followed,
            &DelayedUntil::new(),
            &std::collections::BTreeSet::default(),
        );

        check!(round.entries.is_empty());
        check!(round.wanted.is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_fetcher_stops_without_dialling() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let fetcher = test_fetcher(&cfg, FollowedPartitions::default());
        fetcher.shutdown.cancel();

        // The fixture's leader endpoint is `127.0.0.1:9`, the discard port,
        // so a loop that dialled would hang here rather than return.
        run_fetcher_loop(&fetcher)
            .await
            .expect("a cancelled fetcher stops cleanly");
    }

    /// Opening a follower's local log must not wait behind the dial. Every
    /// other broker-side fact about a follower replica -- the ISR report, a
    /// later election, what a consumer's metadata says -- is keyed off the
    /// partition existing, and a leader that is not accepting connections yet
    /// is the normal state during a rolling restart.
    #[tokio::test]
    async fn a_fetcher_opens_its_partitions_before_it_reaches_the_leader() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        let partitions = cfg.partitions.clone();
        let topic = Arc::clone(&cfg.topic);
        let partition = cfg.partition;
        let followed: FollowedPartitions = Arc::new(super::super::FollowedPartitionsState::new(
            [((Arc::clone(&topic), partition), Arc::new(cfg))]
                .into_iter()
                .collect(),
        ));
        // `test_config` points the fetcher at port 9, the discard port, so the
        // dial never completes and the loop cannot get past it.
        let fetcher = test_fetcher(
            &followed
                .lock()
                .expect("followed")
                .values()
                .next()
                .expect("one partition")
                .clone(),
            Arc::clone(&followed),
        );
        let shutdown = fetcher.shutdown.clone();

        let task = tokio::spawn(async move { run_fetcher_loop(&fetcher).await });
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while partitions.get(TOPIC, partition).is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the fetcher never opened its partition's log"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        shutdown.cancel();
        let _ = task.await;
    }

    /// `WireUuid` is only here to keep the import used when the assertions
    /// above change shape.
    #[test]
    fn the_fixture_topic_id_is_the_one_the_requests_carry() {
        check!(WIRE_TOPIC_ID != WireUuid::ZERO);
    }
}
