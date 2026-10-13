//! KIP-227 incremental fetch sessions: the classification the cache made
//! becomes the response session id here, and the per-partition state the
//! response just carried is snapshotted, filtered and written back.

use {
    super::request::{EffectivePartition, EffectiveTopic},
    crate::{
        broker::Broker,
        codes,
        fetch_session::{
            CachedPartitionState, FIRST_TOPIC_ID_FETCH_VERSION, FetchSessionKey,
            INVALID_SESSION_ID, SessionDecision,
        },
    },
    krabka_protocol::{
        owned::fetch_response::{AbortedTransaction, FetchableTopicResponse},
        primitives::uuid::Uuid as WireUuid,
    },
};

/// What a fetch owes the session cache once its response is final.
///
/// Kafka's `FetchContext` decides the rows a response carries before the
/// quota check, and changes the cache only in `updateAndGenerateResponseData`,
/// which a throttled fetch never reaches. The handler therefore prepares the
/// session first and commits it only for a response it sends unthrottled.
pub(super) enum SessionCommit {
    /// A sessionless fetch: nothing to cache.
    Sessionless,
    /// A full fetch that may open a session over these rows.
    NewSession(Vec<(FetchSessionKey, CachedPartitionState)>),
    /// An incremental fetch on `session_id` that sent these rows.
    Incremental {
        session_id: i32,
        sent: Vec<(FetchSessionKey, CachedPartitionState)>,
    },
}

impl SessionCommit {
    /// The session id of a throttled response. Kafka's
    /// `IncrementalFetchContext.getThrottledResponse` keeps the session, and
    /// every other context answers `INVALID_SESSION_ID`.
    pub(super) const fn throttled_session_id(&self) -> i32 {
        match self {
            Self::Incremental { session_id, .. } => *session_id,
            Self::Sessionless | Self::NewSession(_) => INVALID_SESSION_ID,
        }
    }
}

/// Decide the rows `responses` carries under the session `decision` and what
/// the cache owes the fetch. An incremental fetch drops the rows the client
/// already has.
pub(super) fn prepare_fetch_session(
    decision: &SessionDecision,
    effective_topics: &[EffectiveTopic],
    responses: &mut Vec<FetchableTopicResponse>,
) -> SessionCommit {
    match decision {
        SessionDecision::Sessionless => SessionCommit::Sessionless,
        SessionDecision::NewSession => {
            SessionCommit::NewSession(snapshot_response_state(effective_topics, responses))
        }
        SessionDecision::Incremental {
            session_id,
            partitions,
            ..
        } => {
            let cached: std::collections::HashMap<FetchSessionKey, CachedPartitionState> =
                partitions.iter().cloned().collect();
            SessionCommit::Incremental {
                session_id: *session_id,
                sent: filter_incremental_response(responses, &cached),
            }
        }
        SessionDecision::Error { .. } => unreachable!("returned above"),
    }
}

/// Write a sent response's session state into the cache and return the
/// response's session id.
pub(super) fn commit_fetch_session(
    broker: &Broker,
    commit: SessionCommit,
    (is_follower_fetch, version): (bool, i16),
    principal_name: &str,
) -> i32 {
    let session_id = match commit {
        SessionCommit::Sessionless => INVALID_SESSION_ID,
        SessionCommit::NewSession(snapshot) => broker.fetch_session_cache.try_allocate(
            is_follower_fetch,
            version >= FIRST_TOPIC_ID_FETCH_VERSION,
            principal_name.to_owned(),
            snapshot,
        ),
        SessionCommit::Incremental { session_id, sent } => {
            broker
                .fetch_session_cache
                .finalize_incremental(session_id, &sent);
            session_id
        }
    };
    refresh_fetch_session_metrics(broker);
    session_id
}

pub(super) fn refresh_fetch_session_metrics(broker: &Broker) {
    broker
        .metrics
        .incremental_fetch_sessions
        .set(i64::try_from(broker.fetch_session_cache.len()).unwrap_or(i64::MAX));
    broker.metrics.incremental_fetch_partitions_cached.set(
        i64::try_from(broker.fetch_session_cache.total_partitions_cached()).unwrap_or(i64::MAX),
    );
    let current = broker.fetch_session_cache.evictions_total();
    let previous = broker
        .metrics
        .incremental_fetch_session_evictions_total
        .get();
    if current > previous {
        broker
            .metrics
            .incremental_fetch_session_evictions_total
            .inc_by(current - previous);
    }
}

/// Walk `responses` and snapshot every `(topic, partition)` row into a
/// `CachedPartitionState`.
///
/// Each snapshot describes what the handler just emitted, in the `last_*`
/// fields. It merges that with the client's wanted state for the partition
/// from `effective`, which is `fetch_offset`, `max_bytes`, and `leader_epoch`.
/// The caller uses the result to seed a brand-new session.
fn snapshot_response_state(
    effective: &[EffectiveTopic],
    responses: &[FetchableTopicResponse],
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    use std::collections::HashMap;
    // Pre-index the desired state. Topic identity differs by wire
    // version: v ≤ 12 carries topic name and zero topic_id, v ≥ 13
    // carries topic_id and empty name. The server-side response always
    // has the resolved name *and* the id, but `effective` (built from
    // `req.topics`) may have only one or the other. Index by both so
    // lookup succeeds in either direction.
    let mut by_name: HashMap<(String, i32), &EffectivePartition> = HashMap::new();
    let mut by_id: HashMap<(WireUuid, i32), &EffectivePartition> = HashMap::new();
    for et in effective {
        for ep in &et.partitions {
            if !et.topic.is_empty() {
                by_name.insert((et.topic.clone(), ep.partition), ep);
            }
            if et.topic_id != WireUuid::ZERO {
                by_id.insert((et.topic_id, ep.partition), ep);
            }
        }
    }
    let mut out = Vec::new();
    for tr in responses {
        for p in &tr.partitions {
            let key = FetchSessionKey {
                topic_name: tr.topic.clone(),
                topic_id: tr.topic_id,
                partition: p.partition_index,
            };
            let mut state = CachedPartitionState {
                last_high_watermark: p.high_watermark,
                last_last_stable_offset: p.last_stable_offset,
                last_log_start_offset: p.log_start_offset,
                last_preferred_read_replica: p.preferred_read_replica,
                last_aborted_txns_hash: hash_aborted_transactions(p.aborted_transactions.as_ref()),
                last_error_code: p.error_code,
                ..Default::default()
            };
            let ep = by_id
                .get(&(tr.topic_id, p.partition_index))
                .or_else(|| by_name.get(&(tr.topic.clone(), p.partition_index)));
            if let Some(ep) = ep {
                state.fetch_offset = ep.fetch_offset;
                state.log_start_offset = ep.log_start_offset;
                state.max_bytes = ep.partition_max_bytes;
                state.current_leader_epoch = ep.current_leader_epoch;
                state.last_fetched_epoch = ep.last_fetched_epoch;
            }
            out.push((key, state));
        }
    }
    out
}

/// The cached set, indexed by each half of a topic's identity.
///
/// A response row carries the name the broker resolved *and* the topic id,
/// but a cache entry created by a KIP-516 request carries only the id (Fetch
/// v13 sends no names) -- and one created by an older request carries only the
/// name. Looking the row up by the whole key would miss both, so every
/// partition would look new, every response would carry the whole subscription
/// set, and `finalize_incremental` would find nothing to update. The lookup
/// therefore matches on either half, id first, exactly as the session diff
/// does when it merges a request row into the cached set.
struct CachedIndex<'cache> {
    by_id: std::collections::HashMap<
        (WireUuid, i32),
        (&'cache FetchSessionKey, &'cache CachedPartitionState),
    >,
    by_name: std::collections::HashMap<
        (&'cache str, i32),
        (&'cache FetchSessionKey, &'cache CachedPartitionState),
    >,
}

impl<'cache> CachedIndex<'cache> {
    fn of(
        cached: &'cache std::collections::HashMap<FetchSessionKey, CachedPartitionState>,
    ) -> Self {
        let mut index = Self {
            by_id: std::collections::HashMap::new(),
            by_name: std::collections::HashMap::new(),
        };
        for (key, state) in cached {
            if key.topic_id != WireUuid::ZERO {
                index
                    .by_id
                    .insert((key.topic_id, key.partition), (key, state));
            }
            if !key.topic_name.is_empty() {
                index
                    .by_name
                    .insert((key.topic_name.as_str(), key.partition), (key, state));
            }
        }
        index
    }

    fn get(
        &self,
        topic_id: WireUuid,
        topic_name: &str,
        partition: i32,
    ) -> Option<(&'cache FetchSessionKey, &'cache CachedPartitionState)> {
        self.by_id
            .get(&(topic_id, partition))
            .or_else(|| self.by_name.get(&(topic_name, partition)))
            .copied()
    }
}

/// KIP-227 incremental-response filter.
///
/// The function drops the partitions whose outgoing state matches the cached
/// `last_*` snapshot. The broker already told the client these values, and a
/// second send would waste bytes. The function returns the
/// `(key, sent_state)` list for the partitions that survived, keyed by the
/// key the cache actually holds so the caller's update lands on it. The caller
/// uses that list to update the cache's `last_*` fields to what it just
/// emitted.
fn filter_incremental_response(
    responses: &mut Vec<FetchableTopicResponse>,
    cached: &std::collections::HashMap<FetchSessionKey, CachedPartitionState>,
) -> Vec<(FetchSessionKey, CachedPartitionState)> {
    let index = CachedIndex::of(cached);
    let mut sent: Vec<(FetchSessionKey, CachedPartitionState)> = Vec::new();
    for tr in responses.iter_mut() {
        tr.partitions.retain(|p| {
            let entry = index.get(tr.topic_id, &tr.topic, p.partition_index);
            let aborted_hash = hash_aborted_transactions(p.aborted_transactions.as_ref());
            let records_present = p.records.as_ref().is_some_and(|b| b.payload_len() > 0);
            let changed = match entry {
                // Kafka's `CachedPartition.maybeUpdateResponseData` always
                // includes a partition with an error, so an incremental
                // response repeats the error on every fetch until it clears.
                // It compares `highWatermark`, `logStartOffset` and
                // `preferredReadReplica` (plus records and the error code
                // here); it does not compare `lastStableOffset` or the
                // aborted-transaction list, so those two never force a resend
                // on their own.
                Some((_, prev)) => {
                    records_present
                        || p.error_code != codes::NONE
                        || p.error_code != prev.last_error_code
                        || p.high_watermark != prev.last_high_watermark
                        || p.log_start_offset != prev.last_log_start_offset
                        || p.preferred_read_replica != prev.last_preferred_read_replica
                        || p.diverging_epoch.end_offset >= 0
                }
                // Partition not in the cached set — newly added by this
                // request. Always send it once so the client sees its
                // initial state.
                None => true,
            };
            if changed {
                let key = entry.map_or_else(
                    || FetchSessionKey {
                        topic_name: tr.topic.clone(),
                        topic_id: tr.topic_id,
                        partition: p.partition_index,
                    },
                    |(key, _)| key.clone(),
                );
                sent.push((key, CachedPartitionState {
                    last_high_watermark: p.high_watermark,
                    last_last_stable_offset: p.last_stable_offset,
                    last_log_start_offset: p.log_start_offset,
                    last_preferred_read_replica: p.preferred_read_replica,
                    last_aborted_txns_hash: aborted_hash,
                    last_error_code: p.error_code,
                    ..Default::default()
                }));
            }
            changed
        });
    }
    // Drop topics that ended up with no partitions.
    responses.retain(|tr| !tr.partitions.is_empty());
    sent
}

/// Stable hash of the aborted-transaction list for the "did anything change?"
/// comparison.
///
/// The iteration order within a single response is deterministic, because
/// `do_read` produces the list in offset order. A plain `DefaultHasher` over
/// the sequence is therefore enough.
fn hash_aborted_transactions(list: Option<&Vec<AbortedTransaction>>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match list {
        None => 0_u8.hash(&mut h),
        Some(v) => {
            1_u8.hash(&mut h);
            (v.len() as u64).hash(&mut h);
            for tx in v {
                tx.producer_id.hash(&mut h);
                tx.first_offset.hash(&mut h);
            }
        }
    }
    h.finish()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_protocol::owned::fetch_response::{EpochEndOffset, PartitionData},
    };

    fn cached_entry(
        topic_name: &str,
        topic_id: u8,
        partition: i32,
        high_watermark: i64,
    ) -> (FetchSessionKey, CachedPartitionState) {
        (
            FetchSessionKey {
                topic_name: topic_name.to_owned(),
                topic_id: WireUuid([topic_id; 16]),
                partition,
            },
            sent_state(high_watermark),
        )
    }

    /// What the broker recorded for a partition it answered with
    /// `partition_data(high_watermark)`. Deriving it from the same row keeps
    /// the "nothing changed" case honest: every field the filter compares is
    /// set from the row rather than left at whichever default happens to
    /// match.
    fn sent_state(high_watermark: i64) -> CachedPartitionState {
        let row = partition_data(high_watermark);
        CachedPartitionState {
            last_high_watermark: row.high_watermark,
            last_last_stable_offset: row.last_stable_offset,
            last_log_start_offset: row.log_start_offset,
            last_preferred_read_replica: row.preferred_read_replica,
            last_aborted_txns_hash: hash_aborted_transactions(row.aborted_transactions.as_ref()),
            last_error_code: row.error_code,
            ..CachedPartitionState::default()
        }
    }

    fn partition_data(high_watermark: i64) -> PartitionData {
        PartitionData {
            partition_index: 0,
            high_watermark,
            diverging_epoch: EpochEndOffset {
                epoch: -1,
                end_offset: -1,
                ..EpochEndOffset::default()
            },
            ..PartitionData::default()
        }
    }

    fn topic_response(topic: &str, topic_id: u8, high_watermark: i64) -> FetchableTopicResponse {
        FetchableTopicResponse {
            topic: topic.to_owned(),
            topic_id: WireUuid([topic_id; 16]),
            partitions: vec![partition_data(high_watermark)],
            ..FetchableTopicResponse::default()
        }
    }

    macro_rules! filter_single_topic {
        (
            ($cached:ident, $responses:ident, $sent:ident),($name:expr, $id:expr, $watermark:expr),
            $response_watermark:expr
        ) => {
            let $cached: std::collections::HashMap<_, _> =
                [cached_entry($name, $id, 0, $watermark)]
                    .into_iter()
                    .collect();
            let mut $responses = vec![topic_response("orders", 7, $response_watermark)];
            let $sent = filter_incremental_response(&mut $responses, &$cached);
        };
    }

    /// A Fetch v13 request names topics by id alone, so the cached key it
    /// created has no name -- while the response row the broker built carries
    /// the name it resolved. The unchanged partition still has to drop out.
    #[test]
    fn an_unchanged_partition_cached_by_id_alone_is_not_resent() {
        filter_single_topic!((cached, responses, sent), ("", 7, 42), 42);

        assert!(responses.is_empty());
        assert!(sent.is_empty());
    }

    /// The same row, changed. It is sent, and the state comes back under the
    /// key the cache holds -- not under the response's resolved name, which
    /// `finalize_incremental` would not find.
    #[test]
    fn a_changed_partition_reports_the_key_the_cache_holds() {
        filter_single_topic!((cached, responses, sent), ("", 7, 42), 43);

        assert!(
            sent == vec![(
                FetchSessionKey {
                    topic_name: String::new(),
                    topic_id: WireUuid([7; 16]),
                    partition: 0,
                },
                sent_state(43),
            )]
        );
    }

    /// A pre-KIP-516 session caches the name and no id, and the response
    /// carries both. The name half has to match on its own.
    #[test]
    fn a_partition_cached_by_name_alone_still_matches() {
        filter_single_topic!((cached, responses, sent), ("orders", 0, 42), 42);

        assert!(responses.is_empty());
        assert!(sent.is_empty());
    }

    /// Kafka's `CachedPartition.maybeUpdateResponseData` always includes a
    /// partition with an error. A row whose error and offsets match the cached
    /// state goes out again, and the cache records it again.
    #[test]
    fn an_unchanged_error_row_is_resent() {
        let row = PartitionData {
            error_code: codes::UNKNOWN_TOPIC_ID,
            ..partition_data(-1)
        };
        let state = CachedPartitionState {
            last_error_code: codes::UNKNOWN_TOPIC_ID,
            ..sent_state(-1)
        };
        let key = FetchSessionKey {
            topic_name: String::new(),
            topic_id: WireUuid([9; 16]),
            partition: 0,
        };
        let cached: std::collections::HashMap<_, _> =
            [(key.clone(), state.clone())].into_iter().collect();
        let topic = FetchableTopicResponse {
            topic: String::new(),
            topic_id: WireUuid([9; 16]),
            partitions: vec![row],
            ..FetchableTopicResponse::default()
        };
        let mut responses = vec![topic.clone()];

        let sent = filter_incremental_response(&mut responses, &cached);

        assert!((responses, sent) == (vec![topic], vec![(key, state)]));
    }

    /// Two topics cached by id alone are two partitions, not one: the filter
    /// must not answer for `b` out of `a`'s cached state.
    #[test]
    fn two_nameless_cached_topics_are_told_apart_by_id() {
        let cached: std::collections::HashMap<_, _> =
            [cached_entry("", 1, 0, 10), cached_entry("", 2, 0, 20)]
                .into_iter()
                .collect();
        let mut responses = vec![topic_response("a", 1, 10), topic_response("b", 2, 99)];

        let sent = filter_incremental_response(&mut responses, &cached);

        let surviving: Vec<(String, i64)> = responses
            .iter()
            .map(|topic| (topic.topic.clone(), topic.partitions[0].high_watermark))
            .collect();
        assert!(surviving == vec![("b".to_owned(), 99)]);
        assert!(sent.len() == 1);
    }

    /// A new session caches what the fetcher asked for, the log start offset
    /// included, so a follower that leaves an unchanged partition out of its
    /// next incremental request still counts at the log start it reported
    /// (#746).
    #[test]
    fn a_new_session_caches_the_requested_log_start_offset() {
        let effective = [EffectiveTopic {
            topic: "orders".to_owned(),
            topic_id: WireUuid([7; 16]),
            partitions: vec![EffectivePartition {
                partition: 0,
                current_leader_epoch: 3,
                last_fetched_epoch: 2,
                fetch_offset: 40,
                log_start_offset: 25,
                partition_max_bytes: 1024,
            }],
        }];

        let snapshot = snapshot_response_state(&effective, &[topic_response("orders", 7, 42)]);

        assert!(
            snapshot
                == vec![(
                    FetchSessionKey {
                        topic_name: "orders".to_owned(),
                        topic_id: WireUuid([7; 16]),
                        partition: 0,
                    },
                    CachedPartitionState {
                        fetch_offset: 40,
                        last_fetched_epoch: 2,
                        current_leader_epoch: 3,
                        max_bytes: 1024,
                        log_start_offset: 25,
                        ..sent_state(42)
                    },
                )]
        );
    }
}
