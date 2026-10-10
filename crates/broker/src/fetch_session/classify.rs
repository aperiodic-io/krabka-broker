//! Request classification: the three-way decision that the fetch handler
//! dispatches on, and the epoch check and cached-set update behind it.
//!
//! `classify` reads the `session_id` and `session_epoch` pair off the request.
//! It decides full vs. incremental from `session_epoch` alone (Kafka's
//! `FetchMetadata.isFull()`), rejects a stale epoch or an unknown id on the
//! incremental path with the KIP-227 error code, and on a valid incremental
//! fetch applies the partition-set diff and returns the full effective
//! partition set under one lock.

use std::sync::atomic::Ordering;

use krabka_protocol::owned::fetch_request::FetchRequest;
use qubit_clock::MonotonicClock as _;

use super::{
    cache::FetchSessionCache,
    diff::apply_incremental,
    epoch::{
        FINAL_EPOCH, FIRST_TOPIC_ID_FETCH_VERSION, FetchSessionEpoch, FetchSessionId,
        INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
    },
    state::{CachedPartitionState, FetchSessionKey},
};
use crate::codes;

/// Outcome of `FetchSessionCache::classify`. The handler dispatches on this
/// value before it does any read.
#[derive(Debug)]
pub enum SessionDecision {
    /// `(session_id=0, epoch=-1)`: serve from `req.topics`, with no caching,
    /// and a response `session_id = 0`.
    Sessionless,
    /// `(session_id=0, epoch=0)`: serve from `req.topics`, then ask the cache
    /// to allocate a new session for the result. The cache can refuse the
    /// allocation when it is full of privileged sessions. The response
    /// `session_id` is then `0`, and the client falls back to sessionless
    /// fetches next time.
    NewSession,
    /// `(session_id!=0, epoch>=0)` that matches the cached epoch: serve from
    /// the cached subscription set. The cache merges `req.topics` in as
    /// updates and new entries, and removes `forgotten_topics_data`. The
    /// response holds only the partitions whose state has changed.
    Incremental {
        session_id: FetchSessionId,
        /// This value is already incremented. It goes nowhere on the wire,
        /// because the response has no epoch field. The cache uses it as the
        /// *next* expected epoch for the following request.
        new_epoch: FetchSessionEpoch,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    },
    /// Protocol violation. Emit an empty response with this top-level
    /// `error_code` and `session_id = 0`.
    Error { code: i16 },
}

impl FetchSessionCache {
    /// Inspects the request and decides which of the four branches the fetch
    /// handler should take. On `Incremental` it does the following atomically:
    /// - validates the epoch,
    /// - removes `forgotten_topics_data` from the cached partition set,
    /// - merges `req.topics` into the cached set (updates `fetch_offset` etc.
    ///   on existing entries; adds new entries verbatim),
    /// - bumps `next_epoch`,
    /// - and returns the full effective partition set for the handler to
    ///   read.
    ///
    /// The handler must call `finalize_incremental` after it assembles the
    /// response, so that the `last_*` comparison fields stay in step with what
    /// the broker sent.
    ///
    /// The branch taken is decided from `epoch` alone, matching Kafka's
    /// `FetchMetadata.isFull()`. The session id carried alongside a full-fetch
    /// epoch (`INITIAL_EPOCH` or `FINAL_EPOCH`) is not looked up for a match;
    /// it names a session to close before the full read runs, if one exists.
    /// This is the ordinary reconnect path of the Java consumer
    /// (`nextCloseExistingAttemptNew`): it closes its old session by sending
    /// `INITIAL_EPOCH` with the old id, and expects a fresh session back, not
    /// an error.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn classify(&self, req: &FetchRequest, version: i16) -> SessionDecision {
        let sid = req.session_id;
        let epoch = req.session_epoch;

        if epoch == INITIAL_EPOCH || epoch == FINAL_EPOCH {
            if sid != INVALID_SESSION_ID {
                // Best-effort close: a session that does not exist (already
                // evicted, or named by a client that never had one) is not an
                // error here, exactly as Kafka's `newContext` ignores a
                // `cache.remove` that finds nothing.
                self.close(sid);
            }
            return if epoch == FINAL_EPOCH {
                SessionDecision::Sessionless
            } else {
                SessionDecision::NewSession
            };
        }

        // Not a full fetch: a genuine incremental epoch. Kafka looks the id
        // up unconditionally, `session_id == 0` included — id 0 is never
        // allocated, so an incremental naming it always misses.
        let mut guard = self.inner.lock().expect("poisoned");

        let Some(session) = guard.sessions.get_mut(&sid) else {
            return SessionDecision::Error {
                code: codes::FETCH_SESSION_ID_NOT_FOUND,
            };
        };

        if epoch != session.next_epoch {
            return SessionDecision::Error {
                code: codes::INVALID_FETCH_SESSION_EPOCH,
            };
        }

        // Kafka's `FetchManager.newContext` checks the topic-id use of the
        // session after its epoch, and before it touches the cached set.
        if session.uses_topic_ids != (version >= FIRST_TOPIC_ID_FETCH_VERSION) {
            return SessionDecision::Error {
                code: codes::FETCH_SESSION_TOPIC_ID_ERROR,
            };
        }

        // The forget + merge below add and drop partitions; snapshot the
        // count now so we can fold the net delta into `num_partitions`
        // (which backs the lock-free `total_partitions_cached()` gauge).
        let partitions_before = session.partitions.len();

        // Drop forgotten partitions then merge request topics (KIP-227). The
        // forget/merge logic — and the half-identity matching that prevents a
        // partial-identity request from shadowing a fully-resolved cached key —
        // lives in `apply_incremental`, verified by `fetch_session_model`.
        apply_incremental(
            &mut session.partitions,
            &req.forgotten_topics_data,
            &req.topics,
        );

        let partitions_after = session.partitions.len();
        if partitions_after >= partitions_before {
            self.num_partitions
                .fetch_add(partitions_after - partitions_before, Ordering::Relaxed);
        } else {
            self.num_partitions
                .fetch_sub(partitions_before - partitions_after, Ordering::Relaxed);
        }

        if partitions_after == 0 {
            // Kafka's `IncrementalFetchContext.updateAndGenerateResponseData`:
            // an incremental that forgets its last partitions and adds none
            // leaves nothing worth caching. The session is dropped and the
            // response reports no session, so the client falls back to a
            // sessionless fetch next time rather than keep re-polling an
            // empty one.
            guard.sessions.remove(&sid);
            guard.order.remove(sid);
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            return SessionDecision::Incremental {
                session_id: INVALID_SESSION_ID,
                new_epoch: next_epoch(epoch),
                partitions: Vec::new(),
            };
        }

        let new_epoch = next_epoch(session.next_epoch);
        session.next_epoch = new_epoch;

        let partitions: Vec<(FetchSessionKey, CachedPartitionState)> = session
            .partitions
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Kafka's `touch`: the session was used now, and holds the partitions
        // the merge above left it. This is the only place a live session moves
        // in the eviction index. It happens after the `session` borrow above
        // ends, because both halves of the cache live behind the one `guard`.
        guard.order.touch(
            sid,
            partitions_after,
            self.clock.now().elapsed_since_origin(),
        );

        SessionDecision::Incremental {
            session_id: sid,
            new_epoch,
            partitions,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchTopic},
        primitives::uuid::Uuid as WireUuid,
    };

    use super::*;
    use crate::fetch_session::test_support::{
        ForgottenTopicSetup, NAME_FETCH_VERSION, SessionAllocationSetup, SessionKeySetup,
        SessionRequestSetup, TopicAddressing, allocate_session, allocated_cache, error_code,
        forgotten_topic, req, seed_resolved_partition, session_key, topic,
    };

    #[test]
    fn sessionless_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(SessionRequestSetup {
            session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(FINAL_EPOCH),
            ..Default::default()
        });
        assert!(matches!(
            cache.classify(&r, NAME_FETCH_VERSION),
            SessionDecision::Sessionless
        ));
    }

    #[test]
    fn new_session_request_is_classified_correctly() {
        let cache = FetchSessionCache::new(10);
        let r = req(SessionRequestSetup {
            session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(INITIAL_EPOCH),
            topics: vec![topic("t", &[krabka_ids::PartitionIndex(0)])],
            ..Default::default()
        });
        assert!(matches!(
            cache.classify(&r, NAME_FETCH_VERSION),
            SessionDecision::NewSession
        ));
    }

    #[test]
    fn unknown_session_id_returns_not_found() {
        let cache = FetchSessionCache::new(10);
        let r = req(SessionRequestSetup::incremental(
            crate::fetch_session::test_support::RequestSessionId(12345),
        ));
        assert!(error_code(&cache, &r, NAME_FETCH_VERSION) == codes::FETCH_SESSION_ID_NOT_FOUND);
    }

    #[test]
    fn stale_epoch_returns_invalid_epoch() {
        let (cache, id) = allocated_cache(SessionAllocationSetup::default());
        // Session's expected next_epoch is 1; send epoch=99.
        let r = req(SessionRequestSetup {
            session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(99),
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });
        assert!(error_code(&cache, &r, NAME_FETCH_VERSION) == codes::INVALID_FETCH_SESSION_EPOCH);
    }

    /// Kafka's `FetchManager.newContext`: an incremental fetch whose version
    /// does not match the topic-id use of the request that created its
    /// session answers `FETCH_SESSION_TOPIC_ID_ERROR`, and a stale epoch
    /// still wins over it. The table records the error code, or `None` for an
    /// accepted incremental fetch, and the epoch the session expects next.
    #[test]
    fn an_incremental_fetch_must_match_the_topic_id_use_of_its_session() {
        struct Case {
            name: &'static str,
            created_with_topic_ids: TopicAddressing,
            epoch: i32,
            version: i16,
            want: (Option<i16>, i32),
        }
        let cases = [
            Case {
                name: "name session, v12 incremental",
                created_with_topic_ids: TopicAddressing::Name,
                epoch: 1,
                version: 12,
                want: (None, 2),
            },
            Case {
                name: "name session, v13 incremental",
                created_with_topic_ids: TopicAddressing::Name,
                epoch: 1,
                version: 13,
                want: (Some(codes::FETCH_SESSION_TOPIC_ID_ERROR), 1),
            },
            Case {
                name: "id session, v17 incremental",
                created_with_topic_ids: TopicAddressing::Id,
                epoch: 1,
                version: 17,
                want: (None, 2),
            },
            Case {
                name: "id session, v12 incremental",
                created_with_topic_ids: TopicAddressing::Id,
                epoch: 1,
                version: 12,
                want: (Some(codes::FETCH_SESSION_TOPIC_ID_ERROR), 1),
            },
            Case {
                name: "id session, v12 incremental at a stale epoch",
                created_with_topic_ids: TopicAddressing::Id,
                epoch: 7,
                version: 12,
                want: (Some(codes::INVALID_FETCH_SESSION_EPOCH), 1),
            },
        ];
        for case in cases {
            let cache = FetchSessionCache::new(10);
            let key = FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid([1; 16]),
                partition: 0,
            };
            let id = allocate_session(
                &cache,
                SessionAllocationSetup {
                    addressing: case.created_with_topic_ids,
                    partitions: vec![(key, CachedPartitionState::default())],
                    ..Default::default()
                },
            );
            let decision = cache.classify(
                &req(SessionRequestSetup {
                    session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(
                        case.epoch,
                    ),
                    ..SessionRequestSetup::incremental(
                        crate::fetch_session::test_support::RequestSessionId(id),
                    )
                }),
                case.version,
            );
            let code = match decision {
                SessionDecision::Error { code } => Some(code),
                SessionDecision::Incremental { .. } => None,
                other => panic!("{}: unexpected decision {other:?}", case.name),
            };
            let next_epoch = cache.inner.lock().expect("poisoned").sessions[&id].next_epoch;
            check!((code, next_epoch) == case.want, "{}", case.name);
        }
    }

    #[test]
    fn close_request_closes_the_session_inline() {
        let (cache, id) = allocated_cache(SessionAllocationSetup::default());
        let r = req(SessionRequestSetup {
            session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(FINAL_EPOCH),
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });
        assert!(matches!(
            cache.classify(&r, NAME_FETCH_VERSION),
            SessionDecision::Sessionless
        ));
        // classify() already dropped the session; nothing left to close.
        assert!(cache.len() == 0);
        // Subsequent classify with the same id is now NOT_FOUND.
        let r2 = req(SessionRequestSetup::incremental(
            crate::fetch_session::test_support::RequestSessionId(id),
        ));
        assert!(error_code(&cache, &r2, NAME_FETCH_VERSION) == codes::FETCH_SESSION_ID_NOT_FOUND);
    }

    /// Table over `(session_id, session_epoch, session exists?)` to
    /// `(top-level error code or decision, whether the cache still holds the
    /// id)`. Matches Kafka's `FetchMetadata.isFull()` classification (#871):
    /// full/final is decided by `session_epoch` alone, and a full fetch's
    /// session id (if any) is closed rather than epoch-checked.
    #[test]
    fn classification_matches_kafkas_epoch_only_rule() {
        enum Want {
            Sessionless,
            NewSession,
            Error(i16),
        }

        /// `Existing` allocates a session first and requests classify against
        /// its own id. `Fixed` requests classify against a literal id, which
        /// names no session in a fresh cache.
        enum RequestId {
            Existing,
            Fixed(i32),
        }

        struct Case {
            name: &'static str,
            request_id: RequestId,
            epoch: i32,
            want: Want,
        }

        let cases = [
            Case {
                name: "id=N>0, epoch=0 (reconnect): closes old, opens new",
                request_id: RequestId::Existing,
                epoch: INITIAL_EPOCH,
                want: Want::NewSession,
            },
            Case {
                name: "id=0, epoch>0: incremental lookup of id 0 misses",
                request_id: RequestId::Fixed(0),
                epoch: 5,
                want: Want::Error(codes::FETCH_SESSION_ID_NOT_FOUND),
            },
            Case {
                name: "id=N unknown, epoch=-1: full and final, no error",
                request_id: RequestId::Fixed(12345),
                epoch: FINAL_EPOCH,
                want: Want::Sessionless,
            },
            Case {
                name: "id=N known, epoch=-1: closes and answers sessionless",
                request_id: RequestId::Existing,
                epoch: FINAL_EPOCH,
                want: Want::Sessionless,
            },
            Case {
                name: "id=0, epoch=0: ordinary new-session open",
                request_id: RequestId::Fixed(0),
                epoch: INITIAL_EPOCH,
                want: Want::NewSession,
            },
        ];

        for case in cases {
            let cache = FetchSessionCache::new(10);
            let requested_id = match case.request_id {
                RequestId::Existing => allocate_session(&cache, SessionAllocationSetup::default()),
                RequestId::Fixed(id) => id,
            };
            let r = req(SessionRequestSetup {
                session_epoch: crate::fetch_session::test_support::RequestSessionEpoch(case.epoch),
                ..SessionRequestSetup::incremental(
                    crate::fetch_session::test_support::RequestSessionId(requested_id),
                )
            });
            match (cache.classify(&r, NAME_FETCH_VERSION), case.want) {
                (SessionDecision::Sessionless, Want::Sessionless)
                | (SessionDecision::NewSession, Want::NewSession) => {}
                (SessionDecision::Error { code }, Want::Error(want_code)) => {
                    check!(code == want_code, "{}", case.name);
                }
                (other, _) => panic!("{}: unexpected decision {other:?}", case.name),
            }
            // Every branch above closes or never opens a session, so the
            // cache holds nothing afterward in any case.
            check!(cache.len() == 0, "{}", case.name);
        }
    }

    #[test]
    fn incremental_merges_request_topics_and_bumps_epoch() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid::ZERO,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 100,
                max_bytes: 1024,
                ..Default::default()
            },
        )];
        let id = allocate_session(
            &cache,
            SessionAllocationSetup {
                partitions: initial,
                ..Default::default()
            },
        );

        // Incremental that updates partition 0's fetch_offset and adds partition 1.
        let r = req(SessionRequestSetup {
            topics: vec![topic(
                "t",
                &[krabka_ids::PartitionIndex(0), krabka_ids::PartitionIndex(1)],
            )],
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental {
                session_id,
                new_epoch,
                partitions,
            } => {
                check!(session_id == id);
                check!(new_epoch == 2);
                check!(partitions.len() == 2);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }

        // Re-sending with the old epoch fails — broker advanced to 2.
        let r2 = req(SessionRequestSetup::incremental(
            crate::fetch_session::test_support::RequestSessionId(id),
        ));
        assert!(error_code(&cache, &r2, NAME_FETCH_VERSION) == codes::INVALID_FETCH_SESSION_EPOCH);
    }

    #[derive(Clone, Copy, Default)]
    struct FetchByteLimit(i32);

    #[derive(krabka_macros::FieldDefaults)]
    struct IncrementalTopicSetup {
        #[default("t".into())]
        name: String,
        topic_id: WireUuid,
        fetch_offset: krabka_ids::Offset,
        #[default(FetchByteLimit(1024))]
        partition_max_bytes: FetchByteLimit,
    }

    fn incremental_topic(setup: IncrementalTopicSetup) -> FetchTopic {
        let IncrementalTopicSetup {
            name,
            topic_id,
            fetch_offset,
            partition_max_bytes,
        } = setup;
        FetchTopic {
            topic: name,
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: fetch_offset.0,
                partition_max_bytes: partition_max_bytes.0,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn incremental_partitions(
        decision: SessionDecision,
    ) -> Vec<(FetchSessionKey, CachedPartitionState)> {
        let SessionDecision::Incremental { partitions, .. } = decision else {
            panic!("expected Incremental");
        };
        partitions
    }

    fn expected_incremental_state(
        fetch_offset: krabka_ids::Offset,
        max_bytes: FetchByteLimit,
    ) -> CachedPartitionState {
        CachedPartitionState {
            fetch_offset: fetch_offset.0,
            last_fetched_epoch: -1,
            current_leader_epoch: -1,
            max_bytes: max_bytes.0,
            log_start_offset: -1,
            last_high_watermark: 0,
            last_last_stable_offset: 0,
            last_log_start_offset: 0,
            last_preferred_read_replica: 0,
            last_aborted_txns_hash: 0,
            last_error_code: 0,
        }
    }

    /// Incrementals retain the cache's resolved topic identity.
    fn resolved_incremental_partition(
        topic_id: WireUuid,
        state: CachedPartitionState,
    ) -> Vec<(FetchSessionKey, CachedPartitionState)> {
        vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id,
                partition: 0,
            },
            state,
        )]
    }

    fn check_resolved_incremental(
        cache: &FetchSessionCache,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        topic_id: WireUuid,
        expected: CachedPartitionState,
    ) {
        let partitions = incremental_partitions(cache.classify(request, NAME_FETCH_VERSION));
        assert!(partitions == resolved_incremental_partition(topic_id, expected));
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_id_only() {
        // Reproduces the broker-jvm-acceptance regression: a v ≥ 13 client
        // opens a session, the broker resolves and caches `(name, id, p)`;
        // then the client sends an incremental that only carries `topic_id`
        // (empty `topic`). The merge must update the cached entry — not
        // insert a duplicate with default `max_bytes`, which would silently
        // drop bytes from the subsequent read.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([7u8; 16]);
        let id = seed_resolved_partition(&cache, tid);

        // v ≥ 13 incremental: topic_id set, topic_name empty, new fetch_offset.
        let r = req(SessionRequestSetup {
            topics: vec![incremental_topic(IncrementalTopicSetup {
                name: String::new(),
                topic_id: tid,
                fetch_offset: krabka_ids::Offset(42),
                partition_max_bytes: FetchByteLimit(2048),
            })],
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });

        // No duplicate entry created; the cached (fully-resolved) key is
        // preserved and its desired state updated in place.
        check_resolved_incremental(
            &cache,
            &r,
            tid,
            expected_incremental_state(krabka_ids::Offset(42), FetchByteLimit(2048)),
        );
    }

    #[test]
    fn incremental_merge_matches_cached_key_by_topic_name_only() {
        // Mirror case for v ≤ 12 clients: cache has (name, id, p) after
        // server-side resolution; request carries name only, id ZERO.
        let cache = FetchSessionCache::new(10);
        let tid = WireUuid([9u8; 16]);
        let id = seed_resolved_partition(&cache, tid);

        let r = req(SessionRequestSetup {
            topics: vec![incremental_topic(IncrementalTopicSetup {
                fetch_offset: krabka_ids::Offset(99),
                partition_max_bytes: FetchByteLimit(4096),
                ..Default::default()
            })],
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });

        check_resolved_incremental(
            &cache,
            &r,
            tid,
            expected_incremental_state(krabka_ids::Offset(99), FetchByteLimit(4096)),
        );
    }

    #[test]
    fn forgotten_topics_drop_partitions_from_cache() {
        let cache = FetchSessionCache::new(10);
        let initial = vec![
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 0,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 1,
                },
                CachedPartitionState::default(),
            ),
            (
                FetchSessionKey {
                    topic_name: "t".into(),
                    topic_id: WireUuid::ZERO,
                    partition: 2,
                },
                CachedPartitionState::default(),
            ),
        ];
        let id = allocate_session(
            &cache,
            SessionAllocationSetup {
                partitions: initial,
                ..Default::default()
            },
        );

        let forgotten = vec![forgotten_topic(ForgottenTopicSetup {
            partitions: vec![krabka_ids::PartitionIndex(1)],
            ..Default::default()
        })];
        let r = req(SessionRequestSetup::forgetting(
            crate::fetch_session::test_support::RequestSessionId(id),
            forgotten,
        ));
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental { partitions, .. } => {
                let mut parts: Vec<i32> = partitions.iter().map(|(k, _)| k.partition).collect();
                parts.sort_unstable();
                assert!(parts == vec![0, 2]);
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
    }

    /// An incremental that forgets every cached partition and adds none
    /// leaves the session with nothing. Kafka's
    /// `IncrementalFetchContext.updateAndGenerateResponseData` drops such a
    /// session and answers `session_id = 0` (#871).
    #[test]
    fn incremental_that_empties_the_session_drops_it_and_reports_no_session() {
        let cache = FetchSessionCache::new(10);
        let key = session_key(SessionKeySetup::default());
        let id = allocate_session(
            &cache,
            SessionAllocationSetup {
                partitions: vec![(key, CachedPartitionState::default())],
                ..Default::default()
            },
        );

        let forgotten = vec![forgotten_topic(ForgottenTopicSetup::default())];
        let r = req(SessionRequestSetup::forgetting(
            crate::fetch_session::test_support::RequestSessionId(id),
            forgotten,
        ));
        match cache.classify(&r, NAME_FETCH_VERSION) {
            SessionDecision::Incremental {
                session_id,
                partitions,
                ..
            } => {
                check!(session_id == 0);
                check!(partitions.is_empty());
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
        assert!(cache.len() == 0);
        assert!(cache.total_partitions_cached() == 0);
    }
}
