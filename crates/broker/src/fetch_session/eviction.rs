//! Session allocation and the eviction that makes room for it.
//!
//! `try_allocate` asks `super::order` for a victim when the cache is full,
//! then draws a fresh wire-legal session id and inserts the new session. The
//! order names a victim only where Kafka's `FetchSessionCacheShard.tryEvict`
//! does: a session unused for more than two minutes, or a cheaper session that
//! this caller may displace. It refuses the allocation otherwise, and the
//! caller then falls back to a sessionless response.

use std::{collections::HashMap, sync::atomic::Ordering};

use qubit_clock::MonotonicClock as _;

use super::{
    cache::{FetchSession, FetchSessionCache},
    epoch::{
        FIRST_SESSION_ID, FetchSessionId, INITIAL_EPOCH, INVALID_SESSION_ID, next_epoch,
        session_id_is_reserved,
    },
    state::{CachedPartitionState, FetchSessionKey},
};

impl FetchSessionCache {
    /// Allocates a fresh session for a `NewSession` decision. `partitions`
    /// must carry both the desired state (`fetch_offset`, `max_bytes`, and the
    /// rest) and the response-side `last_*` values for what the broker just
    /// sent. The next incremental fetch compares the new response state to
    /// those values.
    ///
    /// Returns the assigned id, or `INVALID_SESSION_ID` (0) when the cache is
    /// full and it can evict no eligible victim. On a refused allocation the
    /// caller emits `response.session_id = 0`, and the client falls back to
    /// sessionless full fetches without further signalling.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn try_allocate(
        &self,
        privileged: bool,
        uses_topic_ids: bool,
        creator_principal: String,
        partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
    ) -> FetchSessionId {
        if self.max_slots == 0 {
            return INVALID_SESSION_ID;
        }
        let mut guard = self.inner.lock().expect("poisoned");
        let now = self.clock.now().elapsed_since_origin();

        if guard.sessions.len() >= self.max_slots {
            // A stale session, or a cheaper one this caller may displace.
            // Otherwise the newcomer is refused, however full the cache is,
            // and answers sessionless: displacing a healthy session would
            // only make its client reconnect and displace another.
            let Some(id) = guard.order.victim(privileged, partitions.len(), now) else {
                return INVALID_SESSION_ID;
            };
            let evicted = guard.sessions.remove(&id).expect("victim present");
            guard.order.remove(id);
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            self.num_partitions
                .fetch_sub(evicted.partitions.len(), Ordering::Relaxed);
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }

        // Allocate a fresh id. AtomicI32::fetch_add wraps, so we skip
        // 0 (sentinel) and any negative (would round-trip on the wire
        // as a "negative session id" the client rejects) and any
        // id that's already taken (extremely rare — happens only after
        // 2^31 allocations of overlap). The loop is bounded by the number of
        // live ids it could collide with plus the reserved wrap value and reset.
        let mut id = None;
        for _ in 0..guard.sessions.len().saturating_add(3) {
            let candidate = self.next_id.fetch_add(1, Ordering::Relaxed);
            if session_id_is_reserved(candidate) {
                // Wrapped past i32::MAX or hit zero. Reset to the first
                // allocatable id and try again; the next iteration will
                // fetch_add to 2 and store 3.
                self.next_id.store(FIRST_SESSION_ID, Ordering::Relaxed);
                continue;
            }
            if !guard.sessions.contains_key(&candidate) {
                id = Some(candidate);
                break;
            }
        }
        let Some(id) = id else {
            return INVALID_SESSION_ID;
        };

        let partitions: HashMap<FetchSessionKey, CachedPartitionState> =
            partitions.into_iter().collect();
        let session = FetchSession {
            id,
            // Client's first incremental request after a new-session
            // allocation must carry the epoch after INITIAL (i.e. 1).
            next_epoch: next_epoch(INITIAL_EPOCH),
            privileged,
            uses_topic_ids,
            creator_principal,
            partitions,
        };
        let added_partitions = session.partitions.len();
        guard.sessions.insert(id, session);
        guard.order.insert(id, privileged, added_partitions, now);
        self.num_sessions.fetch_add(1, Ordering::Relaxed);
        self.num_partitions
            .fetch_add(added_partitions, Ordering::Relaxed);
        id
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;

    use super::*;
    use crate::fetch_session::{
        SessionDecision,
        order::MIN_EVICTION,
        test_support::{
            NAME_FETCH_VERSION, SessionAllocationSetup, SessionPrivilege, SessionRequestSetup,
            TICK, allocate_session, manual_cache, req,
        },
    };

    #[test]
    fn allocate_returns_nonzero_monotonic_ids() {
        let cache = FetchSessionCache::new(10);
        let a = allocate_session(&cache, SessionAllocationSetup::default());
        let b = allocate_session(&cache, SessionAllocationSetup::default());
        // Id allocation starts at 1 and increments monotonically.
        check!(a == 1);
        check!(b == 2);
        check!(cache.len() == 2);
    }

    #[test]
    fn allocate_skips_zero_on_wrap() {
        let cache = FetchSessionCache::new(10);
        // Force the next id to be 0 — the loop should skip and start from 1.
        cache.next_id.store(0, Ordering::Relaxed);
        let id = allocate_session(&cache, SessionAllocationSetup::default());
        assert!(id > 0);
    }

    #[test]
    fn allocate_skips_existing_session_id_collision() {
        let cache = FetchSessionCache::new(10);
        let first = allocate_session(&cache, SessionAllocationSetup::default());

        cache.next_id.store(first, Ordering::Relaxed);
        let second = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "bob".into(),
                ..Default::default()
            },
        );

        assert!(second == first + 1);
        assert!(cache.len() == 2);
    }

    #[test]
    fn allocate_returns_zero_when_max_slots_zero() {
        let cache = FetchSessionCache::new(0);
        let id = allocate_session(&cache, SessionAllocationSetup::default());
        assert!(id == INVALID_SESSION_ID);
    }

    /// One second past the minimum: a session idle this long is stale, and a
    /// session this old may be displaced by a larger newcomer.
    const PAST_MIN_EVICTION: Duration = Duration::from_secs(121);

    /// `count` cached partitions of topic `t`.
    fn partitions(count: i32) -> Vec<(FetchSessionKey, CachedPartitionState)> {
        (0..count)
            .map(|partition| {
                (
                    FetchSessionKey {
                        topic_name: "t".into(),
                        topic_id: WireUuid::ZERO,
                        partition,
                    },
                    CachedPartitionState::default(),
                )
            })
            .collect()
    }

    /// The ids of the live sessions, ascending.
    fn live_ids(cache: &FetchSessionCache) -> Vec<FetchSessionId> {
        let guard = cache.inner.lock().unwrap();
        let mut ids: Vec<FetchSessionId> = guard.sessions.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// An incremental fetch on `id` that changes nothing: it uses the session,
    /// which is what Kafka's `touch` records.
    fn use_session(
        cache: &FetchSessionCache,
        id: crate::fetch_session::test_support::RequestSessionId,
        epoch: crate::fetch_session::test_support::RequestSessionEpoch,
    ) {
        assert!(matches!(
            cache.classify(
                &req(SessionRequestSetup {
                    session_epoch: epoch,
                    ..SessionRequestSetup::incremental(id)
                }),
                NAME_FETCH_VERSION
            ),
            SessionDecision::Incremental { .. }
        ));
    }

    /// A full cache hands a newcomer nothing while its sessions are in use.
    /// Displacing one would make its client reconnect with a full fetch and
    /// displace another, so Kafka answers the newcomer sessionless instead.
    #[test]
    fn a_full_cache_refuses_a_newcomer_while_its_sessions_are_active() {
        let (cache, clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(2));
        let a = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "a".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        clock.advance(TICK).expect("manual time moves forward");
        let b = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "b".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        clock.advance(TICK).expect("manual time moves forward");

        let newcomer = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "c".into(),
                partitions: partitions(100),
                ..Default::default()
            },
        );

        check!(newcomer == INVALID_SESSION_ID);
        check!(cache.evictions_total() == 0);
        check!(live_ids(&cache) == vec![a, b]);
    }

    /// The session unused for more than the minimum is displaced whoever asks
    /// and whoever it belongs to, and however small the newcomer is. Its
    /// neighbour, idle for less, stays.
    #[test]
    fn the_session_unused_for_more_than_the_minimum_is_displaced() {
        let cases = [
            (
                "consumer for consumer",
                SessionPrivilege::Consumer,
                SessionPrivilege::Consumer,
            ),
            (
                "consumer for follower",
                SessionPrivilege::Consumer,
                SessionPrivilege::Follower,
            ),
            (
                "follower for consumer",
                SessionPrivilege::Follower,
                SessionPrivilege::Consumer,
            ),
            (
                "follower for follower",
                SessionPrivilege::Follower,
                SessionPrivilege::Follower,
            ),
        ];
        for (label, holder_is_follower, newcomer_is_follower) in cases {
            let (cache, clock) =
                manual_cache(crate::fetch_session::test_support::SessionSlotCount(2));
            let stale = allocate_session(
                &cache,
                SessionAllocationSetup {
                    privilege: holder_is_follower,
                    principal: "a".into(),
                    partitions: partitions(9),
                    ..Default::default()
                },
            );
            clock
                .advance(MIN_EVICTION / 2)
                .expect("manual time moves forward");
            let active = allocate_session(
                &cache,
                SessionAllocationSetup {
                    privilege: holder_is_follower,
                    principal: "b".into(),
                    partitions: partitions(9),
                    ..Default::default()
                },
            );
            clock
                .advance(MIN_EVICTION / 2 + Duration::from_secs(1))
                .expect("manual time moves forward");

            let newcomer = allocate_session(
                &cache,
                SessionAllocationSetup {
                    privilege: newcomer_is_follower,
                    principal: "c".into(),
                    ..Default::default()
                },
            );

            check!(cache.evictions_total() == 1, "{label}");
            check!(live_ids(&cache) == vec![active, newcomer], "{label}");
            check!(!live_ids(&cache).contains(&stale), "{label}");
        }
    }

    /// An incremental fetch is a use, so a busy session is not the stale one.
    #[test]
    fn an_incremental_fetch_keeps_a_session_from_going_stale() {
        let (cache, clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(2));
        let busy = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "busy".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        let idle = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "idle".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );

        clock
            .advance(Duration::from_secs(100))
            .expect("manual time moves forward");
        use_session(
            &cache,
            crate::fetch_session::test_support::RequestSessionId(busy),
            crate::fetch_session::test_support::RequestSessionEpoch(1),
        );
        clock
            .advance(Duration::from_secs(30))
            .expect("manual time moves forward");
        // `idle` has gone 130 s unused and `busy` 30 s.
        let newcomer = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "newcomer".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );

        check!(cache.evictions_total() == 1);
        check!(live_ids(&cache) == vec![busy, newcomer]);
        check!(!live_ids(&cache).contains(&idle));
    }

    /// Short of staleness, a consumer newcomer may displace a consumer session
    /// that was created more than the minimum ago, when it caches more
    /// partitions than that session does. Kafka keys the newcomer with id 0,
    /// so on equal size it loses.
    #[test]
    fn a_larger_newcomer_displaces_a_smaller_session_older_than_the_minimum() {
        let cases = [
            ("larger", 3, true),
            ("equal", 2, false),
            ("smaller", 1, false),
        ];
        for (label, newcomer_partitions, displaces) in cases {
            let (cache, clock) =
                manual_cache(crate::fetch_session::test_support::SessionSlotCount(1));
            let held = allocate_session(
                &cache,
                SessionAllocationSetup {
                    principal: "held".into(),
                    partitions: partitions(2),
                    ..Default::default()
                },
            );
            clock
                .advance(PAST_MIN_EVICTION)
                .expect("manual time moves forward");
            // The use just now is what makes the session evictable by its age,
            // and it keeps the session from being stale.
            use_session(
                &cache,
                crate::fetch_session::test_support::RequestSessionId(held),
                crate::fetch_session::test_support::RequestSessionEpoch(1),
            );

            let newcomer = allocate_session(
                &cache,
                SessionAllocationSetup {
                    principal: "new".into(),
                    partitions: partitions(newcomer_partitions),
                    ..Default::default()
                },
            );

            check!((newcomer != INVALID_SESSION_ID) == displaces, "{label}");
            check!(cache.evictions_total() == u64::from(displaces), "{label}");
            check!(
                live_ids(&cache)
                    == if displaces {
                        vec![newcomer]
                    } else {
                        vec![held]
                    },
                "{label}"
            );
        }
    }

    /// A follower fetch may displace a consumer session outright, whatever its
    /// age or size: the replication that keeps the cluster in sync outranks a
    /// consumer's incremental session.
    #[test]
    fn a_follower_displaces_a_consumer_session_that_is_still_active() {
        let (cache, _clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(1));
        let consumer = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "consumer".into(),
                partitions: partitions(5),
                ..Default::default()
            },
        );

        let follower = allocate_session(
            &cache,
            SessionAllocationSetup {
                privilege: SessionPrivilege::Follower,
                principal: "follower".into(),
                ..Default::default()
            },
        );

        check!(follower != INVALID_SESSION_ID);
        check!(follower != consumer);
        check!(cache.evictions_total() == 1);
        check!(live_ids(&cache) == vec![follower]);
    }

    #[test]
    fn non_privileged_cannot_evict_privileged() {
        let cache = FetchSessionCache::new(1);
        let p = allocate_session(
            &cache,
            SessionAllocationSetup {
                privilege: SessionPrivilege::Follower,
                principal: "follower".into(),
                ..Default::default()
            },
        );
        assert!(p > 0);
        // Cache full, only session is privileged. Consumer alloc refused.
        let c = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "consumer".into(),
                ..Default::default()
            },
        );
        check!(c == INVALID_SESSION_ID);
        check!(cache.evictions_total() == 0);
        check!(cache.len() == 1);
    }

    /// A follower session is displaced by another follower only when it is
    /// stale, or older than the minimum and smaller than the newcomer.
    #[test]
    fn a_recent_follower_session_is_not_displaced_by_another_follower() {
        let (cache, clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(1));
        let first = allocate_session(
            &cache,
            SessionAllocationSetup {
                privilege: SessionPrivilege::Follower,
                principal: "f1".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        clock.advance(TICK).expect("manual time moves forward");

        let second = allocate_session(
            &cache,
            SessionAllocationSetup {
                privilege: SessionPrivilege::Follower,
                principal: "f2".into(),
                partitions: partitions(50),
                ..Default::default()
            },
        );

        check!(second == INVALID_SESSION_ID);
        check!(cache.evictions_total() == 0);
        check!(live_ids(&cache) == vec![first]);
    }

    #[test]
    fn a_closed_session_is_never_chosen_as_a_victim() {
        // Close has to drop the session from the eviction index as well as
        // from the map. If it did not, the index would still name the closed
        // session as the stale one and the next allocation into a full cache
        // would go looking for a session that is no longer there.
        let (cache, clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(2));
        let closed = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "closed".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        clock.advance(TICK).expect("manual time moves forward");
        let oldest_live = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "oldest-live".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        cache.close(closed);

        clock.advance(TICK).expect("manual time moves forward");
        let refill = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "refill".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        clock
            .advance(PAST_MIN_EVICTION)
            .expect("manual time moves forward");
        let newcomer = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "newcomer".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );

        // The cache refilled to {oldest_live, refill}; both are stale by now,
        // and `newcomer` displaced `oldest_live`, the one unused the longest.
        check!(cache.len() == 2);
        check!(live_ids(&cache) == vec![refill, newcomer]);
        check!(!live_ids(&cache).contains(&oldest_live));
    }

    #[test]
    fn counters_track_eviction() {
        let (cache, clock) = manual_cache(crate::fetch_session::test_support::SessionSlotCount(1));
        allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "a".into(),
                partitions: partitions(2),
                ..Default::default()
            },
        );
        assert!(cache.total_partitions_cached() == 2);
        clock
            .advance(PAST_MIN_EVICTION)
            .expect("manual time moves forward");
        // Allocating into the full cache evicts the lone stale session (2
        // parts) and inserts a fresh one (1 part).
        allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "b".into(),
                partitions: partitions(1),
                ..Default::default()
            },
        );
        assert!(cache.len() == 1);
        assert!(cache.total_partitions_cached() == 1);
    }
}
