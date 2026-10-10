//! The bounded session store: the `FetchSession` record, the map that holds
//! the live sessions, and the cache's constructors, lock-free counters, and
//! per-session lifecycle operations.
//!
//! `classify` and `try_allocate` are inherent methods of `FetchSessionCache`
//! too, and they live beside the request-classification and eviction logic
//! they belong to.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering},
    },
};

use qubit_clock::{MonotonicClock, StdMonotonicClock};

use super::{
    epoch::{FIRST_SESSION_ID, FetchSessionEpoch, FetchSessionId},
    order::SessionOrder,
    state::{CachedPartitionState, FetchSessionKey},
};

pub struct FetchSession {
    pub id: FetchSessionId,
    /// The epoch the *next* incremental request must carry. The cache sets it
    /// to `1` on allocation and raises it after each successful incremental
    /// fetch.
    pub next_epoch: FetchSessionEpoch,
    pub privileged: bool,
    /// Whether the request that created the session named topics by id
    /// (`Fetch` v13 and later). Kafka's `FetchSession.usesTopicIds`.
    pub uses_topic_ids: bool,
    pub creator_principal: String,
    pub partitions: HashMap<FetchSessionKey, CachedPartitionState>,
}

pub(super) struct Inner {
    pub(super) sessions: HashMap<FetchSessionId, FetchSession>,
    /// Recency order over `sessions`, split by privilege. It carries each
    /// session's last-use stamp and is what makes victim selection O(1). The
    /// two must be updated together under this lock, which is why they share
    /// one `Inner`.
    pub(super) order: SessionOrder,
}

pub struct FetchSessionCache {
    pub(super) inner: Mutex<Inner>,
    pub(super) next_id: AtomicI32,
    pub(super) max_slots: usize,
    pub(super) evictions: AtomicU64,
    /// Live session count. The cache maintains it under `inner`'s lock on
    /// every insert, evict, and close. `len()` exposes it lock-free, so the
    /// metrics gauge refresh on the hot fetch path never touches the cache
    /// mutex.
    pub(super) num_sessions: AtomicUsize,
    /// Sum of `session.partitions.len()` across every live session. The cache
    /// keeps it in step as it adds partitions on merge and allocate, and drops
    /// them on forget, evict, and close. `total_partitions_cached()` reads it
    /// lock-free.
    pub(super) num_partitions: AtomicUsize,
    /// Monotonic time source that the cache stamps onto each session's entry
    /// in the recency order for LRU eviction. It is injectable, so tests drive
    /// the eviction order with a [`qubit_clock::ManualMonotonicClock`] instead
    /// of `thread::sleep`.
    pub(super) clock: Arc<dyn MonotonicClock>,
}

impl FetchSessionCache {
    #[must_use]
    pub fn new(max_slots: usize) -> Self {
        Self::with_clock(max_slots, Arc::new(StdMonotonicClock::new()))
    }

    /// Constructs a cache with a caller-supplied [`MonotonicClock`].
    ///
    /// Production uses [`FetchSessionCache::new`], which supplies a
    /// [`StdMonotonicClock`]. Tests pass a
    /// [`qubit_clock::ManualMonotonicClock`], so that successive allocations
    /// land on distinct, deterministic points in the recency order without a
    /// sleep between them.
    #[must_use]
    pub fn with_clock(max_slots: usize, clock: Arc<dyn MonotonicClock>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                sessions: HashMap::new(),
                order: SessionOrder::new(),
            }),
            // Id allocation starts at FIRST_SESSION_ID — id 0 is reserved
            // as the INVALID_SESSION_ID sentinel.
            next_id: AtomicI32::new(FIRST_SESSION_ID),
            max_slots,
            evictions: AtomicU64::new(0),
            num_sessions: AtomicUsize::new(0),
            num_partitions: AtomicUsize::new(0),
            clock,
        }
    }

    /// Number of live sessions in the cache. This is a lock-free read of an
    /// atomic counter and does not touch the cache mutex.
    #[must_use]
    pub fn len(&self) -> usize {
        self.num_sessions.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of `session.partitions.len()` across every live session. The
    /// metrics sampler reads it. This is a lock-free read of an atomic counter
    /// and does not touch the cache mutex or scan the session map.
    #[must_use]
    pub fn total_partitions_cached(&self) -> usize {
        self.num_partitions.load(Ordering::Relaxed)
    }

    /// Cumulative count of eviction events since `new()`. There is one
    /// increment for each session that an allocation displaces. It does *not*
    /// count refused allocations, because those displace nothing.
    #[must_use]
    pub fn evictions_total(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// Updates the `last_*` fields on cached partitions to match what the
    /// handler emitted in the response that just finished. Only the partitions
    /// that the response included need an update. A filtered-out partition
    /// already matches the cache, which is why the broker filtered it.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn finalize_incremental(
        &self,
        session_id: FetchSessionId,
        sent: &[(FetchSessionKey, CachedPartitionState)],
    ) {
        let mut guard = self.inner.lock().expect("poisoned");
        let Some(session) = guard.sessions.get_mut(&session_id) else {
            return;
        };
        for (k, s) in sent {
            if let Some(state) = session.partitions.get_mut(k) {
                state.last_high_watermark = s.last_high_watermark;
                state.last_last_stable_offset = s.last_last_stable_offset;
                state.last_log_start_offset = s.last_log_start_offset;
                state.last_preferred_read_replica = s.last_preferred_read_replica;
                state.last_aborted_txns_hash = s.last_aborted_txns_hash;
                state.last_error_code = s.last_error_code;
            }
        }
    }

    /// Drops the session. The handler calls it when the request is `Close`,
    /// which is an existing session with epoch -1, or after the handler
    /// decides to invalidate the session by force.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn close(&self, session_id: FetchSessionId) {
        let mut guard = self.inner.lock().expect("poisoned");
        if let Some(session) = guard.sessions.remove(&session_id) {
            guard.order.remove(session_id);
            self.num_sessions.fetch_sub(1, Ordering::Relaxed);
            self.num_partitions
                .fetch_sub(session.partitions.len(), Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::primitives::uuid::Uuid as WireUuid;

    use super::*;
    use crate::fetch_session::{
        SessionDecision,
        test_support::{
            ForgottenTopicSetup, NAME_FETCH_VERSION, SessionAllocationSetup, SessionKeySetup,
            SessionRequestSetup, allocate_session, allocated_cache, forgotten_topic, req,
            session_key, topic,
        },
    };

    #[derive(Clone, Copy)]
    struct CachedPartitionCount(usize);

    /// Classification applies the partition diff before the count is inspected.
    fn check_incremental_count(
        cache: &FetchSessionCache,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        expected: CachedPartitionCount,
    ) {
        assert!(matches!(
            cache.classify(request, NAME_FETCH_VERSION),
            SessionDecision::Incremental { .. }
        ));
        assert!(cache.total_partitions_cached() == expected.0);
    }

    #[test]
    fn is_empty_tracks_session_lifecycle() {
        let cache = FetchSessionCache::new(10);
        assert!(cache.is_empty());

        let id = allocate_session(&cache, SessionAllocationSetup::default());
        assert!(!cache.is_empty());

        cache.close(id);
        assert!(cache.is_empty());
    }

    #[test]
    fn finalize_incremental_updates_last_state() {
        let cache = FetchSessionCache::new(10);
        let key = session_key(SessionKeySetup::default());
        let id = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "a".into(),
                partitions: vec![(key.clone(), CachedPartitionState::default())],
                ..Default::default()
            },
        );
        let sent = vec![(
            key.clone(),
            CachedPartitionState {
                last_high_watermark: 42,
                last_log_start_offset: 7,
                ..Default::default()
            },
        )];
        cache.finalize_incremental(id, &sent);
        let g = cache.inner.lock().unwrap();
        let s = g.sessions.get(&id).unwrap().partitions.get(&key).unwrap();
        assert!(s.last_high_watermark == 42);
        assert!(s.last_log_start_offset == 7);
    }

    fn cached_partition(
        partition: krabka_ids::PartitionIndex,
    ) -> (FetchSessionKey, CachedPartitionState) {
        (
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid::ZERO,
                partition: partition.0,
            },
            CachedPartitionState::default(),
        )
    }

    #[test]
    fn total_partitions_cached_sums_across_sessions() {
        let cache = FetchSessionCache::new(10);
        allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "a".into(),
                partitions: vec![
                    cached_partition(krabka_ids::PartitionIndex(0)),
                    cached_partition(krabka_ids::PartitionIndex(1)),
                ],
                ..Default::default()
            },
        );
        allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "b".into(),
                partitions: vec![
                    cached_partition(krabka_ids::PartitionIndex(2)),
                    cached_partition(krabka_ids::PartitionIndex(3)),
                    cached_partition(krabka_ids::PartitionIndex(4)),
                ],
                ..Default::default()
            },
        );
        assert!(cache.total_partitions_cached() == 5);
    }

    fn two_partition_cache() -> (FetchSessionCache, FetchSessionId) {
        allocated_cache(SessionAllocationSetup {
            principal: "a".into(),
            partitions: vec![
                cached_partition(krabka_ids::PartitionIndex(0)),
                cached_partition(krabka_ids::PartitionIndex(1)),
            ],
            ..Default::default()
        })
    }

    #[test]
    fn counters_track_merge_forget_and_close() {
        let (cache, id) = two_partition_cache();
        assert!(cache.len() == 1);
        assert!(cache.total_partitions_cached() == 2);

        // Incremental that forgets partition 1 and adds partitions 2 and 3:
        // net partition count goes 2 -> 3.
        let forgotten = vec![forgotten_topic(ForgottenTopicSetup {
            partitions: vec![krabka_ids::PartitionIndex(1)],
            ..Default::default()
        })];
        let r = req(SessionRequestSetup {
            topics: vec![topic(
                "t",
                &[
                    krabka_ids::PartitionIndex(0),
                    krabka_ids::PartitionIndex(2),
                    krabka_ids::PartitionIndex(3),
                ],
            )],
            forgotten,
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });
        check_incremental_count(&cache, &r, CachedPartitionCount(3));

        // Close drops the whole session and its partitions.
        cache.close(id);
        assert!(cache.len() == 0);
        assert!(cache.total_partitions_cached() == 0);
    }

    #[test]
    fn counters_track_large_incremental_add_delta() {
        let (cache, id) = two_partition_cache();

        let r = req(SessionRequestSetup {
            topics: vec![topic(
                "t",
                &[
                    krabka_ids::PartitionIndex(0),
                    krabka_ids::PartitionIndex(1),
                    krabka_ids::PartitionIndex(2),
                    krabka_ids::PartitionIndex(3),
                    krabka_ids::PartitionIndex(4),
                ],
            )],
            ..SessionRequestSetup::incremental(
                crate::fetch_session::test_support::RequestSessionId(id),
            )
        });
        check_incremental_count(&cache, &r, CachedPartitionCount(5));
    }

    #[test]
    fn counters_track_large_incremental_forget_delta() {
        let cache = FetchSessionCache::new(10);
        let id = allocate_session(
            &cache,
            SessionAllocationSetup {
                principal: "a".into(),
                partitions: vec![
                    cached_partition(krabka_ids::PartitionIndex(0)),
                    cached_partition(krabka_ids::PartitionIndex(1)),
                    cached_partition(krabka_ids::PartitionIndex(2)),
                    cached_partition(krabka_ids::PartitionIndex(3)),
                    cached_partition(krabka_ids::PartitionIndex(4)),
                ],
                ..Default::default()
            },
        );
        let forgotten = vec![forgotten_topic(ForgottenTopicSetup {
            partitions: vec![
                krabka_ids::PartitionIndex(2),
                krabka_ids::PartitionIndex(3),
                krabka_ids::PartitionIndex(4),
            ],
            ..Default::default()
        })];

        let r = req(SessionRequestSetup::forgetting(
            crate::fetch_session::test_support::RequestSessionId(id),
            forgotten,
        ));
        check_incremental_count(&cache, &r, CachedPartitionCount(2));
    }
}
