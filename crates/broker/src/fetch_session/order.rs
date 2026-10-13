//! The eviction index of the session cache: Kafka's `FetchSessionCacheShard`
//! bookkeeping, `lastUsed`, `evictableByAll` and `evictableByPrivileged`, and
//! its `tryEvict` decision (KIP-227).
//!
//! A full cache does not hand a newcomer the least recently used session. A
//! session is displaced only when it is stale, or when the newcomer is worth
//! more than the cheapest session that may be displaced:
//!
//! 1. The session unused the longest goes when it has been unused for more than
//!    [`MIN_EVICTION`], whoever asks.
//! 2. Otherwise the newcomer takes the first entry of the class it may
//!    displace, and only when its own [`EvictableKey`] does not order below that
//!    entry's: a privileged newcomer (a follower fetch, `replica_id >= 0`) may
//!    take the smallest consumer session outright, or the smallest follower
//!    session that is older than [`MIN_EVICTION`] and holds fewer partitions;
//!    a consumer newcomer may take only a consumer session older than
//!    [`MIN_EVICTION`] that holds fewer partitions.
//!
//! Anything else refuses the allocation, and the caller answers sessionless,
//! which is what keeps a full broker from churning healthy sessions: each
//! displaced consumer would reconnect with a full fetch and displace another.
//!
//! The three indexes are ordered sets, so a lookup is O(log n) in the number
//! of sessions and reads the first entry of one of them, not a scan of the
//! session map with the cache mutex held. Time is a [`Duration`] elapsed since
//! the cache's own [`MonotonicClock`](qubit_clock::MonotonicClock) origin.
//! Every stamp in one cache reads that one clock, so they compare as a total
//! order, and a test drives the order with a
//! [`qubit_clock::ManualMonotonicClock`] instead of a sleep.

use {
    super::epoch::FetchSessionId,
    std::{
        collections::{BTreeSet, HashMap},
        time::Duration,
    },
};

/// Kafka's `KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS`: how long a
/// session has to sit unused, or how old a session has to be, before a newcomer
/// may displace it.
pub(super) const MIN_EVICTION: Duration = Duration::from_millis(120_000);

/// Kafka's `FetchSession.EvictableKey`: sessions are worth more the more
/// privileged they are, then the more partitions they cache, then by id. The
/// derived order is the field order, which is Kafka's `compareTo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EvictableKey {
    privileged: bool,
    size: usize,
    id: FetchSessionId,
}

/// A session's place in the `last_used` index: when it was last used, then its
/// id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LastUsedKey {
    last_used: Duration,
    id: FetchSessionId,
}

/// What the index knows about one live session: what the entries it put in the
/// three sets were built from, so that a touch or a removal finds them again.
struct Entry {
    privileged: bool,
    created: Duration,
    last_used: Duration,
    /// The number of partitions the session cached at its last touch.
    size: usize,
}

impl Entry {
    fn key(&self, id: FetchSessionId) -> EvictableKey {
        EvictableKey {
            privileged: self.privileged,
            size: self.size,
            id,
        }
    }
}

/// The eviction index over the live sessions.
///
/// The keys of `entries` are exactly the key set of `Inner::sessions`. Every
/// insert, touch and removal updates both under the same lock, so the two
/// never drift.
pub(super) struct SessionOrder {
    entries: HashMap<FetchSessionId, Entry>,
    /// Kafka's `lastUsed`: `(last used, id)`, oldest first.
    last_used: BTreeSet<LastUsedKey>,
    /// Kafka's `evictableByAll`: the sessions created more than
    /// [`MIN_EVICTION`] before their last touch. A consumer newcomer reads
    /// its first entry.
    evictable_by_all: BTreeSet<EvictableKey>,
    /// Kafka's `evictableByPrivileged`: every consumer session, and the
    /// follower sessions that are old enough. A follower newcomer reads its
    /// first entry.
    evictable_by_privileged: BTreeSet<EvictableKey>,
}

impl SessionOrder {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            last_used: BTreeSet::new(),
            evictable_by_all: BTreeSet::new(),
            evictable_by_privileged: BTreeSet::new(),
        }
    }

    /// Records a session that was just created at `now`, holding `size`
    /// partitions.
    pub(super) fn insert(
        &mut self,
        id: FetchSessionId,
        privileged: bool,
        size: usize,
        now: Duration,
    ) {
        self.index(id, Entry {
            privileged,
            created: now,
            last_used: now,
            size,
        });
    }

    /// Records a use of session `id` at `now`, when it holds `size`
    /// partitions: Kafka's `FetchSessionCacheShard.touch`.
    ///
    /// Whether a session is old enough to be displaced is decided here, at
    /// each touch, from its age then. A session that is never touched again
    /// stays out of the evictable sets and leaves through staleness instead.
    pub(super) fn touch(&mut self, id: FetchSessionId, size: usize, now: Duration) {
        if let Some(mut entry) = self.unindex(id) {
            entry.last_used = now;
            entry.size = size;
            self.index(id, entry);
        }
    }

    /// Drops `id` from the index. Called for both an eviction and a
    /// client-requested close.
    pub(super) fn remove(&mut self, id: FetchSessionId) { self.unindex(id); }

    /// The session that a newcomer of this privilege and size may displace at
    /// `now`, or `None` when it may displace nothing and the allocation has to
    /// be refused: Kafka's `FetchSessionCacheShard.tryEvict`.
    pub(super) fn victim(
        &self,
        privileged_caller: bool,
        size: usize,
        now: Duration,
    ) -> Option<FetchSessionId> {
        let &LastUsedKey {
            last_used,
            id: oldest,
        } = self.last_used.first()?;
        if now.saturating_sub(last_used) > MIN_EVICTION {
            return Some(oldest);
        }
        let candidates = if privileged_caller {
            &self.evictable_by_privileged
        } else {
            &self.evictable_by_all
        };
        let cheapest = candidates.first()?;
        // The newcomer has no id yet, so Kafka keys it with 0, which orders
        // below every real session: on equal size and privilege it loses.
        let newcomer = EvictableKey {
            privileged: privileged_caller,
            size,
            id: 0,
        };
        (newcomer >= *cheapest).then_some(cheapest.id)
    }

    fn index(&mut self, id: FetchSessionId, entry: Entry) {
        let key = entry.key(id);
        self.last_used.insert(LastUsedKey {
            last_used: entry.last_used,
            id,
        });
        let old_enough = entry.last_used.saturating_sub(entry.created) > MIN_EVICTION;
        if !entry.privileged || old_enough {
            self.evictable_by_privileged.insert(key);
        }
        if old_enough {
            self.evictable_by_all.insert(key);
        }
        self.entries.insert(id, entry);
    }

    fn unindex(&mut self, id: FetchSessionId) -> Option<Entry> {
        let entry = self.entries.remove(&id)?;
        let key = entry.key(id);
        self.last_used.remove(&LastUsedKey {
            last_used: entry.last_used,
            id,
        });
        self.evictable_by_privileged.remove(&key);
        self.evictable_by_all.remove(&key);
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    const SECOND: Duration = Duration::from_secs(1);

    /// One step in the life of the index.
    enum Step {
        /// `(id, privileged, size)` created at the given second.
        Insert(FetchSessionId, bool, usize, u32),
        /// `(id, size)` touched at the given second.
        Touch(FetchSessionId, usize, u32),
    }

    /// The index after `steps`, in the order given. The seconds must not go
    /// backwards, because the production clock is monotonic.
    fn seeded(steps: &[Step]) -> SessionOrder {
        let mut order = SessionOrder::new();
        let mut previous = 0;
        for step in steps {
            let (Step::Insert(.., seconds) | Step::Touch(_, _, seconds)) = step;
            assert!(*seconds >= previous, "time must not go backwards");
            previous = *seconds;
            match *step {
                Step::Insert(id, privileged, size, seconds) => {
                    order.insert(id, privileged, size, seconds * SECOND);
                }
                Step::Touch(id, size, seconds) => order.touch(id, size, seconds * SECOND),
            }
        }
        order
    }

    /// A newcomer: its privilege, the partitions it caches, and the second it
    /// arrives.
    #[derive(Clone, Copy)]
    struct Newcomer {
        privileged: bool,
        size: usize,
        seconds: u32,
    }

    /// A newcomer, as the table writes it.
    const fn newcomer(privileged: bool, size: usize, seconds: u32) -> Newcomer {
        Newcomer {
            privileged,
            size,
            seconds,
        }
    }

    /// One `victim` scenario.
    struct VictimCase {
        label: &'static str,
        /// The steps that built the index.
        steps: &'static [Step],
        newcomer: Newcomer,
        /// The session the newcomer may displace.
        want: Option<FetchSessionId>,
    }

    #[test]
    fn victim_selection_follows_kafkas_try_evict() {
        // Ids 1 and 2 are consumer sessions, ids 3 and 4 follower sessions.
        // Every session below that has been touched after `MIN_EVICTION` (120
        // s) of life is "old" by creation; the others are not.
        let cases: [VictimCase; 14] = [
            VictimCase {
                label: "an empty cache has no victim",
                steps: &[],
                newcomer: newcomer(false, 1, 0),
                want: None,
            },
            VictimCase {
                label: "a session unused for more than the minimum is stale, and goes",
                steps: &[Step::Insert(1, false, 9, 0)],
                newcomer: newcomer(false, 1, 121),
                want: Some(1),
            },
            VictimCase {
                label: "a session unused for exactly the minimum is not stale",
                steps: &[Step::Insert(1, false, 9, 0)],
                newcomer: newcomer(false, 1, 120),
                want: None,
            },
            VictimCase {
                label: "a stale follower session goes to a consumer newcomer too",
                steps: &[Step::Insert(3, true, 9, 0)],
                newcomer: newcomer(false, 1, 121),
                want: Some(3),
            },
            VictimCase {
                label: "staleness picks the session unused the longest",
                steps: &[
                    Step::Insert(1, false, 1, 0),
                    Step::Insert(2, false, 1, 10),
                    Step::Touch(1, 1, 20),
                ],
                newcomer: newcomer(false, 1, 131),
                want: Some(2),
            },
            VictimCase {
                label: "a consumer never displaces a recently used session of its own class",
                steps: &[Step::Insert(1, false, 1, 0), Step::Insert(2, false, 1, 1)],
                newcomer: newcomer(false, 100, 2),
                want: None,
            },
            VictimCase {
                label: "a consumer never displaces a session created within the minimum",
                steps: &[Step::Insert(1, false, 1, 100), Step::Touch(1, 1, 110)],
                newcomer: newcomer(false, 100, 115),
                want: None,
            },
            VictimCase {
                label: "a larger consumer displaces an older, smaller consumer session",
                steps: &[Step::Insert(1, false, 2, 0), Step::Touch(1, 2, 121)],
                newcomer: newcomer(false, 3, 122),
                want: Some(1),
            },
            VictimCase {
                label: "an equally large consumer does not",
                steps: &[Step::Insert(1, false, 2, 0), Step::Touch(1, 2, 121)],
                newcomer: newcomer(false, 2, 122),
                want: None,
            },
            VictimCase {
                label: "a smaller consumer does not",
                steps: &[Step::Insert(1, false, 2, 0), Step::Touch(1, 2, 121)],
                newcomer: newcomer(false, 1, 122),
                want: None,
            },
            VictimCase {
                label: "a consumer displaces the smallest old consumer session, not the oldest",
                steps: &[
                    Step::Insert(1, false, 5, 0),
                    Step::Insert(2, false, 3, 1),
                    Step::Touch(1, 5, 121),
                    Step::Touch(2, 3, 122),
                ],
                newcomer: newcomer(false, 9, 123),
                want: Some(2),
            },
            VictimCase {
                label: "a consumer never displaces a follower session, however old",
                steps: &[Step::Insert(3, true, 1, 0), Step::Touch(3, 1, 121)],
                newcomer: newcomer(false, 1000, 122),
                want: None,
            },
            VictimCase {
                label: "a follower displaces any consumer session, however small the follower",
                steps: &[Step::Insert(1, false, 50, 0), Step::Insert(3, true, 1, 1)],
                newcomer: newcomer(true, 1, 2),
                want: Some(1),
            },
            VictimCase {
                label: "a follower displaces an old follower session only when it is larger",
                steps: &[Step::Insert(3, true, 2, 0), Step::Touch(3, 2, 121)],
                newcomer: newcomer(true, 2, 122),
                want: None,
            },
        ];
        for VictimCase {
            label,
            steps,
            newcomer:
                Newcomer {
                    privileged,
                    size,
                    seconds,
                },
            want,
        } in cases
        {
            let order = seeded(steps);
            check!(
                order.victim(privileged, size, seconds * SECOND) == want,
                "{label}"
            );
        }
    }

    #[test]
    fn a_larger_follower_displaces_an_old_follower_session() {
        let order = seeded(&[Step::Insert(3, true, 2, 0), Step::Touch(3, 2, 121)]);
        assert!(order.victim(true, 3, 122 * SECOND) == Some(3));
    }

    #[test]
    fn touching_a_session_moves_it_off_the_stale_head() {
        let mut order = seeded(&[Step::Insert(1, false, 1, 0), Step::Insert(2, false, 1, 1)]);
        assert!(order.victim(false, 1, 121 * SECOND) == Some(1));
        order.touch(1, 1, 121 * SECOND);
        assert!(order.victim(false, 1, 122 * SECOND) == Some(2));
    }

    #[test]
    fn touching_a_session_updates_the_size_it_is_ranked_by() {
        let mut order = seeded(&[Step::Insert(1, false, 2, 0), Step::Touch(1, 2, 121)]);
        assert!(order.victim(false, 3, 122 * SECOND) == Some(1));
        // The session grew to 4 partitions at its last use.
        order.touch(1, 4, 122 * SECOND);
        assert!(order.victim(false, 3, 123 * SECOND) == None);
        assert!(order.victim(false, 5, 123 * SECOND) == Some(1));
    }

    #[test]
    fn removing_a_session_removes_it_from_every_index() {
        let mut order = seeded(&[Step::Insert(1, false, 1, 0), Step::Touch(1, 1, 121)]);
        order.remove(1);
        check!(order.victim(false, 9, 500 * SECOND) == None);
        check!(order.victim(true, 9, 500 * SECOND) == None);
        check!(order.entries.is_empty());
    }
}
