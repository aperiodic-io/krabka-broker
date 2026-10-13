//! The replica's volatile role within the current epoch and its per-role state.

use {
    crate::types::{NodeId, SimInstant},
    std::collections::{BTreeMap, BTreeSet},
};

/// How recently a replica must have fetched for `AddRaftVoter` to call it
/// caught up: Kafka's `LeaderState.isReplicaCaughtUp` window of one hour.
const CAUGHT_UP_FETCH_WINDOW_MS: u64 = 3_600_000;

/// Per-replica replication progress that a leader tracks: a voter's for the HWM
/// and any replica's for `DescribeQuorum` and the `AddRaftVoter` catch-up check.
///
/// A zero instant means the leader has not recorded that event, as Kafka's -1
/// timestamps do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplicaProgress {
    /// Highest offset the follower has acknowledged, that is, its fetch offset.
    pub fetch_offset: i64,
    /// Logical instant the leader received the most recent Fetch from this follower.
    pub last_fetch: SimInstant,
    /// Logical instant this follower was known to be caught up with the leader's log end.
    pub last_caught_up: SimInstant,
    /// The leader's log end offset when this follower last fetched: the offset
    /// the next fetch has to reach for the follower to count as having kept
    /// pace with the appends in between.
    pub last_fetch_leader_log_end: i64,
}

impl ReplicaProgress {
    /// Record a valid fetch at `fetch_offset` while the leader's log ended at
    /// `leader_log_end`, as Kafka's `ReplicaState.updateFollowerState` does.
    ///
    /// A fetch at or past the leader's log end is caught up now. A fetch short
    /// of it still counts as caught up as of the previous fetch when it reaches
    /// the log end that fetch saw, so a follower that keeps pace under
    /// continuous appends does not look stale. The caught-up time is settled
    /// before `last_fetch` moves, because the second case reads the old one.
    pub fn record_fetch(&mut self, now: SimInstant, fetch_offset: i64, leader_log_end: i64) {
        if fetch_offset >= leader_log_end {
            self.last_caught_up = self.last_caught_up.max(now);
        } else if self.last_fetch_leader_log_end > 0
            && fetch_offset >= self.last_fetch_leader_log_end
        {
            self.last_caught_up = self.last_caught_up.max(self.last_fetch);
        }
        self.last_fetch_leader_log_end = leader_log_end;
        self.last_fetch = self.last_fetch.max(now);
        self.fetch_offset = fetch_offset;
    }

    /// Kafka's `LeaderState.isReplicaCaughtUp`: the replica has been caught up
    /// at some point and fetched within the last hour.
    #[must_use]
    pub fn is_caught_up(&self, now: SimInstant) -> bool {
        self.last_caught_up > SimInstant(0) && self.fetched_within_caught_up_window(now)
    }

    /// Whether the replica fetched within the hour that
    /// [`Self::is_caught_up`] allows. A replica that has been quiet for longer
    /// can never count as caught up until it fetches again, so a leader may
    /// forget its progress.
    #[must_use]
    pub fn fetched_within_caught_up_window(&self, now: SimInstant) -> bool {
        self.last_fetch > SimInstant(0)
            && self.last_fetch.0 > now.0.saturating_sub(CAUGHT_UP_FETCH_WINDOW_MS)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, krabka_macros::EnumStr)]
#[enum_str(as_str = name)]
pub enum Role {
    /// Knows the epoch, no leader yet. May hold a non-binding pre-vote grant.
    Unattached { election_deadline: SimInstant },
    /// Cast a binding vote this epoch; now waits for a leader.
    Voted { election_deadline: SimInstant },
    /// Has a leader for the epoch and fetches from it.
    Follower {
        leader_id: NodeId,
        fetch_deadline: SimInstant,
        /// Whether a Fetch to `leader_id` has been answered successfully since
        /// this replica began following it: Kafka's
        /// `FollowerState.hasFetchedFromLeader`. Until then the leader may be
        /// unreachable from here, so the follower grants a pre-vote.
        has_fetched_from_leader: bool,
    },
    /// KIP-996 pre-vote candidate that collects non-binding grants.
    Prospective {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
        /// The leader of this epoch that the replica gave up on when it began
        /// the round, if it had one. Kafka's `ProspectiveState` keeps the
        /// `leaderId` of the `FollowerState` it came from, and a replica that
        /// knows a leader for its epoch grants no binding vote in it.
        /// `QuorumState::leader_id` is `None` meanwhile, so that the replica
        /// still attaches to a leader announced at this epoch.
        abandoned_leader: Option<NodeId>,
    },
    /// Real candidacy: the epoch is bumped and the replica voted for itself.
    Candidate {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Won the election; tracks follower progress for HWM.
    Leader {
        replicas: BTreeMap<NodeId, ReplicaProgress>,
        /// Voters that have fetched inside the current check-quorum window.
        ///
        /// This is Kafka's `LeaderState.fetchedVoters`: it never contains the
        /// leader itself, and it is emptied — and the check-quorum timer
        /// re-armed — the moment it reaches the majority the leader needs
        /// besides its own vote. A leader that reaches the deadline with the
        /// set still short of that count has lost the quorum and resigns.
        fetched_voters: BTreeSet<NodeId>,
        high_watermark: i64,
        /// Log end offset at the moment of promotion, that is, where this
        /// leader's `LeaderChange` record sits. That record is the first
        /// current-epoch record.
        ///
        /// The HWM may only advance past this offset. This enforces Raft Fig.8
        /// leader completeness: a current-epoch entry must be
        /// majority-replicated before commit.
        epoch_start_offset: i64,
    },
    /// Stepped down: told the voters to elect, and waits for its own election
    /// timer to start a pre-vote round.
    ///
    /// A leader reaches this variant when its check-quorum window expires. It
    /// no longer serves Fetch, no longer advances the high watermark, and no
    /// longer claims the leadership in `QuorumState`, so an isolated old leader
    /// stops answering as leader for its epoch.
    Resigned,
    /// Not in the voter set; only ever fetches.
    Observer {
        leader_id: Option<NodeId>,
        fetch_deadline: SimInstant,
    },
}

impl Default for Role {
    fn default() -> Self {
        Role::Unattached {
            election_deadline: SimInstant(0),
        }
    }
}

impl Role {
    #[must_use]
    pub fn is_leader(&self) -> bool { matches!(self, Role::Leader { .. }) }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};
    /// Every role reports its own name. These strings reach operators through
    /// metrics and logs, so one standing in for another is a real confusion.
    #[test]
    fn each_role_reports_its_own_name() {
        let names = [Role::default().name(), Role::Resigned.name()];
        assert2::assert!(names == ["Unattached", "Resigned"]);
        // Distinct, not merely non-empty: a single constant would satisfy the
        // latter for every variant.
        assert2::assert!(names[0] != names[1]);
    }

    /// `ReplicaState.updateFollowerState`: a fetch at or past the leader's log
    /// end is caught up now, and one short of it is caught up as of the
    /// previous fetch when it reaches the log end that fetch saw.
    #[test]
    fn a_fetch_updates_the_caught_up_time_by_kafkas_two_branch_rule() {
        /// A run of fetches and the progress the last of them leaves.
        struct Case {
            label: &'static str,
            fetches: Vec<Fetch>,
            expected: ReplicaProgress,
        }

        /// One fetch of the run.
        struct Fetch {
            now: u64,
            offset: i64,
            log_end: i64,
        }
        let fetch = |now, offset, log_end| Fetch {
            now,
            offset,
            log_end,
        };
        let progress =
            |fetch_offset, last_fetch, last_caught_up, last_fetch_leader_log_end| ReplicaProgress {
                fetch_offset,
                last_fetch: SimInstant(last_fetch),
                last_caught_up: SimInstant(last_caught_up),
                last_fetch_leader_log_end,
            };
        let cases = [
            Case {
                label: "a first fetch short of the end",
                fetches: vec![fetch(100, 5, 10)],
                expected: progress(5, 100, 0, 10),
            },
            Case {
                label: "a first fetch at the end",
                fetches: vec![fetch(100, 10, 10)],
                expected: progress(10, 100, 100, 10),
            },
            Case {
                label: "keeping pace under appends is caught up as of the previous fetch",
                fetches: vec![fetch(100, 5, 10), fetch(200, 10, 20)],
                expected: progress(10, 200, 100, 20),
            },
            Case {
                label: "still short of the previous end stays not caught up",
                fetches: vec![fetch(100, 5, 10), fetch(200, 9, 20)],
                expected: progress(9, 200, 0, 20),
            },
            Case {
                label: "catching up to the current end is caught up now",
                fetches: vec![fetch(100, 5, 10), fetch(200, 10, 20), fetch(300, 20, 20)],
                expected: progress(20, 300, 300, 20),
            },
        ];
        for Case {
            label,
            fetches,
            expected,
        } in cases
        {
            let mut actual = ReplicaProgress::default();
            for Fetch {
                now,
                offset,
                log_end,
            } in fetches
            {
                actual.record_fetch(SimInstant(now), offset, log_end);
            }
            assert2::assert!(actual == expected, "{label}");
        }
    }

    /// `LeaderState.isReplicaCaughtUp`: caught up at some point, and fetched
    /// within the last hour.
    #[test]
    fn caught_up_needs_a_caught_up_time_and_a_fetch_within_the_hour() {
        const HOUR: u64 = 3_600_000;
        let now = SimInstant(10 * HOUR);
        let progress = |last_fetch: u64, last_caught_up: u64| ReplicaProgress {
            last_fetch: SimInstant(last_fetch),
            last_caught_up: SimInstant(last_caught_up),
            ..Default::default()
        };
        let cases = [
            ("never fetched", progress(0, 0), false),
            ("fetched but never caught up", progress(now.0 - 1, 0), false),
            (
                "caught up and fetching",
                progress(now.0 - 1, now.0 - 1),
                true,
            ),
            (
                "caught up long ago, fetching now",
                progress(now.0 - 1, now.0 - 5 * HOUR),
                true,
            ),
            (
                "caught up, last fetch just inside the hour",
                progress(now.0 - HOUR + 1, now.0 - HOUR + 1),
                true,
            ),
            (
                "caught up, last fetch exactly an hour ago",
                progress(now.0 - HOUR, now.0 - HOUR),
                false,
            ),
            (
                "caught up, silent for two hours",
                progress(now.0 - 2 * HOUR, now.0 - 2 * HOUR),
                false,
            ),
        ];
        for (label, progress, expected) in cases {
            assert2::assert!(progress.is_caught_up(now) == expected, "{label}");
        }
    }

    /// A leader forgets an observer that has been silent for the hour, and no
    /// sooner: until then `is_caught_up` may still read it.
    #[test]
    fn a_replica_is_forgotten_only_after_an_hour_of_silence() {
        const HOUR: u64 = 3_600_000;
        let now = SimInstant(10 * HOUR);
        let fetched_at = |last_fetch: u64| ReplicaProgress {
            last_fetch: SimInstant(last_fetch),
            ..Default::default()
        };
        for (label, progress, expected) in [
            ("never fetched", fetched_at(0), false),
            ("fetched a moment ago", fetched_at(now.0 - 1), true),
            ("silent for six minutes", fetched_at(now.0 - 360_000), true),
            ("just inside the hour", fetched_at(now.0 - HOUR + 1), true),
            ("exactly an hour ago", fetched_at(now.0 - HOUR), false),
        ] {
            assert2::assert!(
                progress.fetched_within_caught_up_window(now) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn role_defaults_to_unattached() {
        let r = Role::default();
        assert2::assert!((matches!(r, Role::Unattached { .. }), r.is_leader()) == (true, false));
    }
}
