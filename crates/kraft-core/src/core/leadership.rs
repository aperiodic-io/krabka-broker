//! Following somebody else: the `BeginQuorumEpoch` and `EndQuorumEpoch`
//! announcements a leader sends about its own epoch.
//!
//! Both events decide whether this replica attaches to the announced leader
//! or detaches from it, and both apply the same staleness and membership
//! guards, so they share a module.

use super::QuorumStateMachine;
use crate::{
    action::{Action, TimerKind},
    event::SuccessorRank,
    role::Role,
    types::{Epoch, LogView, NodeId, SimInstant},
};

impl QuorumStateMachine {
    /// A leader announced its epoch.
    ///
    /// If the epoch is at least our current epoch, we follow that leader and
    /// become a `Follower` or an attached `Observer`. This method ignores a
    /// stale announcement at a lower epoch.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_id = leader_id.0, leader_epoch)
    )]
    pub(super) fn handle_begin_quorum_epoch(
        &mut self,
        leader_id: NodeId,
        leader_epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        // A never-initialized joiner has no membership view yet and discovers
        // its first leader through configured bootstrap endpoints. Once a view
        // exists, accept only the current or explicitly adjacent KIP-853 set.
        let membership_known = !self.state.voters.is_empty()
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| !voters.is_empty());
        if membership_known && !self.current_or_adjacent_voter(leader_id) {
            return Vec::new();
        }
        // Accept a strictly-higher epoch, or an equal epoch only if we do not
        // already know a leader for it (one leader per epoch). Otherwise ignore.
        let accept = leader_epoch > self.state.leader_epoch
            || (leader_epoch == self.state.leader_epoch && self.state.leader_id.is_none());
        if !accept {
            return Vec::new();
        }
        // A vote belongs to its epoch, so only a newer epoch clears it. Kafka's
        // `QuorumState.transitionToFollower` keeps the voted key of the same
        // epoch: a replica that voted for one candidate and then follows the
        // leader that won must not vote again in that epoch after it times out.
        if leader_epoch > self.state.leader_epoch {
            self.state.voted_key = None;
        }
        self.state.leader_epoch = leader_epoch;
        self.state.leader_id = Some(leader_id);
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = if self.is_voter() {
            Role::Follower {
                leader_id,
                fetch_deadline,
                has_fetched_from_leader: false,
            }
        } else {
            Role::Observer {
                leader_id: Some(leader_id),
                fetch_deadline,
            }
        };
        vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::SendFetch { leader_id },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }

    /// A resigning leader asked us to start an election.
    ///
    /// If the request is not stale, a voter backs its election off by its
    /// place in the leader's preferred candidates, as Kafka's
    /// `KafkaRaftClient.handleEndQuorumEpochRequest` does. The first candidate
    /// starts a pre-vote round at once. Any other voter drops the resigned
    /// leader, so that it can grant the first candidate's pre-vote, and arms
    /// its election timer for the backoff. An observer detaches and continues
    /// to observe.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_epoch)
    )]
    pub(super) fn handle_end_quorum_epoch(
        &mut self,
        log: &dyn LogView,
        _leader_id: NodeId,
        leader_epoch: Epoch,
        successor_rank: SuccessorRank,
        now: SimInstant,
    ) -> Vec<Action> {
        if leader_epoch < self.state.leader_epoch {
            return Vec::new();
        }
        if !self.is_voter() {
            let mut actions = Vec::new();
            self.transition_to_unattached(self.state.leader_epoch, now, &mut actions);
            return actions;
        }
        let backoff_ms = end_epoch_election_backoff_ms(successor_rank, self.election_timeout_ms);
        if backoff_ms == 0 {
            return self.start_election(log, now);
        }
        self.state.leader_id = None;
        let election_deadline = now.saturating_add_ms(backoff_ms);
        self.role = if self.state.voted_key.is_some() {
            Role::Voted { election_deadline }
        } else {
            Role::Unattached { election_deadline }
        };
        vec![
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline: election_deadline,
            },
        ]
    }
}

/// Kafka's `KafkaRaftClient.strictExponentialElectionBackoffMs`, with the
/// election timeout as `controller.quorum.election.backoff.max.ms`.
///
/// The first candidate waits 0. A replica the list does not name waits the
/// maximum. Candidate `n` of `k` waits `(max >> (k - 1)) << (n - 1)`, capped
/// at the maximum.
fn end_epoch_election_backoff_ms(rank: SuccessorRank, max_ms: u64) -> u64 {
    if rank.position == 0 {
        0
    } else if rank.position >= rank.successors {
        max_ms
    } else {
        let base = max_ms.checked_shr(rank.successors - 1).unwrap_or(0);
        // `position < successors`, so the shift keeps the value at or below
        // `max_ms` and cannot overflow.
        base.checked_shl(rank.position - 1)
            .map_or(max_ms, |backoff| backoff.min(max_ms))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{
        core::test_support::{FakeLog, machine, three_voter_machine, voters},
        event::Event,
    };

    /// A follower attached to voter 2 at epoch 4 before it resigns.
    fn following_second_voter() -> (QuorumStateMachine, FakeLog) {
        let mut m = three_voter_machine();
        let log = FakeLog::new(5, 1);
        m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        (m, log)
    }

    #[test]
    fn begin_quorum_epoch_makes_us_follower() {
        let mut m = three_voter_machine();
        let log = FakeLog::new(5, 1);
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                matches!(
                    m.role(),
                    Role::Follower {
                        leader_id: NodeId(2),
                        ..
                    }
                ),
                m.quorum_state().leader_epoch,
                m.quorum_state().leader_id,
                actions.iter().any(|a| matches!(
                    a,
                    Action::SendFetch {
                        leader_id: NodeId(2)
                    }
                )),
                actions
                    .iter()
                    .any(|a| matches!(a, Action::PersistQuorumState)),
            ) == (true, 4, Some(NodeId(2)), true, true)
        );
    }

    #[test]
    fn end_quorum_epoch_triggers_immediate_election() {
        let (mut m, log) = following_second_voter();
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
                successor_rank: SuccessorRank::default(),
            },
            &log,
            SimInstant(11),
        );
        // immediately start pre-vote (Prospective), not wait for timeout
        crate::core::test_support::check_prevote_started(&m, &actions);
    }

    #[test]
    fn stale_begin_quorum_epoch_ignored() {
        let mut m = three_voter_machine();
        m.force_epoch(7);
        let log = FakeLog::new(5, 7);
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!((actions.is_empty(), m.quorum_state().leader_id) == (true, None));
    }

    #[test]
    fn begin_quorum_epoch_from_adjacent_voter_view_is_accepted() {
        // KIP-853: a newly elected leader may be absent from our temporarily
        // stale local voter view. Adopt the higher epoch and fetch its log.
        let mut m = three_voter_machine();
        let _ = m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(99)]), SimInstant(0));
        let log = FakeLog::new(5, 1);
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99), // not a voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(
            (
                m.quorum_state().leader_id,
                m.quorum_state().leader_epoch,
                matches!(m.role(), Role::Follower { .. }),
                actions.iter().any(|action| matches!(
                    action,
                    Action::SendFetch {
                        leader_id: NodeId(99)
                    }
                )),
            ) == (Some(NodeId(99)), 4, true, true)
        );
    }

    #[test]
    fn begin_quorum_epoch_from_voter_leader_still_accepted() {
        // C-2 must not break the legitimate path: a voter leader is adopted.
        let mut m = three_voter_machine();
        let log = FakeLog::new(5, 1);
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2), // a real voter
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        assert2::assert!(matches!(
            m.role(),
            Role::Follower {
                leader_id: NodeId(2),
                ..
            }
        ));
        check!(m.quorum_state().leader_id == Some(NodeId(2)));
        assert2::assert!(actions.iter().any(|a| matches!(
            a,
            Action::SendFetch {
                leader_id: NodeId(2)
            }
        )));
    }

    /// Kafka's `strictExponentialElectionBackoffMs` with a 1000 ms maximum.
    #[test]
    fn end_epoch_election_backoff_follows_the_candidate_position() {
        for (position, successors, want) in [
            (0, 0, 0),
            (0, 3, 0),
            (1, 3, 250),
            (2, 3, 500),
            (1, 2, 500),
            (3, 3, 1000),
            (2, 0, 1000),
            (1, 11, 0),
        ] {
            check!(
                end_epoch_election_backoff_ms(
                    SuccessorRank {
                        position,
                        successors
                    },
                    1000
                ) == want,
                "position {position} of {successors}"
            );
        }
    }

    /// A voter that is not the first preferred candidate waits for the backoff
    /// instead of electing at once, and it drops the resigned leader so that it
    /// can grant the first candidate's pre-vote.
    #[test]
    fn end_quorum_epoch_backs_off_a_later_candidate() {
        let (mut m, log) = following_second_voter();
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
                successor_rank: SuccessorRank {
                    position: 1,
                    successors: 2,
                },
            },
            &log,
            SimInstant(100),
        );
        let backoff = end_epoch_election_backoff_ms(
            SuccessorRank {
                position: 1,
                successors: 2,
            },
            m.election_timeout_ms,
        );
        check!(backoff > 0);
        check!(
            actions
                == vec![
                    Action::PersistQuorumState,
                    Action::TransitionedTo("Unattached"),
                    Action::ResetTimer {
                        kind: TimerKind::Election,
                        deadline: SimInstant(100 + backoff),
                    },
                ]
        );
        check!(
            (
                m.role().clone(),
                m.quorum_state().leader_id,
                m.quorum_state().leader_epoch
            ) == (
                Role::Unattached {
                    election_deadline: SimInstant(100 + backoff)
                },
                None,
                4
            )
        );
    }

    #[test]
    fn begin_quorum_epoch_membership_and_duplicate_leader_checks() {
        let log = FakeLog::new(5, 1);

        // 1. When voters known, unknown leader is rejected
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        let actions = m.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(actions.is_empty());
        check!(m.quorum_state().leader_id.is_none());

        // 2. When voters empty but adjacent_voters set, unknown leader is rejected
        let mut m2 = machine(NodeId(1), &[]);
        m2.adjacent_voters = Some(voters(&[NodeId(1), NodeId(2)]));
        let actions = m2.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(actions.is_empty());
        check!(m2.quorum_state().leader_id.is_none());

        // 3. When bootstrap joiner (voters empty, adjacent_voters None), any leader accepted
        let mut m3 = machine(NodeId(1), &[]);
        let actions = m3.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(99),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(!actions.is_empty());
        check!(m3.quorum_state().leader_id == Some(NodeId(99)));

        // 4. Duplicate leader for same epoch is rejected
        let mut m4 = three_voter_machine();
        m4.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(m4.quorum_state().leader_id == Some(NodeId(2)));
        let actions = m4.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(3),
                leader_epoch: 4,
            },
            &log,
            SimInstant(20),
        );
        check!(actions.is_empty());
        check!(m4.quorum_state().leader_id == Some(NodeId(2)));
    }

    #[test]
    fn end_quorum_epoch_ignores_stale_epoch() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        m.force_epoch(4);
        let log = FakeLog::new(5, 1);
        let actions = m.on_event(
            Event::ReceiveEndQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 3,
                successor_rank: SuccessorRank::default(),
            },
            &log,
            SimInstant(10),
        );
        check!(actions.is_empty());
    }
}
