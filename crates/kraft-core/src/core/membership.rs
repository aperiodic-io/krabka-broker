//! Voter-set updates and the KIP-853 adjacent-voter view.
//!
//! A replica applies a `VotersRecord` as soon as it reads one, before the
//! record commits, so it must keep both sides of the one in-flight
//! transition addressable. This module holds that bookkeeping and the role
//! changes a membership change forces on the local replica.

use {
    super::QuorumStateMachine,
    crate::{
        action::{Action, TimerKind},
        role::Role,
        types::{NodeId, ReplicaKey, SimInstant},
    },
    krabka_voters::VoterSet,
};

impl QuorumStateMachine {
    /// Apply the latest voter set read from the Raft log or a snapshot.
    ///
    /// KIP-853 requires replicas to use an uncommitted `VotersRecord`
    /// immediately. The durable engine owns record history and invokes this
    /// method again with the preceding set if the log is truncated.
    pub fn apply_voter_set(&mut self, voters: VoterSet, now: SimInstant) -> Vec<Action> {
        let was_voter = self.is_voter();
        let leader_id = self.state.leader_id;
        if self.state.voters != voters {
            self.adjacent_voters = Some(self.state.voters.clone());
        }
        self.state.voters = voters;

        if let Role::Leader {
            replicas,
            fetched_voters,
            ..
        } = &mut self.role
        {
            replicas.retain(|id, _| self.state.voters.contains(*id) && *id != self.me);
            for id in self.state.voters.ids() {
                if id != self.me {
                    replicas.entry(id).or_default();
                }
            }
            // Start a fresh check-quorum window against the new configuration,
            // tally and deadline together. Re-arming matters because a sole
            // voter arms nothing at promotion, so without it the leader of a
            // cluster that has just grown past one voter would never run
            // check-quorum again for its epoch; it also gives an added voter a
            // full window to make its first Fetch rather than deposing the
            // leader the instant the record applies. Emptying the tally is the
            // other half: a contact counted before the change belongs to the
            // window that just ended, and carrying it over would let one stale
            // fetch plus one fresh one re-arm a window no majority reached.
            fetched_voters.clear();
            if self.runs_check_quorum() {
                return vec![Action::ResetTimer {
                    kind: TimerKind::CheckQuorum,
                    deadline: self.check_quorum_deadline(now),
                }];
            }
            return Vec::new();
        }

        match (was_voter, self.is_voter()) {
            (true, false) => {
                let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                self.role = Role::Observer {
                    leader_id,
                    fetch_deadline,
                };
                let mut actions = vec![Action::TransitionedTo(self.role.name())];
                if let Some(leader_id) = leader_id {
                    actions.push(Action::SendFetch { leader_id });
                    actions.push(Action::ResetTimer {
                        kind: TimerKind::Fetch,
                        deadline: fetch_deadline,
                    });
                }
                actions
            }
            (false, true) => {
                if let Some(leader_id) = leader_id {
                    let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
                    self.role = Role::Follower {
                        leader_id,
                        fetch_deadline,
                        has_fetched_from_leader: false,
                    };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::SendFetch { leader_id },
                        Action::ResetTimer {
                            kind: TimerKind::Fetch,
                            deadline: fetch_deadline,
                        },
                    ]
                } else {
                    let election_deadline = self.election_deadline(now);
                    self.role = Role::Unattached { election_deadline };
                    vec![
                        Action::TransitionedTo(self.role.name()),
                        Action::ResetTimer {
                            kind: TimerKind::Election,
                            deadline: election_deadline,
                        },
                    ]
                }
            }
            _ => Vec::new(),
        }
    }

    /// Apply a committed `KRaftVersionRecord`.
    pub fn set_kraft_version(&mut self, version: u16) { self.state.kraft_version = version; }

    /// Forget the preceding voter view once the latest voter record commits.
    pub fn commit_voter_set(&mut self) { self.adjacent_voters = None; }

    fn voter_key_matches(&self, voters: &VoterSet, key: ReplicaKey) -> bool {
        voters.get(key.id).is_some_and(|voter| {
            self.state.kraft_version == 0 || voter.directory_id == key.directory_id
        })
    }

    pub(super) fn local_voter_directory_matches(&self, directory_id: uuid::Uuid) -> bool {
        self.state
            .voters
            .get(self.me)
            .is_none_or(|voter| self.state.kraft_version == 0 || voter.directory_id == directory_id)
    }

    /// Voter `id` from the current voter set, or from the adjacent set of an
    /// uncommitted KIP-853 change. A leader that only the adjacent set names is
    /// still followed (see `handle_begin_quorum_epoch`), so its endpoints are
    /// looked up the same way.
    #[must_use]
    pub fn current_or_adjacent_voter_entry(&self, id: NodeId) -> Option<&krabka_voters::Voter> {
        self.state.voters.get(id).or_else(|| {
            self.adjacent_voters
                .as_ref()
                .and_then(|voters| voters.get(id))
        })
    }

    pub(super) fn current_or_adjacent_voter(&self, id: NodeId) -> bool {
        self.state.voters.contains(id)
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| voters.contains(id))
    }

    pub(super) fn current_or_adjacent_voter_key(&self, key: ReplicaKey) -> bool {
        self.voter_key_matches(&self.state.voters, key)
            || self
                .adjacent_voters
                .as_ref()
                .is_some_and(|voters| self.voter_key_matches(voters, key))
    }

    pub(super) fn same_voter(&self, left: ReplicaKey, right: ReplicaKey) -> bool {
        left.id == right.id
            && (self.state.kraft_version == 0 || left.directory_id == right.directory_id)
    }

    /// Complete removal of the local leader after the reduced voter set has
    /// committed. Fetch serving continues until the engine invokes this edge.
    ///
    /// The removed leader becomes a discovering observer and arms its fetch
    /// timer. No later leader announces itself to a node outside the voter
    /// set, so the Fetch that the timer sends is how this node finds the new
    /// leader. Kafka's removed leader also goes on as an observer of its
    /// successor, and `DescribeQuorum` on that successor lists it.
    pub fn finish_local_leader_removal(&mut self, now: SimInstant) -> Vec<Action> {
        if self.is_voter() || !self.role.is_leader() {
            return Vec::new();
        }
        let epoch = self.state.leader_epoch;
        let preferred_successors = self.preferred_successors();
        self.state.leader_id = None;
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        self.role = Role::Observer {
            leader_id: None,
            fetch_deadline,
        };
        vec![
            Action::SendEndQuorumEpoch {
                epoch,
                preferred_successors,
            },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            core::test_support::{FakeLog, machine, voters, win_election},
            event::Event,
        },
        assert2::check,
    };

    #[test]
    fn kraft_version_and_commit_voter_set() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        m.set_kraft_version(2);
        check!(m.quorum_state().kraft_version == 2);

        m.adjacent_voters = Some(voters(&[NodeId(1)]));
        m.commit_voter_set();
        check!(m.adjacent_voters.is_none());
    }

    #[test]
    fn same_voter_and_key_matching() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        let k1 = ReplicaKey {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(100),
        };
        let k2 = ReplicaKey {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(200),
        };
        let k3 = ReplicaKey {
            id: NodeId(2),
            directory_id: uuid::Uuid::from_u128(100),
        };

        // kraft_version == 0: compares only id
        m.set_kraft_version(0);
        check!(m.same_voter(k1, k2));
        check!(!m.same_voter(k1, k3));

        // kraft_version > 0: compares both id and directory_id
        m.set_kraft_version(1);
        check!(!m.same_voter(k1, k2));
        check!(m.same_voter(k1, k1));
        check!(!m.same_voter(k1, k3));
    }

    #[test]
    fn current_or_adjacent_voter_queries() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        m.adjacent_voters = Some(voters(&[NodeId(3)]));

        check!(m.current_or_adjacent_voter(NodeId(1)));
        check!(m.current_or_adjacent_voter(NodeId(2)));
        check!(m.current_or_adjacent_voter(NodeId(3)));
        check!(!m.current_or_adjacent_voter(NodeId(4)));

        check!(m.current_or_adjacent_voter_entry(NodeId(1)).is_some());
        check!(m.current_or_adjacent_voter_entry(NodeId(3)).is_some());
        check!(m.current_or_adjacent_voter_entry(NodeId(4)).is_none());
    }

    #[test]
    fn finish_local_leader_removal_guards_and_transitions() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        // 1. If still voter, returns empty
        check!(m.finish_local_leader_removal(SimInstant(10)).is_empty());

        // 2. Win election to become leader
        win_election(&mut m, &log, &[NodeId(2)], SimInstant(20));
        check!(m.role().is_leader());

        // Still a voter: returns empty
        check!(m.finish_local_leader_removal(SimInstant(30)).is_empty());

        // 3. Apply a voter set that excludes self (NodeId 1 is now non-voter)
        m.apply_voter_set(voters(&[NodeId(2), NodeId(3)]), SimInstant(40));
        check!(!m.is_voter());
        check!(m.role().is_leader());

        // Now finish_local_leader_removal succeeds: it hands the epoch to the
        // remaining voters and becomes a discovering observer whose fetch
        // timer is armed, so its next Fetch finds the new leader.
        let epoch = m.quorum_state().leader_epoch;
        let preferred_successors = m.preferred_successors();
        let actions = m.finish_local_leader_removal(SimInstant(50));
        let fetch_deadline = SimInstant(50).saturating_add_ms(m.election_timeout_ms);
        check!(
            (m.role().clone(), m.quorum_state().leader_id, actions)
                == (
                    Role::Observer {
                        leader_id: None,
                        fetch_deadline,
                    },
                    None,
                    vec![
                        Action::SendEndQuorumEpoch {
                            epoch,
                            preferred_successors,
                        },
                        Action::PersistQuorumState,
                        Action::TransitionedTo("Observer"),
                        Action::ResetTimer {
                            kind: TimerKind::Fetch,
                            deadline: fetch_deadline,
                        },
                    ]
                )
        );
    }

    #[test]
    fn apply_voter_set_role_transitions_and_adjacent_tracking() {
        let mut m = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        check!(m.adjacent_voters.is_none());

        // Applying SAME voter set does not set adjacent_voters
        m.apply_voter_set(voters(&[NodeId(1), NodeId(2)]), SimInstant(10));
        check!(m.adjacent_voters.is_none());

        // Applying DIFFERENT voter set sets adjacent_voters to previous set
        m.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(3)]), SimInstant(20));
        check!(m.adjacent_voters.is_some());
        check!(
            m.adjacent_voters
                .as_ref()
                .unwrap()
                .ids()
                .iter()
                .copied()
                .collect::<Vec<_>>()
                == vec![NodeId(1), NodeId(2)]
        );

        // Transition from voter to observer: (was_voter: true, is_voter: false)
        let mut m2 = machine(NodeId(1), &[NodeId(1), NodeId(2)]);
        let log = FakeLog {
            end: 5,
            last_epoch: 1,
        };
        m2.on_event(
            Event::ReceiveBeginQuorumEpoch {
                leader_id: NodeId(2),
                leader_epoch: 4,
            },
            &log,
            SimInstant(10),
        );
        check!(matches!(m2.role(), Role::Follower { .. }));
        let actions = m2.apply_voter_set(voters(&[NodeId(2), NodeId(3)]), SimInstant(30));
        check!(matches!(m2.role(), Role::Observer { .. }));
        check!(actions.iter().any(|a| matches!(a, Action::SendFetch {
            leader_id: NodeId(2)
        })));

        // Transition from observer to voter: (was_voter: false, is_voter: true)
        let mut m3 = machine(NodeId(1), &[NodeId(2), NodeId(3)]);
        check!(matches!(m3.role(), Role::Observer { .. }));
        let actions =
            m3.apply_voter_set(voters(&[NodeId(1), NodeId(2), NodeId(3)]), SimInstant(40));
        check!(matches!(m3.role(), Role::Unattached { .. }));
        check!(actions.iter().any(|a| matches!(a, Action::ResetTimer {
            kind: TimerKind::Election,
            ..
        })));
    }
}
