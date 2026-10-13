//! Answering an inbound `Vote`: the recipient side of both the KIP-996
//! pre-vote and the binding vote.
//!
//! The grant rules differ between the two rounds, and only the binding vote
//! is persisted, so the whole decision is kept in one place next to the
//! log-recency comparison it depends on.

use {
    super::{QuorumStateMachine, VoteRequest},
    crate::{
        action::{Action, TimerKind},
        event::LogEnd,
        role::Role,
        types::{Epoch, LogView, ReplicaKey, SimInstant},
    },
    krabka_verified::{
        VoteAdmissionDecision,
        vote::{VoteMembership, VoteTarget},
        vote_admission_decision,
    },
};

#[cfg(test)]
mod tests;

impl QuorumStateMachine {
    /// `true` if `candidate_log` is at least as up-to-date as ours.
    ///
    /// KIP-595: the higher last epoch wins. On a tie, the higher or equal
    /// offset wins.
    fn log_is_up_to_date(log: &dyn LogView, cand: LogEnd) -> bool {
        krabka_verified::log_is_up_to_date(
            log.last_epoch(),
            log.end_offset(),
            cand.last_epoch,
            cand.last_offset,
        )
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = request.from.0, ?request.cluster_id, voter_id = request.voter_id.0, voter_directory_id = %request.voter_directory_id, candidate = request.candidate.0, candidate_directory_id = %request.candidate_directory_id, candidate_epoch = request.candidate_epoch, pre_vote = request.pre_vote)
    )]
    pub(super) fn handle_vote_request(
        &mut self,
        request: VoteRequest,
        log: &dyn LogView,
        now: SimInstant,
    ) -> Vec<Action> {
        let VoteRequest {
            from,
            cluster_id,
            voter_id,
            voter_directory_id,
            candidate_epoch,
            candidate,
            candidate_directory_id,
            candidate_log_end: cand_log,
            pre_vote,
        } = request;
        let mut actions = Vec::new();
        let candidate_key = ReplicaKey {
            id: candidate,
            directory_id: candidate_directory_id,
        };
        // The wire adapter rejects signed sentinels before conversion. The pure
        // classifier then requires the exact target and both membership checks;
        // the candidate can be in either side of one adjacent KIP-853 transition.
        let target = VoteTarget {
            voter_id: voter_id.0,
            local_id: self.me.0,
            directory_matches: self.local_voter_directory_matches(voter_directory_id),
        };
        let membership = VoteMembership {
            cluster_matches: cluster_id.is_none_or(|id| id == self.state.cluster_id),
            local_is_voter: self.is_voter(),
            candidate_is_voter: self.current_or_adjacent_voter_key(candidate_key),
        };
        match vote_admission_decision(target, membership) {
            VoteAdmissionDecision::IgnoreWrongTarget => {
                tracing::warn!(
                    addressed_to = voter_id.0,
                    me = self.me.0,
                    "ignoring Vote addressed to a different voter"
                );
                return Vec::new();
            }
            VoteAdmissionDecision::Deny => {
                actions.push(Action::ReplyVote {
                    to: from,
                    epoch: self.state.leader_epoch,
                    granted: false,
                });
                return actions;
            }
            VoteAdmissionDecision::Consider => {}
        }
        // Fenced: candidate is behind our epoch.
        if candidate_epoch < self.state.leader_epoch {
            actions.push(Action::ReplyVote {
                to: from,
                epoch: self.state.leader_epoch,
                granted: false,
            });
            return actions;
        }
        // A vote or a pre-vote from a higher epoch first advances us to that
        // epoch (Unattached), clearing any prior vote and stepping a stale
        // leader or follower down. Kafka does this for both rounds, before it
        // looks at the grant rule of the state it lands in.
        actions.extend(self.advance_epoch_for_vote(candidate_key, candidate_epoch, now));
        let up_to_date = Self::log_is_up_to_date(log, cand_log);
        let granted = self.can_grant_vote(candidate_key, up_to_date, pre_vote);
        if granted && !pre_vote {
            // Binding: persist the vote, become Voted.
            self.state.voted_key = Some(candidate_key);
            let deadline = self.election_deadline(now);
            self.role = Role::Voted {
                election_deadline: deadline,
            };
            actions.push(Action::PersistQuorumState);
            actions.push(Action::TransitionedTo(self.role.name()));
            // Arm the election timer: if the candidate we voted for dies, this
            // node must time out and start its own election (else deadlock).
            actions.push(Action::ResetTimer {
                kind: TimerKind::Election,
                deadline,
            });
        }
        actions.push(Action::ReplyVote {
            to: from,
            epoch: self.state.leader_epoch,
            granted,
        });
        actions
    }

    /// Kafka's `transitionToUnattached` on seeing a vote or pre-vote from
    /// `candidate` at an epoch above ours: adopt that epoch as an unattached
    /// replica with no leader and no vote. A request at our own epoch, or below
    /// it, changes nothing, and neither does one between replicas that are not
    /// both voters, which this replica denies without acting on its epoch.
    ///
    /// `KafkaRaftClient.handleVoteRequest` makes this transition before it checks
    /// the voter key the request names, so the engine calls it for a request it
    /// will refuse for its key, and the response then carries the new epoch.
    pub fn advance_epoch_for_vote(
        &mut self,
        candidate: ReplicaKey,
        epoch: Epoch,
        now: SimInstant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        if epoch > self.state.leader_epoch
            && self.is_voter()
            && self.current_or_adjacent_voter_key(candidate)
        {
            self.transition_to_unattached(epoch, self.election_deadline(now), &mut actions);
        }
        actions
    }

    /// Whether this replica, in its current state, grants a vote to
    /// `candidate`: Kafka's `EpochState.canGrantVote` of that state.
    ///
    /// A leader grants nothing. A follower grants only a pre-vote, and only
    /// while it has not yet fetched from its leader, so a follower cut off from
    /// its leader lets an election proceed at once, and one that hears from the
    /// leader does not disturb it. A candidate or a resigned leader grants a
    /// pre-vote on log recency alone and never a binding vote. An unattached,
    /// prospective or voted replica grants a pre-vote on log recency, and a
    /// binding vote only to the candidate it already voted for, or, having voted
    /// for nobody and followed nobody in this epoch, to one whose log is up to
    /// date. A prospective replica counts the leader it abandoned as one it
    /// followed, as Kafka's `ProspectiveState` keeps that `leaderId`.
    fn can_grant_vote(&self, candidate: ReplicaKey, up_to_date: bool, pre_vote: bool) -> bool {
        match &self.role {
            Role::Leader { .. } | Role::Observer { .. } => false,
            Role::Follower {
                has_fetched_from_leader,
                ..
            } => pre_vote && !has_fetched_from_leader && up_to_date,
            Role::Candidate { .. } | Role::Resigned => pre_vote && up_to_date,
            Role::Unattached { .. } | Role::Prospective { .. } | Role::Voted { .. } => {
                let leader_known = self.state.leader_id.is_some()
                    || matches!(&self.role, Role::Prospective {
                        abandoned_leader: Some(_),
                        ..
                    });
                if pre_vote {
                    up_to_date
                } else if let Some(voted) = self.state.voted_key {
                    self.same_voter(voted, candidate)
                } else {
                    !leader_known && up_to_date
                }
            }
        }
    }
}
