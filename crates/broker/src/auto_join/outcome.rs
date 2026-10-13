//! Classification and logging of the leader's `AddRaftVoter` reply.
//!
//! The join loop never terminates on a reply — only on seeing itself in the
//! committed voter set — so this mapping from error code to `JoinOutcome` is
//! purely diagnostic. It lives apart from the loop so the code-to-outcome table
//! can be unit tested without a running broker.

use {crate::codes, krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JoinOutcome {
    Accepted,
    NotLeader,
    TimedOut,
    Refused,
    Unexpected(i16),
}

/// Log the leader's `AddRaftVoter` reply at the appropriate level. None of the
/// outcomes terminate the loop — the `voters().contains` check at the top of
/// `run` is the sole exit — so this is purely diagnostic.
pub(super) fn log_join_outcome(
    self_id: krabka_raft::NodeId,
    target: &str,
    resp: &AddRaftVoterResponse,
) -> JoinOutcome {
    match resp.error_code {
        codes::NONE => {
            tracing::info!(
                node_id = self_id.0,
                leader = %target,
                "auto-join accepted by leader"
            );
            // The committed V1Voters record may not be visible in our local
            // image yet (we're still catching up); the next loop iteration's
            // `voters().contains` check confirms before exiting.
            JoinOutcome::Accepted
        }
        codes::NOT_LEADER_OR_FOLLOWER => {
            // Not the leader. The error message may name the current leader,
            // but it isn't a routable address — fall back to rotating across
            // the configured bootstrap servers.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join target is not the leader; trying next bootstrap server"
            );
            JoinOutcome::NotLeader
        }
        codes::REQUEST_TIMED_OUT => {
            // Kafka's `AddVoterHandler` answers this for a pending voter change
            // and for a candidate that is not caught up yet. Keep replicating
            // and retry shortly.
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: reconfiguration pending or not yet caught up; retrying"
            );
            JoinOutcome::TimedOut
        }
        codes::INVALID_REQUEST => {
            tracing::debug!(
                node_id = self_id.0,
                server = %target,
                msg = ?resp.error_message,
                "auto-join: request refused; retrying"
            );
            JoinOutcome::Refused
        }
        other => {
            tracing::warn!(
                node_id = self_id.0,
                server = %target,
                error_code = other,
                msg = ?resp.error_message,
                "auto-join: unexpected error_code; retrying"
            );
            JoinOutcome::Unexpected(other)
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, krabka_raft::NodeId};

    #[test]
    fn log_join_outcome_classifies_response_codes() {
        let target = "127.0.0.1:9092";
        let response = |error_code| AddRaftVoterResponse {
            error_code,
            ..Default::default()
        };

        assert2::assert!(
            (log_join_outcome(NodeId(1), target, &response(codes::NONE)))
                == (JoinOutcome::Accepted)
        );
        assert2::assert!(
            (log_join_outcome(NodeId(1), target, &response(codes::NOT_LEADER_OR_FOLLOWER)))
                == (JoinOutcome::NotLeader)
        );
        assert2::assert!(
            (log_join_outcome(NodeId(1), target, &response(codes::REQUEST_TIMED_OUT)))
                == (JoinOutcome::TimedOut)
        );
        assert2::assert!(
            (log_join_outcome(NodeId(1), target, &response(codes::INVALID_REQUEST)))
                == (JoinOutcome::Refused)
        );
        assert2::assert!(
            (log_join_outcome(NodeId(1), target, &response(1234)))
                == (JoinOutcome::Unexpected(1234))
        );
    }
}
