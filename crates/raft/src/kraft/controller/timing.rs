//! Derivation of the engine's timer deadlines from the configured election
//! timeout, plus the small role predicates that decide which timer is armed and
//! when a leadership change invalidates parked work.

use krabka_units::prelude::{Time, TimeExt as _};
use tokio::time::{Duration, Instant};

use crate::kraft::{
    core::{QuorumStateMachine, election_jitter_ms},
    role::Role,
    types::{Epoch, NodeId, SimInstant},
};

/// Leader heartbeat interval as a fraction of the election timeout. The leader
/// re-broadcasts `BeginQuorumEpoch` this often so followers that lost the
/// initial announcement (or a rejoining old leader) re-attach without waiting
/// for an election.
const HEARTBEAT_DIVISOR: u64 = 3;

/// The configured election timeout as whole milliseconds.
///
/// Every deadline derived from the timeout crosses into integers here. The
/// core's per-(node, epoch) jitter is defined over integer milliseconds
/// (`election_jitter_ms`), so keeping the base in the same domain leaves every
/// election deadline bit-identical to the raw-integer arithmetic it replaces.
pub fn election_timeout_ms(election_timeout: Time) -> u64 {
    u64::try_from(election_timeout.millis_i64()).unwrap_or(0)
}

pub fn initial_election_at(
    core: &QuorumStateMachine,
    initial_leader: Option<NodeId>,
    clock_base: Instant,
    me: NodeId,
    initial_epoch: Epoch,
    election_timeout: Time,
) -> Option<Instant> {
    match (
        core.is_voter(),
        initial_leader,
        core.quorum_state().voters.len(),
    ) {
        (true, None, 1) => {
            // Sole voter: there is no peer to race, so the election timeout
            // jitter stagger is pure startup latency. Fire on the first tick;
            // the lone-voter fast path already holds the only vote.
            Some(clock_base)
        }
        (true, None, _) => {
            // Same deterministic per-(node, epoch) jitter the core applies to
            // re-election timers, so the first election round is staggered
            // across closely-synchronized voters.
            let base_ms = election_timeout_ms(election_timeout);
            let jitter = election_jitter_ms(me, initial_epoch, base_ms);
            let delay_ms = base_ms.saturating_add(jitter);
            Some(
                clock_base
                    .checked_add(Duration::from_millis(delay_ms))
                    .unwrap_or(clock_base),
            )
        }
        _ => None,
    }
}

pub fn heartbeat_period(election_timeout: Time, configured: Option<Time>) -> Time {
    if let Some(configured) = configured {
        return configured;
    }
    let period_ms = election_timeout_ms(election_timeout)
        .div_euclid(HEARTBEAT_DIVISOR)
        .max(1);
    Time::from_millis(i64::try_from(period_ms).unwrap_or(i64::MAX))
}

pub fn election_timer_starts_election(is_voter: bool, is_leader: bool) -> bool {
    matches!((is_voter, is_leader), (true, false))
}

pub fn following_leader_for_role(role: &Role) -> Option<NodeId> {
    match role {
        Role::Follower { leader_id, .. } => Some(*leader_id),
        Role::Observer { leader_id, .. } => *leader_id,
        _ => None,
    }
}

pub fn should_fail_waiters_on_leadership_change(
    was_leader: bool,
    is_leader: bool,
    held_epoch: Epoch,
    current_epoch: Epoch,
) -> bool {
    matches!(
        (was_leader, is_leader, held_epoch == current_epoch),
        (true, false, _) | (true, true, false)
    )
}

pub fn instant_from_clock_base(clock_base: Instant, deadline: SimInstant) -> Instant {
    clock_base
        .checked_add(Duration::from_millis(deadline.0))
        .unwrap_or(clock_base)
}
