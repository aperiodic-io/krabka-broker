//! The harness's timer vocabulary: the timer kinds it schedules, the staggered
//! election timeout each node is configured with, and the earliest-deadline
//! comparison the scheduler picks with. Determinism depends on all three, so
//! they stay together.

pub(super) use krabka_kraft_core::simulation_support::SimulationTimer as SimTimer;
use {
    krabka_raft::kraft::types::NodeId,
    krabka_units::prelude::{Time, TimeExt as _},
};

/// Leader heartbeat period. It stays well below the election timeout, so a
/// healthy leader's re-announcements always reach the voters before any
/// watchdog escalates.
pub(super) const HEARTBEAT_MS: u64 = 300;

/// The base election timeout, which is also the fetch watchdog period,
/// configured for node `id`. It is staggered by node id, so timer ties break
/// deterministically and the lowest live id tends to win the election race.
/// Elections therefore always converge.
pub(super) fn election_timeout_ms_of(id: NodeId) -> u64 { 1000 + id.0 * 50 }

/// [`election_timeout_ms_of`] as the quantity [`QuorumStateMachine::new`] takes.
/// The simulation's own clock stays in integer logical milliseconds, because a
/// [`SimInstant`] is a coordinate and not an extent. This conversion therefore
/// happens only at the core's constructor.
pub(super) fn election_timeout_of(id: NodeId) -> Time {
    Time::from_millis(i64::try_from(election_timeout_ms_of(id)).unwrap_or(i64::MAX))
}
