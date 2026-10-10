//! The simulated node and the message that travels between nodes.
//!
//! A [`Node`] owns one [`QuorumStateMachine`], the in-memory log it replicates,
//! and the three harness deadlines the scheduler arms on its behalf. This
//! module also holds the timing model: the heartbeat period and the election
//! timeout that node ids stagger, so ties break deterministically and elections
//! converge.

use krabka_units::prelude::{Time, TimeExt as _, millis};

use super::log::SimLog;
use crate::{
    core::QuorumStateMachine,
    event::Event,
    types::{NodeId, SimInstant},
};

/// A message in flight on the bus: a destination node and the event it will
/// observe.
///
/// The bus records `src` for partition filtering and for trace labelling.
#[derive(Debug, Clone)]
pub(super) struct Message {
    pub(super) src: NodeId,
    pub(super) dst: NodeId,
    pub(super) event: Event,
}

/// A node and everything the harness owns on its behalf.
pub(super) struct Node {
    pub(super) id: NodeId,
    pub(super) machine: QuorumStateMachine,
    pub(super) log: SimLog,
    pub(super) high_watermark: i64,
    pub(super) election_deadline: Option<SimInstant>,
    pub(super) fetch_deadline: Option<SimInstant>,
    pub(super) heartbeat_deadline: Option<SimInstant>,
    /// Leader-side check-quorum deadline. Armed by the core's `ResetTimer`, and
    /// cleared for every role that is not the leadership.
    pub(super) check_quorum_deadline: Option<SimInstant>,
}

impl Node {
    /// Append under the machine's current epoch, returning the epoch for the trace.
    pub(super) fn append_in_current_epoch(&mut self, count: usize) -> crate::types::Epoch {
        let epoch = self.machine.quorum_state().leader_epoch;
        self.log.append_in_epoch(epoch, count);
        epoch
    }
}

/// How often a simulated leader re-announces its epoch to the cluster.
pub(super) const HEARTBEAT: Time = millis(300);

/// The election timeout a simulated voter with the lowest node id starts from.
const BASE_ELECTION_TIMEOUT: Time = millis(1000);

/// The simulator adds this to [`BASE_ELECTION_TIMEOUT`] once per unit of node
/// id. Voters then do not arm their election timers in lockstep, and ties break
/// deterministically.
const ELECTION_TIMEOUT_STAGGER: Time = millis(50);

pub(super) fn election_timeout_of(id: NodeId) -> Time {
    let rank = f64::from(u32::try_from(id.0).unwrap_or(u32::MAX));
    BASE_ELECTION_TIMEOUT + ELECTION_TIMEOUT_STAGGER * rank
}

/// `extent` in whole milliseconds, for arithmetic on the raw-integer
/// [`SimInstant`] clock.
///
/// A [`SimInstant`] is a coordinate on the simulator's logical millisecond
/// timeline and not a magnitude, so it stays an integer. Only the extents added
/// to it are quantities. The conversion is exact for every extent the simulator
/// uses.
pub(super) fn deadline_millis(extent: Time) -> u64 {
    u64::try_from(extent.millis_i64()).unwrap_or(0)
}

pub(super) use crate::simulation_support::voter_set as make_voter_set;

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn election_timeouts_land_on_whole_staggered_milliseconds() {
        // The scheduler's clock is an integer millisecond timeline, and which
        // voter wins a race is decided by these exact values. Pin them so a
        // rounding shift in the extent arithmetic cannot silently rewrite the
        // simulated timeline.
        for (id, expected_millis) in [
            (NodeId(1), 1050),
            (NodeId(2), 1100),
            (NodeId(3), 1150),
            (NodeId(7), 1350),
        ] {
            check!(
                deadline_millis(election_timeout_of(id)) == expected_millis,
                "node {id}"
            );
        }
        check!(deadline_millis(HEARTBEAT) == 300);
    }
}
