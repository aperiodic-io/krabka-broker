//! The two records the harness owns per simulated cluster: a message in flight
//! on the bus, and everything the harness tracks for one node. They share a file
//! because the bus and the node bookkeeping are meaningless apart.

use krabka_raft::kraft::{
    QuorumStateMachine,
    event::Event,
    types::{NodeId, SimInstant},
};

use super::node_log::SimNodeLog;

/// A message in flight on the bus: a destination node plus the event that node
/// observes. The `src` field is recorded for partition filtering.
#[derive(Debug, Clone, Copy)]
pub(super) struct Message {
    pub(super) src: NodeId,
    pub(super) dst: NodeId,
    pub(super) event: Event,
}

/// A node and everything the harness owns on its behalf.
pub(super) struct Node<L: SimNodeLog> {
    pub(super) id: NodeId,
    pub(super) machine: QuorumStateMachine,
    pub(super) log: L,
    /// Harness mirror of the leader's high watermark. The `Role::Leader`
    /// variant also carries it, but the harness tracks it here for non-leaders
    /// and observers.
    pub(super) high_watermark: i64,
    /// Next election-timer deadline, if armed.
    pub(super) election_deadline: Option<SimInstant>,
    /// Next fetch-timer deadline, if armed.
    pub(super) fetch_deadline: Option<SimInstant>,
    /// Next leader heartbeat deadline, if armed.
    ///
    /// A leader periodically re-sends `BeginQuorumEpoch` to voters that are not
    /// actively fetching from it. This is genuine `KRaft` behaviour, and it is
    /// how a deposed leader that rejoins after a partition learns of the newer
    /// epoch and steps down. The core does not emit this on a timer, because a
    /// leader has no core-level timer, so the harness drives it.
    pub(super) heartbeat_deadline: Option<SimInstant>,
    /// Next leader check-quorum deadline, if armed.
    ///
    /// The core arms this on promotion and re-arms it whenever a majority of
    /// the voters has fetched. Reaching it means the leader has lost contact
    /// with the quorum and resigns.
    pub(super) check_quorum_deadline: Option<SimInstant>,
}

impl<L: SimNodeLog> Node<L> {
    /// Append under the machine's current epoch without advancing its durability frontier.
    pub(super) fn append_in_current_epoch(&mut self, count: usize) {
        self.log
            .append_in_epoch(self.machine.quorum_state().leader_epoch, count);
    }
}
