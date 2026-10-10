//! Translation of the actions the consensus core emits into bus traffic, timer
//! updates, and log bookkeeping, plus the replication the harness performs on a
//! follower's behalf. This is where the core's outputs become the peers' inputs.

use krabka_raft::kraft::types::NodeId;

use super::{cluster::Sim, node::Message, node_log::SimNodeLog};

impl<L: SimNodeLog> Sim<L> {
    // Broadcasts a vote or pre-vote request from `id` to every other voter.
    krabka_macros::simulation_actions!(krabka_kraft_core);

    fn advance_simulated_watermark(&mut self, id: NodeId, hwm: i64) {
        let node = self.nodes.get_mut(&id).unwrap();
        node.high_watermark = hwm;
        node.log.advance_hwm(hwm);
        // In KRaft the new high watermark rides along on the leader's
        // next fetch response, so every follower eventually learns it —
        // including a caught-up follower that is long-polling and would
        // otherwise never re-fetch. Model that by pushing the committed
        // boundary to every peer's log now (each `advance_hwm` is
        // monotonic and clamped to that peer's own replicated log end, so
        // a lagging follower only commits what it actually holds).
        for peer in self.all_node_ids() {
            if peer != id && !self.partitioned.contains(&peer) {
                let p = self.nodes.get_mut(&peer).unwrap();
                p.log.advance_hwm(hwm);
                p.high_watermark = hwm.min(p.log.end_offset());
            }
        }
    }

    /// Copies the log entries from `leader` that `follower` is missing, so the
    /// follower logs converge and the follower's fetch offset advances toward
    /// the leader's end.
    ///
    /// The method respects the epochs, because it delegates the byte-faithful
    /// copy and the divergence truncation to the log impl. It runs only when
    /// `leader` actually believes it is the leader and neither endpoint is
    /// partitioned.
    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if !self.can_replicate(follower, leader) {
            return;
        }
        // Lift the follower out to borrow both nodes without aliasing.
        let leader_hwm = self.leader_high_watermark(leader);
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        // The follower learns the leader's committed offset on each fetch (the
        // fetch response carries the leader's high watermark in real KRaft), so
        // its own committed-read boundary tracks the consensus HWM, bounded by
        // what it has actually replicated.
        follower_node.log.advance_hwm(leader_hwm);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    krabka_macros::simulation_transport!(krabka_kraft_core);
}
