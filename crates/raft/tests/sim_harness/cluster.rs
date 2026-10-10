//! The `Sim` cluster itself: its construction and the surface the integration
//! tests drive it through. Everything here is what a test is allowed to say to
//! the simulation, which keeps the scheduler internals out of view.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use krabka_raft::kraft::{
    QuorumStateMachine,
    role::Role,
    types::{Epoch, NodeId, QuorumState, SimInstant},
};

use super::{
    node::{Message, Node},
    node_log::SimNodeLog,
    timers::{election_timeout_ms_of, election_timeout_of},
};

/// A deterministic multi-node `KRaft` simulation, generic over the per-node log.
pub struct Sim<L: SimNodeLog> {
    pub(super) nodes: BTreeMap<NodeId, Node<L>>,
    pub(super) voter_ids: Vec<NodeId>,
    /// Logical clock in milliseconds.
    pub(super) now: SimInstant,
    /// FIFO queue of in-flight messages. They are processed before the clock
    /// advances.
    pub(super) queue: VecDeque<Message>,
    /// Partitioned nodes. Every message to them or from them is dropped.
    pub(super) partitioned: BTreeSet<NodeId>,
}

impl<L: SimNodeLog> Sim<L> {
    /// Constructs a cluster of `voter_ids` whose per-node logs `make_log`
    /// produces. There is one fresh log per node, for example a tempdir-backed
    /// `KraftLog`.
    pub fn new_with(voter_ids: &[NodeId], mut make_log: impl FnMut(NodeId) -> L) -> Self {
        let voters = make_voter_set(voter_ids);
        let mut nodes = BTreeMap::new();
        for &id in voter_ids {
            // Stagger election timeouts deterministically so ties break and the
            // lowest-id node tends to win the race — elections always converge.
            let election_timeout_ms = election_timeout_ms_of(id);
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                election_timeout_of(id),
            );
            nodes.insert(
                id,
                Node {
                    id,
                    machine,
                    log: make_log(id),
                    high_watermark: 0,
                    // Arm the initial election timer: an idle voter must time out
                    // and begin an election to bootstrap the cluster.
                    election_deadline: Some(SimInstant(election_timeout_ms)),
                    fetch_deadline: None,
                    heartbeat_deadline: None,
                    check_quorum_deadline: None,
                },
            );
        }
        Self {
            nodes,
            voter_ids: voter_ids.to_vec(),
            now: SimInstant(0),
            queue: VecDeque::new(),
            partitioned: BTreeSet::new(),
        }
    }

    // ---- public test surface -------------------------------------------------

    /// Drives the simulation until it reaches a fixed point, or until
    /// `max_ticks` event steps have elapsed.
    ///
    /// Each step delivers one queued message. When the queue drains, the step
    /// fires the earliest pending timer instead. A healthy cluster long-polls
    /// forever, because the fetch watchdog re-polls indefinitely, so the harness
    /// detects "stable" as a fixed point: one whole timer-driven round that
    /// leaves every node's observable state unchanged, that is its role, epoch,
    /// log length, and HWM. That strips the otherwise-unbounded steady-state
    /// fetch loop and masks no real progress, because an election or a
    /// replication advance always changes the fingerprint and resets the
    /// counter.
    pub fn run_until_stable(&mut self, max_ticks: usize) {
        let mut stability =
            krabka_kraft_core::simulation_support::StableFingerprint::new(self.fingerprint());
        for _ in 0..max_ticks {
            if let Some(msg) = self.queue.pop_front() {
                self.deliver(msg);
                continue;
            }
            // Queue drained: fire the next timer (if any), then check for a fixed
            // point. Two consecutive no-change rounds means converged.
            let fired = self.fire_next_timer();
            if stability.observe(self.fingerprint()) {
                return;
            }
            if !fired && self.queue.is_empty() {
                // Nothing queued and no timer armed: fully quiescent.
                return;
            }
        }
    }

    /// Reach a stable cluster within the caller's limit and require exactly one leader.
    pub fn stabilize_one_leader(&mut self, max_ticks: usize) -> NodeId {
        self.run_until_stable(max_ticks);
        assert2::assert!(self.leaders().len() == 1);
        self.leaders()[0]
    }

    /// A deterministic snapshot of every node's observable state, which detects
    /// the steady-state fixed point. It is ordered by node id, in a `BTreeMap`.
    fn fingerprint(&self) -> Vec<(NodeId, &'static str, Epoch, usize, i64)> {
        self.nodes
            .values()
            .map(|n| {
                krabka_kraft_core::simulation_support::fingerprint(
                    n.id,
                    &n.machine,
                    n.log.record_count(),
                    n.high_watermark,
                )
            })
            .collect()
    }

    /// Isolates a node. The harness drops every message to it and from it, and
    /// its timers no longer affect its peers. The node keeps ticking
    /// internally, but no peer can hear it.
    pub fn partition(&mut self, node: NodeId) {
        self.partitioned.insert(node);
        // Drop any in-flight messages touching the partitioned node.
        self.queue.retain(|m| m.src != node && m.dst != node);
    }

    /// Heals a partition. The node can send and receive again.
    pub fn heal(&mut self, node: NodeId) {
        self.partitioned.remove(&node);
    }

    /// Appends `n` data records to the log of `leader` in its current leader
    /// epoch, then re-runs the leader's HWM bookkeeping over the new end offset.
    ///
    /// This models a produce. The records must then be replicated to a majority
    /// through the fetch loop before the HWM can advance past them.
    pub fn leader_append(&mut self, leader: NodeId, n: usize) {
        self.nodes
            .get_mut(&leader)
            .expect("no entry found for key")
            .append_in_current_epoch(n);
    }

    /// Injects a conflicting-epoch tail straight into the log of `follower` and
    /// bypasses the leader, so the next fetch round forces a divergence and a
    /// truncation. The `epoch` should differ from what the leader holds at those
    /// offsets.
    pub fn inject_conflicting_tail(&mut self, follower: NodeId, epoch: Epoch, n: usize) {
        let node = self.nodes.get_mut(&follower).unwrap();
        node.log.append_in_epoch(epoch, n);
    }

    /// The ids of all nodes currently in the `Leader` role.
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| n.id)
            .collect()
    }

    /// The configured voter ids.
    pub fn voters(&self) -> Vec<NodeId> {
        self.voter_ids.clone()
    }

    /// The set of distinct leader epochs across all voters. It holds one epoch
    /// once the cluster has converged.
    pub fn distinct_epochs(&self) -> BTreeSet<Epoch> {
        self.nodes
            .values()
            .map(|n| n.machine.quorum_state().leader_epoch)
            .collect()
    }

    /// The leader's current high watermark.
    pub fn leader_high_watermark(&self, node: NodeId) -> i64 {
        match self.nodes[&node].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&node].high_watermark,
        }
    }

    /// The log end offset of `node`.
    pub fn log_end_offset(&self, node: NodeId) -> i64 {
        self.nodes[&node].log.end_offset()
    }

    /// Borrows the log of `node`, for byte-level and decoded assertions in
    /// tests.
    pub fn node_log(&self, node: NodeId) -> &L {
        &self.nodes[&node].log
    }

    /// True if every voter's log end offset has reached `offset`.
    pub fn all_voters_fetched_to(&self, offset: i64) -> bool {
        self.voter_ids
            .iter()
            .all(|id| self.nodes[id].log.end_offset() >= offset)
    }
}

krabka_macros::empty_endpoint_voters!(make_voter_set);
