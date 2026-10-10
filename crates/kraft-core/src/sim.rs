//! Curated, deterministic `KRaft` consensus failure scenarios with trace
//! recording.
//!
//! This module promotes the deterministic, pure-synchronous multi-node `KRaft`
//! simulator into the library behind the `scenarios` feature. The simulator
//! uses no tokio, and it is the same scheduler the integration tests use
//! (`crates/raft/tests/sim_harness/mod.rs`). This module also instruments it to
//! RECORD a serializable [`ScenarioTrace`] of every step. `krabka-docgen` runs
//! [`scenarios`] in process and renders the traces into a Mermaid
//! sequence-diagram slideshow, so the diagrams show the real algorithm.
//!
//! Determinism is non-negotiable. There is no `Instant::now`, no `rand`, and no
//! `HashMap` iteration-order dependence anywhere. The clock is a `u64` of
//! logical milliseconds. All node and message containers are `BTreeMap` or
//! `BTreeSet`, so the iteration order is fixed. Node ids stagger the election
//! timeouts, so ties break deterministically and elections converge.
//!
//! This module root holds the [`Sim`] harness itself: its construction, the
//! trace recording, and the cluster-level fault surface. The submodules hold
//! the parts it is built from. `node` and `log` model a simulated node and the
//! log it replicates, `trace` holds the serializable recording types,
//! `scheduler` picks and fires the next-due timer, `bus` delivers messages and
//! applies the resulting actions, `playground` is the interactive control
//! surface, and `curated` drives the recorded failure scenarios.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use self::{
    log::SimLog,
    node::{Message, Node, deadline_millis, election_timeout_of, make_voter_set},
};
use crate::{
    core::QuorumStateMachine,
    role::Role,
    types::{Epoch, NodeId, QuorumState, SimInstant},
};

mod bus;
mod curated;
mod log;
mod node;
mod playground;
mod scheduler;
mod trace;

pub use self::{
    curated::scenarios,
    trace::{InFlight, NodeRole, ScenarioTrace, SimSnapshot, TraceAction, TraceStep},
};

/// A deterministic multi-node `KRaft` simulation over the in-memory fake log,
/// instrumented to record a [`ScenarioTrace`].
///
/// This type also backs the interactive in-browser playground through
/// `krabka-playground`, not only the curated [`scenarios`]. The playground
/// drives the same scheduler one step at a time with operator-injected faults:
/// partition, heal, drop, reorder, duplicate, and append. It reads back a
/// serializable [`SimSnapshot`] after each step. The recorded
/// [`steps`](Self::steps) also serve as the playground's event timeline.
pub struct Sim {
    nodes: BTreeMap<NodeId, Node>,
    voter_ids: Vec<NodeId>,
    now: SimInstant,
    queue: VecDeque<Message>,
    partitioned: BTreeSet<NodeId>,
    steps: Vec<TraceStep>,
    /// Leader epochs already recorded with an `Elected` step, so we record each
    /// promotion exactly once.
    elected_seen: BTreeSet<(NodeId, Epoch)>,
}

impl Sim {
    #[must_use]
    pub fn new(voter_ids: &[NodeId]) -> Self {
        let voters = make_voter_set(voter_ids);
        let mut nodes = BTreeMap::new();
        for &id in voter_ids {
            let election_timeout = election_timeout_of(id);
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                election_timeout,
            );
            nodes.insert(
                id,
                Node {
                    id,
                    machine,
                    log: SimLog::default(),
                    high_watermark: 0,
                    election_deadline: Some(SimInstant(deadline_millis(election_timeout))),
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
            steps: Vec::new(),
            elected_seen: BTreeSet::new(),
        }
    }

    // ---- trace recording -----------------------------------------------------

    /// Snapshot every node's observable state, in ascending id order.
    fn snapshot_roles(&self) -> Vec<NodeRole> {
        self.nodes
            .values()
            .map(|n| {
                let hwm = match n.machine.role() {
                    Role::Leader { high_watermark, .. } => *high_watermark,
                    _ => n.high_watermark,
                };
                NodeRole {
                    id: n.id.0,
                    role: n.machine.role().name().to_string(),
                    epoch: u64::from(n.machine.quorum_state().leader_epoch),
                    log_len: n.log.record_count(),
                    hwm,
                    partitioned: self.partitioned.contains(&n.id),
                }
            })
            .collect()
    }

    /// Push a recorded step that holds `action`, `note`, and a fresh role snapshot.
    fn record(&mut self, action: TraceAction, note: impl Into<String>) {
        let index = self.steps.len();
        let roles = self.snapshot_roles();
        self.steps.push(TraceStep {
            index,
            clock_ms: self.now.0,
            action,
            note: note.into(),
            roles,
        });
    }

    /// Record any newly-promoted leaders after the machine runs.
    fn record_new_leaders(&mut self) {
        let promotions: Vec<(NodeId, Epoch)> = self
            .nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| (n.id, n.machine.quorum_state().leader_epoch))
            .filter(|key| !self.elected_seen.contains(key))
            .collect();
        for (id, epoch) in promotions {
            self.elected_seen.insert((id, epoch));
            self.record(
                TraceAction::Elected {
                    node: id.0,
                    epoch: u64::from(epoch),
                },
                format!("N{id} won the election for epoch {epoch}"),
            );
        }
    }

    // ---- public scenario surface ---------------------------------------------

    pub fn partition(&mut self, node: NodeId) {
        self.partitioned.insert(node);
        self.queue.retain(|m| m.src != node && m.dst != node);
        self.record(
            TraceAction::Partition { node: node.0 },
            format!("N{node} is isolated from the cluster"),
        );
    }

    pub fn heal(&mut self, node: NodeId) {
        self.partitioned.remove(&node);
        self.record(
            TraceAction::Heal { node: node.0 },
            format!("N{node} rejoins the cluster"),
        );
    }

    /// # Panics
    ///
    /// Panics if `leader` is not present in the simulated cluster. Callers must
    /// elect or add the node before they append through it.
    pub fn leader_append(&mut self, leader: NodeId, n: usize) {
        let epoch = self
            .nodes
            .get_mut(&leader)
            .expect("no entry found for key")
            .append_in_current_epoch(n);
        self.record(
            TraceAction::Append {
                node: leader.0,
                count: n,
            },
            format!("Leader N{leader} appends {n} record(s) in epoch {epoch}"),
        );
    }

    #[must_use]
    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.machine.role().is_leader())
            .map(|n| n.id)
            .collect()
    }
}
