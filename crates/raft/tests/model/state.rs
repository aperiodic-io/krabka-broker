//! The fingerprinted model state, the action alphabet the checker explores, and
//! the read-only queries over a state. Every one of these types is hashed into
//! the state space, so they are grouped here where their `Eq + Hash` derives
//! stay visible together.

use {
    super::{
        log::ModelLog,
        spec::{AppenderId, ClientId, KraftLogSpec},
    },
    krabka_raft::kraft::{
        QuorumStateMachine,
        action::TimerKind,
        event::Event,
        role::Role,
        types::{Epoch, NodeId},
    },
    stateright::semantics::LinearizabilityTester,
    std::collections::{BTreeMap, BTreeSet},
};

/// One node: the real consensus machine, its log, and its committed high
/// watermark.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeModel {
    pub machine: QuorumStateMachine,
    pub log: ModelLog,
    pub high_watermark: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CommitPoint {
    KRaftHighWatermark,
    WalQuorumDurable,
}

/// An in-flight message. The network is an unordered multiset of these.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Envelope {
    pub src: NodeId,
    pub dst: NodeId,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelState {
    pub nodes: BTreeMap<NodeId, NodeModel>,
    /// Unordered in-flight messages. A `BTreeSet` gives a deterministic `Hash`
    /// and `Eq`, and identical duplicate envelopes collapse to one. Explicit
    /// `DuplicateDeliver` actions model network duplication without an
    /// accumulation of copies in the set.
    pub network: BTreeSet<Envelope>,
    /// Linearizability auxiliary state, recomputed and fingerprinted for each
    /// state.
    pub linz: LinearizabilityTester<ClientId, KraftLogSpec>,
    /// Client appends not yet observed as committed, keyed by the
    /// leader-assigned offset they were written at. Each entry names the
    /// durability frontier that records its `on_return`.
    pub pending: BTreeMap<i64, (ClientId, u64, CommitPoint)>,
    /// Durable end offset on each diskless WAL member. A diskless append is
    /// acknowledged only after the majority-th frontier passes its offset.
    pub wal_frontiers: BTreeMap<NodeId, i64>,
    /// Appender identities that have entered the history. The action generator
    /// canonicalizes the first identity to zero, which is a symmetry reduction
    /// over otherwise exchangeable stateless appenders.
    pub appenders_seen: BTreeSet<AppenderId>,
    /// Authoritative committed client values, in commit order. It grows as
    /// appends commit, and the linearizability return values are checked
    /// against it.
    pub committed: Vec<u64>,
    /// The leader epoch stamped on every committed raft offset, indexed by that
    /// offset: `committed_epochs[o]` is the epoch of offset `o`, recorded at the
    /// moment the high watermark first passed it.
    ///
    /// It covers every committed offset, control records included, where
    /// [`ModelState::committed`] holds only client values. It is append-only,
    /// so it is the model's memory of what the cluster has irrevocably
    /// promised, which is what `leader_completeness` holds a newly elected
    /// leader to.
    pub committed_epochs: Vec<Epoch>,
    /// Total client appends issued so far. `ConsensusModel::max_appends`
    /// bounds it.
    pub appends_issued: u32,
    /// Crashed, that is unreachable, nodes.
    ///
    /// This is an omission model. A crashed node sends nothing, receives
    /// nothing, and is offered no actions until `Recover`. On `Crash` the model
    /// also drops its in-flight messages. That is conservative: a real
    /// crash-stop can still let already-sent messages arrive, so the model would
    /// miss a violation that needs such a delivery. It stays sound, because it
    /// only removes interleavings.
    ///
    /// The node's `QuorumStateMachine` keeps its durable state. A model of
    /// volatile-state loss on restart is out of scope for this phase, because
    /// there is no public reset API.
    pub crashed: BTreeSet<NodeId>,
    /// Latched when a check-quorum expiry left its node still leading.
    ///
    /// Check-quorum is a property of a *transition*, not of a single state: the
    /// timer is only ever re-armed once a majority has fetched, so reaching its
    /// deadline is itself the proof that contact was lost, and the leader must
    /// be gone by the time the transition ends. The transition records the
    /// verdict here so the `check_quorum_expiry_ends_leadership` property can
    /// read it out of the fingerprinted state.
    pub check_quorum_violation: bool,
    /// Set once some node has actually reached [`Role::Resigned`], which is the
    /// witness that the resignation path is reachable at all.
    pub leader_resigned: bool,
    /// What the transition into this state showed a witness, if anything. It
    /// is cleared by the next transition, so it adds states only where such a
    /// transition happens.
    pub step_witness: Option<StepWitness>,
}

/// A one-transition observation that a `sometimes` property looks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StepWitness {
    /// A vote request was refused for log recency alone: the same voter, in
    /// the same state, grants it once the candidate's log end is replaced by
    /// its own.
    StaleCandidateRefused,
    /// The production `TruncateTo` removed entries: the divergence path a
    /// disagreeing follower takes, since the model never overwrites its log.
    DivergentLogTruncated,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ModelAction {
    Deliver(Envelope),
    Timeout(NodeId, TimerKind),
    /// A client appends `value`, as `client`, to the single current leader.
    ClientAppend(ClientId, u64),
    /// A diskless WAL appender connected to `via` reserves through the same
    /// ordered controller path, so the append still lands at the current leader.
    AppendVia(AppenderId, ClientId, u64),
    /// Persist the diskless WAL prefix through `end_offset` on one member.
    WalFsync(NodeId, i64),
    /// Drops an in-flight message without a delivery. This models network
    /// loss.
    DropMsg(Envelope),
    /// Delivers a copy of an in-flight message and leaves the original queued.
    /// This models network duplication.
    DuplicateDeliver(Envelope),
    /// A node crashes and becomes unreachable. This is the omission model.
    Crash(NodeId),
    /// A crashed node recovers and becomes reachable again.
    Recover(NodeId),
}

/// High watermark of a node regardless of role.
pub(super) fn node_high_watermark(n: &NodeModel) -> i64 {
    match n.machine.role() {
        Role::Leader { high_watermark, .. } => *high_watermark,
        _ => n.high_watermark,
    }
}

/// True if and only if `n` currently believes it is the leader.
pub(super) fn is_leader(n: &NodeModel) -> bool { n.machine.role().is_leader() }

/// Select the one authority an appender would route to. During an election
/// handoff the model can contain leaders from different epochs; the
/// highest-epoch live leader is the current authority. Node id is only a stable
/// tie-breaker, and election safety separately rejects two leaders in one
/// epoch.
pub(super) fn live_authority(state: &ModelState) -> Option<NodeId> {
    state
        .nodes
        .iter()
        .filter(|(id, node)| is_leader(node) && !state.crashed.contains(*id))
        .max_by_key(|(id, node)| (node.machine.quorum_state().leader_epoch, **id))
        .map(|(&id, _)| id)
}
