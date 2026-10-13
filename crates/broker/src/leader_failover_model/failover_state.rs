//! The bounded configuration of the failover-scan model, the state its search
//! enumerates, the actions that move between two states, and the projection
//! onto the `PartitionRecord` that the real `failover_one` reads.
//!
//! Order is significant: the replica order is what every pick walks -- the
//! clean one, the KIP-966 ELR one and the KIP-841 one, as Kafka's
//! `electAnyLeader` does -- and the emitted ISR keeps the ISR order, so both
//! stay `Vec` rather than a set. The assignment is `[1, 3, 2]` rather than
//! sorted, because an ISR that a re-admission rebuilt is sorted by id: a
//! partition whose ISR holds 2 and 3 then lists them in the opposite order to
//! the assignment, which is what tells a clean pick that walks the ISR apart
//! from Kafka's.

use {
    super::elr,
    crate::config_keys::RecoveryStrategy,
    krabka_metadata::{MetadataImage, PartitionRecord},
    krabka_raft::NodeId,
    std::collections::{BTreeSet, HashSet},
};

/// Bounded config for the failover-scan model.
pub(super) struct FailoverModel {
    pub(super) replicas: Vec<NodeId>, // replicas[0] is the fixed initial leader
    /// Data-bearing witnesses among `replicas`. A witness stays in the ISR and
    /// counts toward min-ISR, and it never leads. `replicas[0]` must not be a
    /// witness, because it is the initial leader.
    pub(super) witnesses: HashSet<NodeId>,
    pub(super) strategy: RecoveryStrategy,
    pub(super) unclean_enabled: bool,
    pub(super) max_epoch: i32,
    /// The metadata image the real ELR maintenance rule reads the topic's
    /// `min.insync.replicas` out of.
    pub(super) image: MetadataImage,
    /// `min.insync.replicas` as the image resolves it. Above 1 a replica can
    /// leave an ISR that is about to fall below it, which is the only way
    /// KIP-966 puts one in the eligible-leader set.
    pub(super) min_isr: usize,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct FailoverState {
    pub(super) leader: NodeId,
    pub(super) isr: Vec<NodeId>, // order significant (an election emits it in this order)
    pub(super) replicas: Vec<NodeId>, // fixed; order significant (every pick walks it)
    pub(super) leader_epoch: i32,
    pub(super) alive: BTreeSet<NodeId>,
    /// The published KIP-966 eligible-leader set, as the sorted wire ids the
    /// real maintenance rule returns and `failover_one` reads.
    pub(super) elr: Vec<i32>,
    /// Ghost: some election along the path was decided by `failover_one`'s
    /// ELR rung. A property reads one state and an election is a transition,
    /// so this is what the transition leaves behind for the anti-vacuity
    /// witness.
    pub(super) elected_from_elr: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum FailoverAction {
    Die(NodeId),
    Revive(NodeId),
    Failover(NodeId),
    /// The live leader proposes re-admitting a caught-up follower, and the
    /// controller rules on the `AlterPartition`.
    ExpandIsr(NodeId),
}

impl FailoverModel {
    /// `witness_ids` names the replicas that carry the witness role, and
    /// `min_isr` is the topic's `min.insync.replicas`.
    pub(super) fn config(
        strategy: RecoveryStrategy,
        unclean_enabled: bool,
        witness_ids: &[u64],
        min_isr: usize,
    ) -> Self {
        let image = elr::image(min_isr);
        let min_isr = elr::min_insync_replicas(&image);
        Self {
            // Assignment order differs from id order; see the module docs.
            replicas: vec![
                krabka_audit::NodeId(1),
                krabka_audit::NodeId(3),
                krabka_audit::NodeId(2),
            ],
            witnesses: witness_ids
                .iter()
                .copied()
                .map(krabka_audit::NodeId)
                .collect(),
            strategy,
            unclean_enabled,
            max_epoch: 6,
            image,
            min_isr,
        }
    }

    /// Whether this configuration publishes an eligible-leader set in the
    /// ordinary course. At `min.insync.replicas` 1 the rule publishes one only
    /// for an ISR that has emptied outright, so the ELR anti-vacuity
    /// witnesses are stated only above it.
    pub(super) fn elr_configured(&self) -> bool { self.min_isr > 1 }
}

/// Build a minimal `PartitionRecord` from the model state to drive the real
/// `failover_one`. This function fills the fields `failover_one` ignores with
/// dummy values.
pub(super) fn pr_of(s: &FailoverState) -> PartitionRecord {
    let epoch = krabka_metadata::LeaderEpoch(s.leader_epoch);
    PartitionRecord {
        topic: elr::TOPIC.to_string(),
        partition: 0,
        leader: s.leader,
        replicas: s.replicas.clone(),
        isr: s.isr.clone(),
        leader_epoch: epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}
