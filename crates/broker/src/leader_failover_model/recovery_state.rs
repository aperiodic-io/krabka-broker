//! The bounded configuration of the KIP-966 winner-selection model, the
//! gathered responses its search enumerates, and the projection onto the real
//! `ReplicaLogInfo`.
//!
//! The model state has to be hashable for the search, and `ReplicaLogInfo` is
//! not, so the reported log state is mirrored here and converted on the way
//! into the production selectors.
//!
//! # The published sets are configuration, not state
//!
//! `select_leader` reads three things: the responses, the partition's
//! eligible-leader-replica set, and the cluster's witnesses. Only the first
//! moves while a recovery runs -- the ELR was published before the poll
//! started and the witness set is a static role -- so the search enumerates
//! responses and the model is instantiated once per published set. That keeps
//! the two sets orthogonal to the state space instead of multiplying it, and
//! it lets a configuration name an ELR member that never answers, which is a
//! case the election has to get right.

use {
    crate::unclean_recovery::ReplicaLogInfo,
    krabka_raft::NodeId,
    std::collections::{BTreeMap, HashSet},
};

/// One replica's reported log state. This is a hashable mirror of
/// `ReplicaLogInfo`, which isn't `Hash`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct ReplicaLog {
    pub(super) last_written_leader_epoch: i32,
    pub(super) log_end_offset: i64,
    pub(super) current_leader_epoch: i32,
}

/// Bounded config for the KIP-966 winner-selection model.
pub(super) struct RecoveryModel {
    pub(super) replicas: Vec<NodeId>,
    pub(super) max_epoch: i32,
    pub(super) max_leo: i64,
    pub(super) known_leader_epoch: i32,
    /// The partition's published eligible-leader-replica set, as the wire ids
    /// `select_leader` takes. It may name a replica that never answers.
    pub(super) eligible: Vec<i32>,
    /// The cluster's `broker.witness` nodes. Neither election rule may elect
    /// one, ELR membership included.
    pub(super) witnesses: HashSet<NodeId>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct RecoveryState {
    pub(super) responses: BTreeMap<NodeId, ReplicaLog>,
    pub(super) known_leader_epoch: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum RecoveryAction {
    AddResponse {
        node: NodeId,
        last_written_epoch: i32,
        leo: i64,
        current_epoch: i32,
    },
}

impl RecoveryModel {
    /// The three-replica recovery, with `eligible` published as the ELR and
    /// `witness_ids` holding the `broker.witness` role.
    pub(super) fn offset_recovery(eligible: &[i32], witness_ids: &[u64]) -> Self {
        Self {
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            max_epoch: 2,
            max_leo: 2,
            known_leader_epoch: 1,
            eligible: eligible.to_vec(),
            witnesses: witness_ids.iter().copied().map(NodeId).collect(),
        }
    }

    /// The replicas that may lead: the ones the witness role does not bar.
    pub(super) fn electable(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.replicas
            .iter()
            .copied()
            .filter(|node| !self.witnesses.contains(node))
    }

    /// Whether this configuration can reach an election that the ELR rule, and
    /// not the most-complete-log fallback, decided differently: it needs an
    /// electable ELR member, an electable non-member for it to outrank, and a
    /// log length for them to differ by.
    pub(super) fn can_reach_an_elr_upset(&self) -> bool {
        self.max_leo > 0
            && self
                .electable()
                .any(|node| self.eligible.contains(&wire_id(node)))
            && self
                .electable()
                .any(|node| !self.eligible.contains(&wire_id(node)))
    }
}

/// The wire id of a modelled node. Every id in this model is small.
pub(super) fn wire_id(node: NodeId) -> i32 {
    i32::try_from(node.0).expect("a modelled node id fits in the wire type")
}

/// Project the gathered responses into the real wire-decoupled type.
pub(super) fn infos_of(s: &RecoveryState) -> Vec<ReplicaLogInfo> {
    s.responses
        .iter()
        .map(|(id, l)| ReplicaLogInfo {
            broker_id: *id,
            last_written_leader_epoch: l.last_written_leader_epoch,
            log_end_offset: l.log_end_offset,
            current_leader_epoch: l.current_leader_epoch,
        })
        .collect()
}
