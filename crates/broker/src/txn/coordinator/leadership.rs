//! Which `__transaction_state` partitions this broker leads, and the load
//! status of each one.
//!
//! Kafka's `TransactionCoordinator.onElection` loads a state partition when
//! the broker becomes its leader, and `onResignation` unloads it when the
//! broker stops leading it. While a partition loads,
//! `TransactionStateManager.getAndMaybeAddTransactionState` answers
//! `COORDINATOR_LOAD_IN_PROGRESS`, and it answers `NOT_COORDINATOR` for a
//! partition that has no cache entry.
//!
//! Many tasks apply the leadership from the metadata image that each one read:
//! the reconcile loop, every transaction handler, the completion task and the
//! expiry sweeps. A task can apply its image after another task has applied a
//! newer one. Kafka orders elections and resignations by the coordinator
//! epoch, which is the partition leader epoch
//! (`removeLoadingPartitionWithEpoch`). This module does the same: an image
//! changes a partition only when its leader epoch is higher than the epoch
//! that the coordinator already holds. Leader epochs restart when the topic is
//! deleted and created again, so each value also records the topic id.

use {
    crate::{
        codes::{COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR},
        txn::bootstrap,
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::{LeaderEpoch, MetadataImage, NodeId},
    std::collections::HashMap,
    uuid::Uuid,
};

/// The load status of a `__transaction_state` partition that this broker
/// leads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadStatus {
    /// The image names this broker as the leader, but the partition log is not
    /// open locally yet. The first refresh after the log opens starts the
    /// load.
    Pending,
    /// A replay of the partition log runs.
    Loading,
    /// The replay ended. The partition serves requests.
    Loaded,
    /// The replay failed. The partition answers `NOT_COORDINATOR` until the
    /// next election, as a Kafka partition whose load failed has no cache
    /// entry.
    Failed,
}

/// One term of this broker as the leader of a state partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LeaderTerm {
    /// Identifies the load of this term. A load or an append that ends after
    /// a newer term started does not publish its state.
    pub(super) generation: u64,
    pub(super) status: LoadStatus,
}

/// The leadership of one `__transaction_state` partition, from the newest
/// leader epoch that the coordinator has seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StatePartitionLeadership {
    /// The `__transaction_state` topic id of the image that set this value.
    pub(super) topic_id: Uuid,
    /// The partition leader epoch of the image that set this value.
    pub(super) leader_epoch: LeaderEpoch,
    /// The term of this broker, or `None` when another broker leads.
    pub(super) term: Option<LeaderTerm>,
}

/// Map from `__transaction_state` partition to its newest known leadership.
pub(super) type StatePartitionLeaders = HashMap<PartitionIndex, StatePartitionLeadership>;

/// The work that one application of an image asks for.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct LeadershipChanges {
    /// Partitions whose in-memory transactions must be dropped, because a new
    /// term started or another broker leads now.
    pub(super) unload: Vec<PartitionIndex>,
    /// Partitions to replay, each with the generation of its new load.
    pub(super) load: Vec<(PartitionIndex, u64)>,
}

impl LeadershipChanges {
    pub(super) fn is_empty(&self) -> bool { self.unload.is_empty() && self.load.is_empty() }
}

/// Applies the `__transaction_state` leadership of `image` to `known`.
///
/// A partition changes its leader only when `image` gives it a leader epoch
/// higher than the one in `known`, or a different topic id (the topic was
/// deleted and created again, and its epochs restarted).
///
/// When `image` does not hold the topic, the image is either older than the
/// topic or newer than its deletion. `is_local` tells the two apart: the
/// reconcile loop removes the local log of a deleted topic, so a partition
/// whose log is gone loses its value, and a partition whose log is still open
/// keeps it. `is_local` also tells whether a new term can load now.
/// `next_generation` gives the generation of each new load.
pub(super) fn apply_image(
    known: &mut StatePartitionLeaders,
    node_id: NodeId,
    image: &MetadataImage,
    is_local: impl Fn(PartitionIndex) -> bool,
    mut next_generation: impl FnMut() -> u64,
) -> LeadershipChanges {
    let mut changes = LeadershipChanges::default();
    let topic_id = image.topic(bootstrap::TOPIC).map(|topic| topic.topic_id);
    known.retain(|partition, leadership| {
        let keep = match topic_id {
            Some(topic_id) => leadership.topic_id == topic_id,
            None => is_local(*partition),
        };
        if !keep && leadership.term.is_some() {
            changes.unload.push(*partition);
        }
        keep
    });
    changes
        .unload
        .sort_unstable_by_key(|partition| partition.get());
    let Some(topic_id) = topic_id else {
        return changes;
    };
    for partition in image.partitions_of(bootstrap::TOPIC) {
        let index = PartitionIndex(partition.partition);
        let current = known.get(&index).copied();
        let newer = current.is_none_or(|current| partition.leader_epoch > current.leader_epoch);
        if newer {
            if current.is_some_and(|current| current.term.is_some()) {
                changes.unload.push(index);
            }
            let term = (partition.leader == node_id)
                .then(|| start_term(index, is_local(index), &mut next_generation, &mut changes));
            known.insert(index, StatePartitionLeadership {
                topic_id,
                leader_epoch: partition.leader_epoch,
                term,
            });
            continue;
        }
        let Some(mut current) = current else {
            continue;
        };
        let retry = current
            .term
            .is_some_and(|term| term.status == LoadStatus::Pending && is_local(index));
        if retry {
            current.term = Some(start_term(index, true, &mut next_generation, &mut changes));
            known.insert(index, current);
        }
    }
    changes
}

fn start_term(
    index: PartitionIndex,
    local: bool,
    next_generation: &mut impl FnMut() -> u64,
    changes: &mut LeadershipChanges,
) -> LeaderTerm {
    let generation = next_generation();
    if local {
        changes.load.push((index, generation));
    }
    LeaderTerm {
        generation,
        status: if local {
            LoadStatus::Loading
        } else {
            LoadStatus::Pending
        },
    }
}

/// The load status of `partition`, or `None` when this broker does not lead
/// it.
pub(super) fn status(
    known: &StatePartitionLeaders,
    partition: PartitionIndex,
) -> Option<LoadStatus> {
    known
        .get(&partition)
        .and_then(|leadership| leadership.term)
        .map(|term| term.status)
}

/// The generation of the loaded term of `partition`, or `None` when the
/// partition is not loaded.
pub(super) fn loaded_generation(
    known: &StatePartitionLeaders,
    partition: PartitionIndex,
) -> Option<u64> {
    known
        .get(&partition)
        .and_then(|leadership| leadership.term)
        .filter(|term| term.status == LoadStatus::Loaded)
        .map(|term| term.generation)
}

/// The Kafka error code for a request to a partition in `status`, or `None`
/// when the partition serves requests.
///
/// `TransactionStateManager.getAndMaybeAddTransactionState` answers
/// `COORDINATOR_LOAD_IN_PROGRESS` while the partition loads, and
/// `NOT_COORDINATOR` when this broker has no loaded cache for it.
pub(crate) fn coordinator_error(status: Option<LoadStatus>) -> Option<i16> {
    match status {
        Some(LoadStatus::Loaded) => None,
        Some(LoadStatus::Pending | LoadStatus::Loading) => Some(COORDINATOR_LOAD_IN_PROGRESS),
        Some(LoadStatus::Failed) | None => Some(NOT_COORDINATOR),
    }
}

#[cfg(test)]
mod tests;
