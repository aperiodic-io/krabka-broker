//! One streams-group member: its identity, its epochs, its three per-role task
//! sets, and the reconciliation state of its tasks.
//!
//! The member is the unit the KIP-848-style epoch exchange acts on, so the
//! [`StreamsMemberAssignmentState`] enum and the `i8` conversions that
//! persistence needs live beside it rather than in the group state machine.

use std::{collections::BTreeMap, time::Instant};

use krabka_log::Offset;

use super::TaskMap;

/// The reconciliation state of one streams-group member, Kafka's
/// `org.apache.kafka.coordinator.group.streams.MemberState`. It covers the
/// active, standby and warmup tasks.
///
/// This is the broker's own numbering, which counts from zero. Kafka's
/// `org.apache.kafka.coordinator.group.streams.MemberState` counts from one, so
/// the persistence layer converts through its own `StreamsMemberWireState`
/// rather than storing these discriminants. [`as_i8`](Self::as_i8) and
/// [`from_i8`](Self::from_i8) expose the discriminant for the model checks that
/// compare states as numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsMemberAssignmentState {
    /// The member's tasks match its target at its member epoch.
    #[default]
    Stable = 0,
    /// The member still owns tasks that the new target took away from it. It
    /// keeps its member epoch until a heartbeat no longer reports them.
    UnrevokedTasks = 1,
    /// The member's target includes tasks that another member still owns, or
    /// that another member of the same process still runs in another role.
    /// The member waits for their release.
    UnreleasedTasks = 2,
}

impl StreamsMemberAssignmentState {
    /// Raw `i8` discriminant for the persistence layer.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        self as i8
    }

    /// Inverse of [`as_i8`](Self::as_i8). It returns `None` for an unknown
    /// discriminant instead of a panic, so that the caller, the actor, decides
    /// how to report a corrupt persisted record.
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Stable),
            1 => Some(Self::UnrevokedTasks),
            2 => Some(Self::UnreleasedTasks),
            _ => None,
        }
    }
}

/// One member of a streams group.
///
/// A member holds three disjoint sets of tasks by role: `active`, `standby`,
/// and `warmup`. Each set keys by subtopology id and holds a sorted, deduped
/// partition list. The pending revocation maps hold the tasks of each role
/// that the member must give up before it can advance its epoch.
#[derive(Debug, Clone)]
pub struct StreamsMemberState {
    // --- identity ---
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    /// The Streams `process.id`, a stable per-process UUID string. The
    /// assignor reads it to co-locate the tasks of one process.
    pub process_id: String,
    /// Optional `(host, port)` the member advertises for interactive-query
    /// routing.
    pub user_endpoint: Option<(String, u16)>,
    /// Arbitrary `(key, value)` client tags for rack-aware and custom
    /// assignment.
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,

    // --- epochs ---
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    /// The topology epoch this member last acknowledged.
    pub topology_epoch: i32,

    // --- assignment ---
    pub assignment_state: StreamsMemberAssignmentState,
    /// Assigned active tasks: `subtopology_id` -> sorted, deduped partitions.
    pub active: TaskMap,
    /// Assigned standby tasks.
    pub standby: TaskMap,
    /// Assigned warmup tasks.
    pub warmup: TaskMap,
    /// Active tasks the member must revoke before it advances.
    pub active_pending_revocation: TaskMap,
    /// Standby tasks the member must revoke before it advances.
    pub standby_pending_revocation: TaskMap,
    /// Warmup tasks the member must revoke before it advances.
    pub warmup_pending_revocation: TaskMap,
    /// The epoch at which each active task, assigned or pending revocation,
    /// was assigned to the member: Kafka's
    /// `TasksTupleWithEpochs.activeTasksWithEpochs` (KIP-1251), which the
    /// current assignment record writes as `AssignmentEpochs`.
    pub active_epochs: BTreeMap<(String, i32), i32>,
    /// The active, standby and warmup tasks that the last heartbeat response
    /// sent to the member. A response sends the task lists again only when
    /// the assignment differs from them.
    pub sent_tasks: [TaskMap; 3],

    // --- reported catch-up progress (for warmup -> active promotion) ---
    /// `(subtopology, partition)` -> the changelog position the member last
    /// reported for that task.
    pub task_offsets: BTreeMap<(String, i32), Offset>,
    /// `(subtopology, partition)` -> the changelog end offset the member last
    /// reported for that task.
    pub task_end_offsets: BTreeMap<(String, i32), Offset>,

    pub last_seen: Instant,
}

impl StreamsMemberState {
    /// The assignment epochs of the partitions of `partitions` of
    /// `subtopology_id`, in their order, as Kafka's
    /// `StreamsCoordinatorRecordHelpers.toTaskIdsWithEpochs` lists them. A
    /// partition without a recorded epoch gets the member epoch, as Kafka's
    /// reader gives a task of a record that lists none.
    #[must_use]
    pub fn active_task_epochs(&self, subtopology_id: &str, partitions: &[i32]) -> Vec<i32> {
        partitions
            .iter()
            .map(|partition| {
                self.active_epochs
                    .get(&(subtopology_id.to_owned(), *partition))
                    .copied()
                    .unwrap_or(self.member_epoch)
            })
            .collect()
    }

    /// Constructs a newly joining member at epoch 0 with no assignment.
    ///
    /// When the client supplies no `process_id`, this method synthesizes a
    /// random UUID. A caller that already knows the process id should set the
    /// field afterwards.
    pub fn joining(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            process_id: uuid::Uuid::new_v4().to_string(),
            user_endpoint: None,
            client_tags: Vec::new(),
            rebalance_timeout_ms: 0,
            member_epoch: 0,
            previous_member_epoch: 0,
            topology_epoch: 0,
            assignment_state: StreamsMemberAssignmentState::Stable,
            active: BTreeMap::new(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: BTreeMap::new(),
            standby_pending_revocation: BTreeMap::new(),
            warmup_pending_revocation: BTreeMap::new(),
            active_epochs: BTreeMap::new(),
            sent_tasks: [BTreeMap::new(), BTreeMap::new(), BTreeMap::new()],
            task_offsets: BTreeMap::new(),
            task_end_offsets: BTreeMap::new(),
            last_seen: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn assignment_state_i8_roundtrips() {
        for s in [
            StreamsMemberAssignmentState::Stable,
            StreamsMemberAssignmentState::UnrevokedTasks,
            StreamsMemberAssignmentState::UnreleasedTasks,
        ] {
            assert!(StreamsMemberAssignmentState::from_i8(s.as_i8()) == Some(s));
        }
        assert!(StreamsMemberAssignmentState::from_i8(99).is_none());
        assert!(StreamsMemberAssignmentState::default() == StreamsMemberAssignmentState::Stable);
    }
}
