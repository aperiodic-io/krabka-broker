//! The [`MemberState`] record for one KIP-848 consumer-group member and the
//! [`ClassicMemberFacade`] a classic member carries inside an upgraded group.
//!
//! A member is pure data plus accessors. The group-level transitions that add,
//! remove, and reconcile members live in the `group` and `reconcile` siblings,
//! and the topics that a member's regex subscription resolves to live in the
//! group, in `regex`.

use {
    crate::coordinator::unified::persistence_next_gen::MemberAssignmentState,
    bytes::Bytes,
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant},
    },
};

/// Classic-protocol state for a member hosted inside a consumer group: Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`.
///
/// The value is `None` on a native consumer-protocol member. It is `Some` once
/// the broker upgraded a classic member's group, and when a classic member
/// joins a consumer group.
///
/// The member keeps speaking the classic `JoinGroup`, `SyncGroup`, and
/// `Heartbeat` protocol. The coordinator serves it from the consumer-group
/// machinery: its `JoinGroup` reconciles it as a heartbeat does, its
/// `SyncGroup` returns its assigned partitions as a
/// `ConsumerProtocolAssignment` blob, and its `Heartbeat` asks it to rejoin
/// while it has not reconciled to the group's target assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberFacade {
    /// `(protocol_name, metadata)` pairs the member proposed in `JoinGroup`,
    /// in request order. The first names the protocol the member is answered
    /// with, and its metadata's version prefix the version of the assignment
    /// blob it receives. A downgrade restores the classic member from them.
    pub supported_protocols: Vec<(String, Bytes)>,
    /// The member's classic `session.timeout.ms`.
    pub session_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. When set, the group's resolution
    /// of it (`GroupState::resolved_regex`) names the topics that the member
    /// subscribes to beside `subscribed_topic_names`. `None` means "no regex",
    /// that is an exact-name subscription only. The empty pattern a client
    /// sends to drop its regex is stored as `None`, as Kafka's `isNotEmpty`
    /// gate reads it.
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout: Duration,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub assignment_state: MemberAssignmentState,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub partitions_pending_revocation: HashMap<Uuid, Vec<i32>>,
    /// The epoch at which each partition the member holds was assigned to
    /// it, by topic id and partition index (KIP-1251). The map covers
    /// `assigned_partitions` and `partitions_pending_revocation`, which are
    /// disjoint. A pending partition keeps the epoch it was assigned at, as
    /// Kafka's `ConsumerGroupMember.pendingRevocationEpoch` does.
    ///
    /// Change it only through [`MemberState::stamp_assignment_epochs`] and
    /// [`MemberState::reset_assignment_epochs`], after the partition maps
    /// change.
    pub assignment_epochs: HashMap<Uuid, HashMap<i32, i32>>,
    pub last_seen: Instant,
    /// Set if and only if this is a classic member hosted in an upgraded
    /// group.
    pub classic: Option<ClassicMemberFacade>,
}

impl MemberState {
    /// A native member before subscriptions, assignments or epochs have been installed.
    /// The caller supplies the observation time rather than reading another clock here.
    pub(crate) fn empty(member_id: impl Into<String>, last_seen: Instant) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            client_id: String::new(),
            client_host: String::new(),
            subscribed_topic_names: HashSet::new(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::ZERO,
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs: HashMap::new(),
            last_seen,
            classic: None,
        }
    }

    /// `true` if this member speaks the classic protocol inside an upgraded
    /// group. Its RPCs are then `JoinGroup`, `SyncGroup`, and `Heartbeat`, not
    /// `ConsumerGroupHeartbeat`.
    #[must_use]
    pub fn is_classic(&self) -> bool { self.classic.is_some() }

    /// `true` when the member holds `partition` of `topic_id`, as assigned or
    /// as pending revocation.
    #[must_use]
    pub fn holds(&self, topic_id: &Uuid, partition: i32) -> bool {
        [
            &self.assigned_partitions,
            &self.partitions_pending_revocation,
        ]
        .into_iter()
        .any(|map| {
            map.get(topic_id)
                .is_some_and(|parts| parts.contains(&partition))
        })
    }

    /// The epoch at which the member was assigned `partition` of `topic_id`.
    /// This is Kafka's `ConsumerGroupMember.assignmentEpoch`, then
    /// `pendingRevocationEpoch`. It returns `None` when the member does not
    /// hold the partition.
    #[must_use]
    pub fn assignment_epoch(&self, topic_id: &Uuid, partition: i32) -> Option<i32> {
        if !self.holds(topic_id, partition) {
            return None;
        }
        self.assignment_epochs
            .get(topic_id)
            .and_then(|epochs| epochs.get(&partition))
            .copied()
    }

    /// Brings `assignment_epochs` in line with the partitions the member
    /// holds: a partition it no longer holds loses its epoch, and a partition
    /// it holds with no epoch yet gets `epoch`.
    ///
    /// Kafka's `CurrentAssignmentBuilder.computeNextAssignment` stamps a newly
    /// assigned partition with the target assignment epoch, which is the
    /// member epoch the member moves to. A partition the member keeps, or
    /// moves to pending revocation, keeps its epoch.
    pub fn stamp_assignment_epochs(&mut self, epoch: i32) {
        let mut stamped: HashMap<Uuid, HashMap<i32, i32>> = HashMap::new();
        for (topic_id, parts) in self
            .assigned_partitions
            .iter()
            .chain(&self.partitions_pending_revocation)
        {
            let previous = self.assignment_epochs.get(topic_id);
            let epochs = stamped.entry(*topic_id).or_default();
            for &partition in parts {
                let at = previous
                    .and_then(|previous| previous.get(&partition))
                    .copied()
                    .unwrap_or(epoch);
                epochs.insert(partition, at);
            }
        }
        stamped.retain(|_, epochs| !epochs.is_empty());
        self.assignment_epochs = stamped;
    }

    /// Sets the epoch of every held partition to `epoch`, as Kafka's
    /// `ConsumerGroupMember.Builder.resetAssignedPartitionsEpochsToZero` does
    /// with 0 for a static member that leaves for a while.
    pub fn reset_assignment_epochs(&mut self, epoch: i32) {
        self.assignment_epochs.clear();
        self.stamp_assignment_epochs(epoch);
    }
}
