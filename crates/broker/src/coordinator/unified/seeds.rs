//! Hydration seeds that carry a group's replayed state from bootstrap into a
//! freshly-spawned actor.
//!
//! One seed type exists per group protocol, and each is the exact projection
//! of that protocol's `__consumer_offsets` records. They live together here
//! because both the replay paths and the registry that spawns the actors
//! depend on them.

use super::{persistence_next_gen, share, streams};

/// Hydration seed that the bootstrap replayer passes into a freshly-spawned
/// [`actor::GroupActorHandle`].
///
/// All fields come directly from records decoded out of `__consumer_offsets`.
///
/// [`actor::GroupActorHandle`]: super::actor::GroupActorHandle
#[derive(Debug, Default, Clone, PartialEq)]
pub struct GroupSeed {
    pub group_epoch: i32,
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    pub target_epoch: i32,
    /// The `AssignmentTimestamp` of the last target assignment metadata
    /// record, 0 when unknown.
    pub assignment_timestamp_ms: i64,
    pub members: std::collections::HashMap<String, persistence_next_gen::MemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, persistence_next_gen::TargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, persistence_next_gen::CurrentMemberAssignmentValue>,
    /// The `ConsumerGroupRegularExpression` records: what each regular
    /// expression that the members subscribe to resolved to, by regular
    /// expression.
    pub resolved_regexes:
        std::collections::HashMap<String, persistence_next_gen::RegularExpressionValue>,
    /// Kafka's `ConsumerGroup.hasSubscriptionMetadataRecord`: the log holds a
    /// deprecated `ConsumerGroupPartitionMetadata` value (key v4) that no
    /// tombstone has removed yet.
    pub has_subscription_metadata_record: bool,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`].
///
/// All fields come from share-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ShareGroupSeed {
    pub group_epoch: i32,
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    pub target_epoch: i32,
    /// The `AssignmentTimestamp` of the last target assignment metadata
    /// record, 0 when unknown.
    pub assignment_timestamp_ms: i64,
    pub members:
        std::collections::HashMap<String, share::persistence::ShareGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        share::persistence::ShareGroupCurrentMemberAssignmentValue,
    >,
    /// KIP-932 `ShareGroupStatePartitionMetadata`, key v15. It holds the
    /// `(topic_id, partition)` share-states this group has already
    /// initialized, and the topic ids whose share-state the broker deletes.
    /// The lifecycle hook can then skip a re-initialization of those
    /// partitions on restart.
    pub state_partition_metadata: share::persistence::ShareGroupStatePartitionMetadataValue,
}

/// Hydration seed for a [`streams::actor::StreamsGroupActorHandle`], per
/// KIP-1071.
///
/// All fields come from streams-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StreamsGroupSeed {
    pub group_epoch: i32,
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    /// The `ValidatedTopologyEpoch` of the last group metadata record, or 0,
    /// the value of a new group, before one.
    pub validated_topology_epoch: i32,
    /// The `LastAssignmentConfigs` of the last group metadata record, by key;
    /// empty for a null list.
    pub last_assignment_configs: std::collections::BTreeMap<String, String>,
    /// The KIP-1331 description epochs (Kafka trunk's tags 2 and 3) of the
    /// last group metadata record, -1 each when it carried none. A replay
    /// keeps them whatever mode the broker writes in.
    pub description_epochs: streams::persistence::DescriptionEpochs,
    pub assignment_epoch: i32,
    /// The `AssignmentTimestamp` of the last target assignment metadata
    /// record, 0 when unknown.
    pub assignment_timestamp_ms: i64,
    pub topology: Option<streams::persistence::StreamsGroupTopologyValue>,
    pub members:
        std::collections::HashMap<String, streams::persistence::StreamsGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    >,
    pub current_per_member: std::collections::HashMap<
        String,
        streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    >,
}

// Each protocol owns its record types and placeholder factory; member-map replay is shared.
macro_rules! replay_member_maps {
    ($seed:ty, $metadata:ty, $current:ty, $kind:ident) => {
        impl $seed {
            pub(super) fn install_current_member(
                &mut self,
                member_id: &str,
                value: $current,
                uninitialized: impl FnOnce() -> $metadata,
            ) {
                self.members
                    .entry(member_id.into())
                    .or_insert_with(uninitialized);
                self.current_per_member.insert(member_id.into(), value);
            }

            pub(super) fn member_tombstone(
                &self,
                member_id: &str,
            ) -> Result<bool, crate::error::BrokerError> {
                super::replay_policy::member_tombstone(
                    super::replay_policy::ModernGroupType::$kind,
                    member_id,
                    self.members.contains_key(member_id).then(|| {
                        self.current_per_member
                            .get(member_id)
                            .map_or(0, |current| current.member_epoch)
                    }),
                    self.target_per_member.contains_key(member_id),
                )
            }

            pub(super) fn remove_replayed_member(&mut self, member_id: &str) {
                self.members.remove(member_id);
                self.current_per_member.remove(member_id);
            }
        }
    };
}

replay_member_maps!(
    GroupSeed,
    persistence_next_gen::MemberMetadataValue,
    persistence_next_gen::CurrentMemberAssignmentValue,
    Consumer
);
replay_member_maps!(
    ShareGroupSeed,
    share::persistence::ShareGroupMemberMetadataValue,
    share::persistence::ShareGroupCurrentMemberAssignmentValue,
    Share
);
replay_member_maps!(
    StreamsGroupSeed,
    streams::persistence::StreamsGroupMemberMetadataValue,
    streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    Streams
);

/// Snapshot member metadata, current assignments, then the protocol's target map.
macro_rules! snapshot_member_maps {
    ($state:ident; $members:ident, $current:ident, $targets:ident;
        $metadata:path, $assignment:path; |$id:ident, $member:ident| $target:block
    ) => {
        let mut $members = ::std::collections::HashMap::new();
        let mut $targets = ::std::collections::HashMap::new();
        let mut $current = ::std::collections::HashMap::new();
        for ($id, $member) in &$state.members {
            $members.insert($id.clone(), $metadata($member));
            $current.insert($id.clone(), $assignment($member));
            $target
        }
    };
}
pub(super) use snapshot_member_maps;

/// Restore epochs only for members whose metadata survived replay, then apply
/// the protocol's original assignment fields in the same iteration.
macro_rules! hydrate_member_epochs {
    ($state:ident, $seed:ident; $member:ident, $current:ident { $($assignment:tt)* }) => {
        for (mid, $current) in $seed.current_per_member {
            if let Some($member) = $state.members.get_mut(&mid) {
                $member.member_epoch = $current.member_epoch;
                $member.previous_member_epoch = $current.previous_member_epoch;
                $($assignment)*
            }
        }
    };
}
pub(super) use hydrate_member_epochs;

impl GroupSeed {
    /// The consumer group that Kafka's replay creates for the first record of
    /// a group id: a new `ConsumerGroup`, whose group epoch and assignment
    /// epoch start at 1 (`TargetAssignmentMetadata.INITIAL`).
    #[must_use]
    pub fn new_group() -> Self {
        Self {
            group_epoch: super::INITIAL_GROUP_EPOCH,
            target_epoch: super::INITIAL_GROUP_EPOCH,
            ..Self::default()
        }
    }
}

impl ShareGroupSeed {
    /// The share group that Kafka's replay creates for the first record of a
    /// group id: a new `ShareGroup`, whose group epoch and assignment epoch
    /// start at 1.
    #[must_use]
    pub fn new_group() -> Self {
        Self {
            group_epoch: super::INITIAL_GROUP_EPOCH,
            target_epoch: super::INITIAL_GROUP_EPOCH,
            ..Self::default()
        }
    }
}

impl StreamsGroupSeed {
    /// The streams group that Kafka's replay creates for the first record of
    /// a group id: a new `StreamsGroup`, whose group epoch and assignment
    /// epoch start at 1 and whose validated topology epoch starts at 0.
    #[must_use]
    pub fn new_group() -> Self {
        Self {
            group_epoch: streams::state::INITIAL_EPOCH,
            assignment_epoch: streams::state::INITIAL_EPOCH,
            ..Self::default()
        }
    }
}
