//! Hydration seeds that carry a group's replayed state from bootstrap into a
//! freshly-spawned actor.
//!
//! One seed type exists per group protocol, and each is the exact projection
//! of that protocol's `__consumer_offsets` records. They live together here
//! because both the replay paths and the registry that spawns the actors
//! depend on them.

use super::{
    persistence_next_gen::{
        CurrentMemberAssignmentValue, MemberMetadataValue, RegularExpressionValue,
        TargetAssignmentMemberValue,
    },
    share::{
        self,
        persistence::{
            ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            ShareGroupTargetAssignmentMemberValue,
        },
    },
    streams::{
        self,
        persistence::{
            StreamsGroupCurrentMemberAssignmentValue, StreamsGroupMemberMetadataValue,
            StreamsGroupTargetAssignmentMemberValue, StreamsGroupTopologyValue,
        },
    },
};

/// Hydration seed that the bootstrap replayer passes into a freshly-spawned
/// [`actor::GroupActorHandle`].
///
/// All fields come directly from records decoded out of `__consumer_offsets`.
///
/// [`actor::GroupActorHandle`]: super::actor::GroupActorHandle
#[derive(Debug, Default, Clone, PartialEq, krabka_macros::ReplayMemberMaps)]
#[replay_member_maps(protocol = "consumer")]
pub struct GroupSeed {
    pub group_epoch: i32,
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    pub target_epoch: i32,
    /// The `AssignmentTimestamp` of the last target assignment metadata
    /// record, 0 when unknown.
    pub assignment_timestamp_ms: i64,
    pub members: std::collections::HashMap<String, MemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<String, TargetAssignmentMemberValue>,
    pub current_per_member: std::collections::HashMap<String, CurrentMemberAssignmentValue>,
    /// The `ConsumerGroupRegularExpression` records: what each regular
    /// expression that the members subscribe to resolved to, by regular
    /// expression.
    pub resolved_regexes: std::collections::HashMap<String, RegularExpressionValue>,
    /// Kafka's `ConsumerGroup.hasSubscriptionMetadataRecord`: the log holds a
    /// deprecated `ConsumerGroupPartitionMetadata` value (key v4) that no
    /// tombstone has removed yet.
    pub has_subscription_metadata_record: bool,
}

/// Hydration seed for a [`share::actor::ShareGroupActorHandle`].
///
/// All fields come from share-group records decoded out of
/// `__consumer_offsets`.
#[derive(Debug, Default, Clone, PartialEq, krabka_macros::ReplayMemberMaps)]
#[replay_member_maps(protocol = "share")]
pub struct ShareGroupSeed {
    pub group_epoch: i32,
    /// The `MetadataHash` of the last group metadata record.
    pub metadata_hash: i64,
    pub target_epoch: i32,
    /// The `AssignmentTimestamp` of the last target assignment metadata
    /// record, 0 when unknown.
    pub assignment_timestamp_ms: i64,
    pub members: std::collections::HashMap<String, ShareGroupMemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<String, ShareGroupTargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, ShareGroupCurrentMemberAssignmentValue>,
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
#[derive(Debug, Default, Clone, PartialEq, krabka_macros::ReplayMemberMaps)]
#[replay_member_maps(protocol = "streams")]
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
    pub topology: Option<StreamsGroupTopologyValue>,
    pub members: std::collections::HashMap<String, StreamsGroupMemberMetadataValue>,
    pub target_per_member:
        std::collections::HashMap<String, StreamsGroupTargetAssignmentMemberValue>,
    pub current_per_member:
        std::collections::HashMap<String, StreamsGroupCurrentMemberAssignmentValue>,
}

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

#[cfg(test)]
mod replay_member_maps_tests {
    use {
        super::{
            GroupSeed, ShareGroupCurrentMemberAssignmentValue, ShareGroupSeed,
            ShareGroupTargetAssignmentMemberValue, StreamsGroupCurrentMemberAssignmentValue,
            StreamsGroupSeed, StreamsGroupTargetAssignmentMemberValue, TargetAssignmentMemberValue,
        },
        crate::{
            coordinator::unified::test_support::{
                next_current, next_member, share_member, streams_member,
            },
            error::BrokerError,
        },
        assert2::assert,
        std::cell::Cell,
    };

    struct TombstoneCase {
        result: Result<bool, BrokerError>,
        record: &'static str,
    }

    #[test]
    fn share_replay_creates_metadata_lazily_and_removes_only_current_and_metadata() {
        let mut seed = ShareGroupSeed::new_group();
        let factories = Cell::new(0);
        let current = |epoch| ShareGroupCurrentMemberAssignmentValue {
            member_epoch: epoch,
            ..Default::default()
        };
        let placeholder = || {
            factories.set(factories.get() + 1);
            share_member("placeholder")
        };
        seed.install_current_member("member", current(3), placeholder);
        assert!(factories.get() == 1);
        assert!(seed.members["member"].client_id == "placeholder");
        seed.members
            .insert("member".into(), share_member("persisted"));
        seed.install_current_member("member", current(-1), placeholder);
        assert!(factories.get() == 1);
        assert!(seed.members["member"].client_id == "persisted");
        assert!(seed.current_per_member["member"].member_epoch == -1);
        assert!(seed.member_tombstone("member").unwrap());

        seed.target_per_member.insert(
            "member".into(),
            ShareGroupTargetAssignmentMemberValue::default(),
        );
        let before = seed.clone();
        assert!(
            matches!(seed.member_tombstone("member"), Err(BrokerError::Startup(message))
            if message.contains("member exists in target assignment"))
        );
        assert!(seed == before);
        seed.remove_replayed_member("member");
        assert!(seed.members.is_empty());
        assert!(seed.current_per_member.is_empty());
        assert!(seed.target_per_member == before.target_per_member);
        seed.current_per_member.insert("member".into(), current(8));
        assert!(!seed.member_tombstone("member").unwrap());
    }

    #[test]
    fn consumer_and_streams_tombstones_keep_protocol_errors_and_require_current_tombstones() {
        let mut consumer = GroupSeed::new_group();
        consumer
            .members
            .insert("member".into(), next_member("consumer"));
        let mut streams = StreamsGroupSeed::new_group();
        streams
            .members
            .insert("member".into(), streams_member("streams"));
        for case in [
            TombstoneCase {
                result: consumer.member_tombstone("member"),
                record: "ConsumerGroupCurrentMemberAssignmentValue",
            },
            TombstoneCase {
                result: streams.member_tombstone("member"),
                record: "StreamsGroupCurrentMemberAssignmentValue",
            },
        ] {
            assert!(
                matches!(case.result, Err(BrokerError::Startup(message)) if message.contains(case.record))
            );
        }
        consumer.install_current_member("member", next_current(-1), || panic!("existing metadata"));
        streams.install_current_member(
            "member",
            StreamsGroupCurrentMemberAssignmentValue {
                member_epoch: -1,
                ..Default::default()
            },
            || panic!("existing streams metadata"),
        );
        assert!(consumer.member_tombstone("member").unwrap());
        assert!(streams.member_tombstone("member").unwrap());
        consumer
            .target_per_member
            .insert("member".into(), TargetAssignmentMemberValue::default());
        streams.target_per_member.insert(
            "member".into(),
            StreamsGroupTargetAssignmentMemberValue::default(),
        );
        for case in [
            TombstoneCase {
                result: consumer.member_tombstone("member"),
                record: "ConsumerGroupTargetAssignmentMetadataValue",
            },
            TombstoneCase {
                result: streams.member_tombstone("member"),
                record: "StreamsGroupTargetAssignmentMetadataValue",
            },
        ] {
            assert!(
                matches!(case.result, Err(BrokerError::Startup(message)) if message.contains(case.record))
            );
        }
        consumer.remove_replayed_member("member");
        streams.remove_replayed_member("member");
        assert!(consumer.target_per_member.len() == 1);
        assert!(streams.target_per_member.len() == 1);
        assert!(!consumer.member_tombstone("member").unwrap());
        assert!(!streams.member_tombstone("member").unwrap());
    }
}
