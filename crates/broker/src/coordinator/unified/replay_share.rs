//! Bootstrap replay of the KIP-932 share group records, as Kafka 4.3.1's
//! `GroupMetadataManager.replay` applies them.
//!
//! Each method applies one decoded record to the bootstrap seed and to the
//! last-known-good cache. The share-state partition metadata is read back from
//! that cache by the admin share-offset RPCs, so its accessor lives here too.
//! The rules for creating a group, ignoring a tombstone, and failing the load
//! are [`replay_policy`](super::replay_policy)'s.

use super::{
    group_coordinator::GroupCoordinator,
    replay_policy::{
        GroupLookup, LEAVE_GROUP_MEMBER_EPOCH, ModernGroupType, group_tombstone, persisted_group,
        target_metadata, target_metadata_tombstone,
    },
    seeds::ShareGroupSeed,
    share,
};
use crate::error::BrokerError;

/// The fields of a member that Kafka's replay creates from a current
/// assignment record alone: `ShareGroupMember.Builder`'s defaults.
fn uninitialized_member() -> share::persistence::ShareGroupMemberMetadataValue {
    share::persistence::ShareGroupMemberMetadataValue {
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        subscribed_topic_names: Vec::new(),
    }
}

impl GroupCoordinator {
    /// Kafka's `getOrMaybeCreatePersistedShareGroup` among the modern groups:
    /// whether the group exists once the lookup is done. The bootstrap replay
    /// refuses a classic group under the id before it calls this.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a consumer or streams
    /// group.
    pub(crate) fn persisted_share_group(
        &self,
        group_id: &str,
        create_if_not_exists: bool,
    ) -> Result<bool, BrokerError> {
        match persisted_group(
            group_id,
            ModernGroupType::Share,
            self.replayed_modern_group(group_id),
            create_if_not_exists,
        )? {
            GroupLookup::Absent => Ok(false),
            GroupLookup::Found => Ok(true),
            GroupLookup::Create => {
                self.share_seeds
                    .insert(group_id.into(), ShareGroupSeed::new_group());
                self.share_seeds_cache
                    .insert(group_id.into(), ShareGroupSeed::new_group());
                Ok(true)
            }
        }
    }

    /// Applies `update` to the replayed group and to its cached copy.
    fn update_share_seed(&self, group_id: &str, update: impl Fn(&mut ShareGroupSeed)) {
        if let Some(mut seed) = self.share_seeds.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.share_seeds_cache
                .entry(group_id.into())
                .or_insert_with(ShareGroupSeed::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    fn update_share_seed_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut ShareGroupSeed, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.share_seeds.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.share_seeds_cache
                .entry(group_id.into())
                .or_insert_with(ShareGroupSeed::new_group)
                .value_mut(),
            cached,
        );
    }

    /// Reads the replayed group, which must exist.
    fn share_seed<T>(&self, group_id: &str, read: impl FnOnce(&ShareGroupSeed) -> T) -> T {
        read(
            self.share_seeds
                .get(group_id)
                .expect("the caller looked the group up")
                .value(),
        )
    }

    /// `ShareGroupMetadataValue`: the group epoch and the metadata hash.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`.
    pub fn replay_share_group_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        self.update_share_seed(group_id, |seed| {
            seed.group_epoch = v.epoch;
            seed.metadata_hash = v.metadata_hash;
        });
        Ok(())
    }

    /// `ShareGroupMemberMetadataValue`: the member's subscription, which
    /// creates the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`.
    pub fn replay_share_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupMemberMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        self.update_share_seed_with(group_id, v, |seed, v| {
            seed.members.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ShareGroupTargetAssignmentMetadataValue`: the assignment epoch and
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`, or Kafka's
    /// `IllegalArgumentException` for an epoch below -1 or a negative time.
    pub fn replay_share_target_assignment_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        target_metadata(v.assignment_epoch, v.assignment_timestamp_ms)?;
        self.update_share_seed(group_id, |seed| {
            seed.target_epoch = v.assignment_epoch;
            seed.assignment_timestamp_ms = v.assignment_timestamp_ms;
        });
        Ok(())
    }

    /// `ShareGroupTargetAssignmentMemberValue`: one member's target, which
    /// Kafka keeps whether or not the group holds the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`.
    pub fn replay_share_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupTargetAssignmentMemberValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        self.update_share_seed_with(group_id, v, |seed, v| {
            seed.target_per_member.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ShareGroupCurrentMemberAssignmentValue`: one member's epochs and
    /// partitions. It creates a member that the group does not hold yet, with
    /// the defaults of Kafka's `ShareGroupMember.Builder`.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`.
    pub fn replay_share_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: share::persistence::ShareGroupCurrentMemberAssignmentValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        self.update_share_seed_with(group_id, v, |seed, v| {
            seed.install_current_member(member_id, v, uninitialized_member);
        });
        Ok(())
    }

    /// `ShareGroupStatePartitionMetadataValue` (key v15): the share-states
    /// that the group initialized, is initializing, and is deleting, which
    /// let the lifecycle hook skip a re-initialization after a restart.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_share_group`.
    pub fn replay_share_state_partition_metadata(
        &self,
        group_id: &str,
        v: share::persistence::ShareGroupStatePartitionMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_share_group(group_id, true)?;
        self.update_share_seed_with(group_id, v, |seed, v| {
            seed.state_partition_metadata = v;
        });
        Ok(())
    }

    /// Read the cached `ShareGroupStatePartitionMetadata` for `group_id`.
    ///
    /// The value records which `(topic_id, partition)` share-states the group
    /// has initialized. The method returns `None` for an unknown group. It
    /// drives the admin offset RPCs Describe/Alter/Delete `ShareGroupOffsets`.
    /// Those RPCs list the initialized partitions when the request omits an
    /// explicit list.
    #[must_use]
    pub fn share_state_partition_metadata(
        &self,
        group_id: &str,
    ) -> Option<share::persistence::ShareGroupStatePartitionMetadataValue> {
        self.share_seeds_cache
            .get(group_id)
            .map(|e| e.value().state_partition_metadata.clone())
    }

    /// Applies the tombstone of a share group record, as Kafka's replay of a
    /// null value does. A tombstone of a group that the replay does not hold,
    /// or of a member that the group does not hold, changes nothing.
    ///
    /// - Group metadata (k11): removes the group, once its members, its
    ///   target assignment and its target assignment metadata are gone.
    /// - Member subscription (k10): removes the member, once its current
    ///   assignment was tombstoned and its target assignment is gone.
    /// - Target assignment metadata (k12): the assignment epoch becomes -1
    ///   and the time 0, once every member's target assignment is gone.
    /// - Target assignment (k13): removes the member's target.
    /// - Current assignment (k14): the member keeps its subscription at
    ///   epoch -1 with no partitions.
    /// - State partition metadata (k15): the group forgets it.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group of another type
    /// and for a tombstone whose preconditions do not hold.
    pub fn replay_share_tombstone(
        &self,
        key: &share::persistence::ShareGroupKey,
    ) -> Result<(), BrokerError> {
        use share::persistence::ShareGroupKey as K;
        let group_id = key.group_id();
        if !self.persisted_share_group(group_id, false)? {
            return Ok(());
        }
        match key {
            K::GroupMetadata { .. } => {
                self.share_seed(group_id, |seed| {
                    group_tombstone(
                        ModernGroupType::Share,
                        group_id,
                        seed.members.len(),
                        seed.target_per_member.len(),
                        seed.target_epoch,
                    )
                })?;
                self.share_seeds.remove(group_id);
                self.share_seeds_cache.remove(group_id);
                self.group_types.remove(group_id);
            }
            K::MemberMetadata { member_id, .. } => {
                let remove = self.share_seed(group_id, |seed| seed.member_tombstone(member_id))?;
                if remove {
                    self.update_share_seed(group_id, |seed| {
                        seed.remove_replayed_member(member_id);
                    });
                }
            }
            K::TargetAssignmentMetadata { .. } => {
                self.share_seed(group_id, |seed| {
                    target_metadata_tombstone(group_id, seed.target_per_member.len())
                })?;
                self.update_share_seed(group_id, |seed| {
                    seed.target_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                });
            }
            K::TargetAssignmentMember { member_id, .. } => {
                self.update_share_seed(group_id, |seed| {
                    seed.target_per_member.remove(member_id);
                });
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                if self.share_seed(group_id, |seed| seed.members.contains_key(member_id)) {
                    self.update_share_seed(group_id, |seed| {
                        seed.current_per_member.insert(
                            member_id.clone(),
                            share::persistence::ShareGroupCurrentMemberAssignmentValue {
                                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                assigned_partitions: Vec::new(),
                            },
                        );
                    });
                }
            }
            K::StatePartitionMetadata { .. } => self.update_share_seed(group_id, |seed| {
                seed.state_partition_metadata =
                    share::persistence::ShareGroupStatePartitionMetadataValue::default();
            }),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::coordinator::unified::{
        persistence_next_gen::GroupMetadataValue,
        share::persistence::{
            ShareGroupCurrentMemberAssignmentValue, ShareGroupKey, ShareGroupMetadataValue,
            ShareGroupStatePartitionMetadataValue, ShareGroupTargetAssignmentMemberValue,
            ShareGroupTargetAssignmentMetadataValue, TopicPartitionsInfo,
        },
        test_support::{proto_uuid, replay_tombstone_constructor, share_member},
    };

    /// What a log leaves: the group that replay holds, or the message of
    /// the exception that fails the load.
    type Outcome = Result<Option<ShareGroupSeed>, String>;

    /// One record of the replayed log.
    #[derive(Clone)]
    enum Record {
        Group(i32),
        Member(&'static str),
        TargetEpoch(i32),
        Target(&'static str),
        Current(&'static str, i32),
        StateMetadata,
        ConsumerGroup,
        Tombstone(ShareGroupKey),
    }

    fn key(kind: &str, id: &str) -> ShareGroupKey {
        let group_id = "g".to_owned();
        let member_id = id.to_owned();
        match kind {
            "group" => ShareGroupKey::GroupMetadata { group_id },
            "member" => ShareGroupKey::MemberMetadata {
                group_id,
                member_id,
            },
            "target-epoch" => ShareGroupKey::TargetAssignmentMetadata { group_id },
            "target" => ShareGroupKey::TargetAssignmentMember {
                group_id,
                member_id,
            },
            "current" => ShareGroupKey::CurrentMemberAssignment {
                group_id,
                member_id,
            },
            _ => ShareGroupKey::StatePartitionMetadata { group_id },
        }
    }

    fn target() -> ShareGroupTargetAssignmentMemberValue {
        ShareGroupTargetAssignmentMemberValue {
            topic_partitions: vec![(proto_uuid(4), vec![0, 3])],
        }
    }

    fn current(epoch: i32) -> ShareGroupCurrentMemberAssignmentValue {
        ShareGroupCurrentMemberAssignmentValue {
            member_epoch: epoch,
            previous_member_epoch: epoch - 1,
            assigned_partitions: vec![(proto_uuid(4), vec![1])],
        }
    }

    fn state_metadata() -> ShareGroupStatePartitionMetadataValue {
        ShareGroupStatePartitionMetadataValue {
            initializing: Vec::new(),
            initialized: vec![TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_u128(1),
                topic_name: "orders".into(),
                partitions: vec![0, 1],
            }],
            deleting: vec![],
        }
    }

    fn replay(coord: &GroupCoordinator, record: &Record) -> Result<(), BrokerError> {
        match record.clone() {
            Record::Group(epoch) => coord.replay_share_group_metadata(
                "g",
                ShareGroupMetadataValue {
                    epoch,
                    metadata_hash: 9,
                },
            ),
            Record::Member(member) => {
                coord.replay_share_member_metadata("g", member, share_member(member))
            }
            Record::TargetEpoch(epoch) => coord.replay_share_target_assignment_metadata(
                "g",
                ShareGroupTargetAssignmentMetadataValue {
                    assignment_epoch: epoch,
                    assignment_timestamp_ms: 4,
                },
            ),
            Record::Target(member) => {
                coord.replay_share_target_assignment_member("g", member, target())
            }
            Record::Current(member, epoch) => {
                coord.replay_share_current_member_assignment("g", member, current(epoch))
            }
            Record::StateMetadata => {
                coord.replay_share_state_partition_metadata("g", state_metadata())
            }
            Record::ConsumerGroup => coord.replay_group_metadata(
                "g",
                GroupMetadataValue {
                    epoch: 1,
                    metadata_hash: 0,
                },
            ),
            Record::Tombstone(key) => coord.replay_share_tombstone(&key),
        }
    }

    /// Kafka's `GroupMetadataManager.replay` of the share group records, one
    /// log per row: the whole group that replay holds afterwards, or the
    /// exception that fails the load.
    #[test]
    fn share_records_replay_as_kafka() {
        let tombstone = replay_tombstone_constructor(key, Record::Tombstone);
        // Kafka's `ShareGroup.createGroupTombstoneRecords` for member `m`.
        let (group, kafka_deletion) = crate::coordinator::unified::test_support::replay_fixtures(
            ShareGroupSeed::new_group,
            tombstone,
            crate::coordinator::unified::test_support::AssignmentDeletionSetup::default(),
        );
        let rows: Vec<(&str, Vec<Record>, Outcome)> = vec![
            (
                "every record of a group",
                vec![
                    Record::Group(21),
                    Record::Member("m"),
                    Record::TargetEpoch(22),
                    Record::Target("m"),
                    Record::Current("m", 6),
                    Record::StateMetadata,
                ],
                group(&|seed| {
                    seed.group_epoch = 21;
                    seed.metadata_hash = 9;
                    seed.target_epoch = 22;
                    seed.assignment_timestamp_ms = 4;
                    seed.members.insert("m".into(), share_member("m"));
                    seed.target_per_member.insert("m".into(), target());
                    seed.current_per_member.insert("m".into(), current(6));
                    seed.state_partition_metadata = state_metadata();
                }),
            ),
            (
                "a current assignment creates the group and an uninitialized member",
                vec![Record::Current("m", 6)],
                group(&|seed| {
                    seed.members.insert("m".into(), uninitialized_member());
                    seed.current_per_member.insert("m".into(), current(6));
                }),
            ),
            (
                "the state partition metadata creates the group",
                vec![Record::StateMetadata],
                group(&|seed| seed.state_partition_metadata = state_metadata()),
            ),
            (
                "Kafka's deletion order removes the whole group",
                [
                    vec![
                        Record::Group(2),
                        Record::Member("m"),
                        Record::TargetEpoch(2),
                        Record::Target("m"),
                        Record::Current("m", 2),
                        Record::StateMetadata,
                    ],
                    kafka_deletion(),
                ]
                .concat(),
                Ok(None),
            ),
            (
                "tombstones of a group the log does not hold are ignored",
                kafka_deletion(),
                Ok(None),
            ),
            (
                "a member tombstone before its current assignment tombstone",
                vec![Record::Member("m"), tombstone("member", "m")],
                Err(
                    "Received a tombstone record to delete member m with invalid leave group \
                     epoch."
                        .into(),
                ),
            ),
            crate::coordinator::unified::test_support::member_deletion_with_target_case(
                (Record::Member("m"), Record::Target("m")),
                tombstone,
                "Received a tombstone record to delete member m but member exists in \
                     target assignment.",
            ),
            (
                "a group tombstone while a target is left",
                vec![Record::Target("m"), tombstone("group", "")],
                Err(
                    "Received a tombstone record to delete group g but the target assignment \
                     still has 1 members."
                        .into(),
                ),
            ),
            (
                "a group tombstone before the target metadata tombstone",
                vec![Record::Group(2), tombstone("group", "")],
                Err(
                    "Received a tombstone record to delete group g but target assignment \
                     epoch in invalid."
                        .into(),
                ),
            ),
            (
                "a share record for a consumer group",
                vec![Record::ConsumerGroup, Record::Group(2)],
                Err("Group g is not a share group.".into()),
            ),
        ];
        crate::coordinator::unified::test_support::check_replay_cases(
            &rows,
            replay,
            |coord| coord.share_seeds.get("g").map(|seed| seed.value().clone()),
            |coord| coord.cached_share_seed("g"),
        );
    }
}
