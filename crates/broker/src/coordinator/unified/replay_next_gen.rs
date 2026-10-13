//! Bootstrap replay of the KIP-848 consumer group records, as Kafka 4.3.1's
//! `GroupMetadataManager.replay` applies them.
//!
//! Each method applies one decoded record to the bootstrap seed and to the
//! last-known-good cache, so a group that finishes replay is ready to hydrate
//! an actor and to survive a later actor respawn. A record with a value
//! creates the group that it names, and a member record or a current
//! assignment record creates the member. A tombstone of a group or a member
//! that the replay does not hold is ignored, and a tombstone whose
//! preconditions the records before it did not establish fails the load, as
//! the `IllegalStateException` that Kafka throws does. See
//! [`replay_policy`](super::replay_policy).

use {
    super::{
        group_coordinator::GroupCoordinator,
        persistence_next_gen::{self, CurrentMemberAssignmentValue, MemberAssignmentState},
        replay_policy::{
            ExistingGroup, GroupLookup, LEAVE_GROUP_MEMBER_EPOCH, ModernGroupType, group_tombstone,
            persisted_group, target_metadata, target_metadata_tombstone,
        },
        seeds::GroupSeed,
    },
    crate::error::BrokerError,
};

/// The fields of a member that Kafka's replay creates from a current
/// assignment record alone: `ConsumerGroupMember.Builder`'s defaults.
fn uninitialized_member() -> persistence_next_gen::MemberMetadataValue {
    persistence_next_gen::MemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        subscribed_topic_names: Vec::new(),
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout_ms: -1,
        classic: None,
    }
}

impl GroupCoordinator {
    /// The modern group that the replay holds under `group_id`, if any.
    pub(crate) fn replayed_modern_group(&self, group_id: &str) -> Option<ExistingGroup> {
        if self.seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Consumer))
        } else if self.share_seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Share))
        } else if self.streams_seeds.contains_key(group_id) {
            Some(ExistingGroup::Modern(ModernGroupType::Streams))
        } else {
            None
        }
    }

    /// Drops the consumer, share or streams group that the replay holds under
    /// `group_id`, as Kafka does when a classic `GroupMetadata` value puts a
    /// classic group in its place or a classic tombstone removes the group.
    pub(crate) fn forget_replayed_group(&self, group_id: &str) {
        if self.replayed_modern_group(group_id).is_none() {
            return;
        }
        self.seeds.remove(group_id);
        self.seeds_cache.remove(group_id);
        self.share_seeds.remove(group_id);
        self.share_seeds_cache.remove(group_id);
        self.streams_seeds.remove(group_id);
        self.streams_seeds_cache.remove(group_id);
        self.group_types.remove(group_id);
    }

    /// Kafka's `getOrMaybeCreatePersistedConsumerGroup` among the modern
    /// groups: whether the group exists once the lookup is done. The
    /// bootstrap replay settles a classic group under the id before it calls
    /// this.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a share or streams group.
    pub(crate) fn persisted_consumer_group(
        &self,
        group_id: &str,
        create_if_not_exists: bool,
    ) -> Result<bool, BrokerError> {
        match persisted_group(
            group_id,
            ModernGroupType::Consumer,
            self.replayed_modern_group(group_id),
            create_if_not_exists,
        )? {
            GroupLookup::Absent => Ok(false),
            GroupLookup::Found => Ok(true),
            GroupLookup::Create => {
                self.seeds.insert(group_id.into(), GroupSeed::new_group());
                self.seeds_cache
                    .insert(group_id.into(), GroupSeed::new_group());
                Ok(true)
            }
        }
    }

    /// Applies `update` to the replayed group and to its cached copy.
    fn update_consumer_seed(&self, group_id: &str, update: impl Fn(&mut GroupSeed)) {
        if let Some(mut seed) = self.seeds.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.seeds_cache
                .entry(group_id.into())
                .or_insert_with(GroupSeed::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    fn update_consumer_seed_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut GroupSeed, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.seeds.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.seeds_cache
                .entry(group_id.into())
                .or_insert_with(GroupSeed::new_group)
                .value_mut(),
            cached,
        );
    }

    /// Reads the replayed group, which must exist.
    fn consumer_seed<T>(&self, group_id: &str, read: impl FnOnce(&GroupSeed) -> T) -> T {
        read(
            self.seeds
                .get(group_id)
                .expect("the caller looked the group up")
                .value(),
        )
    }

    /// `ConsumerGroupMetadataValue`: the group epoch and the metadata hash.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_group_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::GroupMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.group_epoch = v.epoch;
            seed.metadata_hash = v.metadata_hash;
        });
        Ok(())
    }

    /// A deprecated `ConsumerGroupPartitionMetadata` value (key v4): the group
    /// holds the record, so that its next metadata update tombstones it. The
    /// topics in the value are not kept: the metadata hash replaced them.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_partition_metadata(&self, group_id: &str) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.has_subscription_metadata_record = true;
        });
        Ok(())
    }

    /// `ConsumerGroupMemberMetadataValue`: the member's subscription, which
    /// creates the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::MemberMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.members.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ConsumerGroupTargetAssignmentMetadataValue`: the assignment epoch and
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`, or Kafka's
    /// `IllegalArgumentException` for an epoch below -1 or a negative time.
    pub fn replay_target_assignment_metadata(
        &self,
        group_id: &str,
        v: persistence_next_gen::TargetAssignmentMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        target_metadata(v.assignment_epoch, v.assignment_timestamp_ms)?;
        self.update_consumer_seed(group_id, |seed| {
            seed.target_epoch = v.assignment_epoch;
            seed.assignment_timestamp_ms = v.assignment_timestamp_ms;
        });
        Ok(())
    }

    /// `ConsumerGroupTargetAssignmentMemberValue`: one member's target, which
    /// Kafka keeps whether or not the group holds the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: persistence_next_gen::TargetAssignmentMemberValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.target_per_member.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `ConsumerGroupCurrentMemberAssignmentValue`: one member's epochs and
    /// partitions. It creates a member that the group does not hold yet,
    /// with the defaults of Kafka's `ConsumerGroupMember.Builder`.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: CurrentMemberAssignmentValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.install_current_member(member_id, v, uninitialized_member);
        });
        Ok(())
    }

    /// `ConsumerGroupRegularExpressionValue`: what `regex` resolved to in the
    /// group, which is how a coordinator failover restores the topics of
    /// every regex subscription.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_consumer_group`.
    pub fn replay_regular_expression(
        &self,
        group_id: &str,
        regex: &str,
        v: persistence_next_gen::RegularExpressionValue,
    ) -> Result<(), BrokerError> {
        self.persisted_consumer_group(group_id, true)?;
        self.update_consumer_seed_with(group_id, v, |seed, v| {
            seed.resolved_regexes.insert(regex.into(), v);
        });
        Ok(())
    }

    /// Applies the tombstone of a consumer group record, as Kafka's replay of
    /// a null value does. A tombstone of a group that the replay does not
    /// hold, or of a member that the group does not hold, changes nothing.
    ///
    /// - Group metadata (k3): removes the group, once its members, its
    ///   target assignment and its target assignment metadata are gone.
    /// - Deprecated subscription metadata (k4): the group no longer holds it.
    /// - Member subscription (k5): removes the member, once its current
    ///   assignment was tombstoned and its target assignment is gone.
    /// - Target assignment metadata (k6): the assignment epoch becomes -1 and
    ///   the time 0, once every member's target assignment is gone.
    /// - Target assignment (k7): removes the member's target.
    /// - Current assignment (k8): the member keeps its subscription and state
    ///   at epoch -1 with no partitions.
    /// - Regular expression (k16): removes the resolution.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group of another type
    /// and for a tombstone whose preconditions do not hold.
    pub fn replay_next_gen_tombstone(
        &self,
        key: &persistence_next_gen::NextGenKey,
    ) -> Result<(), BrokerError> {
        use persistence_next_gen::NextGenKey as K;
        let group_id = key.group_id();
        if !self.persisted_consumer_group(group_id, false)? {
            return Ok(());
        }
        match key {
            K::GroupMetadata { .. } => {
                self.consumer_seed(group_id, |seed| {
                    group_tombstone(
                        ModernGroupType::Consumer,
                        group_id,
                        seed.members.len(),
                        seed.target_per_member.len(),
                        seed.target_epoch,
                    )
                })?;
                self.seeds.remove(group_id);
                self.seeds_cache.remove(group_id);
                self.group_types.remove(group_id);
            }
            K::PartitionMetadata { .. } => self.update_consumer_seed(group_id, |seed| {
                seed.has_subscription_metadata_record = false;
            }),
            K::MemberMetadata { member_id, .. } => {
                let remove =
                    self.consumer_seed(group_id, |seed| seed.member_tombstone(member_id))?;
                if remove {
                    self.update_consumer_seed(group_id, |seed| {
                        seed.remove_replayed_member(member_id);
                    });
                }
            }
            K::TargetAssignmentMetadata { .. } => {
                self.consumer_seed(group_id, |seed| {
                    target_metadata_tombstone(group_id, seed.target_per_member.len())
                })?;
                self.update_consumer_seed(group_id, |seed| {
                    seed.target_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                });
            }
            K::TargetAssignmentMember { member_id, .. } => {
                self.update_consumer_seed(group_id, |seed| {
                    seed.target_per_member.remove(member_id);
                });
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                if self.consumer_seed(group_id, |seed| seed.members.contains_key(member_id)) {
                    self.update_consumer_seed(group_id, |seed| {
                        let state = seed
                            .current_per_member
                            .get(member_id)
                            .map_or(MemberAssignmentState::Stable, |current| current.state);
                        seed.current_per_member.insert(
                            member_id.clone(),
                            CurrentMemberAssignmentValue {
                                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                state,
                                assigned_partitions: Vec::new(),
                                partitions_pending_revocation: Vec::new(),
                            },
                        );
                    });
                }
            }
            K::RegularExpression { regex, .. } => self.update_consumer_seed(group_id, |seed| {
                seed.resolved_regexes.remove(regex);
            }),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::coordinator::unified::{
            persistence_next_gen::{
                AssignedTopicPartitions, GroupMetadataValue, NextGenKey, RegularExpressionValue,
                TargetAssignmentMemberValue, TargetAssignmentMetadataValue,
            },
            share::persistence::ShareGroupMetadataValue,
            test_support::{
                ReplayCase, check_replay_cases, deletion_replay_cases,
                group_deletion_with_member_case, member_deletion_with_target_case, next_current,
                next_member, proto_uuid, replay_tombstone_constructor,
            },
        },
    };

    /// One record of the replayed log.
    #[derive(Clone)]
    enum Record {
        Group(i32),
        Member(&'static str),
        TargetEpoch(i32),
        Target(&'static str),
        Current(&'static str, i32),
        Regex(&'static str),
        ShareGroup,
        Tombstone(NextGenKey),
    }

    fn key(kind: &str, id: &str) -> NextGenKey {
        let group_id = "g".to_owned();
        let member_id = id.to_owned();
        match kind {
            "group" => NextGenKey::GroupMetadata { group_id },
            "partition" => NextGenKey::PartitionMetadata { group_id },
            "member" => NextGenKey::MemberMetadata {
                group_id,
                member_id,
            },
            "target-epoch" => NextGenKey::TargetAssignmentMetadata { group_id },
            "target" => NextGenKey::TargetAssignmentMember {
                group_id,
                member_id,
            },
            "current" => NextGenKey::CurrentMemberAssignment {
                group_id,
                member_id,
            },
            _ => NextGenKey::RegularExpression {
                group_id,
                regex: member_id,
            },
        }
    }

    fn target() -> TargetAssignmentMemberValue {
        TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: proto_uuid(3),
                partitions: vec![0],
            }],
        }
    }

    fn resolved() -> RegularExpressionValue {
        RegularExpressionValue {
            topics: vec!["t".into()],
            version: 1,
            timestamp_ms: 2,
        }
    }

    fn replay(coord: &GroupCoordinator, record: &Record) -> Result<(), BrokerError> {
        match record.clone() {
            Record::Group(epoch) => coord.replay_group_metadata("g", GroupMetadataValue {
                epoch,
                metadata_hash: 9,
            }),
            Record::Member(member) => {
                coord.replay_member_metadata("g", member, next_member(member))
            }
            Record::TargetEpoch(epoch) => {
                coord.replay_target_assignment_metadata("g", TargetAssignmentMetadataValue {
                    assignment_epoch: epoch,
                    assignment_timestamp_ms: 4,
                })
            }
            Record::Target(member) => coord.replay_target_assignment_member("g", member, target()),
            Record::Current(member, epoch) => {
                coord.replay_current_member_assignment("g", member, next_current(epoch))
            }
            Record::Regex(regex) => coord.replay_regular_expression("g", regex, resolved()),
            Record::ShareGroup => coord.replay_share_group_metadata("g", ShareGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            Record::Tombstone(key) => coord.replay_next_gen_tombstone(&key),
        }
    }

    /// The deletion of a group with member `m` and regex `r`, in the order of
    /// Kafka's `ConsumerGroup.createGroupTombstoneRecords`.
    fn kafka_deletion() -> Vec<Record> {
        [
            ("current", "m"),
            ("target", "m"),
            ("target-epoch", ""),
            ("member", "m"),
            ("regex", "r"),
            ("partition", ""),
            ("group", ""),
        ]
        .into_iter()
        .map(|(kind, id)| Record::Tombstone(key(kind, id)))
        .collect()
    }

    /// Kafka's `GroupMetadataManager.replay` of the consumer group records,
    /// one log per row: the whole group that replay holds afterwards, or the
    /// `IllegalStateException` or `IllegalArgumentException` that fails the
    /// load.
    #[test]
    fn consumer_records_replay_as_kafka() {
        let group = |update: &dyn Fn(&mut GroupSeed)| {
            let mut seed = GroupSeed::new_group();
            update(&mut seed);
            Ok(Some(seed))
        };
        let full_group = || {
            vec![
                Record::Group(5),
                Record::Member("m"),
                Record::TargetEpoch(5),
                Record::Target("m"),
                Record::Current("m", 5),
                Record::Regex("r"),
            ]
        };
        let tombstone = replay_tombstone_constructor(key, Record::Tombstone);
        let mut rows: Vec<ReplayCase<'_, Record, GroupSeed>> = vec![
            ReplayCase {
                case: "a member record creates the group and the member",
                records: vec![Record::Member("m")],
                expected: group(&|seed| {
                    seed.members.insert("m".into(), next_member("m"));
                }),
            },
            ReplayCase {
                case: "a current assignment creates an uninitialized member",
                records: vec![Record::Current("m", 3)],
                expected: group(&|seed| {
                    seed.members.insert("m".into(), uninitialized_member());
                    seed.current_per_member.insert("m".into(), next_current(3));
                }),
            },
            ReplayCase {
                case: "a target assignment creates the group but no member",
                records: vec![Record::Target("m")],
                expected: group(&|seed| {
                    seed.target_per_member.insert("m".into(), target());
                }),
            },
            ReplayCase {
                case: "a regular expression creates the group",
                records: vec![Record::Regex("r")],
                expected: group(&|seed| {
                    seed.resolved_regexes.insert("r".into(), resolved());
                }),
            },
            ReplayCase {
                case: "an epoch replaces the last one, lower or not",
                records: vec![Record::Group(5), Record::Group(2), Record::TargetEpoch(0)],
                expected: group(&|seed| {
                    seed.group_epoch = 2;
                    seed.metadata_hash = 9;
                    seed.target_epoch = 0;
                    seed.assignment_timestamp_ms = 4;
                }),
            },
            ReplayCase {
                case: "the tombstone of a current assignment leaves the member at epoch -1",
                records: vec![
                    Record::Member("m"),
                    Record::Current("m", 3),
                    tombstone("current", "m"),
                    tombstone("target", "other"),
                    tombstone("member", "other"),
                ],
                expected: group(&|seed| {
                    seed.members.insert("m".into(), next_member("m"));
                    seed.current_per_member
                        .insert("m".into(), next_current(LEAVE_GROUP_MEMBER_EPOCH));
                    let current = seed.current_per_member.get_mut("m").unwrap();
                    current.previous_member_epoch = LEAVE_GROUP_MEMBER_EPOCH;
                    current.assigned_partitions.clear();
                    current.partitions_pending_revocation.clear();
                }),
            },
            ReplayCase {
                case: "a member tombstone before its current assignment tombstone",
                records: vec![
                    Record::Member("m"),
                    Record::Current("m", 3),
                    tombstone("member", "m"),
                ],
                expected: Err(
                    "Received a tombstone record to delete member m but did not receive \
                     ConsumerGroupCurrentMemberAssignmentValue tombstone."
                        .into(),
                ),
            },
            ReplayCase {
                case: "a member tombstone of a member that never had a current assignment",
                records: vec![Record::Member("m"), tombstone("member", "m")],
                expected: Err(
                    "Received a tombstone record to delete member m but did not receive \
                     ConsumerGroupCurrentMemberAssignmentValue tombstone."
                        .into(),
                ),
            },
            member_deletion_with_target_case(
                Record::Member("m"),
                Record::Target("m"),
                tombstone,
                "Received a tombstone record to delete member m but did not receive \
                     ConsumerGroupTargetAssignmentMetadataValue tombstone.",
            ),
            ReplayCase {
                case: "a target metadata tombstone while a member has a target",
                records: vec![Record::Target("m"), tombstone("target-epoch", "")],
                expected: Err(
                    "Received a tombstone record to delete target assignment of g but the \
                     assignment still has 1 members."
                        .into(),
                ),
            },
            group_deletion_with_member_case(Record::Member("m"), tombstone),
            ReplayCase {
                case: "a group tombstone before the target metadata tombstone",
                records: vec![Record::Group(3), tombstone("group", "")],
                expected: Err(
                    "Received a tombstone record to delete group g but did not receive \
                     ConsumerGroupTargetAssignmentMetadataValue tombstone."
                        .into(),
                ),
            },
            ReplayCase {
                case: "an assignment epoch below -1",
                records: vec![Record::TargetEpoch(-2)],
                expected: Err("The assignment epoch must be non-negative or -1.".into()),
            },
            ReplayCase {
                case: "a consumer record for a share group",
                records: vec![Record::ShareGroup, Record::Member("m")],
                expected: Err("Group g is not a consumer group".into()),
            },
            ReplayCase {
                case: "a consumer tombstone for a share group",
                records: vec![Record::ShareGroup, tombstone("member", "m")],
                expected: Err("Group g is not a consumer group".into()),
            },
        ];
        rows.splice(5..5, deletion_replay_cases(full_group(), kafka_deletion()));
        check_replay_cases(
            &rows,
            replay,
            |coord| coord.seeds.get("g").map(|seed| seed.value().clone()),
            |coord| coord.cached_seed("g"),
        );
    }
}
