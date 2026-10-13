//! Bootstrap replay of the KIP-1071 streams group records, as Kafka 4.3.1's
//! `GroupMetadataManager.replay` applies them.
//!
//! Each method applies one decoded record to the bootstrap seed and to the
//! last-known-good cache. The rules for creating a group, ignoring a
//! tombstone, and failing the load are
//! [`replay_policy`](super::replay_policy)'s.

use {
    super::{
        group_coordinator::GroupCoordinator,
        replay_policy::{
            GroupLookup, LEAVE_GROUP_MEMBER_EPOCH, ModernGroupType, group_tombstone,
            persisted_group, target_metadata, target_metadata_tombstone,
        },
        seeds::StreamsGroupSeed,
        streams,
    },
    crate::error::BrokerError,
};

/// The fields of a member that Kafka's replay creates from a current
/// assignment record alone. Kafka's `getOrCreateUninitializedMember` leaves
/// them null; these are the values the broker gives a member before its
/// first heartbeat.
fn uninitialized_member() -> streams::persistence::StreamsGroupMemberMetadataValue {
    streams::persistence::StreamsGroupMemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: String::new(),
        client_host: String::new(),
        process_id: String::new(),
        user_endpoint: None,
        client_tags: Vec::new(),
        rebalance_timeout_ms: -1,
        topology_epoch: -1,
    }
}

impl GroupCoordinator {
    /// Kafka's `getOrMaybeCreatePersistedStreamsGroup` among the modern
    /// groups: whether the group exists once the lookup is done. The
    /// bootstrap replay settles a classic group under the id before it calls
    /// this.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a consumer or share group.
    pub(crate) fn persisted_streams_group(
        &self,
        group_id: &str,
        create_if_not_exists: bool,
    ) -> Result<bool, BrokerError> {
        match persisted_group(
            group_id,
            ModernGroupType::Streams,
            self.replayed_modern_group(group_id),
            create_if_not_exists,
        )? {
            GroupLookup::Absent => Ok(false),
            GroupLookup::Found => Ok(true),
            GroupLookup::Create => {
                self.streams_seeds
                    .insert(group_id.into(), StreamsGroupSeed::new_group());
                self.streams_seeds_cache
                    .insert(group_id.into(), StreamsGroupSeed::new_group());
                Ok(true)
            }
        }
    }

    /// Applies `update` to the replayed group and to its cached copy.
    fn update_streams_seed(&self, group_id: &str, update: impl Fn(&mut StreamsGroupSeed)) {
        if let Some(mut seed) = self.streams_seeds.get_mut(group_id) {
            update(seed.value_mut());
        }
        update(
            self.streams_seeds_cache
                .entry(group_id.into())
                .or_insert_with(StreamsGroupSeed::new_group)
                .value_mut(),
        );
    }

    /// Applies `update` with `value` to the replayed group, and with a copy
    /// of it to the cached copy.
    fn update_streams_seed_with<T: Clone>(
        &self,
        group_id: &str,
        value: T,
        update: impl Fn(&mut StreamsGroupSeed, T),
    ) {
        let cached = value.clone();
        if let Some(mut seed) = self.streams_seeds.get_mut(group_id) {
            update(seed.value_mut(), value);
        }
        update(
            self.streams_seeds_cache
                .entry(group_id.into())
                .or_insert_with(StreamsGroupSeed::new_group)
                .value_mut(),
            cached,
        );
    }

    /// Reads the replayed group, which must exist.
    fn streams_seed<T>(&self, group_id: &str, read: impl FnOnce(&StreamsGroupSeed) -> T) -> T {
        read(
            self.streams_seeds
                .get(group_id)
                .expect("the caller looked the group up")
                .value(),
        )
    }

    /// Applies a `StreamsGroupMetadataValue`, as Kafka's
    /// `GroupMetadataManager.replay` of the record does: the group epoch, the
    /// metadata hash, the validated topology epoch, and the last assignment
    /// configuration, empty for a null list. It also keeps Kafka trunk's
    /// KIP-1331 description epochs, which 4.3.1 keeps as unknown tags, in
    /// either write mode.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`, or
    /// [`BrokerError::Startup`] when the configuration list names a key twice:
    /// Kafka's `Collectors.toMap` throws `IllegalStateException` for it, which
    /// fails the load.
    pub fn replay_streams_group_metadata(
        &self,
        group_id: &str,
        value: streams::persistence::StreamsGroupMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        let mut last_assignment_configs = std::collections::BTreeMap::new();
        for config in value.last_assignment_configs.into_iter().flatten() {
            if let Some(first) = last_assignment_configs.get(&config.key) {
                return Err(BrokerError::Startup(format!(
                    "Duplicate key {} (attempted merging values {first} and {})",
                    config.key, config.value
                )));
            }
            last_assignment_configs.insert(config.key, config.value);
        }
        self.update_streams_seed(group_id, |seed| {
            seed.group_epoch = value.epoch;
            seed.metadata_hash = value.metadata_hash;
            seed.validated_topology_epoch = value.validated_topology_epoch;
            seed.last_assignment_configs
                .clone_from(&last_assignment_configs);
            seed.description_epochs = value.description;
        });
        Ok(())
    }

    /// `StreamsGroupMemberMetadataValue`: the member's metadata, which
    /// creates the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_member_metadata(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupMemberMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.members.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `StreamsGroupTopologyValue`: the group's topology.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_topology(
        &self,
        group_id: &str,
        v: streams::persistence::StreamsGroupTopologyValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.topology = Some(v);
        });
        Ok(())
    }

    /// `StreamsGroupTargetAssignmentMetadataValue`: the assignment epoch and
    /// time.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`, or Kafka's
    /// `IllegalArgumentException` for an epoch below -1 or a negative time.
    pub fn replay_streams_target_assignment_metadata(
        &self,
        group_id: &str,
        value: streams::persistence::StreamsGroupTargetAssignmentMetadataValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        target_metadata(value.assignment_epoch, value.assignment_timestamp_ms)?;
        self.update_streams_seed(group_id, |seed| {
            seed.assignment_epoch = value.assignment_epoch;
            seed.assignment_timestamp_ms = value.assignment_timestamp_ms;
        });
        Ok(())
    }

    /// `StreamsGroupTargetAssignmentMemberValue`: one member's target, which
    /// Kafka keeps whether or not the group holds the member.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_target_assignment_member(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupTargetAssignmentMemberValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.target_per_member.insert(member_id.into(), v);
        });
        Ok(())
    }

    /// `StreamsGroupCurrentMemberAssignmentValue`: one member's epochs and
    /// tasks. It creates a member that the group does not hold yet, as
    /// Kafka's `getOrCreateUninitializedMember` does.
    ///
    /// # Errors
    ///
    /// Returns the error of `Self::persisted_streams_group`.
    pub fn replay_streams_current_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        v: streams::persistence::StreamsGroupCurrentMemberAssignmentValue,
    ) -> Result<(), BrokerError> {
        self.persisted_streams_group(group_id, true)?;
        self.update_streams_seed_with(group_id, v, |seed, v| {
            seed.install_current_member(member_id, v, uninitialized_member);
        });
        Ok(())
    }

    /// Applies the tombstone of a streams group record, as Kafka's replay of
    /// a null value does. A tombstone of a group that the replay does not
    /// hold, or of a member that the group does not hold, changes nothing.
    ///
    /// - Group metadata (k17): removes the group, once its members are gone
    ///   and its target assignment metadata was tombstoned. Kafka's streams
    ///   check does not look at the members' targets.
    /// - Member metadata (k18): removes the member, once its current
    ///   assignment was tombstoned and its target assignment is gone.
    /// - Topology (k19): the group has no topology.
    /// - Target assignment metadata (k20): the assignment epoch becomes -1
    ///   and the time 0, once every member's target assignment is gone.
    /// - Target assignment (k21): removes the member's target.
    /// - Current assignment (k22): the member keeps its metadata and state at
    ///   epoch -1 with no tasks.
    ///
    /// # Errors
    ///
    /// Returns Kafka's `IllegalStateException` for a group of another type
    /// and for a tombstone whose preconditions do not hold.
    pub fn replay_streams_tombstone(
        &self,
        key: &streams::persistence::StreamsGroupKey,
    ) -> Result<(), BrokerError> {
        use streams::persistence::StreamsGroupKey as K;
        let group_id = key.group_id();
        if !self.persisted_streams_group(group_id, false)? {
            return Ok(());
        }
        match key {
            K::GroupMetadata { .. } => {
                self.streams_seed(group_id, |seed| {
                    group_tombstone(
                        ModernGroupType::Streams,
                        group_id,
                        seed.members.len(),
                        seed.target_per_member.len(),
                        seed.assignment_epoch,
                    )
                })?;
                self.streams_seeds.remove(group_id);
                self.streams_seeds_cache.remove(group_id);
                self.group_types.remove(group_id);
            }
            K::MemberMetadata { member_id, .. } => {
                let remove =
                    self.streams_seed(group_id, |seed| seed.member_tombstone(member_id))?;
                if remove {
                    self.update_streams_seed(group_id, |seed| {
                        seed.remove_replayed_member(member_id);
                    });
                }
            }
            K::Topology { .. } => self.update_streams_seed(group_id, |seed| seed.topology = None),
            K::TargetAssignmentMetadata { .. } => {
                self.streams_seed(group_id, |seed| {
                    target_metadata_tombstone(group_id, seed.target_per_member.len())
                })?;
                self.update_streams_seed(group_id, |seed| {
                    seed.assignment_epoch = -1;
                    seed.assignment_timestamp_ms = 0;
                });
            }
            K::TargetAssignmentMember { member_id, .. } => {
                self.update_streams_seed(group_id, |seed| {
                    seed.target_per_member.remove(member_id);
                });
            }
            K::CurrentMemberAssignment { member_id, .. } => {
                if self.streams_seed(group_id, |seed| seed.members.contains_key(member_id)) {
                    self.update_streams_seed(group_id, |seed| {
                        let state = seed.current_per_member.get(member_id).map_or(
                            streams::persistence::StreamsMemberWireState::Stable,
                            |current| current.state,
                        );
                        seed.current_per_member.insert(
                            member_id.clone(),
                            streams::persistence::StreamsGroupCurrentMemberAssignmentValue {
                                member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                                state,
                                ..Default::default()
                            },
                        );
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            persistence_next_gen::GroupMetadataValue,
            streams::persistence::{
                LastAssignmentConfig, StreamsGroupCurrentMemberAssignmentValue, StreamsGroupKey,
                StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMemberValue,
                StreamsGroupTargetAssignmentMetadataValue, StreamsGroupTopologyValue,
                StreamsMemberWireState,
            },
            test_support::{
                AssignmentDeletionProtocol, check_replay_cases, deletion_replay_cases,
                replay_fixtures, replay_tombstone_constructor, streams_member,
            },
        },
        std::collections::BTreeMap,
    };

    /// What a log leaves: the group that replay holds, or the message of
    /// the exception that fails the load.
    type Outcome = Result<Option<StreamsGroupSeed>, String>;

    /// One record of the replayed log.
    #[derive(Clone)]
    enum Record {
        Group(i32, Option<Vec<(&'static str, &'static str)>>),
        Member(&'static str),
        Topology,
        TargetEpoch(i32),
        Target(&'static str),
        Current(&'static str, i32),
        ConsumerGroup,
        Tombstone(StreamsGroupKey),
    }

    fn key(kind: &str, id: &str) -> StreamsGroupKey {
        let group_id = "g".to_owned();
        let member_id = id.to_owned();
        match kind {
            "group" => StreamsGroupKey::GroupMetadata { group_id },
            "member" => StreamsGroupKey::MemberMetadata {
                group_id,
                member_id,
            },
            "topology" => StreamsGroupKey::Topology { group_id },
            "target-epoch" => StreamsGroupKey::TargetAssignmentMetadata { group_id },
            "target" => StreamsGroupKey::TargetAssignmentMember {
                group_id,
                member_id,
            },
            _ => StreamsGroupKey::CurrentMemberAssignment {
                group_id,
                member_id,
            },
        }
    }

    fn topology() -> StreamsGroupTopologyValue {
        StreamsGroupTopologyValue {
            epoch: 31,
            subtopologies: vec![],
        }
    }

    fn target() -> StreamsGroupTargetAssignmentMemberValue {
        StreamsGroupTargetAssignmentMemberValue {
            active: maplit::btreemap! {"0".to_string() => vec![0, 1]},
            ..Default::default()
        }
    }

    fn current(epoch: i32) -> StreamsGroupCurrentMemberAssignmentValue {
        StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: epoch,
            previous_member_epoch: epoch - 1,
            state: StreamsMemberWireState::UnrevokedTasks,
            active: maplit::btreemap! {"0".to_string() => vec![0, 1]},
            active_epochs: maplit::btreemap! {"0".to_string() => vec![epoch, epoch]},
            ..Default::default()
        }
    }

    fn replay(coord: &GroupCoordinator, record: &Record) -> Result<(), BrokerError> {
        match record.clone() {
            Record::Group(epoch, configs) => {
                coord.replay_streams_group_metadata("g", StreamsGroupMetadataValue {
                    epoch,
                    metadata_hash: 44,
                    validated_topology_epoch: 31,
                    last_assignment_configs: configs.map(|configs| {
                        configs
                            .into_iter()
                            .map(|(key, value)| LastAssignmentConfig {
                                key: key.into(),
                                value: value.into(),
                            })
                            .collect()
                    }),
                    description: streams::persistence::DescriptionEpochs::default(),
                })
            }
            Record::Member(member) => {
                coord.replay_streams_member_metadata("g", member, streams_member(member))
            }
            Record::Topology => coord.replay_streams_topology("g", topology()),
            Record::TargetEpoch(epoch) => coord.replay_streams_target_assignment_metadata(
                "g",
                StreamsGroupTargetAssignmentMetadataValue {
                    assignment_epoch: epoch,
                    assignment_timestamp_ms: 4,
                },
            ),
            Record::Target(member) => {
                coord.replay_streams_target_assignment_member("g", member, target())
            }
            Record::Current(member, epoch) => {
                coord.replay_streams_current_member_assignment("g", member, current(epoch))
            }
            Record::ConsumerGroup => coord.replay_group_metadata("g", GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            Record::Tombstone(key) => coord.replay_streams_tombstone(&key),
        }
    }

    /// Kafka's `GroupMetadataManager.replay` of the streams group records,
    /// one log per row: the whole group that replay holds afterwards, or the
    /// exception that fails the load.
    #[test]
    fn streams_records_replay_as_kafka() {
        let tombstone = replay_tombstone_constructor(key, Record::Tombstone);
        // Kafka's `StreamsGroup.createGroupTombstoneRecords` for member `m`:
        // the topology tombstone comes after the group's, and is ignored.
        let (group, kafka_deletion) = replay_fixtures()
            .seed(StreamsGroupSeed::new_group)
            .tombstone(tombstone)
            .protocol(AssignmentDeletionProtocol::Streams)
            .call();
        let every_record = || {
            vec![
                Record::Group(30, Some(vec![("num.standby.replicas", "1")])),
                Record::Member("m"),
                Record::Topology,
                Record::TargetEpoch(32),
                Record::Target("m"),
                Record::Current("m", 7),
            ]
        };
        let mut rows: Vec<(&str, Vec<Record>, Outcome)> = vec![
            (
                "every record of a group",
                every_record(),
                group(&|seed| {
                    seed.group_epoch = 30;
                    seed.metadata_hash = 44;
                    seed.validated_topology_epoch = 31;
                    seed.last_assignment_configs =
                        BTreeMap::from([("num.standby.replicas".into(), "1".into())]);
                    seed.assignment_epoch = 32;
                    seed.assignment_timestamp_ms = 4;
                    seed.topology = Some(topology());
                    seed.members.insert("m".into(), streams_member("m"));
                    seed.target_per_member.insert("m".into(), target());
                    seed.current_per_member.insert("m".into(), current(7));
                }),
            ),
            (
                "a current assignment creates the group and an uninitialized member",
                vec![Record::Current("m", 7)],
                group(&|seed| {
                    seed.members.insert("m".into(), uninitialized_member());
                    seed.current_per_member.insert("m".into(), current(7));
                }),
            ),
            (
                "a topology creates the group, and its tombstone removes the topology",
                vec![Record::Topology, tombstone("topology", "")],
                group(&|_| {}),
            ),
            (
                "the tombstone of a current assignment leaves the member at epoch -1",
                vec![
                    Record::Member("m"),
                    Record::Current("m", 7),
                    tombstone("current", "m"),
                ],
                group(&|seed| {
                    seed.members.insert("m".into(), streams_member("m"));
                    seed.current_per_member.insert(
                        "m".into(),
                        StreamsGroupCurrentMemberAssignmentValue {
                            member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                            previous_member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
                            state: StreamsMemberWireState::UnrevokedTasks,
                            ..Default::default()
                        },
                    );
                }),
            ),
            (
                "a member tombstone before its current assignment tombstone",
                vec![Record::Member("m"), tombstone("member", "m")],
                Err(
                    "Received a tombstone record to delete member m but did not receive \
                     StreamsGroupCurrentMemberAssignmentValue tombstone."
                        .into(),
                ),
            ),
            crate::coordinator::unified::test_support::group_deletion_with_member_case(
                Record::Member("m"),
                tombstone,
            ),
            (
                "a group tombstone with a target left but no member: the streams check \
                 does not look at targets",
                vec![
                    Record::Target("m"),
                    tombstone("target", "m"),
                    tombstone("target-epoch", ""),
                    Record::Target("m"),
                    tombstone("group", ""),
                ],
                Ok(None),
            ),
            (
                "a group tombstone before the target metadata tombstone",
                vec![Record::Group(2, None), tombstone("group", "")],
                Err(
                    "Received a tombstone record to delete group g but did not receive \
                     StreamsGroupTargetAssignmentMetadataValue tombstone."
                        .into(),
                ),
            ),
            (
                "a configuration list that names a key twice",
                vec![Record::Group(2, Some(vec![("a", "1"), ("a", "2")]))],
                Err("Duplicate key a (attempted merging values 1 and 2)".into()),
            ),
            (
                "a streams record for a consumer group",
                vec![Record::ConsumerGroup, Record::Topology],
                Err("Group g is not a streams group.".into()),
            ),
        ];
        rows.splice(
            3..3,
            deletion_replay_cases(every_record(), kafka_deletion()),
        );
        check_replay_cases(
            &rows,
            replay,
            |coord| {
                coord
                    .streams_seeds
                    .get("g")
                    .map(|seed| seed.value().clone())
            },
            |coord| coord.cached_streams_seed("g"),
        );
    }
}
