//! The durable delta for one group-state transition.
//!
//! [`PendingRecords`] collects the mutations a transition makes, encodes them
//! as the single `RecordBatch` that `OffsetsLog::append` takes, and applies the
//! same delta to the coordinator's respawn cache once the append succeeds.

use krabka_protocol::records::RecordBatch;

use crate::{
    coordinator::unified::{
        GroupCoordinator, OffsetRecordBatchBuilder,
        member_records::{MemberRecordFamilies, MemberRecordLists},
        persistence_next_gen::{
            CurrentMemberAssignmentValue, GroupMetadataValue, MemberMetadataValue, NextGenKey,
            RegularExpressionValue, TargetAssignmentMemberValue, TargetAssignmentMetadataValue,
            encode_key,
        },
    },
    error::BrokerError,
};

/// The key of the deprecated k4 `ConsumerGroupPartitionMetadata` record.
fn partition_metadata_key(group_id: &str) -> Result<bytes::Bytes, BrokerError> {
    encode_key(&NextGenKey::PartitionMetadata {
        group_id: group_id.into(),
    })
}

/// What a transition writes for the deprecated k4
/// `ConsumerGroupPartitionMetadata` record.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PartitionMetadataWrite {
    /// Nothing.
    #[default]
    Keep,
    /// Its tombstone, right after the k3 epoch record.
    Tombstone,
}

/// What Kafka's `ConsumerGroup.createGroupTombstoneRecords` tombstones: the
/// members, each with its current assignment, target assignment and
/// subscription, and the resolved regular expressions.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct GroupTombstone {
    pub members: Vec<String>,
    pub regexes: Vec<String>,
}

/// The keys of Kafka's `ConsumerGroup.createGroupTombstoneRecords`, in its
/// order: every member's current assignment (k8), every member's target
/// assignment (k7), the target assignment metadata (k6), every member's
/// subscription (k5), every resolved regular expression (k16), the
/// deprecated subscription metadata (k4) whether or not the log holds one,
/// and the group epoch (k3).
///
/// The order is what lets Kafka's replay accept the tombstones: a member
/// tombstone needs the member's current assignment tombstoned and its target
/// gone, the target metadata tombstone needs every target gone, and the
/// group tombstone needs every member gone and the target metadata
/// tombstoned. Kafka iterates the hash maps of the group; the broker sorts
/// the member ids and regexes so that the bytes do not depend on map order.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when a string of a key is longer than
/// 32767 bytes.
pub(crate) fn group_tombstone_keys(
    group_id: &str,
    tombstone: &GroupTombstone,
) -> Result<Vec<bytes::Bytes>, BrokerError> {
    let mut members = tombstone.members.clone();
    members.sort_unstable();
    let mut regexes = tombstone.regexes.clone();
    regexes.sort_unstable();
    let group = || group_id.to_owned();
    let mut keys = Vec::new();
    for member_id in &members {
        keys.push(NextGenKey::CurrentMemberAssignment {
            group_id: group(),
            member_id: member_id.clone(),
        });
    }
    for member_id in &members {
        keys.push(NextGenKey::TargetAssignmentMember {
            group_id: group(),
            member_id: member_id.clone(),
        });
    }
    keys.push(NextGenKey::TargetAssignmentMetadata { group_id: group() });
    for member_id in &members {
        keys.push(NextGenKey::MemberMetadata {
            group_id: group(),
            member_id: member_id.clone(),
        });
    }
    for regex in regexes {
        keys.push(NextGenKey::RegularExpression {
            group_id: group(),
            regex,
        });
    }
    keys.push(NextGenKey::PartitionMetadata { group_id: group() });
    keys.push(NextGenKey::GroupMetadata { group_id: group() });
    keys.iter().map(encode_key).collect()
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct PendingRecords {
    pub group_metadata: Option<GroupMetadataValue>,
    /// The regular expressions whose resolution the transition writes
    /// (`Some(value)`) or tombstones (`None`), by regular expression.
    pub resolved_regexes: Vec<(String, Option<RegularExpressionValue>)>,
    /// `Some(value)` writes the record. `None` writes a tombstone (null
    /// value).
    pub member_metadata: Vec<(String, Option<MemberMetadataValue>)>,
    pub target_metadata: Option<TargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<TargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<CurrentMemberAssignmentValue>)>,
    /// When set, the batch also tombstones the classic k2 `GroupMetadata`
    /// record for this group. An upgrade flip sets it.
    pub classic_group_metadata_tombstone: bool,
    /// Whether the batch tombstones the deprecated k4
    /// `ConsumerGroupPartitionMetadata` on a metadata update, as Kafka 4.3.1
    /// does for a group that holds one. A k3 tombstone carries a k4 tombstone
    /// whatever this says, as Kafka's `createGroupTombstoneRecords` does.
    pub partition_metadata: PartitionMetadataWrite,
    /// The tombstones of the whole consumer group, in the order of Kafka's
    /// `ConsumerGroup.createGroupTombstoneRecords` (downgrade flip).
    pub group_tombstone: Option<GroupTombstone>,
    /// Write the classic k2 `GroupMetadata` value (downgrade flip).
    pub classic_group_metadata:
        Option<crate::coordinator::unified::persistence::GroupMetadataValue>,
    /// The records that follow these in the same batch, such as a
    /// heartbeat's own records after the records of Kafka's `replaceMember`.
    pub then: Option<Box<PendingRecords>>,
}

impl MemberRecordFamilies for PendingRecords {
    type Metadata = MemberMetadataValue;
    type Target = TargetAssignmentMemberValue;
    type Current = CurrentMemberAssignmentValue;

    fn member_record_families(&mut self) -> MemberRecordLists<'_, Self> {
        (
            &mut self.member_metadata,
            &mut self.target_per_member,
            &mut self.current_per_member,
        )
    }
}

impl PendingRecords {
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.resolved_regexes.is_empty()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && !self.classic_group_metadata_tombstone
            && self.partition_metadata == PartitionMetadataWrite::Keep
            && self.group_tombstone.is_none()
            && self.classic_group_metadata.is_none()
            && self.then.as_ref().is_none_or(|then| then.is_empty())
    }

    /// Whether these records, or the records that follow them, tombstone the
    /// deprecated k4 record on a metadata update.
    pub fn tombstones_partition_metadata(&self) -> bool {
        self.partition_metadata == PartitionMetadataWrite::Tombstone
            || self
                .then
                .as_ref()
                .is_some_and(|then| then.tombstones_partition_metadata())
    }

    /// Appends `next` after these records and the records already chained
    /// after them.
    #[must_use]
    pub fn followed_by(mut self, next: PendingRecords) -> PendingRecords {
        let mut tail = &mut self;
        while tail.then.is_some() {
            tail = tail.then.as_mut().expect("checked above");
        }
        tail.then = Some(Box::new(next));
        self
    }

    /// Encodes the delta as the one batch that `OffsetsLog::append` takes, in
    /// the order of Kafka's `GroupMetadataManager`:
    ///
    /// 1. the classic k2 tombstone of an upgrade, before any consumer record,
    ///    as Kafka's `convertToConsumerGroup` writes it, since the replay of a
    ///    consumer record refuses a group that is still classic;
    /// 2. the tombstones of each removed member, current assignment (k8),
    ///    target assignment (k7) and subscription (k5), as Kafka's
    ///    `removeMember` writes them and its replay requires;
    /// 3. the subscriptions (k5) of `consumerGroupHeartbeat`'s step 1, the
    ///    resolved regular expressions (k16), and the group epoch (k3) with
    ///    the deprecated k4 tombstone;
    /// 4. the target assignment of `TargetAssignmentBuilder`: the members'
    ///    targets (k7), then its metadata (k6);
    /// 5. the current assignments (k8);
    /// 6. the group tombstones of [`group_tombstone_keys`], and the classic
    ///    k2 value of a downgrade after them.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when a string of a record key or of the
    /// classic group value is longer than 32767 bytes, which a non-flexible
    /// field cannot carry.
    pub fn to_batch(&self, group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
        let mut batch = OffsetRecordBatchBuilder::default();
        let mut next = Some(self);
        while let Some(records) = next {
            records.push_records(group_id, &mut batch)?;
            next = records.then.as_deref();
        }
        Ok(batch.finish(now_ms))
    }

    fn push_records(
        &self,
        group_id: &str,
        batch: &mut OffsetRecordBatchBuilder,
    ) -> Result<(), BrokerError> {
        let key = |key: NextGenKey| encode_key(&key);
        let group = || group_id.to_owned();

        if self.classic_group_metadata_tombstone {
            batch.push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group(),
                    },
                )?,
                None,
            );
        }
        let removed: Vec<&String> = self
            .member_metadata
            .iter()
            .filter(|(_, value)| value.is_none())
            .map(|(member_id, _)| member_id)
            .collect();
        let is_removed = |member_id: &String| removed.contains(&member_id);
        for member_id in &removed {
            if self
                .current_per_member
                .iter()
                .any(|(id, value)| id == *member_id && value.is_none())
            {
                batch.push(
                    key(NextGenKey::CurrentMemberAssignment {
                        group_id: group(),
                        member_id: (*member_id).clone(),
                    })?,
                    None,
                );
            }
            if self
                .target_per_member
                .iter()
                .any(|(id, value)| id == *member_id && value.is_none())
            {
                batch.push(
                    key(NextGenKey::TargetAssignmentMember {
                        group_id: group(),
                        member_id: (*member_id).clone(),
                    })?,
                    None,
                );
            }
            batch.push(
                key(NextGenKey::MemberMetadata {
                    group_id: group(),
                    member_id: (*member_id).clone(),
                })?,
                None,
            );
        }
        for (member_id, v) in &self.member_metadata {
            if let Some(v) = v {
                batch.push(
                    key(NextGenKey::MemberMetadata {
                        group_id: group(),
                        member_id: member_id.clone(),
                    })?,
                    Some(v.encode()),
                );
            }
        }
        for (regex, v) in &self.resolved_regexes {
            batch.push(
                key(NextGenKey::RegularExpression {
                    group_id: group(),
                    regex: regex.clone(),
                })?,
                v.as_ref().map(RegularExpressionValue::encode),
            );
        }
        if let Some(v) = self.group_metadata {
            batch.push(
                key(NextGenKey::GroupMetadata { group_id: group() })?,
                Some(v.encode()),
            );
        }
        // Kafka's `updateSubscriptionMetadata` adds the k4 tombstone right
        // after the epoch record, which it writes only for a bump.
        if self.partition_metadata == PartitionMetadataWrite::Tombstone {
            batch.push(partition_metadata_key(group_id)?, None);
        }
        for (member_id, v) in &self.target_per_member {
            if is_removed(member_id) && v.is_none() {
                continue;
            }
            batch.push(
                key(NextGenKey::TargetAssignmentMember {
                    group_id: group(),
                    member_id: member_id.clone(),
                })?,
                v.as_ref().map(TargetAssignmentMemberValue::encode),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                key(NextGenKey::TargetAssignmentMetadata { group_id: group() })?,
                Some(v.encode()),
            );
        }
        for (member_id, v) in &self.current_per_member {
            if is_removed(member_id) && v.is_none() {
                continue;
            }
            batch.push(
                key(NextGenKey::CurrentMemberAssignment {
                    group_id: group(),
                    member_id: member_id.clone(),
                })?,
                v.as_ref().map(CurrentMemberAssignmentValue::encode),
            );
        }
        if let Some(tombstone) = &self.group_tombstone {
            for key in group_tombstone_keys(group_id, tombstone)? {
                batch.push(key, None);
            }
        }
        if let Some(v) = &self.classic_group_metadata {
            batch.push(
                crate::coordinator::unified::persistence::encode_key(
                    &crate::coordinator::unified::persistence::Key::GroupMetadata {
                        group_id: group(),
                    },
                )?,
                Some(v.encode_value()?),
            );
        }
        Ok(())
    }

    /// Apply exactly this durable next-gen record delta to the respawn cache.
    pub(super) fn apply_to_cache(mut self, coordinator: &GroupCoordinator, group_id: &str) {
        let then = self.then.take();
        self.apply_own_to_cache(coordinator, group_id);
        if let Some(then) = then {
            then.apply_to_cache(coordinator, group_id);
        }
    }

    fn apply_own_to_cache(self, coordinator: &GroupCoordinator, group_id: &str) {
        if self.group_tombstone.is_some() {
            coordinator.remove_cached_seed(group_id);
            return;
        }
        coordinator.update_cached_seed(group_id, |seed| {
            if let Some(value) = self.group_metadata {
                seed.group_epoch = value.epoch;
                seed.metadata_hash = value.metadata_hash;
            }
            if self.partition_metadata == PartitionMetadataWrite::Tombstone {
                seed.has_subscription_metadata_record = false;
            }
            for (regex, value) in self.resolved_regexes {
                if let Some(value) = value {
                    seed.resolved_regexes.insert(regex, value);
                } else {
                    seed.resolved_regexes.remove(&regex);
                }
            }
            for (member_id, value) in self.member_metadata {
                if let Some(value) = value {
                    seed.members.insert(member_id, value);
                } else {
                    seed.members.remove(&member_id);
                }
            }
            if let Some(value) = self.target_metadata {
                seed.target_epoch = value.assignment_epoch;
                seed.assignment_timestamp_ms = value.assignment_timestamp_ms;
            }
            for (member_id, value) in self.target_per_member {
                if let Some(value) = value {
                    seed.target_per_member.insert(member_id, value);
                } else {
                    seed.target_per_member.remove(&member_id);
                }
            }
            for (member_id, value) in self.current_per_member {
                if let Some(value) = value {
                    seed.current_per_member.insert(member_id, value);
                } else {
                    seed.current_per_member.remove(&member_id);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::assert;

    use super::*;
    use crate::{
        coordinator::unified::{
            actor::{
                persistence::full_pending_records,
                test_support::{make_coordinator, subscribed_member},
            },
            consumer_state::GroupState,
            persistence_next_gen::MemberAssignmentState,
        },
        error::BrokerError,
    };

    #[test]
    fn pending_records_empty_yields_empty_batch() {
        let p = PendingRecords::default();
        let batch = p.to_batch("g", 0).unwrap();
        assert!(batch.records.is_empty());
    }

    #[test]
    fn pending_records_offset_deltas_are_sequential() {
        let p = PendingRecords {
            group_metadata: Some(GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            member_metadata: vec![(
                "m1".into(),
                Some(MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c".into(),
                    client_host: "h".into(),
                    subscribed_topic_names: vec!["t".into()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: None,
                }),
            )],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }),
            ..Default::default()
        };
        let batch = p.to_batch("g", 0).unwrap();
        assert!(batch.records.len() == 3);
        let deltas: Vec<i32> = batch.records.iter().map(|r| r.offset_delta).collect();
        assert!(deltas == vec![0, 1, 2]);
        assert!(batch.last_offset_delta == 2);
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.to_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    /// A delta with one record of every kind that writes a string with an
    /// `INT16` length: the keys of the next-gen records, the classic group
    /// value, and the classic group tombstone.
    fn every_record() -> PendingRecords {
        use crate::coordinator::unified::persistence::{
            GroupMetadataValue as ClassicGroupValue, MemberMetadata,
        };

        PendingRecords {
            group_metadata: Some(GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            resolved_regexes: vec![("r".into(), None)],
            member_metadata: vec![("m".into(), None)],
            target_metadata: Some(TargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }),
            target_per_member: vec![("m".into(), None)],
            current_per_member: vec![("m".into(), None)],
            classic_group_metadata_tombstone: true,
            partition_metadata: PartitionMetadataWrite::Tombstone,
            group_tombstone: Some(GroupTombstone {
                members: vec!["m".into()],
                regexes: vec!["r".into()],
            }),
            classic_group_metadata: Some(ClassicGroupValue {
                protocol_type: "consumer".into(),
                generation: 1,
                protocol_name: Some("range".into()),
                leader: Some("m".into()),
                current_state_timestamp_ms: 0,
                members: vec![MemberMetadata {
                    member_id: "m".into(),
                    group_instance_id: Some("i".into()),
                    client_id: "c".into(),
                    client_host: "h".into(),
                    rebalance_timeout_ms: 1,
                    session_timeout_ms: 1,
                    subscription: bytes::Bytes::new(),
                    assignment: bytes::Bytes::new(),
                }],
            }),
            then: None,
        }
    }

    /// Every string that a group request or the broker supplies to these
    /// records is written with an `INT16` length. A string of 32767 bytes
    /// encodes, and one of 32768 bytes makes the whole batch an error, not a
    /// panic in the actor that writes it.
    #[test]
    fn a_record_string_over_32767_bytes_does_not_encode() {
        type Field = (&'static str, fn(&mut PendingRecords, String));
        let fields: [Field; 11] = [
            ("member id of a member record", |p, s| {
                p.member_metadata[0].0 = s;
            }),
            ("member id of a target assignment", |p, s| {
                p.target_per_member[0].0 = s;
            }),
            ("member id of a current assignment", |p, s| {
                p.current_per_member[0].0 = s;
            }),
            ("regular expression", |p, s| p.resolved_regexes[0].0 = s),
            ("classic protocol type", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().protocol_type = s;
            }),
            ("classic protocol name", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().protocol_name = Some(s);
            }),
            ("classic leader", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().leader = Some(s);
            }),
            ("classic member id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].member_id = s;
            }),
            ("classic instance id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].group_instance_id = Some(s);
            }),
            ("classic client id", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].client_id = s;
            }),
            ("classic client host", |p, s| {
                p.classic_group_metadata.as_mut().unwrap().members[0].client_host = s;
            }),
        ];
        let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
        for (name, set) in fields {
            for (length, encodes) in [(limit, true), (limit + 1, false)] {
                let mut pending = every_record();
                set(&mut pending, "a".repeat(length));

                let outcome = pending.to_batch("g", 0);

                assert!(outcome.is_ok() == encodes, "{name} of {length} bytes");
                assert!(
                    encodes || matches!(outcome, Err(BrokerError::Protocol(_))),
                    "{name} of {length} bytes is a protocol error"
                );
            }
        }
        for (length, encodes) in [(limit, true), (limit + 1, false)] {
            let outcome = every_record().to_batch(&"g".repeat(length), 0);

            assert!(outcome.is_ok() == encodes, "group id of {length} bytes");
        }
    }

    #[test]
    fn pending_delta_populates_cache_including_classic_facade() {
        use crate::coordinator::unified::persistence_next_gen as p;

        let topic = {
            let mut b = [0u8; 16];
            b[15] = 0xEF;
            krabka_protocol::primitives::uuid::Uuid(b)
        };
        let mut state = GroupState::new("g");
        state.group_epoch = 7;
        state.target.epoch = 6;

        let mut m = subscribed_member(
            crate::coordinator::unified::actor::test_support::ConsumerMemberSetup {
                client: crate::coordinator::unified::ClientIdentity {
                    id: "client-a",
                    host: "h",
                },
                now: Instant::now(),
                ..Default::default()
            },
        );
        m.member_epoch = 7;
        m.previous_member_epoch = 6;
        m.assigned_partitions.insert(topic, vec![0, 1]);
        m.assignment_epochs.insert(topic, [(0, 4), (1, 7)].into());
        m.classic = Some(
            crate::coordinator::unified::consumer_state::ClassicMemberFacade {
                supported_protocols: vec![(
                    "range".to_string(),
                    bytes::Bytes::from_static(b"meta"),
                )],
                session_timeout: Duration::from_secs(45),
            },
        );
        state
            .target
            .per_member
            .insert("m1".to_string(), maplit::hashmap! {topic => vec![0, 1, 2]});
        state.add_or_update_member(m);

        let mut pending = full_pending_records(&state);
        let resolution = p::RegularExpressionValue {
            topics: vec!["t".to_string()],
            version: 9,
            timestamp_ms: 1_000,
        };
        pending.resolved_regexes = vec![("t.*".to_string(), Some(resolution.clone()))];
        let (coordinator, _) = make_coordinator();
        pending.apply_to_cache(&coordinator, "g");
        let seed = coordinator.cached_seed("g").expect("cached seed");

        let expected = crate::coordinator::unified::GroupSeed {
            has_subscription_metadata_record: false,
            group_epoch: 7,
            metadata_hash: 0,
            target_epoch: 6,
            assignment_timestamp_ms: 0,
            members: maplit::hashmap! {"m1".to_string() => p::MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "client-a".to_string(),
                client_host: "h".to_string(),
                subscribed_topic_names: vec!["t".to_string()],
                subscribed_topic_regex: Some(String::new()),
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic: Some(p::ClassicMemberMetadata {
                    session_timeout_ms: 45_000,
                    supported_protocols: vec![(
                        "range".to_string(),
                        bytes::Bytes::from_static(b"meta"),
                    )],
                }),
            }},
            target_per_member: maplit::hashmap! {"m1".to_string() => p::TargetAssignmentMemberValue {
                topic_partitions: vec![p::AssignedTopicPartitions {
                    topic_id: topic,
                    partitions: vec![0, 1, 2],
                }],
            }},
            current_per_member: maplit::hashmap! {"m1".to_string() => p::CurrentMemberAssignmentValue {
                member_epoch: 7,
                previous_member_epoch: 6,
                state: MemberAssignmentState::Stable,
                assigned_partitions: vec![p::CurrentTopicPartitions {
                    topic_id: topic,
                    partitions: vec![0, 1],
                    assignment_epochs: Some(vec![4, 7]),
                }],
                partitions_pending_revocation: vec![],
            }},
            resolved_regexes: maplit::hashmap! {"t.*".to_string() => resolution},
        };
        assert!(seed == expected);
    }

    /// A resolved regular expression is written under the key of Kafka's
    /// `ConsumerGroupRegularExpressionKey`, and a tombstone removes it from
    /// the respawn cache.
    #[test]
    fn a_resolved_regex_is_written_and_tombstoned() {
        use crate::coordinator::unified::persistence_next_gen as p;

        let value = p::RegularExpressionValue {
            topics: vec!["t".to_string()],
            version: 9,
            timestamp_ms: 1_000,
        };
        let write = PendingRecords {
            resolved_regexes: vec![("t.*".to_string(), Some(value.clone()))],
            ..Default::default()
        };
        let batch = write.to_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(
            batch.records[0].key.as_deref()
                == Some(
                    &p::encode_key(&p::NextGenKey::RegularExpression {
                        group_id: "g".into(),
                        regex: "t.*".into(),
                    })
                    .unwrap()[..]
                )
        );
        assert!(batch.records[0].value.as_deref() == Some(&value.encode()[..]));

        let (coordinator, _) = make_coordinator();
        write.apply_to_cache(&coordinator, "g");
        assert!(coordinator.cached_seed("g").unwrap().resolved_regexes.len() == 1);

        let tombstone = PendingRecords {
            resolved_regexes: vec![("t.*".to_string(), None)],
            ..Default::default()
        };
        assert!(
            tombstone.to_batch("g", 0).unwrap().records[0]
                .value
                .is_none()
        );
        tombstone.apply_to_cache(&coordinator, "g");
        assert!(
            coordinator
                .cached_seed("g")
                .unwrap()
                .resolved_regexes
                .is_empty()
        );
    }

    /// The records of one transition come in the order of Kafka's
    /// `GroupMetadataManager`, which is also the order its replay accepts: a
    /// classic tombstone before the consumer records of an upgrade, a removed
    /// member's k8, k7 and k5 tombstones first, then the subscriptions, the
    /// epoch, the targets and their metadata, the current assignments, and
    /// the group tombstones of `createGroupTombstoneRecords` last.
    #[test]
    fn records_follow_kafkas_order() {
        use crate::coordinator::unified::persistence::{Key, encode_key as encode_classic_key};

        let ng = |key: NextGenKey| encode_key(&key).unwrap();
        let group = || "g".to_string();
        let member = |id: &str| id.to_string();
        let k3 = ng(NextGenKey::GroupMetadata { group_id: group() });
        let k5 = |id: &str| {
            ng(NextGenKey::MemberMetadata {
                group_id: group(),
                member_id: member(id),
            })
        };
        let k4 = ng(NextGenKey::PartitionMetadata { group_id: group() });
        let k6 = ng(NextGenKey::TargetAssignmentMetadata { group_id: group() });
        let k7 = |id: &str| {
            ng(NextGenKey::TargetAssignmentMember {
                group_id: group(),
                member_id: member(id),
            })
        };
        let k8 = |id: &str| {
            ng(NextGenKey::CurrentMemberAssignment {
                group_id: group(),
                member_id: member(id),
            })
        };
        let k2 = encode_classic_key(&Key::GroupMetadata { group_id: group() }).unwrap();
        let metadata = MemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec![],
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout_ms: 1,
            classic: None,
        };
        let current = CurrentMemberAssignmentValue {
            member_epoch: 2,
            previous_member_epoch: 1,
            state: MemberAssignmentState::Stable,
            assigned_partitions: vec![],
            partitions_pending_revocation: vec![],
        };
        let target = TargetAssignmentMemberValue {
            topic_partitions: vec![],
        };
        // (case, the delta, its (key, is a tombstone) in order)
        let rows = [
            (
                "a member leaves and the group assigns again",
                PendingRecords {
                    group_metadata: Some(GroupMetadataValue {
                        epoch: 2,
                        metadata_hash: 0,
                    }),
                    member_metadata: vec![
                        ("stays".into(), Some(metadata.clone())),
                        ("leaves".into(), None),
                    ],
                    target_metadata: Some(TargetAssignmentMetadataValue {
                        assignment_epoch: 2,
                        assignment_timestamp_ms: 0,
                    }),
                    target_per_member: vec![
                        ("leaves".into(), None),
                        ("stays".into(), Some(target.clone())),
                    ],
                    current_per_member: vec![
                        ("stays".into(), Some(current.clone())),
                        ("leaves".into(), None),
                    ],
                    ..Default::default()
                },
                vec![
                    (k8("leaves"), true),
                    (k7("leaves"), true),
                    (k5("leaves"), true),
                    (k5("stays"), false),
                    (k3.clone(), false),
                    (k7("stays"), false),
                    (k6.clone(), false),
                    (k8("stays"), false),
                ],
            ),
            (
                "an upgrade tombstones the classic group first",
                PendingRecords {
                    classic_group_metadata_tombstone: true,
                    group_metadata: Some(GroupMetadataValue {
                        epoch: 2,
                        metadata_hash: 0,
                    }),
                    member_metadata: vec![("m".into(), Some(metadata.clone()))],
                    target_metadata: Some(TargetAssignmentMetadataValue {
                        assignment_epoch: 2,
                        assignment_timestamp_ms: 0,
                    }),
                    target_per_member: vec![("m".into(), Some(target.clone()))],
                    current_per_member: vec![("m".into(), Some(current.clone()))],
                    ..Default::default()
                },
                vec![
                    (k2.clone(), true),
                    (k5("m"), false),
                    (k3.clone(), false),
                    (k7("m"), false),
                    (k6.clone(), false),
                    (k8("m"), false),
                ],
            ),
            // Kafka's `replaceMember` records come first in the batch, then
            // the heartbeat's own.
            (
                "a static replacement precedes the heartbeat's records",
                PendingRecords {
                    member_metadata: vec![
                        ("leaves".into(), None),
                        ("stays".into(), Some(metadata.clone())),
                    ],
                    target_per_member: vec![
                        ("leaves".into(), None),
                        ("stays".into(), Some(target.clone())),
                    ],
                    current_per_member: vec![
                        ("leaves".into(), None),
                        ("stays".into(), Some(current.clone())),
                    ],
                    ..Default::default()
                }
                .followed_by(PendingRecords {
                    member_metadata: vec![("stays".into(), Some(metadata.clone()))],
                    current_per_member: vec![("stays".into(), Some(current.clone()))],
                    ..Default::default()
                }),
                vec![
                    (k8("leaves"), true),
                    (k7("leaves"), true),
                    (k5("leaves"), true),
                    (k5("stays"), false),
                    (k7("stays"), false),
                    (k8("stays"), false),
                    (k5("stays"), false),
                    (k8("stays"), false),
                ],
            ),
            // `updateSubscriptionMetadata` without a bump: only the k4
            // tombstone.
            (
                "a metadata refresh without a bump tombstones only the k4 record",
                PendingRecords {
                    partition_metadata: PartitionMetadataWrite::Tombstone,
                    ..Default::default()
                },
                vec![(k4.clone(), true)],
            ),
        ];
        for (case, pending, expected) in rows {
            let written: Vec<_> = pending
                .to_batch("g", 0)
                .unwrap()
                .records
                .into_iter()
                .map(|record| (record.key.unwrap(), record.value.is_none()))
                .collect();
            assert!(written == expected, "{case}");
        }
    }

    #[test]
    fn pending_group_tombstone_removes_cached_seed() {
        let (coordinator, _) = make_coordinator();
        coordinator.update_cached_seed("g", |seed| seed.group_epoch = 7);
        PendingRecords {
            group_tombstone: Some(GroupTombstone::default()),
            ..Default::default()
        }
        .apply_to_cache(&coordinator, "g");

        assert!(coordinator.cached_seed("g").is_none());
    }
}
