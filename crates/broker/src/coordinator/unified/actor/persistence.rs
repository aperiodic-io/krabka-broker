//! Building the durable records for a group-state transition and appending
//! them.
//!
//! These functions project live group state into the persisted next-gen
//! values, assemble the [`PendingRecords`] delta for one transition, and flush
//! it — the classic k2 snapshot for a classic group, the next-gen record set
//! plus the respawn-cache update for a consumer group.

use std::collections::HashMap;

use krabka_protocol::primitives::uuid::Uuid;

use super::{
    FALLBACK_REBALANCE_TIMEOUT_MS_I32, FALLBACK_SESSION_TIMEOUT_MS_I32, chrono_now_ms,
    pending_records::{PartitionMetadataWrite, PendingRecords},
};
use crate::coordinator::unified::{
    classic_state::ClassicGroup,
    consumer_state::{GroupState, MemberState},
    member_records::MemberValues,
    offsets_log::OffsetsLog,
    persistence_next_gen::{
        ClassicMemberMetadata, CurrentMemberAssignmentValue, CurrentTopicPartitions,
        GroupMetadataValue, MemberMetadataValue, TargetAssignmentMemberValue,
        TargetAssignmentMetadataValue,
    },
};

/// Maps a member's in-memory classic facade, if there is one, into the
/// persisted k5 `ClassicMemberMetadata` sub-block. It is the single source of
/// truth for the log-write path and its incremental cache update.
fn classic_member_metadata(m: &MemberState) -> Option<ClassicMemberMetadata> {
    m.classic.as_ref().map(|f| ClassicMemberMetadata {
        session_timeout_ms: i32::try_from(f.session_timeout.as_millis())
            .unwrap_or(FALLBACK_SESSION_TIMEOUT_MS_I32),
        supported_protocols: f.supported_protocols.clone(),
    })
}

/// Kafka's `newConsumerGroupMemberSubscriptionRecord`: the topic names
/// sorted, and the regular expression as the member holds it, which is the
/// empty string for a member that subscribes by name
/// (`ConsumerGroupMember.Builder`'s default and the classic join's).
pub(super) fn member_metadata_value(member: &MemberState) -> MemberMetadataValue {
    let mut subscribed_topic_names: Vec<String> =
        member.subscribed_topic_names.iter().cloned().collect();
    subscribed_topic_names.sort_unstable();
    MemberMetadataValue {
        instance_id: member.instance_id.clone(),
        rack_id: member.rack_id.clone(),
        client_id: member.client_id.clone(),
        client_host: member.client_host.clone(),
        subscribed_topic_names,
        subscribed_topic_regex: Some(member.subscribed_topic_regex.clone().unwrap_or_default()),
        server_assignor: member.server_assignor.clone(),
        rebalance_timeout_ms: i32::try_from(member.rebalance_timeout.as_millis())
            .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS_I32),
        classic: classic_member_metadata(member),
    }
}

/// Projects one of the member's partition maps into the k8
/// `TopicPartitions`, with the assignment epoch of every partition, as
/// Kafka's `GroupCoordinatorRecordHelpers.toTopicPartitions` writes it.
///
/// The topics go in id order and the partitions in ascending order, so that
/// two equal assignments give equal records.
fn current_topic_partitions(
    member: &MemberState,
    partitions: &HashMap<Uuid, Vec<i32>>,
) -> Vec<CurrentTopicPartitions> {
    crate::coordinator::unified::persistence::sorted_partitions(partitions)
        .into_iter()
        .map(|(topic_id, partitions)| CurrentTopicPartitions {
            topic_id,
            assignment_epochs: Some(
                partitions
                    .iter()
                    .map(|&partition| {
                        member
                            .assignment_epoch(&topic_id, partition)
                            .unwrap_or_else(|| member.member_epoch.max(0))
                    })
                    .collect(),
            ),
            partitions,
        })
        .collect()
}

pub(super) fn current_assignment_value(member: &MemberState) -> CurrentMemberAssignmentValue {
    CurrentMemberAssignmentValue {
        member_epoch: member.member_epoch,
        previous_member_epoch: member.previous_member_epoch,
        state: member.assignment_state,
        assigned_partitions: current_topic_partitions(member, &member.assigned_partitions),
        partitions_pending_revocation: current_topic_partitions(
            member,
            &member.partitions_pending_revocation,
        ),
    }
}

pub(super) fn target_assignment_value(
    target: &HashMap<Uuid, Vec<i32>>,
) -> TargetAssignmentMemberValue {
    use crate::coordinator::unified::persistence_next_gen::AssignedTopicPartitions;
    let mut topic_partitions: Vec<AssignedTopicPartitions> = target
        .iter()
        .filter(|(_, partitions)| !partitions.is_empty())
        .map(|(topic_id, partitions)| {
            let mut partitions = partitions.clone();
            partitions.sort_unstable();
            AssignedTopicPartitions {
                topic_id: *topic_id,
                partitions,
            }
        })
        .collect();
    topic_partitions.sort_by_key(|topic| topic.topic_id.0);
    TargetAssignmentMemberValue { topic_partitions }
}

/// The records of one consumer-group transition, as Kafka's
/// `GroupMetadataManager` writes them: a record only where the transition
/// changed what it holds.
///
/// [`Recorder::start`] takes the values of the members that the transition
/// may change, and the group epoch. [`Recorder::finish`] compares them with
/// the group after the transition:
///
/// - a member that went gets the tombstones of Kafka's `removeMember`;
/// - a member whose subscription changed, a new member included, gets a
///   `ConsumerGroupMemberMetadata` record (`hasMemberSubscriptionChanged`);
/// - a member whose current assignment changed gets a
///   `ConsumerGroupCurrentMemberAssignment` record (`maybeReconcile`);
/// - a moved group epoch gets a `ConsumerGroupMetadata` record (the epoch bump
///   of `updateSubscriptionMetadata` and of a fence);
/// - a computed target gets the target records of the members whose target
///   changed, and the target metadata (`TargetAssignmentBuilder`).
pub(super) struct Recorder {
    group_epoch: i32,
    members: MemberValues<MemberMetadataValue, CurrentMemberAssignmentValue>,
}

/// The values of `member_id` that [`Recorder`] compares, if the group holds
/// the member.
fn member_values(
    state: &GroupState,
    member_id: &str,
) -> Option<(MemberMetadataValue, CurrentMemberAssignmentValue)> {
    state.members.get(member_id).map(|member| {
        (
            member_metadata_value(member),
            current_assignment_value(member),
        )
    })
}

impl Recorder {
    /// Takes the group epoch and the values of `member_ids`.
    pub(super) fn start<S: AsRef<str>>(state: &GroupState, member_ids: &[S]) -> Self {
        Self {
            group_epoch: state.group_epoch,
            members: MemberValues::take(member_ids.iter().map(AsRef::as_ref), |member_id| {
                member_values(state, member_id)
            }),
        }
    }

    /// The records of the transition. `target` lists the members whose target
    /// changed when the transition computed a target. `partition_metadata`
    /// is set when the transition ran Kafka's `updateSubscriptionMetadata`
    /// for a group whose log holds the deprecated k4 record.
    pub(super) fn finish(
        self,
        state: &GroupState,
        target: Option<&[String]>,
        partition_metadata: bool,
    ) -> PendingRecords {
        let mut pending = PendingRecords::default();
        self.members
            .record_changes(&mut pending, |member_id| member_values(state, member_id));
        if state.group_epoch != self.group_epoch {
            pending.group_metadata = Some(GroupMetadataValue {
                epoch: state.group_epoch,
                metadata_hash: state.metadata_hash(),
            });
        }
        if partition_metadata {
            pending.partition_metadata = PartitionMetadataWrite::Tombstone;
        }
        if let Some(changed) = target {
            pending.target_metadata = Some(TargetAssignmentMetadataValue {
                assignment_epoch: state.target.epoch,
                assignment_timestamp_ms: state.assignment_timestamp_ms(),
            });
            crate::coordinator::unified::member_records::append_target_records(
                &mut pending.target_per_member,
                changed,
                &state.target.per_member,
                target_assignment_value,
            );
        }
        pending
    }
}

/// Builds a `PendingRecords` set that describes the WHOLE consumer group: the
/// group epoch, the target epoch when non-zero, and every member's k5
/// member-metadata (facade included), k8 current-assignment, and k7 target
/// when present. The upgrade flip uses it to write the full converted group
/// atomically in one batch.
///
/// It is Kafka's `ConsumerGroup.createConsumerGroupRecords`: every member's
/// subscription, the group epoch, every member's target (an empty one for a
/// member with none), the target metadata and every member's current
/// assignment. The broker sorts the members where Kafka iterates a hash map.
pub(crate) fn full_pending_records(state: &GroupState) -> PendingRecords {
    let mut member_ids: Vec<&String> = state.members.keys().collect();
    member_ids.sort_unstable();
    let mut pending = PendingRecords {
        group_metadata: Some(GroupMetadataValue {
            epoch: state.group_epoch,
            metadata_hash: state.metadata_hash(),
        }),
        target_metadata: Some(TargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
            assignment_timestamp_ms: state.assignment_timestamp_ms(),
        }),
        ..PendingRecords::default()
    };
    for member_id in member_ids {
        let member = &state.members[member_id];
        pending
            .member_metadata
            .push((member_id.clone(), Some(member_metadata_value(member))));
        let target = state
            .target
            .per_member
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        pending
            .target_per_member
            .push((member_id.clone(), Some(target_assignment_value(&target))));
        pending
            .current_per_member
            .push((member_id.clone(), Some(current_assignment_value(member))));
    }
    pending
}

/// Builds a wire-faithful classic k2 `GroupMetadataValue` from a downgraded
/// [`ClassicGroup`].
///
/// It persists every classic member with its `subscription` (the selected
/// `protocol_metadata`) and its `assignment` (the seed the downgrade computed
/// from the next-gen target). Bootstrap replay therefore reconstructs the
/// classic group with its members and their assignments intact. See
/// `apply_group_metadata` in `coordinator::bootstrap`. The downgrade flip uses
/// this function.
pub(crate) fn classic_group_metadata_record(
    state: &ClassicGroup,
    now_ms: i64,
) -> crate::coordinator::unified::persistence::GroupMetadataValue {
    use crate::coordinator::unified::persistence::{GroupMetadataValue, MemberMetadata};
    let members = state
        .members
        .values()
        .map(|m| MemberMetadata {
            member_id: m.id.clone(),
            group_instance_id: m.group_instance_id.clone(),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            rebalance_timeout_ms: i32::try_from(m.rebalance_timeout.as_millis())
                .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS_I32),
            session_timeout_ms: i32::try_from(m.session_timeout.as_millis())
                .unwrap_or(FALLBACK_SESSION_TIMEOUT_MS_I32),
            subscription: m.protocol_metadata.clone(),
            assignment: m.assignment.clone().unwrap_or_default(),
        })
        .collect();
    GroupMetadataValue {
        protocol_type: state
            .protocol_type
            .clone()
            .unwrap_or_else(|| "consumer".into()),
        generation: state.generation_id,
        protocol_name: state.protocol_name.clone(),
        leader: state.leader_id.clone(),
        current_state_timestamp_ms: now_ms,
        members,
    }
}

/// Append the complete classic k2 snapshot for one durable group transition.
pub(super) async fn flush_classic_metadata(
    state: &ClassicGroup,
    offsets_log: &dyn OffsetsLog,
) -> Result<(), crate::error::BrokerError> {
    let now_ms = chrono_now_ms();
    let pending = PendingRecords {
        classic_group_metadata: Some(classic_group_metadata_record(state, now_ms)),
        ..PendingRecords::default()
    };
    let batch = pending.to_batch(&state.group_id, now_ms)?;
    offsets_log.append(&state.group_id, batch).await
}

crate::coordinator::unified::persistence::flush_pending_records! {
    state: &mut GroupState, pending: PendingRecords;
    offsets_log, coordinator, now_ms;
    group &state.group_id;
    encode pending.to_batch(&state.group_id, now_ms);
    cache {
        state.mark_persisted();
        // Kafka clears `hasSubscriptionMetadataRecord` when it replays the k4
        // tombstone it just wrote.
        if pending.tombstones_partition_metadata() {
            state.set_has_subscription_metadata_record(false);
        }
        pending.apply_to_cache(coordinator, &state.group_id);
    };
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use assert2::{assert, check};
    use krabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

    use super::*;
    use crate::coordinator::unified::actor::member_state::build_member;

    fn joined(member_id: &str) -> MemberState {
        super::super::test_support::subscribed_member(
            super::super::test_support::ConsumerMemberSetup {
                member_id,
                ..Default::default()
            },
        )
    }

    /// Kafka writes a record only where a transition changed what it holds:
    /// (case, the change to a group that holds `m1` at epoch 2 with target
    /// [0] at epoch 2, the members the transition may change, the target
    /// records it reports, the whole delta it writes).
    #[test]
    fn a_transition_writes_only_what_it_changed() {
        type Change = fn(&mut GroupState);
        type Row<'a> = (&'a str, Change, &'a [&'a str], Option<Vec<String>>);
        let topic_id = Uuid([9; 16]);
        let base = || {
            let mut state = GroupState::new("g");
            state.add_or_update_member(joined("m1"));
            state.group_epoch = 2;
            state.target.epoch = 2;
            state
                .target
                .per_member
                .insert("m1".into(), [(Uuid([9; 16]), vec![0])].into());
            state
        };
        let metadata = |state: &GroupState, member_id: &str| {
            Some(member_metadata_value(&state.members[member_id]))
        };
        let current = |state: &GroupState, member_id: &str| {
            Some(current_assignment_value(&state.members[member_id]))
        };
        let rows: [Row<'_>; 7] = [
            ("nothing changes", |_| {}, &["m1"], None),
            (
                "a member joins",
                |state| state.add_or_update_member(joined("m2")),
                &["m2"],
                None,
            ),
            (
                "a member's client id changes",
                |state| state.members.get_mut("m1").unwrap().client_id = "other".into(),
                &["m1"],
                None,
            ),
            (
                "a member moves to the target epoch",
                |state| {
                    let member = state.members.get_mut("m1").unwrap();
                    member.previous_member_epoch = member.member_epoch;
                    member.member_epoch = 2;
                },
                &["m1"],
                None,
            ),
            (
                "a member leaves",
                |state| drop(state.remove_member("m1")),
                &["m1"],
                None,
            ),
            (
                "the group epoch moves",
                |state| {
                    state.bump_epoch();
                },
                &[],
                None,
            ),
            (
                "a target is computed",
                |state| {
                    state.bump_epoch();
                    state.install_target(
                        [("m1".to_string(), [(Uuid([9; 16]), vec![0, 1])].into())].into(),
                    );
                },
                &["m1"],
                Some(vec!["m1".to_string()]),
            ),
        ];
        for (case, change, member_ids, target) in rows {
            let mut state = base();
            let recorder = Recorder::start(&state, member_ids);
            change(&mut state);
            let pending = recorder.finish(&state, target.as_deref(), false);
            let expected = match case {
                "nothing changes" => PendingRecords::default(),
                "a member joins" => PendingRecords {
                    member_metadata: vec![("m2".into(), metadata(&state, "m2"))],
                    current_per_member: vec![("m2".into(), current(&state, "m2"))],
                    ..PendingRecords::default()
                },
                "a member's client id changes" => PendingRecords {
                    member_metadata: vec![("m1".into(), metadata(&state, "m1"))],
                    ..PendingRecords::default()
                },
                "a member moves to the target epoch" => PendingRecords {
                    current_per_member: vec![("m1".into(), current(&state, "m1"))],
                    ..PendingRecords::default()
                },
                "a member leaves" => PendingRecords {
                    member_metadata: vec![("m1".into(), None)],
                    target_per_member: vec![("m1".into(), None)],
                    current_per_member: vec![("m1".into(), None)],
                    ..PendingRecords::default()
                },
                "the group epoch moves" => PendingRecords {
                    group_metadata: Some(GroupMetadataValue {
                        epoch: 3,
                        metadata_hash: 0,
                    }),
                    ..PendingRecords::default()
                },
                _ => PendingRecords {
                    group_metadata: Some(GroupMetadataValue {
                        epoch: 3,
                        metadata_hash: 0,
                    }),
                    target_metadata: Some(TargetAssignmentMetadataValue {
                        assignment_epoch: 3,
                        assignment_timestamp_ms: 0,
                    }),
                    target_per_member: vec![(
                        "m1".into(),
                        Some(target_assignment_value(&[(topic_id, vec![0, 1])].into())),
                    )],
                    ..PendingRecords::default()
                },
            };
            check!(pending == expected, "{case}");
        }
    }

    /// Kafka's `newConsumerGroupMemberSubscriptionRecord` sorts the topic
    /// names and writes the empty string for a member without a pattern.
    #[test]
    fn the_member_subscription_record_is_kafkas() {
        let mut member = joined("m1");
        member.subscribed_topic_names = ["b".to_string(), "a".to_string(), "c".to_string()].into();
        let value = member_metadata_value(&member);
        check!(value.subscribed_topic_names == vec!["a", "b", "c"]);
        check!(value.subscribed_topic_regex.as_deref() == Some(""));
    }

    /// The group metadata record writes each member id with an `INT16` length.
    /// A group whose member id has 32768 bytes cannot write it: the flush is an
    /// error that appends nothing, and the actor that flushes does not panic. A
    /// member id of 32767 bytes is written.
    #[tokio::test]
    async fn a_classic_group_with_an_unencodable_member_id_appends_nothing() {
        use crate::coordinator::unified::{
            classic_state::Member, offsets_log::fake::InMemoryOffsetsLog,
            persistence::MAX_STRING_BYTES,
        };

        let log = InMemoryOffsetsLog::default();
        for (length, written) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let mut state = ClassicGroup::new("g");
            state.insert_joining_member(
                Member::new(
                    "m".repeat(length),
                    "client",
                    "host",
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(10),
                    vec![("range".into(), bytes::Bytes::new())],
                ),
                "consumer",
            );

            let result = flush_classic_metadata(&state, &log).await;

            check!(result.is_ok() == written, "member id of {length} bytes");
            check!(
                written || matches!(result, Err(crate::error::BrokerError::Protocol(_))),
                "member id of {length} bytes is a protocol error"
            );
        }
        check!(log.batches().await.len() == 1);
    }

    #[test]
    fn full_pending_records_contains_every_member_record() {
        let mut state = GroupState::new("g");
        for member_id in ["m1", "m2"] {
            state.add_or_update_member(build_member(
                member_id,
                &ConsumerGroupHeartbeatRequest::default(),
                crate::coordinator::unified::ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
            ));
            state
                .target
                .per_member
                .insert(member_id.into(), HashMap::new());
        }
        state.group_epoch = 4;
        state.target.epoch = 4;

        let pending = full_pending_records(&state);

        check!(
            pending.group_metadata
                == Some(GroupMetadataValue {
                    epoch: 4,
                    metadata_hash: 0
                })
        );
        check!(
            pending.target_metadata
                == Some(TargetAssignmentMetadataValue {
                    assignment_epoch: 4,
                    assignment_timestamp_ms: 0,
                })
        );
        check!(pending.member_metadata.len() == 2);
        check!(pending.target_per_member.len() == 2);
        assert!(pending.current_per_member.len() == 2);
    }

    /// Kafka 4.3.1's `updateSubscriptionMetadata` adds the deprecated k4
    /// tombstone right after the epoch record while the group holds a k4
    /// value, and the replay of that tombstone clears the mark, so the next
    /// write carries none.
    #[tokio::test]
    async fn a_held_partition_metadata_record_is_tombstoned_once() {
        use crate::coordinator::unified::{
            actor::test_support::make_coordinator,
            persistence_next_gen::{NextGenKey, encode_key},
        };

        let (coordinator, log) = make_coordinator();
        coordinator.update_cached_seed("g", |seed| seed.has_subscription_metadata_record = true);
        let mut state = GroupState::new("g");
        state.group_epoch = 2;
        state.set_has_subscription_metadata_record(true);
        let key = |key: NextGenKey| Some(encode_key(&key).unwrap());
        let epoch_record = |epoch| {
            (
                key(NextGenKey::GroupMetadata {
                    group_id: "g".into(),
                }),
                Some(
                    GroupMetadataValue {
                        epoch,
                        metadata_hash: 0,
                    }
                    .encode(),
                ),
            )
        };

        for expected in [
            vec![
                epoch_record(3),
                (
                    key(NextGenKey::PartitionMetadata {
                        group_id: "g".into(),
                    }),
                    None,
                ),
            ],
            vec![epoch_record(4)],
        ] {
            let tombstone = state.has_subscription_metadata_record();
            let recorder = Recorder::start(&state, &[] as &[&str]);
            state.bump_epoch();
            let pending = recorder.finish(&state, None, tombstone);
            flush_pending(&mut state, pending, log.as_ref(), &coordinator, 0)
                .await
                .unwrap();

            let written: Vec<_> = log
                .batches()
                .await
                .pop()
                .unwrap()
                .records
                .into_iter()
                .map(|record| (record.key, record.value))
                .collect();
            check!(written == expected);
        }
        check!(!state.has_subscription_metadata_record());
        check!(
            !coordinator
                .cached_seed("g")
                .unwrap()
                .has_subscription_metadata_record
        );
    }
}
