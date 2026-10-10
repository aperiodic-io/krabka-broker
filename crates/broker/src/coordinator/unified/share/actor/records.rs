//! Durable encoding of share-group state transitions. A
//! [`PendingShareRecords`] set collects the mutations of one transition,
//! encodes them as a single `RecordBatch`, and appends that batch to
//! `__consumer_offsets`. It is its own file because every handler in this
//! module writes through it.

use std::collections::HashMap;

use krabka_protocol::{primitives::uuid::Uuid, records::RecordBatch};

use super::seed::snapshot_seed;
use crate::{
    coordinator::unified::{
        OffsetRecordBatchBuilder,
        member_records::{
            MemberRecordFamilies, MemberRecordLists, MemberTombstone, MemberValues,
            append_removed_members,
        },
        share::{
            persistence::{
                DeletingTopic, ShareGroupCurrentMemberAssignmentValue, ShareGroupKey,
                ShareGroupMemberMetadataValue, ShareGroupMetadataValue,
                ShareGroupStatePartitionMetadataValue, ShareGroupTargetAssignmentMemberValue,
                ShareGroupTargetAssignmentMetadataValue, TopicPartitionsInfo, UNKNOWN_TOPIC_NAME,
                encode_share_key,
            },
            state::{ShareGroupState, ShareMemberState},
        },
    },
    error::BrokerError,
};

#[derive(Debug, Default, PartialEq)]
pub(crate) struct PendingShareRecords {
    pub group_metadata: Option<ShareGroupMetadataValue>,
    /// `Some(value)` writes the record. `None` writes a tombstone, which is a
    /// null value.
    pub member_metadata: Vec<(String, Option<ShareGroupMemberMetadataValue>)>,
    pub target_metadata: Option<ShareGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<ShareGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<ShareGroupCurrentMemberAssignmentValue>)>,
    /// KIP-932 `ShareGroupStatePartitionMetadata` (key v15). `Some` writes the
    /// updated Initialized/deleting record after a lifecycle Initialize/Delete.
    pub state_partition_metadata: Option<ShareGroupStatePartitionMetadataValue>,
}

impl MemberRecordFamilies for PendingShareRecords {
    type Metadata = ShareGroupMemberMetadataValue;
    type Target = ShareGroupTargetAssignmentMemberValue;
    type Current = ShareGroupCurrentMemberAssignmentValue;

    fn member_record_families(&mut self) -> MemberRecordLists<'_, Self> {
        (
            &mut self.member_metadata,
            &mut self.target_per_member,
            &mut self.current_per_member,
        )
    }
}

impl PendingShareRecords {
    pub(super) fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
            && self.state_partition_metadata.is_none()
    }

    /// Encodes the delta as the one batch that `OffsetsLog::append` takes, in
    /// the order of Kafka's `GroupMetadataManager`, which is also the order
    /// its replay accepts: the tombstones of each removed member, current
    /// assignment, target assignment and subscription, as
    /// `shareGroupFenceMember` writes them; then the members' subscriptions
    /// and the group epoch of `shareGroupHeartbeat`; then the targets and
    /// their metadata of `TargetAssignmentBuilder`; then the current
    /// assignments; and the share-state partition metadata last.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the group id or a member id is
    /// longer than 32767 bytes, which a non-flexible key string cannot carry.
    pub fn into_batch(self, group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
        let mut batch = OffsetRecordBatchBuilder::default();
        let key = |key: ShareGroupKey| encode_share_key(&key);
        let group = || group_id.to_owned();
        let removed = append_removed_members(
            &mut batch,
            &self.member_metadata,
            &self.target_per_member,
            &self.current_per_member,
            |family, member_id| {
                key(match family {
                    MemberTombstone::Current => ShareGroupKey::CurrentMemberAssignment {
                        group_id: group(),
                        member_id: member_id.to_owned(),
                    },
                    MemberTombstone::Target => ShareGroupKey::TargetAssignmentMember {
                        group_id: group(),
                        member_id: member_id.to_owned(),
                    },
                    MemberTombstone::Metadata => ShareGroupKey::MemberMetadata {
                        group_id: group(),
                        member_id: member_id.to_owned(),
                    },
                })
            },
        )?;
        for (member_id, v) in self.member_metadata {
            if let Some(v) = v {
                batch.push(
                    key(ShareGroupKey::MemberMetadata {
                        group_id: group(),
                        member_id,
                    })?,
                    Some(v.encode()),
                );
            }
        }
        if let Some(v) = self.group_metadata {
            batch.push(
                key(ShareGroupKey::GroupMetadata { group_id: group() })?,
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.target_per_member {
            if v.is_none() && removed.contains(&member_id) {
                continue;
            }
            batch.push(
                key(ShareGroupKey::TargetAssignmentMember {
                    group_id: group(),
                    member_id,
                })?,
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                key(ShareGroupKey::TargetAssignmentMetadata { group_id: group() })?,
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            if v.is_none() && removed.contains(&member_id) {
                continue;
            }
            batch.push(
                key(ShareGroupKey::CurrentMemberAssignment {
                    group_id: group(),
                    member_id,
                })?,
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.state_partition_metadata {
            batch.push(
                encode_share_key(&ShareGroupKey::StatePartitionMetadata {
                    group_id: group_id.into(),
                })?,
                Some(v.encode()),
            );
        }

        Ok(batch.finish(now_ms))
    }
}

use crate::coordinator::unified::persistence::sorted_partitions;

/// The records of one share-group transition, as Kafka's
/// `GroupMetadataManager` writes them: a record only where the transition
/// changed what it holds. [`ShareRecorder::start`] takes the values of the
/// members that the transition may change and the group epoch, and
/// [`ShareRecorder::finish`] compares them with the group after it: a member
/// that went gets `shareGroupFenceMember`'s tombstones, a changed
/// subscription a member record (`hasMemberSubscriptionChanged`), a changed
/// current assignment its record (`maybeReconcile`), a moved epoch the group
/// record, and a computed target the targets that changed and the target
/// metadata (`TargetAssignmentBuilder`).
pub(super) struct ShareRecorder {
    group_epoch: i32,
    members: MemberValues<ShareGroupMemberMetadataValue, ShareGroupCurrentMemberAssignmentValue>,
}

/// The values of `member_id` that [`ShareRecorder`] compares, if the group
/// holds the member.
fn member_values(
    state: &ShareGroupState,
    member_id: &str,
) -> Option<(
    ShareGroupMemberMetadataValue,
    ShareGroupCurrentMemberAssignmentValue,
)> {
    state.members.get(member_id).map(|member| {
        (
            member_metadata_value(member),
            current_assignment_value(member),
        )
    })
}

impl ShareRecorder {
    pub(super) fn start(state: &ShareGroupState, member_ids: &[&str]) -> Self {
        Self {
            group_epoch: state.group_epoch,
            members: MemberValues::take(member_ids.iter().copied(), |member_id| {
                member_values(state, member_id)
            }),
        }
    }

    /// The records of the transition. `target` lists the members whose target
    /// changed when the transition computed a target.
    pub(super) fn finish(
        self,
        state: &ShareGroupState,
        target: Option<&[String]>,
    ) -> PendingShareRecords {
        let mut pending = PendingShareRecords::default();
        self.members
            .record_changes(&mut pending, |member_id| member_values(state, member_id));
        if state.group_epoch != self.group_epoch {
            pending.group_metadata = Some(ShareGroupMetadataValue {
                epoch: state.group_epoch,
                metadata_hash: state.metadata_hash,
            });
        }
        if let Some(changed) = target {
            pending.target_metadata = Some(ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: state.target.epoch,
                assignment_timestamp_ms: state.assignment_timestamp_ms,
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

/// Kafka's `newShareGroupMemberSubscriptionRecord`: the topic names sorted.
pub(super) fn member_metadata_value(member: &ShareMemberState) -> ShareGroupMemberMetadataValue {
    let mut subscribed_topic_names: Vec<String> =
        member.subscribed_topic_names.iter().cloned().collect();
    subscribed_topic_names.sort_unstable();
    ShareGroupMemberMetadataValue {
        rack_id: member.rack_id.clone(),
        client_id: member.client_id.clone(),
        client_host: member.client_host.clone(),
        subscribed_topic_names,
    }
}

pub(super) fn current_assignment_value(
    member: &ShareMemberState,
) -> ShareGroupCurrentMemberAssignmentValue {
    ShareGroupCurrentMemberAssignmentValue {
        member_epoch: member.member_epoch,
        previous_member_epoch: member.previous_member_epoch,
        assigned_partitions: sorted_partitions(&member.assigned_partitions),
    }
}

pub(super) fn target_assignment_value(
    target: &HashMap<Uuid, Vec<i32>>,
) -> ShareGroupTargetAssignmentMemberValue {
    ShareGroupTargetAssignmentMemberValue {
        topic_partitions: sorted_partitions(target),
    }
}

/// Build the `ShareGroupStatePartitionMetadata` (key v15) value from the live
/// initializing, initialized and deleting sets. There is one row per topic in
/// each, and the partitions are sorted for a stable encoding.
///
/// Each row names its topic. The name comes from
/// [`ShareGroupState::topic_names`], which the lifecycle hook fills from the
/// metadata image and bootstrap replay refills from the previous record. A
/// topic id with no name left — its topic was deleted from the cluster while
/// the group still held share state for it — is written as
/// [`UNKNOWN_TOPIC_NAME`], which is what Kafka's
/// `GroupMetadataManager.attachInitValue` writes in the same position.
pub(super) fn state_partition_metadata_from(
    state: &ShareGroupState,
) -> ShareGroupStatePartitionMetadataValue {
    ShareGroupStatePartitionMetadataValue {
        initializing: topic_partitions_infos(state, state.initializing.keys()),
        initialized: topic_partitions_infos(state, state.initialized.iter()),
        deleting: deleting_topics(state),
    }
}

/// The deleting set as `DeletingTopics` rows, sorted by topic id.
fn deleting_topics(state: &ShareGroupState) -> Vec<DeletingTopic> {
    let mut topics: Vec<DeletingTopic> = state
        .deleting
        .iter()
        .map(|(topic_id, topic_name)| DeletingTopic {
            topic_id: uuid::Uuid::from_bytes(topic_id.0),
            topic_name: topic_name.clone(),
        })
        .collect();
    topics.sort_by_key(|topic| topic.topic_id);
    topics
}

/// Groups `partitions` into one named, sorted row per topic.
fn topic_partitions_infos<'a>(
    state: &ShareGroupState,
    partitions: impl Iterator<Item = &'a (Uuid, i32)>,
) -> Vec<TopicPartitionsInfo> {
    let mut by_topic: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, p) in partitions {
        by_topic.entry(*tid).or_default().push(*p);
    }
    let mut topics: Vec<TopicPartitionsInfo> = by_topic
        .into_iter()
        .map(|(tid, mut parts)| {
            parts.sort_unstable();
            TopicPartitionsInfo {
                topic_id: uuid::Uuid::from_bytes(tid.0),
                topic_name: state
                    .topic_names
                    .get(&tid)
                    .map_or_else(|| UNKNOWN_TOPIC_NAME.to_owned(), Clone::clone),
                partitions: parts,
            }
        })
        .collect();
    topics.sort_by_key(|topic| topic.topic_id);
    topics
}

crate::coordinator::unified::persistence::flush_pending_records! {
    state: &ShareGroupState, pending: PendingShareRecords;
    offsets_log, coordinator, now_ms;
    group &state.group_id;
    encode pending.into_batch(&state.group_id, now_ms);
    cache coordinator.update_share_cache(&state.group_id, snapshot_seed(state));
}

/// The wall-clock reading this actor stamps share-group records with, in
/// milliseconds since the Unix epoch. It reads `std::time`, not chrono, which
/// the name predates.
///
/// This is deliberately **not** [`crate::time_util::now_ms`], for the reason
/// its twin in [`crate::coordinator::unified::actor`] gives: the two disagree
/// on the `i64`-overflow arm, which saturates to `i64::MAX` in the shared
/// helper and to `0` here.
pub(super) use crate::txn::util::now_millis as chrono_now_ms;

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn state_partition_metadata_names_every_topic_it_lists() {
        let named = Uuid([1; 16]);
        let forgotten = Uuid([2; 16]);
        let mut state = ShareGroupState::new("g");
        for partition in [1, 0] {
            state.initialized.insert((named, partition));
        }
        state.initialized.insert((forgotten, 0));
        state.initializing.insert((named, 2), 5);
        state.topic_names.insert(named, "orders".to_owned());
        state.deleting.insert(forgotten, "carts".to_owned());
        state.deleting.insert(named, "orders".to_owned());

        // Rows sorted by topic id, partitions sorted, and the topic whose name
        // the group no longer knows gets Kafka's `<UNKNOWN>` placeholder.
        assert!(
            state_partition_metadata_from(&state)
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![TopicPartitionsInfo {
                        topic_id: uuid::Uuid::from_bytes([1; 16]),
                        topic_name: "orders".to_owned(),
                        partitions: vec![2],
                    }],
                    initialized: vec![
                        TopicPartitionsInfo {
                            topic_id: uuid::Uuid::from_bytes([1; 16]),
                            topic_name: "orders".to_owned(),
                            partitions: vec![0, 1],
                        },
                        TopicPartitionsInfo {
                            topic_id: uuid::Uuid::from_bytes([2; 16]),
                            topic_name: UNKNOWN_TOPIC_NAME.to_owned(),
                            partitions: vec![0],
                        },
                    ],
                    deleting: vec![
                        DeletingTopic {
                            topic_id: uuid::Uuid::from_bytes([1; 16]),
                            topic_name: "orders".to_owned(),
                        },
                        DeletingTopic {
                            topic_id: uuid::Uuid::from_bytes([2; 16]),
                            topic_name: "carts".to_owned(),
                        },
                    ],
                }
        );
    }

    #[test]
    fn pending_records_tombstone_omits_value() {
        let p = PendingShareRecords {
            member_metadata: vec![("m1".into(), None)],
            ..Default::default()
        };
        let batch = p.into_batch("g", 0).unwrap();
        assert!(batch.records.len() == 1);
        assert!(batch.records[0].value.is_none());
    }

    // The keys of the share-group records write the group id and the member
    // id with an `INT16` length. A string of 32767 bytes encodes, and one of
    // 32768 bytes makes the whole batch an error, not a panic in the actor.
    crate::coordinator::unified::persistence::key_string_boundaries!(PendingShareRecords, || {
        PendingShareRecords {
            group_metadata: Some(ShareGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }),
            member_metadata: vec![("m".into(), None)],
            target_per_member: vec![("m".into(), None)],
            current_per_member: vec![("m".into(), None)],
            ..Default::default()
        }
    });
}
