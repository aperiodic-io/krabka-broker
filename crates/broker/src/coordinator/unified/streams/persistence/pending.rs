//! [`PendingStreamsRecords`], the set of record mutations for one group-state
//! transition.
//!
//! One heartbeat can change the group epoch, the topology, and several members
//! at once. The actor collects the whole change here and encodes it as a
//! single `RecordBatch` that is ready for `OffsetsLog::append`, so the
//! transition lands in the log atomically.

use krabka_protocol::records::RecordBatch;

use super::{
    assignment::{
        StreamsGroupCurrentMemberAssignmentValue, StreamsGroupTargetAssignmentMemberValue,
    },
    epochs::{StreamsGroupMetadataValue, StreamsGroupTargetAssignmentMetadataValue},
    keys::{
        encode_current_member_assignment_key, encode_group_metadata_key,
        encode_member_metadata_key, encode_target_assignment_member_key,
        encode_target_assignment_metadata_key, encode_topology_key,
    },
    member::StreamsGroupMemberMetadataValue,
    topology::StreamsGroupTopologyValue,
};
use crate::{
    coordinator::unified::{
        OffsetRecordBatchBuilder,
        member_records::{
            MemberRecordFamilies, MemberRecordLists, MemberTombstone, append_removed_members,
        },
    },
    error::BrokerError,
};

#[derive(Debug, Default)]
pub struct PendingStreamsRecords {
    pub group_metadata: Option<StreamsGroupMetadataValue>,
    /// `Some(value)` writes the record. `None` writes a tombstone (null
    /// value).
    pub member_metadata: Vec<(String, Option<StreamsGroupMemberMetadataValue>)>,
    pub topology: Option<StreamsGroupTopologyValue>,
    pub target_metadata: Option<StreamsGroupTargetAssignmentMetadataValue>,
    pub target_per_member: Vec<(String, Option<StreamsGroupTargetAssignmentMemberValue>)>,
    pub current_per_member: Vec<(String, Option<StreamsGroupCurrentMemberAssignmentValue>)>,
}

impl MemberRecordFamilies for PendingStreamsRecords {
    type Metadata = StreamsGroupMemberMetadataValue;
    type Target = StreamsGroupTargetAssignmentMemberValue;
    type Current = StreamsGroupCurrentMemberAssignmentValue;

    fn member_record_families(&mut self) -> MemberRecordLists<'_, Self> {
        (
            &mut self.member_metadata,
            &mut self.target_per_member,
            &mut self.current_per_member,
        )
    }
}

impl PendingStreamsRecords {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.group_metadata.is_none()
            && self.member_metadata.is_empty()
            && self.topology.is_none()
            && self.target_metadata.is_none()
            && self.target_per_member.is_empty()
            && self.current_per_member.is_empty()
    }

    /// Encodes the delta as the one batch that `OffsetsLog::append` takes, in
    /// the order of Kafka's `GroupMetadataManager`, which is also the order
    /// its replay accepts: the tombstones of each removed member, current
    /// assignment (k22), target assignment (k21) and metadata (k18), as
    /// `removeStreamsMember` writes them; then the members' metadata (k18),
    /// the topology (k19) and the group epoch (k17) of
    /// `streamsGroupHeartbeat`; then the targets (k21) and their metadata
    /// (k20) of `TargetAssignmentBuilder`; then the current assignments
    /// (k22).
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the group id or a member id is
    /// longer than 32767 bytes, which a non-flexible key string cannot carry.
    pub fn into_batch(self, group_id: &str, now_ms: i64) -> Result<RecordBatch, BrokerError> {
        let mut batch = OffsetRecordBatchBuilder::default();
        let removed = append_removed_members(
            &mut batch,
            &self.member_metadata,
            &self.target_per_member,
            &self.current_per_member,
            |family, member_id| match family {
                MemberTombstone::Current => {
                    encode_current_member_assignment_key(group_id, member_id)
                }
                MemberTombstone::Target => encode_target_assignment_member_key(group_id, member_id),
                MemberTombstone::Metadata => encode_member_metadata_key(group_id, member_id),
            },
        )?;
        for (member_id, v) in self.member_metadata {
            if let Some(v) = v {
                batch.push(
                    encode_member_metadata_key(group_id, &member_id)?,
                    Some(v.encode()),
                );
            }
        }
        if let Some(v) = self.topology {
            batch.push(encode_topology_key(group_id)?, Some(v.encode()));
        }
        if let Some(v) = self.group_metadata {
            batch.push(encode_group_metadata_key(group_id)?, Some(v.encode()));
        }
        for (member_id, v) in self.target_per_member {
            if v.is_none() && removed.contains(&member_id) {
                continue;
            }
            batch.push(
                encode_target_assignment_member_key(group_id, &member_id)?,
                v.map(|x| x.encode()),
            );
        }
        if let Some(v) = self.target_metadata {
            batch.push(
                encode_target_assignment_metadata_key(group_id)?,
                Some(v.encode()),
            );
        }
        for (member_id, v) in self.current_per_member {
            if v.is_none() && removed.contains(&member_id) {
                continue;
            }
            batch.push(
                encode_current_member_assignment_key(group_id, &member_id)?,
                v.map(|x| x.encode()),
            );
        }

        Ok(batch.finish(now_ms))
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn pending_records_into_batch_emits_one_record_per_key() {
        let mut pending = PendingStreamsRecords {
            group_metadata: Some(StreamsGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
                validated_topology_epoch: -1,
                last_assignment_configs: None,
                description: super::super::DescriptionEpochs::default(),
            }),
            topology: Some(StreamsGroupTopologyValue::default()),
            ..Default::default()
        };
        pending.member_metadata.push(("m1".into(), None)); // tombstone
        let batch = pending.into_batch("g1", 123).unwrap();
        // group_metadata + topology + one member tombstone = 3 records.
        check!(batch.records.len() == 3);
        check!(batch.max_timestamp == 123);
        check!(batch.last_offset_delta == 2);
        // The tombstone record carries a null value.
        let tombstone = batch.records.iter().find(|r| r.value.is_none()).unwrap();
        assert!(tombstone.key.is_some());
    }

    // The keys of the streams records write the group id and the member id
    // with an `INT16` length. A string of 32767 bytes encodes, and one of
    // 32768 bytes makes the whole batch an error, not a panic in the actor.
    crate::coordinator::unified::persistence::key_string_boundaries!(PendingStreamsRecords, || {
        PendingStreamsRecords {
            group_metadata: Some(StreamsGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
                validated_topology_epoch: -1,
                last_assignment_configs: None,
                description: super::super::DescriptionEpochs::default(),
            }),
            member_metadata: vec![("m".into(), None)],
            topology: Some(StreamsGroupTopologyValue::default()),
            target_per_member: vec![("m".into(), None)],
            current_per_member: vec![("m".into(), None)],
            ..Default::default()
        }
    });
}
