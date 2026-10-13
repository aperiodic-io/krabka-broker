//! KIP-1071 classic→streams cold conversion. When a drained classic group
//! receives a `StreamsGroupHeartbeat`, the coordinator converts the group in
//! place. It tombstones the classic `GroupMetadata` (k2) and forces the type
//! lock to `Streams`. That tombstone is defensive: it is a no-op when the
//! classic path persisted none. The coordinator KEEPS the drained classic actor
//! in the `groups` registry as the protocol-agnostic offset home, so committed
//! offsets (k0/k1) survive the flip untouched. `OffsetFetch` and `OffsetCommit`
//! route there through `coordinator.find()` for every protocol, streams
//! included.
//!
//! Streams migration is COLD only, because Kafka does not support online
//! streams migration, so there is no hosted-classic-member translation here.

use {
    super::persistence::{
        encode_current_member_assignment_key, encode_group_metadata_key,
        encode_member_metadata_key, encode_target_assignment_member_key,
        encode_target_assignment_metadata_key, encode_topology_key,
    },
    crate::{
        coordinator::unified::{OffsetRecordBatchBuilder, actor::PendingRecords},
        error::BrokerError,
    },
    krabka_protocol::records::RecordBatch,
};

/// Result of inspecting a `group_id` for classic→streams conversion.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConvertOutcome {
    /// Not a classic group (fresh streams group or already streams). Serve normally.
    NotClassic,
    /// Was a drained classic group. The coordinator converted it in place to streams.
    Converted,
    /// Classic group has live members. Online streams migration is not supported.
    RejectLiveMembers,
}

/// Result of inspecting a `group_id` for a streams→classic cold downgrade.
/// The mirror of [`ConvertOutcome`] for the opposite direction.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DowngradeOutcome {
    /// Not a streams group. Serve the classic `JoinGroup` normally.
    NotStreams,
    /// Was a drained streams group, and the join names no member. The
    /// coordinator wrote [`streams_to_classic_batch`] and converted the group
    /// in place to an empty classic group, which serves the join.
    Converted,
    /// Was a drained streams group, and the join names a member. The
    /// coordinator dropped the streams group without a record, and the join
    /// answers `UNKNOWN_MEMBER_ID`.
    Removed,
    /// Streams group has live members. The join answers
    /// `INCONSISTENT_GROUP_PROTOCOL`.
    RejectLiveMembers,
}

/// Build the single-record batch that tombstones the classic k2 `GroupMetadata`
/// for `group_id`. Reuses the consumer-migration `PendingRecords` encoder so the
/// tombstone key bytes are identical to the upgrade flip's.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `group_id` is longer than 32767
/// bytes.
pub(crate) fn classic_group_metadata_tombstone_batch(
    group_id: &str,
    now_ms: i64,
) -> Result<RecordBatch, BrokerError> {
    PendingRecords {
        classic_group_metadata_tombstone: true,
        ..Default::default()
    }
    .to_batch(group_id, now_ms)
}

/// Build the batch that tombstones every streams record for `group_id`, for the
/// streams→classic downgrade and the type-aware streams delete, in the order
/// of Kafka's `StreamsGroup.createGroupTombstoneRecords`: each member's k22
/// current assignment, each member's k21 target assignment, the k20
/// `TargetAssignmentMetadata`, each member's k19 metadata, the k17
/// `GroupMetadata`, and the k23 `Topology`. The group-level keys are
/// tombstoned unconditionally; a tombstone for a never-written key is a
/// harmless replay no-op. The k17 tombstone is load-bearing, because a
/// surviving k17 would resurrect the group as streams. A drained group has no
/// members, because members tombstone their own per-member records on leave,
/// so `member_ids` is typically empty.
///
/// This function builds the batch directly from the key encoders rather than
/// through `PendingStreamsRecords`. That type's group-level fields are
/// `Option<Value>`, present or absent, with no way to express a group-level
/// null-value tombstone.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `group_id` or a member id is longer
/// than 32767 bytes.
pub(crate) fn streams_records_tombstone_batch(
    group_id: &str,
    member_ids: &[String],
    now_ms: i64,
) -> Result<RecordBatch, BrokerError> {
    let mut batch = OffsetRecordBatchBuilder::default();
    for key in streams_tombstone_keys(group_id, member_ids)? {
        batch.push(key, None);
    }
    Ok(batch.finish(now_ms))
}

/// Build the one batch of Kafka 4.3.1's `classicGroupJoinToClassicGroup` for
/// a join with no member id to the drained streams group `group_id`.
///
/// `maybeDeleteEmptyStreamsGroup` puts the tombstones of
/// `StreamsGroup.createGroupTombstoneRecords` first. A drained group has no
/// members, so they are the k20 `TargetAssignmentMetadata`, the k17
/// `GroupMetadata` and the k23 `Topology`. The join of a new classic group
/// returns no records of its own, because its first rebalance waits for the
/// initial delay, so Kafka adds `newEmptyGroupMetadataRecord` to the same list:
/// a k2 `GroupMetadata` with the empty protocol type, generation 0, no
/// protocol, no leader, no members, and the time of the group's last state
/// change, which the join has just made `now_ms`. The coordinator writes the
/// list atomically.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `group_id` is longer than 32767
/// bytes.
pub(crate) fn streams_to_classic_batch(
    group_id: &str,
    now_ms: i64,
) -> Result<RecordBatch, BrokerError> {
    use crate::coordinator::unified::persistence::GroupMetadataValue;

    let mut batch = OffsetRecordBatchBuilder::default();
    for key in streams_tombstone_keys(group_id, &[])? {
        batch.push(key, None);
    }
    let empty = GroupMetadataValue {
        protocol_type: String::new(),
        generation: 0,
        protocol_name: None,
        leader: None,
        current_state_timestamp_ms: now_ms,
        members: Vec::new(),
    };
    batch.push(
        GroupMetadataValue::encode_key(group_id)?,
        Some(empty.encode_value()?),
    );
    Ok(batch.finish(now_ms))
}

/// The keys of `StreamsGroup.createGroupTombstoneRecords`, in its order.
fn streams_tombstone_keys(
    group_id: &str,
    member_ids: &[String],
) -> Result<Vec<bytes::Bytes>, BrokerError> {
    let mut keys = Vec::with_capacity(3 + 3 * member_ids.len());
    for mid in member_ids {
        keys.push(encode_current_member_assignment_key(group_id, mid)?);
    }
    for mid in member_ids {
        keys.push(encode_target_assignment_member_key(group_id, mid)?);
    }
    keys.push(encode_target_assignment_metadata_key(group_id)?);
    for mid in member_ids {
        keys.push(encode_member_metadata_key(group_id, mid)?);
    }
    keys.push(encode_group_metadata_key(group_id)?);
    keys.push(encode_topology_key(group_id)?);
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_contents(batch: &RecordBatch) -> Vec<(Option<bytes::Bytes>, Option<bytes::Bytes>)> {
        batch
            .records
            .iter()
            .map(|record| (record.key.clone(), record.value.clone()))
            .collect()
    }

    #[test]
    fn tombstone_batch_has_one_null_value_k2_record() {
        let batch = classic_group_metadata_tombstone_batch("g", 123).unwrap();
        assert2::assert!((batch.records.len()) == (1), "exactly one record");
        let r = &batch.records[0];
        assert2::assert!(r.key.is_some(), "k2 GroupMetadata key present");
        assert2::assert!(r.value.is_none(), "tombstone = null value");
        let key = r.key.as_ref().unwrap();
        assert2::assert!(
            (&key[..2]) == (&2i16.to_be_bytes()),
            "classic GroupMetadata key version 2"
        );
    }

    /// The streams tombstones follow Kafka 4.3.1's
    /// `StreamsGroup.createGroupTombstoneRecords`: k22 and k21 for each
    /// member, k20, k19 for each member, then k17 and k23. Kafka defines no
    /// type 18, so none is written.
    #[test]
    fn streams_tombstone_batch_follows_kafka() {
        let g = "g";
        let rows = [
            ("no members", vec![], vec![
                encode_target_assignment_metadata_key(g).unwrap(),
                encode_group_metadata_key(g).unwrap(),
                encode_topology_key(g).unwrap(),
            ]),
            ("one member", vec!["m1".to_string()], vec![
                encode_current_member_assignment_key(g, "m1").unwrap(),
                encode_target_assignment_member_key(g, "m1").unwrap(),
                encode_target_assignment_metadata_key(g).unwrap(),
                encode_member_metadata_key(g, "m1").unwrap(),
                encode_group_metadata_key(g).unwrap(),
                encode_topology_key(g).unwrap(),
            ]),
        ];
        for (name, members, keys) in rows {
            let batch = streams_records_tombstone_batch(g, &members, 123).unwrap();
            let records = record_contents(&batch);
            let expected: Vec<_> = keys.into_iter().map(|key| (Some(key), None)).collect();
            assert2::check!(records == expected, "{name}");
            assert2::check!(batch.max_timestamp == 123, "{name}");
        }
    }

    /// Kafka 4.3.1's `classicGroupJoinToClassicGroup` for a drained streams
    /// group: the streams tombstones, then `newEmptyGroupMetadataRecord`, in
    /// one batch.
    #[test]
    fn streams_to_classic_batch_follows_kafka() {
        use crate::coordinator::unified::persistence::GroupMetadataValue;

        let batch = streams_to_classic_batch("g", 123).unwrap();

        let records = record_contents(&batch);
        let empty = GroupMetadataValue {
            protocol_type: String::new(),
            generation: 0,
            protocol_name: None,
            leader: None,
            current_state_timestamp_ms: 123,
            members: vec![],
        };
        let expected = vec![
            (
                Some(encode_target_assignment_metadata_key("g").unwrap()),
                None,
            ),
            (Some(encode_group_metadata_key("g").unwrap()), None),
            (Some(encode_topology_key("g").unwrap()), None),
            (
                Some(GroupMetadataValue::encode_key("g").unwrap()),
                Some(empty.encode_value().unwrap()),
            ),
        ];
        assert2::check!(records == expected);
        assert2::check!(batch.max_timestamp == 123);
    }
}
