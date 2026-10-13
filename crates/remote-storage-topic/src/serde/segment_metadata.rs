//! apiKey 0, `RemoteLogSegmentMetadataRecord`: the domain-to-protocol and
//! protocol-to-domain conversions for a `CopySegmentStarted` event.
//!
//! This is the widest of the three records. It carries the segment id, the
//! offset and timestamp bounds, the leader-epoch map, the segment size, the
//! optional custom metadata, the state byte, and the transaction-index flag,
//! and the field order below is the order the JVM `RemoteLogMetadataSerde`
//! writes them in.

use {
    super::primitives::{
        bytes_to_custom_metadata, custom_metadata_to_bytes, domain_uuid_to_proto,
        i8_to_segment_state, proto_uuid_to_domain, segment_state_to_i8,
    },
    crate::error::CodecError,
    krabka_ids::LeaderEpoch,
    krabka_protocol::owned::remote_log_segment_metadata_record::{
        RemoteLogSegmentIdEntry as SegIdEntry, RemoteLogSegmentMetadataRecord,
        SegmentLeaderEpochEntry, TopicIdPartitionEntry as TpEntry,
    },
    krabka_remote_storage::{RemoteLogSegmentId, RemoteLogSegmentMetadata, TopicIdPartition},
    std::collections::BTreeMap,
};

fn tp_to_proto_add(tp: &TopicIdPartition) -> TpEntry {
    TpEntry {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_add_to_domain(tp: TpEntry) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

fn seg_id_to_proto_add(id: &RemoteLogSegmentId) -> SegIdEntry {
    SegIdEntry {
        topic_id_partition: tp_to_proto_add(&id.topic_id_partition),
        id: domain_uuid_to_proto(id.id),
        ..Default::default()
    }
}

fn proto_seg_id_add_to_domain(id: SegIdEntry) -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(
        proto_tp_add_to_domain(id.topic_id_partition),
        proto_uuid_to_domain(id.id),
    )
}

fn epochs_to_proto(epochs: &BTreeMap<LeaderEpoch, i64>) -> Vec<SegmentLeaderEpochEntry> {
    epochs
        .iter()
        .map(|(&epoch, &offset)| SegmentLeaderEpochEntry {
            // Wire boundary: unwrap `LeaderEpoch` to the raw `int32` the
            // JVM `RemoteLogMetadataSerde` writes.
            leader_epoch: epoch.0,
            offset,
            ..Default::default()
        })
        .collect()
}

fn proto_epochs_to_domain(entries: Vec<SegmentLeaderEpochEntry>) -> BTreeMap<LeaderEpoch, i64> {
    entries
        .into_iter()
        // Wire boundary: wrap the raw `int32` back into `LeaderEpoch`.
        .map(|e| (LeaderEpoch(e.leader_epoch), e.offset))
        .collect()
}

pub(super) fn to_proto_add(md: &RemoteLogSegmentMetadata) -> RemoteLogSegmentMetadataRecord {
    RemoteLogSegmentMetadataRecord {
        remote_log_segment_id: seg_id_to_proto_add(md.remote_log_segment_id()),
        start_offset: md.start_offset(),
        end_offset: md.end_offset(),
        broker_id: md.broker_id(),
        max_timestamp_ms: md.max_timestamp_ms(),
        event_timestamp_ms: md.event_timestamp_ms(),
        segment_leader_epochs: epochs_to_proto(md.segment_leader_epochs()),
        segment_size_in_bytes: md.segment_size_in_bytes(),
        custom_metadata: custom_metadata_to_bytes(md.custom_metadata()),
        remote_log_segment_state: segment_state_to_i8(md.state()),
        txn_index_empty: md.txn_index_empty(),
        ..Default::default()
    }
}

pub(super) fn from_proto_add(
    r: RemoteLogSegmentMetadataRecord,
) -> Result<RemoteLogSegmentMetadata, CodecError> {
    let id = proto_seg_id_add_to_domain(r.remote_log_segment_id);
    let state = i8_to_segment_state(r.remote_log_segment_state)?;
    let segment_leader_epochs = proto_epochs_to_domain(r.segment_leader_epochs);
    let custom = bytes_to_custom_metadata(r.custom_metadata);
    let mut md = RemoteLogSegmentMetadata::new(
        id,
        r.start_offset,
        r.end_offset,
        r.max_timestamp_ms,
        r.broker_id,
        r.event_timestamp_ms,
        krabka_remote_storage::RemoteLogSegmentDetails::new(
            r.segment_size_in_bytes,
            state,
            segment_leader_epochs,
        ),
    )
    .map_err(|e| CodecError::Domain(e.to_string()))?;
    if let Some(c) = custom {
        md = md.with_custom_metadata(c);
    }
    md = md.with_txn_index_empty(r.txn_index_empty);
    Ok(md)
}
