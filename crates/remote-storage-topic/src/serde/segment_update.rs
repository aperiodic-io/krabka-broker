//! apiKey 1, `RemoteLogSegmentMetadataUpdateRecord`: the domain-to-protocol
//! and protocol-to-domain conversions for a segment lifecycle transition.
//!
//! The update record repeats the segment id and then carries only what a
//! transition changes: the broker that made it, its event timestamp, the
//! optional custom metadata, and the new state byte. Its
//! `TopicIdPartitionEntry` and `RemoteLogSegmentIdEntry` are distinct
//! generated types from the ones apiKey 0 uses, which is why the conversions
//! are not shared with `segment_metadata`.

use {
    super::primitives::{
        bytes_to_custom_metadata, custom_metadata_to_bytes, domain_uuid_to_proto,
        i8_to_segment_state, proto_uuid_to_domain, segment_state_to_i8,
    },
    crate::error::CodecError,
    krabka_protocol::owned::remote_log_segment_metadata_update_record::{
        RemoteLogSegmentIdEntry as SegIdEntryUpd, RemoteLogSegmentMetadataUpdateRecord,
        TopicIdPartitionEntry as TpEntryUpd,
    },
    krabka_remote_storage::{RemoteLogSegmentId, RemoteLogSegmentMetadataUpdate, TopicIdPartition},
};

fn tp_to_proto_upd(tp: &TopicIdPartition) -> TpEntryUpd {
    TpEntryUpd {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_upd_to_domain(tp: TpEntryUpd) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

fn seg_id_to_proto_upd(id: &RemoteLogSegmentId) -> SegIdEntryUpd {
    SegIdEntryUpd {
        topic_id_partition: tp_to_proto_upd(&id.topic_id_partition),
        id: domain_uuid_to_proto(id.id),
        ..Default::default()
    }
}

fn proto_seg_id_upd_to_domain(id: SegIdEntryUpd) -> RemoteLogSegmentId {
    RemoteLogSegmentId::new(
        proto_tp_upd_to_domain(id.topic_id_partition),
        proto_uuid_to_domain(id.id),
    )
}

pub(super) fn to_proto_update(
    u: &RemoteLogSegmentMetadataUpdate,
) -> RemoteLogSegmentMetadataUpdateRecord {
    RemoteLogSegmentMetadataUpdateRecord {
        remote_log_segment_id: seg_id_to_proto_upd(&u.remote_log_segment_id),
        broker_id: u.broker_id,
        event_timestamp_ms: u.event_timestamp_ms,
        custom_metadata: custom_metadata_to_bytes(u.custom_metadata.as_ref()),
        remote_log_segment_state: segment_state_to_i8(u.state),
        ..Default::default()
    }
}

pub(super) fn from_proto_update(
    r: RemoteLogSegmentMetadataUpdateRecord,
) -> Result<RemoteLogSegmentMetadataUpdate, CodecError> {
    let remote_log_segment_id = proto_seg_id_upd_to_domain(r.remote_log_segment_id);
    let state = i8_to_segment_state(r.remote_log_segment_state)?;
    Ok(RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id,
        event_timestamp_ms: r.event_timestamp_ms,
        custom_metadata: bytes_to_custom_metadata(r.custom_metadata),
        state,
        broker_id: r.broker_id,
    })
}
