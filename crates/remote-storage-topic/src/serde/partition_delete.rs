//! apiKey 2, `RemotePartitionDeleteMetadataRecord`: the domain-to-protocol and
//! protocol-to-domain conversions for the partition-delete lifecycle.
//!
//! The record names a partition rather than a segment, and its state byte is
//! [`RemotePartitionDeleteState`], whose numbering below is the JVM
//! `RemotePartitionDeleteState` ordinal and is therefore part of the wire
//! format.

use {
    super::primitives::{domain_uuid_to_proto, proto_uuid_to_domain},
    crate::error::CodecError,
    krabka_protocol::owned::remote_partition_delete_metadata_record::{
        RemotePartitionDeleteMetadataRecord, TopicIdPartitionEntry as TpEntryDel,
    },
    krabka_remote_storage::{
        RemotePartitionDeleteMetadata, RemotePartitionDeleteState, TopicIdPartition,
    },
};

fn partition_state_to_i8(s: RemotePartitionDeleteState) -> i8 {
    match s {
        RemotePartitionDeleteState::DeletePartitionMarked => 0,
        RemotePartitionDeleteState::DeletePartitionStarted => 1,
        RemotePartitionDeleteState::DeletePartitionFinished => 2,
    }
}

fn i8_to_partition_state(v: i8) -> Result<RemotePartitionDeleteState, CodecError> {
    match v {
        0 => Ok(RemotePartitionDeleteState::DeletePartitionMarked),
        1 => Ok(RemotePartitionDeleteState::DeletePartitionStarted),
        2 => Ok(RemotePartitionDeleteState::DeletePartitionFinished),
        other => Err(CodecError::UnknownState(
            other.cast_unsigned(),
            "RemotePartitionDeleteState",
        )),
    }
}

fn tp_to_proto_del(tp: &TopicIdPartition) -> TpEntryDel {
    TpEntryDel {
        name: tp.topic.clone(),
        id: domain_uuid_to_proto(tp.topic_id),
        partition: tp.partition,
        ..Default::default()
    }
}

fn proto_tp_del_to_domain(tp: TpEntryDel) -> TopicIdPartition {
    TopicIdPartition::new(proto_uuid_to_domain(tp.id), tp.name, tp.partition)
}

pub(super) fn to_proto_partition_delete(
    d: &RemotePartitionDeleteMetadata,
) -> RemotePartitionDeleteMetadataRecord {
    RemotePartitionDeleteMetadataRecord {
        topic_id_partition: tp_to_proto_del(&d.topic_id_partition),
        broker_id: d.broker_id,
        event_timestamp_ms: d.event_timestamp_ms,
        remote_partition_delete_state: partition_state_to_i8(d.state),
        ..Default::default()
    }
}

pub(super) fn from_proto_partition_delete(
    r: RemotePartitionDeleteMetadataRecord,
) -> Result<RemotePartitionDeleteMetadata, CodecError> {
    let topic_id_partition = proto_tp_del_to_domain(r.topic_id_partition);
    let state = i8_to_partition_state(r.remote_partition_delete_state)?;
    Ok(RemotePartitionDeleteMetadata {
        topic_id_partition,
        state,
        event_timestamp_ms: r.event_timestamp_ms,
        broker_id: r.broker_id,
    })
}
