//! The scalar conversions the three `__remote_log_metadata` record codecs
//! share: UUIDs, the [`RemoteLogSegmentState`] byte, and custom metadata.
//!
//! Each of these crosses the boundary between the domain types and the
//! generated protocol structs, and the segment-state byte in particular is
//! part of the wire format: the numbering below is the JVM
//! `RemoteLogSegmentState` ordinal.

use {
    crate::error::CodecError,
    bytes::Bytes,
    krabka_protocol::primitives::uuid::Uuid as ProtoUuid,
    krabka_remote_storage::{CustomMetadata, RemoteLogSegmentState},
};

pub(super) fn domain_uuid_to_proto(u: uuid::Uuid) -> ProtoUuid { ProtoUuid(*u.as_bytes()) }

pub(super) fn proto_uuid_to_domain(u: ProtoUuid) -> uuid::Uuid { uuid::Uuid::from_bytes(u.0) }

pub(super) fn segment_state_to_i8(s: RemoteLogSegmentState) -> i8 {
    match s {
        RemoteLogSegmentState::CopySegmentStarted => 0,
        RemoteLogSegmentState::CopySegmentFinished => 1,
        RemoteLogSegmentState::DeleteSegmentStarted => 2,
        RemoteLogSegmentState::DeleteSegmentFinished => 3,
    }
}

pub(super) fn i8_to_segment_state(v: i8) -> Result<RemoteLogSegmentState, CodecError> {
    match v {
        0 => Ok(RemoteLogSegmentState::CopySegmentStarted),
        1 => Ok(RemoteLogSegmentState::CopySegmentFinished),
        2 => Ok(RemoteLogSegmentState::DeleteSegmentStarted),
        3 => Ok(RemoteLogSegmentState::DeleteSegmentFinished),
        other => Err(CodecError::UnknownState(
            other.cast_unsigned(),
            "RemoteLogSegmentState",
        )),
    }
}

pub(super) fn custom_metadata_to_bytes(cm: Option<&CustomMetadata>) -> Option<Bytes> {
    cm.map(|c| Bytes::from(c.0.clone()))
}

pub(super) fn bytes_to_custom_metadata(b: Option<Bytes>) -> Option<CustomMetadata> {
    b.map(|b| CustomMetadata(b.to_vec()))
}
