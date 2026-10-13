//! Binary wire format for `__remote_log_metadata` events.
//!
//! This module encodes records through
//! [`krabka_protocol::RemoteLogMetadataRecord`], which produces bytes that are
//! byte-identical to the JVM `RemoteLogMetadataSerde`. That is the
//! `AbstractApiMessageSerde` envelope, with all three header fields written as
//! unsigned varints: `frameVersion(uvarint)=1 | apiKey(uvarint) |
//! apiVersion(uvarint) | flexible-message-body`.
//!
//! apiKey mapping:
//! - 0 = `RemoteLogSegmentMetadataRecord`  → [`MetadataEvent::AddSegment`]
//! - 1 = `RemoteLogSegmentMetadataUpdateRecord` → [`MetadataEvent::UpdateSegment`]
//! - 2 = `RemotePartitionDeleteMetadataRecord` → [`MetadataEvent::PartitionDelete`]
//!
//! One submodule per record, plus the scalar conversions they share:
//! `segment_metadata` for apiKey 0, `segment_update` for apiKey 1,
//! `partition_delete` for apiKey 2, and `primitives` for the UUID, state-byte
//! and custom-metadata conversions all three use. The unrelated varint and
//! cursor helpers that `snapshot.rs` frames its own envelope with live in
//! `reader`.

use {
    crate::error::CodecError,
    bytes::Bytes,
    krabka_protocol::RemoteLogMetadataRecord,
    krabka_remote_storage::{
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemotePartitionDeleteMetadata,
    },
};

mod partition_delete;
mod primitives;
mod reader;
mod segment_metadata;
mod segment_update;
#[cfg(test)]
mod tests;

pub(crate) use self::reader::{Reader, read_uvarint, write_uvarint};
use self::{
    partition_delete::{from_proto_partition_delete, to_proto_partition_delete},
    segment_metadata::{from_proto_add, to_proto_add},
    segment_update::{from_proto_update, to_proto_update},
};

/// One of the three event variants the topic carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataEvent {
    /// A new segment starts to copy (`CopySegmentStarted`).
    AddSegment(RemoteLogSegmentMetadata),
    /// Lifecycle transition for an existing segment.
    UpdateSegment(RemoteLogSegmentMetadataUpdate),
    /// Partition-delete lifecycle.
    PartitionDelete(RemotePartitionDeleteMetadata),
}

impl MetadataEvent {
    /// Encode this event into freshly-allocated [`Bytes`] with the JVM
    /// `RemoteLogMetadataSerde` wire format.
    ///
    /// # Panics
    ///
    /// Panics only if the generated codec rejects the version. That is
    /// impossible for version 0, which every record uses.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let record = match self {
            Self::AddSegment(md) => RemoteLogMetadataRecord::SegmentMetadata(to_proto_add(md)),
            Self::UpdateSegment(u) => {
                RemoteLogMetadataRecord::SegmentMetadataUpdate(to_proto_update(u))
            }
            Self::PartitionDelete(d) => {
                RemoteLogMetadataRecord::PartitionDelete(to_proto_partition_delete(d))
            }
        };
        record
            .encode_value()
            .expect("RemoteLogMetadataRecord::encode_value must not fail for version 0")
    }

    /// Decode an event from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] for any malformed input: a truncated envelope,
    /// an unknown or unsupported apiKey, an out-of-range state byte, or a
    /// domain invariant violation that [`RemoteLogSegmentMetadata::new`]
    /// reports. apiKey 3, `SegmentMetadataSnapshot`, never appears on the
    /// topic.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let record = RemoteLogMetadataRecord::decode_value(bytes)
            .map_err(|e| CodecError::Protocol(e.to_string()))?;
        match record {
            RemoteLogMetadataRecord::SegmentMetadata(r) => Ok(Self::AddSegment(from_proto_add(r)?)),
            RemoteLogMetadataRecord::SegmentMetadataUpdate(r) => {
                Ok(Self::UpdateSegment(from_proto_update(r)?))
            }
            RemoteLogMetadataRecord::PartitionDelete(r) => {
                Ok(Self::PartitionDelete(from_proto_partition_delete(r)?))
            }
            RemoteLogMetadataRecord::SegmentMetadataSnapshot(_) => Err(CodecError::Protocol(
                "apiKey 3 (SegmentMetadataSnapshot) must not appear on __remote_log_metadata"
                    .into(),
            )),
            RemoteLogMetadataRecord::Unknown { api_key, .. } => Err(CodecError::Protocol(format!(
                "unknown __remote_log_metadata apiKey {api_key}"
            ))),
        }
    }
}
