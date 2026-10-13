//! The framing and checksum check for a segment's producer-state `.snapshot`.
//! Decoding is `krabka_log`'s own [`krabka_log::decode_producer_snapshot`], so
//! the archive side accepts exactly what the broker's reload accepts; this
//! file only maps each rejection onto the restore error it reports.

use {
    super::offset_as_u64,
    crate::error::RestoreError,
    krabka_log::{Offset, SnapshotDecodeError, decode_producer_snapshot},
    object_store::path::Path,
};

/// Check a Kafka producer-state `.snapshot` at its exclusive log frontier
/// `snapshot_offset` with the log's decoder: header, version, length, CRC32C,
/// that every producer state is legal strictly before `snapshot_offset`, and
/// that producer IDs are unique.
///
/// A CRC failure is a [`RestoreError::ChecksumMismatch`]; every other
/// rejection is a [`RestoreError::TruncatedSegment`] whose `position` is where
/// the offending bytes start and whose `declared` and `available` are what the
/// check wanted and what it found.
pub(super) fn validate_producer_snapshot(
    key: &Path,
    bytes: &[u8],
    snapshot_offset: i64,
) -> Result<(), RestoreError> {
    decode_producer_snapshot(bytes, Offset(snapshot_offset))
        .map(drop)
        .map_err(|error| snapshot_error(key, bytes.len(), error, snapshot_offset))
}

fn snapshot_error(
    key: &Path,
    bytes_len: usize,
    error: SnapshotDecodeError,
    snapshot_offset: i64,
) -> RestoreError {
    let key = key.to_string();
    let (position, declared, available) = match error {
        SnapshotDecodeError::ChecksumMismatch { stored, computed } => {
            return RestoreError::ChecksumMismatch {
                key,
                position: 0,
                expected: stored,
                computed,
            };
        }
        SnapshotDecodeError::ShortHeader {
            required,
            available,
        } => (0, usize_as_u64(required), usize_as_u64(available)),
        // A version mismatch is a framing problem, like a length mismatch:
        // `declared` is the version this restore understands, `available` is
        // the version the snapshot declares.
        SnapshotDecodeError::UnsupportedVersion { version } => {
            (0, 1, offset_as_u64(i64::from(version)))
        }
        SnapshotDecodeError::NegativeEntryCount { .. }
        | SnapshotDecodeError::EntryCountOverflow { .. } => (0, u64::MAX, usize_as_u64(bytes_len)),
        SnapshotDecodeError::LengthMismatch {
            expected,
            available,
        } => (0, usize_as_u64(expected), usize_as_u64(available)),
        SnapshotDecodeError::InvalidEntry {
            position,
            last_offset,
            current_txn_first_offset,
            ..
        } => (
            usize_as_u64(position),
            offset_as_u64(snapshot_offset),
            offset_as_u64(last_offset.max(current_txn_first_offset)),
        ),
        SnapshotDecodeError::DuplicateProducerId { position, .. } => {
            (usize_as_u64(position), offset_as_u64(snapshot_offset), 0)
        }
    };
    RestoreError::TruncatedSegment {
        key,
        position,
        declared,
        available,
    }
}

fn usize_as_u64(value: usize) -> u64 { u64::try_from(value).unwrap_or(u64::MAX) }
