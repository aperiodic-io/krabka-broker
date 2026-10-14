//! The Kafka `OffsetIndex` on-disk layout, its floor lookup, and the byte
//! range a remote fetch derives from it.
//!
//! An entry maps a relative offset to a byte position in the segment's `.log`
//! file, so the read path turns a requested offset into a position and then
//! into an inclusive object-store byte range.

use {
    super::{BytePosition, RelativeOffset, corrupt_index},
    crate::error::RemoteStorageError,
    zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned, byteorder::U32},
};

/// 8 bytes per entry: rel u32 BE, then pos u32 BE. It mirrors
/// `krabka_log::index::OffsetEntryRaw`, so the remote-tier copy of an
/// `OffsetIndex` file decodes through the same byte layout that wrote the
/// local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct OffsetIndexEntry {
    /// Offset of the indexed record, relative to the segment's base offset.
    pub relative_offset: U32<BigEndian>,
    /// Byte position of that record's batch within the `.log` file.
    pub position: U32<BigEndian>,
}

/// Byte length of one serialized offset-index entry.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

const _: [(); 8] = [(); OFFSET_INDEX_ENTRY_LEN];

/// Computes the inclusive `end_position` for a remote byte-range fetch.
///
/// It returns `None`, which means read to the end of the segment, when
/// `start_position` plus `max_bytes` would reach or pass `segment_size`. In
/// every other case it returns the inclusive last byte to read.
#[must_use]
pub fn end_position_for(
    start_position: BytePosition,
    segment_size: u32,
    max_bytes: usize,
) -> Option<BytePosition> {
    let max_bytes_u32 = u32::try_from(max_bytes).unwrap_or(u32::MAX);
    krabka_verified::remote_read::remote_fetch_end_position(
        start_position,
        segment_size,
        max_bytes_u32,
    )
}

/// Borrows Kafka's `OffsetIndex` on-disk format as a zero-copy
/// `&[OffsetIndexEntry]`, at 8 bytes per entry: rel u32 BE, then pos u32 BE.
/// It ignores trailing bytes that do not complete an 8-byte entry. The result
/// borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_offset_index(bytes: &[u8]) -> Result<&[OffsetIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / OFFSET_INDEX_ENTRY_LEN) * OFFSET_INDEX_ENTRY_LEN;
    <[OffsetIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("offset"))
}

/// Floor lookup: the byte position of the largest entry with
/// `rel <= target_rel`. It returns 0 when the index is empty, and when the
/// target is before the first entry. It runs directly against the borrowed
/// zero-copy slice and builds no owned `Vec`.
#[must_use]
pub fn position_for_relative_offset(
    entries: &[OffsetIndexEntry],
    target_rel: RelativeOffset,
) -> BytePosition {
    match entries.binary_search_by_key(&target_rel, |e| e.relative_offset.get()) {
        Ok(i) => entries[i].position.get(),
        Err(0) => 0,
        Err(i) => entries[i - 1].position.get(),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// An offset index entry's decoded relative offset and byte position.
    #[derive(Debug, PartialEq, Eq)]
    struct OffsetSpec {
        relative_offset: u32,
        position: u32,
    }

    const fn offset_at(relative_offset: u32, position: u32) -> OffsetSpec {
        OffsetSpec {
            relative_offset,
            position,
        }
    }

    fn offset_entries(specs: &[OffsetSpec]) -> Vec<OffsetIndexEntry> {
        specs
            .iter()
            .map(|spec| OffsetIndexEntry {
                relative_offset: U32::new(spec.relative_offset),
                position: U32::new(spec.position),
            })
            .collect()
    }

    #[yare::parameterized(
        one_entry = { vec![(0_u32, 0_u32)] },
        two_entries = { vec![(0_u32, 0_u32), (10, 256)] },
        all_known_entries = { vec![(0_u32, 0_u32), (10, 256), (20, 512)] },
    )]
    fn parse_offset_index_round_trips_known_entries(specs: Vec<(u32, u32)>) {
        // Mirror OffsetIndex::append: 4B rel BE, 4B pos BE.
        let mut buf = Vec::new();
        for (rel, pos) in &specs {
            buf.extend_from_slice(&rel.to_be_bytes());
            buf.extend_from_slice(&pos.to_be_bytes());
        }
        let entries = parse_offset_index(&buf).expect("valid offset index");
        let decoded: Vec<OffsetSpec> = entries
            .iter()
            .map(|e| offset_at(e.relative_offset.get(), e.position.get()))
            .collect();
        assert2::assert!(
            decoded
                == specs
                    .iter()
                    .map(|(rel, pos)| offset_at(*rel, *pos))
                    .collect::<Vec<_>>()
        );
    }

    #[yare::parameterized(
        exact = { 10, 256 },
        between = { 15, 256 },
        first_entry = { 0, 0 },
        after_last = { 100, 1024 },
    )]
    fn position_for_relative_offset_returns_floor(rel: u32, want: u32) {
        let entries = offset_entries(&[
            offset_at(0, 0),
            offset_at(10, 256),
            offset_at(20, 512),
            offset_at(30, 1024),
        ]);
        assert2::assert!(position_for_relative_offset(&entries, rel) == want);
    }

    #[test]
    fn position_for_relative_offset_empty_index_returns_zero() {
        assert!(position_for_relative_offset(&[], 50) == 0);
    }

    #[test]
    fn position_for_relative_offset_below_first() {
        // Synthetic: first entry isn't at rel=0. Floor below it returns 0.
        let entries = offset_entries(&[offset_at(5, 100), offset_at(10, 200)]);
        assert!(position_for_relative_offset(&entries, 3) == 0);
    }

    #[yare::parameterized(
        finite_inclusive_end = { 0, 1024, 256, Some(255) },
        cap_beyond_segment = { 512, 1024, 999_999, None },
        zero_cap_means_no_cap = { 0, 1024, 0, None },
        start_past_segment = { u32::MAX, 1024, 100, None },
        wide_cap_saturates = { 0, u32::MAX, usize::MAX, None },
    )]
    fn end_position_for_caps_with_max_bytes(
        start: u32,
        segment: u32,
        max_bytes: usize,
        want: Option<u32>,
    ) {
        assert2::assert!(end_position_for(start, segment, max_bytes) == want);
    }
}
