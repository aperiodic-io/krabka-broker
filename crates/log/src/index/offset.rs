//! Sparse offset index. Each entry is 8 bytes: `relative_offset` as u32 BE
//! and position as u32 BE. Entries increase monotonically. As in Kafka's
//! `LogSegment.append`, the offset is the **last** offset of the batch that
//! starts at the position, not its base offset.

use super::prelude::*;

/// 8 bytes per entry.
pub const OFFSET_ENTRY_SIZE: usize = 8;

/// On-disk byte layout of one offset-index entry.
type OffsetEntryRaw = super::IndexEntryRaw<U32<BigEndian>>;

const _: [(); OFFSET_ENTRY_SIZE] = [(); std::mem::size_of::<OffsetEntryRaw>()];

#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    io: std::sync::Arc<dyn LogIo>,
    /// Entries currently in the file. The constructor loads them into memory
    /// lazily.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    index_methods!(IoTarget::OffsetIndex);

    // Byte positions strictly increase across real entries. A Kafka
    // index file is preallocated to `segment.index.bytes` and only
    // truncated on clean roll/shutdown, so an unclean copy carries
    // trailing zero-padding that decodes to `(0, 0)`. Stop at the
    // first non-increasing position to keep `lookup`'s binary search
    // operating over a monotonic slice.
    index_constructor!(OffsetEntryRaw,
        "length is a multiple of OFFSET_ENTRY_SIZE and OffsetEntryRaw is Unaligned",
        |raw| (raw.key.get(), raw.coordinate.get());
        /// Open or create an offset-index file. If the file exists, this method
        /// loads its entries into memory. If it does not exist, this method
        /// creates an empty file.
    );

    /// Append a new entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<(), LogError> {
        let raw = OffsetEntryRaw {
            key: U32::new(relative_offset),
            coordinate: zerocopy::byteorder::U32::new(position),
        };
        super::append_index(
            &mut self.file,
            &*self.io,
            IoTarget::OffsetIndex,
            raw.as_bytes(),
            &mut self.entries,
            (relative_offset, position),
        )
    }

    /// Find the byte position where a read for a given relative offset must
    /// start. This method returns the position of the largest entry with
    /// `relative_offset <= target`, or 0 when there are no entries.
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        krabka_verified::offset_index_lookup(&self.entries, target)
    }

    /// The entry [`OffsetIndex::lookup`] lands on: the largest entry with
    /// `relative_offset <= target`, or `None` when every entry is above it.
    #[must_use]
    pub fn floor_entry(&self, target: u32) -> Option<(u32, u32)> {
        let above = self
            .entries
            .partition_point(|&(relative_offset, _)| relative_offset <= target);
        above.checked_sub(1).map(|index| self.entries[index])
    }

    /// Truncate the entries, and the on-disk file, so that no entry with
    /// `position >= max_position_exclusive` remains.
    #[instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
    pub fn truncate_by_position(&mut self, max_position_exclusive: u32) -> Result<(), LogError> {
        super::truncate_index(
            &mut self.file,
            &mut self.entries,
            OFFSET_ENTRY_SIZE,
            max_position_exclusive,
        )
    }

    /// Byte position of the first entry whose `relative_offset >= target`, or
    /// `None` when every entry is below `target`. An entry holds the last
    /// offset of the batch that starts at its position, so every batch that
    /// ends below `target` lies strictly below this position, and the position
    /// bounds a scan from the start that must stop at `target`.
    #[must_use]
    pub fn position_at_or_after(&self, target: u32) -> Option<u32> {
        krabka_verified::offset_index_position_at_or_after(&self.entries, target)
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(u32, u32)> { self.entries.last().copied() }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        std::{fs::OpenOptions, io::Write},
    };

    seed_index_fixture!(OffsetIndex, u32);

    fn reference_index() -> (tempfile::TempDir, std::path::PathBuf, OffsetIndex) {
        populated_index("00000000000000000000.index", &[
            (0, 0),
            (100, 4096),
            (200, 8192),
        ])
    }

    fn assert_entry_metadata(index: &OffsetIndex, count: usize, last: Option<(u32, u32)>) {
        assert2::assert!(index.entry_count() == count);
        assert2::assert!(index.last_entry() == last);
    }

    /// Truncation drops every entry at or past the bound and shortens the file
    /// to match.
    ///
    /// The bound is exclusive, so an entry sitting exactly on it goes. And the
    /// file has to end up exactly `kept * ENTRY_SIZE` bytes long: a stale tail
    /// is read back as entries on the next open, which is how a torn index
    /// resurrects offsets the log no longer has.
    #[test]
    fn offset_index_truncation_drops_entries_at_the_bound_and_shortens_the_file() {
        let (_dir, path, mut idx) =
            super::super::index_fixture("00000000000000000000.index", OffsetIndex::open);
        for i in 0..5u32 {
            idx.append(i * 10, i * 100).unwrap();
        }
        check!(std::fs::metadata(&path).unwrap().len() == (5 * OFFSET_ENTRY_SIZE) as u64);

        // Positions are 0, 100, 200, 300, 400; the bound is exclusive, so the
        // entry at 200 goes with everything past it.
        idx.truncate_by_position(200).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * OFFSET_ENTRY_SIZE) as u64,
            "file should hold exactly the two surviving entries"
        );
        // Reopening reads the file back: the dropped entries must not return.
        let reopened = OffsetIndex::open(&path).unwrap();
        check!(reopened.lookup(9999) == 100, "the last surviving position");
    }

    #[test]
    fn append_and_lookup() {
        let (_dir, _path, idx) = reference_index();
        for (name, offset, want) in [
            ("floor first", 50, 0),
            ("exact middle", 100, 4096),
            ("floor middle", 150, 4096),
            ("exact last", 200, 8192),
            ("past last", 9999, 8192),
        ] {
            check!(idx.lookup(offset) == want, "case {name}: offset={offset}");
        }
    }

    #[test]
    fn empty_index_returns_zero() {
        let (_dir, _path, idx) =
            super::super::index_fixture("00000000000000000000.index", OffsetIndex::open);
        for (_name, offset) in [("zero", 0), ("positive", 1000)] {
            assert2::assert!(idx.lookup(offset) == 0);
        }
    }

    #[test]
    fn persists_across_reopen() {
        let (_dir, path) = written_index("00000000000000000000.index", &[(0, 0), (100, 4096)]);
        let idx = OffsetIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.lookup(100) == 4096);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        // Kafka preallocates `.index` to `segment.index.bytes` and only
        // truncates on clean shutdown; an unclean copy carries trailing
        // zero entries. Loading must stop at the real data so the binary
        // search stays monotonic.
        let (_dir, path) = written_index("00000000000000000000.index", &[(0, 0), (100, 4096)]);
        // Append two zero-filled entries (preallocation padding).
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; OFFSET_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = OffsetIndex::open(&path).unwrap();
        assert_entry_metadata(&idx, 2, Some((100, 4096)));
        assert2::assert!(idx.lookup(150) == 4096);
    }

    /// `floor_entry` is the entry `lookup` lands on, so a read can tell whether
    /// the indexed batch ends exactly on its target.
    #[test]
    fn floor_entry_is_the_entry_lookup_lands_on() {
        let (_dir, _path, idx) = populated_index("00000000000000000000.index", &[(4, 0), (9, 50)]);
        for (name, target, want) in [
            ("below the first entry", 3, None),
            ("exact first", 4, Some((4, 0))),
            ("between entries", 8, Some((4, 0))),
            ("exact last", 9, Some((9, 50))),
            ("past the last entry", 100, Some((9, 50))),
        ] {
            check!(idx.floor_entry(target) == want, "case {name}");
            check!(
                idx.lookup(target) == want.map_or(0, |(_, position)| position),
                "case {name}: lookup agrees"
            );
        }
    }

    #[test]
    fn position_at_or_after_finds_ceiling() {
        let (_dir, _path, idx) = reference_index();
        for (name, offset, want) in [
            ("exact", 100, Some(4096)),
            ("ceiling", 150, Some(8192)),
            ("first", 0, Some(0)),
            ("past last", 201, None),
        ] {
            check!(
                idx.position_at_or_after(offset) == want,
                "case {name}: offset={offset}"
            );
        }
    }

    #[test]
    fn truncate_by_position() {
        let (_dir, _path, mut idx) = reference_index();
        idx.truncate_by_position(8192).unwrap();
        assert_entry_metadata(&idx, 2, Some((100, 4096)));
    }
}
