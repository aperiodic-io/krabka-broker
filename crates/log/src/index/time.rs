//! Sparse time index. Each entry is 12 bytes: a timestamp as i64 BE and a
//! `relative_offset` as u32 BE. Both columns increase strictly, as Kafka's
//! `TimeIndex.maybeAppend` keeps them, and the offset is the last offset of the
//! batch that set the timestamp.

use super::prelude::*;

/// 12 bytes per entry: timestamp as i64 BE and `relative_offset` as u32 BE.
pub const TIME_ENTRY_SIZE: usize = 12;

/// Kafka's `RecordBatch.NO_TIMESTAMP`: the timestamp a batch without one
/// reports.
const NO_TIMESTAMP: i64 = -1;

/// On-disk byte layout of one time-index entry.
type TimeEntryRaw = super::IndexEntryRaw<I64<BigEndian>>;

const _: [(); TIME_ENTRY_SIZE] = [(); std::mem::size_of::<TimeEntryRaw>()];

/// One entry of a [`TimeIndex`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeIndexEntry {
    /// The running maximum timestamp as of the batch the entry indexes.
    pub timestamp: i64,
    /// The last offset of that batch, relative to the segment's base offset.
    pub relative_offset: u32,
}

// Relative offsets strictly increase across real entries; trailing
// `(0, 0)` padding from a preallocated Kafka index decodes as a
// non-increasing offset. Stop there. Padding carries timestamp 0,
// which is a legal timestamp, so the offset column is the
// discriminator.
#[derive(Debug, krabka_macros::SparseIndex)]
#[sparse_index(kind = "time")]
pub struct TimeIndex {
    file: File,
    io: std::sync::Arc<dyn LogIo>,
    entries: Vec<IndexEntry<i64, u32>>,
}

impl TimeIndex {
    /// Kafka's `TimeIndex.maybeAppend`: append the entry only when `timestamp`
    /// is greater than the newest entry's, so the timestamps in the file
    /// strictly increase. `kafka-dump-log` reports any other order as out of
    /// order. An empty index compares against `NO_TIMESTAMP` (`-1`), so a run
    /// of batches without timestamps leaves it empty.
    pub fn maybe_append(&mut self, timestamp: i64, relative_offset: u32) -> Result<(), LogError> {
        let newest = self.entries.last().map_or(NO_TIMESTAMP, |entry| entry.key);
        if timestamp > newest {
            self.append(timestamp, relative_offset)?;
        }
        Ok(())
    }

    /// Append an entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, timestamp: i64, relative_offset: u32) -> Result<(), LogError> {
        let raw = TimeEntryRaw {
            key: I64::new(timestamp),
            coordinate: zerocopy::byteorder::U32::new(relative_offset),
        };
        super::append_index(
            &mut self.file,
            &*self.io,
            IoTarget::TimeIndex,
            raw.as_bytes(),
            &mut self.entries,
            timestamp,
            relative_offset,
        )
    }

    /// Start a forward scan before the first record at or above the target.
    /// Equal running maxima cannot advance the start without skipping ties.
    #[must_use]
    pub(crate) fn scan_start(&self, target_timestamp: i64) -> u32 {
        krabka_verified::log_index::time_index_scan_start(&self.entries, target_timestamp)
    }

    #[tracing::instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
    pub fn truncate_by_relative_offset(&mut self, max_rel_exclusive: u32) -> Result<(), LogError> {
        super::truncate_index(
            &mut self.file,
            &mut self.entries,
            TIME_ENTRY_SIZE,
            max_rel_exclusive,
        )
    }

    /// Newest entry, or `None` when the index holds none.
    ///
    /// The entry's timestamp is the running maximum as of the batch it
    /// indexes, so it is the floor a reopened segment restores its
    /// `max_timestamp` from.
    #[must_use]
    pub fn last_entry(&self) -> Option<TimeIndexEntry> {
        self.entries.last().map(|entry| TimeIndexEntry {
            timestamp: entry.key,
            relative_offset: entry.coordinate,
        })
    }
}

#[cfg(test)]
mod time_tests {
    use {
        super::*,
        crate::{
            index::{IndexFixture, IndexSeed},
            test_support::TempPath,
        },
        assert2::check,
    };

    krabka_macros::index_seed_helpers!(TimeIndex, i64);

    /// The time index truncates on relative offset, with the same exclusive
    /// bound and the same file-length obligation.
    #[test]
    fn time_index_truncation_drops_entries_at_the_bound_and_shortens_the_file() {
        let IndexFixture {
            dir: _dir,
            path,
            index: mut idx,
        } = super::super::index_fixture("00000000000000000000.timeindex", TimeIndex::open);
        for i in 0..5u32 {
            idx.append(1_000 + i64::from(i), i * 10).unwrap();
        }
        check!(std::fs::metadata(&path).unwrap().len() == (5 * TIME_ENTRY_SIZE) as u64);

        // Relative offsets are 0, 10, 20, 30, 40. Exclusive at 20: two survive.
        idx.truncate_by_relative_offset(20).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * TIME_ENTRY_SIZE) as u64,
            "file should hold exactly the two surviving entries"
        );

        // Truncating to zero clears it; truncating past the end keeps everything.
        let mut idx = TimeIndex::open(&path).unwrap();
        idx.truncate_by_relative_offset(9_999).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == (2 * TIME_ENTRY_SIZE) as u64,
            "a bound past the end drops nothing"
        );
        idx.truncate_by_relative_offset(0).unwrap();
        check!(
            std::fs::metadata(&path).unwrap().len() == 0,
            "a bound of zero drops everything"
        );
    }

    /// Kafka's `TimeIndex.maybeAppend` takes an entry only when its timestamp
    /// is above the newest entry's, and an empty index compares against
    /// `NO_TIMESTAMP`. `kafka-dump-log` reports any other order as out of
    /// order.
    #[test]
    fn maybe_append_takes_only_strictly_newer_timestamps() {
        struct AppendCase {
            label: &'static str,
            existing: Vec<IndexSeed<i64>>,
            appended: IndexEntry<i64, u32>,
            expected: Vec<IndexEntry<i64, u32>>,
        }
        let seed = |key, coordinate| IndexSeed { key, coordinate };
        for AppendCase {
            label,
            existing,
            appended,
            expected: want,
        } in [
            AppendCase {
                label: "no timestamp on an empty index",
                existing: vec![],
                appended: IndexEntry::new(-1, 0),
                expected: vec![],
            },
            AppendCase {
                label: "first timestamp",
                existing: vec![],
                appended: IndexEntry::new(0, 0),
                expected: vec![IndexEntry::new(0, 0)],
            },
            AppendCase {
                label: "same timestamp, later offset",
                existing: vec![seed(100, 5)],
                appended: IndexEntry::new(100, 9),
                expected: vec![IndexEntry::new(100, 5)],
            },
            AppendCase {
                label: "older timestamp",
                existing: vec![seed(100, 5)],
                appended: IndexEntry::new(99, 9),
                expected: vec![IndexEntry::new(100, 5)],
            },
            AppendCase {
                label: "newer timestamp",
                existing: vec![seed(100, 5)],
                appended: IndexEntry::new(101, 9),
                expected: vec![IndexEntry::new(100, 5), IndexEntry::new(101, 9)],
            },
        ] {
            let IndexFixture {
                dir: _dir,
                path,
                index: mut idx,
            } = populated_index("0.timeindex", &existing);
            idx.maybe_append(appended.key, appended.coordinate).unwrap();
            check!(idx.entries == want, "case {label}");
            let reopened = TimeIndex::open(&path).unwrap();
            check!(reopened.entries == want, "case {label}: on disk");
        }
    }

    #[test]
    fn append_and_choose_scan_start() {
        let IndexFixture {
            dir: _dir,
            path: _path,
            index: idx,
        } = populated_index("00000000000000000000.timeindex", &[
            IndexSeed {
                key: 1_000_000,
                coordinate: 0,
            },
            IndexSeed {
                key: 2_000_000,
                coordinate: 100,
            },
            IndexSeed {
                key: 3_000_000,
                coordinate: 200,
            },
        ]);
        for (name, ts, want) in [
            ("before first", 0, 0),
            ("floor first", 1_500_000, 0),
            ("exact middle", 2_000_000, 0),
            ("floor middle", 2_500_000, 100),
            ("past last", 5_000_000, 200),
        ] {
            check!(idx.scan_start(ts) == want, "case {name}: ts={ts}");
        }
    }

    #[test]
    fn persists_across_reopen() {
        let TempPath { dir: _dir, path } = written_index("00000000000000000000.timeindex", &[
            IndexSeed {
                key: 1,
                coordinate: 0,
            },
            IndexSeed {
                key: 2,
                coordinate: 50,
            },
        ]);
        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        let TempPath { dir: _dir, path } = written_index("00000000000000000000.timeindex", &[
            IndexSeed {
                key: 1_000,
                coordinate: 0,
            },
            IndexSeed {
                key: 2_000,
                coordinate: 100,
            },
        ]);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        std::io::Write::write_all(&mut f, &[0u8; TIME_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(
            idx.last_entry()
                == Some(TimeIndexEntry {
                    timestamp: 2_000,
                    relative_offset: 100
                })
        );
        assert2::assert!(idx.scan_start(2_500) == 100);
    }
}
