//! Per-segment `.txnindex` file. One fixed-width, version-prefixed record per
//! aborted transaction in the segment:
//!
//!   `version`:             i16 (big-endian), always 0
//!   `producer_id`:         i64 (big-endian)
//!   `first_offset`:        i64 (big-endian)
//!   `last_offset`:         i64 (big-endian)
//!   `last_stable_offset`:  i64 (big-endian)
//!
//! The byte layout matches Apache Kafka's `TransactionIndex` (a version
//! prefix on every record, derived from `AbortedTxn.json`'s field order), so
//! `kafka-dump-log --offsets-decoder` can dump it.

use std::{fs::OpenOptions, io::Write, path::PathBuf};

use krabka_ids::{Offset, ProducerId};
use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::{I16, I64},
};

use crate::{error::LogError, index::open_index};

const ENTRY_BYTES: usize = 34;

/// The only aborted-transaction record version krabka writes or accepts.
const SUPPORTED_VERSION: i16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub start_offset: Offset,
    pub last_offset: Offset,
    pub producer_id: ProducerId,
    /// The last stable offset at the moment this transaction aborted, that
    /// is, Kafka's `ProducerStateManager.lastStableOffset(completedTxn)`.
    pub last_stable_offset: Offset,
}

/// On-disk byte layout of one `AbortedTxn` entry. `zerocopy` reinterprets it
/// in place from the file bytes.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AbortedTxnRaw {
    version: I16<BigEndian>,
    producer_id: I64<BigEndian>,
    first_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    last_stable_offset: I64<BigEndian>,
}

const _: [(); ENTRY_BYTES] = [(); std::mem::size_of::<AbortedTxnRaw>()];

impl AbortedTxnRaw {
    fn new(entry: AbortedTxn) -> Self {
        Self {
            version: I16::new(SUPPORTED_VERSION),
            producer_id: I64::new(entry.producer_id.0),
            first_offset: I64::new(entry.start_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            last_stable_offset: I64::new(entry.last_stable_offset.0),
        }
    }
}

#[derive(Debug)]
pub struct TxnIndex {
    path: PathBuf,
    entries: Vec<AbortedTxn>,
}

impl TxnIndex {
    open_index! {
        /// Open or recover a `.txnindex` file at the given path. This method
        /// reads the entire file into memory at startup. An empty file or a
        /// missing file is acceptable and means zero aborted transactions.
        /// # Errors
        /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
        /// # Panics
        /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
        pub fn open(path: PathBuf) -> Result<Self, LogError> {
            let entries = crate::index::read_sidecar::<AbortedTxnRaw, _>(
                &path,
                "txnindex",
                "length is a multiple of ENTRY_BYTES and AbortedTxnRaw is Unaligned",
                |raws| {
                    let mut entries = Vec::with_capacity(raws.len());
                    for raw in raws {
                        if raw.version.get() != SUPPORTED_VERSION {
                            return Err(LogError::Corrupt(format!(
                                "txnindex {} contains an unsupported record version {}",
                                path.display(),
                                raw.version.get(),
                            )));
                        }
                        let entry = AbortedTxn {
                            start_offset: Offset(raw.first_offset.get()),
                            last_offset: Offset(raw.last_offset.get()),
                            producer_id: ProducerId(raw.producer_id.get()),
                            last_stable_offset: Offset(raw.last_stable_offset.get()),
                        };
                        if !Self::entry_valid(entry) {
                            return Err(LogError::Corrupt(format!(
                                "txnindex {} contains an invalid aborted interval",
                                path.display()
                            )));
                        }
                        if !entries.contains(&entry) {
                            entries.push(entry);
                        }
                    }
                    Ok(entries)
                },
            )?;
            tracing::Span::current().record("entries", entries.len());
            Ok(Self { path, entries })
        }
    }

    /// Append one aborted-txn entry.
    #[instrument(
        level = "debug",
        skip(self),
        fields(producer_id = entry.producer_id.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, entry: AbortedTxn) -> Result<(), LogError> {
        if !Self::entry_valid(entry) {
            return Err(LogError::InvalidArgument(format!(
                "invalid aborted transaction {}..={} for producer {}",
                entry.start_offset, entry.last_offset, entry.producer_id
            )));
        }
        if self.entries.contains(&entry) {
            return Ok(());
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let raw = AbortedTxnRaw::new(entry);
        f.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        f.sync_data().map_err(LogError::Io)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Remove entries whose inclusive end reaches the truncated suffix and
    /// rewrite the sidecar before it is reused as the active transaction index.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the transaction-index sidecar cannot be
    /// rewritten or synchronized.
    pub fn truncate_from(&mut self, offset: Offset) -> Result<(), LogError> {
        if let Some(entries) = crate::index::rewrite_retained_entries(
            &self.entries,
            |entry| entry.last_offset < offset,
            |entries| {
                let mut file = crate::index::rewrite_sidecar_file(&self.path)?;
                for entry in entries {
                    let raw = AbortedTxnRaw::new(*entry);
                    file.write_all(raw.as_bytes()).map_err(LogError::Io)?;
                }
                file.sync_data().map_err(LogError::Io)
            },
        )? {
            self.entries = entries;
        }
        Ok(())
    }

    #[must_use]
    pub fn entries(&self) -> &[AbortedTxn] {
        &self.entries
    }

    fn entry_valid(entry: AbortedTxn) -> bool {
        krabka_verified::aborted_transaction_interval(
            Some(entry.start_offset.0),
            entry.last_offset.0,
            entry.producer_id.get(),
        ) == Some((entry.start_offset.0, entry.last_offset.0))
    }

    /// Aborted transactions whose offset range overlaps `[start, end)`.
    pub fn aborted_in_range(
        &self,
        start: Offset,
        end: Offset,
    ) -> impl Iterator<Item = &AbortedTxn> {
        self.entries.iter().filter(move |entry| {
            krabka_verified::aborted_transaction_overlaps(
                entry.start_offset.0,
                entry.last_offset.0,
                start.0,
                end.0,
            )
        })
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use super::*;

    fn populated_index(name: &str, entries: &[AbortedTxn]) -> (TempDir, PathBuf, TxnIndex) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(name);
        let mut index = TxnIndex::open(path.clone()).unwrap();
        for &entry in entries {
            index.append(entry).unwrap();
        }
        (dir, path, index)
    }

    /// The same three-offset entry seeds retry, mutation and disk-round-trip cases.
    fn retry_entry() -> AbortedTxn {
        crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
            bounds: Offset(5)..=Offset(7),
            stable: Offset(8),
            ..Default::default()
        })
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct RangedIndexSetup<'a> {
        #[default("00.txnindex")]
        name: &'a str,
    }

    /// One transaction covers offsets 10 through 20 for fetch-boundary checks.
    fn ranged_index(setup: RangedIndexSetup<'_>) -> (TempDir, PathBuf, TxnIndex) {
        populated_index(
            setup.name,
            &[crate::test_support::aborted_txn(
                crate::test_support::AbortedTxnSetup {
                    producer: crate::ProducerId(1),
                    bounds: Offset(10)..=Offset(20),
                    stable: Offset(21),
                },
            )],
        )
    }

    /// The two independently specified malformed entries are refused both at append and open.
    fn malformed_entries() -> [(&'static str, AbortedTxn); 2] {
        [
            (
                "inverted",
                crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                    producer: crate::ProducerId(1),
                    bounds: Offset(8)..=Offset(7),
                    stable: Offset(8),
                }),
            ),
            (
                "negative-producer",
                crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                    producer: crate::ProducerId(-1),
                    bounds: Offset(7)..=Offset(8),
                    stable: Offset(9),
                }),
            ),
        ]
    }

    fn write_entry(path: &std::path::Path, entry: AbortedTxn) {
        let raw = AbortedTxnRaw::new(entry);
        std::fs::write(path, raw.as_bytes()).unwrap();
    }

    /// The exact 34-byte wire layout of one aborted-transaction record:
    /// a big-endian i16 version of 0, then `producer_id`, `first_offset`,
    /// `last_offset`, and `last_stable_offset`, each a big-endian i64.
    #[test]
    fn append_writes_the_exact_kafka_wire_layout() {
        let entry = crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
            bounds: crate::Offset(5)..=crate::Offset(9),
            stable: crate::Offset(10),
            ..Default::default()
        });
        let (_dir, path, _index) = populated_index("00.txnindex", &[entry]);

        let bytes = std::fs::read(&path).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&0_i16.to_be_bytes()); // version
        expected.extend_from_slice(&1000_i64.to_be_bytes()); // producer_id
        expected.extend_from_slice(&5_i64.to_be_bytes()); // first_offset
        expected.extend_from_slice(&9_i64.to_be_bytes()); // last_offset
        expected.extend_from_slice(&10_i64.to_be_bytes()); // last_stable_offset
        assert2::assert!(bytes.len() == ENTRY_BYTES);
        assert2::assert!(bytes == expected);
    }

    /// Three aborted transactions produce a file whose length is exactly
    /// `3 * 34` and whose decoded entries round-trip exactly.
    #[test]
    fn three_entries_round_trip_at_the_exact_kafka_entry_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let entries = [
            crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup::default()),
            crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                producer: crate::ProducerId(2000),
                bounds: crate::Offset(4)..=crate::Offset(6),
                stable: crate::Offset(7),
            }),
            crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                producer: crate::ProducerId(3000),
                bounds: crate::Offset(8)..=crate::Offset(10),
                stable: crate::Offset(11),
            }),
        ];
        let mut index = TxnIndex::open(path.clone()).unwrap();
        for entry in entries {
            index.append(entry).unwrap();
        }

        let bytes = std::fs::read(&path).unwrap();
        assert2::assert!(bytes.len() == 3 * ENTRY_BYTES);

        let reopened = TxnIndex::open(path).unwrap();
        assert2::assert!(reopened.entries() == entries);
    }

    /// A record whose version prefix is not 0 is refused on open.
    #[test]
    fn an_unsupported_version_is_refused_on_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1_i16.to_be_bytes()); // unsupported version
        bytes.extend_from_slice(&1000_i64.to_be_bytes());
        bytes.extend_from_slice(&0_i64.to_be_bytes());
        bytes.extend_from_slice(&3_i64.to_be_bytes());
        bytes.extend_from_slice(&4_i64.to_be_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert2::assert!(let LogError::Corrupt(_) = TxnIndex::open(path).unwrap_err());
    }

    /// The range end is exclusive: an aborted transaction that begins exactly
    /// where the fetch ends is not in it.
    ///
    /// A consumer uses this list to skip aborted records inside the batch range
    /// it was handed. Including one that starts past the end makes it skip
    /// records it was never sent.
    #[test]
    fn an_aborted_txn_starting_at_the_range_end_is_outside_it() {
        let (_dir, _path, index) = ranged_index(RangedIndexSetup {
            name: "00000000000000000000.txnindex",
        });

        let found =
            |start: i64, end: i64| index.aborted_in_range(Offset(start), Offset(end)).count();
        assert2::check!(found(0, 10) == 0, "a fetch ending where the txn begins");
        assert2::check!(found(0, 11) == 1, "a fetch reaching one past its start");
        assert2::check!(found(20, 30) == 1, "a fetch starting on its last offset");
        assert2::check!(found(21, 30) == 0, "a fetch starting past its last offset");
    }

    #[test]
    fn empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let idx = TxnIndex::open(path).unwrap();
        assert2::assert!(idx.entries() == &[]);
    }

    #[test]
    fn malformed_intervals_fail_closed_at_append_and_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut index = TxnIndex::open(path).unwrap();
        for (_, entry) in malformed_entries() {
            assert2::assert!(let LogError::InvalidArgument(_) = index.append(entry).unwrap_err());
        }
        assert2::assert!(index.entries().is_empty());

        let partial = dir.path().join("partial.txnindex");
        std::fs::write(&partial, [0_u8]).unwrap();
        assert2::assert!(let LogError::Corrupt(_) = TxnIndex::open(partial).unwrap_err());

        for (name, entry) in malformed_entries() {
            let path = dir.path().join(format!("{name}.txnindex"));
            write_entry(&path, entry);
            assert2::assert!(let LogError::Corrupt(_) = TxnIndex::open(path).unwrap_err());
        }
    }

    #[test]
    fn exact_aborted_interval_retry_is_idempotent() {
        let entry = retry_entry();
        let (_dir, path, mut index) = populated_index("00.txnindex", &[entry]);
        index.append(entry).unwrap();
        assert2::assert!(index.entries() == [entry]);

        let raw = AbortedTxnRaw::new(entry);
        let mut bytes = raw.as_bytes().to_vec();
        bytes.extend_from_slice(raw.as_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert2::assert!(TxnIndex::open(path).unwrap().entries() == [entry]);
    }

    #[test]
    fn mutation_io_failures_leave_the_in_memory_index_unchanged() {
        let original = retry_entry();
        let (_dir, path, mut index) = populated_index("00.txnindex", &[original]);
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        assert2::assert!(
            let LogError::Io(_) = index
                .append(crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup { bounds: crate::Offset(10)..=crate::Offset(12), stable: crate::Offset(13), ..Default::default() }))
                .unwrap_err()
        );
        assert2::assert!(index.entries() == [original]);
        assert2::assert!(let LogError::Io(_) = index.truncate_from(Offset(0)).unwrap_err());
        assert2::assert!(index.entries() == [original]);
    }

    #[test]
    fn empty_or_inverted_fetch_range_matches_no_aborted_interval() {
        let (_dir, _path, index) = ranged_index(RangedIndexSetup::default());
        assert2::assert!(index.aborted_in_range(Offset(10), Offset(10)).count() == 0);
        assert2::assert!(index.aborted_in_range(Offset(20), Offset(10)).count() == 0);
    }

    #[test]
    fn append_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path.clone()).unwrap();
        idx.append(retry_entry()).unwrap();
        idx.append(crate::test_support::aborted_txn(
            crate::test_support::AbortedTxnSetup {
                bounds: crate::Offset(10)..=crate::Offset(12),
                stable: crate::Offset(13),
                ..Default::default()
            },
        ))
        .unwrap();

        let idx2 = TxnIndex::open(path).unwrap();
        assert2::assert!(
            idx2.entries()
                == &[
                    AbortedTxn {
                        start_offset: Offset(5),
                        last_offset: Offset(7),
                        producer_id: ProducerId(1000),
                        last_stable_offset: Offset(8)
                    },
                    AbortedTxn {
                        start_offset: Offset(10),
                        last_offset: Offset(12),
                        producer_id: ProducerId(1000),
                        last_stable_offset: Offset(13)
                    },
                ]
        );
    }

    #[test]
    fn truncate_from_removes_overlapping_tail_entries_and_is_retryable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut index = TxnIndex::open(path.clone()).unwrap();
        for (start, last) in [(0, 4), (5, 9)] {
            index
                .append(AbortedTxn {
                    start_offset: Offset(start),
                    last_offset: Offset(last),
                    producer_id: ProducerId(1),
                    last_stable_offset: Offset(last + 1),
                })
                .unwrap();
        }

        for _ in 0..2 {
            index.truncate_from(Offset(5)).unwrap();
            assert2::assert!(index.entries().len() == 1);
            assert2::assert!(index.entries()[0].last_offset == Offset(4));
        }
        let reopened = TxnIndex::open(path.clone()).unwrap();
        assert2::assert!(reopened.entries() == index.entries());

        index.truncate_from(Offset(4)).unwrap();
        assert2::assert!(index.entries().is_empty());
    }

    #[test]
    fn aborted_in_range_overlaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path).unwrap();
        idx.append(crate::test_support::aborted_txn(
            crate::test_support::AbortedTxnSetup {
                producer: crate::ProducerId(1),
                bounds: crate::Offset(0)..=crate::Offset(4),
                stable: crate::Offset(5),
            },
        ))
        .unwrap();
        idx.append(crate::test_support::aborted_txn(
            crate::test_support::AbortedTxnSetup {
                producer: crate::ProducerId(2),
                bounds: crate::Offset(10)..=crate::Offset(14),
                stable: crate::Offset(15),
            },
        ))
        .unwrap();

        let in_3_to_12 = idx
            .aborted_in_range(Offset(3), Offset(12))
            .copied()
            .collect::<Vec<_>>();
        let in_5_to_9 = idx
            .aborted_in_range(Offset(5), Offset(9))
            .copied()
            .collect::<Vec<_>>();
        assert2::assert!(
            in_3_to_12
                == vec![
                    crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                        producer: crate::ProducerId(1),
                        bounds: crate::Offset(0)..=crate::Offset(4),
                        stable: crate::Offset(5)
                    }),
                    crate::test_support::aborted_txn(crate::test_support::AbortedTxnSetup {
                        producer: crate::ProducerId(2),
                        bounds: crate::Offset(10)..=crate::Offset(14),
                        stable: crate::Offset(15)
                    }),
                ]
        );
        assert2::assert!(in_5_to_9 == Vec::new());
    }
}
