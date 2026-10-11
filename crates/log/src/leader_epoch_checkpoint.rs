//! Per-partition `.leader-epoch-checkpoint` file. The two-column text format
//! matches Apache Kafka exactly:
//!
//! ```text
//!   0          <-- header version
//!   <n>        <-- row count
//!   <epoch_0> <start_offset_0>
//!   <epoch_1> <start_offset_1>
//!   ...
//! ```
//!
//! The byte layout stays the same, so `kafka-dump-log` can read these files.

use std::path::PathBuf;

use krabka_ids::{LeaderEpoch, Offset};
pub use krabka_verified::{EpochEntry, epoch_and_offset_for_entries};

use crate::io::LogIo;

mod file;
mod lookup;
mod mutation;

#[cfg(test)]
mod fuzz;
#[cfg(test)]
mod test_support;

#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    io: std::sync::Arc<dyn LogIo>,
    entries: Vec<EpochEntry>,
}

/// Kafka sentinel: "no leader epoch information".
#[cfg(test)]
pub const UNDEFINED_EPOCH: LeaderEpoch = LeaderEpoch(-1);
/// Kafka sentinel: "no offset".
pub const UNDEFINED_OFFSET: Offset = Offset(-1);

impl LeaderEpochCheckpoint {
    /// Route this checkpoint's writes, syncs and renames through `io`.
    ///
    /// Only [`crate::Log::test_set_io`] calls this: the checkpoint is opened
    /// with the real filesystem and stays there unless a test replaces it.
    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn set_io(&mut self, io: std::sync::Arc<dyn LogIo>) {
        self.io = io;
    }

    #[must_use]
    pub fn latest_epoch(&self) -> Option<LeaderEpoch> {
        self.entries.iter().map(|e| e.epoch).max()
    }

    #[must_use]
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }
}

pub(crate) fn is_strict_successor(previous: &EpochEntry, next: &EpochEntry) -> bool {
    previous.epoch < next.epoch && previous.start_offset < next.start_offset
}

/// Pure core of [`LeaderEpochCheckpoint::append`]: an idempotent
/// push-if-absent. It returns `true` when it added a new entry, so the caller
/// knows that it must flush.
pub(crate) fn append_to(
    entries: &mut Vec<EpochEntry>,
    epoch: LeaderEpoch,
    start_offset: Offset,
) -> bool {
    if entries.iter().any(|e| e.epoch == epoch) {
        return false;
    }
    entries.push(EpochEntry {
        epoch,
        start_offset,
    });
    true
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_end`]: drop entries that
/// begin at or after `end_offset`.
pub(crate) fn truncate_to(entries: &mut Vec<EpochEntry>, end_offset: Offset) {
    entries.retain(|e| e.start_offset < end_offset);
}

#[cfg(test)]
#[path = "leader_epoch_model.rs"]
mod leader_epoch_model;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_successor_requires_strictly_increasing_epoch_and_offset() {
        let e1 = EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(0),
        };
        let e2 = EpochEntry {
            epoch: LeaderEpoch(1),
            start_offset: Offset(5),
        };
        assert2::assert!(!is_strict_successor(&e1, &e2));
        assert2::assert!(!is_strict_successor(&e1, &e1));
        let e3 = EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(0),
        };
        assert2::assert!(!is_strict_successor(&e1, &e3));
        let e4 = EpochEntry {
            epoch: LeaderEpoch(2),
            start_offset: Offset(5),
        };
        assert2::assert!(is_strict_successor(&e1, &e4));
    }
}
