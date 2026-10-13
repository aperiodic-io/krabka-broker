//! The mutations a live checkpoint accepts -- `append`, `assign`,
//! `truncate_from_end`, `truncate_from_start` and `clear` -- each of which
//! persists the file only when it actually changed the entry list. They wrap the pure cores in [`super`]
//! and mirror Kafka's `LeaderEpochFileCache`.

use {
    super::{EpochEntry, LeaderEpochCheckpoint, append_to, is_strict_successor, truncate_to},
    crate::error::LogError,
    krabka_ids::{LeaderEpoch, Offset},
    tracing::instrument,
};

impl LeaderEpochCheckpoint {
    /// Append `(epoch, start_offset)`. This method is idempotent. A second
    /// append of an entry with the same epoch does nothing and keeps the
    /// earliest recorded `start_offset`. The method rewrites the file
    /// atomically.
    #[instrument(level = "debug", skip(self), fields(epoch = epoch.0, start_offset = start_offset.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, epoch: LeaderEpoch, start_offset: Offset) -> Result<(), LogError> {
        if self.entries.iter().any(|entry| entry.epoch == epoch) {
            return Ok(());
        }
        let entry = EpochEntry {
            epoch,
            start_offset,
        };
        if self
            .entries
            .last()
            .is_some_and(|previous| !is_strict_successor(previous, &entry))
        {
            return Err(LogError::InvalidArgument(format!(
                "leader epoch checkpoint entry ({}, {}) is not strictly after the previous entry",
                epoch.0, start_offset.0
            )));
        }
        if append_to(&mut self.entries, epoch, start_offset) {
            self.flush()?;
        }
        Ok(())
    }

    /// Kafka's `LeaderEpochFileCache.assign(epoch, startOffset)`, which
    /// `UnifiedLog.assignEpochStartOffset` calls when `Partition.makeLeader`
    /// records a new leader epoch at the log end before anything is written.
    ///
    /// It is a no-op when `epoch` is already the latest recorded epoch and
    /// `start_offset` is at or after that entry's start
    /// (`isUpdateNeeded`). Otherwise it first drops every trailing entry whose
    /// epoch is `>= epoch` or whose start is `>= start_offset`
    /// (`maybeTruncateNonMonotonicEntries`) and then records the new entry, so
    /// the history stays strictly increasing in both columns. The file is
    /// rewritten only when the entry list changed, and a failed rewrite
    /// leaves the entries as they were.
    ///
    /// # Errors
    /// Returns [`LogError::InvalidArgument`] for a negative epoch or start
    /// offset, as Kafka throws `IllegalArgumentException`, and an I/O error
    /// when the checkpoint cannot be persisted.
    #[instrument(level = "debug", skip(self), fields(epoch = epoch.0, start_offset = start_offset.0), err)]
    pub fn assign(&mut self, epoch: LeaderEpoch, start_offset: Offset) -> Result<(), LogError> {
        if epoch.0 < 0 || start_offset.0 < 0 {
            return Err(LogError::InvalidArgument(format!(
                "invalid leader epoch checkpoint entry ({}, {})",
                epoch.0, start_offset.0
            )));
        }
        // `isUpdateNeeded` first, so the per-batch call on every append costs
        // one comparison and no copy of the history.
        if !assign_needed(&self.entries, epoch, start_offset) {
            return Ok(());
        }
        let previous = self.entries.clone();
        assign_to(&mut self.entries, epoch, start_offset);
        self.flush_or_restore(previous)
    }

    /// Remove epoch entries that begin at or after `end_offset`. This mirrors
    /// Kafka's LeaderEpochFileCache.truncateFromEnd. The method persists the
    /// file if anything changed.
    #[instrument(level = "debug", skip(self), fields(end_offset = end_offset.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn truncate_from_end(&mut self, end_offset: Offset) -> Result<(), LogError> {
        let before = self.entries.len();
        truncate_to(&mut self.entries, end_offset);
        if self.entries.len() != before {
            self.flush()?;
        }
        Ok(())
    }

    /// Kafka's `LeaderEpochFileCache.truncateFromStart(startOffset)`, which
    /// `UnifiedLog.maybeIncrementLogStartOffset` calls when the log start
    /// moves up and `LogLoader.load` calls with the checkpointed log start.
    ///
    /// Every entry that starts at or below `start_offset` is removed, and the
    /// newest of them comes back with its start raised to `start_offset`, so
    /// the epoch that covers the new log start still answers for it and no
    /// entry points below the log. The call is exclusive: an entry that
    /// starts exactly at `start_offset` keeps its place. The file is
    /// rewritten only when an entry was removed, and a failed rewrite leaves
    /// the entries as they were.
    ///
    /// # Errors
    /// Returns an I/O error when the checkpoint cannot be persisted.
    #[instrument(level = "debug", skip(self), fields(start_offset = start_offset.0), err)]
    pub fn truncate_from_start(&mut self, start_offset: Offset) -> Result<(), LogError> {
        let previous = self.entries.clone();
        if truncate_start_of(&mut self.entries, start_offset) {
            self.flush_or_restore(previous)?;
        }
        Ok(())
    }

    fn flush_or_restore(&mut self, previous: Vec<EpochEntry>) -> Result<(), LogError> {
        self.flush().inspect_err(|_| {
            // The file still holds `previous`; so must memory.
            self.entries = previous;
        })
    }

    /// Drop every recorded epoch. This mirrors Kafka's
    /// `LeaderEpochFileCache.clearAndFlush`, which
    /// `LocalLog.truncateFullyAndStartAt` invokes.
    ///
    /// [`crate::Log::reset_to`] uses this method. Once the log is empty, no
    /// offset has a backing record, so the broker may advertise no epoch. The
    /// method persists the now-empty file only when it removed something.
    #[instrument(level = "debug", skip(self), fields(cleared = self.entries.len()), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn clear(&mut self) -> Result<(), LogError> {
        if self.entries.is_empty() {
            return Ok(());
        }
        self.entries.clear();
        self.flush()
    }
}

/// Kafka's `LeaderEpochFileCache.isUpdateNeeded`: whether `assign` would
/// change `entries`. Only the latest epoch at or after its own start is a
/// no-op.
fn assign_needed(entries: &[EpochEntry], epoch: LeaderEpoch, start_offset: Offset) -> bool {
    entries
        .last()
        .is_none_or(|latest| latest.epoch != epoch || start_offset < latest.start_offset)
}

/// Pure core of [`LeaderEpochCheckpoint::assign`], for an entry that
/// [`assign_needed`] admitted: Kafka's `maybeTruncateNonMonotonicEntries`
/// followed by the put.
fn assign_to(entries: &mut Vec<EpochEntry>, epoch: LeaderEpoch, start_offset: Offset) {
    while entries
        .last()
        .is_some_and(|last| last.epoch >= epoch || last.start_offset >= start_offset)
    {
        entries.pop();
    }
    entries.push(EpochEntry {
        epoch,
        start_offset,
    });
}

/// Pure core of [`LeaderEpochCheckpoint::truncate_from_start`]. Returns
/// `true` when it removed an entry, so the caller knows that it must flush.
fn truncate_start_of(entries: &mut Vec<EpochEntry>, start_offset: Offset) -> bool {
    let removed = entries
        .iter()
        .take_while(|entry| entry.start_offset <= start_offset)
        .count();
    let Some(newest_removed) = removed.checked_sub(1).map(|index| entries[index].epoch) else {
        return false;
    };
    if removed == 1 && entries[0].start_offset == start_offset {
        // The one entry at the new start is put back unchanged.
        return false;
    }
    entries.splice(..removed, [EpochEntry {
        epoch: newest_removed,
        start_offset,
    }]);
    true
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            leader_epoch_checkpoint::test_support::{
                TempCheckpoint, check_persisted, checkpoint, entry, fresh,
            },
            test_support::TempPath,
        },
    };

    #[test]
    fn append_preserves_existing_rows() {
        let TempPath { dir: _d, path } = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(LeaderEpoch(0), Offset(0)).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(LeaderEpoch(1), Offset(50)).unwrap();
        assert2::assert!(c2.entries() == &[entry(0, 0), entry(1, 50)]);
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0)]);
        c.append(LeaderEpoch(0), Offset(999)).unwrap(); // ignored; epoch 0 already recorded
        assert2::assert!(c.entries() == &[entry(0, 0)]);
    }

    #[test]
    fn append_rejects_novel_out_of_order_entries_without_mutating() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0), entry(5, 100)]);
        let before = std::fs::read(&c.path).unwrap();

        for (epoch, offset) in [(2, 150), (6, 100)] {
            let error = c.append(LeaderEpoch(epoch), Offset(offset)).unwrap_err();
            assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        }

        assert2::assert!(std::fs::read(&c.path).unwrap() == before);
        assert2::assert!(c.entries() == &[entry(0, 0), entry(5, 100)]);
    }

    #[test]
    fn append_after_truncation_restores_a_strict_history() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(0, 0), entry(5, 100)]);
        c.truncate_from_end(Offset(100)).unwrap();
        c.append(LeaderEpoch(2), Offset(50)).unwrap();
        c.append(LeaderEpoch(4), Offset(80)).unwrap();

        assert2::assert!(
            c.epoch_and_offset_for(LeaderEpoch(3), Offset(120)) == (LeaderEpoch(2), Offset(80))
        );
    }

    /// One row per `LeaderEpochFileCache.assign` outcome, each checked in
    /// memory and after a reopen of the persisted file.
    #[test]
    fn assign_follows_kafka_leader_epoch_file_cache_assign() {
        for (name, recorded, (epoch, start), expected) in [
            ("first entry", &[][..], (2, 6), vec![entry(2, 6)]),
            (
                "new epoch after the log end appends",
                &[entry(0, 0), entry(2, 6)][..],
                (3, 9),
                vec![entry(0, 0), entry(2, 6), entry(3, 9)],
            ),
            (
                "latest epoch at a later offset is a no-op",
                &[entry(2, 6)][..],
                (2, 7),
                vec![entry(2, 6)],
            ),
            (
                "latest epoch at its own offset is a no-op",
                &[entry(2, 6)][..],
                (2, 6),
                vec![entry(2, 6)],
            ),
            (
                "latest epoch at an earlier offset moves its start",
                &[entry(1, 0), entry(2, 6)][..],
                (2, 5),
                vec![entry(1, 0), entry(2, 5)],
            ),
            (
                "newer epoch at the same start replaces an unwritten epoch",
                &[entry(1, 0), entry(2, 6)][..],
                (3, 6),
                vec![entry(1, 0), entry(3, 6)],
            ),
            (
                "older epoch drops every entry it does not strictly follow",
                &[entry(1, 0), entry(2, 6), entry(3, 8)][..],
                (2, 7),
                vec![entry(1, 0), entry(2, 7)],
            ),
        ] {
            let TempCheckpoint {
                dir: _d,
                checkpoint: mut c,
            } = checkpoint(recorded);
            c.assign(LeaderEpoch(epoch), Offset(start)).unwrap();
            check_persisted(&c, &expected, name);
        }
    }

    #[test]
    fn assign_rejects_negative_entries_without_mutating() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0)]);
        for (epoch, start) in [(-1, 5), (2, -1)] {
            let error = c.assign(LeaderEpoch(epoch), Offset(start)).unwrap_err();
            assert2::assert!(matches!(error, LogError::InvalidArgument(_)));
        }
        assert2::assert!(c.entries() == &[entry(1, 0)]);
    }

    #[test]
    fn truncate_from_end_removes_entries_at_or_after_end_offset() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0), entry(7, 4)]);
        c.truncate_from_end(Offset(4)).unwrap();
        assert2::assert!(c.latest_epoch() == Some(LeaderEpoch(1)));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(7), Offset(4)) == Offset(-1));
        assert2::assert!(c.end_offset_for_epoch(LeaderEpoch(1), Offset(4)) == Offset(4));
        // Persisted: a reopen sees only epoch 1.
        let reopened = LeaderEpochCheckpoint::open(c.path.clone()).unwrap();
        assert2::assert!(reopened.latest_epoch() == Some(LeaderEpoch(1)));
    }

    /// One row per `LeaderEpochFileCache.truncateFromStart` outcome, over the
    /// history `(0, 5) (1, 10) (2, 15)`, each checked in memory and after a
    /// reopen of the persisted file.
    #[test]
    fn truncate_from_start_follows_kafka_leader_epoch_file_cache() {
        let history = vec![entry(0, 5), entry(1, 10), entry(2, 15)];
        for (name, recorded, start, expected) in [
            ("an empty cache stays empty", vec![], 7, vec![]),
            (
                "a start below every entry changes nothing",
                history.clone(),
                4,
                history.clone(),
            ),
            (
                "a start on the first entry keeps it",
                history.clone(),
                5,
                history.clone(),
            ),
            (
                "a start inside the first epoch raises its start",
                history.clone(),
                8,
                vec![entry(0, 8), entry(1, 10), entry(2, 15)],
            ),
            (
                "a start on a later entry drops every earlier one",
                history.clone(),
                10,
                vec![entry(1, 10), entry(2, 15)],
            ),
            (
                "a start past every entry keeps the latest epoch at the start",
                history.clone(),
                17,
                vec![entry(2, 17)],
            ),
        ] {
            let TempCheckpoint {
                dir: _d,
                checkpoint: mut c,
            } = checkpoint(&recorded);
            c.truncate_from_start(Offset(start)).unwrap();
            check_persisted(&c, &expected, name);
        }
    }

    #[test]
    fn clear_removes_all_entries_and_persists_empty() {
        let TempCheckpoint {
            dir: _d,
            checkpoint: mut c,
        } = checkpoint(&[entry(1, 0), entry(2, 50)]);
        c.clear().unwrap();
        assert2::assert!(c.entries() == &[][..]);
        assert2::assert!(c.latest_epoch() == None);
        // Persisted: a reopen sees no entries.
        let reopened = LeaderEpochCheckpoint::open(c.path.clone()).unwrap();
        assert2::assert!(reopened.entries().is_empty());
    }

    #[test]
    fn clear_on_empty_cache_skips_flush_and_writes_no_file() {
        let TempPath { dir: _d, path } = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.clear().unwrap();
        assert2::assert!(c.entries().is_empty());
        // The early-return skips the flush for an already-empty cache, so no
        // checkpoint file is written. A forced-`false` empty-guard would flush
        // an empty file here instead.
        assert2::assert!(!path.exists());
    }
}
