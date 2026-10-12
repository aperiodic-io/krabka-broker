//! Log-level accessors and the whole-log reset.
//!
//! These are the coordinates the broker reads off a partition -- the log
//! start and end, the size, the last stable offset, the producer and
//! transaction state -- together with the config swap and the hard reset
//! that empties the log at a new base offset.

use std::path::Path;

use krabka_ids::{Offset, ProducerId};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::Log;
#[cfg(any(test, feature = "test-helpers"))]
use crate::io::LogIo;
use crate::{
    RecoveredProducer,
    config::LogConfig,
    error::LogError,
    leader_epoch_checkpoint::LeaderEpochCheckpoint,
    log_start_offset_checkpoint,
    producer_snapshot::{self, ProducerSnapshotEntry},
    segment::Segment,
    txn_index::{AbortedTxn, TxnIndex},
};

impl Log {
    /// Empty the volatile state before a hard reset or a durable producer-state reload.
    pub(super) fn clear_producer_and_transaction_state(&mut self) {
        self.pending.clear();
        self.verification_states.clear();
        self.unreplicated.clear();
        self.pending_stamp_ranges.clear();
        self.coordinator_epochs.clear();
        self.producer_state.clear();
        self.earlier_batches.clear();
    }

    /// Directory this log was opened against. The broker's intra-broker
    /// log-dir reassignment (KIP-113) reads this to find the current owning
    /// `log.dir` of a partition. The broker does not have to repeat the
    /// directory-layout convention.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// First absolute offset any reader may ask for, wherever the records for
    /// it live: Kafka's global `logStartOffset`.
    ///
    /// On a tiered topic (KIP-405) this can sit below
    /// [`Log::local_log_start_offset`], and the offsets between the two are
    /// the ones the remote tier serves. A fetch below *this* floor is
    /// `OFFSET_OUT_OF_RANGE` and no tier answers it.
    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        self.start_offset
    }

    /// The global floor, when this process is the one that moved it.
    ///
    /// `None` on a log whose floor was only inferred from the segments present
    /// at [`Log::open`]. A caller that would *refuse* or *delete* on the
    /// strength of the floor must use this rather than
    /// [`Log::log_start_offset`]: on a tiered partition whose local segments
    /// were evicted, the inferred floor sits above everything the archive
    /// holds, so refusing below it hides readable records and deleting below
    /// it destroys them. The log-start checkpoint carries an established floor
    /// across a restart, so a reopened log answers `None` only when nobody has
    /// moved it.
    #[must_use]
    pub fn established_log_start(&self) -> Option<Offset> {
        self.start_offset_established.then_some(self.start_offset)
    }

    /// The epoch of the earliest leader-epoch entry, when a log start that
    /// somebody established truncated the cache to it: Kafka's
    /// `leaderEpochCache.earliestEntry` after `truncateFromStart(logStartOffset)`.
    ///
    /// Only then does the entry say something about the remote tier. Kafka's
    /// `RemoteLogManager` deletes a remote segment whose epochs all lie below
    /// it (`deleteLogSegmentsDueToLeaderEpochCacheTruncation`), which is right
    /// for the epoch that owns the log start, because every offset from there
    /// on has that epoch or a later one. An earliest entry that no advance of
    /// the log start put there (a cache that never held the older epochs, or
    /// one cleared by [`Log::reset_to`] and refilled from the fetched
    /// records) says nothing about them, so this answers `None`. The epoch
    /// cache is cut from the start only by [`Log::set_log_start_offset`] and
    /// by the log-start checkpoint [`Log::open`] restores, never by the
    /// segments a tiered partition still holds locally, so a restart does not
    /// move it.
    #[must_use]
    pub fn log_start_epoch(&self) -> Option<krabka_ids::LeaderEpoch> {
        let start = self.established_log_start()?;
        self.epoch_checkpoint
            .entries()
            .first()
            .filter(|entry| entry.start_offset == start)
            .map(|entry| entry.epoch)
    }

    /// The first offset the segments on disk begin at, before the global
    /// floor is applied.
    pub(super) fn first_local_offset(&self) -> Offset {
        if let Some(first) = self.segments.first() {
            first.base_offset()
        } else if let Some(active) = &self.active {
            active.base_offset()
        } else {
            Offset(0)
        }
    }

    /// Move the global log start up to `new_start`.
    ///
    /// The pointer only ever moves forward, the way Kafka's
    /// `maybeIncrementLogStartOffset` does: a request that names an offset at
    /// or below the current floor is a no-op. `trim_to_offset` uses this
    /// method for the active-segment case, the broker's `DeleteRecords`
    /// handler uses it, and the `RemoteLogManager` uses it after a remote
    /// segment is deleted. This method does NOT truncate on-disk segments. It
    /// only moves the start pointer.
    ///
    /// The new value is checkpointed to `log-start-offset-checkpoint` before
    /// this method returns, so a start no segment name witnesses -- a trim
    /// that landed inside a segment, or a tiered floor below every segment
    /// still on disk -- survives a reopen. Without that, [`Log::open`] derives
    /// the start from the first surviving base offset and serves records that
    /// a `DeleteRecords` already deleted.
    ///
    /// When the start does move up, the rest of the log follows it the way
    /// Kafka's `maybeIncrementLogStartOffset` makes it follow:
    ///
    /// - the leader-epoch cache drops what lies below the new start
    ///   (`leaderEpochCache.truncateFromStart`, see
    ///   [`LeaderEpochCheckpoint::truncate_from_start`]);
    /// - a complete transaction whose marker lies below the new start stops
    ///   holding the last stable offset
    ///   (`ProducerStateManager.onLogStartOffsetIncremented` →
    ///   `removeUnreplicatedTransactions`), because nothing a leader change
    ///   could truncate is left of it;
    /// - the first unstable offset is recomputed (`maybeIncrementFirstUnstableOffset`).
    ///
    /// `new_start` must be non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `new_start` is negative, and
    /// [`LogError::Io`] if the log-start or leader-epoch checkpoint cannot be
    /// written.
    pub fn set_log_start_offset(&mut self, new_start: Offset) -> Result<(), LogError> {
        if new_start < 0 {
            return Err(LogError::InvalidArgument(
                "set_log_start_offset: new_start must be >= 0".into(),
            ));
        }
        let incremented = new_start > self.start_offset;
        let new_start = self.start_offset.max(new_start);
        // The checkpoint is durable when this returns, directory sync
        // included: `DeleteRecords` is acknowledged as soon as the trim does,
        // and a trimmed partition can then sit idle with no later `sync()` to
        // pay a deferred debt.
        log_start_offset_checkpoint::write(&*self.io, &self.dir, new_start)?;
        self.start_offset = new_start;
        // Whoever calls this deleted the records below `new_start`, so the
        // floor now means something a reader may be refused against, and the
        // checkpoint carries that meaning across a reopen.
        self.start_offset_established = true;
        if incremented {
            self.epoch_checkpoint.truncate_from_start(new_start)?;
            self.unreplicated
                .retain(|_, marker_offset| *marker_offset >= new_start);
            self.refresh_lso()?;
        }
        Ok(())
    }

    /// Move the log start down to `ceiling` when it sits above it. Truncation
    /// is the one caller: Kafka's `UnifiedLog.truncateTo` sets
    /// `logStartOffset = Math.min(targetOffset, logStartOffset)`, so a cut that
    /// lands below a start `DeleteRecords` or retention had advanced pulls the
    /// start back onto the retained data. The new value is checkpointed for
    /// the same reason [`Log::set_log_start_offset`] checkpoints an advance.
    pub(super) fn lower_log_start_offset(&mut self, ceiling: Offset) -> Result<(), LogError> {
        if ceiling >= self.start_offset {
            return Ok(());
        }
        log_start_offset_checkpoint::write(&*self.io, &self.dir, ceiling)?;
        self.start_offset = ceiling;
        Ok(())
    }

    /// Reset the log to be empty at `new_base`.
    ///
    /// This method drops every segment and every on-disk file, then creates
    /// a fresh active segment at `new_base`. The replicator's
    /// `OFFSET_OUT_OF_RANGE` recovery path uses it when the follower has
    /// fallen behind the leader's `log_start`. `truncate_to` cannot help
    /// there, because `log_start` must move *forward* past the point where
    /// no local data exists.
    #[instrument(level = "info", skip_all, fields(new_base = new_base.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn reset_to(&mut self, new_base: Offset) -> Result<(), LogError> {
        self.rollover_flusher.finish()?;
        self.roll_jitter = None;
        if new_base < 0 {
            return Err(LogError::OffsetMismatch {
                expected: Offset(0),
                actual: new_base,
            });
        }

        producer_snapshot::remove_all(&self.dir)?;
        self.invalidate_delivery_schedule(new_base);

        // Drop every sealed segment + its on-disk files.
        while let Some(segment) = self.segments.last() {
            let base = segment.base_offset();
            self.remove_truncated_segment_files(base)?;
            self.segments.pop();
            self.sealed_txn_indexes.remove(&base);
            self.stamp_indexes.remove(&base);
        }

        // Drop the active segment + its on-disk files.
        if let Some(active) = &self.active {
            let base = active.base_offset();
            self.remove_truncated_segment_files(base)?;
        }

        // A hard reset re-bases the local log after a divergence or a
        // snapshot install. It says nothing about the remote tier, which may
        // still hold lower offsets, so the floor moves without becoming a
        // statement anything may be refused or deleted against -- and the
        // checkpoint that would restore such a statement goes with it.
        self.start_offset = new_base;
        self.start_offset_established = false;
        log_start_offset_checkpoint::remove(&self.dir)?;

        let mut new_active = Segment::create(&self.dir, new_base)?;
        new_active.set_io(self.io.clone());
        self.active_txn_index = TxnIndex::open(new_active.txn_index_path())?;
        let stamp_index_path = new_active.stamp_index_path();
        self.clear_producer_and_transaction_state();
        self.sealed_txn_indexes.clear();
        self.stamp_indexes.clear();
        self.lso = new_active.last_offset() + 1; // = new_base (empty segment)
        self.active = Some(new_active);
        self.dir_sync_needed = true;
        // Preserve any injected stamp source; reopen its (fresh) sidecar.
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        // The log now holds no records, so the leader-epoch cache must hold no
        // entries (Kafka's truncateFullyAndStartAt → leaderEpochCache.clearAndFlush).
        // Leaving stale entries makes a follower advertise a `last_fetched_epoch`
        // it has no record for, so the leader's KIP-320 reconciliation serves a
        // batch at a mismatched base offset and the follower loops forever on
        // append_at — a phantom ISR member that pins the high-watermark.
        self.epoch_checkpoint.clear()?;
        // Kafka's `truncateFullyAndStartAt` rebuilds the (now empty) producer
        // state and takes a snapshot at the new base: its
        // `ProducerStateManager.truncateFullyAndStartAt` resets the last
        // snapshot offset to 0, so `takeSnapshot` writes one at any base above
        // 0.
        if new_base.0 > 0 {
            producer_snapshot::write(&*self.io, &self.dir, new_base, &self.producer_state)?;
        }
        self.reap_deleted_files(std::time::SystemTime::now())
    }

    /// Kafka's `ProducerStateManager.takeSnapshot`: write the producer state at
    /// the log end offset, so a reopen restores it from the snapshot and
    /// replays nothing. A snapshot already at that offset is kept, and none is
    /// written at or below the log start, which Kafka takes as the last
    /// snapshot offset when no snapshot survives a reload.
    ///
    /// # Errors
    /// Returns an error when the snapshot cannot be written.
    pub fn take_producer_snapshot(&mut self) -> Result<(), LogError> {
        let log_end = self.log_end_offset();
        if log_end.0 > self.producer_reload_range(log_end).log_start {
            self.sync()?;
            producer_snapshot::write(&*self.io, &self.dir, log_end, &self.producer_state)?;
        }
        Ok(())
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        Offset(0)
    }

    /// Total `.log` size across sealed and active segments.
    ///
    /// The value comes from the segments' tracked logical size, not from a
    /// filesystem stat. It therefore shows buffered appends immediately and
    /// in the same way on every platform. On some operating systems a
    /// directory stat can lag an open, unflushed write handle.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        let active = self.active.as_ref().map_or(ByteSize::ZERO, Segment::size);
        self.segments
            .iter()
            .fold(active, |total, seg| total + seg.size())
    }

    /// First unstable offset: the first offset of the earliest transaction
    /// that is open, or complete with a marker the high watermark has not
    /// passed. It is the log end offset when there is no such transaction.
    ///
    /// This value does not know the high watermark. A reader must use
    /// [`Log::last_stable_offset`], which releases replicated transactions
    /// and caps the answer at the high watermark.
    #[must_use]
    pub fn lso(&self) -> Offset {
        self.lso
    }

    /// First offset of `producer_id`'s currently open transaction on this
    /// partition, or `None` when no transaction from that producer is pending.
    #[must_use]
    pub fn pending_transaction_start(&self, producer_id: ProducerId) -> Option<Offset> {
        self.pending.get(&producer_id).copied()
    }

    /// Producer and coordinator generations used to admit one transaction
    /// marker, plus whether that producer currently has an open transaction.
    /// Missing generations use Kafka's `-1` sentinel.
    #[must_use]
    pub fn transaction_marker_state(&self, producer_id: ProducerId) -> (i16, i32, bool) {
        let producer_epoch = self
            .producer_state
            .get(&producer_id)
            .map_or(-1, |entry| entry.producer_epoch);
        let coordinator_epoch = self
            .coordinator_epochs
            .get(&producer_id)
            .copied()
            .unwrap_or(-1);
        (
            producer_epoch,
            coordinator_epoch,
            self.pending.contains_key(&producer_id),
        )
    }

    /// The producer state this log holds for `producer_id`, if any.
    ///
    /// Append and recovery update the same entry with the same rule, so the
    /// value after a live append equals the value that a reopen rebuilds.
    #[must_use]
    pub fn producer_state_entry(&self, producer_id: ProducerId) -> Option<ProducerSnapshotEntry> {
        self.producer_state.get(&producer_id).copied()
    }

    /// Producer state restored from the newest valid Kafka-compatible
    /// snapshot and the uncovered local log tail.
    #[must_use]
    pub fn producer_state_snapshot(&self) -> Vec<ProducerSnapshotEntry> {
        self.producer_state.values().copied().collect()
    }

    /// Every producer's state with the batches it appended before its last
    /// one: [`Self::producer_state_snapshot`] plus the retained batches that
    /// Kafka's `ProducerStateEntry` keeps for duplicate detection. After a
    /// reopen they are the batches the replay past the loaded snapshot
    /// rebuilt.
    #[must_use]
    pub fn recovered_producers(&self) -> Vec<RecoveredProducer> {
        self.producer_state
            .values()
            .map(|entry| RecoveredProducer {
                entry: *entry,
                earlier: self
                    .earlier_batches
                    .get(&entry.producer_id)
                    .map(|earlier| earlier.iter().copied().collect())
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// Close all segments, first taking a producer-state snapshot at the log
    /// end as Kafka's `UnifiedLog.close` does. A snapshot that cannot be
    /// written is not an error: the next open replays the log instead.
    pub fn close(mut self) {
        if let Err(error) = self.take_producer_snapshot() {
            tracing::warn!(
                dir = %self.dir.display(),
                %error,
                "producer-state snapshot on close failed; the next open replays the log"
            );
        }
        drop(self);
    }

    /// Atomically swap the active `LogConfig`.
    ///
    /// The next retention or roll check reads the new value. In-flight
    /// `append` calls hold the lock for very short windows and will not see
    /// a half-applied config.
    ///
    /// Callers can use this method through `&self`. The `Arc<RwLock<…>>`
    /// wrapper permits mutation of the inner value without an exclusive
    /// borrow on the `Log`.
    ///
    /// Delivery settings apply to subsequent watermark advances. Changing
    /// clock uncertainty does not retract the already published delivery
    /// prefix; see [`LogConfig::delivery_clock_uncertainty`].
    /// # Panics
    /// Panics when another thread poisoned the log configuration lock.
    pub fn set_config(&self, new: LogConfig) {
        *self.config.write().unwrap() = new;
    }

    /// Snapshot the current config. This allocates a clone, which is cheap
    /// because `LogConfig` is small and `Clone`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn config_snapshot(&self) -> LogConfig {
        self.config.read().unwrap().clone()
    }

    /// Replace the log's I/O implementation for fault-injection tests.
    ///
    /// This reaches every durable write the log makes: the active `.log`, both
    /// sparse indexes of every segment, the `.stampindex` sidecars, the
    /// leader-epoch checkpoint, and -- through `self.io`, which the free
    /// functions take -- the producer snapshots, the compaction swap, and the
    /// segment deletions retention performs.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_io(&mut self, io: std::sync::Arc<dyn LogIo>) {
        self.io = io.clone();
        for segment in &mut self.segments {
            segment.set_io(io.clone());
        }
        if let Some(active) = &mut self.active {
            active.set_io(io.clone());
        }
        for index in self.stamp_indexes.values_mut() {
            index.set_io(io.clone());
        }
        self.epoch_checkpoint.set_io(io);
    }

    /// Return all aborted transactions whose offset range overlaps
    /// `[start, end)`, including entries in sealed segments.
    #[must_use]
    pub fn aborted_in_range(&self, start: Offset, end: Offset) -> Vec<AbortedTxn> {
        let mut aborted = Vec::new();
        if let Some(first_base) = self
            .segments
            .iter()
            .find(|segment| segment.last_offset() >= start)
            .map(Segment::base_offset)
        {
            for index in self
                .sealed_txn_indexes
                .range(first_base..)
                .map(|(_, index)| index)
            {
                aborted.extend(index.aborted_in_range(start, end).copied());
            }
        }
        aborted.extend(self.active_txn_index.aborted_in_range(start, end).copied());
        aborted
    }

    /// Access the per-partition leader-epoch checkpoint.
    #[must_use]
    pub fn epoch_checkpoint(&self) -> &LeaderEpochCheckpoint {
        &self.epoch_checkpoint
    }

    /// Kafka's `UnifiedLog.assignEpochStartOffset`: record that `epoch`
    /// starts at `start_offset` through
    /// [`LeaderEpochCheckpoint::assign`]. `Partition.makeLeader` calls it with
    /// the log end offset when a replica takes a new leader epoch, so the
    /// leader can place a follower's `last_fetched_epoch` before it has
    /// written anything in that epoch.
    ///
    /// # Errors
    /// Returns an error for a negative epoch or offset, or when the
    /// checkpoint cannot be persisted.
    pub fn assign_epoch_start_offset(
        &mut self,
        epoch: krabka_ids::LeaderEpoch,
        start_offset: Offset,
    ) -> Result<(), LogError> {
        self.epoch_checkpoint.assign(epoch, start_offset)
    }

    /// Reconcile append-at offset assignment to an external next-offset frontier.
    ///
    /// Diskless partitions use the `KRaft` metadata log as the offset authority.
    /// After a crash, `KRaft` may have committed a next-offset that is ahead of the
    /// recovered local WAL tail. In that case the gap is intentional: the caller
    /// sets this frontier and the next append-at must use it instead of the local
    /// LEO. Classic logs never call this method and keep the default frontier 0.
    pub fn reconcile_next_offset(&mut self, frontier: Offset) {
        self.reconciled_frontier = self.reconciled_frontier.max(frontier);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use krabka_units::prelude::{kibibytes, minutes};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        log::test_support::{
            append_samples, append_transaction, rolled_sample_log, sample_batch,
            sample_batch_with_epoch, test_log, tiny_segments,
        },
        test_support::open_log,
    };

    /// A hard reset leaves the log empty at the new base, with the last stable
    /// offset there too.
    ///
    /// `lso` is derived from the fresh segment rather than from the base it was
    /// asked for, and it gates what a `read_committed` consumer may see -- one
    /// short of the base would expose an offset the log does not have.
    #[test]
    fn a_reset_puts_the_stable_offset_at_the_new_base() {
        let (_dir, mut log) = test_log();
        append_samples(&mut log, 3, 2);
        check!(log.log_end_offset() == Offset(6));

        log.reset_to(Offset(50)).expect("reset");
        check!(
            log.log_start_offset() == Offset(50),
            "starts at the new base"
        );
        check!(log.log_end_offset() == Offset(50), "and is empty there");
        check!(
            log.lso() == Offset(50),
            "the stable offset is the base, got {:?}",
            log.lso()
        );
    }

    /// Zero is a legal log start; only a negative one is rejected.
    #[test]
    fn the_log_start_may_be_set_to_zero_but_not_below() {
        let (_dir, mut log) = test_log();
        check!(
            log.set_log_start_offset(Offset(0)).is_ok(),
            "zero is a real offset"
        );
        check!(log.set_log_start_offset(Offset(7)).is_ok());
        check!(
            log.set_log_start_offset(Offset(-1)).is_err(),
            "negative is not"
        );
    }

    /// Kafka's `maybeIncrementLogStartOffset` → `onLogStartOffsetIncremented`:
    /// a complete transaction whose marker falls below the new log start
    /// stops holding the last stable offset, and one whose marker the start
    /// only reaches keeps holding it. Producer 42 writes a transaction at
    /// offset 0 and commits it at offset 1, the high watermark never passes
    /// the marker, and three plain records follow at offsets 2 to 4.
    #[test]
    fn raising_the_log_start_past_a_marker_releases_its_transaction() {
        use crate::log::test_support::commit_marker;

        for (name, new_start, released) in [
            ("the start reaches the marker", 1, false),
            ("the start passes the marker", 2, true),
        ] {
            let (_dir, mut log) = test_log();
            append_transaction(&mut log, (42, 0), &["a"]);
            log.append(&mut commit_marker(42, 0)).unwrap();
            log.append(&mut sample_batch(3)).unwrap();
            check!(log.lso() == Offset(0), "{name}: held before the move");

            log.set_log_start_offset(Offset(new_start)).unwrap();

            check!(
                (log.lso() == log.log_end_offset()) == released,
                "{name}: lso {:?}",
                log.lso()
            );
        }
    }

    /// Kafka's `maybeIncrementLogStartOffset` →
    /// `leaderEpochCache.truncateFromStart`: raising the start drops the
    /// epochs that end below it, the epoch that covers it starts there, and
    /// the trimmed history is what a reopen reads back. A start that does not
    /// move leaves the cache alone.
    #[test]
    fn raising_the_log_start_truncates_the_epoch_cache_from_the_start() {
        use crate::leader_epoch_checkpoint::EpochEntry;

        let (dir, mut log) = test_log();
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        log.append(&mut sample_batch_with_epoch(3, 2)).unwrap(); // epoch 2 @ 3
        log.append(&mut sample_batch_with_epoch(3, 4)).unwrap(); // epoch 4 @ 6

        log.set_log_start_offset(Offset(4)).unwrap();
        log.set_log_start_offset(Offset(2)).unwrap();

        let expected = [
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(4),
            },
            EpochEntry {
                epoch: LeaderEpoch(4),
                start_offset: Offset(6),
            },
        ];
        check!(log.epoch_checkpoint().entries() == &expected[..]);
        drop(log);
        let reopened = open_log(dir.path());
        check!(reopened.epoch_checkpoint().entries() == &expected[..]);
    }

    /// The epoch the remote tier's leader-epoch-cache cleanup may measure
    /// against is the earliest cache entry an advance of the log start put
    /// there (#1200). The segments a tiered partition still holds locally
    /// are not that: evicting them after the archive took them, and a restart
    /// on what is left, leave the whole epoch history in place and answer
    /// `None`, so the archive's older epochs are never mistaken for a
    /// truncated lineage. A `DeleteRecords`-style advance is one, and it is
    /// still one after a restart.
    #[test]
    fn the_log_start_epoch_comes_only_from_an_established_log_start() {
        let config = LogConfig {
            remote_storage_enable: true,
            ..tiny_segments()
        };
        // Three batches of three records, each rolling into its own segment:
        // epoch 1 at 0, epoch 2 at 3, epoch 4 at 6.
        let filled = |dir: &std::path::Path| {
            let mut log = Log::open(dir, config.clone()).unwrap();
            for epoch in [1, 2, 4] {
                log.append(&mut sample_batch_with_epoch(3, epoch)).unwrap();
            }
            log
        };

        // Local retention drops the segments the archive holds, and the
        // restart infers a log start from what is left.
        let evicted = tempdir().unwrap();
        let mut log = filled(evicted.path());
        check!(log.log_start_epoch() == None, "nobody moved the log start");
        check!(log.delete_local_segments_through(Offset(6)).unwrap() == 2);
        drop(log);
        let log = Log::open(evicted.path(), config.clone()).unwrap();
        check!(log.epoch_checkpoint().entries().len() == 3);
        check!(
            log.log_start_epoch() == None,
            "the restart inferred a start from the surviving segments"
        );

        // A start that was moved cuts the cache to the epoch that owns it, and
        // the restart keeps both.
        let moved = tempdir().unwrap();
        let mut log = filled(moved.path());
        log.set_log_start_offset(Offset(4)).unwrap();
        check!(log.log_start_epoch() == Some(LeaderEpoch(2)));
        drop(log);
        let log = Log::open(moved.path(), config).unwrap();
        check!(log.log_start_epoch() == Some(LeaderEpoch(2)));
    }

    /// The log's size is every segment's size added up, the sealed ones as
    /// well as the active one.
    #[test]
    fn log_size_sums_the_sealed_segments_and_the_active_one() {
        let dir = tempdir().unwrap();
        let log = rolled_sample_log(dir.path());

        let expected = log.segments.iter().map(Segment::size).fold(
            log.active.as_ref().map_or(ByteSize::ZERO, Segment::size),
            |a, b| a + b,
        );
        check!(
            log.size() == expected,
            "size {:?}, expected {:?}",
            log.size(),
            expected
        );
        check!(
            log.size() > kibibytes(1),
            "several segments should exceed one"
        );
    }

    #[test]
    fn dir_returns_open_path() {
        // The broker's KIP-113 move machinery reads this back to
        // determine a partition's current owning `log.dir` without
        // re-implementing the directory-layout convention.
        let (dir, log) = test_log();
        assert2::assert!(log.dir() == dir.path());
    }

    #[test]
    fn reset_to_clears_leader_epoch_checkpoint() {
        let (dir, mut log) = test_log();
        // A follower that replicated real data builds an epoch history.
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        log.append(&mut sample_batch_with_epoch(2, 2)).unwrap(); // epoch 2 @ 3
        log.append(&mut sample_batch_with_epoch(1, 5)).unwrap(); // epoch 5 @ 5
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(5)));

        // Hard reset to an empty log — the replicator's OFFSET_OUT_OF_RANGE
        // recovery path (Kafka's `truncateFullyAndStartAt`). The log now has
        // NO records, so it must advertise NO leader epoch. Otherwise the
        // follower keeps sending a stale `last_fetched_epoch` and the leader's
        // KIP-320 reconciliation serves a batch at a mismatched base offset,
        // looping forever on `append_at` (phantom ISR member → pinned HW →
        // acks=all stall).
        log.reset_to(Offset(0)).unwrap();

        assert2::assert!(log.epoch_checkpoint().latest_epoch() == None);
        assert2::assert!(log.epoch_checkpoint().entries() == &[][..]);
        // The cleared state must survive a reopen (a restarted broker re-reads
        // the on-disk checkpoint file).
        let reopened = open_log(dir.path());
        assert2::assert!(reopened.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn reset_to_nonzero_base_clears_all_epochs_not_just_tail() {
        // Guards against the subtly-wrong fix `truncate_from_end(new_base)`,
        // which retains an entry whose `start_offset < new_base` even though
        // the reset log holds no records below `new_base`.
        let (_dir, mut log) = test_log();
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
        log.reset_to(Offset(1000)).unwrap(); // empty log starting at 1000
        assert2::assert!(log.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn set_config_swaps_active_config() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(
            dir.path(),
            LogConfig {
                retention: Some(minutes(1)),
                ..LogConfig::default()
            },
        )
        .expect("open");
        log.set_config(LogConfig {
            retention: Some(minutes(2)),
            ..LogConfig::default()
        });
        assert2::assert!(log.config_snapshot().retention == Some(minutes(2)));
    }

    #[test]
    fn transaction_marker_state_and_producer_state_entry() {
        use crate::log::test_support::commit_marker;

        let (_dir, mut log) = test_log();
        let pid = ProducerId(42);
        assert2::assert!(log.transaction_marker_state(pid) == (-1, -1, false));
        assert2::assert!(log.producer_state_entry(pid).is_none());

        let mut batch = sample_batch(1);
        batch.producer_id = 42;
        batch.producer_epoch = 2;
        batch.base_sequence = 0;
        batch.attributes = batch.attributes.with_transactional(true);
        log.append(&mut batch).unwrap();

        assert2::assert!(log.transaction_marker_state(pid) == (2, -1, true));
        let entry = log
            .producer_state_entry(pid)
            .expect("producer state exists");
        assert2::assert!(entry.producer_id == 42);
        assert2::assert!(entry.producer_epoch == 2);

        let mut marker = commit_marker(42, 2);
        log.append(&mut marker).unwrap();
        assert2::assert!(log.transaction_marker_state(pid) == (2, 17, false));

        log.close();
    }
}
