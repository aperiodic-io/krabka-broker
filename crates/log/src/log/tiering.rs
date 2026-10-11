//! Tiered storage (KIP-405): describing sealed segments for offload, rolling
//! the active segment when local retention asks for it, and dropping the local
//! copies once they are safely remote.
//!
//! `Log` enforces no tiered-storage invariant of its own. It reports what
//! a `RemoteLogManager` needs, rolls when that manager tells it to roll, and
//! deletes what that manager tells it to delete.

use std::path::{Path, PathBuf};

use krabka_ids::{LeaderEpoch, Offset};
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};
use tracing::instrument;

use super::Log;
use crate::{
    error::LogError, leader_epoch_checkpoint::EpochEntry, name, producer_snapshot, retention,
    segment::Segment,
};

/// A sealed segment described for tiered-storage offload (KIP-405).
///
/// It carries the on-disk file paths, the offset, timestamp, and size
/// metadata, and the leader-epoch ranges that a `RemoteLogManager` needs to
/// build remote-segment metadata. [`Log::tierable_segments`] produces these
/// values.
// No `Eq`: `size` is a `ByteSize`, which stores `f64`. The derive was unused —
// `SegmentExport` is never hashed nor used as a map key.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentExport {
    /// First absolute offset in the segment.
    pub base_offset: Offset,
    /// Last absolute offset (inclusive) in the segment.
    pub last_offset: Offset,
    /// Kafka's `LogSegment.largestTimestamp()`, which is what Kafka's copy
    /// records as the remote segment's `maxTimestampMs`: the highest record
    /// timestamp in the segment, or [`SegmentExport::last_modified_ms`] when
    /// no record carries a non-negative one.
    pub max_timestamp: i64,
    /// Kafka's `LogSegment.lastModified()`: the `.log` file's modification
    /// time in epoch milliseconds, or `0` when the filesystem cannot say, as
    /// `File.lastModified()` answers. Kafka's tiered local retention ages a
    /// segment whose records claim a future timestamp by this instead.
    pub last_modified_ms: i64,
    /// `.log` file size.
    pub size: ByteSize,
    /// Path to the `.log` data file.
    pub log_path: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index_path: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index_path: PathBuf,
    /// Path to the `.txnindex` file, present only when it exists on disk.
    pub transaction_index_path: Option<PathBuf>,
    /// Producer-state snapshot at `last_offset + 1`.
    pub producer_snapshot_path: PathBuf,
    /// Leader epochs whose coverage overlaps `[base_offset, last_offset]`,
    /// as `(epoch, start_offset)` clamped to `base_offset`, ordered by
    /// offset. May be empty when no epochs were recorded for this log.
    pub leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

/// The active segment as tiered local retention measures it (KIP-405).
///
/// Kafka's `UnifiedLog.deletableSegments` walks a tiered log's active segment
/// last. When the retention predicate holds for it, Kafka rolls it, so that
/// the next copy can upload its records and the next retention pass can drop
/// them from local disk. This value carries what that predicate reads.
/// [`Log::active_segment_export`] produces it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActiveSegmentExport {
    /// First absolute offset in the segment.
    pub base_offset: Offset,
    /// Kafka's `LogSegment.largestTimestamp()`, as in
    /// [`SegmentExport::max_timestamp`].
    pub max_timestamp: i64,
    /// Kafka's `LogSegment.lastModified()`, as in
    /// [`SegmentExport::last_modified_ms`].
    pub last_modified_ms: i64,
    /// `.log` file size.
    pub size: ByteSize,
}

/// Leader epochs whose coverage `[start_e, start_{e+1})` overlaps the
/// segment range `[base, last]`, returned as `(epoch, start_offset)` with
/// the start clamped up to `base` and ordered by offset. An epoch with no
/// recorded entries yields an empty result.
///
/// `sorted` must be ordered by `start_offset` ascending (the caller sorts
/// once and reuses the slice across segments).
fn epochs_for_range(
    sorted: &[EpochEntry],
    base: Offset,
    last: Offset,
) -> Vec<(LeaderEpoch, Offset)> {
    let mut out = Vec::new();
    for (i, e) in sorted.iter().enumerate() {
        // Coverage of this epoch is [start_offset, next.start_offset).
        let end = sorted
            .get(i + 1)
            .map_or(Offset(i64::MAX), |n| n.start_offset);
        if e.start_offset <= last && end > base {
            out.push((e.epoch, e.start_offset.max(base)));
        }
    }
    out
}

/// Kafka's `LogSegment.lastModified()` for the file at `path`: its
/// modification time in epoch milliseconds, or `0` when it cannot be read,
/// which is what `java.io.File.lastModified()` returns for an I/O error.
fn last_modified_ms(path: &Path) -> i64 {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .map_or(0, retention::now_ms)
}

impl Log {
    /// Pair each sealed segment with the exclusive end supplied by its successor.
    pub(super) fn sealed_segments_with_next_base(
        &self,
    ) -> impl Iterator<Item = (&Segment, Offset)> {
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        self.segments.iter().zip(
            self.segments
                .iter()
                .skip(1)
                .map(Segment::base_offset)
                .chain(std::iter::once(active_base)),
        )
    }

    /// Kafka's `LogSegment.largestTimestamp()`: the segment's highest record
    /// timestamp when one is non-negative, and otherwise its `.log` file's
    /// modification time. Retention ages a segment by this, so a segment
    /// whose records carry no timestamp still expires once its file is old,
    /// and an empty one is aged by when it was last written.
    pub(super) fn largest_timestamp(&self, segment: &Segment) -> i64 {
        let max_timestamp = segment.max_timestamp();
        if max_timestamp >= 0 {
            max_timestamp
        } else {
            last_modified_ms(&name::log_path(&self.dir, segment.base_offset().0))
        }
    }

    /// First absolute offset still readable from this broker's local disk
    /// (KIP-405): Kafka's `localLogStartOffset`.
    ///
    /// It is the base offset of the oldest segment on disk, raised to the
    /// global [`Log::log_start_offset`] when a `DeleteRecords` floor sits
    /// above the files that are still there. Dropping a local segment whose
    /// copy is in the remote tier moves this pointer and leaves the global one
    /// where it was, so the offsets in `[log_start_offset(),
    /// local_log_start_offset())` are exactly the ones the remote tier serves.
    #[must_use]
    pub fn local_log_start_offset(&self) -> Offset {
        self.first_local_offset().max(self.log_start_offset())
    }

    /// Delete every sealed segment whose `last_offset < target` from disk
    /// (KIP-405).
    ///
    /// Only [`Log::local_log_start_offset`] moves. The global
    /// [`Log::log_start_offset`] stays where it was, because the records are
    /// still in the remote tier and a fetch for them must reach it rather than
    /// answer `OFFSET_OUT_OF_RANGE`. Kafka splits the two floors the same way:
    /// `local.retention.*` moves `localLogStartOffset` and leaves
    /// `logStartOffset` to `DeleteRecords` and to remote-segment deletion.
    ///
    /// This method never touches the active segment. The producer snapshot at
    /// each removed segment's base goes with it. It returns the number of
    /// segments removed. It does nothing and returns `Ok(0)` when
    /// `target <= local_log_start_offset()`.
    ///
    /// The caller must confirm that these segments are safely in the remote
    /// tier (`CopySegmentFinished`) before it calls this method. `Log`
    /// enforces no tiered-storage invariants. See
    /// `crates/broker/src/remote_log_manager.rs` for the production caller.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `target` is negative.
    #[instrument(
        level = "info",
        skip(self),
        fields(removed = tracing::field::Empty),
        err,
    )]
    pub fn delete_local_segments_through(&mut self, target: Offset) -> Result<usize, LogError> {
        self.rollover_flusher.finish()?;
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "delete_local_segments_through: target must be >= 0".into(),
            ));
        }
        if target <= self.local_log_start_offset() {
            return Ok(0);
        }

        let to_drop: Vec<Offset> = self
            .sealed_segments_with_next_base()
            .filter(|(_, next_base)| *next_base - 1 < target)
            .map(|(segment, _)| segment.base_offset())
            .collect();

        let removed = to_drop.len();
        tracing::Span::current().record("removed", removed);
        self.remove_sealed_segments(&to_drop)?;

        Ok(removed)
    }

    /// Describe the active segment for tiered local retention (KIP-405), or
    /// return `None` when the log has no active segment.
    ///
    /// `max_timestamp` is Kafka's `largestTimestamp()`, so an empty segment
    /// and a segment whose records carry no timestamp both report the `.log`
    /// file's modification time.
    #[must_use]
    pub fn active_segment_export(&self) -> Option<ActiveSegmentExport> {
        self.active.as_ref().map(|segment| {
            let base_offset = segment.base_offset();
            ActiveSegmentExport {
                base_offset,
                max_timestamp: self.largest_timestamp(segment),
                last_modified_ms: last_modified_ms(&name::log_path(&self.dir, base_offset.0)),
                size: segment.size(),
            }
        })
    }

    /// Kafka's `UnifiedLog.roll()`, as tiered local retention calls it: seal
    /// the active segment and open an empty one at the log end.
    ///
    /// Kafka's `deletableSegments` rolls a tiered log's active segment once
    /// it breaches `local.retention.ms` or `local.retention.bytes`. The
    /// remote tier never holds the active segment, so without the roll an
    /// idle partition keeps its newest records on local disk until
    /// `segment.ms` or `segment.bytes` rolls the segment.
    ///
    /// Returns `true` when the log rolled. It does nothing and returns
    /// `false` when there is no active segment or the active segment is
    /// empty: an empty segment holds nothing to copy, and the new segment
    /// would open at the same base offset.
    ///
    /// # Errors
    ///
    /// Returns an error when the flush, the producer snapshot, the seal, or
    /// the new segment's files fail.
    pub fn roll(&mut self) -> Result<bool, LogError> {
        if self
            .active
            .as_ref()
            .is_none_or(|segment| segment.size() == ByteSize::ZERO)
        {
            return Ok(false);
        }
        self.roll_active_segment()?;
        Ok(true)
    }

    /// Describe every sealed segment for tiered-storage offload (KIP-405).
    ///
    /// The result includes sealed segments after their rollover flush has
    /// published the boundary snapshot. It never includes the active segment.
    ///
    /// `last_offset` comes from the next segment's `base_offset`. For the
    /// most-recent sealed segment it comes from the active segment's base.
    /// The value is therefore correct even for segments loaded from disk
    /// without a tail scan. `max_timestamp` is Kafka's `largestTimestamp()`,
    /// so a segment with no record timestamp reports its file's
    /// modification time rather than an unknown.
    #[must_use]
    pub fn tierable_segments(&self) -> Vec<SegmentExport> {
        // Sort the epoch entries once here rather than per-segment inside
        // `epochs_for_range`.
        let mut epoch_entries = self.epoch_checkpoint.entries().to_vec();
        epoch_entries.sort_by_key(|e| e.start_offset);
        self.sealed_segments_with_next_base()
            // A rollover's snapshot appears only after its records and indexes
            // have flushed. Pending segments stay local until then.
            .filter(|(_, next_base)| producer_snapshot::path(&self.dir, *next_base).exists())
            .map(|(seg, next_base)| {
                let base = seg.base_offset();
                let last = next_base - 1;
                let txn = name::txnindex_path(&self.dir, base.0);
                let log_path = name::log_path(&self.dir, base.0);
                SegmentExport {
                    base_offset: base,
                    last_offset: last,
                    max_timestamp: self.largest_timestamp(seg),
                    last_modified_ms: last_modified_ms(&log_path),
                    size: seg.size(),
                    log_path,
                    offset_index_path: name::index_path(&self.dir, base.0),
                    time_index_path: name::timeindex_path(&self.dir, base.0),
                    transaction_index_path: txn.exists().then_some(txn),
                    producer_snapshot_path: producer_snapshot::path(&self.dir, next_base),
                    leader_epochs: epochs_for_range(&epoch_entries, base, last),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
