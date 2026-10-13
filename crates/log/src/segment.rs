//! A single segment: the `.log`, `.index`, and `.timeindex` files that share
//! a base offset.

use {
    bytes::Bytes,
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt},
    std::{
        fs::File,
        path::{Path, PathBuf},
        sync::Arc,
    },
};

mod activation;
mod append;
mod direct;
mod direct_mode;
#[cfg(test)]
mod dump_log_tests;
mod header_walk;
mod io;
mod lifecycle;
mod open;
mod read;
mod read_raw;
#[krabka_macros::sendfile_cfg]
mod read_raw_desc;
#[cfg(test)]
mod test_support;
mod timestamp_scan;

pub(crate) use self::append::PlacedBatch;
use crate::{
    index::{OFFSET_ENTRY_SIZE, OffsetIndex, TIME_ENTRY_SIZE, TimeIndex},
    io::LogIo,
    name::{leader_epoch_checkpoint_path, stampindex_path, txnindex_path},
};

/// A single log segment: the `.log` data file paired with its sparse
/// `.index` (offset → byte position) and `.timeindex` (timestamp →
/// relative offset) sidecars.
///
/// The `base_offset` identifies a segment. It is the absolute offset of the
/// segment's first record, encoded into the segment's 20-digit zero-padded
/// filename. [`Segment::create`] makes a new active segment.
/// [`Segment::open`] validates and opens a sealed segment for reading, and
/// [`Segment::open_active`] opens an active segment with tail recovery.
#[derive(Debug)]
pub struct Segment {
    dir: PathBuf,
    base_offset: Offset,
    /// The `.log` data file, wrapped in `Arc`.
    ///
    /// The `Arc` lets the zero-copy fetch path (Increment D) hand a
    /// `FileRegion { file: Arc<File>, .. }` to the connection's async
    /// `sendfile` loop. The `Arc` pins the inode through the send even when
    /// retention rolls or removes this segment in the meantime, because the
    /// open fd keeps the inode alive on Unix. Writes and data syncs go through
    /// `io`, which normally delegates straight to this file.
    log_file: Arc<File>,
    io: Arc<dyn LogIo>,
    log_size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    /// `true` once a new segment has started after this one. Sealed segments
    /// do not accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset of the first batch that carried `max_timestamp`:
    /// Kafka's `shallowOffsetOfMaxTimestampSoFar`. The time index pairs the
    /// running maximum timestamp with this offset.
    max_timestamp_offset: Offset,
    /// First record's timestamp, cached for append-time segment rolling.
    /// Truncation clears it so a replacement first batch gets a fresh clock.
    first_timestamp: Option<i64>,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: Offset,
    /// End of the disk blocks the `.log` file may hold past `log_size`:
    /// those a [`Segment::reserve`] reserved, or, for a segment opened from
    /// disk, those the filesystem reports allocated. Sealing gives back the
    /// range `log_size..reserved_end`. It is `0` when there is nothing to
    /// give back.
    reserved_end: u64,
    /// The end the last [`Segment::reserve`] asked for, granted or not, so
    /// a filesystem that refuses is asked once per segment rather than once
    /// per append. `0` when nothing has been asked.
    reserve_requested: u64,
    /// The `O_DIRECT` writer and the newest bytes it wrote, while this
    /// segment writes through `O_DIRECT`. See [`direct_mode`]. Boxed: every
    /// sealed segment carries the field, and a log is held across `await`s.
    direct: Option<Box<direct_mode::DirectMode>>,
    /// Whether [`Segment::write_direct`] has asked for an `O_DIRECT` handle
    /// since the segment last wrote through the page cache, granted or not.
    direct_asked: bool,
}

/// Verbatim, decode-free output of [`Segment::read_raw`].
#[derive(Debug, Clone)]
pub struct RawSegmentRead {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: Offset,
    /// Last absolute offset covered by `bytes` (`start_offset - 1` if empty).
    pub last_offset: Offset,
    /// Verbatim `.log` bytes: one or more complete v2 batches.
    pub bytes: Bytes,
}

#[krabka_macros::sendfile_cfg]
/// Descriptor form of [`Segment::read_raw`] for the zero-copy fetch path,
/// Increments D + E.
///
/// This type carries the same offset and boundary metadata, but the
/// records run is a [`krabka_protocol::records::FileRegion`] descriptor of
/// the form `(Arc<File>, offset, len)`, not an owned `Bytes` slice. The
/// broker can therefore `sendfile(2)` the run straight from the page cache
/// with no userspace copy. This type is compiled on the SENDFILE alias:
/// Linux, Apple, and FreeBSD/DragonFly.
#[derive(Debug, Clone)]
pub struct RawSegmentDesc {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: Offset,
    /// Last absolute offset covered by the region (`start_offset - 1` if empty).
    pub last_offset: Offset,
    /// The records run, as a file-backed descriptor. `None` when the walk
    /// found no complete batch in range.
    pub region: Option<krabka_protocol::records::FileRegion>,
}

#[krabka_macros::sendfile_cfg]
impl RawSegmentDesc {
    fn empty() -> Self {
        Self {
            start_offset: Offset(0),
            last_offset: Offset(-1),
            region: None,
        }
    }

    /// Byte length of the region. It is 0 when the region is empty.
    #[must_use]
    pub fn len(&self) -> usize { self.region.as_ref().map_or(0, |r| r.len) }

    /// `true` when no batch bytes were described.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.region.is_none() }
}

impl RawSegmentRead {
    // cargo-mutants: the `last_offset` of an empty read is a never-read sentinel: every
    // consumer guards on `is_empty()` (which checks `bytes`) before touching
    // `last_offset`, so `Offset(-1)` vs `Offset(1)` is unobservable.
    #[cfg_attr(test, mutants::skip)]
    fn empty() -> Self {
        Self {
            start_offset: Offset(0),
            last_offset: Offset(-1),
            bytes: Bytes::new(),
        }
    }

    /// `true` when no batch bytes were returned.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.bytes.is_empty() }
}

/// What an activation walk found in one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActivationScan {
    /// Exclusive end of the run of active batches that begins at the offset
    /// the walk started from. It equals that offset when the first batch is
    /// already waiting.
    pub active_end: Offset,
    /// Activation time of the first batch that is not active yet, or `None`
    /// when every batch from the starting offset to the end of this segment
    /// is active.
    pub pending_at: Option<i64>,
}

impl Segment {
    /// Absolute offset of the first record this segment can hold.
    #[must_use]
    pub fn base_offset(&self) -> Offset { self.base_offset }

    /// Path to this segment's `.txnindex` file (may not exist yet).
    #[must_use]
    pub fn txn_index_path(&self) -> std::path::PathBuf {
        txnindex_path(&self.dir, self.base_offset.0)
    }

    /// Path to this segment's `.stampindex` sidecar (may not exist yet).
    #[must_use]
    pub fn stamp_index_path(&self) -> std::path::PathBuf {
        stampindex_path(&self.dir, self.base_offset.0)
    }

    /// Path to the per-partition `.leader-epoch-checkpoint` file in this
    /// segment's directory. All segments in a partition share the checkpoint,
    /// and epoch history accumulates over the log's lifetime.
    #[must_use]
    pub fn leader_epoch_checkpoint_path(&self) -> std::path::PathBuf {
        leader_epoch_checkpoint_path(&self.dir)
    }

    /// Highest absolute offset (inclusive) of any batch appended to this
    /// segment. Returns `base_offset - 1` for an empty segment.
    #[must_use]
    pub fn last_offset(&self) -> Offset { self.last_offset }

    /// Current `.log` file size.
    ///
    /// The field itself stays a raw `u64`, because it is a file position and
    /// goes directly into a `pread` or `seek` argument. It converts here,
    /// where the value becomes a magnitude that the roll and retention
    /// policies compare.
    #[must_use]
    pub fn size(&self) -> ByteSize { ByteSize::from_bytes(self.log_size) }

    /// Whether the offset index or the time index is full under a
    /// `segment.index.bytes` of `segment_index_size`, Kafka's
    /// `offsetIndex().isFull() || timeIndex().isFull()`.
    ///
    /// Kafka sizes each index file to `segment.index.bytes` rounded down to
    /// whole entries, so the offset index holds `size / 8` entries and the
    /// time index `size / 12`. Kafka's `TimeIndex.isFull` keeps the last
    /// slot free for the entry that sealing the segment writes, so the time
    /// index is full one entry early.
    #[must_use]
    pub(crate) fn indexes_full(&self, segment_index_size: ByteSize) -> bool {
        self.indexes_full_after(segment_index_size, 0)
    }

    /// Check capacity with at most one new entry in each index per batch.
    pub(crate) fn indexes_full_after(&self, segment_index_size: ByteSize, batches: usize) -> bool {
        let max_entries = |entry_size: usize| {
            segment_index_size.bytes_u64() / u64::try_from(entry_size).unwrap_or(u64::MAX)
        };
        let entries =
            |count: usize| u64::try_from(count.saturating_add(batches)).unwrap_or(u64::MAX);
        entries(self.offset_index.entry_count()) >= max_entries(OFFSET_ENTRY_SIZE)
            || entries(self.time_index.entry_count()).saturating_add(1)
                >= max_entries(TIME_ENTRY_SIZE)
    }

    /// Highest timestamp observed across all batches in this segment.
    /// Returns `i64::MIN` for an empty segment, and can also read `-1`
    /// (`NO_TIMESTAMP`) when every batch in the segment carries no
    /// timestamp. See [`Segment::last_modified`] for Kafka's fallback for
    /// both cases.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 { self.max_timestamp }

    /// The `.log` file's last-modified time, as Kafka's `LogSegment.
    /// lastModified` reads it. Time retention falls back to this when the
    /// segment carries no record timestamp of its own (`max_timestamp()` is
    /// `i64::MIN` or `-1`), the same as `UnifiedLog`'s
    /// `largestRecordTimestamp().orElse(lastModified())`.
    ///
    /// `None` when the filesystem cannot report it, which retention treats
    /// as "unknown, not overdue" rather than as license to guess.
    #[must_use]
    pub fn last_modified(&self) -> Option<std::time::SystemTime> {
        self.log_file.metadata().ok()?.modified().ok()
    }

    /// `true` once [`Segment::seal`] has sealed the segment. Sealed segments
    /// reject appends.
    #[must_use]
    pub fn is_sealed(&self) -> bool { self.sealed }

    /// Directory that holds this segment's `.log`, `.index`, and `.timeindex`
    /// files. The compactor uses it to read the `.log` file directly and to
    /// avoid the `Segment::read` path. That path depends on the in-memory
    /// `last_offset`, which is stale for sealed segments loaded through
    /// the log's no-scan loader.
    #[must_use]
    pub fn dir(&self) -> &Path { &self.dir }
}
