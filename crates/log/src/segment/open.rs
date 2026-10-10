//! Constructors for a segment and the tail recovery that an active segment
//! needs when it is reopened.
//!
//! These are the three ways a [`Segment`] comes into existence -- a fresh
//! create, a no-scan open, and an active open that walks the trailing bytes --
//! together with the walk itself.

use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Arc,
};

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::{Segment, io::seek_to_log_size};
use crate::{
    error::LogError,
    index::{OffsetIndex, TimeIndex},
    io::FileIo,
    name,
};

/// One sparse-index point the tail scan found, as Kafka's `LogSegment.recover`
/// writes it: the offset index gets the indexed batch's last offset and its
/// byte position, and the time index gets the running maximum timestamp with
/// the last offset of the batch that set it.
struct IndexPoint {
    /// Last offset of the indexed batch, relative to the segment base.
    last_offset: u32,
    /// Byte position where the indexed batch starts.
    position: u32,
    /// Maximum timestamp of every batch up to and including the indexed one.
    max_timestamp: i64,
    /// Last offset of the batch that set `max_timestamp`, relative to the
    /// segment base.
    max_timestamp_offset: u32,
}

struct TailScan {
    valid_end: u64,
    last_offset: Offset,
    max_timestamp: i64,
    max_timestamp_offset: Offset,
    index_points: Vec<IndexPoint>,
}

impl Segment {
    /// Attach the sparse indexes and initial frontiers to an already-open log file.
    fn from_log_file(
        dir: &Path,
        base_offset: Offset,
        log_file: File,
        log_size: u64,
    ) -> Result<Self, LogError> {
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
            io: Arc::new(FileIo),
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            max_timestamp_offset: base_offset - 1,
            first_timestamp: None,
            last_offset: base_offset - 1,
            reserved_end: 0,
            reserve_requested: 0,
            direct: None,
            direct_asked: false,
        })
    }

    /// Create a fresh active segment at the given base offset. This fails if
    /// the `.log` file already exists.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn create(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        Self::from_log_file(dir, base_offset, log_file, 0)
    }

    /// Open as the active segment.
    ///
    /// When `validate` is true, this method scans from byte zero to EOF. It
    /// truncates a partial or invalid trailing batch, rebuilds both sparse
    /// indexes, and updates `last_offset` and `max_timestamp` from the same
    /// maximal valid prefix.
    #[instrument(
        level = "info",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0, validate),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open_active(dir: &Path, base_offset: Offset, validate: bool) -> Result<Self, LogError> {
        Self::open_active_with_index_interval(
            dir,
            base_offset,
            validate,
            crate::config::DEFAULT_INDEX_INTERVAL,
        )
    }

    pub(crate) fn open_active_with_index_interval(
        dir: &Path,
        base_offset: Offset,
        validate: bool,
        index_interval: ByteSize,
    ) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail(index_interval)?;
        }
        Ok(seg)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            base_offset = self.base_offset.0,
            log_size = self.log_size,
            recovered_last_offset = tracing::field::Empty,
        ),
        err,
    )]
    fn recover_active_tail(&mut self, index_interval: ByteSize) -> Result<(), LogError> {
        let recovered = self.scan_valid_tail(index_interval)?;
        u32::try_from(recovered.valid_end)
            .map_err(|_| LogError::Corrupt("recovered segment position exceeds u32".into()))?;
        krabka_verified::local_recovery_index_frontier(self.base_offset.0, recovered.last_offset.0)
            .ok_or_else(|| {
                LogError::Corrupt("recovered segment offset exceeds index range".into())
            })?;
        if recovered.valid_end < self.log_size {
            self.log_file.set_len(recovered.valid_end)?;
            self.renew_reservation();
        }
        self.log_size = recovered.valid_end;
        seek_to_log_size(&self.log_file, self.log_size)?;
        self.offset_index.truncate_by_position(0)?;
        self.time_index.truncate_by_relative_offset(0)?;
        for point in recovered.index_points {
            self.offset_index
                .append(point.last_offset, point.position)?;
            self.time_index
                .maybe_append(point.max_timestamp, point.max_timestamp_offset)?;
        }
        self.last_offset = recovered.last_offset;
        self.max_timestamp = recovered.max_timestamp;
        self.max_timestamp_offset = recovered.max_timestamp_offset;
        // Kafka's `LogSegment.recover` ends the same way: a segment that
        // closed normally always had its newest timestamp in the index.
        self.append_running_max_time_entry()?;
        tracing::Span::current().record("recovered_last_offset", self.last_offset.0);
        Ok(())
    }

    fn scan_valid_tail(&self, index_interval: ByteSize) -> Result<TailScan, LogError> {
        let mut buf = Vec::new();
        let to_read = usize::try_from(self.log_size).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

        let mut cur: &[u8] = &buf;
        let mut valid_end = 0;
        let mut next_offset = self.base_offset.0;
        let mut last_offset = self
            .base_offset
            .0
            .checked_sub(1)
            .map(Offset)
            .ok_or_else(|| LogError::Corrupt("recovery offset underflow".into()))?;
        let mut max_timestamp = i64::MIN;
        let mut max_timestamp_offset = last_offset;
        let mut index_points: Vec<IndexPoint> = Vec::new();
        while !cur.is_empty() {
            let batch_position = valid_end;
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let encoded_len = u64::try_from(before - cur.len())
                .map_err(|_| LogError::Corrupt("decoded batch length exceeds u64".into()))?;
            let Some(step) = krabka_verified::local_recovery_batch_step(
                valid_end,
                self.log_size,
                next_offset,
                batch.base_offset,
                batch.last_offset_delta,
                encoded_len,
            ) else {
                break;
            };
            valid_end = step.valid_end;
            last_offset = Offset(step.last_offset);
            next_offset = step.next_offset;
            if batch.max_timestamp > max_timestamp {
                max_timestamp = batch.max_timestamp;
                max_timestamp_offset = last_offset;
            }
            // Kafka's `LogSegment.recover`: `validBytes - lastIndexEntry >
            // indexIntervalBytes`, the last entry starting at zero.
            let last_indexed = index_points
                .last()
                .map_or(0, |previous| u64::from(previous.position));
            let should_index =
                batch_position.saturating_sub(last_indexed) > index_interval.bytes_u64();
            if should_index {
                let relative = |offset: Offset| {
                    krabka_verified::truncation_relative_offset(self.base_offset.0, offset.0)
                        .ok_or_else(|| {
                            LogError::Corrupt("recovered batch offset exceeds index range".into())
                        })
                };
                let position = u32::try_from(batch_position).map_err(|_| {
                    LogError::Corrupt("recovered batch position exceeds index range".into())
                })?;
                index_points.push(IndexPoint {
                    last_offset: relative(last_offset)?,
                    position,
                    max_timestamp,
                    max_timestamp_offset: relative(max_timestamp_offset)?,
                });
            }
        }
        Ok(TailScan {
            valid_end,
            last_offset,
            max_timestamp,
            max_timestamp_offset,
            index_points,
        })
    }

    /// Open an existing segment for reading. This is lightweight and does no
    /// full scan.
    ///
    /// The log and index files must already exist on disk. The segment starts
    /// with `last_offset = base_offset - 1` and `max_timestamp = i64::MIN`
    /// until tail recovery through [`Segment::open_active`] fills them in.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let metadata = log_file.metadata()?;
        let log_size = metadata.len();
        seek_to_log_size(&log_file, log_size)?;
        let mut segment = Self::from_log_file(dir, base_offset, log_file, log_size)?;
        // A reservation outlives the process that made it, and nothing on
        // disk records it but the blocks themselves. Count them, so sealing
        // this segment later gives back what an earlier run reserved.
        segment.reserved_end = allocated_bytes(&metadata);
        Ok(segment)
    }
}

/// Bytes of disk the file behind `metadata` occupies, which is past its
/// length when blocks are reserved beyond its end.
#[cfg(unix)]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::blocks(metadata).saturating_mul(512)
}

/// The targets without block counts cannot reserve blocks either.
#[cfg(not(unix))]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    let _ = metadata;
    0
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use krabka_units::prelude::kibibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, NO_LIMIT, sample_batch, seeded_segment};

    #[test]
    fn append_after_open_active_writes_at_eof() {
        let dir = tempdir().unwrap();
        drop(seeded_segment(
            dir.path(),
            crate::segment::test_support::SeededSegmentSetup {
                batches: &[
                    crate::segment::test_support::ONE_RECORD_BATCH,
                    crate::segment::test_support::SECOND_SINGLE_RECORD_BATCH,
                ],
                ..Default::default()
            },
        ));

        let mut seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        let position = seg
            .append(
                &sample_batch(crate::segment::test_support::THIRD_SINGLE_RECORD_BATCH),
                DENSE_INDEX,
            )
            .unwrap();

        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(position > 0);
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(read == crate::segment::test_support::three_single_record_batches());
    }

    /// Tail recovery must physically truncate a partial or garbage tail and
    /// rebuild both indexes from exactly the batches in the retained prefix.
    #[test]
    fn recover_active_tail_truncates_trailing_garbage() {
        let dir = tempdir().unwrap();
        let valid_size = {
            let mut seg = seeded_segment(
                dir.path(),
                crate::segment::test_support::SeededSegmentSetup {
                    batches: crate::segment::test_support::FIVE_OFFSET_TIMESTAMP_BATCHES,
                    ..Default::default()
                },
            );
            seg.flush().unwrap();
            let valid_size = seg.log_size;
            let stale_position = u32::try_from(valid_size).unwrap();
            seg.offset_index.append(999, stale_position).unwrap();
            seg.time_index.append(i64::MAX, 999).unwrap();
            valid_size
        };

        // Append 16 bytes of garbage (an undecodable partial batch tail).
        let log_path = name::log_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xCD; 16]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen with validation: the tail scan must clip the garbage.
        let seg =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(seg.log_size == valid_size);
        assert2::assert!(std::fs::metadata(&log_path).unwrap().len() == valid_size);
        // The first batch takes no entry (Kafka's `LogSegment.recover` indexes
        // a batch once more than the interval lies between it and the last
        // entry, which starts at zero), so the second batch's is the one.
        assert2::assert!(seg.offset_index.entry_count() == 1);
        assert2::assert!(seg.time_index.entry_count() == 1);
        assert2::assert!(u64::from(seg.offset_index.lookup(999)) < valid_size);
        assert2::assert!(seg.time_index.last_entry().unwrap().1 < 999);
        drop(seg);

        // Recovery is idempotent once the file and both indexes share the
        // proved frontiers.
        let retry =
            Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX)
                .unwrap();
        assert2::assert!(retry.last_offset() == Offset(4));
        assert2::assert!(retry.log_size == valid_size);
        assert2::assert!(std::fs::metadata(&log_path).unwrap().len() == valid_size);
        assert2::assert!(retry.offset_index.entry_count() == 1);
        assert2::assert!(retry.time_index.entry_count() == 1);
    }

    /// Recovery ends with Kafka's final time-index entry, `(maxTimestampSoFar,
    /// shallowOffsetOfMaxTimestampSoFar)`: the sparse index holds no point for
    /// these small batches, so without it the index would not know any of
    /// their timestamps.
    #[test]
    fn recover_active_tail_appends_the_final_time_index_entry() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            for (base, timestamp) in [(0, 100), (1, 300), (2, 200)] {
                seg.append(
                    &sample_batch(crate::segment::test_support::SampleBatchSetup {
                        offset: crate::Offset(base),
                        timestamp: crate::segment::test_support::RecordTimestamp(timestamp),
                        ..Default::default()
                    }),
                    kibibytes(4),
                )
                .unwrap();
            }
        }

        let seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();

        assert2::assert!(seg.time_index.entry_count() == 1);
        assert2::assert!(seg.time_index.last_entry() == Some((300, 1)));
    }

    #[test]
    fn unvalidated_open_reports_last_offset_as_base_minus_one() {
        let dir = tempdir().unwrap();
        Segment::create(dir.path(), Offset(10)).unwrap();
        let seg = Segment::open(dir.path(), Offset(10)).unwrap();
        assert2::assert!(seg.last_offset() == Offset(9));
    }
}
