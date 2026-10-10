//! State transitions of a segment after it holds data: sealing, flushing, and
//! truncation.
//!
//! Each one rewrites the segment's own view of what it holds -- the sealed
//! flag, the byte length, the last offset, the maximum timestamp, and the
//! sparse indexes -- so they belong together rather than beside a read path.

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};
use tracing::instrument;

use super::{Segment, io::seek_to_log_size};
use crate::error::LogError;

/// Log a block reservation, a release, or a switch to `O_DIRECT` that the
/// filesystem refused.
///
/// A filesystem without `fallocate` or `O_DIRECT` refuses every one, so that
/// is not news worth more than a debug line; anything else, a full disk above
/// all, is.
pub(super) fn log_refused(operation: &'static str, base_offset: Offset, error: &std::io::Error) {
    if error.kind() == std::io::ErrorKind::Unsupported {
        tracing::debug!(operation, base_offset = base_offset.0, %error, "segment preallocation unsupported");
    } else {
        tracing::warn!(operation, base_offset = base_offset.0, %error, "segment preallocation refused");
    }
}

/// What [`Segment::write_snapshot`] saves for [`Segment::rollback_failed_write`].
#[derive(Debug, Clone, Copy)]
pub(super) struct WriteSnapshot {
    last_offset: Offset,
    max_timestamp: i64,
    max_timestamp_offset: Offset,
}

#[cfg(all(test, not(target_os = "wasi")))]
std::thread_local! {
    static FAIL_FLUSH_HANDLES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Segment {
    #[cfg(all(test, not(target_os = "wasi")))]
    pub(crate) fn test_fail_flush_handles(fail: bool) {
        FAIL_FLUSH_HANDLES.set(fail);
    }

    #[cfg(not(target_os = "wasi"))]
    pub(crate) fn flush_handles(&self) -> std::io::Result<[std::sync::Arc<std::fs::File>; 3]> {
        #[cfg(test)]
        if FAIL_FLUSH_HANDLES.get() {
            return Err(std::io::Error::other("injected flush handle clone failure"));
        }
        Ok([
            std::sync::Arc::clone(&self.log_file),
            std::sync::Arc::new(self.offset_index.flush_handle()?),
            std::sync::Arc::new(self.time_index.flush_handle()?),
        ])
    }

    /// Mark this segment as sealed. No more appends.
    ///
    /// Sealing first writes the segment's final time-index entry, Kafka's
    /// `LogSegment.onBecomeInactiveSegment`: the sparse index lags the writes,
    /// so without it a reopened segment would not learn the timestamp of the
    /// batches after its last index point from the index. A segment that
    /// wrote through `O_DIRECT` then goes back to the page cache, which cuts
    /// the padding past its last batch, and what is left of its block
    /// reservation is given back.
    ///
    /// # Errors
    /// Returns an error when the time-index entry cannot be written or its
    /// offset overflows the index range, or when the padding cannot be cut.
    /// The segment stays open then.
    pub fn seal(&mut self) -> Result<(), LogError> {
        self.append_running_max_time_entry()?;
        self.write_buffered()?;
        self.sealed = true;
        self.release_reservation();
        Ok(())
    }

    /// Reserve disk blocks for this segment to grow to `size` without
    /// allocating as it goes: Kafka's `preallocate`, with the file's length
    /// left at the bytes it holds. See
    /// [`SegmentAllocation::Preallocate`](crate::SegmentAllocation::Preallocate).
    ///
    /// The log calls this before every append, so it does nothing once the
    /// segment holds, or has asked for, a reservation reaching `size`. A
    /// refused reservation leaves a segment that grows as it is written,
    /// which is the segment it would have been without one, so it is logged
    /// and not returned, and not asked for again.
    pub(crate) fn reserve(&mut self, size: ByteSize) {
        let end = size.bytes_u64();
        if end
            <= self
                .log_size
                .max(self.reserved_end)
                .max(self.reserve_requested)
        {
            return;
        }
        self.reserve_requested = end;
        match self
            .io
            .reserve(&self.log_file, self.log_size, end - self.log_size)
        {
            Ok(()) => self.reserved_end = end,
            Err(error) => log_refused("reserve", self.base_offset, &error),
        }
    }

    /// Take the reservation again after a truncate.
    ///
    /// A truncate frees every block past the new end, the reservation's with
    /// them -- it is how [`Self::release_reservation`] gives them back -- so
    /// the blocks counted before it are gone, and a reservation this process
    /// asked for has to be asked for again.
    pub(super) fn renew_reservation(&mut self) {
        self.reserved_end = 0;
        let requested = std::mem::take(&mut self.reserve_requested);
        if requested > 0 {
            self.reserve(ByteSize::from_bytes(requested));
        }
    }

    /// Give back the blocks reserved past the bytes this segment holds.
    ///
    /// A segment sealed before it filled -- by `segment.ms`, a full index, or
    /// a batch that did not fit -- would otherwise keep its reservation for as
    /// long as retention keeps the segment. A refused release keeps them only
    /// until then, so it is logged and not returned.
    fn release_reservation(&mut self) {
        let reserved_end = std::mem::take(&mut self.reserved_end);
        if reserved_end <= self.log_size {
            return;
        }
        if let Err(error) = self.io.release(&self.log_file, self.log_size) {
            log_refused("release", self.base_offset, &error);
        }
    }

    /// Kafka's `timeIndex().maybeAppend(maxTimestampSoFar(),
    /// shallowOffsetOfMaxTimestampSoFar())`: the running maximum timestamp
    /// with the last offset of the batch that set it. The index keeps only an
    /// entry whose timestamp is newer than its last, so it stays strictly
    /// increasing, and a segment without a timestamp (`i64::MIN`, or
    /// `NO_TIMESTAMP`) adds nothing.
    pub(super) fn append_running_max_time_entry(&mut self) -> Result<(), LogError> {
        if self.max_timestamp < 0 {
            return Ok(());
        }
        let relative = krabka_verified::truncation_relative_offset(
            self.base_offset.0,
            self.max_timestamp_offset.0,
        )
        .ok_or_else(|| LogError::BadSegmentName("offset overflow in segment".into()))?;
        self.time_index.maybe_append(self.max_timestamp, relative)
    }

    /// Seal a segment loaded through the no-scan [`Segment::open`] path and
    /// set its `last_offset` to `last`.
    ///
    /// Callers pass `next_segment.base_offset - 1`, the highest offset this
    /// sealed segment can hold. `Segment::open` leaves
    /// `last_offset = base_offset - 1` because it does not scan the `.log`.
    /// Without this fix, a sealed segment recovered on
    /// [`Log::open`](crate::Log) reports that stale `last_offset`.
    /// `Log::read_raw` skips any segment whose
    /// `last_offset() < fetch_offset`, so it would skip the first sealed
    /// segment after a restart and serve a later segment's base offset. That
    /// creates an offset gap, and a follower that fetches at 0 then loops on
    /// the resulting append mismatch.
    pub fn seal_at(&mut self, last: Offset) {
        self.sealed = true;
        self.last_offset = last;
    }

    /// Force-sync everything to disk.
    #[instrument(
        level = "debug",
        skip_all,
        fields(base_offset = self.base_offset.0, log_size = self.log_size),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.io.sync_data(&self.log_file)?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    /// The part of the segment's state an append changes, saved before the
    /// write so a failed one can put it back.
    pub(super) fn write_snapshot(&self) -> WriteSnapshot {
        WriteSnapshot {
            last_offset: self.last_offset,
            max_timestamp: self.max_timestamp,
            max_timestamp_offset: self.max_timestamp_offset,
        }
    }

    pub(super) fn rollback_failed_write(
        &mut self,
        position: u64,
        snapshot: WriteSnapshot,
    ) -> Result<(), LogError> {
        let WriteSnapshot {
            last_offset,
            max_timestamp,
            max_timestamp_offset,
        } = snapshot;
        self.log_file.set_len(position)?;
        self.first_timestamp = None;
        seek_to_log_size(&self.log_file, position)?;
        self.log_size = position;
        self.renew_reservation();
        self.resync_direct()?;
        self.last_offset = last_offset;
        self.max_timestamp = max_timestamp;
        self.max_timestamp_offset = max_timestamp_offset;
        let position = u32::try_from(position)
            .map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(position)?;
        let next = last_offset
            .0
            .checked_add(1)
            .and_then(|next| next.checked_sub(self.base_offset.0))
            .ok_or_else(|| LogError::BadSegmentName("offset overflow".into()))?;
        let next_relative =
            u32::try_from(next).map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
        self.time_index.truncate_by_relative_offset(next_relative)?;
        Ok(())
    }

    pub(crate) fn set_io(&mut self, io: std::sync::Arc<dyn crate::io::LogIo>) {
        self.offset_index.set_io(io.clone());
        self.time_index.set_io(io.clone());
        self.io = io;
    }

    /// Truncate the `.log` file and the indexes so that no batch at
    /// `relative_offset` `>= rel` remains. `Log::truncate_to` uses this
    /// method. The segment stays unsealed.
    #[instrument(
        level = "info",
        skip(self),
        fields(base_offset = self.base_offset.0, new_last_offset = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn truncate_to_relative(&mut self, rel: u32) -> Result<(), LogError> {
        // Read only as far as the cut can be: every kept batch lives below
        // the first index entry at or after `rel`. When `rel` is past the
        // last index entry, fall back to the whole file. This avoids
        // slurping the discarded tail on each truncate.
        let read_limit = self
            .offset_index
            .position_at_or_after(rel)
            .map_or(self.log_size, u64::from);
        let mut buf = Vec::new();
        let to_read = usize::try_from(read_limit).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

        let target_abs = self
            .base_offset
            .0
            .checked_add(i64::from(rel))
            .map(Offset)
            .ok_or_else(|| LogError::BadSegmentName("offset overflow".into()))?;
        let mut cur: &[u8] = &buf;
        let mut pos: u64 = 0;
        let mut last_kept_offset = self.base_offset - 1;
        let mut last_kept_ts = i64::MIN;
        let mut last_kept_ts_offset = last_kept_offset;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let batch_last_offset = batch
                .base_offset
                .checked_add(i64::from(batch.last_offset_delta))
                .map(Offset)
                .ok_or_else(|| LogError::Corrupt("batch last offset overflow".into()))?;
            if !krabka_verified::truncation_batch_retained(batch_last_offset.0, target_abs.0) {
                break;
            }
            pos += (before - cur.len()) as u64;
            last_kept_offset = batch_last_offset;
            if batch.max_timestamp > last_kept_ts {
                last_kept_ts = batch.max_timestamp;
                last_kept_ts_offset = batch_last_offset;
            }
        }

        self.log_file.set_len(pos)?;
        self.first_timestamp = None;
        seek_to_log_size(&self.log_file, pos)?;
        self.log_size = pos;
        self.renew_reservation();
        self.resync_direct()?;
        self.last_offset = last_kept_offset;
        self.max_timestamp = last_kept_ts;
        self.max_timestamp_offset = last_kept_ts_offset;

        let pos_u32 =
            u32::try_from(pos).map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(pos_u32)?;
        self.time_index.truncate_by_relative_offset(rel)?;
        self.sealed = false;
        tracing::Span::current().record("new_last_offset", self.last_offset.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{ByteSize, ByteSizeExt, kibibytes};
    use tempfile::tempdir;

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, NO_LIMIT, sample_batch};

    /// Truncating to a relative offset keeps every batch that ends before it,
    /// and leaves the segment describing exactly what it kept.
    ///
    /// Three things are rewritten from the walk and each is read back later:
    /// the byte length, which decides where the next append lands; the last
    /// offset, which decides what the next batch is numbered; and the maximum
    /// timestamp, which time-based retention and `MAX_TIMESTAMP` both read.
    /// Truncating everything away is the case that pins the last offset down --
    /// it has to fall back to one before the base.
    #[test]
    fn truncating_a_segment_leaves_it_describing_what_it_kept() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(100)).unwrap();
        // Three batches: offsets 100..=101, 102..=103, 104..=105, with
        // timestamps 500, 600, 700.
        for i in 0..3i64 {
            seg.append(
                &sample_batch(crate::segment::test_support::SampleBatchSetup {
                    offset: crate::Offset(100 + i * 2),
                    records: crate::segment::test_support::RecordCount(2),
                    timestamp: crate::segment::test_support::RecordTimestamp(500 + i * 100),
                }),
                DENSE_INDEX,
            )
            .unwrap();
        }
        let full_size = seg.size();
        check!(seg.last_offset() == Offset(105));
        check!(
            seg.max_timestamp() == 701,
            "batch 3 carries the newest record"
        );

        // Keep only the first batch: relative offset 2 is the start of the
        // second, and the bound is exclusive of the batch containing it.
        seg.truncate_to_relative(2).unwrap();
        check!(
            seg.last_offset() == Offset(101),
            "last kept batch ends at 101"
        );
        check!(seg.max_timestamp() == 501, "the newest surviving record");
        let kept = seg.size();
        check!(
            kept > ByteSize::ZERO && kept < full_size,
            "shorter, not empty"
        );

        // Truncating everything away: nothing is kept, so the segment reports
        // one before its base and no timestamp at all.
        seg.truncate_to_relative(0).unwrap();
        check!(seg.size() == ByteSize::ZERO, "no bytes survive");
        check!(
            seg.last_offset() == Offset(99),
            "one before the base, got {:?}",
            seg.last_offset()
        );
        check!(seg.max_timestamp() == i64::MIN, "no records, no timestamp");
    }

    /// Sealing is what `is_sealed` reports.
    #[test]
    fn is_sealed_follows_seal() {
        let (_dir, mut seg) = crate::segment::test_support::two_record_fixture();
        check!(!seg.is_sealed(), "a fresh segment is open");
        seg.seal().unwrap();
        check!(seg.is_sealed(), "a sealed segment reports it");
    }

    /// Sealing writes Kafka's final time-index entry, the running maximum
    /// timestamp with the last offset of the batch that set it, when the
    /// sparse index has not already recorded it. A reopened segment restores
    /// its `max_timestamp` from that entry, and the entries stay strictly
    /// increasing. Each case is `(label, batches as (base offset, timestamp,
    /// index interval), the entry count and last entry after sealing)`.
    #[test]
    fn seal_appends_the_final_time_index_entry() {
        let sparse = kibibytes(4);
        let cases = [
            (
                "no index point yet, and the newest timestamp came first",
                vec![(0, 100, sparse), (1, 300, sparse), (2, 200, sparse)],
                (1, Some((300, 1))),
            ),
            (
                "the index already holds the newest timestamp",
                vec![(0, 100, DENSE_INDEX), (1, 200, DENSE_INDEX)],
                (1, Some((200, 1))),
            ),
            (
                "an index point lags the newest batch",
                vec![
                    (0, 100, DENSE_INDEX),
                    (1, 200, DENSE_INDEX),
                    (2, 300, sparse),
                ],
                (2, Some((300, 2))),
            ),
        ];
        for (label, batches, expected) in cases {
            let (_dir, mut seg) = crate::segment::test_support::test_segment();
            for (base, timestamp, interval) in batches {
                seg.append(
                    &sample_batch(crate::segment::test_support::SampleBatchSetup {
                        offset: crate::Offset(base),
                        timestamp: crate::segment::test_support::RecordTimestamp(timestamp),
                        ..Default::default()
                    }),
                    interval,
                )
                .unwrap();
            }
            seg.seal().unwrap();
            check!(
                (seg.time_index.entry_count(), seg.time_index.last_entry()) == expected,
                "{label}"
            );
        }
    }

    /// `truncate_to_relative` decides which batches to drop by each batch's
    /// last offset, `batch.base_offset + last_offset_delta`, compared against
    /// `target_abs`. MULTI-record batches make the `+` load-bearing. Batch A
    /// spans 0..=2 and batch B spans 3..=5, so a truncate to rel 3, where
    /// `target_abs = 3`, must keep A, whose last offset 2 is < 3, and drop B,
    /// whose last offset 5 is >= 3. A mutation of `+` to `-` computes A's last
    /// offset as -2 and B's as 1, so it wrongly keeps B and the read still
    /// returns batch B.
    #[test]
    fn truncate_to_relative_uses_batch_last_offset() {
        let (_dir, mut seg) = crate::segment::test_support::seeded_fixture(
            crate::segment::test_support::SeededSegmentSetup {
                batches: &[
                    crate::segment::test_support::THREE_RECORD_BATCH, // offsets 0..=2
                    crate::segment::test_support::SECOND_THREE_RECORD_BATCH, // offsets 3..=5
                ],
                ..Default::default()
            },
        );
        assert2::assert!(seg.last_offset() == 5);

        // target_abs = base(0) + rel(3) = 3. Drop batches with last >= 3.
        seg.truncate_to_relative(3).unwrap();
        let read = seg.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(
            read == vec![sample_batch(
                crate::segment::test_support::THREE_RECORD_BATCH
            )]
        );
        let expected_size: usize = read.iter().map(RecordBatch::encoded_len).sum();
        assert2::assert!(seg.size().bytes_usize() == expected_size);
    }

    #[test]
    fn truncate_to_relative_rejects_an_absolute_offset_overflow() {
        let (_dir, mut seg) = crate::segment::test_support::segment_at(crate::Offset(i64::MAX));

        let error = seg
            .truncate_to_relative(1)
            .expect_err("base plus relative cut must be checked");
        assert2::assert!(error.to_string().contains("offset overflow"));
    }

    #[test]
    fn flush_succeeds() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        seg.append(
            &sample_batch(crate::segment::test_support::SampleBatchSetup {
                timestamp: crate::segment::test_support::RecordTimestamp(42),
                ..Default::default()
            }),
            kibibytes(4),
        )
        .unwrap();
        seg.flush().unwrap();
    }
}
