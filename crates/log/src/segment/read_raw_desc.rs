//! The zero-copy descriptor form of the verbatim read, for the `sendfile`
//! fetch path.
//!
//! The walk mirrors `read_raw` byte for byte but returns a file-region
//! descriptor instead of an owned buffer, so the broker sends the run straight
//! out of the page cache. The whole module is gated on the SENDFILE alias by
//! its declaration in the parent, which is why nothing here carries a `cfg`.

use std::sync::Arc;

use krabka_ids::Offset;
use krabka_protocol::records::HEADER_LEN;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::{
    RawSegmentDesc, Segment,
    io::read_full_at,
    read_raw::{RawBatch, select_raw_range},
};
use crate::{config::DEFAULT_READ_AHEAD_MAX, error::LogError};

impl Segment {
    /// Descriptor variant of [`Segment::read_raw`] for the zero-copy
    /// `sendfile` fetch path.
    ///
    /// This method runs the **same** boundary walk and selects the identical
    /// `[start_pos+range_start, start_pos+range_end)` byte range that
    /// `read_raw` would have sliced. It returns a [`krabka_protocol::records::FileRegion`] descriptor
    /// instead of a `pread` of the payload into an owned `Bytes`.
    ///
    /// The walk is header-only. It `pread`s only the fixed v2 batch headers to
    /// find batch boundaries, and it uses the header's `batch_length`. It
    /// never reads the record payloads. The region is byte-identical to the
    /// `bytes` of `read_raw` for the same
    /// `(fetch_offset, limit_offset, max_bytes)`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawSegmentDesc, LogError> {
        self.read_raw_desc_with_policy(fetch_offset, limit_offset, max_size, DEFAULT_READ_AHEAD_MAX)
    }

    /// [`Segment::read_raw_desc`] under the log's configured read policy: the
    /// readahead hint reaches at most `read_ahead_max` past the read's own
    /// window.
    pub(crate) fn read_raw_desc_with_policy(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
        read_ahead_max: ByteSize,
    ) -> Result<RawSegmentDesc, LogError> {
        let Some(start_pos) =
            self.raw_start_position(fetch_offset, limit_offset, "read_raw_desc")?
        else {
            return Ok(RawSegmentDesc::empty());
        };
        // A run that starts in the bytes an `O_DIRECT` segment holds in
        // memory is not in the page cache, so `sendfile` would read it from
        // the disk. Describing nothing hands the fetch to the byte-copy path,
        // which reads it from memory: an empty description is what the
        // broker falls back on, and a segment that contributes nothing is
        // one the log's walk passes over.
        if self.cached_from().is_some_and(|cached| start_pos >= cached) {
            return Ok(RawSegmentDesc::empty());
        }

        // Use the buffered reader's initial window, without reading payloads.
        let max_bytes = max_size.bytes_usize();
        let window =
            (max_bytes.max(HEADER_LEN) as u64).min(self.log_size.saturating_sub(start_pos));
        self.advise_read(start_pos, window, read_ahead_max);
        let window = usize::try_from(window)
            .map_err(|_| LogError::Corrupt("read_raw_desc window too large".into()))?;
        let mut header = [0; HEADER_LEN];
        let Some(range) = select_raw_range(fetch_offset, limit_offset, max_bytes, window, |pos| {
            if read_full_at(&self.log_file, start_pos + pos as u64, &mut header)? < HEADER_LEN {
                return Ok(None);
            }
            RawBatch::decode(&header).map(Some)
        })?
        else {
            return Ok(RawSegmentDesc::empty());
        };
        if start_pos + range.positions.end as u64 > self.log_size {
            return Ok(RawSegmentDesc::empty());
        }
        Ok(RawSegmentDesc {
            start_offset: range.start_offset,
            last_offset: range.last_offset,
            region: Some(krabka_protocol::records::FileRegion {
                file: Arc::clone(&self.log_file),
                offset: start_pos + range.positions.start as u64,
                len: range.positions.len(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::{bytes, mebibytes};

    use super::*;
    use crate::segment::test_support::{DENSE_INDEX, test_batch_at, test_segment};

    // `pread` a `FileRegion` into a fresh `Vec`. These are the bytes that the
    // broker's sendfile would transmit, and that its TLS pread-fallback would
    // copy.
    krabka_macros::file_region_fixture!(region_bytes);

    /// The load-bearing Increment-D/E invariant: the `read_raw_desc` region
    /// maps to exactly the bytes that `read_raw` would have returned, for the
    /// same `(fetch_offset, limit_offset, max_bytes)`. This test covers
    /// single-batch reads, multi-batch reads, mid-stream start offsets, the
    /// limit clamp, and the one-batch-over-budget anti-stall rule.
    #[test]
    fn read_raw_desc_region_equals_read_raw_bytes() {
        let (dir, mut seg) = test_segment();
        for off in 0..5i64 {
            seg.append(&test_batch_at(crate::Offset(off)), DENSE_INDEX)
                .unwrap();
        }
        let batch_len = u32::try_from(test_batch_at(crate::Offset(0)).encoded_len()).unwrap();
        let cases = [
            ("all batches", 0i64, 5i64, mebibytes(10)),
            ("limit clamp", 0, 3, mebibytes(10)),
            ("mid-stream start", 2, 5, mebibytes(10)),
            ("one-batch anti-stall", 0, 5, bytes(1)),
            ("mid-stream anti-stall", 2, 5, bytes(1)),
            ("zero-byte anti-stall", 0, 5, bytes(0)),
            ("window ends inside first batch", 0, 5, bytes(batch_len - 1)),
            ("window ends after first batch", 0, 5, bytes(batch_len)),
            (
                "window holds a second header",
                0,
                5,
                bytes(batch_len + u32::try_from(HEADER_LEN).unwrap()),
            ),
            ("last batch", 4, 5, mebibytes(10)),
            ("past last batch", 5, 9, mebibytes(10)),
            ("at limit", 0, 0, mebibytes(10)),
            ("past limit", 2, 1, mebibytes(10)),
        ];
        for (_name, fo, lo, mb) in cases {
            let raw = seg.read_raw(Offset(fo), Offset(lo), mb).unwrap();
            let desc = seg.read_raw_desc(Offset(fo), Offset(lo), mb).unwrap();
            assert2::assert!(desc.start_offset == raw.start_offset);
            assert2::assert!(desc.last_offset == raw.last_offset);
            match &desc.region {
                Some(region) => {
                    assert2::assert!(region.len == raw.bytes.len());
                    assert2::assert!(region_bytes(region) == raw.bytes.to_vec());
                }
                None => assert2::assert!(raw.bytes.is_empty()),
            }
        }
        drop(dir);
    }

    /// A truncated trailing batch, where the byte budget cuts mid-batch, must
    /// produce a region whose bytes equal the clipped output of `read_raw`. A
    /// sendfile of a clipped range is wire-valid, because the consumer drops
    /// the partial batch.
    #[test]
    fn read_raw_desc_matches_read_raw_when_budget_clips_run() {
        let (dir, mut seg) = test_segment();
        // Several batches; a mid-size budget will include some but not all.
        for off in 0..6i64 {
            seg.append(&test_batch_at(crate::Offset(off)), DENSE_INDEX)
                .unwrap();
        }
        // Budget that admits ~2-3 batches (each batch is small but > a few bytes).
        let raw = seg.read_raw(Offset(0), Offset(6), bytes(80)).unwrap();
        let desc = seg.read_raw_desc(Offset(0), Offset(6), bytes(80)).unwrap();
        let region = desc.region.expect("non-empty");
        assert2::assert!(desc.start_offset == raw.start_offset);
        assert2::assert!(desc.last_offset == raw.last_offset);
        assert2::assert!(region.len == raw.bytes.len());
        assert2::assert!(region_bytes(&region) == raw.bytes.to_vec());
        drop(dir);
    }

    /// The descriptor read asks the kernel for the region it describes, and
    /// the window after it, in one hint given before its header walk. The
    /// walk then reads headers the hint already brought in, rather than
    /// taking one disk read per batch on a cold segment.
    #[test]
    fn read_raw_desc_advises_its_region_and_the_next_window_in_one_hint() {
        use crate::segment::test_support::recording_advice;

        let (_dir, mut seg) = test_segment();
        for off in 0..20i64 {
            seg.append(&test_batch_at(crate::Offset(off)), DENSE_INDEX)
                .unwrap();
        }
        let advice = recording_advice(&mut seg);
        let batch_len = u64::try_from(test_batch_at(crate::Offset(0)).encoded_len()).unwrap();
        let budget = 3 * batch_len;

        let desc = seg
            .read_raw_desc(Offset(4), Offset(20), bytes(u32::try_from(budget).unwrap()))
            .unwrap();
        let region = desc.region.expect("a read inside the segment");
        assert2::check!(region.len == usize::try_from(budget).unwrap());
        assert2::check!(advice.take() == vec![(region.offset, 2 * budget)]);
    }
}
