//! The verbatim, decode-free read path.
//!
//! A fetch serves the producer's own batch bytes, so this walk reads only the
//! fixed v2 batch headers to find run boundaries and never touches a record
//! body. The zero-copy descriptor form of the same walk is the sibling
//! `read_raw_desc` module.

use std::ops::Range;

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::{HEADER_LEN, RecordBatchHeader};
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;
use zerocopy::FromBytes;

use super::{RawSegmentRead, Segment};
use crate::{
    config::{DEFAULT_READ_AHEAD_MAX, DEFAULT_READ_BUFFER_CAP},
    error::LogError,
};

/// Header fields used to choose a verbatim range without decoding records.
pub(super) struct RawBatch {
    start_offset: Offset,
    last_offset: Offset,
    len: usize,
}

impl RawBatch {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LogError> {
        let header = RecordBatchHeader::ref_from_bytes(bytes)
            .map_err(|_| LogError::Corrupt("record batch header".into()))?;
        let base = header.base_offset.get();
        Ok(Self {
            start_offset: Offset(base),
            last_offset: Offset(base + i64::from(header.last_offset_delta.get())),
            len: 12 + usize::try_from(header.batch_length.get().max(0)).unwrap_or(0),
        })
    }
}

/// A selected byte range relative to the first read position.
pub(super) struct RawRange {
    pub(super) positions: Range<usize>,
    pub(super) start_offset: Offset,
    pub(super) last_offset: Offset,
}

/// Select the same complete batches for buffered and file-region fetches.
///
/// Only the first eligible batch may extend past the initial window. The
/// caller checks that this anti-stall batch is complete in its source.
pub(super) fn select_raw_range(
    fetch_offset: Offset,
    limit_offset: Offset,
    max_bytes: usize,
    window: usize,
    mut read_header: impl FnMut(usize) -> Result<Option<RawBatch>, LogError>,
) -> Result<Option<RawRange>, LogError> {
    let mut pos = 0;
    let mut range: Option<RawRange> = None;
    while pos + HEADER_LEN <= window {
        let Some(batch) = read_header(pos)? else {
            break;
        };
        let end = pos + batch.len;
        if batch.last_offset < fetch_offset {
            pos = end;
            continue;
        }
        if batch.start_offset >= limit_offset || (end > window && range.is_some()) {
            break;
        }
        let selected = range.get_or_insert(RawRange {
            positions: pos..end,
            start_offset: batch.start_offset,
            last_offset: batch.last_offset,
        });
        selected.positions.end = end;
        selected.last_offset = batch.last_offset;
        pos = end;
        if end > window || selected.positions.len() >= max_bytes {
            break;
        }
    }
    Ok(range)
}

impl Segment {
    /// Read a contiguous run of **complete, verbatim** record-batch bytes.
    ///
    /// The run starts at the batch that contains `fetch_offset`. It includes
    /// only batches whose `base_offset < limit_offset`, up to about
    /// `max_bytes`, and always at least one batch. That last rule is Kafka's
    /// anti-stall rule. This method decodes no records. It reads only the
    /// fixed batch headers to find the boundaries.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0, bytes = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawSegmentRead, LogError> {
        self.read_raw_with_policy(
            fetch_offset,
            limit_offset,
            max_size,
            DEFAULT_READ_BUFFER_CAP,
            DEFAULT_READ_AHEAD_MAX,
        )
    }

    pub(super) fn raw_start_position(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        operation: &str,
    ) -> Result<Option<u64>, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(None);
        }
        let target_rel = u32::try_from((fetch_offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::Corrupt(format!("{operation} target offset out of range")))?;
        self.read_start_position(target_rel).map(Some)
    }

    /// [`Segment::read_raw`] under the log's configured read policy: the
    /// initial allocation is capped at `read_buffer_cap`, and the readahead
    /// hint reaches at most `read_ahead_max` past the read's own window.
    pub(crate) fn read_raw_with_policy(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
        read_buffer_cap: ByteSize,
        read_ahead_max: ByteSize,
    ) -> Result<RawSegmentRead, LogError> {
        let Some(start_pos) = self.raw_start_position(fetch_offset, limit_offset, "read_raw")?
        else {
            return Ok(RawSegmentRead::empty());
        };

        // Below this line the budget indexes into a byte buffer, so it
        // crosses back to `usize` once, here.
        let max_bytes = max_size.bytes_usize();
        let first_read = max_bytes.max(HEADER_LEN);
        self.advise_read(start_pos, first_read as u64, read_ahead_max);
        let mut buf: Vec<u8> = Vec::with_capacity(first_read.min(read_buffer_cap.bytes_usize()));
        self.read_log_range(start_pos, &mut buf, first_read)?;

        let Some(range) =
            select_raw_range(fetch_offset, limit_offset, max_bytes, buf.len(), |pos| {
                RawBatch::decode(&buf[pos..pos + HEADER_LEN]).map(Some)
            })?
        else {
            return Ok(RawSegmentRead::empty());
        };
        let bytes = if range.positions.end > buf.len() {
            let len = range.positions.len();
            let mut one = Vec::with_capacity(len);
            self.read_log_range(start_pos + range.positions.start as u64, &mut one, len)?;
            if one.len() < len {
                return Ok(RawSegmentRead::empty());
            }
            Bytes::from(one)
        } else {
            Bytes::from(buf).slice(range.positions)
        };
        tracing::Span::current().record("bytes", bytes.len());
        Ok(RawSegmentRead {
            start_offset: range.start_offset,
            last_offset: range.last_offset,
            bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::{bytes, mebibytes};

    use super::*;
    use crate::segment::test_support::{
        DENSE_INDEX, NO_LIMIT, sample_batch, test_batch_at, test_segment,
    };

    /// A fetch reads nothing when it starts past the segment, and nothing when
    /// it starts at or past the limit. Either condition alone is enough --
    /// joined with `&&` a fetch would have to be both before it read nothing.
    #[derive(Clone, Copy)]
    struct BatchCount(i64);

    fn append_two_record_batches(seg: &mut Segment, count: BatchCount) {
        for i in 0..count.0 {
            seg.append(
                &sample_batch(crate::segment::test_support::SampleBatchSetup {
                    offset: crate::Offset(i * 2),
                    records: crate::segment::test_support::RecordCount(2),
                    timestamp: crate::segment::test_support::RecordTimestamp(100 + i),
                }),
                DENSE_INDEX,
            )
            .unwrap();
        }
    }

    #[test]
    fn a_fetch_past_the_segment_or_at_the_limit_reads_nothing() {
        let (_dir, seg) = crate::segment::test_support::seeded_fixture(
            crate::segment::test_support::SeededSegmentSetup {
                batches: &[
                    crate::segment::test_support::THREE_RECORD_BATCH, // offsets 0..=2
                ],
                ..Default::default()
            },
        );

        let past = seg.read_raw(Offset(3), Offset(99), NO_LIMIT).unwrap();
        check!(past.is_empty(), "a fetch past the last offset");

        let at_limit = seg.read_raw(Offset(0), Offset(0), NO_LIMIT).unwrap();
        check!(at_limit.is_empty(), "a fetch at the limit");

        let inside = seg.read_raw(Offset(0), Offset(3), NO_LIMIT).unwrap();
        check!(
            !inside.is_empty(),
            "a fetch inside the segment and below the limit"
        );
    }

    /// Batches before the fetch offset are stepped over by their own length.
    /// Advancing by anything else lands mid-batch and the walk reads a header
    /// out of the middle of a record.
    #[test]
    fn a_fetch_mid_segment_steps_over_the_batches_before_it() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        // A sparse index, so only the first batch gets an entry and the lookup
        // lands at the segment head. The walk then has to step over the two
        // batches before the one asked for -- with a dense index it would jump
        // straight there and never step at all.
        let sparse = mebibytes(1);
        seg.append(
            &sample_batch(crate::segment::test_support::TWO_RECORD_BATCH),
            sparse,
        )
        .unwrap(); // 0..=1
        seg.append(
            &sample_batch(crate::segment::test_support::SECOND_TWO_RECORD_BATCH),
            sparse,
        )
        .unwrap(); // 2..=3
        seg.append(
            &sample_batch(crate::segment::test_support::THIRD_TWO_RECORD_BATCH),
            sparse,
        )
        .unwrap(); // 4..=5

        let read = seg
            .read_raw_with_policy(
                Offset(4),
                Offset(99),
                NO_LIMIT,
                mebibytes(1),
                DEFAULT_READ_AHEAD_MAX,
            )
            .unwrap();
        check!(
            read.start_offset == Offset(4),
            "start {:?}",
            read.start_offset
        );
        check!(read.last_offset == Offset(5), "last {:?}", read.last_offset);
    }

    /// A batch that will not fit the first read is fetched on its own, from
    /// its own position in the file.
    ///
    /// That position is `start_pos + pos`, and with a dense index and a fetch
    /// past the first batch `start_pos` is well away from the file head -- so
    /// reading from anywhere else returns a different batch, or nothing.
    #[test]
    fn a_batch_too_large_for_the_first_read_is_fetched_from_its_own_position() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        append_two_record_batches(&mut seg, BatchCount(4));

        // A one-byte budget makes the first read header-sized, so the batch
        // cannot fit it and takes the read-one-batch path.
        let read = seg.read_raw(Offset(4), Offset(99), bytes(1)).unwrap();
        check!(
            read.start_offset == Offset(4),
            "start {:?}",
            read.start_offset
        );
        check!(read.last_offset == Offset(5), "last {:?}", read.last_offset);
        check!(!read.is_empty());
    }

    #[test]
    fn batch_too_large_with_nonzero_start_pos_and_pos() {
        let (_dir, mut seg) = crate::segment::test_support::two_record_fixture();
        let p1 = seg.log_size;
        seg.append(
            &sample_batch(crate::segment::test_support::SECOND_TWO_RECORD_BATCH),
            DENSE_INDEX,
        )
        .unwrap();
        let b1_len = seg.log_size - p1;
        let no_index = mebibytes(1);
        seg.append(
            &sample_batch(crate::segment::test_support::THIRD_TWO_RECORD_BATCH),
            no_index,
        )
        .unwrap();
        seg.append(
            &sample_batch(crate::segment::test_support::SampleBatchSetup {
                offset: crate::Offset(6),
                records: crate::segment::test_support::RecordCount(2),
                timestamp: crate::segment::test_support::RecordTimestamp(400),
            }),
            no_index,
        )
        .unwrap();

        let budget = bytes(u32::try_from(b1_len).unwrap() + 61);
        let read = seg.read_raw(Offset(4), Offset(99), budget).unwrap();
        check!(read.start_offset == Offset(4));
        check!(read.last_offset == Offset(5));
        check!(!read.is_empty());
    }

    /// The byte budget stops the walk once the selected range reaches it, so a
    /// small budget returns fewer batches than an unlimited one.
    #[test]
    fn the_byte_budget_bounds_how_much_a_fetch_returns() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        append_two_record_batches(&mut seg, BatchCount(6));

        let everything = seg.read_raw(Offset(0), Offset(99), NO_LIMIT).unwrap();
        let clipped = seg.read_raw(Offset(0), Offset(99), bytes(1)).unwrap();
        check!(
            !clipped.is_empty(),
            "a budget of one byte still returns a batch"
        );
        check!(
            clipped.last_offset < everything.last_offset,
            "clipped to {:?}, unlimited reached {:?}",
            clipped.last_offset,
            everything.last_offset
        );

        let batch_size = u32::try_from(
            sample_batch(crate::segment::test_support::TWO_RECORD_BATCH).encoded_len(),
        )
        .unwrap();
        let two_batches = seg
            .read_raw(Offset(0), Offset(99), bytes(batch_size * 2))
            .unwrap();
        check!(two_batches.start_offset == Offset(0));
        check!(two_batches.last_offset == Offset(3));
    }

    // `Segment::read_raw` maps the fetch offset to the relative index key the
    // same way. base_offset 100, dense index, `read_raw(103)` must begin at
    // the offset-103 batch (`start_offset == 103`). Mutating `-`→`+` computes
    // `203`, whose lookup skips past the offset-103 batch → `start_offset`
    // becomes 105.
    #[test]
    fn read_raw_uses_relative_offset_for_index_lookup() {
        let (_dir, seg) = super::super::test_support::indexed_segment();

        let r = seg.read_raw(Offset(103), Offset(1000), NO_LIMIT).unwrap();
        assert2::assert!(!r.is_empty());
        assert2::assert!(r.start_offset == Offset(103));
    }

    #[test]
    fn read_raw_is_byte_exact_and_multi_batch() {
        let (dir, mut seg) = test_segment();
        let mut wire = bytes::BytesMut::new();
        for off in 0..3i64 {
            let b = test_batch_at(crate::Offset(off));
            seg.append(&b, DENSE_INDEX).unwrap();
            b.encode(&mut wire).unwrap();
        }
        let wire = wire.freeze();
        let r = seg.read_raw(Offset(0), Offset(3), mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(2));
        assert2::assert!(&r.bytes[..] == &wire[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_clamps_at_limit_offset() {
        let (dir, mut seg) = test_segment();
        let mut expected = bytes::BytesMut::new();
        for off in 0..3i64 {
            let batch = test_batch_at(crate::Offset(off));
            seg.append(&batch, DENSE_INDEX).unwrap();
            if off < 2 {
                batch.encode(&mut expected).unwrap();
            }
        }
        let r = seg.read_raw(Offset(0), Offset(2), mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(1));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_returns_at_least_one_batch_over_budget() {
        let (dir, mut seg) = test_segment();
        let batch = test_batch_at(crate::Offset(0));
        let mut expected = bytes::BytesMut::new();
        batch.encode(&mut expected).unwrap();
        seg.append(&batch, DENSE_INDEX).unwrap();
        let r = seg.read_raw(Offset(0), Offset(1), bytes(1)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(0));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }
}
