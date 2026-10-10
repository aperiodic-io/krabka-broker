//! The decoding read path: batches come back as [`RecordBatch`] values.
//!
//! This is the read that callers use when they need the records themselves.
//! The verbatim, decode-free counterpart lives in the sibling `read_raw`
//! module.

use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt};
use tracing::instrument;

use super::Segment;
use crate::{config::DEFAULT_READ_BUFFER_CAP, error::LogError};

impl Segment {
    /// Read batches from `offset` or just before it, up to about `max_bytes`
    /// of `.log` data. The result is an empty `Vec` when `offset` is past
    /// `last_offset`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0, batches = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read(&self, offset: Offset, max_size: ByteSize) -> Result<Vec<RecordBatch>, LogError> {
        self.read_with_buffer_cap(offset, max_size, DEFAULT_READ_BUFFER_CAP)
    }

    pub(crate) fn read_with_buffer_cap(
        &self,
        offset: Offset,
        max_size: ByteSize,
        read_buffer_cap: ByteSize,
    ) -> Result<Vec<RecordBatch>, LogError> {
        if offset > self.last_offset {
            return Ok(vec![]);
        }
        let target_rel = u32::try_from((offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::BadSegmentName("target offset out of range".into()))?;
        let start_pos = self.read_start_position(target_rel)?;

        // Below this line the budget is a buffer length and a file-read
        // count, so it crosses back to `usize` once, here.
        let max_bytes = max_size.bytes_usize();
        let initial_cap = max_size.min(read_buffer_cap).bytes_usize();
        let mut buf: Vec<u8> = Vec::with_capacity(initial_cap);
        self.read_log_range(start_pos, &mut buf, max_bytes)?;

        let mut out: Vec<RecordBatch> = Vec::new();
        let mut total: usize = 0;
        let mut cursor: &[u8] = &buf;
        while !cursor.is_empty() {
            let before = cursor.len();
            let Ok(batch) = RecordBatch::decode(&mut cursor) else {
                break; // partial trailing batch — stop.
            };
            let consumed = before - cursor.len();
            let batch_last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            if batch_last >= offset {
                out.push(batch);
                total += consumed;
                if total >= max_bytes {
                    break;
                }
            }
        }
        tracing::Span::current().record("batches", out.len());
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::prelude::kibibytes;

    use super::*;
    use crate::segment::test_support::{NO_LIMIT, sample_batch};

    // `Segment::read` maps the absolute fetch offset to a relative index key
    // via `offset - base_offset`. With a dense index and base_offset 100,
    // reading from offset 103 must start at the batch containing 103 and
    // return the batches at 103 and 105. Mutating `-`→`+` computes
    // `103 + 100 = 203`, whose index lookup lands at (or past) the last
    // batch, skipping the offset-103 batch.
    #[test]
    fn read_uses_relative_offset_for_index_lookup() {
        let (_dir, seg) = super::super::test_support::indexed_segment();
        let read = seg.read(Offset(103), NO_LIMIT).unwrap();
        assert2::assert!(seg.last_offset() == Offset(105));
        assert2::assert!(
            read == vec![
                sample_batch(crate::segment::test_support::INDEXED_READ_BATCHES[1]),
                sample_batch(crate::segment::test_support::INDEXED_READ_BATCHES[2])
            ]
        );
    }

    /// `Segment::read` accumulates consumed bytes as `before - cursor.len()`,
    /// the exact number of bytes each batch decode advanced, to enforce the
    /// `max_bytes` budget. With `max_bytes` set to the segment's full size,
    /// all three batches fit and the read returns them. A mutation of `-` to
    /// `+` inflates `consumed` on the first batch past `max_bytes`, so the
    /// read stops after one batch.
    #[test]
    fn read_consumed_bytes_gates_max_bytes_budget() {
        let (_dir, seg) = crate::segment::test_support::seeded_fixture(
            crate::segment::test_support::SeededSegmentSetup {
                batches: &[
                    crate::segment::test_support::ONE_RECORD_BATCH,
                    crate::segment::test_support::SECOND_SINGLE_RECORD_BATCH,
                    crate::segment::test_support::THIRD_SINGLE_RECORD_BATCH,
                ],
                ..Default::default()
            },
        );
        // Exactly the whole segment: correct consumed accounting fits all three
        // batches; inflated accounting overshoots after the first.
        let max_size = seg.size();

        let read = seg.read(Offset(0), max_size).unwrap();
        assert2::assert!(read == crate::segment::test_support::three_single_record_batches());
    }

    #[test]
    fn read_at_higher_offset_skips_earlier_batches() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        seg.append(
            &sample_batch(crate::segment::test_support::FIRST_LARGE_TIMESTAMP_BATCH),
            kibibytes(4),
        )
        .unwrap();
        seg.append(
            &sample_batch(crate::segment::test_support::SECOND_LARGE_TIMESTAMP_BATCH),
            kibibytes(4),
        )
        .unwrap();
        let read = seg.read(Offset(4), NO_LIMIT).unwrap();
        // Offset 4 falls inside the second batch (offsets 3..=4).
        assert2::assert!(
            read == vec![sample_batch(
                crate::segment::test_support::SECOND_LARGE_TIMESTAMP_BATCH
            )]
        );
    }

    #[test]
    fn read_past_last_offset_returns_empty() {
        let (_dir, mut seg) = crate::segment::test_support::test_segment();
        seg.append(
            &sample_batch(crate::segment::test_support::SampleBatchSetup {
                records: crate::segment::test_support::RecordCount(2),
                timestamp: crate::segment::test_support::RecordTimestamp(1_000),
                ..Default::default()
            }),
            kibibytes(4),
        )
        .unwrap();
        let read = seg.read(Offset(100), NO_LIMIT).unwrap();
        assert2::assert!(read.is_empty());
    }
}
