//! Accumulator that turns a run of coordinator records into one
//! `__consumer_offsets` [`RecordBatch`].
//!
//! Every coordinator write path — classic, next-gen, share, and streams —
//! assembles its records through this builder, so the offset-delta
//! bookkeeping of a batch has one home.

use bytes::Bytes;
use krabka_protocol::records::{Record, RecordBatch};

#[derive(Default)]
pub(crate) struct OffsetRecordBatchBuilder {
    records: Vec<Record>,
}

impl OffsetRecordBatchBuilder {
    pub(crate) fn push(&mut self, key: Bytes, value: Option<Bytes>) {
        let delta = i32::try_from(self.records.len()).expect("batch size fits i32");
        self.records.push(Record {
            offset_delta: delta,
            key: Some(key),
            value,
            ..Default::default()
        });
    }

    pub(crate) fn finish(self, now_ms: i64) -> RecordBatch {
        let last_delta = i32::try_from(self.records.len().saturating_sub(1)).unwrap_or(0);
        RecordBatch {
            base_timestamp: now_ms,
            max_timestamp: now_ms,
            records: self.records,
            last_offset_delta: last_delta,
            ..RecordBatch::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_log::Log;

    use super::*;

    #[test]
    fn coordinator_records_roll_only_after_the_segment_age_limit() {
        let mut fixture = crate::test_support::segment_age_fixture();
        for elapsed_ms in [0, 1, 2, 3, fixture.roll_ms] {
            let mut builder = OffsetRecordBatchBuilder::default();
            builder.push(
                Bytes::from_static(b"metadata"),
                Some(Bytes::from_static(b"value")),
            );
            builder.push(Bytes::from_static(b"offset"), None);
            fixture
                .log
                .append(&mut builder.finish(fixture.first_ms + elapsed_ms))
                .unwrap();
        }
        check!(fixture.log.tierable_segments().is_empty());

        // Recovery must retain the original record timestamp used for rolling.
        fixture.log.close();
        fixture.log = Log::open(fixture.dir.path(), fixture.config.clone()).unwrap();
        let mut builder = OffsetRecordBatchBuilder::default();
        builder.push(Bytes::from_static(b"offset"), None);
        fixture
            .log
            .append(&mut builder.finish(fixture.first_ms + fixture.roll_ms + 1))
            .unwrap();
        check!(fixture.log.tierable_segments().len() == 1);
    }
}
