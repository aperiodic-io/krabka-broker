//! Fixture builders and budget constants that the segment modules' unit tests
//! share.

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, ByteSizeExt, gibibytes};
use tempfile::tempdir;

use super::Segment;

/// Index every batch but the first. Every later batch is more than `0` bytes
/// past the last entry, and the first is `0` bytes past the segment start,
/// which Kafka's `bytesSinceLastIndexEntry > indexIntervalBytes` does not index.
pub(super) const DENSE_INDEX: ByteSize = ByteSize::ZERO;

/// A read budget larger than anything these tests write, so the byte
/// budget never clips the result.
pub(super) const NO_LIMIT: ByteSize = gibibytes(4);

#[derive(Clone, Copy, Default)]
pub(super) struct RecordCount(pub i32);

#[derive(Clone, Copy, Default)]
pub(super) struct RecordTimestamp(pub i64);

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SampleBatchSetup {
    pub offset: Offset,
    #[default(RecordCount(1))]
    pub records: RecordCount,
    pub timestamp: RecordTimestamp,
}

/// A one-record first batch at timestamp 100, reused by read and recovery fixtures.
pub(super) const ONE_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(0),
    records: RecordCount(1),
    timestamp: RecordTimestamp(100),
};

pub(super) const TWO_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    records: RecordCount(2),
    ..ONE_RECORD_BATCH
};
pub(super) const THREE_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    records: RecordCount(3),
    ..ONE_RECORD_BATCH
};
pub(super) const SECOND_SINGLE_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(1),
    timestamp: RecordTimestamp(200),
    ..ONE_RECORD_BATCH
};
pub(super) const THIRD_SINGLE_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(2),
    timestamp: RecordTimestamp(300),
    ..ONE_RECORD_BATCH
};
pub(super) const TWO_RECORD_AFTER_THREE_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(3),
    records: RecordCount(2),
    timestamp: RecordTimestamp(200),
};
pub(super) const SECOND_THREE_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(3),
    records: RecordCount(3),
    timestamp: RecordTimestamp(200),
};
pub(super) const FIRST_HEADER_WALK_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(10),
    records: RecordCount(5),
    timestamp: RecordTimestamp(1000),
};
pub(super) const SECOND_HEADER_WALK_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(15),
    records: RecordCount(5),
    timestamp: RecordTimestamp(2000),
};
pub(super) const SECOND_TWO_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(2),
    records: RecordCount(2),
    timestamp: RecordTimestamp(200),
};
pub(super) const THIRD_TWO_RECORD_BATCH: SampleBatchSetup = SampleBatchSetup {
    offset: Offset(4),
    records: RecordCount(2),
    timestamp: RecordTimestamp(300),
};

/// The two timestamp ranges used to search offsets 0 through 4.
pub(super) const FIVE_OFFSET_TIMESTAMP_BATCHES: &[SampleBatchSetup] =
    &[THREE_RECORD_BATCH, TWO_RECORD_AFTER_THREE_BATCH];

/// The pair used to round-trip and fetch across the second batch's interior.
pub(super) const FIRST_LARGE_TIMESTAMP_BATCH: SampleBatchSetup = SampleBatchSetup {
    timestamp: RecordTimestamp(1_000_000),
    ..THREE_RECORD_BATCH
};
pub(super) const SECOND_LARGE_TIMESTAMP_BATCH: SampleBatchSetup = SampleBatchSetup {
    timestamp: RecordTimestamp(2_000_000),
    ..TWO_RECORD_AFTER_THREE_BATCH
};

pub(super) const INDEXED_READ_BATCHES: &[SampleBatchSetup] = &[
    SampleBatchSetup {
        offset: Offset(100),
        ..THREE_RECORD_BATCH
    },
    SampleBatchSetup {
        offset: Offset(103),
        ..TWO_RECORD_AFTER_THREE_BATCH
    },
    SampleBatchSetup {
        offset: Offset(105),
        ..THIRD_SINGLE_RECORD_BATCH
    },
];

pub(super) fn sample_batch(setup: SampleBatchSetup) -> RecordBatch {
    let SampleBatchSetup {
        offset,
        records,
        timestamp,
    } = setup;
    let mut b = RecordBatch {
        base_offset: offset.0,
        base_timestamp: timestamp.0,
        max_timestamp: timestamp.0 + i64::from(records.0 - 1),
        last_offset_delta: records.0 - 1,
        ..RecordBatch::default()
    };
    for i in 0..records.0 {
        b.records
            .push(crate::test_support::numbered_record(i, i64::from(i)));
    }
    b
}

pub(super) fn test_segment() -> (tempfile::TempDir, Segment) {
    segment_at(Offset(0))
}

pub(super) fn segment_at(base: Offset) -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let seg = Segment::create(dir.path(), base).unwrap();
    (dir, seg)
}

pub(super) fn test_batch_at(off: Offset) -> RecordBatch {
    crate::test_support::single_record_batch(off.0, 1_000, Bytes::from(format!("v{}", off.0)))
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct SeededSegmentSetup<'a> {
    pub offset: Offset,
    pub batches: &'a [SampleBatchSetup],
    #[default(DENSE_INDEX)]
    pub index_interval: ByteSize,
}

pub(super) fn seeded_segment(dir: &std::path::Path, setup: SeededSegmentSetup<'_>) -> Segment {
    let mut segment = Segment::create(dir, setup.offset).unwrap();
    for &batch in setup.batches {
        segment
            .append(&sample_batch(batch), setup.index_interval)
            .unwrap();
    }
    segment
}

pub(super) fn seeded_fixture(setup: SeededSegmentSetup<'_>) -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let segment = seeded_segment(dir.path(), setup);
    (dir, segment)
}

pub(super) fn three_single_record_batches() -> Vec<RecordBatch> {
    sample_batches(&[
        ONE_RECORD_BATCH,
        SECOND_SINGLE_RECORD_BATCH,
        THIRD_SINGLE_RECORD_BATCH,
    ])
}

/// An ordinary dense-index segment holding the first two records.
pub(super) fn two_record_fixture() -> (tempfile::TempDir, Segment) {
    seeded_fixture(SeededSegmentSetup {
        batches: &[TWO_RECORD_BATCH],
        ..Default::default()
    })
}

pub(super) fn indexed_segment() -> (tempfile::TempDir, Segment) {
    let dir = tempdir().unwrap();
    let segment = seeded_segment(
        dir.path(),
        crate::segment::test_support::SeededSegmentSetup {
            offset: crate::Offset(100),
            batches: INDEXED_READ_BATCHES,
            ..Default::default()
        },
    );
    (dir, segment)
}

pub(super) fn sample_batches(batches: &[SampleBatchSetup]) -> Vec<RecordBatch> {
    batches.iter().copied().map(sample_batch).collect()
}

/// `seg` with its I/O routed through a fresh
/// [`crate::test_support::RecordedAdvice`], which the caller keeps to read the
/// hints back.
pub(super) fn recording_advice(
    seg: &mut Segment,
) -> std::sync::Arc<crate::test_support::RecordedAdvice> {
    let advice = std::sync::Arc::new(crate::test_support::RecordedAdvice::default());
    seg.set_io(advice.clone());
    advice
}
