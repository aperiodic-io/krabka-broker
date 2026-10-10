//! What `kafka-dump-log` checks of a segment's `.index` and `.timeindex`
//! (Kafka's `DumpLogSegments.dumpIndex` and `dumpTimeIndex`), applied to the
//! files krabka writes on every path that writes them: the encoding append,
//! the verbatim append, and the tail recovery that rebuilds them after a
//! reopen or a compaction.
//!
//! The checker reads the three files straight from disk, the way the tool
//! does, and shares nothing with the code that wrote them.

use std::path::Path;

use bytes::BytesMut;
use krabka_ids::{LeaderEpoch, Offset};
use krabka_protocol::records::RecordBatch;
use krabka_units::prelude::{ByteSize, bytes};
use tempfile::tempdir;

use super::{
    Segment,
    test_support::{DENSE_INDEX, sample_batch},
};
use crate::name;

/// The `(relative last offset, position)` entries of a `.index` file.
fn offset_entries(dir: &Path, base: i64) -> Vec<(u32, u32)> {
    let bytes = std::fs::read(name::index_path(dir, base)).unwrap();
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|entry| {
            (
                u32::from_be_bytes(entry[0..4].try_into().unwrap()),
                u32::from_be_bytes(entry[4..8].try_into().unwrap()),
            )
        })
        .collect()
}

/// The `(timestamp, relative offset)` entries of a `.timeindex` file.
fn time_entries(dir: &Path, base: i64) -> Vec<(i64, u32)> {
    let bytes = std::fs::read(name::timeindex_path(dir, base)).unwrap();
    bytes
        .as_chunks::<12>()
        .0
        .iter()
        .map(|entry| {
            (
                i64::from_be_bytes(entry[0..8].try_into().unwrap()),
                u32::from_be_bytes(entry[8..12].try_into().unwrap()),
            )
        })
        .collect()
}

/// Every problem `kafka-dump-log --files <segment>.index/.timeindex` reports:
/// the batch at an offset-index entry's position must end at the entry's
/// offset; each time-index entry's offset must be the last offset of a batch
/// whose newest record carries the entry's timestamp; and the timestamps must
/// strictly increase.
fn dump_log_problems(dir: &Path, base: i64) -> Vec<String> {
    let log = std::fs::read(name::log_path(dir, base)).unwrap();
    let mut batches: Vec<(u32, RecordBatch)> = Vec::new();
    let mut cursor: &[u8] = &log;
    while !cursor.is_empty() {
        let position = u32::try_from(log.len() - cursor.len()).unwrap();
        batches.push((position, RecordBatch::decode(&mut cursor).unwrap()));
    }
    let last_offset = |batch: &RecordBatch| batch.base_offset + i64::from(batch.last_offset_delta);

    let mut problems = Vec::new();
    for (relative, position) in offset_entries(dir, base) {
        let entry_offset = base + i64::from(relative);
        match batches.iter().find(|(start, _)| *start == position) {
            None => problems.push(format!("no batch starts at position {position}")),
            Some((_, batch)) if last_offset(batch) != entry_offset => problems.push(format!(
                "offset entry {entry_offset} at position {position}: the batch ends at {}",
                last_offset(batch)
            )),
            Some(_) => {}
        }
    }
    let mut previous_timestamp = -1;
    for (timestamp, relative) in time_entries(dir, base) {
        let entry_offset = base + i64::from(relative);
        match batches
            .iter()
            .find(|(_, batch)| last_offset(batch) >= entry_offset)
        {
            None => problems.push(format!("time entry offset {entry_offset} is past the log")),
            Some((_, batch)) if last_offset(batch) != entry_offset => problems.push(format!(
                "shallow offset {entry_offset} not found, the next batch ends at {}",
                last_offset(batch)
            )),
            Some((_, batch)) => {
                let newest = batch
                    .records
                    .iter()
                    .map(|record| batch.base_timestamp + record.timestamp_delta)
                    .max();
                if newest != Some(timestamp) {
                    problems.push(format!(
                        "time entry {timestamp} at offset {entry_offset}: the batch's newest record is {newest:?}"
                    ));
                }
            }
        }
        if previous_timestamp >= timestamp {
            problems.push(format!(
                "timestamp {timestamp} is not above the previous entry's {previous_timestamp}"
            ));
        }
        previous_timestamp = timestamp;
    }
    problems
}

/// Batches `(base offset, records, base timestamp)`. Every record's timestamp
/// is the base plus its position, so a batch's newest is `base + records - 1`.
///
/// The running maximum after each batch is 102, 102, 201, 201, 300: the second
/// batch is older than the first and the fourth ties the third, so neither may
/// add a time-index entry, and the entries that do exist point at the batch
/// that set the maximum, not at the batch that happened to be indexed.
const BATCHES: [(i64, i32, i64); 5] = [
    (0, 3, 100),
    (3, 1, 50),
    (4, 2, 200),
    (6, 1, 201),
    (7, 1, 300),
];

fn encoded(base_offset: i64, records: i32, timestamp: i64) -> (RecordBatch, bytes::Bytes) {
    let batch = sample_batch(crate::segment::test_support::SampleBatchSetup {
        offset: crate::Offset(base_offset),
        records: crate::segment::test_support::RecordCount(records),
        timestamp: crate::segment::test_support::RecordTimestamp(timestamp),
    });
    let mut wire = BytesMut::new();
    batch.encode(&mut wire).unwrap();
    (batch, wire.freeze())
}

fn write_all_encoded(segment: &mut Segment, interval: ByteSize) {
    for (base, records, timestamp) in BATCHES {
        segment
            .append(
                &sample_batch(crate::segment::test_support::SampleBatchSetup {
                    offset: crate::Offset(base),
                    records: crate::segment::test_support::RecordCount(records),
                    timestamp: crate::segment::test_support::RecordTimestamp(timestamp),
                }),
                interval,
            )
            .unwrap();
    }
}

fn write_all_verbatim(segment: &mut Segment, interval: ByteSize) {
    for (base, records, timestamp) in BATCHES {
        let (batch, wire) = encoded(base, records, timestamp);
        segment
            .append_verbatim(
                &wire,
                Offset(base),
                batch.last_offset_delta,
                batch.max_timestamp,
                LeaderEpoch(0),
                interval,
            )
            .unwrap();
    }
}

/// `.index` entries are `(last offset of the batch, position of the batch)`,
/// and `.timeindex` entries are `(running maximum, last offset of the batch
/// that set it)` that strictly increase in timestamp. The last offsets are 2,
/// 3, 5, 6 and 7, and the maximum is set by the batches ending at 2, 5 and 7.
///
/// The first batch takes no `.index` entry: Kafka's `LogSegment.append`
/// indexes a batch once more than `index.interval.bytes` were written since
/// the last entry, and nothing was written before the first. The second
/// batch's entry carries the maximum the first one set, so the time index
/// still names the batch ending at 2.
#[test]
fn the_append_paths_write_the_indexes_kafka_dump_log_verifies() {
    let expected_time_entries = vec![(102, 2), (201, 5), (300, 7)];
    for (label, verbatim) in [("encoding append", false), ("verbatim append", true)] {
        let (dir, mut segment) = crate::segment::test_support::test_segment();
        if verbatim {
            write_all_verbatim(&mut segment, DENSE_INDEX);
        } else {
            write_all_encoded(&mut segment, DENSE_INDEX);
        }

        let offsets: Vec<u32> = offset_entries(dir.path(), 0)
            .iter()
            .map(|(relative, _)| *relative)
            .collect();
        assert2::assert!(offsets == vec![3, 5, 6, 7], "{label}");
        assert2::assert!(
            time_entries(dir.path(), 0) == expected_time_entries,
            "{label}"
        );
        assert2::assert!(
            dump_log_problems(dir.path(), 0) == Vec::<String>::new(),
            "{label}"
        );
    }
}

/// A sparse index takes an entry only once the batches since the last entry
/// add up to the interval, and what it takes still verifies.
#[test]
fn a_sparse_index_still_verifies() {
    let (dir, mut segment) = crate::segment::test_support::test_segment();
    write_all_encoded(&mut segment, bytes(200));

    let entries = offset_entries(dir.path(), 0).len();
    assert2::assert!(0 < entries && entries < BATCHES.len());
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());
}

/// The tail recovery a reopen or a compaction swap runs rebuilds the same
/// indexes the append wrote, byte for byte, and they verify.
#[test]
fn recovery_rebuilds_the_indexes_the_append_wrote() {
    let dir = tempdir().unwrap();
    {
        let mut segment = Segment::create(dir.path(), Offset(0)).unwrap();
        write_all_encoded(&mut segment, DENSE_INDEX);
    }
    let index = std::fs::read(name::index_path(dir.path(), 0)).unwrap();
    let time_index = std::fs::read(name::timeindex_path(dir.path(), 0)).unwrap();

    let recovered =
        Segment::open_active_with_index_interval(dir.path(), Offset(0), true, DENSE_INDEX).unwrap();

    assert2::assert!(std::fs::read(name::index_path(dir.path(), 0)).unwrap() == index);
    assert2::assert!(std::fs::read(name::timeindex_path(dir.path(), 0)).unwrap() == time_index);
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());
    assert2::assert!(recovered.max_timestamp() == 300);
}

/// Kafka's `LogSegment.append` indexes a batch when `bytesSinceLastIndexEntry >
/// indexIntervalBytes`, a strict comparison over the bytes written since the
/// last entry, which start at zero: the first batch takes no entry, and a batch
/// exactly one interval past the last one takes none either (#1198). The
/// recovery a reopen runs applies the same rule (`LogSegment.recover`). Each
/// case is `(label, the interval in batch sizes and bytes, the relative last
/// offsets that get an entry)`, over five one-record batches of one size.
#[test]
fn the_index_takes_an_entry_only_once_more_than_the_interval_was_written() {
    let size =
        u64::try_from(sample_batch(crate::segment::test_support::ONE_RECORD_BATCH).encoded_len())
            .unwrap();
    let cases: [(&str, u64, Vec<u32>); 4] = [
        (
            "an interval of one batch skips a batch exactly that far",
            size,
            vec![2, 4],
        ),
        (
            "an interval a byte short of a batch indexes the next batch",
            size - 1,
            vec![1, 2, 3, 4],
        ),
        (
            "an interval past every batch indexes nothing",
            10 * size,
            vec![],
        ),
        (
            "a zero interval indexes every batch but the first",
            0,
            vec![1, 2, 3, 4],
        ),
    ];
    for (label, interval, expected) in cases {
        let dir = tempdir().unwrap();
        let interval = bytes(u32::try_from(interval).unwrap());
        {
            let mut segment = Segment::create(dir.path(), Offset(0)).unwrap();
            for offset in 0..5 {
                segment
                    .append(
                        &sample_batch(crate::segment::test_support::SampleBatchSetup {
                            offset: crate::Offset(offset),
                            timestamp: crate::segment::test_support::RecordTimestamp(100 + offset),
                            ..Default::default()
                        }),
                        interval,
                    )
                    .unwrap();
            }
        }
        let entries = |dir: &Path| -> Vec<u32> {
            offset_entries(dir, 0)
                .iter()
                .map(|(relative, _)| *relative)
                .collect()
        };

        assert2::assert!(entries(dir.path()) == expected, "append: {label}");

        // The reopen rebuilds the index from the log.
        std::fs::write(name::index_path(dir.path(), 0), []).unwrap();
        Segment::open_active_with_index_interval(dir.path(), Offset(0), true, interval).unwrap();
        assert2::assert!(entries(dir.path()) == expected, "recovery: {label}");
    }
}

/// A truncation drops the entries of the batches it removes, and an append
/// after it goes on to write entries that verify: the running maximum the
/// segment keeps is the one of the batches that survived.
#[test]
fn appends_after_a_truncation_keep_the_indexes_verifying() {
    let (dir, mut segment) = crate::segment::test_support::test_segment();
    write_all_encoded(&mut segment, DENSE_INDEX);

    // Keep the batches ending at 2, 3 and 5.
    segment.truncate_to_relative(6).unwrap();
    assert2::assert!(time_entries(dir.path(), 0) == vec![(102, 2), (201, 5)]);
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());

    // Older than the surviving maximum: no time entry, one offset entry.
    segment
        .append(
            &sample_batch(crate::segment::test_support::SampleBatchSetup {
                offset: crate::Offset(6),
                records: crate::segment::test_support::RecordCount(2),
                timestamp: crate::segment::test_support::RecordTimestamp(150),
            }),
            DENSE_INDEX,
        )
        .unwrap();
    assert2::assert!(time_entries(dir.path(), 0) == vec![(102, 2), (201, 5)]);
    // Newer: the entry names the new batch's last offset.
    segment
        .append(
            &sample_batch(crate::segment::test_support::SampleBatchSetup {
                offset: crate::Offset(8),
                records: crate::segment::test_support::RecordCount(2),
                timestamp: crate::segment::test_support::RecordTimestamp(400),
            }),
            DENSE_INDEX,
        )
        .unwrap();
    assert2::assert!(time_entries(dir.path(), 0) == vec![(102, 2), (201, 5), (401, 9)]);
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());
}

/// A timestamp search still lands on the first record at or after the target
/// when the time index names the batch that set the maximum, including a
/// record in the middle of that batch.
#[test]
fn a_timestamp_search_finds_the_record_inside_the_batch_that_set_the_maximum() {
    let (_dir, mut segment) = crate::segment::test_support::test_segment();
    write_all_encoded(&mut segment, DENSE_INDEX);

    // Batch (4, 2 records, 200): records at offsets 4 and 5 with timestamps
    // 200 and 201.
    assert2::assert!(segment.offset_for_timestamp(200) == Some((Offset(4), 200)));
    assert2::assert!(segment.offset_for_timestamp(201) == Some((Offset(5), 201)));
    assert2::assert!(segment.offset_for_timestamp(101) == Some((Offset(1), 101)));
    assert2::assert!(segment.offset_for_timestamp(301).is_none());
}
