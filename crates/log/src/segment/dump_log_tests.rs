//! What `kafka-dump-log` checks of a segment's `.index` and `.timeindex`
//! (Kafka's `DumpLogSegments.dumpIndex` and `dumpTimeIndex`), applied to the
//! files krabka writes on every path that writes them: the encoding append,
//! the verbatim append, and the tail recovery that rebuilds them after a
//! reopen or a compaction.
//!
//! The checker reads the three files straight from disk, the way the tool
//! does, and shares nothing with the code that wrote them.

use {
    super::{
        Segment,
        test_support::{DENSE_INDEX, sample_batch},
    },
    crate::{
        index::{OffsetIndexEntry, TimeIndexEntry},
        name,
        segment::test_support::{ONE_RECORD_BATCH, TempSegment, test_segment},
    },
    bytes::BytesMut,
    krabka_ids::{LeaderEpoch, Offset},
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSize, bytes},
    std::path::Path,
    tempfile::tempdir,
};

/// The entries of a `.index` file.
fn offset_entries(dir: &Path, base: i64) -> Vec<OffsetIndexEntry> {
    let bytes = std::fs::read(name::index_path(dir, base)).unwrap();
    bytes
        .as_chunks::<8>()
        .0
        .iter()
        .map(|entry| OffsetIndexEntry {
            relative_offset: u32::from_be_bytes(entry[0..4].try_into().unwrap()),
            position: u32::from_be_bytes(entry[4..8].try_into().unwrap()),
        })
        .collect()
}

/// The entries of a `.timeindex` file.
fn time_entries(dir: &Path, base: i64) -> Vec<TimeIndexEntry> {
    let bytes = std::fs::read(name::timeindex_path(dir, base)).unwrap();
    bytes
        .as_chunks::<12>()
        .0
        .iter()
        .map(|entry| TimeIndexEntry {
            timestamp: i64::from_be_bytes(entry[0..8].try_into().unwrap()),
            relative_offset: u32::from_be_bytes(entry[8..12].try_into().unwrap()),
        })
        .collect()
}

/// One expected `.timeindex` entry.
fn time_entry(timestamp: i64, relative_offset: u32) -> TimeIndexEntry {
    TimeIndexEntry {
        timestamp,
        relative_offset,
    }
}

/// A decoded batch and the byte position in the `.log` file where it starts.
struct BatchAtPosition {
    position: u32,
    batch: RecordBatch,
}

/// Every problem `kafka-dump-log --files <segment>.index/.timeindex` reports:
/// the batch at an offset-index entry's position must end at the entry's
/// offset; each time-index entry's offset must be the last offset of a batch
/// whose newest record carries the entry's timestamp; and the timestamps must
/// strictly increase.
fn dump_log_problems(dir: &Path, base: i64) -> Vec<String> {
    let log = std::fs::read(name::log_path(dir, base)).unwrap();
    let mut batches: Vec<BatchAtPosition> = Vec::new();
    let mut cursor: &[u8] = &log;
    while !cursor.is_empty() {
        let position = u32::try_from(log.len() - cursor.len()).unwrap();
        batches.push(BatchAtPosition {
            position,
            batch: RecordBatch::decode(&mut cursor).unwrap(),
        });
    }
    let last_offset = |batch: &RecordBatch| batch.base_offset + i64::from(batch.last_offset_delta);

    let mut problems = Vec::new();
    for OffsetIndexEntry {
        relative_offset: relative,
        position,
    } in offset_entries(dir, base)
    {
        let entry_offset = base + i64::from(relative);
        match batches.iter().find(|found| found.position == position) {
            None => problems.push(format!("no batch starts at position {position}")),
            Some(BatchAtPosition { batch, .. }) if last_offset(batch) != entry_offset => problems
                .push(format!(
                    "offset entry {entry_offset} at position {position}: the batch ends at {}",
                    last_offset(batch)
                )),
            Some(_) => {}
        }
    }
    let mut previous_timestamp = -1;
    for TimeIndexEntry {
        timestamp,
        relative_offset: relative,
    } in time_entries(dir, base)
    {
        let entry_offset = base + i64::from(relative);
        match batches
            .iter()
            .find(|found| last_offset(&found.batch) >= entry_offset)
        {
            None => problems.push(format!("time entry offset {entry_offset} is past the log")),
            Some(BatchAtPosition { batch, .. }) if last_offset(batch) != entry_offset => problems
                .push(format!(
                    "shallow offset {entry_offset} not found, the next batch ends at {}",
                    last_offset(batch)
                )),
            Some(BatchAtPosition { batch, .. }) => {
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

/// A batch to write: its base offset, record count, and base timestamp.
#[derive(Clone, Copy)]
struct BatchSpec {
    base_offset: i64,
    records: i32,
    timestamp: i64,
}

/// Batches `(base offset, records, base timestamp)`. Every record's timestamp
/// is the base plus its position, so a batch's newest is `base + records - 1`.
///
/// The running maximum after each batch is 102, 102, 201, 201, 300: the second
/// batch is older than the first and the fourth ties the third, so neither may
/// add a time-index entry, and the entries that do exist point at the batch
/// that set the maximum, not at the batch that happened to be indexed.
const BATCHES: [BatchSpec; 5] = [
    BatchSpec {
        base_offset: 0,
        records: 3,
        timestamp: 100,
    },
    BatchSpec {
        base_offset: 3,
        records: 1,
        timestamp: 50,
    },
    BatchSpec {
        base_offset: 4,
        records: 2,
        timestamp: 200,
    },
    BatchSpec {
        base_offset: 6,
        records: 1,
        timestamp: 201,
    },
    BatchSpec {
        base_offset: 7,
        records: 1,
        timestamp: 300,
    },
];

/// A batch and its wire bytes.
struct EncodedBatch {
    batch: RecordBatch,
    wire: bytes::Bytes,
}

fn encoded(base_offset: i64, records: i32, timestamp: i64) -> EncodedBatch {
    let batch = sample_batch()
        .offset(base_offset)
        .records(records)
        .timestamp(timestamp)
        .call();
    let mut wire = BytesMut::new();
    batch.encode(&mut wire).unwrap();
    EncodedBatch {
        batch,
        wire: wire.freeze(),
    }
}

fn write_all_encoded(segment: &mut Segment, interval: ByteSize) {
    for BatchSpec {
        base_offset: base,
        records,
        timestamp,
    } in BATCHES
    {
        segment
            .append(
                &sample_batch()
                    .offset(base)
                    .records(records)
                    .timestamp(timestamp)
                    .call(),
                interval,
            )
            .unwrap();
    }
}

fn write_all_verbatim(segment: &mut Segment, interval: ByteSize) {
    for BatchSpec {
        base_offset: base,
        records,
        timestamp,
    } in BATCHES
    {
        let EncodedBatch { batch, wire } = encoded(base, records, timestamp);
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
#[yare::parameterized(
    encoding_append = { "encoding append", false },
    verbatim_append = { "verbatim append", true },
)]
fn the_append_paths_write_the_indexes_kafka_dump_log_verifies(label: &'static str, verbatim: bool) {
    let expected_time_entries = vec![time_entry(102, 2), time_entry(201, 5), time_entry(300, 7)];
    let TempSegment { dir, mut segment } = test_segment();
    if verbatim {
        write_all_verbatim(&mut segment, DENSE_INDEX);
    } else {
        write_all_encoded(&mut segment, DENSE_INDEX);
    }

    let offsets: Vec<u32> = offset_entries(dir.path(), 0)
        .iter()
        .map(|entry| entry.relative_offset)
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

/// A sparse index takes an entry only once the batches since the last entry
/// add up to the interval, and what it takes still verifies.
#[test]
fn a_sparse_index_still_verifies() {
    let TempSegment { dir, mut segment } = test_segment();
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
    /// An interval and the relative last offsets that get an entry under it.
    struct IntervalCase {
        label: &'static str,
        interval: u64,
        expected: Vec<u32>,
    }
    let size = u64::try_from(
        sample_batch()
            .offset(ONE_RECORD_BATCH.offset.0)
            .records(ONE_RECORD_BATCH.records.0)
            .timestamp(ONE_RECORD_BATCH.timestamp.0)
            .call()
            .encoded_len(),
    )
    .unwrap();
    let cases = [
        IntervalCase {
            label: "an interval of one batch skips a batch exactly that far",
            interval: size,
            expected: vec![2, 4],
        },
        IntervalCase {
            label: "an interval a byte short of a batch indexes the next batch",
            interval: size - 1,
            expected: vec![1, 2, 3, 4],
        },
        IntervalCase {
            label: "an interval past every batch indexes nothing",
            interval: 10 * size,
            expected: vec![],
        },
        IntervalCase {
            label: "a zero interval indexes every batch but the first",
            interval: 0,
            expected: vec![1, 2, 3, 4],
        },
    ];
    for IntervalCase {
        label,
        interval,
        expected,
    } in cases
    {
        let dir = tempdir().unwrap();
        let interval = bytes(u32::try_from(interval).unwrap());
        {
            let mut segment = Segment::create(dir.path(), Offset(0)).unwrap();
            for offset in 0..5 {
                segment
                    .append(
                        &sample_batch().offset(offset).timestamp(100 + offset).call(),
                        interval,
                    )
                    .unwrap();
            }
        }
        let entries = |dir: &Path| -> Vec<u32> {
            offset_entries(dir, 0)
                .iter()
                .map(|entry| entry.relative_offset)
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
    let TempSegment { dir, mut segment } = test_segment();
    write_all_encoded(&mut segment, DENSE_INDEX);

    // Keep the batches ending at 2, 3 and 5.
    segment.truncate_to_relative(6).unwrap();
    assert2::assert!(time_entries(dir.path(), 0) == vec![time_entry(102, 2), time_entry(201, 5)]);
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());

    // Older than the surviving maximum: no time entry, one offset entry.
    segment
        .append(
            &sample_batch().offset(6).records(2).timestamp(150).call(),
            DENSE_INDEX,
        )
        .unwrap();
    assert2::assert!(time_entries(dir.path(), 0) == vec![time_entry(102, 2), time_entry(201, 5)]);
    // Newer: the entry names the new batch's last offset.
    segment
        .append(
            &sample_batch().offset(8).records(2).timestamp(400).call(),
            DENSE_INDEX,
        )
        .unwrap();
    assert2::assert!(
        time_entries(dir.path(), 0)
            == vec![time_entry(102, 2), time_entry(201, 5), time_entry(401, 9)]
    );
    assert2::assert!(dump_log_problems(dir.path(), 0) == Vec::<String>::new());
}

/// A timestamp search still lands on the first record at or after the target
/// when the time index names the batch that set the maximum, including a
/// record in the middle of that batch.
#[test]
fn a_timestamp_search_finds_the_record_inside_the_batch_that_set_the_maximum() {
    let TempSegment {
        dir: _dir,
        mut segment,
    } = test_segment();
    write_all_encoded(&mut segment, DENSE_INDEX);

    // Batch (4, 2 records, 200): records at offsets 4 and 5 with timestamps
    // 200 and 201.
    assert2::assert!(segment.offset_for_timestamp(200) == Some((Offset(4), 200)));
    assert2::assert!(segment.offset_for_timestamp(201) == Some((Offset(5), 201)));
    assert2::assert!(segment.offset_for_timestamp(101) == Some((Offset(1), 101)));
    assert2::assert!(segment.offset_for_timestamp(301).is_none());
}
