//! The Kafka `TimeIndex` on-disk layout and the relative-offset floor a
//! timestamp lookup scans from.
//!
//! An entry maps a timestamp to a relative offset. The index is sparse and
//! preallocated, so the floor lookup deliberately stops short of the first
//! entry at or above the target and treats non-increasing relative offsets as
//! trailing padding.

use {
    super::{RelativeOffset, TimestampMs, corrupt_index},
    crate::error::RemoteStorageError,
    krabka_verified::log_index::TimeEntry,
    zerocopy::{
        BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
        byteorder::{I64, U32},
    },
};

/// 12 bytes per entry: ts i64 BE, then rel u32 BE. It mirrors
/// `krabka_log::index::TimeEntryRaw`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct TimeIndexEntry {
    /// Largest record timestamp at or before the indexed offset.
    pub timestamp: I64<BigEndian>,
    /// Offset that carries the timestamp, relative to the segment's base
    /// offset.
    pub relative_offset: U32<BigEndian>,
}

/// Byte length of one serialized time-index entry.
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

const _: [(); 12] = [(); TIME_INDEX_ENTRY_LEN];

/// Borrows Kafka's `TimeIndex` on-disk format as a zero-copy
/// `&[TimeIndexEntry]`, at 12 bytes per entry: ts i64 BE, then rel u32 BE. It
/// ignores trailing bytes that do not complete a 12-byte entry. The result
/// borrows from `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes that
/// do not form an entry array.
pub fn parse_time_index(bytes: &[u8]) -> Result<&[TimeIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TIME_INDEX_ENTRY_LEN) * TIME_INDEX_ENTRY_LEN;
    <[TimeIndexEntry]>::ref_from_bytes(&bytes[..truncated_len]).map_err(|_| corrupt_index("time"))
}

/// Returns a safe relative-offset floor for an exact timestamp scan.
///
/// The last entry strictly below `target_ts` is used rather than an entry at
/// or above it: a sparse index entry is only a seek hint and may follow the
/// earliest qualifying record. Non-increasing relative offsets mark trailing
/// preallocation padding and end the usable index.
#[must_use]
pub fn relative_offset_floor_for_timestamp(
    entries: &[TimeIndexEntry],
    target_ts: TimestampMs,
) -> RelativeOffset {
    let decoded: Vec<TimeEntry> = entries
        .iter()
        .map(|entry| TimeEntry::new(entry.timestamp.get(), entry.relative_offset.get()))
        .collect();
    // The kernel ends the usable index at the padding itself.
    let candidate_count = krabka_verified::remote_time_index_candidate_count(&decoded, target_ts);
    candidate_count
        .checked_sub(1)
        .map_or(0, |index| decoded[index].coordinate)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A time index entry's decoded timestamp and relative offset.
    #[derive(Debug, PartialEq, Eq)]
    struct TimeSpec {
        timestamp: i64,
        relative_offset: u32,
    }

    const fn time_at(timestamp: i64, relative_offset: u32) -> TimeSpec {
        TimeSpec {
            timestamp,
            relative_offset,
        }
    }

    fn time_entries(specs: &[TimeSpec]) -> Vec<TimeIndexEntry> {
        specs
            .iter()
            .map(|spec| TimeIndexEntry {
                timestamp: I64::new(spec.timestamp),
                relative_offset: U32::new(spec.relative_offset),
            })
            .collect()
    }

    #[yare::parameterized(
        one_entry = { vec![(1_000_i64, 0_u32)] },
        two_entries = { vec![(1_000_i64, 0_u32), (2_000, 10)] },
        all_known_entries = { vec![(1_000_i64, 0_u32), (2_000, 10), (3_000, 20)] },
    )]
    fn parse_time_index_round_trips_known_entries(specs: Vec<(i64, u32)>) {
        let mut buf = Vec::new();
        for (ts, rel) in &specs {
            buf.extend_from_slice(&ts.to_be_bytes());
            buf.extend_from_slice(&rel.to_be_bytes());
        }
        let entries = parse_time_index(&buf).expect("valid time index");
        let decoded: Vec<TimeSpec> = entries
            .iter()
            .map(|e| time_at(e.timestamp.get(), e.relative_offset.get()))
            .collect();
        assert2::assert!(
            decoded
                == specs
                    .iter()
                    .map(|(ts, rel)| time_at(*ts, *rel))
                    .collect::<Vec<_>>()
        );
    }

    #[yare::parameterized(
        before_first = { 500, 0 },
        exact_first = { 1_000, 0 },
        between_entries = { 1_500, 0 },
        duplicate_exact_matches = { 2_000, 0 },
        between_duplicate_and_last = { 2_500, 20 },
        after_last = { 4_000, 30 },
    )]
    fn timestamp_floor_covers_strict_predecessor_boundaries(ts: i64, want: u32) {
        let entries = time_entries(&[
            time_at(1_000, 0),
            time_at(2_000, 10),
            time_at(2_000, 20),
            time_at(3_000, 30),
        ]);
        assert2::assert!(relative_offset_floor_for_timestamp(&entries, ts) == want);
    }

    #[test]
    fn timestamp_floor_for_empty_index_is_zero() {
        assert!(relative_offset_floor_for_timestamp(&[], 1_000) == 0);
    }

    #[yare::parameterized(
        zero_interval = { 0 },
        sparse_interval = { 65_536 },
    )]
    fn archived_segment_timestamp_scan_matches_local_and_record_oracle(interval_bytes: u32) {
        use {
            crate::{
                first_record_at_or_after_timestamp, parse_offset_index,
                position_for_relative_offset,
            },
            krabka_ids::Offset,
            krabka_log::Segment,
            krabka_protocol::records::{Record, RecordBatch},
            krabka_units::prelude::bytes,
        };

        let timestamps = [[100, 100], [500, 100], [100, 300]];
        let interval = bytes(interval_bytes);
        let dir = tempfile::tempdir().unwrap();
        let mut segment = Segment::create(dir.path(), Offset(100)).unwrap();
        let mut records = Vec::new();
        for (batch_index, timestamps) in timestamps.iter().enumerate() {
            let base = 100 + i64::try_from(batch_index).unwrap() * 3;
            let mut batch = RecordBatch {
                base_offset: base,
                last_offset_delta: 1,
                base_timestamp: timestamps[0],
                max_timestamp: *timestamps.iter().max().unwrap(),
                ..Default::default()
            };
            for (delta, timestamp) in timestamps.iter().enumerate() {
                let delta = i32::try_from(delta).unwrap();
                batch.records.push(Record {
                    offset_delta: delta,
                    timestamp_delta: timestamp - timestamps[0],
                    ..Default::default()
                });
                records.push((base + i64::from(delta), *timestamp));
            }
            segment.append(&batch, interval).unwrap();
        }
        segment.flush().unwrap();
        let time_bytes = std::fs::read(dir.path().join("00000000000000000100.timeindex")).unwrap();
        let offset_bytes = std::fs::read(dir.path().join("00000000000000000100.index")).unwrap();
        let log_bytes = std::fs::read(dir.path().join("00000000000000000100.log")).unwrap();
        let mut padded = time_bytes.clone();
        padded.extend_from_slice(&[0; 24]);
        for time_bytes in [&time_bytes[..], &padded[..]] {
            for target in [i64::MIN, 100, 150, 300, 500, i64::MAX] {
                let relative = relative_offset_floor_for_timestamp(
                    parse_time_index(time_bytes).unwrap(),
                    target,
                );
                let position = position_for_relative_offset(
                    parse_offset_index(&offset_bytes).unwrap(),
                    relative,
                );
                let remote = first_record_at_or_after_timestamp(
                    &log_bytes[usize::try_from(position).unwrap()..],
                    100 + i64::from(relative),
                    target,
                    None,
                )
                .unwrap();
                let expected = records
                    .iter()
                    .copied()
                    .find(|(_, timestamp)| *timestamp >= target);
                let local = segment
                    .offset_for_timestamp(target)
                    .map(|(offset, timestamp)| (offset.0, timestamp));
                assert2::assert!(remote == expected);
                assert2::assert!(local == expected);
            }
        }
    }

    #[test]
    fn timestamp_floor_ignores_trailing_index_padding() {
        let entries = time_entries(&[
            time_at(1_000, 0),
            time_at(2_000, 10),
            time_at(0, 0),
            time_at(0, 0),
        ]);
        assert!(relative_offset_floor_for_timestamp(&entries, 3_000) == 10);
    }
}
