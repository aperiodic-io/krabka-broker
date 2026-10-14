//! Scans over a remote segment's `.log` bytes, which the offset index can only
//! point near.
//!
//! Kafka offset and time indexes are sparse, so a byte range fetched from the
//! object store usually starts before the record the caller asked for. These
//! scans decode the batches in that range and pick the first one, or the first
//! record, that satisfies the request.

use {
    super::{LogOffset, TimestampMs, corrupt_log},
    crate::error::RemoteStorageError,
    krabka_protocol::records::{Attributes, RecordBatch, TimestampType},
};

/// Decodes remote log batches and returns the earliest record at or after both
/// `floor_offset` and `target_timestamp`. Under `LogAppendTime`, every record
/// uses the batch append stamp instead of producer base time and deltas.
///
/// `max_record_body` is Kafka trunk's `max.decompressed.message.bytes`, or
/// `None` for no limit. Kafka's `RemoteLogManager.lookupTimestamp` decompresses
/// a batch only when its max timestamp reaches `target_timestamp` and its last
/// offset reaches `floor_offset`, and it reads that batch a record at a time up
/// to the record it returns, so only those records are held to the limit. A
/// batch the producer did not compress never is.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when a batch does not decode, or when a
/// record's offset or timestamp delta overflows its base, and
/// [`RemoteStorageError::RecordTooLarge`] for a record above `max_record_body`.
pub fn first_record_at_or_after_timestamp(
    data: &[u8],
    floor_offset: LogOffset,
    target_timestamp: TimestampMs,
    max_record_body: Option<usize>,
) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
    let mut cur = data;
    while !cur.is_empty() {
        let batch = RecordBatch::decode(&mut cur).map_err(corrupt_log)?;
        let limit = max_record_body.filter(|_| {
            batch.attributes.compression() != Attributes::default().compression()
                && batch.max_timestamp >= target_timestamp
                && batch
                    .base_offset
                    .checked_add(i64::from(batch.last_offset_delta))
                    .is_some_and(|last_offset| last_offset >= floor_offset)
        });
        let append_time = (batch.attributes.timestamp_type() == TimestampType::LogAppendTime)
            .then_some(batch.max_timestamp);
        for record in &batch.records {
            if let Some(limit) = limit {
                let size = record.body_len();
                if size > limit {
                    return Err(RemoteStorageError::RecordTooLarge { size, limit });
                }
            }
            let offset = batch
                .base_offset
                .checked_add(i64::from(record.offset_delta))
                .ok_or_else(|| corrupt_log("record offset overflow"))?;
            if offset < floor_offset {
                continue;
            }
            let timestamp = krabka_verified::timestamp::timestamp_record_time(
                batch.base_timestamp,
                record.timestamp_delta,
                append_time,
            )
            .ok_or_else(|| corrupt_log("record timestamp overflow"))?;
            if timestamp >= target_timestamp {
                return Ok(Some((offset, timestamp)));
            }
        }
    }
    Ok(None)
}

/// Decodes batches from `data` and returns the first one whose last offset is
/// `>= floor`. It skips the batches at the start of the returned byte range
/// that the offset index pointed at but that do not cover the requested
/// offset. Kafka offset indexes are sparse, so such batches occur.
#[must_use]
pub fn first_batch_at_or_after(data: &[u8], floor: LogOffset) -> Option<RecordBatch> {
    let mut cur: &[u8] = data;
    while !cur.is_empty() {
        let Ok(batch) = RecordBatch::decode(&mut cur) else {
            break;
        };
        let last_offset = batch
            .base_offset
            .checked_add(i64::from(batch.last_offset_delta))?;
        if last_offset >= floor {
            return Some(batch);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        bytes::{Bytes, BytesMut},
        krabka_protocol::records::Record,
    };

    fn test_batch_at(base_offset: i64, record_count: i32, value_byte: u8) -> RecordBatch {
        let mut batch = RecordBatch {
            base_offset,
            last_offset_delta: record_count - 1,
            ..RecordBatch::default()
        };
        for offset_delta in 0..record_count {
            batch.records.push(Record {
                offset_delta,
                value: Some(Bytes::from(vec![value_byte; 4])),
                ..Default::default()
            });
        }
        batch
    }

    krabka_macros::timestamped_batch!(timestamped_batch_at);

    fn encoded(batches: &[RecordBatch]) -> Bytes {
        let mut buf = BytesMut::new();
        for batch in batches {
            batch.encode(&mut buf).unwrap();
        }
        buf.freeze()
    }

    #[yare::parameterized(
        skips_first_batch = { 10, Some(10) },
        below_all_batches = { 0, Some(0) },
        above_all_batches = { 1_000, None },
    )]
    fn first_batch_at_or_after_decodes_and_skips(floor: i64, want_base: Option<i64>) {
        // Two adjacent batches; floor=10 should skip the first (last=9) and
        // return the second.
        let bytes = encoded(&[test_batch_at(0, 10, b'a'), test_batch_at(10, 10, b'b')]);
        assert2::assert!(
            first_batch_at_or_after(&bytes, floor).map(|b| b.base_offset) == want_base
        );
    }

    #[test]
    fn first_batch_at_or_after_empty_buffer_returns_none() {
        assert!(first_batch_at_or_after(&[], 0).is_none());
    }

    #[test]
    fn first_batch_at_or_after_rejects_floor_past_base_plus_delta() {
        let bytes = encoded(&[test_batch_at(3, 4, b'z')]);

        assert2::assert!(
            first_batch_at_or_after(&bytes, 7).is_none(),
            "batch 3..6 must not cover floor 7"
        );
    }

    #[test]
    fn first_batch_at_or_after_rejects_offset_overflow() {
        let bytes = encoded(&[test_batch_at(i64::MAX, 2, b'z')]);
        assert!(first_batch_at_or_after(&bytes, i64::MAX).is_none());
    }

    #[yare::parameterized(
        both_floors_inside_first_batch = { 10, 1_600, Some((12, 1_600)) },
        offset_floor_skips_match = { 14, 1_000, Some((14, 2_000)) },
        no_timestamp_match = { 10, 9_999, None },
    )]
    fn first_record_at_or_after_timestamp_honours_both_floors(
        floor_offset: i64,
        target: i64,
        want: Option<(i64, i64)>,
    ) {
        let bytes = encoded(&[
            timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a'),
            timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b'),
        ]);
        assert2::assert!(
            first_record_at_or_after_timestamp(&bytes, floor_offset, target, None).unwrap() == want
        );
    }

    #[yare::parameterized(
        ordinary_and_first_match = { 1_000, 0, 10, 1_500, Some((10, 2_400)) },
        ordinary_and_second_match = { 1_000, 0, 11, 2_400, Some((11, 2_400)) },
        ordinary_offset_past_batch = { 1_000, 0, 12, 0, None },
        ordinary_timestamp_past_max = { 1_000, 0, 10, 2_401, None },
        max_base_and_first_match = { i64::MAX, 1, 10, 1_500, Some((10, 2_400)) },
        max_base_and_second_match = { i64::MAX, 1, 11, 2_400, Some((11, 2_400)) },
        max_base_and_offset_past_batch = { i64::MAX, 1, 12, 0, None },
        max_base_and_timestamp_past_max = { i64::MAX, 1, 10, 2_401, None },
        min_base_and_first_match = { i64::MIN, -1, 10, 1_500, Some((10, 2_400)) },
        min_base_and_second_match = { i64::MIN, -1, 11, 2_400, Some((11, 2_400)) },
        min_base_and_offset_past_batch = { i64::MIN, -1, 12, 0, None },
        min_base_and_timestamp_past_max = { i64::MIN, -1, 10, 2_401, None },
    )]
    fn append_time_lookup_ignores_producer_timestamps(
        base: i64,
        delta: i64,
        floor: i64,
        target: i64,
        expected: Option<(i64, i64)>,
    ) {
        let mut batch = timestamped_batch_at(10, &[1_000, 1_100], b'a');
        batch.attributes = batch
            .attributes
            .with_timestamp_type(TimestampType::LogAppendTime);
        batch.max_timestamp = 2_400;
        batch.base_timestamp = base;
        for record in &mut batch.records {
            record.timestamp_delta = delta;
        }
        let bytes = encoded(&[batch]);
        assert2::assert!(
            first_record_at_or_after_timestamp(&bytes, floor, target, None).unwrap() == expected
        );
    }

    #[test]
    fn first_record_at_or_after_timestamp_reports_corrupt_bytes() {
        // A truncated batch header decodes to an error, never a panic.
        let bytes = encoded(&[test_batch_at(0, 2, b'a')]);
        let error = first_record_at_or_after_timestamp(&bytes[..12], 0, 0, None).unwrap_err();
        assert!(matches!(error, RemoteStorageError::Io(_)));
    }

    /// A record's timestamp and the length of its value.
    struct SizedRecord {
        timestamp: i64,
        value_len: usize,
    }

    const fn sized(timestamp: i64, value_len: usize) -> SizedRecord {
        SizedRecord {
            timestamp,
            value_len,
        }
    }

    /// A batch at `base_offset` with one record per [`SizedRecord`], compressed
    /// with `codec`.
    fn sized_batch_at(
        base_offset: i64,
        records: &[SizedRecord],
        codec: krabka_compression::CompressionType,
    ) -> RecordBatch {
        let timestamps: Vec<i64> = records.iter().map(|record| record.timestamp).collect();
        let mut batch = timestamped_batch_at(base_offset, &timestamps, b'z');
        batch.attributes = batch.attributes.with_compression(codec);
        for (record, sized) in batch.records.iter_mut().zip(records) {
            record.value = Some(Bytes::from(vec![b'z'; sized.value_len]));
        }
        batch
    }

    /// The size of a record with a `1000`-byte value and nothing else: its
    /// attributes byte, a one-byte timestamp and offset delta, a null key, the
    /// value behind its two-byte length, and no headers.
    const LARGE: usize = 1_007;

    /// Kafka's `RemoteLogManager.lookupTimestamp` decompresses a batch only when
    /// its max timestamp and its last offset reach the lookup's, and holds each
    /// record it reads to the limit. A batch the producer did not compress is
    /// never held to it.
    #[yare::parameterized(
        found_before_oversized_batch = {
            krabka_compression::CompressionType::Gzip,
            10,
            1_000,
            Some(100),
            Ok(Some((10, 1_000))),
        },
        oversized_record_is_match = {
            krabka_compression::CompressionType::Gzip,
            10,
            1_100,
            Some(100),
            Err(LARGE),
        },
        oversized_record_before_match = {
            krabka_compression::CompressionType::Gzip,
            10,
            1_200,
            Some(100),
            Err(LARGE),
        },
        batch_below_offset_floor = {
            krabka_compression::CompressionType::Gzip,
            13,
            1_100,
            Some(100),
            Ok(None),
        },
        no_limit = {
            krabka_compression::CompressionType::Gzip,
            10,
            1_100,
            None,
            Ok(Some((11, 1_100))),
        },
        uncompressed_batch = {
            krabka_compression::CompressionType::None,
            10,
            1_100,
            Some(100),
            Ok(Some((11, 1_100))),
        },
    )]
    fn first_record_at_or_after_timestamp_refuses_only_the_records_kafka_reads(
        codec: krabka_compression::CompressionType,
        floor_offset: i64,
        target: i64,
        limit: Option<usize>,
        want: Result<Option<(i64, i64)>, usize>,
    ) {
        let layout = |codec| {
            encoded(&[
                sized_batch_at(10, &[sized(1_000, 10)], codec),
                sized_batch_at(11, &[sized(1_100, 1_000), sized(1_200, 10)], codec),
            ])
        };
        let got =
            match first_record_at_or_after_timestamp(&layout(codec), floor_offset, target, limit) {
                Ok(found) => Ok(found),
                Err(RemoteStorageError::RecordTooLarge { size, limit }) => {
                    assert2::assert!(limit == 100);
                    Err(size)
                }
                Err(other) => panic!("unexpected error: {other}"),
            };
        assert2::assert!(got == want);
    }
}
