//! Kafka trunk's `max.decompressed.message.bytes` at the places the log
//! decompresses a batch.
//!
//! Trunk hands `LogConfig.maxDecompressedMessageBytes()` to the iterator that
//! decompresses a compressed batch, and `DefaultRecord.readFrom` throws
//! `InvalidRecordException` for a record whose declared body is larger,
//! before it allocates the body. Besides `Produce` validation, which the broker
//! runs, three log operations iterate compressed batches that way: the
//! cleaner's offset-map build and rewrite (`Cleaner.buildOffsetMapForSegment`
//! and `MemoryRecords.filterTo`), and the by-timestamp lookups
//! (`FileRecords.searchForTimestamp`, `RecordBatch.offsetOfMaxTimestamp`).
//! Follower appends do not: `UnifiedLog.appendAsFollower` skips
//! `LogValidator`, so a record that got past the leader's limit is replicated
//! as it is.
//!
//! An uncompressed batch is never held to the limit: Kafka reads it in place
//! and bounds it by `max.message.bytes` alone.
//!
//! krabka's reads have decoded the whole batch by the time these checks run, so
//! the check is not an allocation guard here. It is the verdict the operator
//! configured, reached on the same records Kafka reaches it on.

use {
    crate::error::LogError,
    krabka_compression::CompressionType,
    krabka_protocol::records::RecordBatch,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
};

/// Refuse `batch` if Kafka's iterator would refuse it after reading its first
/// `read` records, under `limit`.
///
/// `read` is how many records the caller's scan decodes before it stops:
/// Kafka's iterators decode lazily, so a lookup that returns at the third
/// record never sees a fourth that is oversized. A scan that reads the whole
/// batch passes `batch.records.len()`. A batch the producer did not compress,
/// or a `limit` of `None`, passes.
///
/// # Errors
///
/// Returns [`LogError::RecordTooLarge`] for the first record among those read
/// whose body is larger than `limit`.
pub(crate) fn check_records_read(
    batch: &RecordBatch,
    read: usize,
    limit: Option<ByteSize>,
) -> Result<(), LogError> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if batch.attributes.compression() == CompressionType::None {
        return Ok(());
    }
    let limit = limit.bytes_usize();
    for record in batch.records.iter().take(read) {
        let size = record.body_len();
        if size > limit {
            return Err(LogError::RecordTooLarge { size, limit });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_protocol::records::{Attributes, Record},
        krabka_units::prelude::bytes as byte_size,
    };

    krabka_macros::record_limit_fixture!(record);

    fn batch(codec: CompressionType, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
            records,
            ..Default::default()
        }
    }

    /// A record whose body is `body` bytes passes a limit of `body` and is
    /// refused, with that size, under a limit one byte lower. Kafka's
    /// `sizeOfBodyInBytes` of the record is its attributes byte, a two-byte
    /// timestamp delta, a one-byte offset delta, `1 + 3` bytes of key, the value
    /// behind its own length prefix, a one-byte header count and one header of
    /// `1 + 1` bytes of key with a null value. The sizes straddle the widths of
    /// the record's own length prefix: a body of 63 bytes takes a one-byte
    /// prefix and one of 64 takes two, and 8191 takes two where 8192 takes
    /// three.
    #[yare::parameterized(
        empty_value = { 0, 13_u32 },
        prefix_one_byte_max = { 50, 63 },
        prefix_two_byte_min = { 51, 64 },
        ordinary = { 100, 114 },
        prefix_two_byte_max = { 8_177, 8_191 },
        prefix_three_byte_min = { 8_178, 8_192 },
        large_value = { 70_000, 70_015 },
    )]
    fn a_record_is_held_to_the_limit_by_its_body_size(value_len: usize, body: u32) {
        let held = |limit| {
            check_records_read(
                &batch(CompressionType::Gzip, vec![record(value_len)]),
                1,
                Some(byte_size(limit)),
            )
        };
        assert2::check!(held(body).is_ok(), "{value_len}");
        assert2::assert!(let Err(LogError::RecordTooLarge { size, limit }) = held(body - 1));
        assert2::check!(
            (size, limit)
                == (
                    usize::try_from(body).unwrap(),
                    usize::try_from(body - 1).unwrap()
                ),
            "{value_len}"
        );
    }

    /// The body size of the record `check_records_read` refuses under `limit`,
    /// or `None` when it passes the batch.
    fn refused_size(
        codec: CompressionType,
        records: Vec<Record>,
        read: usize,
        limit: Option<ByteSize>,
    ) -> Option<usize> {
        match check_records_read(&batch(codec, records), read, limit) {
            Ok(()) => None,
            Err(LogError::RecordTooLarge { size, limit }) => {
                assert2::check!(limit == 100);
                Some(size)
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    /// Only a compressed batch is held to the limit, and the first oversized
    /// record among the ones read decides.
    #[yare::parameterized(
        compressed_oversized = {
            "compressed, oversized",
            CompressionType::Gzip,
            vec![record(10), record(1_000)],
            2,
            Some(1_014),
        },
        compressed_oversized_not_read = {
            "compressed, oversized record not read",
            CompressionType::Gzip,
            vec![record(10), record(1_000)],
            1,
            None,
        },
        compressed_within_limit = {
            "compressed, within the limit",
            CompressionType::Snappy,
            vec![record(10), record(10)],
            2,
            None,
        },
        uncompressed_oversized = {
            "uncompressed, oversized",
            CompressionType::None,
            vec![record(10), record(1_000)],
            2,
            None,
        },
    )]
    fn only_a_compressed_batch_is_held_to_the_limit(
        name: &str,
        codec: CompressionType,
        records: Vec<Record>,
        read: usize,
        expected: Option<usize>,
    ) {
        assert2::check!(
            refused_size(codec, records, read, Some(byte_size(100))) == expected,
            "{name}"
        );
    }

    #[test]
    fn no_limit_holds_a_compressed_batch_to_nothing() {
        assert2::check!(refused_size(CompressionType::Gzip, vec![record(1_000)], 1, None) == None);
    }
}
