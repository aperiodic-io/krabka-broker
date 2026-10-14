//! Errors returned by `Log` and `Segment`.

use {krabka_ids::Offset, thiserror::Error};

/// Errors returned by [`Log`](crate::Log) and [`Segment`](crate::Segment).
#[derive(Debug, Error)]
pub enum LogError {
    /// An I/O operation against the filesystem failed.
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    /// Recovery found a partial batch at the tail of a `.log` file. The log
    /// truncates the trailing bytes back to the last cleanly decoded batch.
    #[error("partial batch at offset {file_offset} in segment {segment}: truncating")]
    PartialBatch {
        /// Absolute base offset of the segment containing the partial batch.
        segment: Offset,
        /// Byte position within the `.log` file where the partial batch starts.
        file_offset: u64,
    },

    /// A batch's stored CRC did not match the one computed over its bytes.
    #[error(
        "CRC mismatch at offset {file_offset} in segment {segment}: \
         expected {expected:#x}, computed {computed:#x}"
    )]
    CrcMismatch {
        /// Absolute base offset of the segment.
        segment: Offset,
        /// Byte position within the `.log` file where the corrupt batch starts.
        file_offset: u64,
        /// CRC value embedded in the batch header.
        expected: u32,
        /// CRC value re-computed by the reader.
        computed: u32,
    },

    /// A caller requested an offset below [`Log::log_start_offset`](crate::Log::log_start_offset).
    #[error("offset {requested} below log start {log_start}")]
    OffsetTooLow {
        /// Offset the caller asked for.
        requested: Offset,
        /// Current log start.
        log_start: Offset,
    },

    /// A caller requested an offset that is at or above
    /// [`Log::log_start_offset`](crate::Log::log_start_offset) but below
    /// [`Log::local_log_start_offset`](crate::Log::local_log_start_offset).
    ///
    /// KIP-405: the records are in the remote tier and no local segment holds
    /// them. The broker's fetch path answers `OFFSET_OUT_OF_RANGE` and then
    /// falls through to the remote reader, which is where these offsets are
    /// served from.
    #[error("offset {requested} is tiered: below local log start {local_log_start}")]
    OffsetBelowLocalStart {
        /// Offset the caller asked for.
        requested: Offset,
        /// First offset the local segments still hold.
        local_log_start: Offset,
    },

    /// The encode or decode of a `RecordBatch` failed.
    #[error("records: {0}")]
    Records(#[from] krabka_protocol::records::RecordsError),

    /// A segment filename would not parse. For example, it has the wrong
    /// length, or it is not all digits.
    #[error("invalid segment filename: {0}")]
    BadSegmentName(String),

    /// A caller supplied an explicit offset to [`Log::append_at`](crate::Log::append_at)
    /// that was below the log's current end offset. Replication paths use
    /// this to detect a duplicate or a divergence between leader-assigned offsets and the local
    /// log's expected next offset.
    #[error("offset mismatch: expected {expected}, got {actual}")]
    OffsetMismatch {
        /// The lowest offset the log takes, that is, its current end offset.
        expected: Offset,
        /// The offset the caller actually supplied.
        actual: Offset,
    },

    /// A log file such as `.txnindex` is corrupt. It has the wrong size, a
    /// bad checksum, or a similar defect.
    #[error("corrupt log: {0}")]
    Corrupt(String),

    /// A persisted sidecar or checkpoint names a format version this build
    /// does not read. A future 1.x broker wrote it, or it is not the artifact
    /// its name says.
    #[error("unrecognized version {found} of the {artifact} file {}", path.display())]
    UnsupportedFormatVersion {
        /// The artifact kind, such as `stampindex` or `leader-epoch-checkpoint`.
        artifact: &'static str,
        /// The file that carries the version.
        path: std::path::PathBuf,
        /// The version the file carries.
        found: i64,
    },

    /// A persisted sidecar or checkpoint has no version marker. Only a broker
    /// before 1.0 wrote that layout, and 1.x reads no 0.x data.
    #[error(
        "the {artifact} file {} has no version marker: it predates krabka 1.0, so the node \
         must be reformatted",
        path.display()
    )]
    MissingFormatVersion {
        /// The artifact kind, such as `stampindex` or `leader-epoch-checkpoint`.
        artifact: &'static str,
        /// The file that lacks the marker.
        path: std::path::PathBuf,
    },

    /// A control record's key or end-transaction-marker value is too short to
    /// hold its fields. Kafka's `ControlRecordType.parseTypeId` throws
    /// `InvalidRecordException` for a short key, and
    /// `EndTransactionMarker.deserializeValue` cannot read a short value.
    #[error(
        "Invalid value size found for {record}. Must have at least {needed} bytes, but found \
         only {found}"
    )]
    InvalidControlRecordSize {
        /// `end control record key` or `end transaction marker value`.
        record: &'static str,
        /// The bytes the record must hold.
        needed: usize,
        /// The bytes it holds; a missing key or value counts as zero.
        found: usize,
    },

    /// A control record names a negative key version or end-transaction-marker
    /// value version. Kafka's `ControlRecordType.parseTypeId` and
    /// `EndTransactionMarker.deserializeValue` throw `InvalidRecordException`
    /// for one: the bytes are corrupt. A version above the current one is not
    /// this error; Kafka parses it as the current version.
    #[error("Invalid version found for {record}: {version}. May indicate data corruption")]
    InvalidControlRecordVersion {
        /// `control record` for the key, `end transaction marker` for the value.
        record: &'static str,
        /// The negative version the record carries.
        version: i16,
    },

    /// A caller supplied an invalid argument.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Kafka trunk's `max.decompressed.message.bytes`: a compressed batch that a
    /// lookup or a compaction pass had to decompress holds a record whose body
    /// is larger than [`LogConfig::max_decompressed_record`](crate::LogConfig::max_decompressed_record).
    ///
    /// Kafka throws `InvalidRecordException` from `DefaultRecord.readFrom`, and
    /// the message is its own. The broker answers `INVALID_RECORD` (87) to a
    /// `ListOffsets`, and the cleaner leaves the partition uncleanable until a
    /// pass succeeds.
    #[error("Invalid record size: {size} exceeds the configured maximum record size of {limit}.")]
    RecordTooLarge {
        /// The record's body size in bytes, Kafka's `sizeOfBodyInBytes`.
        size: usize,
        /// The configured maximum.
        limit: usize,
    },

    /// KFC-1 `delivery.schedule.monotonic`: the appended batch's delivery time
    /// precedes a delivery time the partition already holds, so the batch
    /// would make the partition's schedule run backwards.
    ///
    /// The log raises it because the log is what serializes appends: the test
    /// and the write it guards are one critical section here and nowhere
    /// above. The broker answers it with `INVALID_TIMESTAMP` (32).
    #[error("delivery time {delivery_ms} precedes a delivery time already in the partition")]
    ScheduleRunsBackwards {
        /// The refused batch's delivery time, which is its `max_timestamp`.
        delivery_ms: i64,
    },

    /// The log was fenced with [`Log::fence`](crate::Log::fence) because its
    /// partition is being removed, and takes no more appends.
    #[error("the log is fenced: its partition is being removed")]
    Fenced,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_partial_batch() {
        let e = LogError::PartialBatch {
            segment: Offset(0),
            file_offset: 1024,
        };
        assert2::assert!(e.to_string() == "partial batch at offset 1024 in segment 0: truncating");
    }
}
