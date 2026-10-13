//! Rebuilding a partition index from the last window of its log.

use {
    super::identity::{RecordIdentity, visit_wire_batch},
    crate::partition_writer::lock_log as lock,
    krabka_compression::RecordDecompressionPolicy,
    krabka_dedup::DedupWindowFilter,
    krabka_log::{Log, LogError, Offset},
    krabka_protocol::records::validate_one_v2_batch,
    krabka_units::mebibytes,
    std::{fmt, sync::Mutex},
};

/// How a rebuild ended.
#[derive(Debug)]
pub(super) struct RebuildOutcome {
    /// The log end offset that the rebuilt index covers.
    pub(super) covered_through: Offset,
    /// Why the rebuild stopped before the log end, if it did. The index then
    /// holds the identities read before it, which is a narrower index and
    /// never a wrong one.
    pub(super) stopped: Option<RebuildStop>,
}

/// Why a rebuild stopped before the log end.
#[derive(Debug)]
pub(super) enum RebuildStop {
    /// A read of the log failed.
    Read(LogError),
    /// A read returned nothing at `next`, below the log end.
    NoProgress(Offset),
}

impl fmt::Display for RebuildStop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(f, "{error}"),
            Self::NoProgress(next) => write!(f, "a read at offset {} returned no batch", next.0),
        }
    }
}

/// Fill a cleared `filter` from the records of the log that fall inside its
/// window.
///
/// The rebuild starts at the first offset whose timestamp is at or after
/// `now_ms - window`, which is the offset `ListOffsets` gives for that
/// timestamp. A record before it can never cause a drop again, so reading it
/// would only cost time. Producer timestamps are not ordered, so this start is
/// approximate in the same way the edge of the window is.
///
/// Each read takes the log lock for the copy of up to 1 MiB of the log's wire
/// bytes and nothing else, the way a fetch does: the batches are decoded and
/// decompressed after the lock is released. A highly compressible megabyte
/// can hold millions of records, and decoding them under the lock would hold
/// up every fetch of the partition for as long. A read always returns the
/// first batch whole, so a batch larger than the read size is indexed too.
/// The caller is the partition writer, the only task that changes the log, so
/// the log end offset does not move between the reads.
pub(super) fn rebuild_from_log(
    filter: &mut DedupWindowFilter,
    (key_header, policy): (Option<&str>, RecordDecompressionPolicy),
    log: &Mutex<Log>,
    now_ms: i64,
) -> RebuildOutcome {
    let (start, end) = {
        let log = lock(log);
        let end = log.log_end_offset();
        let target = now_ms.saturating_sub(filter.window_ms());
        let start = log
            .offset_for_timestamp(target)
            .map_or(end, |(offset, _)| offset.max(log.local_log_start_offset()));
        (start, end)
    };
    let stopped = |stop| RebuildOutcome {
        covered_through: end,
        stopped: Some(stop),
    };
    let mut next = start;
    while next < end {
        let read = match lock(log).read_raw(next, end, mebibytes(1)) {
            Ok(read) => read,
            Err(error) => return stopped(RebuildStop::Read(error)),
        };
        let Some(last) = read.last_offset.filter(|last| *last >= next) else {
            return stopped(RebuildStop::NoProgress(next));
        };
        let mut rest = &read.bytes[..];
        while let Ok(batch) = validate_one_v2_batch(rest) {
            let (wire, tail) = rest.split_at(batch.total_len.min(rest.len()));
            visit_wire_batch(wire, key_header, policy, |identity| {
                if let RecordIdentity::Indexed(record) = identity
                    && let Some(digest) = record.identity
                {
                    filter.populate(digest, record.timestamp_ms, now_ms);
                }
            });
            rest = tail;
        }
        next = Offset(last.0.saturating_add(1));
    }
    RebuildOutcome {
        covered_through: end,
        stopped: None,
    }
}
