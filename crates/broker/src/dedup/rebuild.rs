//! Rebuilding a partition index from the last window of its log.

use {
    super::identity::{RecordIdentity, visit_owned_batch},
    crate::partition_writer::lock_log as lock,
    krabka_dedup::DedupWindowFilter,
    krabka_log::{Log, LogError, Offset},
    krabka_units::mebibytes,
    std::sync::Mutex,
};

/// How a rebuild ended.
#[derive(Debug)]
pub(super) struct RebuildOutcome {
    /// The log end offset that the rebuilt index covers.
    pub(super) covered_through: Offset,
    /// The read that stopped the rebuild early, if one did. The index then
    /// holds the identities read before it, which is a narrower index and
    /// never a wrong one.
    pub(super) error: Option<LogError>,
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
/// The log lock is taken once for each read of up to 1 MiB, not for the whole
/// rebuild, so fetches from this partition continue while it runs. The caller
/// is the partition writer, the only task that changes the log, so the log end
/// offset does not move between the reads.
pub(super) fn rebuild_from_log(
    filter: &mut DedupWindowFilter,
    key_header: Option<&str>,
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
    let mut next = start;
    while next < end {
        let before = next;
        let read = match lock(log).read(next, mebibytes(1)) {
            Ok(read) => read,
            Err(error) => {
                return RebuildOutcome {
                    covered_through: end,
                    error: Some(error),
                };
            }
        };
        if read.batches.is_empty() {
            break;
        }
        for batch in &read.batches {
            let last = batch
                .base_offset
                .saturating_add(i64::from(batch.last_offset_delta));
            // A read starts at the batch that holds `next`, which the
            // previous read already indexed when `next` is inside it.
            if last < next.0 {
                continue;
            }
            visit_owned_batch(batch, key_header, |identity| {
                if let RecordIdentity::Indexed(record) = identity
                    && let Some(digest) = record.identity
                {
                    filter.populate(digest, record.timestamp_ms, now_ms);
                }
            });
            next = Offset(last.saturating_add(1));
        }
        if next == before {
            // A read that does not move forward would repeat forever.
            break;
        }
    }
    RebuildOutcome {
        covered_through: end,
        error: None,
    }
}
