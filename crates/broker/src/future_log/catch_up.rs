//! One catch-up pass that copies the batches a future log is missing from the
//! partition's current log.
//!
//! The pass is synchronous and holds each log mutex only for a short time, so
//! the replicator task can repeat it between cancellation checks. It also
//! charges the copied bytes against the broker-wide log-directory move budget,
//! and it re-bases the future log when retention advances the source start.

use std::sync::{Arc, Mutex};

use krabka_log::{Log, Offset};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::{error::BrokerError, partition::Partition};

/// One catch-up iteration. It reads whatever the future log is missing,
/// up to `read_chunk`, and appends it. Returns `true` if the
/// future log was caught up at the end of the iteration, that is, if it read
/// nothing AND `future.LEO >= source.LEO`.
pub(super) struct CatchUpProgress {
    pub(super) caught_up: bool,
    pub(super) throttled: bool,
}

pub(super) fn catch_up(
    part: &Arc<Partition>,
    future_log: &Arc<Mutex<Log>>,
    read_chunk: ByteSize,
    throttle: &crate::throttle::TokenBucket,
) -> Result<CatchUpProgress, BrokerError> {
    // Snapshot both source bounds under one lock so a retention update cannot
    // produce an internally inconsistent start/end pair.
    // The move copies local files, so the floor that bounds it is the local
    // one (KIP-405): on a tiered partition the offsets below it are in the
    // remote tier, and `Log::read` refuses them. Reading from the global floor
    // instead would fail every pass and the move would never finish.
    let (current_start, current_leo) = {
        let current = part
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (current.local_log_start_offset(), current.log_end_offset())
    };
    // Recover the guard if a panic elsewhere poisoned the mutex rather
    // than killing this (discarded-JoinHandle) replicator task.
    let future_leo = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .log_end_offset();
    if future_leo >= current_leo && future_leo >= current_start {
        return Ok(CatchUpProgress {
            caught_up: true,
            throttled: false,
        });
    }

    let granted = throttle.try_consume(read_chunk.bytes_u64());
    if granted == 0 {
        return Ok(CatchUpProgress {
            caught_up: false,
            throttled: true,
        });
    }

    // Retention may advance the source start beyond the future log. Read from
    // the new logical start; the returned first batch can begin below it when
    // the start falls inside a batch, so reset to that physical base and restore
    // the logical start after appending.
    let reset_for_retention = future_leo < current_start;
    let fetch_offset = if reset_for_retention {
        current_start
    } else {
        future_leo
    };
    let read = {
        let log = part
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        log.read(fetch_offset, ByteSize::from_bytes(granted))?
    };
    if read.batches.is_empty() {
        if reset_for_retention {
            future_log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .reset_to(current_start)?;
        }
        return Ok(CatchUpProgress {
            caught_up: true,
            throttled: false,
        });
    }

    let mut future = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if reset_for_retention {
        future.reset_to(read.start_offset)?;
    }
    for mut batch in read.batches {
        let base = batch.base_offset;
        future
            .append_at(&mut batch, Offset(base))
            .map_err(BrokerError::from)?;
    }
    if reset_for_retention {
        future.set_log_start_offset(current_start)?;
    }
    Ok(CatchUpProgress {
        caught_up: false,
        throttled: false,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use krabka_units::mebibytes;
    use tempfile::tempdir;

    use super::*;
    use crate::future_log::test_support::fixture_partition;

    fn first_catch_up(
        part: &Arc<Partition>,
        target: &std::path::Path,
    ) -> (Arc<Mutex<Log>>, CatchUpProgress) {
        let (_future_path, future) = crate::future_log::test_support::open_future_log(target);
        let progress = catch_up(
            part,
            &future,
            mebibytes(1),
            &crate::throttle::TokenBucket::new(),
        )
        .expect("catch up");
        (future, progress)
    }

    #[tokio::test]
    async fn catch_up_resets_after_source_retention_without_dropping_batch_data() {
        let fixture = crate::future_log::test_support::source_fixture(
            crate::future_log::test_support::FutureSourceSetup::default(),
        );
        let _primary = fixture.primary;
        let target = fixture.target;
        let part = fixture.partition;
        part.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_log_start_offset(Offset(2))
            .expect("advance source start");
        let (future, progress) = first_catch_up(&part, target.path());
        assert!(!progress.caught_up);
        let future = future
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(future.log_start_offset() == Offset(2));
        assert!(future.log_end_offset() == Offset(3));
        let read = future.read(Offset(2), mebibytes(1)).expect("read future");
        assert!(read.batches.len() == 1);
        assert!(read.batches[0].base_offset == 0);
    }

    #[tokio::test]
    async fn catch_up_resets_to_empty_source_frontier() {
        let primary = tempdir().unwrap();
        let target = tempdir().unwrap();
        let part = fixture_partition(primary.path(), "t", PartitionIndex(0));
        part.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reset_to(Offset(5))
            .expect("reset source");
        let (future, progress) = first_catch_up(&part, target.path());
        assert!(progress.caught_up);
        let future = future
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(future.log_start_offset() == Offset(5));
        assert!(future.log_end_offset() == Offset(5));
    }

    #[tokio::test]
    async fn catch_up_waits_when_move_throttle_is_exhausted() {
        let fixture = crate::future_log::test_support::source_fixture(
            crate::future_log::test_support::FutureSourceSetup {
                count: crate::test_support::RecordCount(1),
                ..Default::default()
            },
        );
        let _primary = fixture.primary;
        let target = fixture.target;
        let part = fixture.partition;
        let (_future_path, future) =
            crate::future_log::test_support::open_future_log(target.path());
        let throttle = crate::throttle::TokenBucket::new();
        throttle
            .set_byte_rate_with_burst(krabka_units::bytes_per_sec(1024), krabka_units::bytes(0));

        let progress = catch_up(&part, &future, mebibytes(1), &throttle).expect("catch up");

        assert!(progress.throttled);
        assert!(!progress.caught_up);
        assert!(
            future
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .log_end_offset()
                == Offset(0)
        );
    }
}
