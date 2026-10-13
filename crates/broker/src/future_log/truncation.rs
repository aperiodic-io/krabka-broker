//! The cuts that keep a KIP-113 future log a prefix of the current log's
//! history.
//!
//! Kafka's swap condition is only `futureLog.logEndOffset ==
//! log.logEndOffset` (`Partition.runCallbackIfFutureReplicaCaughtUp`), so the
//! `ReplicaAlterLogDirsThread` has to keep the future replica consistent with
//! the current one by two other means: `markPartitionsForTruncation`, which a
//! follower calls when it truncates the current log, and a leader-epoch
//! exchange with the current log whenever the fetcher starts
//! (`AbstractFetcherThread.truncateToEpochEndOffsets`). The second one covers a
//! future replica that was offline when the current one truncated and grew back,
//! for example across a broker restart: the mark is only in memory.

use {
    crate::error::BrokerError,
    krabka_ids::LeaderEpoch,
    krabka_log::{LeaderEpochCheckpoint, Log, Offset},
    std::sync::Mutex,
};

/// Cut the future log to `offset`, or to nothing when `offset` lies below its
/// log start, as the current log did.
pub(super) fn cut_future_log(future_log: &Mutex<Log>, offset: Offset) -> Result<(), BrokerError> {
    let mut future = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if offset < future.log_start_offset() {
        future.reset_to(offset)?;
    } else {
        future.truncate_to(offset)?;
    }
    Ok(())
}

/// Kafka's `OffsetTruncationState`: where to truncate, and whether that ends
/// the exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TruncationState {
    offset: Offset,
    completed: bool,
}

/// A current log's answer about an epoch: the epoch it names and where that
/// epoch ends.
#[derive(Clone, Copy)]
struct EpochEnd {
    epoch: LeaderEpoch,
    end: Offset,
}

/// Kafka's `AbstractFetcherThread.getOffsetTruncationState` for the
/// `ReplicaAlterLogDirsThread`, where the current log answers as the leader.
///
/// `current` is the current log's `endOffsetFor` answer for the future log's
/// latest epoch: the largest current epoch at or below it and where that epoch
/// ends, or `(-1, -1)` when the current log cannot place it. `future` is the
/// future log's own epoch history and `future_end` its log end.
///
/// - The current log cannot place the epoch: Kafka truncates to the future
///   replica's high watermark. This broker keeps none for a future log, so it
///   drops the future log's latest epoch and asks again for the one before.
/// - The future log does not know the epoch the current log named (it knows a
///   smaller one): truncate to the end of that smaller epoch and ask again.
/// - Otherwise the lowest of the two ends for the epoch and the future log
///   end, and the exchange is over.
fn offset_truncation_state(
    current: EpochEnd,
    future: &LeaderEpochCheckpoint,
    future_end: Offset,
    latest_epoch_start: Offset,
) -> TruncationState {
    let EpochEnd {
        epoch: current_epoch,
        end: current_end,
    } = current;
    if current_end < Offset(0) {
        return TruncationState {
            offset: latest_epoch_start,
            completed: false,
        };
    }
    let (future_epoch, future_epoch_end) = future.epoch_and_offset_for(current_epoch, future_end);
    if future_epoch_end < Offset(0) {
        TruncationState {
            offset: current_end.min(future_end),
            completed: true,
        }
    } else if future_epoch != current_epoch {
        TruncationState {
            offset: future_epoch_end.min(future_end),
            completed: false,
        }
    } else {
        TruncationState {
            offset: future_epoch_end.min(current_end).min(future_end),
            completed: true,
        }
    }
}

/// Truncates the future log by leader epoch until it and the current log agree
/// on the history they share, as the `ReplicaAlterLogDirsThread` does for a
/// partition it starts to fetch: the future log's latest epoch is put to the
/// current log, which answers where that epoch ends there, and the future log
/// keeps only what both hold.
///
/// A future log with records but no epoch entry cannot be compared, so it is
/// emptied (Kafka truncates it to its high watermark).
///
/// # Errors
/// Returns an error when a cut fails.
pub(super) fn truncate_to_current_epochs(
    current_log: &Mutex<Log>,
    future_log: &Mutex<Log>,
) -> Result<(), BrokerError> {
    // The lock order the swap uses: the current log, then the future log.
    let current = current_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut future = future_log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Every pass drops the future log's latest epoch or ends the exchange.
    for _ in 0..=future.epoch_checkpoint().entries().len() {
        let future_end = future.log_end_offset();
        let Some(latest) = future.epoch_checkpoint().entries().last().copied() else {
            if future_end > future.log_start_offset() {
                let start = future.log_start_offset();
                future.reset_to(start)?;
            }
            return Ok(());
        };
        let (reply_epoch, reply_end) = current
            .epoch_checkpoint()
            .epoch_and_offset_for(latest.epoch, current.log_end_offset());
        let state = offset_truncation_state(
            EpochEnd {
                epoch: reply_epoch,
                end: reply_end,
            },
            future.epoch_checkpoint(),
            future_end,
            latest.start_offset,
        );
        if state.offset < future_end {
            if state.offset < future.log_start_offset() {
                future.reset_to(state.offset)?;
            } else {
                future.truncate_to(state.offset)?;
            }
        }
        if state.completed {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::log_dir,
        assert2::assert,
        bytes::Bytes,
        krabka_log::LogConfig,
        krabka_protocol::records::{Record, RecordBatch},
        tempfile::tempdir,
    };

    const NO_ANSWER: EpochEnd = EpochEnd {
        epoch: LeaderEpoch(-1),
        end: Offset(-1),
    };

    /// An epoch and the offset it starts at.
    #[derive(Clone, Copy)]
    struct EpochStart {
        epoch: i32,
        start: i64,
    }

    /// A checkpoint, and the directory that holds its file.
    struct CheckpointInDir {
        dir: tempfile::TempDir,
        checkpoint: LeaderEpochCheckpoint,
    }

    /// A checkpoint that records `entries`.
    fn checkpoint(entries: &[EpochStart]) -> CheckpointInDir {
        let dir = tempdir().unwrap();
        let mut checkpoint =
            LeaderEpochCheckpoint::open(dir.path().join("leader-epoch-checkpoint")).unwrap();
        for &EpochStart { epoch, start } in entries {
            checkpoint
                .append(LeaderEpoch(epoch), Offset(start))
                .unwrap();
        }
        CheckpointInDir { dir, checkpoint }
    }

    /// One row per case of `getOffsetTruncationState`: the current log's
    /// answer, the future log's epochs (epoch 1 from 0, epoch 3 from 10) and
    /// end (15), and the truncation it makes.
    #[test]
    fn the_truncation_state_follows_kafkas_getoffsettruncationstate() {
        struct Case {
            label: &'static str,
            current: EpochEnd,
            offset: Offset,
            completed: bool,
        }

        let CheckpointInDir {
            dir: _dir,
            checkpoint: future,
        } = checkpoint(&[EpochStart { epoch: 1, start: 0 }, EpochStart {
            epoch: 3,
            start: 10,
        }]);
        let epoch_end = |epoch, end| EpochEnd { epoch, end };
        let cases = [
            Case {
                label: "the current log cannot place the epoch",
                current: NO_ANSWER,
                offset: Offset(10),
                completed: false,
            },
            Case {
                label: "the epochs agree: the lower end wins",
                current: epoch_end(LeaderEpoch(3), Offset(12)),
                offset: Offset(12),
                completed: true,
            },
            Case {
                label: "the epochs agree and the future log ends first",
                current: epoch_end(LeaderEpoch(3), Offset(40)),
                offset: Offset(15),
                completed: true,
            },
            Case {
                label: "the future log does not know epoch 2: cut to the end of epoch 1",
                current: epoch_end(LeaderEpoch(2), Offset(12)),
                offset: Offset(10),
                completed: false,
            },
            Case {
                label: "the current log ends epoch 1 below the future log's end for it",
                current: epoch_end(LeaderEpoch(1), Offset(4)),
                offset: Offset(4),
                completed: true,
            },
            Case {
                label: "the current log names an epoch below every epoch of the future log",
                current: epoch_end(LeaderEpoch(0), Offset(6)),
                offset: Offset(0),
                completed: true,
            },
        ];
        for Case {
            label,
            current,
            offset,
            completed,
        } in cases
        {
            let state = offset_truncation_state(current, &future, Offset(15), Offset(10));
            assert!(state == TruncationState { offset, completed }, "{label}");
        }
    }

    /// A log at `<dir>/<name>` with `batches` one-record batches, each stamped
    /// with the epoch it is paired with.
    fn log_with_epochs(dir: &std::path::Path, name: &str, batches: &[LeaderEpoch]) -> Mutex<Log> {
        let path = log_dir::partition_dir(dir, name, 0);
        std::fs::create_dir_all(&path).unwrap();
        let mut log = Log::open(&path, LogConfig::default()).unwrap();
        for &epoch in batches {
            let mut batch = RecordBatch {
                partition_leader_epoch: epoch.0,
                records: vec![Record {
                    value: Some(Bytes::from_static(b"v")),
                    ..Record::default()
                }],
                ..RecordBatch::default()
            };
            log.append(&mut batch).unwrap();
        }
        Mutex::new(log)
    }

    fn leo(log: &Mutex<Log>) -> Offset { log.lock().unwrap().log_end_offset() }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct TruncationLogsSetup<'a> {
        #[default(&[])]
        future: &'a [LeaderEpoch],
        #[default(&[LeaderEpoch(1), LeaderEpoch(1)])]
        current: &'a [LeaderEpoch],
    }

    struct TruncationLogsFixture {
        current: Mutex<Log>,
        future: Mutex<Log>,
        _directory: tempfile::TempDir,
    }

    impl TruncationLogsFixture {
        fn truncate(&self) { truncate_to_current_epochs(&self.current, &self.future).unwrap(); }
    }

    #[bon::builder]
    fn truncation_logs(
        #[builder(default = & [])] future: &'_ [LeaderEpoch],
        #[builder(default = & [LeaderEpoch (1) , LeaderEpoch (1)])] current: &'_ [LeaderEpoch],
    ) -> TruncationLogsFixture {
        let setup = TruncationLogsSetup { future, current };
        let directory = tempdir().unwrap();
        let future = log_with_epochs(directory.path(), "future", setup.future);
        let current = log_with_epochs(directory.path(), "current", setup.current);
        TruncationLogsFixture {
            current,
            future,
            _directory: directory,
        }
    }

    /// The case the truncation mark cannot cover: the current log lost its tail
    /// while no move task ran (across a restart) and grew back in a newer
    /// epoch, to a log end equal to the future log's. A swap that checks only
    /// the log ends would install the future log's discarded tail.
    #[test]
    fn a_future_log_that_kept_a_discarded_tail_is_cut_at_the_end_of_the_shared_epoch() {
        // Both logs hold offsets 0-1 in epoch 1. The future log went on with
        // offsets 2-3 in epoch 1, which the current log dropped and replaced
        // with offsets 2-3 in epoch 2.
        let logs = truncation_logs()
            .future(&[
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(1),
            ])
            .current(&[
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(2),
                LeaderEpoch(2),
            ])
            .call();
        assert!(leo(&logs.future) == leo(&logs.current));

        logs.truncate();

        assert!(leo(&logs.future) == Offset(2));
        assert!(leo(&logs.current) == Offset(4));
    }

    #[test]
    fn a_future_log_that_is_a_prefix_of_the_current_log_is_left_alone() {
        let logs = truncation_logs()
            .future(&[LeaderEpoch(1), LeaderEpoch(1)])
            .current(&[
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(2),
            ])
            .call();

        logs.truncate();

        assert!(leo(&logs.future) == Offset(2));
    }

    /// The future log holds an epoch the current log never had, above the
    /// current log's newest one: it is dropped epoch by epoch, down to what the
    /// two share.
    #[test]
    fn epochs_the_current_log_does_not_know_are_dropped_one_at_a_time() {
        let logs = truncation_logs()
            .future(&[
                LeaderEpoch(1),
                LeaderEpoch(1),
                LeaderEpoch(2),
                LeaderEpoch(3),
                LeaderEpoch(3),
            ])
            .call();

        logs.truncate();

        assert!(leo(&logs.future) == Offset(2));
    }

    #[test]
    fn future_logs_with_no_epoch_history_are_emptied_or_stay_empty() {
        for (label, epochs) in [
            ("without epochs", &[LeaderEpoch(-1), LeaderEpoch(-1)][..]),
            ("already empty", &[][..]),
        ] {
            let logs = truncation_logs().future(epochs).call();
            logs.truncate();
            assert!(leo(&logs.future) == Offset(0), "{label}");
        }
    }
}
