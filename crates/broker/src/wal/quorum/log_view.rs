//! `krabka-log` adapter for the sans-IO quorum core.

use {
    krabka_kraft_core::{Epoch, LogOffsetMetadata, LogView},
    krabka_log::Log,
    krabka_raft::kraft::log::latest_epoch_in,
    std::sync::{Arc, Mutex, MutexGuard},
};

/// A durable WAL-replica log exposed through [`LogView`].
#[derive(Debug, Clone)]
pub(crate) struct ShardLog {
    log: Arc<Mutex<Log>>,
}

impl ShardLog {
    #[must_use]
    pub(crate) fn new(log: Arc<Mutex<Log>>) -> Self { Self { log } }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Log> {
        self.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) fn shares_log(&self, other: &Arc<Mutex<Log>>) -> bool {
        Arc::ptr_eq(&self.log, other)
    }
}

impl LogView for ShardLog {
    fn end_offset(&self) -> i64 { self.lock().log_end_offset().0 }

    fn last_epoch(&self) -> Epoch { latest_epoch_in(self.lock().epoch_checkpoint()) }

    fn end_offset_for_epoch(&self, epoch: Epoch) -> LogOffsetMetadata {
        let log = self.lock();
        krabka_raft::kraft::log::end_offset_for_epoch_in(
            log.epoch_checkpoint(),
            log.log_end_offset(),
            epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_log::{Log, LogConfig},
        krabka_protocol::records::{Record, RecordBatch},
        std::sync::{Arc, Mutex},
    };

    #[test]
    fn shard_log_view_reports_offset_and_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut batch(3, 2)).unwrap();

        let view = ShardLog::new(Arc::new(Mutex::new(log)));

        assert!(view.end_offset() == 3);
        assert!(view.last_epoch() == 2);
        assert!(
            view.end_offset_for_epoch(2)
                == LogOffsetMetadata {
                    offset: 3,
                    epoch: 2
                }
        );
        assert!(
            view.end_offset_for_epoch(3)
                == LogOffsetMetadata {
                    offset: 3,
                    epoch: 2
                }
        );
    }

    fn batch(records: i32, epoch: i32) -> RecordBatch {
        let mut batch = RecordBatch {
            last_offset_delta: records - 1,
            partition_leader_epoch: epoch,
            ..RecordBatch::default()
        };
        for offset_delta in 0..records {
            batch.records.push(Record {
                offset_delta,
                ..Record::default()
            });
        }
        batch
    }
}
