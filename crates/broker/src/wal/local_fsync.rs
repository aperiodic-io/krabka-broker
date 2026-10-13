//! Test WAL medium: a single-node, `fsync`-durable WAL that reuses the
//! partition's existing local `Log`. Production diskless partitions use the
//! quorum WAL.

use {
    super::WalStore,
    crate::{blocking::run_blocking, error::BrokerError, partition_writer::storage_failure_error},
    async_trait::async_trait,
    krabka_ids::Offset,
    krabka_log::Log,
    std::sync::{Arc, Mutex},
};

/// A [`WalStore`] backed by the partition's local `Log` plus an explicit
/// `fsync` (`Log::sync`).
pub struct LocalFsyncWal {
    log: Arc<Mutex<Log>>,
}

impl LocalFsyncWal {
    #[must_use]
    pub fn new(log: Arc<Mutex<Log>>) -> Self { Self { log } }
}

#[async_trait]
impl WalStore for LocalFsyncWal {
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        let log = self.log.clone();
        // fsync off the async poller, through the seam
        // run_produce_append_batch uses.
        let res = run_blocking(move || {
            log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .sync()
        })
        .await
        .map_err(|e| storage_failure_error("wal fsync task panicked", &e))?;
        res.map_err(BrokerError::from)?;
        Ok(leo)
    }

    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError> {
        let log = self.log.clone();
        let result = run_blocking(move || {
            log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .trim_to_offset(new_start)
        })
        .await
        .map_err(|error| storage_failure_error("wal trim task panicked", error))?;
        result.map_err(BrokerError::from)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::partition::ProduceData,
        assert2::assert,
        krabka_log::{Log, LogConfig},
        std::sync::{Arc, Mutex},
    };

    fn wal(dir: &std::path::Path) -> LocalFsyncWal {
        let log = Arc::new(Mutex::new(Log::open(dir, LogConfig::default()).unwrap()));
        LocalFsyncWal::new(log)
    }

    #[tokio::test]
    async fn append_assigns_sequential_offsets_then_sync_advances_durable() {
        let dir = tempfile::tempdir().unwrap();
        let w = wal(dir.path());
        let (results, leo, _) = run_produce_append_batch(
            w.log.clone(),
            None,
            (
                vec![
                    sample_owned().count(2).call(),
                    sample_owned().count(3).call(),
                ],
                Vec::new(),
            ),
        )
        .await
        .unwrap();
        let actual_offsets = results
            .iter()
            .map(|result| {
                result
                    .as_ref()
                    .map(|appended| appended.base_offset)
                    .map_err(|_| ())
            })
            .collect::<Vec<_>>();
        assert!(actual_offsets == vec![Ok(Offset(0)), Ok(Offset(2))]);
        assert!(results.iter().all(Result::is_ok));
        assert!(leo == krabka_ids::Offset(5));
        // Durable watermark only advances after sync_durable.
        let durable = w.sync_durable(leo).await.unwrap();
        assert!(durable == leo);
    }

    #[tokio::test]
    async fn trim_advances_the_local_wal_start() {
        let dir = tempfile::tempdir().unwrap();
        let w = wal(dir.path());
        let (_results, leo, _) = run_produce_append_batch(
            w.log.clone(),
            None,
            (vec![sample_owned().count(3).call()], Vec::new()),
        )
        .await
        .unwrap();
        w.sync_durable(leo).await.unwrap();

        let start = w.trim_to_offset(Offset(2)).await.unwrap();

        assert!(start >= Offset(2));
        assert!(w.log.lock().unwrap().log_start_offset() == start);
    }

    #[bon::builder]
    fn sample_owned(
        #[builder(into, default)] count: crate::test_support::RecordCount,
    ) -> ProduceData {
        ProduceData::Owned(sample_batch().count(count.0).call())
    }

    use crate::{
        partition_writer::run_produce_append_batch,
        test_support::default_records_batch as sample_batch,
    };
}
