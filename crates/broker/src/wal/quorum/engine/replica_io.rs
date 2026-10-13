//! The blocking log operations that a WAL replica performs, and the bridge
//! that keeps them off an async worker thread.
//!
//! Appending a verbatim batch, fsyncing, and trimming all block. Each entry
//! point here runs through [`crate::blocking::run_blocking`], which picks
//! `block_in_place`, `spawn_blocking` or the calling thread by runtime and
//! target, so callers on the produce and flusher paths can stay `async`.

use {
    super::{BatchBytes, read_log_batches_exact},
    crate::{
        blocking::run_blocking, error::BrokerError, partition_writer::storage_failure_error,
        wal::quorum::log_view::ShardLog,
    },
    krabka_ids::Offset,
};

pub(in crate::wal::quorum) async fn sync_replica(
    log: ShardLog,
    batches: Vec<BatchBytes>,
) -> Result<(), BrokerError> {
    run_blocking(move || sync_replica_blocking(&log, &batches))
        .await
        .map_err(|e| BrokerError::Replication(format!("wal replica task panicked: {e}")))?
}

pub(super) async fn trim_log(log: ShardLog, new_start: Offset) -> Result<Offset, BrokerError> {
    run_blocking(move || log.lock().trim_to_offset(new_start).map_err(Into::into))
        .await
        .map_err(|error| storage_failure_error("wal trim task panicked", error))?
}

pub(super) fn sync_replica_blocking(
    log: &ShardLog,
    batches: &[BatchBytes],
) -> Result<(), BrokerError> {
    let mut log = log.lock();
    for batch in batches {
        let end = log.log_end_offset();
        if end <= batch.base_offset {
            log.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        } else if end < batch.last_offset + 1 {
            return Err(BrokerError::Replication(format!(
                "wal replica overlaps batch {}..{} at leo {end}",
                batch.base_offset.0,
                batch.last_offset.0 + 1
            )));
        } else {
            let existing = read_log_batches_exact(&log, batch.base_offset, batch.last_offset + 1)?;
            if existing.len() != 1 || existing[0].verbatim.bytes != batch.verbatim.bytes {
                return Err(BrokerError::Replication(format!(
                    "wal replica diverges in batch {}..{}",
                    batch.base_offset.0,
                    batch.last_offset.0 + 1
                )));
            }
        }
    }
    log.sync()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_log::{Log, LogConfig},
        std::sync::{Arc, Mutex},
    };

    #[test]
    fn sync_replica_verifies_an_existing_batch() {
        let root = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(root.path().join("source"), LogConfig::default()).unwrap(),
        ));
        source
            .lock()
            .unwrap()
            .append(&mut batch(b"leader"))
            .unwrap();
        let source = ShardLog::new(source);
        let batches = read_log_batches_exact(&source.lock(), Offset(0), Offset(1)).unwrap();

        let matching = Arc::new(Mutex::new(
            Log::open(root.path().join("matching"), LogConfig::default()).unwrap(),
        ));
        matching
            .lock()
            .unwrap()
            .append(&mut batch(b"leader"))
            .unwrap();
        sync_replica_blocking(&ShardLog::new(matching), &batches).unwrap();

        let divergent = Arc::new(Mutex::new(
            Log::open(root.path().join("divergent"), LogConfig::default()).unwrap(),
        ));
        divergent
            .lock()
            .unwrap()
            .append(&mut batch(b"follower"))
            .unwrap();
        let error = sync_replica_blocking(&ShardLog::new(divergent), &batches).unwrap_err();

        assert2::assert!(error.to_string().contains("diverges"));
    }

    use crate::test_support::single_value_batch as batch;
}
