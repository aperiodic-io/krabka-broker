//! The writer's Retain arm: one local-retention pass over the partition log.
//!
//! `Log::tick` does the time-based segment roll and then the `retention.ms` /
//! `retention.bytes` eviction of sealed segments. Both mutate the log, so the
//! pass runs on the writer actor rather than under a lock taken by the
//! background sweep, exactly as [`super::compaction::handle_compact`] does.
//!
//! Routing the error through `flag_storage_failure` is the point of putting
//! this arm here rather than calling `Log::tick` from the sweep: it is what
//! makes the deletion error that `retention::delete_segment_files` now
//! propagates (#470) reachable in production instead of dying in a background
//! task, and it extends the offline-log-dir escalation of #471 to cover
//! retention as well as compaction -- an `unlink` that fails with `EIO` takes
//! the log directory offline just as a failed compaction rewrite does.

use super::storage::{LogStorage, MaintenanceAck, maintain_log};
use crate::replica_state::ReplicaState;

pub(super) async fn handle_retention(
    storage: LogStorage<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: MaintenanceAck,
) {
    // Every eviction reason is bounded at the high watermark, the same as
    // `handle_compact`'s read of it: a record above the watermark can still
    // be truncated away by a leader election, and this is the only task that
    // moves the log, so nothing can append or truncate between this read and
    // the sweep below.
    maintain_log(
        storage,
        replica_state,
        ack,
        "retention task panicked",
        krabka_log::Log::tick,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arc_swap::ArcSwap;

    use super::*;
    use crate::{
        log_dir_status::LogDirRegistry, partition_writer::test_support::open_log_with_records,
    };

    #[tokio::test]
    async fn handle_retention_completes_ack() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(Mutex::new(open_log_with_records(dir.path(), 5)));
        let log_dir = Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf()));
        let log_dir_status = LogDirRegistry::default();
        let replica_state = tokio::sync::Mutex::new(crate::replica_state::ReplicaState::new());
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();

        handle_retention((&log, &log_dir, &log_dir_status), &replica_state, ack_tx).await;
        let result = ack_rx.await.expect("ack sent");
        assert2::check!(result.is_ok());
    }
}
