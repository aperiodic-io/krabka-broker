//! Log-mutex access and the storage-failure classification that every writer
//! mutation shares.
//!
//! The writer's mutation arms all take the partition log under the same
//! poison-tolerant lock and all report an `io::Error` from the log layer to the
//! broker-wide log-dir registry, so those three helpers and the blocking-pool
//! wrapper that ties them together live in one module.

use {
    crate::{
        blocking::{run_page_cache_io, spawn_blocking},
        error::BrokerError,
        log_dir_status::LogDirRegistry,
        replica_state::ReplicaState,
    },
    arc_swap::ArcSwap,
    krabka_log::Log,
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    },
};

/// The log and the owning directory's storage-failure registry.
pub(super) type LogStorage<'a> = (
    &'a Arc<Mutex<Log>>,
    &'a Arc<ArcSwap<PathBuf>>,
    &'a LogDirRegistry,
);

/// The owning directory's storage-failure registry and the path it keys on.
#[derive(Clone, Copy)]
pub(super) struct StorageStatus<'a> {
    pub(super) log_dir: &'a Arc<ArcSwap<PathBuf>>,
    pub(super) registry: &'a LogDirRegistry,
}

/// Completion of a maintenance pass after storage-error classification.
pub(super) type MaintenanceAck = tokio::sync::oneshot::Sender<Result<(), BrokerError>>;

/// Mark a partition's log dir offline when a mutation hit a storage failure.
///
/// This function inspects a `BrokerError` returned by a partition-writer
/// mutation: `append`, `append_at`, `truncate_to`, `reset_to`, `compact`, or
/// `trim_to_offset`. The supervisor also gives it the error of the
/// leader-epoch checkpoint write that a promotion makes. If the error is a
/// `LogError::Io(_)`, the function marks the partition's owning log dir
/// offline on the broker-wide registry.
///
/// The function is pessimistic. Any `io::Error` from the log layer is a
/// credible disk-failure signal. A false positive, for example a transient
/// `ENOENT` from a misconfigured scratch path, costs one partition's
/// availability. KIP-113 fail-over elsewhere on the cluster keeps the topic
/// live. A false negative silently corrupts produce acks, and this slice
/// exists to prevent that failure mode.
pub(crate) fn flag_storage_failure(
    err: &BrokerError,
    log_dir: &ArcSwap<PathBuf>,
    log_dir_status: &LogDirRegistry,
) -> bool {
    if let BrokerError::Log(krabka_log::LogError::Io(io_err)) = err {
        let dir = log_dir.load();
        return log_dir_status
            .mark_offline(&dir, &format!("partition write/fsync failed: {io_err}"));
    }
    false
}

/// Lock the partition log and recover the guard if the mutex is poisoned.
///
/// A panic in some other critical section can poison the mutex. In this
/// greenfield single-writer model the log data stays consistent enough to keep
/// serving after a poison. The alternative, `expect`, silently kills the writer
/// task because its `JoinHandle` is discarded. That takes the whole partition
/// offline. `into_inner` recovers the guard and keeps the partition live.
pub(super) fn lock_log(log: &Mutex<Log>) -> std::sync::MutexGuard<'_, Log> {
    log.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Build a `BrokerError` that stands in for a panic in a storage closure.
///
/// The closure runs inside `spawn_blocking`. A panic during `write_all` or
/// `fsync` is a credible disk-failure signal, so this function models it as a
/// `LogError::Io`. `flag_storage_failure` recognizes that error and marks the
/// owning log dir offline.
pub(crate) fn storage_failure_error(context: &str, detail: impl std::fmt::Display) -> BrokerError {
    let io = std::io::Error::other(format!("{context}: {detail}"));
    BrokerError::Log(krabka_log::LogError::Io(io))
}

pub(super) async fn run_log_mutation<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, BrokerError> + Send + 'static,
    panic_context: &'static str,
    storage: StorageStatus<'_>,
) -> Result<T, BrokerError> {
    let result = match spawn_blocking(operation).await {
        Ok(value) => value,
        Err(join_err) => Err(storage_failure_error(panic_context, join_err)),
    };
    if let Err(err) = &result {
        flag_storage_failure(err, storage.log_dir, storage.registry);
    }
    result
}

/// [`run_log_mutation`] for an append, which only writes into the page cache:
/// it runs on the calling task instead of the blocking pool, whose thread
/// handoff costs more than the append. The failure handling is the same.
pub(super) async fn run_log_append<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, BrokerError> + Send + 'static,
    panic_context: &'static str,
    storage: StorageStatus<'_>,
) -> Result<T, BrokerError> {
    let result = match run_page_cache_io(operation).await {
        Ok(value) => value,
        Err(panic) => Err(storage_failure_error(panic_context, panic)),
    };
    if let Err(err) = &result {
        flag_storage_failure(err, storage.log_dir, storage.registry);
    }
    result
}

/// Read the writer-owned watermark, then run one clocked maintenance pass
/// under the log lock and apply the usual panic/storage-error classification.
pub(super) async fn maintain_log(
    storage: LogStorage<'_>,
    replica_state: &tokio::sync::Mutex<ReplicaState>,
    ack: MaintenanceAck,
    panic_context: &'static str,
    operation: impl FnOnce(
        &mut Log,
        std::time::SystemTime,
        krabka_log::Offset,
    ) -> Result<(), krabka_log::LogError>
    + Send
    + 'static,
) {
    let (log, log_dir, log_dir_status) = storage;
    let high_watermark = replica_state.lock().await.hw;
    let now = std::time::SystemTime::now();
    let log = Arc::clone(log);
    let result = run_log_mutation(
        move || operation(&mut lock_log(&log), now, high_watermark).map_err(BrokerError::from),
        panic_context,
        StorageStatus {
            log_dir,
            registry: log_dir_status,
        },
    )
    .await;
    let _ = ack.send(result);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{error::BrokerError, log_dir_status::LogDirRegistry},
        assert2::{assert, check},
        tempfile::tempdir,
    };

    struct StorageFixture {
        dir: tempfile::TempDir,
        status: LogDirRegistry,
        log_dir: ArcSwap<std::path::PathBuf>,
    }

    fn storage_fixture() -> StorageFixture {
        let dir = tempdir().expect("tempdir");
        let status = LogDirRegistry::probe(&[dir.path().to_path_buf()]);
        let log_dir = ArcSwap::from_pointee(dir.path().to_path_buf());
        StorageFixture {
            dir,
            status,
            log_dir,
        }
    }

    #[test]
    fn flag_storage_failure_marks_io_errors_offline() {
        let StorageFixture {
            dir,
            status,
            log_dir,
        } = storage_fixture();
        let err = storage_failure_error("append failed", "synthetic EIO");

        assert!(flag_storage_failure(&err, &log_dir, &status));

        assert!(status.is_offline(dir.path()));
        let expected_offline = vec![(
            dir.path().to_path_buf(),
            "partition write/fsync failed: append failed: synthetic EIO".to_string(),
        )];
        assert!(status.offline() == expected_offline);
    }

    #[test]
    fn flag_storage_failure_ignores_non_storage_errors() {
        let StorageFixture {
            dir,
            status,
            log_dir,
        } = storage_fixture();
        let err = BrokerError::UnsupportedApi {
            api_key: 123,
            version: 0,
        };

        check!(!flag_storage_failure(&err, &log_dir, &status));
        check!(!status.is_offline(dir.path()));
        check!(status.offline().is_empty());
    }
}
