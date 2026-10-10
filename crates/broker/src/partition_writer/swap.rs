//! The KIP-113 intra-broker log-dir swap that the writer performs in place.
//!
//! The swap is the only writer message that renames directories and reopens
//! two logs under one lock, so its filesystem recovery path is isolated here.

use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use krabka_log::Log;

use super::storage::lock_log;
use crate::partition::SwapOutcome;

/// KIP-113 intra-broker log-dir swap.
///
/// The writer task calls this function. The function holds the partition's
/// `log` mutex for the full rename, so no other appender sees a half-swapped
/// state.
///
/// The future log MUST be caught up: its LEO == the current log's LEO. If a
/// producer added a batch between the caller's catch-up check and this writer
/// cycle, the function reports `NotCaughtUp`. The replicator loop then drains
/// the lag and retries.
///
/// Log end equality is all Kafka checks too
/// (`Partition.runCallbackIfFutureReplicaCaughtUp`). That the future log holds
/// what the current log holds is the replicator's job, as it is the
/// `ReplicaAlterLogDirsThread`'s: see the `future_log` module.
pub(super) fn swap_future_log(
    log: &Arc<Mutex<Log>>,
    log_dir: &Arc<ArcSwap<PathBuf>>,
    target_log_dir: PathBuf,
    future_log: &Arc<Mutex<Log>>,
    future_path: &std::path::Path,
    target_partition_path: &std::path::Path,
) -> Result<SwapOutcome, crate::error::BrokerError> {
    // Acquire both logs under the writer's serialization and re-check
    // the caught-up invariant. If the future log fell behind between
    // the caller's check and this cycle, refuse the swap and let the
    // replicator catch up.
    let mut log_guard = lock_log(log);
    let config = log_guard.config_snapshot();
    let current_stamp_source = log_guard.stamp_source();
    let current_leo = log_guard.log_end_offset();
    let mut future_guard = lock_log(future_log);
    if !krabka_verified::storage::future_log_swap_admission(
        current_leo.0,
        future_guard.log_end_offset().0,
    ) {
        return Ok(SwapOutcome::NotCaughtUp);
    }
    let future_stamp_source = future_guard.stamp_source();

    let source_partition_path = log_guard.dir().to_path_buf();

    // Release segment file descriptors on both Logs before mutating
    // the filesystem. `Log::close` consumes the value, so we move
    // both out via `mem::replace` against throwaway Logs anchored to
    // a sacrificial `*.tomb` directory we delete at the end.
    let tomb_dir = future_path.with_extension("krabka-swap-tomb");
    std::fs::create_dir_all(&tomb_dir)?;
    let old_current = std::mem::replace(&mut *log_guard, Log::open(&tomb_dir, config.clone())?);
    old_current.close();
    let old_future = std::mem::replace(&mut *future_guard, Log::open(&tomb_dir, config.clone())?);
    old_future.close();
    drop(future_guard);

    // Atomically promote the future dir into the canonical
    // `<topic>-<partition>` slot under the target log.dir, then
    // remove the source dir. If the rename fails, reopen the source
    // so the partition keeps serving and bubble the error.
    if let Err(e) = std::fs::rename(future_path, target_partition_path) {
        // Best-effort recovery: reopen the original log in the
        // source dir so the partition keeps serving against the
        // pre-swap location.
        match Log::open(&source_partition_path, config) {
            Ok(mut reopened) => {
                if let Some(stamp_source) = current_stamp_source {
                    reopened.set_stamp_source(stamp_source)?;
                }
                *log_guard = reopened;
            }
            Err(reopen_err) => {
                tracing::error!(
                    error = %reopen_err,
                    "swap_future_log: rename failed AND source reopen failed; \
                     partition is offline until restart"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&tomb_dir);
        return Err(crate::error::BrokerError::from(e));
    }

    if let Err(e) = std::fs::remove_dir_all(&source_partition_path) {
        tracing::warn!(
            source = %source_partition_path.display(),
            error = %e,
            "swap_future_log: failed to remove source partition dir; \
             partition is live at target, source will be cleaned on next restart"
        );
    }
    let _ = std::fs::remove_dir_all(&tomb_dir);

    let mut reopened = Log::open(target_partition_path, config)?;
    if let Some(stamp_source) = future_stamp_source {
        reopened.set_stamp_source(stamp_source)?;
    }
    *log_guard = reopened;
    log_dir.store(Arc::new(target_log_dir));
    Ok(SwapOutcome::Swapped)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;
    use crate::partition_writer::test_support::{FixedStamp, open_log_with_records};

    struct SwapPaths {
        source_dir: PathBuf,
        target_dir: PathBuf,
        source_partition: PathBuf,
        future_path: PathBuf,
        target_partition_path: PathBuf,
    }

    fn swap_paths(dir: &std::path::Path) -> SwapPaths {
        let source_dir = dir.join("source");
        let target_dir = dir.join("target");
        SwapPaths {
            source_partition: source_dir.join("t-0"),
            future_path: target_dir.join("t-0.future"),
            target_partition_path: target_dir.join("t-0"),
            source_dir,
            target_dir,
        }
    }

    #[test]
    fn swap_future_log_accepts_future_at_same_leo() {
        let dir = tempdir().expect("tempdir");
        let SwapPaths {
            source_dir,
            target_dir,
            source_partition,
            future_path,
            target_partition_path,
        } = swap_paths(dir.path());

        let mut source_log = open_log_with_records(&source_partition, 2);
        source_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("source stamp index");
        let log = Arc::new(Mutex::new(source_log));
        let mut staged_log = open_log_with_records(&future_path, 2);
        staged_log
            .set_stamp_source(Arc::new(FixedStamp(7)))
            .expect("future stamp index");
        let future_log = Arc::new(Mutex::new(staged_log));
        let log_dir = Arc::new(ArcSwap::from_pointee(source_dir.clone()));

        let result = swap_future_log(
            &log,
            &log_dir,
            target_dir.clone(),
            &future_log,
            &future_path,
            &target_partition_path,
        )
        .expect("swap");

        // Pull both log observations under one lock acquisition — two
        // `lock()` temporaries in a single assert statement would deadlock.
        let (leo, log_dir_now, has_stamp_source) = {
            let guard = log.lock().unwrap();
            (
                guard.log_end_offset(),
                guard.dir().to_path_buf(),
                guard.stamp_source().is_some(),
            )
        };
        check!(result == SwapOutcome::Swapped);
        check!(leo == 2);
        check!(log_dir_now == target_partition_path.clone());
        check!(has_stamp_source);
        check!(log_dir.load().as_ref().clone() == target_dir);
        check!(!source_partition.exists());
        check!(target_partition_path.exists());
    }

    #[test]
    fn swap_future_log_rejects_mismatched_leo() {
        for (current_records, future_records) in [(2, 1), (1, 2)] {
            let dir = tempdir().expect("tempdir");
            let SwapPaths {
                source_dir,
                target_dir,
                source_partition,
                future_path,
                target_partition_path,
            } = swap_paths(dir.path());

            let log = Arc::new(Mutex::new(open_log_with_records(
                &source_partition,
                current_records,
            )));
            let future_log = Arc::new(Mutex::new(open_log_with_records(
                &future_path,
                future_records,
            )));
            let log_dir = Arc::new(ArcSwap::from_pointee(source_dir.clone()));
            let result = swap_future_log(
                &log,
                &log_dir,
                target_dir,
                &future_log,
                &future_path,
                &target_partition_path,
            )
            .expect("not caught up response");

            let (leo, log_dir_now) = {
                let guard = log.lock().unwrap();
                (guard.log_end_offset(), guard.dir().to_path_buf())
            };
            check!(result == SwapOutcome::NotCaughtUp);
            check!(leo == i64::from(current_records));
            check!(log_dir_now == source_partition);
            check!(log_dir.load().as_ref().clone() == source_dir);
            check!(source_partition.exists());
            check!(future_path.exists());
            check!(!target_partition_path.exists());
        }
    }
}
