//! Cancellation and teardown of in-progress log-directory moves.
//!
//! A move ends here when a later `AlterReplicaLogDirs` redirects it to another
//! directory, when the broker shuts down, or when a `BrokerHandle` drops
//! without an awaited shutdown. Each path cancels the move's token and stops
//! its replicator task; a redirect also removes the staged future directory.

use std::sync::Arc;

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use tokio::task::JoinHandle;

use super::{FutureLogState, MoveError};

pub(super) async fn cancel_move(
    future_logs: &Arc<DashMap<(String, PartitionIndex), Arc<FutureLogState>>>,
    key: &(String, PartitionIndex),
    existing: Arc<FutureLogState>,
) -> Result<(), MoveError> {
    existing.cancel.cancel();
    let task = existing
        .task
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(task) = task {
        task.abort();
        let _ = task.await;
    }
    future_logs.remove_if(key, |_, state| Arc::ptr_eq(state, &existing));
    let future_path = existing.future_path.clone();
    drop(existing);
    if future_path.exists() {
        std::fs::remove_dir_all(&future_path)?;
    }
    Ok(())
}

fn take_move_tasks(
    future_logs: &DashMap<(String, PartitionIndex), Arc<FutureLogState>>,
) -> Vec<JoinHandle<()>> {
    let states: Vec<_> = future_logs
        .iter()
        .map(|entry| Arc::clone(entry.value()))
        .collect();
    future_logs.clear();
    for state in &states {
        state.cancel.cancel();
    }
    states
        .into_iter()
        .filter_map(|state| {
            state
                .task
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        })
        .collect()
}

/// Cancel and await every in-progress log-directory move. Broker shutdown
/// calls this after it has stopped accepting requests and before it stops the
/// partition writers that the move tasks use.
pub(crate) async fn shutdown_moves(
    future_logs: &DashMap<(String, PartitionIndex), Arc<FutureLogState>>,
) {
    let tasks = take_move_tasks(future_logs);
    for task in &tasks {
        task.abort();
    }
    for task in tasks {
        let _ = task.await;
    }
}

/// Best-effort synchronous counterpart used when a
/// [`crate::broker::BrokerHandle`] is dropped without an awaited shutdown.
pub(crate) fn abort_moves(future_logs: &DashMap<(String, PartitionIndex), Arc<FutureLogState>>) {
    for task in take_move_tasks(future_logs) {
        task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use assert2::assert;
    use krabka_log::{Log, LogConfig};
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;

    macro_rules! pending_move_fixture {
        ($dir:ident, $log:ident, $logs:ident, $started:ident, $dropped:ident) => {
            let $dir = tempdir().expect("tempdir");
            let $log = Arc::new(Mutex::new(
                Log::open($dir.path(), LogConfig::default()).expect("open future log"),
            ));
            let $logs = DashMap::new();
            let $started = Arc::new(AtomicU64::new(0));
            let $dropped = Arc::new(AtomicU64::new(0));
        };
    }

    struct DropCounter(Arc<AtomicU64>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn pending_move(started: &Arc<AtomicU64>, dropped: &Arc<AtomicU64>) -> JoinHandle<()> {
        let started = Arc::clone(started);
        let dropped = Arc::clone(dropped);
        tokio::spawn(async move {
            let _drop_counter = DropCounter(dropped);
            started.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        })
    }

    #[tokio::test]
    async fn shutdown_moves_cancels_and_awaits_every_task() {
        pending_move_fixture!(dir, future_log, future_logs, started, dropped);
        let mut cancels = Vec::new();

        for partition in [PartitionIndex(0), PartitionIndex(1)] {
            let cancel = CancellationToken::new();
            let task = pending_move(&started, &dropped);
            future_logs.insert(
                ("t".to_string(), partition),
                Arc::new(FutureLogState {
                    target_log_dir: dir.path().to_path_buf(),
                    future_path: dir.path().to_path_buf(),
                    future_log: Arc::clone(&future_log),
                    cancel: cancel.clone(),
                    task: Mutex::new(Some(task)),
                }),
            );
            cancels.push(cancel);
        }

        crate::test_support::wait_tasks_started(
            || started.load(Ordering::SeqCst) == 2,
            "move tasks start",
        )
        .await;

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            shutdown_moves(&future_logs),
        )
        .await
        .expect("move task shutdown completes");

        assert!(future_logs.is_empty());
        assert!(cancels.iter().all(CancellationToken::is_cancelled));
        assert!(dropped.load(Ordering::SeqCst) == 2);
    }

    #[tokio::test]
    async fn abort_moves_cancels_and_aborts_every_task() {
        pending_move_fixture!(dir, future_log, future_logs, started, dropped);
        let cancel = CancellationToken::new();
        let task = pending_move(&started, &dropped);
        future_logs.insert(
            ("t".to_string(), PartitionIndex(0)),
            Arc::new(FutureLogState {
                target_log_dir: dir.path().to_path_buf(),
                future_path: dir.path().to_path_buf(),
                future_log,
                cancel: cancel.clone(),
                task: Mutex::new(Some(task)),
            }),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            while started.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("move task starts");

        abort_moves(&future_logs);

        assert!(future_logs.is_empty());
        assert!(cancel.is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            while dropped.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted move task drops");
    }
}
