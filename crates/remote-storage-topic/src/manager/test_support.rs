//! Fixtures shared by the manager submodules' unit tests.
//!
//! Each submodule keeps the tests for the code it holds, and the segment
//! builders, the manager starters, and the flaky-HWM test double here are the
//! parts several of them need. One module for the harness keeps a change to a
//! fixture in one place.

use std::sync::Arc;

use assert2::assert;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{RemoteLogMetadataManager, RemoteStorageError, TopicIdPartition};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::TopicBasedRemoteLogMetadataManager;
use crate::{
    error::MetadataLogError,
    log::{InProcessMetadataEventLog, MetadataEventLog},
    partitioning::metadata_partition_for,
};

/// Test double that delegates to an inner [`InProcessMetadataEventLog`]
/// but can fail `high_water_marks()` on demand. The in-process fixture's
/// HWM RPC always succeeds, which is why the rest of the suite cannot
/// exercise the C1 fail-closed path.
pub struct HwmFlakyLog {
    inner: Arc<InProcessMetadataEventLog>,
    fail_hwm: std::sync::atomic::AtomicBool,
}

impl HwmFlakyLog {
    pub fn new(partition_count: i32) -> Arc<Self> {
        Arc::new(Self {
            inner: InProcessMetadataEventLog::new(partition_count),
            fail_hwm: std::sync::atomic::AtomicBool::new(false),
        })
    }
    pub fn set_fail_hwm(&self, fail: bool) {
        self.fail_hwm
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

#[krabka_macros::metadata_log_delegate(crate)]
#[async_trait::async_trait]
impl MetadataEventLog for HwmFlakyLog {
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
        if self.fail_hwm.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(MetadataLogError::Other("injected HWM failure".into()));
        }
        self.inner.high_water_marks().await
    }
}

static SNAP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn snapshot_test_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "krabka-rlmm-{label}-{}-{}",
        std::process::id(),
        SNAP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

pub fn tp() -> TopicIdPartition {
    TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
}

krabka_macros::remote_segment_fixtures!(started, finish, krabka_remote_storage, next_offset);

/// Run the sync RLMM trait method on the blocking pool, exactly
/// like the broker does.
pub async fn on_blocking<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

/// Poll until `tp` reads `Ok(Some)`, which means assigned and caught up,
/// or panic.
pub async fn wait_ready(m: &Arc<TopicBasedRemoteLogMetadataManager>, tp: &TopicIdPartition) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if matches!(
            m.remote_log_segment_metadata(tp, LeaderEpoch(0), 42),
            Ok(Some(_))
        ) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partition never became ready"
        );
        // Yield-poll the pump's progress rather than sleeping a fixed
        // cadence; the deadline above stays as the hang-guard.
        tokio::task::yield_now().await;
    }
}

pub async fn seeded_manager(
    log: Arc<dyn MetadataEventLog>,
) -> (Arc<TopicBasedRemoteLogMetadataManager>, i32) {
    seed_log(log.clone()).await;
    let partition = metadata_partition_for(&tp(), log.partition_count());
    (start_manager(log), partition)
}

pub async fn wait_finished_metadata(
    manager: &TopicBasedRemoteLogMetadataManager,
    metadata_partition: i32,
    timeout_note: &str,
    unexpected_note: &str,
    gate_note: Option<&str>,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match manager.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42) {
            Ok(Some(metadata)) => {
                assert!(metadata.remote_log_segment_id().id == Uuid::from_u128(10));
                break;
            }
            Err(RemoteStorageError::NotReady { partition }) => {
                if let Some(note) = gate_note {
                    assert!(partition == metadata_partition, "{note}");
                } else {
                    assert!(partition == metadata_partition);
                }
                assert!(std::time::Instant::now() < deadline, "{timeout_note}");
                tokio::task::yield_now().await;
            }
            other => panic!("{unexpected_note}: {other:?}"),
        }
    }
}

/// The periodic snapshot is an hour away, so tests can drive explicit flushes.
pub fn start_manager_in(
    log: Arc<dyn MetadataEventLog>,
    dir: std::path::PathBuf,
) -> Result<Arc<TopicBasedRemoteLogMetadataManager>, RemoteStorageError> {
    TopicBasedRemoteLogMetadataManager::start(
        log,
        Handle::current(),
        dir,
        std::time::Duration::from_hours(1),
    )
}

/// Assign every partition and publish the caller's completed-segment fixture in this directory.
pub async fn start_seeded_manager_in(
    log: Arc<dyn MetadataEventLog>,
    dir: std::path::PathBuf,
    segments: &[(u128, i64, i64)],
) -> Arc<TopicBasedRemoteLogMetadataManager> {
    let manager = start_manager_in(log.clone(), dir).unwrap();
    manager
        .reconcile_assignment(&(0..log.partition_count()).collect::<Vec<_>>())
        .await;
    seed_finished(&manager, segments).await;
    manager
}

/// Start a manager that consumes NOTHING until the caller drives
/// `reconcile_assignment`. The assignment and readiness tests use this,
/// and they assert that pre-assignment reads are a genuine miss.
pub fn start_manager(log: Arc<dyn MetadataEventLog>) -> Arc<TopicBasedRemoteLogMetadataManager> {
    start_manager_in(log, snapshot_test_dir("test")).unwrap()
}

/// Start a manager and assign EVERY metadata partition, which is the
/// eager "consume all" behavior. Tests that publish through the manager
/// and read the result back use this, and so do the multi-broker pre-seed
/// writers. It blocks until each non-empty partition has caught up to its
/// assignment-time HWM, so a subsequent read does not race the pump.
pub async fn start_manager_all(
    log: Arc<dyn MetadataEventLog>,
) -> Arc<TopicBasedRemoteLogMetadataManager> {
    let n = log.partition_count();
    let m = start_manager(log);
    let all: Vec<i32> = (0..n).collect();
    m.reconcile_assignment(&all).await;
    // Wait for the pump to catch up to every assigned partition's HWM so
    // the manager is "ready" for all partitions, mirroring the old
    // bootstrap contract.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !all.iter().all(|&mp| m.metadata_partition_ready(mp)) {
        assert!(
            std::time::Instant::now() < deadline,
            "manager did not catch up on all partitions within 5s"
        );
        tokio::task::yield_now().await;
    }
    m
}

pub async fn seed_finished(
    manager: &Arc<TopicBasedRemoteLogMetadataManager>,
    segments: &[(u128, i64, i64)],
) {
    for &(id, start, end) in segments {
        let m = manager.clone();
        on_blocking(move || {
            m.add_remote_log_segment_metadata(started(id, start, end))
                .unwrap();
        })
        .await;
        let m = manager.clone();
        on_blocking(move || m.update_remote_log_segment_metadata(finish(id)).unwrap()).await;
    }
}

pub async fn seed_log(log: Arc<dyn MetadataEventLog>) {
    let manager = start_manager_all(log).await;
    seed_finished(&manager, &[(10, 0, 99)]).await;
    manager.shutdown();
}
