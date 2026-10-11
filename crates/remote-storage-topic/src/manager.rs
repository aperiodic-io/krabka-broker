//! [`TopicBasedRemoteLogMetadataManager`]: the production
//! [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager)
//! implementation backed by a publish and subscribe [`MetadataEventLog`].
//!
//! The manager keeps the canonical in-memory view in an
//! [`InmemoryRemoteLogMetadataManager`], so the lifecycle state machine is the
//! single source of truth for cache mutation. It uses the
//! [`MetadataEventLog`] as the durable event log.
//!
//! Lifecycle:
//!
//! - [`TopicBasedRemoteLogMetadataManager::start`][]: load any on-disk
//!   snapshot into the cache and spawn the consumer pump subscribed to
//!   NOTHING. The broker then drives the consumed set with
//!   [`TopicBasedRemoteLogMetadataManager::reconcile_assignment`]. That call
//!   adds only the `__remote_log_metadata` partitions that cover the
//!   user-partitions this broker leads or follows. `NotReady` gates a
//!   newly-added partition until the pump reaches the HWM observed at
//!   assignment time. A partition this broker does not consume is a genuine
//!   `Ok(None)`, and the manager never serves it from any stale cache.
//! - Mutation calls, which are `add`, `update`, and `put_partition_delete`:
//!   serialize, publish, and wait until the consumer pump has applied
//!   the published offset to the inner cache. The sync return means
//!   "the event has been recorded and is visible to local reads".
//! - Read calls: pure local lookups against the inner cache.
//! - Drop / [`TopicBasedRemoteLogMetadataManager::shutdown`]: cancel the consumer pump.

use std::sync::Arc;

use krabka_remote_storage::{InmemoryRemoteLogMetadataManager, RemoteStorageError};
use krabka_units::prelude::{StdDurationExt as _, TimeExt as _};
use tokio::{runtime::Handle, sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::warn;

mod assignment;
mod persistence;
mod pump;
mod rlmm;
#[cfg(test)]
pub(crate) mod test_support;

use self::pump::pump_loop;
use crate::{
    log::{AssignmentHandle, MetadataEventLog},
    snapshot::{SNAPSHOT_FILE_NAME, Snapshot},
};

/// Production [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager)
/// backed by the `__remote_log_metadata` topic through a [`MetadataEventLog`]
/// adapter.
///
/// Construct it with [`Self::start`]. That call loads any on-disk snapshot,
/// but it consumes no metadata partitions until [`Self::reconcile_assignment`]
/// adds the broker's leader-derived and follower-derived set.
pub struct TopicBasedRemoteLogMetadataManager {
    log: Arc<dyn MetadataEventLog>,
    inner: Arc<InmemoryRemoteLogMetadataManager>,
    applied: Arc<std::sync::Mutex<Vec<i64>>>,
    applied_tx: watch::Sender<u64>,
    runtime: Handle,
    shutdown: CancellationToken,
    pump: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Directory the on-disk RLMM cache snapshot is written to (one
    /// [`SNAPSHOT_FILE_NAME`](crate::snapshot::SNAPSHOT_FILE_NAME) file).
    snapshot_dir: std::path::PathBuf,
    /// Handle of the background snapshotter task. `Drop` aborts it, and
    /// [`Self::shutdown_and_flush`] joins it.
    snapshotter: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Live assignment handle for the metadata-log subscription. The manager
    /// holds it so the resume-from-snapshot logic and the per-broker
    /// partition-assignment logic can mutate the consumed set at runtime.
    /// [`Self::reconcile_assignment`] drives it.
    assignment: Arc<dyn AssignmentHandle>,
    /// Per-metadata-partition committed offsets loaded from the snapshot at
    /// `start()`, indexed by metadata partition. `-1` means there is no
    /// committed event for that partition, so it needs a full replay. This
    /// map is the single canonical source for resume-offset lookups. The
    /// assignment reconciler reads it with [`Self::committed_offset`] when it
    /// dynamically adds a partition, so that partition starts at
    /// `committed + 1`.
    committed_offsets: Vec<i64>,
    /// Metadata partition → target HWM observed at assignment time.
    /// Presence means this manager is currently assigned that partition.
    /// Reads for a user-partition that hashes into it return
    /// [`RemoteStorageError::NotReady`] until `applied[mp] >= target - 1`.
    /// The map is empty for managers that never call
    /// [`Self::reconcile_assignment`], and every read then delegates straight
    /// to `inner`.
    ready_targets: Arc<std::sync::Mutex<std::collections::HashMap<i32, i64>>>,
}

impl TopicBasedRemoteLogMetadataManager {
    /// Load any on-disk snapshot into the cache and spawn the consumer
    /// pump with an empty assignment. The manager consumes nothing until the
    /// broker drives [`Self::reconcile_assignment`].
    ///
    /// `runtime` must be a Tokio runtime handle that lives at least as long
    /// as the returned manager. The synchronous
    /// [`RemoteLogMetadataManager`](krabka_remote_storage::RemoteLogMetadataManager)
    /// methods bridge to this handle with `block_on`, so a caller must NOT
    /// call them from a task that runs on this same runtime. The broker
    /// invokes them only through `spawn_blocking`, which is the only
    /// supported call pattern.
    ///
    /// # Errors
    ///
    /// This method is currently infallible, because the consumed set starts
    /// empty. It returns a `Result` so the bootstrap contract stays stable if
    /// `start` regains a fallible step.
    /// # Panics
    /// Panics if an internal lock is poisoned or validated block metadata is inconsistent with its index.
    pub fn start(
        log: Arc<dyn MetadataEventLog>,
        runtime: Handle,
        snapshot_dir: std::path::PathBuf,
        snapshot_interval: std::time::Duration,
    ) -> Result<Arc<Self>, RemoteStorageError> {
        // The parameter is a `Duration` because the broker's config layer hands
        // one over; the cadence is a time extent everywhere below this line.
        let snapshot_interval = snapshot_interval.as_time();
        let n = usize::try_from(log.partition_count()).expect("partition_count fits in usize");
        let (applied_tx, _) = watch::channel(0u64);
        let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();

        // Load the snapshot (if any) ONCE. Validate its resume cursors before
        // seeding the cache from its dump; `resume_from_snapshot` turns the
        // loaded snapshot into the per-partition committed offsets through
        // the same proved adapter that dynamic assignment calls.
        // On absence/corruption, committed[] is all -1 (full replay) and the
        // cache stays empty — never fatal.
        let snapshot = match Snapshot::load(&snapshot_dir.join(SNAPSHOT_FILE_NAME)) {
            Ok(snap) => snap,
            Err(e) => {
                warn!(error = ?e, "topic-based RLMM: snapshot corrupt; starting from empty cache");
                None
            }
        };
        // A freshly-started manager consumes NOTHING. The broker drives
        // the consumed set via [`Self::reconcile_assignment`], adding only the
        // metadata partitions covering user-partitions this broker leads or
        // follows (each resumed at its snapshot `committed + 1`). This is what
        // makes an unassigned partition a genuine `Ok(None)` rather than a
        // false hit from globally-replayed state.
        let committed = match Self::resume_from_snapshot(snapshot.as_ref(), n) {
            Ok((committed, _assignment)) => {
                if let Some(snap) = &snapshot {
                    inner.import(snap.dump.clone());
                }
                committed
            }
            Err(e) => {
                warn!(error = ?e, "topic-based RLMM: snapshot cursor invalid; starting from empty cache");
                Self::resume_from_snapshot(None, n)
                    .expect("missing snapshot always yields full-replay cursors")
                    .0
            }
        };

        // Pre-seed `applied` to the committed offsets so readiness checks for
        // a later-added partition only block on the delta from committed+1 to
        // the assignment-time HWM.
        let applied = Arc::new(std::sync::Mutex::new(committed.clone()));

        let (stream, assignment_handle) = log.subscribe(Vec::new());
        let pump = runtime.spawn(pump_loop(
            stream,
            Arc::clone(&inner),
            Arc::clone(&applied),
            applied_tx.clone(),
            shutdown.clone(),
        ));

        let manager = Arc::new(Self {
            log,
            inner,
            applied,
            applied_tx,
            runtime,
            shutdown,
            pump: std::sync::Mutex::new(Some(pump)),
            snapshot_dir,
            snapshotter: std::sync::Mutex::new(None),
            assignment: assignment_handle,
            committed_offsets: committed,
            ready_targets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        });

        // Spawn the periodic snapshotter: flush whenever the cache
        // advanced since the last write, plus a final flush on shutdown.
        let snapshotter = {
            let weak = Arc::downgrade(&manager);
            let shutdown = manager.shutdown.clone();
            manager.runtime.spawn(async move {
                let mut last_written: i64 = -1;
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(snapshot_interval.to_std()) => {}
                    }
                    let Some(m) = weak.upgrade() else { return };
                    // Only write when the cache advanced since the last snapshot.
                    let highest = {
                        let applied = m.applied.lock().expect("applied mutex poisoned");
                        applied.iter().copied().max().unwrap_or(-1)
                    };
                    if highest > last_written {
                        match m.write_snapshot() {
                            Ok(written) => last_written = written,
                            Err(e) => {
                                warn!(error = ?e, "topic-based RLMM: periodic snapshot failed");
                            }
                        }
                    }
                }
            })
        };
        *manager
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned") = Some(snapshotter);

        // Nothing is consumed at bootstrap (empty assignment), so the manager
        // is immediately ready. Per-partition catch-up after a later
        // `reconcile_assignment` is governed by `metadata_partition_ready`,
        // which gates reads with `NotReady` until the pump reaches the
        // assignment-time HWM.
        Ok(manager)
    }

    /// Cancel the consumer pump. Read methods continue to work against
    /// whatever the pump applied before shutdown. Mutation methods time out
    /// or fail to make progress.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Cancel the pump and the snapshotter, then write a final snapshot that
    /// captures everything applied so far. It is safe to call this once on a
    /// graceful shutdown.
    /// # Panics
    /// Panics if an internal lock is poisoned or validated block metadata is inconsistent with its index.
    pub async fn shutdown_and_flush(&self) {
        self.shutdown.cancel();
        // Take the handle out of the lock BEFORE awaiting it, so the
        // (sync) mutex is not held across the await point.
        let handle = self
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned")
            .take();
        // Let the snapshotter observe cancellation and stop touching
        // `applied` before we take the final consistent capture.
        if let Some(h) = handle {
            let _ = h.await;
        }
        if let Err(e) = self.write_snapshot() {
            warn!(error = ?e, "topic-based RLMM: final snapshot flush failed");
        }
    }
}

impl Drop for TopicBasedRemoteLogMetadataManager {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.pump.lock().expect("pump mutex poisoned").take() {
            handle.abort();
        }
        if let Some(handle) = self
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned")
            .take()
        {
            handle.abort();
        }
    }
}
