//! Snapshot capture for the local cache and the resume computation that reads
//! it back.
//!
//! The manager writes an on-disk snapshot so a restart resumes each metadata
//! partition from its committed offset instead of replaying the whole topic.
//! This module holds the write side, the canonical `committed + 1` resume
//! policy, and the accessor the assignment reconciler uses, so the one place
//! that owns that policy is easy to find.

use tracing::instrument;

use super::TopicBasedRemoteLogMetadataManager;
use crate::{
    error::{CodecError, SnapshotError},
    log::PartitionStart,
};

impl TopicBasedRemoteLogMetadataManager {
    /// Capture the pump's committed offsets together with a cache export
    /// under a consistent lock, and write a snapshot.
    ///
    /// This method holds the `applied` lock only long enough to clone the
    /// offsets and run `export()`, which takes the inner partitions lock. No
    /// Kafka round-trip happens inside, so the hold is bounded. Returns the
    /// highest committed offset written, for the "advanced since last"
    /// check.
    #[instrument(skip_all, err)]
    pub(super) fn write_snapshot(&self) -> Result<i64, crate::error::SnapshotError> {
        // Benign-replay invariant: the pump updates `inner` BEFORE bumping
        // `applied`, so the captured cache may lead the captured committed
        // offset by at most one event (the in-flight one). On resume that
        // single event is replayed from committed+1 and harmlessly
        // re-rejected: a re-applied AddSegment hits already-exists, and a
        // re-applied finished→finished update is a no-op. The dangerous
        // direction — cache BEHIND committed, which would skip an event on
        // resume — cannot occur because inner is always updated first.
        let (committed_offsets, dump) = {
            let applied = self.applied.lock().expect("applied mutex poisoned");
            let dump = self.inner.export();
            (applied.clone(), dump)
        };
        let max = committed_offsets.iter().copied().max().unwrap_or(-1);
        let snap = crate::snapshot::Snapshot {
            committed_offsets,
            dump,
        };
        let path = self.snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
        snap.write_atomic(&path)?;
        Ok(max)
    }

    /// Build startup state from the same proved cursor adapter used by
    /// dynamic assignment.
    ///
    /// Given an already-loaded snapshot, or `None` for a missing or corrupt
    /// one, and the metadata-partition count `n`, this function produces:
    ///
    /// - the per-partition committed offsets, indexed by metadata partition
    ///   and padded or truncated to `n`. `-1` means no committed event, so
    ///   that partition needs a full replay.
    /// - the metadata-consumer assignment that resumes each partition at
    ///   `committed + 1`.
    ///
    /// Invalid committed offsets reject the snapshot before its cache dump is
    /// imported. [`Self::resume_start_offset`] is the only place the
    /// `committed + 1` policy lives; do not recompute it elsewhere.
    pub(super) fn resume_from_snapshot(
        snapshot: Option<&crate::snapshot::Snapshot>,
        n: usize,
    ) -> Result<(Vec<i64>, Vec<PartitionStart>), SnapshotError> {
        let mut committed = vec![-1i64; n];
        if let Some(snap) = snapshot {
            for (i, &off) in snap.committed_offsets.iter().take(n).enumerate() {
                Self::resume_start_offset(off)?;
                committed[i] = off;
            }
        }
        let assignment = (0..n)
            .map(|i| PartitionStart {
                partition: i32::try_from(i).expect("metadata partition count came from i32"),
                start_offset: Self::resume_start_offset(committed[i])
                    .expect("committed offsets were validated above"),
            })
            .collect();
        Ok((committed, assignment))
    }

    /// The one adapter from a stored committed offset to an inclusive
    /// consumer cursor. Startup and dynamic assignment both call this exact
    /// function, so their sentinel and overflow rules cannot drift.
    pub(super) fn resume_start_offset(committed: i64) -> Result<i64, SnapshotError> {
        krabka_verified::remote_metadata_resume_cursor(committed).ok_or_else(|| {
            CodecError::Domain(format!(
                "invalid remote-metadata committed offset {committed}"
            ))
            .into()
        })
    }

    /// Committed offset loaded from the snapshot for a single metadata
    /// partition, or `-1` when the partition is out of range or had no
    /// committed event and so needs a full replay. The assignment reconciler
    /// uses this to start a dynamically-added partition at `committed + 1`.
    #[must_use]
    pub fn committed_offset(&self, partition: i32) -> i64 {
        usize::try_from(partition)
            .ok()
            .and_then(|i| self.committed_offsets.get(i).copied())
            .unwrap_or(-1)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_ids::LeaderEpoch;
    use krabka_remote_storage::{PartitionDump, RemoteLogMetadataManager, RlmmCacheDump};

    use super::*;
    use crate::{
        log::{InProcessMetadataEventLog, MetadataEventLog},
        manager::test_support::{finish, on_blocking, snapshot_test_dir, started, tp},
    };

    fn snapshot_with_offsets(committed_offsets: Vec<i64>) -> crate::snapshot::Snapshot {
        crate::snapshot::Snapshot {
            committed_offsets,
            dump: RlmmCacheDump::default(),
        }
    }

    #[test]
    fn startup_and_dynamic_resume_share_exact_cursor_rules() {
        let snapshot = snapshot_with_offsets(vec![4]);
        let first = TopicBasedRemoteLogMetadataManager::resume_from_snapshot(Some(&snapshot), 3)
            .expect("valid cursors");
        let retry = TopicBasedRemoteLogMetadataManager::resume_from_snapshot(Some(&snapshot), 3)
            .expect("retry is deterministic");
        check!(first == retry);
        check!(first.0 == vec![4, -1, -1]);
        check!(first.1.iter().map(|p| p.start_offset).collect::<Vec<_>>() == vec![5, 0, 0]);

        for (committed, expected) in [(-1, 0), (4, 5), (i64::MAX - 1, i64::MAX)] {
            check!(
                TopicBasedRemoteLogMetadataManager::resume_start_offset(committed).unwrap()
                    == expected
            );
        }
    }

    #[test]
    fn malformed_and_overflowing_snapshot_cursors_are_rejected() {
        for committed in [i64::MIN, -2, i64::MAX] {
            let snapshot = snapshot_with_offsets(vec![committed]);
            check!(
                TopicBasedRemoteLogMetadataManager::resume_from_snapshot(Some(&snapshot), 1)
                    .is_err()
            );
            check!(TopicBasedRemoteLogMetadataManager::resume_start_offset(committed).is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_snapshot_cursor_discards_stale_dump_and_replays_from_zero() {
        let dir = snapshot_test_dir("invalid-cursor");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let mp = crate::partitioning::metadata_partition_for(&tp(), log.partition_count());
        let mut committed_offsets = vec![-1; 4];
        committed_offsets[usize::try_from(mp).unwrap()] = i64::MAX;
        let snapshot = crate::snapshot::Snapshot {
            committed_offsets,
            dump: RlmmCacheDump {
                partitions: vec![PartitionDump {
                    topic_id_partition: tp(),
                    segments: vec![started(99, 0, 99)],
                    delete_state: None,
                }],
            },
        };
        snapshot
            .write_atomic(&dir.join(crate::snapshot::SNAPSHOT_FILE_NAME))
            .expect("write invalid semantic snapshot");

        let manager = crate::manager::test_support::start_manager_in(log, dir.clone())
            .expect("invalid cursor falls back to full replay");
        check!(manager.committed_offset(mp) == -1);
        manager.reconcile_assignment(&[mp]).await;
        check!(manager.metadata_partition_ready(mp));
        check!(manager.list_remote_log_segments(&tp()).unwrap().is_empty());
        manager.shutdown();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_snapshot_cursor_replays_the_exact_lifecycle_suffix() {
        let dir = snapshot_test_dir("stale-cursor");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let mp = crate::partitioning::metadata_partition_for(&tp(), log.partition_count());

        let writer =
            crate::manager::test_support::start_manager_in(log.clone(), dir.clone()).unwrap();
        writer.reconcile_assignment(&[mp]).await;
        let writer_for_add = writer.clone();
        on_blocking(move || {
            writer_for_add
                .add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        writer.shutdown_and_flush().await;

        let suffix_offset = log
            .publish(
                mp,
                crate::serde::MetadataEvent::UpdateSegment(finish(10)).encode(),
            )
            .await
            .expect("append lifecycle suffix");
        check!(suffix_offset == 1);

        let resumed = crate::manager::test_support::start_manager_in(log, dir.clone()).unwrap();
        check!(resumed.committed_offset(mp) == 0);
        resumed.reconcile_assignment(&[mp]).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !resumed.metadata_partition_ready(mp) {
            assert!(
                std::time::Instant::now() < deadline,
                "stale snapshot did not consume its exact suffix"
            );
            tokio::task::yield_now().await;
        }
        check!(
            resumed
                .highest_offset_for_epoch(&tp(), LeaderEpoch(0))
                .unwrap()
                == Some(99)
        );
        resumed.shutdown();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_a_snapshot_covering_applied_events() {
        let dir = snapshot_test_dir("mgr-snap");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = crate::manager::test_support::start_seeded_manager_in(
            log.clone(),
            dir.clone(),
            &[(10, 0, 99)],
        )
        .await;

        m.shutdown_and_flush().await;

        let path = dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
        let snap = crate::snapshot::Snapshot::load(&path)
            .unwrap()
            .expect("snapshot written");
        // The orders partition's committed offset covers both events.
        let p = crate::partitioning::metadata_partition_for(&tp(), 4);
        let idx = usize::try_from(p).unwrap();
        check!(
            snap.committed_offsets[idx] >= 1,
            "committed >= last applied offset"
        );
        // The dump contains the finished segment.
        assert!(snap.dump.partitions.len() == 1);
        check!(snap.dump.partitions[0].segments.len() == 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_resumes_from_snapshot_without_replaying_from_zero() {
        let dir = snapshot_test_dir("resume");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);

        // First lifetime: seed three finished segments, then shutdown-flush.
        let pre_cache;
        {
            let m = crate::manager::test_support::start_seeded_manager_in(
                log.clone(),
                dir.clone(),
                &[(10, 0, 99), (11, 100, 199), (12, 200, 299)],
            )
            .await;
            pre_cache = m.list_remote_log_segments(&tp()).unwrap();
            m.shutdown_and_flush().await;
        }

        // Snapshot now records committed offset N for the orders partition.
        let p = crate::partitioning::metadata_partition_for(&tp(), 4);
        let idx = usize::try_from(p).unwrap();
        let snap = crate::snapshot::Snapshot::load(&dir.join(crate::snapshot::SNAPSHOT_FILE_NAME))
            .unwrap()
            .expect("snapshot present");
        let committed = snap.committed_offsets[idx];
        assert!(
            committed >= 5,
            "6 events (3 add + 3 finish) → committed >= 5"
        );

        // The canonical resume computation resumes the orders partition at
        // committed + 1 (same path start() uses).
        let (resumed_committed, assignment) =
            TopicBasedRemoteLogMetadataManager::resume_from_snapshot(Some(&snap), 4)
                .expect("valid snapshot cursor");
        let orders_start = assignment
            .iter()
            .find(|s| s.partition == p)
            .map(|s| s.start_offset)
            .unwrap();
        assert!(orders_start == committed + 1, "resume from N+1, not 0");
        assert!(resumed_committed[idx] == committed);

        // Second lifetime against the SAME log + dir: must resume, not replay.
        let fresh =
            crate::manager::test_support::start_manager_in(log.clone(), dir.clone()).unwrap();
        // The manager exposes the same committed offset via its canonical
        // accessor used by the assignment reconciler.
        assert!(fresh.committed_offset(p) == committed);
        // Assign every partition and wait for catch-up so the gated read
        // methods delegate to the (snapshot-seeded) inner cache. The orders
        // partition has no backlog past `committed`, so it is ready as soon
        // as the assignment-time HWM is recorded.
        fresh
            .reconcile_assignment(&(0..log.partition_count()).collect::<Vec<_>>())
            .await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !fresh.metadata_partition_ready(p) {
            assert!(
                std::time::Instant::now() < deadline,
                "fresh manager did not catch up on the orders partition"
            );
            tokio::task::yield_now().await;
        }
        let post_cache = fresh.list_remote_log_segments(&tp()).unwrap();
        assert!(
            post_cache == pre_cache,
            "post-load cache equals pre-restart cache"
        );
        assert!(
            fresh
                .highest_offset_for_epoch(&tp(), LeaderEpoch(0))
                .unwrap()
                == Some(299)
        );
        fresh.shutdown();
        std::fs::remove_dir_all(&dir).ok();
    }
}
