//! Remote-segment and remote-partition deletion: the shared
//! `DeleteSegmentStarted` to `DeleteSegmentFinished` lifecycle, and the
//! partition-wide cascade that a `DeleteTopics` request starts.

use {
    super::{archive::ArchiveMode, now_ms, rlmm::rlmm_mutate},
    crate::blocking::spawn_blocking,
    krabka_remote_storage::{
        RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentState,
        RemotePartitionDeleteMetadata, RemotePartitionDeleteState, RemoteStorageManager,
        TopicIdPartition,
    },
    std::sync::Arc,
    tracing::{debug, warn},
};

/// KIP-405: cascade the
/// [`DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`] lifecycle for `tp`, and delete every remote
/// segment along the way. The `DeleteTopics` handler runs this as a detached
/// task, so the response does not wait on remote-tier I/O. A failure logs
/// at WARN. Leftover `DeleteSegmentStarted` segments are harmless in the
/// in-memory RLMM, because a `DeleteTopics`-recreate combination regenerates
/// the topic id and the new partition is a fresh `TopicIdPartition`.
///
/// # A write-once archive keeps every byte
///
/// Under [`ArchiveMode::WriteOnce`] the cascade still walks
/// `DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`, and still clears the partition's segment
/// metadata, but it removes nothing from the archive. Deleting a Kafka topic
/// is a cluster operation; it is not, and must not become, an instruction to
/// erase a compliance archive. The archived segments and their manifests
/// outlive the topic, and the verifier reads them without any broker.
pub(crate) async fn cascade_remote_partition_delete(
    tp: TopicIdPartition,
    broker_id: i32,
    archive: ArchiveMode,
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
    index_cache: Arc<krabka_remote_storage::RemoteIndexCache>,
) {
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionMarked,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to mark partition deleted");
        return;
    }
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionStarted,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to start partition delete");
        return;
    }

    let segments = match rlmm.list_remote_log_segments(&tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list segments for partition delete");
            return;
        }
    };
    for md in segments {
        // Skip segments already past `DeleteSegmentStarted` (no-op delete).
        if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
            continue;
        }
        let _ = delete_one_segment(&tp, broker_id, &md, archive, &rsm, &rlmm, &index_cache).await;
    }

    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to finish partition delete");
    }
}

async fn put_partition_state(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
    state: RemotePartitionDeleteState,
    broker_id: i32,
) -> Result<(), krabka_remote_storage::RemoteStorageError> {
    let md = RemotePartitionDeleteMetadata {
        topic_id_partition: tp.clone(),
        state,
        event_timestamp_ms: now_ms(),
        broker_id,
    };
    rlmm_mutate(rlmm, move |m| m.put_remote_partition_delete_metadata(md)).await
}

/// Drive one `CopySegmentFinished` (or in-flight) segment through the
/// `DeleteSegmentStarted` → RSM delete → `DeleteSegmentFinished` chain.
/// Returns `true` when the lifecycle completes cleanly. Shared by
/// [`remote_retention_pass`](super::remote_retention::remote_retention_pass)
/// and [`cascade_remote_partition_delete`].
///
/// Under [`ArchiveMode::WriteOnce`] the RSM delete is skipped outright and
/// only the metadata lifecycle advances. Calling it would fail — the backend
/// refuses every delete, and the bucket's object-lock policy refuses it under
/// that — so the skip is what keeps a routine pass from logging an error every
/// tick.
pub(super) async fn delete_one_segment(
    tp: &TopicIdPartition,
    broker_id: i32,
    md: &RemoteLogSegmentMetadata,
    archive: ArchiveMode,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    index_cache: &Arc<krabka_remote_storage::RemoteIndexCache>,
) -> bool {
    let id = md.remote_log_segment_id().clone();
    // Kafka drops a segment's `RemoteIndexCache` entries as it enters
    // `DeleteSegmentStarted`, so a segment nothing can read any more stops
    // holding the cache's byte budget against the segments that are still
    // readable.
    index_cache.remove_segment(id.id);
    // Transition to DeleteSegmentStarted unless the segment is already
    // there (cascade may retry against a partially-cleaned partition).
    if md.state() == RemoteLogSegmentState::CopySegmentFinished
        && let Err(e) = super::rlmm::update_segment(
            rlmm,
            id.clone(),
            None,
            RemoteLogSegmentState::DeleteSegmentStarted,
            broker_id,
        )
        .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
              error = %e,
              "remote-log-manager: failed to record DeleteSegmentStarted");
        return false;
    }

    match archive {
        ArchiveMode::Mutable => {
            // RSM delete (blocking).
            let rsm_del = rsm.clone();
            let md_del = md.clone();
            let delete_result =
                spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
            match delete_result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete failed");
                    return false;
                }
                Err(e) => {
                    warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                          error = %e, "remote-log-manager: RSM delete task panicked");
                    return false;
                }
            }
        }
        // DEBUG and not WARN on purpose: deleting a topic with ten thousand
        // archived segments would otherwise emit ten thousand warnings for
        // behavior that is working exactly as configured.
        ArchiveMode::WriteOnce => {
            debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                   worm_retained = true,
                   "remote-log-manager: retaining remote segment data; the tier is a \
                    write-once archive");
        }
    }

    if let Err(e) = super::rlmm::update_segment(
        rlmm,
        id,
        None,
        RemoteLogSegmentState::DeleteSegmentFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
              error = %e, "remote-log-manager: failed to record DeleteSegmentFinished");
        return false;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
           worm_retained = archive == ArchiveMode::WriteOnce,
           "remote-log-manager: remote segment reached DeleteSegmentFinished");
    true
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_log_manager::test_support as fixtures,
        assert2::{assert, check},
        fixtures::{copy_all_exports, copy_exports, local_backends},
        krabka_remote_storage::{InmemoryRemoteLogMetadataManager, LocalTieredStorage},
    };

    fn dumped_delete_state(
        rlmm: &InmemoryRemoteLogMetadataManager,
    ) -> Option<RemotePartitionDeleteState> {
        let dump = rlmm.export();
        dump.partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped")
            .delete_state
    }

    /// The delete paths take an index cache so a segment they remove stops
    /// holding its bytes. These tests assert on the RLMM lifecycle, so the
    /// cache they pass stores nothing.
    fn disabled_index_cache() -> Arc<krabka_remote_storage::RemoteIndexCache> {
        Arc::new(krabka_remote_storage::RemoteIndexCache::disabled())
    }
    use fixtures::{FakeWormArchive, tier, tp};

    #[tokio::test]
    async fn cascade_remote_partition_delete_drops_every_segment() {
        let fixtures::RolledLogFixture {
            local: _log_dir,
            remote: remote_dir,
            log: _log,
            exports,
        } = fixtures::rolled_log_fixture();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        copy_all_exports(&tier(ArchiveMode::Mutable, &rsm, &rlmm), &exports).await;

        cascade_remote_partition_delete(
            tp(),
            1,
            ArchiveMode::Mutable,
            rsm.clone(),
            rlmm.clone(),
            disabled_index_cache(),
        )
        .await;

        // All segments are gone from the cache.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
        // The remote directory for this partition is empty (or absent).
        // Kafka LocalTieredStorage layout:
        // <remote_dir>/<topic>-<partition>-<topic_id_base64>/.
        let part_dir = remote_dir.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        let entries: Vec<_> = std::fs::read_dir(&part_dir).unwrap().collect();
        assert!(entries.is_empty(), "stray remote files: {entries:?}");
        let state = dumped_delete_state(&rlmm_impl);
        assert!(state == Some(RemotePartitionDeleteState::DeletePartitionFinished));
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_is_noop_on_empty_partition() {
        let remote_dir = tempfile::tempdir().unwrap();
        let fixtures::Backends { rsm, rlmm } = local_backends(remote_dir.path());
        // No add — partition has no segments. Cascade still walks the
        // three partition-delete states without error.
        cascade_remote_partition_delete(
            tp(),
            1,
            ArchiveMode::Mutable,
            rsm,
            rlmm.clone(),
            disabled_index_cache(),
        )
        .await;
        // No segments after, no panics; that's the test.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn cascade_partition_delete_retains_archive_objects_but_finishes_the_lifecycle() {
        let archive = Arc::new(FakeWormArchive::new());
        let rsm: Arc<dyn RemoteStorageManager> = archive.clone();
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_exports(
            &tier(ArchiveMode::WriteOnce, &rsm, &rlmm),
            fixtures::two_exports(),
        )
        .await;
        check!(copied == 2);

        // The RSM panics on delete, so reaching one fails this test.
        cascade_remote_partition_delete(
            tp(),
            1,
            ArchiveMode::WriteOnce,
            rsm.clone(),
            rlmm.clone(),
            disabled_index_cache(),
        )
        .await;

        check!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "the broker's own metadata is still cleared"
        );
        let state = dumped_delete_state(&rlmm_impl);
        check!(state == Some(RemotePartitionDeleteState::DeletePartitionFinished));
        check!(
            archive.archived_segments() == 2,
            "deleting a topic must not erase a compliance archive"
        );
    }
}
