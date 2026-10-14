//! [`InmemoryRemoteLogMetadataManager`] is a process-memory reference
//! [`RemoteLogMetadataManager`]. It mirrors Kafka's test fixture of the same
//! name. Tiered-storage tests run against this manager.

use {
    crate::{
        cache::{RemoteLogMetadataCache, sort_by_start_offset},
        dump::{PartitionDump, RlmmCacheDump},
        error::RemoteStorageError,
        metadata::{
            RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
            RemotePartitionDeleteMetadata, RemotePartitionDeleteState, TopicIdPartition,
        },
        metadata_manager::RemoteLogMetadataManager,
    },
    krabka_ids::LeaderEpoch,
    std::{collections::HashMap, sync::Mutex},
};

/// In-memory [`RemoteLogMetadataManager`]: one `RemoteLogMetadataCache` per
/// partition behind a single mutex.
///
/// It is not durable, and the state is lost on restart. It does enforce the
/// full lifecycle state machine, so it is a faithful stand-in for the
/// topic-backed production manager in tests and single-process setups.
#[derive(Debug, Default)]
pub struct InmemoryRemoteLogMetadataManager {
    partitions: Mutex<HashMap<TopicIdPartition, RemoteLogMetadataCache>>,
}

impl InmemoryRemoteLogMetadataManager {
    /// Constructs an empty manager.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Dumps every partition's segment and partition-delete metadata for a
    /// snapshot. The result is order-independent. [`Self::import`] re-derives
    /// the order and the epoch index.
    #[must_use]
    /// # Panics
    /// Panics if the partition-cache mutex is poisoned.
    pub fn export(&self) -> RlmmCacheDump {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        let mut partitions: Vec<PartitionDump> = guard
            .iter()
            .map(|(tp, cache)| PartitionDump {
                topic_id_partition: tp.clone(),
                segments: cache.dump_segments(),
                delete_state: cache.delete_state(),
            })
            .collect();
        // Stable order so `export()` is deterministic and comparable.
        partitions.sort_by(|a, b| {
            (
                a.topic_id_partition.topic_id,
                a.topic_id_partition.partition,
            )
                .cmp(&(
                    b.topic_id_partition.topic_id,
                    b.topic_id_partition.partition,
                ))
        });
        // Within a partition, sort segments by (start_offset, id) so the
        // dump is canonical regardless of HashMap iteration order.
        for p in &mut partitions {
            sort_by_start_offset(&mut p.segments);
        }
        RlmmCacheDump { partitions }
    }

    /// Seeds the cache from a dump and does no transition validation.
    ///
    /// Use it on a freshly-constructed manager during a snapshot restore. It
    /// overwrites existing partitions.
    ///
    /// # Panics
    /// Panics if the partition-cache mutex is poisoned.
    pub fn import(&self, dump: RlmmCacheDump) {
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        for p in dump.partitions {
            let cache = guard.entry(p.topic_id_partition).or_default();
            cache.seed(p.segments, p.delete_state);
        }
    }
}

impl RemoteLogMetadataManager for InmemoryRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        guard.entry(tp).or_default().add(metadata)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let tp = update.remote_log_segment_id.topic_id_partition.clone();
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        match guard.get_mut(&tp) {
            Some(cache) => cache.update(&update),
            None => Err(RemoteStorageError::SegmentNotFound(
                update.remote_log_segment_id,
            )),
        }
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .and_then(|c| c.segment_for(leader_epoch, offset)))
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .and_then(|c| c.highest_offset_for_epoch(leader_epoch)))
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .map_or_default(RemoteLogMetadataCache::list))
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .map_or_default(|c| c.list_by_epoch(leader_epoch)))
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        let cache = guard
            .entry(metadata.topic_id_partition.clone())
            .or_default();
        let from = cache.delete_state();
        if !RemotePartitionDeleteState::is_valid_transition(from, metadata.state) {
            return Err(RemoteStorageError::InvalidPartitionDeleteTransition {
                tp: metadata.topic_id_partition,
                from,
                to: metadata.state,
            });
        }
        cache.set_delete_state(metadata.state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::metadata::{RemotePartitionDeleteState, test_topic_partition as tp},
        assert2::{assert, check},
        uuid::Uuid,
    };

    krabka_macros::remote_segment_fixtures!(started, finish, crate, end);

    krabka_macros::remote_metadata_missing!(check_missing_partition, crate);

    krabka_macros::remote_segment_check!(check_finished_segment);

    fn manager_with_finished_segment() -> InmemoryRemoteLogMetadataManager {
        let manager = InmemoryRemoteLogMetadataManager::new();
        manager
            .add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        manager
            .update_remote_log_segment_metadata(finish(10))
            .unwrap();
        manager
    }

    #[test]
    fn add_finish_query_round_trip() {
        let m = manager_with_finished_segment();

        check_finished_segment(&m, &tp());
    }

    #[test]
    fn query_unknown_partition_is_none_not_error() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        check_missing_partition(&m, &other);
    }

    #[test]
    fn list_by_epoch_returns_added_segment() {
        let m = manager_with_finished_segment();
        let listed = m
            .list_remote_log_segments_by_epoch(&tp(), LeaderEpoch(0))
            .unwrap();
        assert!(listed.len() == 1);
        assert!(listed[0].remote_log_segment_id().id == Uuid::from_u128(10));
    }

    #[test]
    fn inmemory_read_outcomes_are_some_none_never_not_ready() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();

        // Found.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 42),
            Ok(Some(_))
        ));
        // Caught up, no covering segment → genuine miss.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), LeaderEpoch(0), 10_000),
            Ok(None)
        ));
        // Unknown partition → genuine miss, NOT NotReady.
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        let got = m.remote_log_segment_metadata(&other, LeaderEpoch(0), 0);
        assert!(matches!(got, Ok(None)));
        assert!(
            !matches!(got, Err(RemoteStorageError::NotReady { .. })),
            "in-memory manager has no consumer lag; never NotReady"
        );
    }

    #[test]
    fn update_before_add_errors() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let err = m
            .update_remote_log_segment_metadata(finish(10))
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn list_returns_all_states_ordered() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(11, 100, 199))
            .unwrap();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        let listed = m.list_remote_log_segments(&tp()).unwrap();
        let start_offsets: Vec<i64> = listed
            .iter()
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect();
        assert!(start_offsets == [0, 100]);
    }

    #[test]
    fn partition_delete_lifecycle() {
        let m = InmemoryRemoteLogMetadataManager::new();
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state,
                event_timestamp_ms: 500,
                broker_id: 1,
            })
            .unwrap();
        }
    }

    #[test]
    fn export_then_import_reproduces_cache() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.add_remote_log_segment_metadata(started(11, 100, 199))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
            topic_id_partition: tp(),
            state: RemotePartitionDeleteState::DeletePartitionMarked,
            event_timestamp_ms: 500,
            broker_id: 1,
        })
        .unwrap();

        let dump = m.export();
        let restored = InmemoryRemoteLogMetadataManager::new();
        restored.import(dump);

        // list_remote_log_segments matches across the partition.
        let before = m.list_remote_log_segments(&tp()).unwrap();
        let after = restored.list_remote_log_segments(&tp()).unwrap();
        check!(before == after);
        // Finished segment still queryable post-import.
        check!(
            restored
                .highest_offset_for_epoch(&tp(), LeaderEpoch(0))
                .unwrap()
                == Some(99)
        );
        // Re-exporting yields the same dump (idempotent round trip).
        check!(m.export() == restored.export());
    }

    #[test]
    fn partition_delete_rejects_out_of_order() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let err = m
            .put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state: RemotePartitionDeleteState::DeletePartitionFinished,
                event_timestamp_ms: 500,
                broker_id: 1,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            RemoteStorageError::InvalidPartitionDeleteTransition { .. }
        ));
    }
}
