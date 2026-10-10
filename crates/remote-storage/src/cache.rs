//! `RemoteLogMetadataCache` holds per-partition metadata state with the
//! lifecycle state machine and an epoch-indexed offset lookup.
//!
//! Mirrors Kafka's `RemoteLogMetadataCache`. The cache holds segments in an
//! id-keyed map. When a segment reaches
//! [`CopySegmentFinished`](RemoteLogSegmentState::CopySegmentFinished), the
//! cache also indexes it per leader epoch by the offset at which that epoch
//! starts to contribute, so an `(epoch, offset)` query is a navigable-map
//! floor lookup. When a segment leaves the readable set at
//! [`DeleteSegmentStarted`](RemoteLogSegmentState::DeleteSegmentStarted), the
//! cache removes it from the epoch index.
//! [`DeleteSegmentFinished`](RemoteLogSegmentState::DeleteSegmentFinished)
//! drops it entirely.

use std::collections::{BTreeMap, HashMap, HashSet};

use krabka_ids::LeaderEpoch;
use krabka_verified::{
    remote_read_relative_offset,
    storage::{RemoteCacheAction, remote_cache_action},
};
use uuid::Uuid;

use crate::{
    error::RemoteStorageError,
    metadata::{
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
        RemotePartitionDeleteState,
    },
};

#[derive(Debug, Default)]
pub(crate) struct RemoteLogMetadataCache {
    /// Every known segment, keyed by its per-segment UUID.
    id_to_metadata: HashMap<Uuid, RemoteLogSegmentMetadata>,
    /// For each leader epoch, a navigable map from the offset that the epoch
    /// starts to contribute → the finished segment id. Only finished,
    /// readable segments appear here.
    epoch_to_offset_to_id: HashMap<LeaderEpoch, BTreeMap<i64, Uuid>>,
    /// Partition-delete lifecycle state, once marked.
    delete_state: Option<RemotePartitionDeleteState>,
}

impl RemoteLogMetadataCache {
    pub(crate) fn add(
        &mut self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let id = metadata.remote_log_segment_id().clone();
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        if self.id_to_metadata.contains_key(&id.id) {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: "segment id already exists".into(),
            });
        }
        self.id_to_metadata.insert(id.id, metadata);
        Ok(())
    }

    pub(crate) fn update(
        &mut self,
        update: &RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let id = update.remote_log_segment_id.clone();
        let Some(existing) = self.id_to_metadata.get(&id.id).cloned() else {
            return match remote_cache_action(None, update.state.lifecycle(), false) {
                RemoteCacheAction::Noop => Ok(()),
                _ => Err(RemoteStorageError::SegmentNotFound(id)),
            };
        };
        let exact_retry = update.state == existing.state()
            && update.event_timestamp_ms == existing.event_timestamp_ms()
            && update.broker_id == existing.broker_id()
            && update
                .custom_metadata
                .as_ref()
                .is_none_or(|custom| existing.custom_metadata() == Some(custom));

        match remote_cache_action(
            Some(existing.state().lifecycle()),
            update.state.lifecycle(),
            exact_retry,
        ) {
            RemoteCacheAction::Reject => Err(RemoteStorageError::InvalidSegmentTransition {
                id,
                from: existing.state(),
                to: update.state,
            }),
            RemoteCacheAction::Noop => Ok(()),
            RemoteCacheAction::StoreFinished | RemoteCacheAction::StoreHidden => {
                let updated = existing.with_update(update)?;
                self.id_to_metadata.insert(id.id, updated);
                self.rebuild_epoch_index();
                Ok(())
            }
            RemoteCacheAction::Remove => {
                existing.with_update(update)?;
                self.id_to_metadata.remove(&id.id);
                self.rebuild_epoch_index();
                Ok(())
            }
        }
    }

    /// Rebuild the derived map from canonical primary state. Sorting makes a
    /// malformed same-epoch/same-start collision deterministic; the lowest
    /// UUID owns the slot until it leaves readable state, after which the
    /// next finished candidate becomes visible.
    fn rebuild_epoch_index(&mut self) {
        let mut entries = self
            .id_to_metadata
            .values()
            .filter(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished)
            .flat_map(|metadata| {
                let id = metadata.remote_log_segment_id().id;
                metadata
                    .segment_leader_epochs()
                    .iter()
                    .map(move |(&epoch, &start)| (epoch, start, id))
            })
            .collect::<Vec<_>>();
        entries.sort_unstable();
        self.epoch_to_offset_to_id.clear();
        for (epoch, start, id) in entries {
            self.epoch_to_offset_to_id
                .entry(epoch)
                .or_default()
                .entry(start)
                .or_insert(id);
        }
    }

    pub(crate) fn segment_for(
        &self,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Option<RemoteLogSegmentMetadata> {
        let map = self.epoch_to_offset_to_id.get(&leader_epoch)?;
        let (_start, id) = map.range(..=offset).next_back()?;
        let md = self.id_to_metadata.get(id)?;
        let (epoch_start, next_epoch_start) = md.epoch_bounds(leader_epoch);
        if remote_read_relative_offset(
            md.start_offset(),
            md.end_offset(),
            offset,
            md.state() == RemoteLogSegmentState::CopySegmentFinished,
            epoch_start,
            next_epoch_start,
        )
        .is_some()
        {
            Some(md.clone())
        } else {
            None
        }
    }

    pub(crate) fn highest_offset_for_epoch(&self, leader_epoch: LeaderEpoch) -> Option<i64> {
        let map = self.epoch_to_offset_to_id.get(&leader_epoch)?;
        map.values()
            .filter_map(|id| self.id_to_metadata.get(id))
            .filter(|metadata| {
                metadata.state() == RemoteLogSegmentState::CopySegmentFinished
                    && metadata.segment_leader_epochs().contains_key(&leader_epoch)
            })
            .map(RemoteLogSegmentMetadata::end_offset)
            .max()
    }

    pub(crate) fn list(&self) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> =
            self.id_to_metadata.values().cloned().collect();
        sort_by_start_offset(&mut out);
        out
    }

    pub(crate) fn list_by_epoch(&self, leader_epoch: LeaderEpoch) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> = self
            .id_to_metadata
            .values()
            .filter(|metadata| {
                metadata.state() == RemoteLogSegmentState::CopySegmentFinished
                    && metadata.segment_leader_epochs().contains_key(&leader_epoch)
            })
            .cloned()
            .collect();
        sort_by_start_offset(&mut out);
        out
    }

    /// Every tracked segment, in all states, unordered. The owning manager
    /// pairs this with [`Self::delete_state`] to dump the partition for a
    /// snapshot.
    pub(crate) fn dump_segments(&self) -> Vec<RemoteLogSegmentMetadata> {
        self.id_to_metadata.values().cloned().collect()
    }

    /// Replace this cache from a dump without replaying lifecycle transitions.
    ///
    /// Duplicate UUIDs fold toward the most advanced lifecycle state, and a
    /// `delete_finished` UUID remains tombstoned for the whole pass so later
    /// stale rows cannot resurrect it. The seed then rebuilds the per-epoch
    /// index from finished primary entries exactly as the live path does.
    /// Repeating the same seed is idempotent. The partition-delete state is
    /// replaced verbatim.
    pub(crate) fn seed(
        &mut self,
        segments: Vec<RemoteLogSegmentMetadata>,
        delete_state: Option<RemotePartitionDeleteState>,
    ) {
        self.id_to_metadata.clear();
        self.epoch_to_offset_to_id.clear();
        let mut tombstones = HashSet::new();
        for md in segments {
            let id = md.remote_log_segment_id().id;
            if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
                self.id_to_metadata.remove(&id);
                tombstones.insert(id);
            } else if !tombstones.contains(&id) {
                match self.id_to_metadata.entry(id) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(md);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if md.state().lifecycle() > entry.get().state().lifecycle() {
                            entry.insert(md);
                        }
                    }
                }
            }
        }
        self.rebuild_epoch_index();
        self.delete_state = delete_state;
    }

    pub(crate) fn delete_state(&self) -> Option<RemotePartitionDeleteState> {
        self.delete_state
    }

    pub(crate) fn set_delete_state(&mut self, state: RemotePartitionDeleteState) {
        self.delete_state = Some(state);
    }
}

fn sort_by_start_offset(segments: &mut [RemoteLogSegmentMetadata]) {
    segments.sort_by(|a, b| {
        a.start_offset().cmp(&b.start_offset()).then_with(|| {
            a.remote_log_segment_id()
                .id
                .cmp(&b.remote_log_segment_id().id)
        })
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::metadata::{CustomMetadata, RemoteLogSegmentId, TopicIdPartition};

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "t", 0)
    }

    use krabka_ids::Offset;

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct CacheSegmentSetup<'a> {
        #[default(Uuid::from_u128(10))]
        id: Uuid,
        #[default(&[(LeaderEpoch(0), Offset(0))])]
        epochs: &'a [(LeaderEpoch, Offset)],
        #[default(Offset(0))]
        start: Offset,
        #[default(Offset(99))]
        end: Offset,
    }

    impl CacheSegmentSetup<'_> {
        fn following_segment() -> Self {
            Self {
                id: Uuid::from_u128(11),
                epochs: &[(LeaderEpoch(0), Offset(100))],
                start: Offset(100),
                end: Offset(199),
            }
        }
    }

    fn seg(setup: CacheSegmentSetup<'_>) -> RemoteLogSegmentMetadata {
        let CacheSegmentSetup {
            id,
            epochs,
            start,
            end,
        } = setup;
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), id),
            start.0,
            end.0,
            end.0,
            1,
            100,
            crate::metadata::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                epochs
                    .iter()
                    .map(|&(epoch, start)| (epoch, start.0))
                    .collect(),
            ),
        )
        .unwrap()
    }

    fn finish(id: Uuid) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), id),
            event_timestamp_ms: 200,
            custom_metadata: Some(CustomMetadata(vec![1])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        }
    }

    fn transition(id: Uuid, state: RemoteLogSegmentState) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), id),
            event_timestamp_ms: 300,
            custom_metadata: None,
            state,
            broker_id: 1,
        }
    }

    #[test]
    fn started_segment_is_invisible_until_finished() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "started not yet readable"
        );
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        let got = c
            .segment_for(LeaderEpoch(0), 50)
            .expect("finished is readable");
        assert!(got.remote_log_segment_id().id == Uuid::from_u128(10));
    }

    fn one_finished_segment() -> RemoteLogMetadataCache {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        c
    }

    fn two_finished_segments() -> RemoteLogMetadataCache {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        c.add(seg(CacheSegmentSetup::following_segment())).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        c
    }

    #[test]
    fn offset_lookup_across_segments_one_epoch() {
        let c = two_finished_segments();
        for (offset, want) in [
            (0, Some(Uuid::from_u128(10))),
            (99, Some(Uuid::from_u128(10))),
            (100, Some(Uuid::from_u128(11))),
            (199, Some(Uuid::from_u128(11))),
            // past the end
            (200, None),
        ] {
            check!(
                c.segment_for(LeaderEpoch(0), offset)
                    .map(|s| s.remote_log_segment_id().id)
                    == want,
                "offset={offset}"
            );
        }
    }

    #[test]
    fn offset_lookup_rejects_offsets_below_segment_start_and_extreme_spans() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup {
            start: Offset(100),
            end: Offset(199),
            ..Default::default()
        }))
        .unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());

        c.add(seg(CacheSegmentSetup {
            id: Uuid::from_u128(11),
            epochs: &[(LeaderEpoch(1), Offset(i64::MIN))],
            start: Offset(i64::MIN),
            end: Offset(i64::MAX),
        }))
        .unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        check!(c.segment_for(LeaderEpoch(1), i64::MAX).is_none());
    }

    #[test]
    fn list_by_epoch_returns_matching_segments() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        assert!(
            c.list_by_epoch(LeaderEpoch(0)).is_empty(),
            "copy-started is not epoch-readable"
        );
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        let listed_ids: Vec<Uuid> = c
            .list_by_epoch(LeaderEpoch(0))
            .iter()
            .map(|s| s.remote_log_segment_id().id)
            .collect();
        assert!(listed_ids == [Uuid::from_u128(10)]);
        assert!(
            c.list_by_epoch(LeaderEpoch(7)).is_empty(),
            "unknown epoch -> empty"
        );
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(
            c.list_by_epoch(LeaderEpoch(0)).is_empty(),
            "delete-started is not epoch-readable"
        );
    }

    #[test]
    fn deindex_removes_epoch_slot() {
        let mut c = one_finished_segment();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));
        // DeleteSegmentStarted deindexes the epoch slot (but the metadata is
        // still present until DeleteSegmentFinished). highest_offset_for_epoch
        // reads the epoch index directly, so it must now miss.
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn offset_lookup_respects_epoch() {
        let mut c = RemoteLogMetadataCache::default();
        // One segment spanning two epochs: epoch 0 owns [0,49], epoch 1 owns [50,99].
        c.add(seg(CacheSegmentSetup {
            epochs: &[(LeaderEpoch(0), Offset(0)), (LeaderEpoch(1), Offset(50))],
            ..Default::default()
        }))
        .unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        // A second segment, epoch 1 only.
        c.add(seg(CacheSegmentSetup {
            epochs: &[(LeaderEpoch(1), Offset(100))],
            ..CacheSegmentSetup::following_segment()
        }))
        .unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();

        for (epoch, offset, want) in [
            // Exact subrange boundary: epoch 0 owns through 49, epoch 1 starts
            // at 50 even though both occur in the same remote segment.
            (LeaderEpoch(0), 10, Some(Uuid::from_u128(10))),
            (LeaderEpoch(0), 49, Some(Uuid::from_u128(10))),
            (LeaderEpoch(0), 50, None),
            (LeaderEpoch(1), 49, None),
            (LeaderEpoch(1), 50, Some(Uuid::from_u128(10))),
            // Epoch 0 has no segment at 150.
            (LeaderEpoch(0), 150, None),
            // Epoch 1 floor lookup picks the right segment.
            (LeaderEpoch(1), 60, Some(Uuid::from_u128(10))),
            (LeaderEpoch(1), 150, Some(Uuid::from_u128(11))),
        ] {
            check!(
                c.segment_for(epoch, offset)
                    .map(|s| s.remote_log_segment_id().id)
                    == want,
                "epoch={epoch:?} offset={offset}"
            );
        }
    }

    #[test]
    fn highest_offset_for_epoch_is_max_end() {
        let c = two_finished_segments();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(199));
        assert!(c.highest_offset_for_epoch(LeaderEpoch(7)) == None);
    }

    #[test]
    fn delete_started_hides_segment_delete_finished_drops_it() {
        let mut c = one_finished_segment();
        assert!(c.segment_for(LeaderEpoch(0), 50).is_some());

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "delete-started hides it"
        );
        assert!(
            c.list().len() == 1,
            "still tracked while delete in progress"
        );

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .unwrap();
        assert!(c.list().is_empty(), "delete-finished drops it entirely");
    }

    #[test]
    fn update_unknown_segment_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let err = c.update(&finish(Uuid::from_u128(404))).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn exact_update_and_absent_tombstone_retries_are_idempotent() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        let finished = finish(Uuid::from_u128(10));
        c.update(&finished).unwrap();
        c.update(&finished).expect("exact retry is a no-op");
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));

        let conflicting = transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::CopySegmentFinished,
        );
        let error = c.update(&conflicting).unwrap_err();
        check!(matches!(
            error,
            RemoteStorageError::InvalidSegmentTransition { .. }
        ));

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .unwrap();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .expect("absent delete-finished tombstone is a no-op");
        check!(c.list().is_empty());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn stale_update_cannot_reindex_a_deleting_segment() {
        let mut c = one_finished_segment();
        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(c.update(&finish(Uuid::from_u128(10))).is_err());
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn add_with_wrong_state_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let mut s = seg(CacheSegmentSetup::default());
        s = s
            .with_update(&finish(Uuid::from_u128(10)))
            .expect("force to finished for the test");
        let err = c.add(s).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn duplicate_add_errors() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        let err = c.add(seg(CacheSegmentSetup::default())).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn dump_then_seed_rebuilds_epoch_index() {
        let mut c = two_finished_segments();
        c.update(&transition(
            Uuid::from_u128(11),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        c.set_delete_state(RemotePartitionDeleteState::DeletePartitionMarked);

        let segments = c.dump_segments();
        let delete_state = c.delete_state();

        let mut seeded = RemoteLogMetadataCache::default();
        seeded.seed(segments, delete_state);

        // Finished seg 10 is queryable; delete-started seg 11 is hidden
        // but still listed; delete_state survives.
        check!(
            seeded
                .segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(10)
        );
        check!(seeded.segment_for(LeaderEpoch(0), 150).is_none());
        check!(seeded.list().len() == 2);
        check!(seeded.delete_state() == Some(RemotePartitionDeleteState::DeletePartitionMarked));
    }

    #[test]
    fn repeated_seed_and_tombstone_duplicates_do_not_resurrect() {
        let started = seg(CacheSegmentSetup::default());
        let finished = started.with_update(&finish(Uuid::from_u128(10))).unwrap();
        let deleting = finished
            .with_update(&transition(
                Uuid::from_u128(10),
                RemoteLogSegmentState::DeleteSegmentStarted,
            ))
            .unwrap();
        let deleted = deleting
            .with_update(&transition(
                Uuid::from_u128(10),
                RemoteLogSegmentState::DeleteSegmentFinished,
            ))
            .unwrap();
        let dump = vec![finished, deleted, started];

        let mut c = RemoteLogMetadataCache::default();
        c.seed(dump.clone(), None);
        check!(c.list().is_empty());
        check!(c.segment_for(LeaderEpoch(0), 50).is_none());
        c.seed(dump, None);
        check!(c.list().is_empty());
        check!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn index_collision_is_deterministic_and_reveals_the_remaining_finished_segment() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup {
            id: Uuid::from_u128(11),
            ..Default::default()
        }))
        .unwrap();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        c.update(&finish(Uuid::from_u128(11))).unwrap();
        c.update(&finish(Uuid::from_u128(10))).unwrap();
        check!(
            c.segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(10)
        );

        c.update(&transition(
            Uuid::from_u128(10),
            RemoteLogSegmentState::DeleteSegmentStarted,
        ))
        .unwrap();
        check!(
            c.segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(11)
        );
    }

    #[test]
    fn list_is_ordered_by_start_offset() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(CacheSegmentSetup::following_segment())).unwrap();
        c.add(seg(CacheSegmentSetup::default())).unwrap();
        let listed = c.list();
        assert!(listed[0].start_offset() == 0);
        assert!(listed[1].start_offset() == 100);
    }
}
