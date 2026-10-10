//! The offset bounds that the remote tier can answer for `ListOffsets`.
//!
//! Both lookups here read only segment metadata, never segment data. They
//! scan the finished segments of a partition for the lowest start offset and
//! for the highest end offset together with the leader epoch that owns it.
//! Copies still in progress stay invisible, so a partially copied segment
//! never widens the range a client sees.

use krabka_remote_storage::{
    LogOffset, RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError,
    TopicIdPartition,
};
use krabka_verified::{
    tiered_earliest_finished_index, tiered_latest_finished_index, tiered_owning_epoch_index,
};

use super::{RemoteReader, TieredOffset};

impl RemoteReader {
    /// Returns the lowest `start_offset` across the finished segments for
    /// `tp`, or `None` when no finished segment exists. It drives
    /// `ListOffsets` EARLIEST below `local_log_start_offset()`.
    pub(crate) async fn earliest_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<LogOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        let (starts, ends, finished) = tiered_segment_inputs(&listed);
        Ok(tiered_earliest_finished_index(&starts, &ends, &finished)
            .map(|index| listed[index].start_offset()))
    }

    /// Returns the highest offset held by a finished remote segment and the
    /// leader epoch that owns that offset. In-progress copies are invisible.
    pub(crate) async fn latest_tiered_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<TieredOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        let (starts, ends, finished) = tiered_segment_inputs(&listed);
        let Some(index) = tiered_latest_finished_index(&starts, &ends, &finished) else {
            return Ok(None);
        };
        let metadata = &listed[index];
        let offset = metadata.end_offset();
        let epochs = metadata
            .segment_leader_epochs()
            .iter()
            .map(|(epoch, start)| (*epoch, *start))
            .collect::<Vec<_>>();
        let epoch_values = epochs.iter().map(|(epoch, _)| epoch.0).collect::<Vec<_>>();
        let epoch_starts = epochs.iter().map(|(_, start)| *start).collect::<Vec<_>>();
        let Some(epoch_index) = tiered_owning_epoch_index(
            &epoch_values,
            &epoch_starts,
            metadata.start_offset(),
            offset,
        ) else {
            return Ok(None);
        };
        Ok(Some(TieredOffset {
            offset,
            leader_epoch: epochs[epoch_index].0,
        }))
    }
}

fn tiered_segment_inputs(listed: &[RemoteLogSegmentMetadata]) -> (Vec<i64>, Vec<i64>, Vec<bool>) {
    let starts = listed
        .iter()
        .map(RemoteLogSegmentMetadata::start_offset)
        .collect();
    let ends = listed
        .iter()
        .map(RemoteLogSegmentMetadata::end_offset)
        .collect();
    let finished = listed
        .iter()
        .map(|metadata| metadata.state() == RemoteLogSegmentState::CopySegmentFinished)
        .collect();
    (starts, ends, finished)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_ids::LeaderEpoch;
    use krabka_remote_storage::{RemoteLogMetadataManager, RemoteLogSegmentMetadataUpdate};
    use uuid::Uuid;

    use super::*;
    use crate::remote_reader::test_support::tp;

    fn reader_with_metadata() -> (RemoteReader, tempfile::TempDir) {
        let remote_dir = tempfile::tempdir().unwrap();
        let (rsm, rlmm) =
            crate::remote_log_manager::test_support::local_backends(remote_dir.path());
        (RemoteReader::new(rsm, rlmm), remote_dir)
    }

    use std::collections::BTreeMap;

    use krabka_ids::Offset;

    #[derive(Clone, Copy, Default)]
    enum SegmentPublication {
        #[default]
        Finished,
        Started,
    }

    #[derive(krabka_macros::FieldDefaults)]
    struct TieredSegmentSetup {
        #[default(Offset(0))]
        start: Offset,
        #[default(Offset(9))]
        end: Offset,
        #[default(maplit::btreemap! {LeaderEpoch(0) => Offset(0)})]
        epochs: BTreeMap<LeaderEpoch, Offset>,
        publication: SegmentPublication,
    }

    fn add_segment(reader: &RemoteReader, setup: TieredSegmentSetup) {
        let TieredSegmentSetup {
            start,
            end,
            epochs,
            publication,
        } = setup;
        let id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        reader
            .rlmm
            .add_remote_log_segment_metadata(
                RemoteLogSegmentMetadata::new(
                    id.clone(),
                    start.0,
                    end.0,
                    0,
                    1,
                    0,
                    krabka_remote_storage::RemoteLogSegmentDetails::new(
                        1,
                        RemoteLogSegmentState::CopySegmentStarted,
                        epochs
                            .into_iter()
                            .map(|(epoch, offset)| (epoch, offset.0))
                            .collect(),
                    ),
                )
                .unwrap(),
            )
            .unwrap();
        if matches!(publication, SegmentPublication::Finished) {
            reader
                .rlmm
                .update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
                    remote_log_segment_id: id,
                    event_timestamp_ms: 1,
                    custom_metadata: None,
                    state: RemoteLogSegmentState::CopySegmentFinished,
                    broker_id: 1,
                })
                .unwrap();
        }
    }

    #[tokio::test]
    async fn tiered_frontiers_ignore_invalid_and_unfinished_candidates() {
        let (reader, _remote_dir) = reader_with_metadata();
        add_segment(
            &reader,
            TieredSegmentSetup {
                start: Offset(-10),
                end: Offset(-1),
                epochs: maplit::btreemap! {LeaderEpoch(0) => Offset(-10)},
                ..Default::default()
            },
        );
        add_segment(
            &reader,
            TieredSegmentSetup {
                start: Offset(20),
                end: Offset(39),
                epochs: maplit::btreemap! {
                    LeaderEpoch(0) => Offset(20),
                    LeaderEpoch(2) => Offset(30),
                    LeaderEpoch(4) => Offset(35),
                },
                ..Default::default()
            },
        );
        add_segment(
            &reader,
            TieredSegmentSetup {
                end: Offset(19),
                ..Default::default()
            },
        );
        add_segment(
            &reader,
            TieredSegmentSetup {
                start: Offset(40),
                end: Offset(99),
                epochs: maplit::btreemap! {LeaderEpoch(8) => Offset(40)},
                publication: SegmentPublication::Started,
            },
        );

        assert!(reader.earliest_offset(&tp()).await.unwrap() == Some(0));
        let expected = Some(TieredOffset {
            offset: 39,
            leader_epoch: LeaderEpoch(4),
        });
        assert!(reader.latest_tiered_offset(&tp()).await.unwrap() == expected);
        assert!(
            reader.latest_tiered_offset(&tp()).await.unwrap() == expected,
            "retry is deterministic"
        );
    }

    #[tokio::test]
    async fn latest_tiered_offset_rejects_a_segment_without_an_owning_epoch() {
        let (reader, _remote_dir) = reader_with_metadata();
        add_segment(&reader, TieredSegmentSetup::default());
        add_segment(
            &reader,
            TieredSegmentSetup {
                start: Offset(10),
                end: Offset(19),
                epochs: maplit::btreemap! {LeaderEpoch(7) => Offset(20)},
                ..Default::default()
            },
        );
        assert!(reader.latest_tiered_offset(&tp()).await.unwrap() == None);
    }

    #[tokio::test]
    async fn tiered_frontiers_accept_the_maximum_offset_without_arithmetic() {
        let (reader, _remote_dir) = reader_with_metadata();
        add_segment(
            &reader,
            TieredSegmentSetup {
                start: Offset(i64::MAX),
                end: Offset(i64::MAX),
                epochs: maplit::btreemap! {LeaderEpoch(i32::MAX) => Offset(i64::MAX)},
                ..Default::default()
            },
        );
        assert!(reader.earliest_offset(&tp()).await.unwrap() == Some(i64::MAX));
        assert!(
            reader.latest_tiered_offset(&tp()).await.unwrap()
                == Some(TieredOffset {
                    offset: i64::MAX,
                    leader_epoch: LeaderEpoch(i32::MAX),
                })
        );
    }

    #[tokio::test]
    async fn earliest_offset_returns_lowest_finished_start() {
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log, exports
        );
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        let got = reader.earliest_offset(&tp()).await.unwrap();
        assert!(got == Some(expected));
    }

    #[tokio::test]
    async fn earliest_offset_returns_none_when_no_finished_segments() {
        let remote_dir = tempfile::tempdir().unwrap();
        let (rsm, rlmm) =
            crate::remote_log_manager::test_support::local_backends(remote_dir.path());
        let reader = RemoteReader::new(rsm, rlmm);
        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
    }

    #[tokio::test]
    async fn latest_tiered_offset_uses_highest_finished_segment_and_its_epoch() {
        crate::remote_reader::test_support::populated_reader_fixture!(
            log_dir, remote_dir, reader, log
        );
        let expected = log
            .tierable_segments()
            .iter()
            .map(|segment| segment.last_offset.0)
            .max()
            .unwrap();

        let started_id = krabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        reader
            .rlmm
            .add_remote_log_segment_metadata(
                RemoteLogSegmentMetadata::new(
                    started_id,
                    expected + 1,
                    expected + 100,
                    0,
                    1,
                    0,
                    krabka_remote_storage::RemoteLogSegmentDetails::new(
                        1,
                        RemoteLogSegmentState::CopySegmentStarted,
                        maplit::btreemap! {LeaderEpoch(7) => expected + 1},
                    ),
                )
                .unwrap(),
            )
            .unwrap();

        let got = reader
            .latest_tiered_offset(&tp())
            .await
            .unwrap()
            .expect("finished segments exist");
        assert!(
            got == TieredOffset {
                offset: expected,
                leader_epoch: LeaderEpoch(0),
            }
        );
    }

    #[tokio::test]
    async fn earliest_offset_propagates_not_ready() {
        let (_remote_dir, reader) = crate::remote_reader::test_support::not_ready_reader();
        let err = reader.earliest_offset(&tp()).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
        let err = reader.latest_tiered_offset(&tp()).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
    }

    // ── I1: the list-based read paths (`earliest_offset` /
    // ── `offset_for_timestamp` → `list_remote_log_segments`) must observe
    // ── `NotReady` from the REAL `TopicBasedRemoteLogMetadataManager` while
    // ── an assigned metadata partition is still catching up, and an empty
    // ── result for a partition this broker does not own (Unassigned). The
    // ── `NotReadyRlmm` stub proves propagation through the reader; this test
    // ── proves the manager's list-path gate actually produces those states.

    /// Drives `reconcile_assignment` and blocks, off the reactor, until the
    /// list path stops returning `NotReady` for `tp`. At that point the
    /// partition is caught up to its assignment-time HWM.
    async fn assign_and_wait_ready(
        m: &Arc<krabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager>,
        mp: i32,
        tp: &TopicIdPartition,
    ) {
        m.reconcile_assignment(&[mp]).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            // `list_remote_log_segments` is the method the list path uses.
            match m.list_remote_log_segments(tp) {
                Ok(_) => return,
                Err(RemoteStorageError::NotReady { .. }) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "list path never became ready"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("unexpected list error: {e:?}"),
            }
        }
    }

    krabka_macros::remote_started_segment!(synthetic_started_segment, krabka_remote_storage);

    #[tokio::test(flavor = "multi_thread")]
    async fn list_path_observes_not_ready_and_unassigned_from_real_manager() {
        use krabka_remote_storage_topic::{
            InProcessMetadataEventLog, MetadataEventLog, TopicBasedRemoteLogMetadataManager,
            metadata_partition_for,
        };

        let topic_id = Uuid::from_u128(0xABCD);
        let owned = TopicIdPartition::new(topic_id, "orders", 0);
        let not_owned = TopicIdPartition::new(topic_id, "orders", 1);

        // Wide metadata topic so the two user-partitions land in distinct
        // metadata partitions.
        let n = 16;
        let mp_owned = metadata_partition_for(&owned, n);
        let mp_other = metadata_partition_for(&not_owned, n);
        assert!(mp_owned != mp_other, "test needs distinct metadata buckets");

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

        let writer_snap_dir = tempfile::tempdir().unwrap();
        let mgr_snap_dir = tempfile::tempdir().unwrap();

        // Pre-seed a finished segment for the owned partition via a transient
        // all-consuming writer.
        {
            let writer = TopicBasedRemoteLogMetadataManager::start(
                log.clone(),
                tokio::runtime::Handle::current(),
                writer_snap_dir.path().to_path_buf(),
                std::time::Duration::from_hours(1),
            )
            .unwrap();
            writer
                .reconcile_assignment(&(0..n).collect::<Vec<_>>())
                .await;
            let id = krabka_remote_storage::RemoteLogSegmentId::new(owned.clone(), Uuid::new_v4());
            let md = synthetic_started_segment(id.clone(), 0, 99, 100);
            let w2 = writer.clone();
            let md2 = md.clone();
            tokio::task::spawn_blocking(move || {
                w2.add_remote_log_segment_metadata(md2).unwrap();
            })
            .await
            .unwrap();
            let w2 = writer.clone();
            tokio::task::spawn_blocking(move || {
                w2.update_remote_log_segment_metadata(
                    krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                        remote_log_segment_id: id,
                        event_timestamp_ms: 100,
                        custom_metadata: None,
                        state: RemoteLogSegmentState::CopySegmentFinished,
                        broker_id: 1,
                    },
                )
                .unwrap();
            })
            .await
            .unwrap();
            writer.shutdown();
        }

        // A fresh manager that consumes NOTHING until assigned.
        let m = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            tokio::runtime::Handle::current(),
            mgr_snap_dir.path().to_path_buf(),
            std::time::Duration::from_hours(1),
        )
        .unwrap();

        let (_remote_dir, reader) =
            crate::remote_reader::test_support::reader_with_metadata(|| m.clone());

        // Unowned partition (never assigned) → the list path treats it as a
        // genuine miss: empty, not an error.
        assert!(
            reader.earliest_offset(&not_owned).await.unwrap() == None,
            "unassigned partition is an empty list-path result, not NotReady"
        );

        // Assign the owned partition. Before catch-up the list path surfaces
        // NotReady through the reader. Poll until ready; observe at least the
        // ready (Some) terminal state.
        assign_and_wait_ready(&m, mp_owned, &owned).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == Some(0),
            "owned + caught up → real earliest from the remote tier"
        );

        // Remove the owned partition: the list path now returns empty (the
        // broker no longer owns it), NOT a stale segment.
        m.reconcile_assignment(&[]).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == None,
            "removed partition's list path returns empty, not stale segments"
        );

        m.shutdown();
    }
}
