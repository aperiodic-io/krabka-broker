//! Unit tests for reconciliation, driven through the scan the way an operator
//! reaches it: a snapshot that agrees, a deletion still in flight that is
//! dropped in silence, and the four disagreements that stop a restore.

use {
    super::*,
    crate::{
        args::RestoreArgs,
        backend::open_archive,
        discover::{
            ArchiveInventory, PartitionInventory, inventory,
            test_support::{
                PartitionKey, SegmentSetup, SingleSegmentArchive, args_from, inventory_error,
                single_segment_archive, snapshot_segment, write_full_segment, write_snapshot,
                write_topic_segment,
            },
        },
    },
    assert2::check,
    krabka_ids::Offset,
    krabka_remote_storage::{RlmmCacheDump, TopicIdPartition},
};

/// One segment as the snapshot records it.
#[derive(Clone, Copy)]
struct SnapshotSegmentEntry {
    segment_id: Uuid,
    base_offset: Offset,
    state: RemoteLogSegmentState,
}

/// Partition 0 of `topic` as the snapshot records it, one entry per segment.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct SnapshotPartitionSetup<'a> {
    #[default("orders")]
    topic: &'a str,
    #[default(Uuid::from_u128(1))]
    topic_id: Uuid,
    segments: &'a [SnapshotSegmentEntry],
}

#[bon::builder]
fn partition_dump(
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = Uuid :: from_u128 (1))] topic_id: Uuid,
    #[builder(default)] segments: &'_ [SnapshotSegmentEntry],
) -> PartitionDump {
    let setup = SnapshotPartitionSetup {
        topic,
        topic_id,
        segments,
    };
    let SnapshotPartitionSetup {
        topic,
        topic_id,
        segments,
    } = setup;
    PartitionDump {
        topic_id_partition: TopicIdPartition::new(topic_id, topic, 0),
        segments: segments
            .iter()
            .map(
                |&SnapshotSegmentEntry {
                     segment_id,
                     base_offset,
                     state,
                 }| {
                    snapshot_segment()
                        .segment(SegmentSetup {
                            partition: PartitionKey {
                                topic,
                                topic_id,
                                ..Default::default()
                            },
                            base_offset,
                            segment_id,
                        })
                        .state(state)
                        .call()
                },
            )
            .collect(),
        delete_state: None,
    }
}

/// Write a snapshot at `path` that holds the one [`partition_dump`].
#[bon::builder]
fn snapshot_of(
    path: &std::path::Path,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = Uuid :: from_u128 (1))] topic_id: Uuid,
    #[builder(default)] segments: &'_ [SnapshotSegmentEntry],
) {
    let setup = SnapshotPartitionSetup {
        topic,
        topic_id,
        segments,
    };
    write_snapshot(path, RlmmCacheDump {
        partitions: vec![
            partition_dump()
                .topic(setup.topic)
                .topic_id(setup.topic_id)
                .segments(setup.segments)
                .call(),
        ],
    });
}

/// A temporary directory and the path of a snapshot file inside it.
struct SnapshotLocation {
    dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

fn snapshot_directory() -> SnapshotLocation {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("snapshot");
    SnapshotLocation { dir, path }
}

/// A fresh snapshot of `topic_id`'s one partition holding `segments`.
fn snapshot_with_segments(topic_id: Uuid, segments: &[SnapshotSegmentEntry]) -> SnapshotLocation {
    let location = snapshot_directory();
    snapshot_of()
        .path(&location.path)
        .topic_id(topic_id)
        .segments(segments)
        .call();
    location
}

/// A two-segment archive on disk, with the ids of its topic and segments.
struct TwoSegmentArchive {
    dir: tempfile::TempDir,
    topic_id: Uuid,
    first: Uuid,
    second: Uuid,
}

fn two_segment_archive() -> TwoSegmentArchive {
    let archive = tempfile::tempdir().expect("temp dir");
    let topic = Uuid::from_u128(1);
    let first = Uuid::from_u128(10);
    let second = Uuid::from_u128(11);
    write_full_segment()
        .root(archive.path())
        .partition(PartitionKey {
            topic_id: topic,
            ..Default::default()
        })
        .segment_id(first)
        .call();
    write_full_segment()
        .root(archive.path())
        .partition(PartitionKey {
            topic_id: topic,
            ..Default::default()
        })
        .base_offset(Offset(100))
        .segment_id(second)
        .call();
    TwoSegmentArchive {
        dir: archive,
        topic_id: topic,
        first,
        second,
    }
}

/// A two-segment archive with a snapshot of it written beside it.
struct TwoSegmentSnapshot {
    archive: tempfile::TempDir,
    snapshot_dir: tempfile::TempDir,
    snapshot: std::path::PathBuf,
    first: Uuid,
}

fn two_segment_snapshot(second_state: RemoteLogSegmentState) -> TwoSegmentSnapshot {
    let TwoSegmentArchive {
        dir: archive,
        topic_id,
        first,
        second,
    } = two_segment_archive();
    let snapshot_dir = tempfile::tempdir().expect("temp dir");
    let snapshot = snapshot_dir.path().join("snapshot");
    snapshot_of()
        .path(&snapshot)
        .topic_id(topic_id)
        .segments(&[
            SnapshotSegmentEntry {
                segment_id: first,
                base_offset: Offset(0),
                state: RemoteLogSegmentState::CopySegmentFinished,
            },
            SnapshotSegmentEntry {
                segment_id: second,
                base_offset: Offset(100),
                state: second_state,
            },
        ])
        .call();
    TwoSegmentSnapshot {
        archive,
        snapshot_dir,
        snapshot,
        first,
    }
}

/// A one-segment archive whose snapshot sits inside it, with the arguments that authenticate the restore.
struct AuthenticatedSnapshot {
    archive: tempfile::TempDir,
    snapshot: std::path::PathBuf,
    args: RestoreArgs,
}

fn authenticated_snapshot(state: RemoteLogSegmentState) -> AuthenticatedSnapshot {
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        segment_id,
    } = single_segment_archive(Offset(0));
    let snapshot = archive.path().join("snapshot");
    snapshot_of()
        .path(&snapshot)
        .topic_id(topic_id)
        .segments(&[SnapshotSegmentEntry {
            segment_id,
            base_offset: Offset(0),
            state,
        }])
        .call();
    let mut args = args_from(archive.path(), &[
        "--rlmm-snapshot",
        &snapshot.display().to_string(),
    ]);
    args.archive.worm_key_id.push("trusted".into());
    args.archive.worm_public_key.push("unused.pub".into());
    AuthenticatedSnapshot {
        archive,
        snapshot,
        args,
    }
}

async fn snapshot_inventory(
    archive: &std::path::Path,
    snapshot: &std::path::Path,
) -> Result<ArchiveInventory, RestoreError> {
    let args = args_from(archive, &[
        "--rlmm-snapshot",
        &snapshot.display().to_string(),
    ]);
    let store = open_archive(&args).expect("store");
    inventory(&store, &args).await
}

async fn expect_metadata_disagreement(archive: &std::path::Path, snapshot: &std::path::Path) {
    let error = snapshot_inventory(archive, snapshot).await.unwrap_err();
    check!(matches!(error, RestoreError::MetadataDisagreement { .. }));
}

async fn sole_partition_inventory(
    archive: &std::path::Path,
    snapshot: &std::path::Path,
) -> PartitionInventory {
    let mut result = snapshot_inventory(archive, snapshot)
        .await
        .expect("inventory");
    check!(result.partitions.len() == 1);
    result.partitions.remove(0)
}

fn check_orders_disagreement(error: RestoreError) {
    check!(
        matches!(error, RestoreError::MetadataDisagreement { topic, partition, .. } if topic == "orders" && partition == 0)
    );
}

#[tokio::test]
async fn a_snapshot_that_agrees_keeps_every_live_segment() {
    let TwoSegmentSnapshot {
        archive,
        snapshot_dir: _snapshot_dir,
        snapshot: snap_path,
        ..
    } = two_segment_snapshot(RemoteLogSegmentState::CopySegmentFinished);

    let result = sole_partition_inventory(archive.path(), &snap_path).await;
    check!(result.segments.len() == 2);
}

#[tokio::test]
async fn a_delete_started_segment_is_excluded_from_the_inventory_without_an_error() {
    // Deletion is in flight: leftover remote bytes are expected and must not
    // make inventory fail while the remote tier catches up.
    let TwoSegmentSnapshot {
        archive,
        snapshot_dir: _snapshot_dir,
        snapshot: snap_path,
        first: seg_a,
    } = two_segment_snapshot(RemoteLogSegmentState::DeleteSegmentStarted);

    let result = sole_partition_inventory(archive.path(), &snap_path).await;
    check!(result.segments.len() == 1);
    check!(result.segments[0].segment_id == seg_a);
}

#[tokio::test]
async fn authenticated_inventory_does_not_let_unsigned_rlmm_remove_a_segment() {
    let AuthenticatedSnapshot {
        archive: _archive,
        args,
        ..
    } = authenticated_snapshot(RemoteLogSegmentState::DeleteSegmentStarted);
    let store = open_archive(&args).expect("store");

    let result = inventory(&store, &args).await.expect("inventory");
    check!(result.partitions[0].segments.len() == 1);
}

#[tokio::test]
async fn authenticated_rlmm_excludes_objects_retained_after_completed_deletion() {
    let AuthenticatedSnapshot {
        archive: _archive,
        snapshot: snap_path,
        args,
    } = authenticated_snapshot(RemoteLogSegmentState::DeleteSegmentFinished);
    let store = open_archive(&args).expect("store");
    let mut result = inventory(&store, &args).await.expect("inventory");

    let authenticated_snapshot = load_snapshot(&snap_path).expect("snapshot");
    std::fs::write(&snap_path, b"changed after authentication").expect("change source path");
    reconcile_authenticated_snapshot(&mut result.partitions, &args, &authenticated_snapshot)
        .expect("authenticated reconciliation");
    check!(result.partitions.is_empty());
}

#[tokio::test]
async fn a_segment_the_snapshot_does_not_mention_is_a_disagreement() {
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        ..
    } = single_segment_archive(Offset(0));

    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_directory();
    // The snapshot knows nothing about this partition's segment at all.
    snapshot_of().path(&snap_path).topic_id(topic_id).call();

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check_orders_disagreement(err);
}

#[tokio::test]
async fn a_delete_finished_segment_with_bytes_still_present_is_a_disagreement() {
    // `DeleteSegmentFinished` says the bytes should be gone. Bytes still
    // being in the archive is a real inconsistency, unlike
    // `DeleteSegmentStarted`, where a deletion still in flight leaving
    // bytes behind is routine and gets dropped silently instead.
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        segment_id: seg,
    } = single_segment_archive(Offset(0));

    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_with_segments(topic_id, &[SnapshotSegmentEntry {
        segment_id: seg,
        base_offset: Offset(0),
        state: RemoteLogSegmentState::DeleteSegmentFinished,
    }]);

    expect_metadata_disagreement(archive.path(), &snap_path).await;
}

#[tokio::test]
async fn a_live_partition_missing_from_the_scan_entirely_is_a_disagreement() {
    let archive = tempfile::tempdir().expect("temp dir");
    let payments_id = Uuid::from_u128(2);
    let payments_seg = Uuid::from_u128(20);
    write_topic_segment()
        .root(archive.path())
        .topic("payments")
        .topic_id(payments_id)
        .segment_id(payments_seg)
        .call();

    let orders_id = Uuid::from_u128(1);
    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_directory();
    write_snapshot(&snap_path, RlmmCacheDump {
        partitions: vec![
            // Matches the scan exactly: no disagreement from this one.
            partition_dump()
                .topic("payments")
                .topic_id(payments_id)
                .segments(&[SnapshotSegmentEntry {
                    segment_id: payments_seg,
                    base_offset: Offset(0),
                    state: RemoteLogSegmentState::CopySegmentFinished,
                }])
                .call(),
            // Live in the snapshot, but the scan never found this
            // partition at all.
            partition_dump()
                .topic_id(orders_id)
                .segments(&[SnapshotSegmentEntry {
                    segment_id: Uuid::from_u128(10),
                    base_offset: Offset(0),
                    state: RemoteLogSegmentState::CopySegmentFinished,
                }])
                .call(),
        ],
    });

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check_orders_disagreement(err);
}

#[tokio::test]
async fn a_missing_snapshot_file_is_reported_as_io_not_found() {
    let SingleSegmentArchive { dir: archive, .. } = single_segment_archive(Offset(0));

    let missing = archive.path().join("does-not-exist-snapshot");
    let args = args_from(archive.path(), &[
        "--rlmm-snapshot",
        &missing.display().to_string(),
    ]);
    let err = inventory_error(&args).await;
    check!(matches!(
        err,
        RestoreError::Io(io_error) if io_error.kind() == std::io::ErrorKind::NotFound
    ));
}

#[tokio::test]
async fn duplicate_segment_keys_in_the_snapshot_are_a_disagreement() {
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        segment_id,
    } = single_segment_archive(Offset(0));

    let entry = SnapshotSegmentEntry {
        segment_id,
        base_offset: Offset(0),
        state: RemoteLogSegmentState::CopySegmentFinished,
    };
    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_with_segments(topic_id, &[entry, entry]);

    expect_metadata_disagreement(archive.path(), &snap_path).await;
}

#[tokio::test]
async fn duplicate_partition_keys_in_the_snapshot_are_a_disagreement() {
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        segment_id,
    } = single_segment_archive(Offset(0));

    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_directory();
    let duplicate = partition_dump()
        .topic_id(topic_id)
        .segments(&[SnapshotSegmentEntry {
            segment_id,
            base_offset: Offset(0),
            state: RemoteLogSegmentState::CopySegmentFinished,
        }])
        .call();
    write_snapshot(&snap_path, RlmmCacheDump {
        partitions: vec![duplicate.clone(), duplicate],
    });

    expect_metadata_disagreement(archive.path(), &snap_path).await;
}

#[tokio::test]
async fn maximum_offset_reconciliation_is_stable_across_retry() {
    let SingleSegmentArchive {
        dir: archive,
        topic_id,
        segment_id,
    } = single_segment_archive(Offset(i64::MAX));

    let SnapshotLocation {
        dir: _snap_dir,
        path: snap_path,
    } = snapshot_directory();
    snapshot_of()
        .path(&snap_path)
        .topic_id(topic_id)
        .segments(&[SnapshotSegmentEntry {
            segment_id,
            base_offset: Offset(i64::MAX),
            state: RemoteLogSegmentState::CopySegmentFinished,
        }])
        .call();

    let args = args_from(archive.path(), &[
        "--rlmm-snapshot",
        &snap_path.display().to_string(),
    ]);
    let store = open_archive(&args).expect("store");
    let first = inventory(&store, &args).await.expect("first inventory");
    let retry = inventory(&store, &args).await.expect("retry inventory");

    check!(first == retry);
    check!(first.partitions[0].segments[0].base_offset.get() == i64::MAX);
}

#[tokio::test]
async fn a_corrupt_snapshot_file_is_reported_as_io_invalid_data() {
    let archive = tempfile::tempdir().expect("temp dir");
    let snap_path = archive.path().join("snapshot");
    std::fs::write(&snap_path, b"not an RLMM snapshot").expect("write corrupt snapshot");

    let err = snapshot_inventory(archive.path(), &snap_path)
        .await
        .unwrap_err();
    check!(matches!(
        err,
        RestoreError::Io(io_error) if io_error.kind() == std::io::ErrorKind::InvalidData
    ));
}
