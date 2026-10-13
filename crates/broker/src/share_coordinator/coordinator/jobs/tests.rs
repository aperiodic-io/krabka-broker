//! Unit tests for the background work: the tombstones of a deleted topic,
//! the cold-partition snapshot, the prune, and the loop that runs them.

use {
    super::*,
    crate::share_coordinator::{
        bootstrap,
        config::ShareCoordinatorConfig,
        coordinator::test_support::{
            Logged, NOW_MS, configured_coordinator, image_with_topics, lead_all, logged_records,
            set_clock,
        },
        persistence::ShareSnapshotValue,
    },
    assert2::{assert, check},
    krabka_log::Offset,
    krabka_metadata::{FeatureLevelRecord, MetadataRecord},
    std::time::Duration,
    tempfile::tempdir,
};

const T1: uuid::Uuid = uuid::Uuid::from_bytes([61; 16]);
const T2: uuid::Uuid = uuid::Uuid::from_bytes([62; 16]);

/// A coordinator with key A (`ga`, T1, 0) and key B (`gb`, T2, 0), both
/// initialized at [`NOW_MS`].
async fn two_keys(
    dir: &std::path::Path,
    config: ShareCoordinatorConfig,
) -> (Arc<ShareCoordinator>, Arc<qubit_clock::ManualWallClock>) {
    let (coord, _reg, clock) = configured_coordinator(dir, config);
    lead_all(&coord).await;
    let image = image_with_topics(&[(T1, 1), (T2, 1)]);
    coord
        .initialize(&image, "ga", T1, 0, 1, Offset(0))
        .await
        .unwrap();
    coord
        .initialize(&image, "gb", T2, 0, 1, Offset(0))
        .await
        .unwrap();
    (Arc::new(coord), clock)
}

fn records_of(
    coord: &ShareCoordinator,
    group: &str,
    topic_id: uuid::Uuid,
) -> Vec<(Offset, Logged)> {
    logged_records(coord, coord.state_partition_for(group, &topic_id, 0))
}

fn snapshot_of_b(snapshot_epoch: i32, write_timestamp: i64) -> Logged {
    Logged::Snapshot(ShareSnapshotValue {
        snapshot_epoch,
        state_epoch: 1,
        leader_epoch: 0,
        start_offset: Offset(0),
        delivery_complete_count: 0,
        create_timestamp: NOW_MS,
        write_timestamp,
        state_batches: vec![],
    })
}

/// A deleted topic loses the state of its keys: a tombstone is appended and
/// the key leaves memory. A key of another topic stays.
#[tokio::test]
async fn deleted_topic_keys_are_tombstoned() {
    let dir = tempdir().unwrap();
    let (coord, _clock) = two_keys(dir.path(), ShareCoordinatorConfig::default()).await;
    let b_before = records_of(&coord, "gb", T2);

    let written = coord.cleanup_deleted_topics(&HashSet::from([T1])).await;

    check!(written == 1);
    let a_records: Vec<Logged> = records_of(&coord, "ga", T1)
        .into_iter()
        .map(|(_, logged)| logged)
        .filter(|logged| *logged == Logged::Tombstone)
        .collect();
    check!(a_records == vec![Logged::Tombstone]);
    check!(coord.state_for_test("ga", T1, 0).await.is_none());
    check!(coord.state_for_test("gb", T2, 0).await.is_some());
    if coord.state_partition_for("ga", &T1, 0) != coord.state_partition_for("gb", &T2, 0) {
        check!(records_of(&coord, "gb", T2) == b_before);
    }
}

/// The cold-partition snapshot, then the prune, as the time since the latest
/// snapshot of key B grows. Key A is gone before the table starts.
#[tokio::test]
async fn cold_snapshot_then_prune_frees_the_log_prefix() {
    let dir = tempdir().unwrap();
    let (coord, clock) = two_keys(dir.path(), ShareCoordinatorConfig::default()).await;
    coord.cleanup_deleted_topics(&HashSet::from([T1])).await;
    let interval_ms = 300_000;

    // (ms after NOW_MS, expected cold snapshots, expected new record of B)
    let rows = [
        (interval_ms - 1, 0, None),
        (interval_ms, 1, Some(snapshot_of_b(1, NOW_MS + 300_000))),
        // Every key already has a cold snapshot: the partition is skipped.
        (interval_ms + 1, 0, None),
    ];
    for (after, expected, record) in rows {
        set_clock(&clock, after);
        let before = records_of(&coord, "gb", T2).len();
        check!(
            coord.snapshot_cold_partitions().await == expected,
            "{after}"
        );
        let new: Vec<Logged> = records_of(&coord, "gb", T2)
            .into_iter()
            .skip(before)
            .map(|(_, logged)| logged)
            .collect();
        check!(new == record.into_iter().collect::<Vec<_>>(), "{after}");
    }

    coord.prune_state_partitions().await;
    let state_partition = coord.state_partition_for("gb", &T2, 0);
    let part = coord
        .partitions
        .get(bootstrap::TOPIC, state_partition)
        .unwrap();
    let latest = coord.state_for_test("gb", T2, 0).await.unwrap();
    check!(latest.last_snapshot_offset > Offset(0));
    assert!(part.log_start_offset() == latest.last_snapshot_offset);
}

fn with_share_version(mut image: MetadataImage, level: i16) -> Arc<MetadataImage> {
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::SHARE_VERSION_FEATURE.into(),
        level,
    }));
    Arc::new(image)
}

#[test]
fn periodic_jobs_follow_share_version() {
    for (level, expected) in [(0, false), (1, true)] {
        let image = with_share_version(image_with_topics(&[]), level);
        check!(periodic_jobs_enabled(&image) == expected, "{level}");
    }
}

/// Waits until `done` holds, for at most ten seconds.
async fn eventually(mut done: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if done() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    done()
}

/// The loop tombstones the keys of a topic that a new image deletes, and
/// runs the cold snapshot and the prune only while share groups are
/// enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_loop_reacts_to_images_and_timers() {
    let config = ShareCoordinatorConfig {
        state_topic_prune_interval: Duration::from_millis(20),
        cold_partition_snapshot_interval: Duration::from_millis(20),
        ..ShareCoordinatorConfig::default()
    };
    // (share.version level, expected: B is snapshotted and pruned)
    for (level, jobs_run) in [(0, false), (1, true)] {
        let dir = tempdir().unwrap();
        let (coord, clock) = two_keys(dir.path(), config.clone()).await;
        set_clock(&clock, 60_000);
        let both = with_share_version(image_with_topics(&[(T1, 1), (T2, 1)]), level);
        let (images, receiver) = watch::channel(both);
        let shutdown = CancellationToken::new();
        spawn(Arc::clone(&coord), receiver, shutdown.clone());

        images
            .send(with_share_version(image_with_topics(&[(T2, 1)]), level))
            .unwrap();
        let a_partition = coord.state_partition_for("ga", &T1, 0);
        check!(
            eventually(|| logged_records(&coord, a_partition)
                .iter()
                .any(|(_, logged)| *logged == Logged::Tombstone))
            .await,
            "level {level}"
        );

        let b_partition = coord.state_partition_for("gb", &T2, 0);
        let part = coord.partitions.get(bootstrap::TOPIC, b_partition).unwrap();
        if jobs_run {
            check!(
                eventually(|| part.log_start_offset() > Offset(0)).await,
                "level {level}"
            );
        } else {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let snapshots = logged_records(&coord, b_partition)
                .iter()
                .filter(|(_, logged)| matches!(logged, Logged::Snapshot(_)))
                .count();
            check!(snapshots == 1, "level {level}");
            check!(part.log_start_offset() == Offset(0), "level {level}");
        }
        shutdown.cancel();
    }
}
