//! Kafka's `deleteLogSegmentsDueToLeaderEpochCacheTruncation`, as the
//! tiered-storage sweep runs it across a broker restart (#1200).
//!
//! The rule deletes a remote segment whose epochs all lie below the earliest
//! entry of the leader's epoch cache. That is only sound when the cache was cut
//! from a log start somebody set. Each case builds a log, moves it as the case
//! says, closes it, reopens it as a restarted broker does, and ticks once over
//! an archive that holds one segment of the lineage below the log start, one
//! of another lineage above it, and one of the current lineage.

use {
    super::*,
    crate::{log_dir::partition_dir, remote_log_manager::test_support as fixtures},
    assert2::check,
    fixtures::{batch, leading_partition_over, local_backends},
    krabka_ids::LeaderEpoch,
    krabka_log::{Log, RemoteTierFlags},
    krabka_remote_storage::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    },
    uuid::Uuid,
};

/// The id and the `(first offset, last offset, epoch)` of each archive segment.
///
/// - 1: epoch 1 over offsets 0 to 3, the current lineage, below a log start of 4.
/// - 2: epoch 0 over offsets 4 to 7, which the current lineage assigns to epoch
///   1: what an unclean election leaves behind.
/// - 3: epoch 2 over offsets 8 to 15, the current lineage.
const ARCHIVE: [(u128, i64, i64, i32); 3] = [(1, 0, 3, 1), (2, 4, 7, 0), (3, 8, 15, 2)];

fn config() -> LogConfig {
    LogConfig {
        segment_size: bytes(256),
        remote_storage_enable: true,
        // The archive is seeded by hand, so nothing may be copied over it.
        remote_tier: RemoteTierFlags {
            copy_disable: true,
            delete_on_disable: false,
        },
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    }
}

/// A log of twelve two-record batches: epoch 1 over offsets 0 to 7, epoch 2
/// over 8 to 15 and epoch 3 over 16 to 23.
fn epoch_log(partition_dir: &std::path::Path) -> Log {
    let mut log = Log::open(partition_dir, config()).unwrap();
    for epoch in [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3] {
        let mut batch = batch(2);
        batch.partition_leader_epoch = epoch;
        log.append(&mut batch).unwrap();
    }
    log
}

/// Ticks once over `log` as a leader that was just restarted on it, with the
/// archive the constants above describe, and returns the ids of the segments
/// the archive still holds.
async fn survivors_of_a_tick(log_dir: &std::path::Path, log: Log) -> Vec<u128> {
    let remote_dir = tempfile::tempdir().unwrap();
    let (rsm, rlmm) = local_backends(remote_dir.path());
    for (id, first, last, epoch) in ARCHIVE {
        let segment_id = RemoteLogSegmentId::new(tp(), Uuid::from_u128(id));
        let started = RemoteLogSegmentMetadata::new(
            segment_id.clone(),
            first,
            last,
            100,
            1,
            100,
            krabka_remote_storage::RemoteLogSegmentDetails::new(
                100,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(epoch) => first},
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(started).unwrap();
        rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: segment_id,
            event_timestamp_ms: 100,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap();
    }
    let partitions = PartitionRegistry::new();
    partitions.insert(
        "orders".into(),
        PartitionIndex(0),
        leading_partition_over(PartitionIndex(0), log_dir, log),
    );

    tick_all(
        &partitions,
        &fixed_source(image_with_orders_topic()),
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        NodeId(1),
        1,
        SweepConcurrency::default(),
    )
    .await;

    let mut ids: Vec<u128> = rlmm
        .list_remote_log_segments(&tp())
        .unwrap()
        .iter()
        .map(|md| md.remote_log_segment_id().id.as_u128())
        .collect();
    ids.sort_unstable();
    ids
}

/// A label, how the log is moved before the restart, and the ids of the archive
/// segments that survive the tick after it.
type Case = (&'static str, fn(&mut Log), Vec<u128>);

/// The ways a log start and its epoch cache reach a restart.
#[tokio::test]
async fn the_epoch_cache_cleanup_follows_an_established_log_start_across_a_restart() {
    let cases: [Case; 3] = [
        (
            "nobody moved the log start",
            |_| {},
            // Nothing is below an epoch cache that starts at epoch 1 and holds
            // no segment of epoch 0's lineage that it could name.
            vec![1, 2, 3],
        ),
        (
            "local retention evicted the archived head",
            |log| {
                log.delete_local_segments_through(Offset(8)).unwrap();
            },
            // The restart infers a log start of 8 from the segments left. An
            // epoch cache cut to it would start at epoch 2, and the rule would
            // delete the archive's epoch 1 segments, which the archive alone
            // still serves.
            vec![1, 2, 3],
        ),
        (
            "a DeleteRecords moved the log start",
            |log| log.set_log_start_offset(Offset(4)).unwrap(),
            // The log-start breach takes segment 1, and the epoch cache, cut
            // to epoch 1 at offset 4, takes segment 2.
            vec![3],
        ),
    ];
    for (name, mutate, expected) in cases {
        let log_dir = tempfile::tempdir().unwrap();
        let partition_dir = partition_dir(log_dir.path(), "orders", 0);
        std::fs::create_dir_all(&partition_dir).unwrap();
        let mut log = epoch_log(&partition_dir);
        mutate(&mut log);
        drop(log);
        let restarted = Log::open(&partition_dir, config()).unwrap();

        let survivors = survivors_of_a_tick(log_dir.path(), restarted).await;

        check!(survivors == expected, "{name}");
    }
}
