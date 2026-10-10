//! `FetchSnapshot` (`api_key` 59, KIP-630), end to end.
//!
//! The test boots one in-process broker, creates a topic so that the metadata
//! image is non-empty, and triggers a controller snapshot. It then fetches the
//! `__cluster_metadata` snapshot byte range over the wire and asserts that the
//! broker serves the page. The broker listener answers as Kafka's
//! `KafkaRaftClient.handleFetchSnapshotRequest` does, so a request names the
//! snapshot it wants and the leader epoch it believes in.

use std::{
    path::Path,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_protocol::owned::fetch_snapshot_request::{
    FetchSnapshotRequest, PartitionSnapshot, SnapshotId, TopicSnapshot,
};

use crate::support::topics::{creatable_topic, create_topic_request};

mod support;

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";
const FENCED_LEADER_EPOCH: i16 = 74;
const SNAPSHOT_NOT_FOUND: i16 = 98;
const POSITION_OUT_OF_RANGE: i16 = 99;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

fn fetch(snapshot: (i64, i32), leader_epoch: i32, position: i64) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        replica_id: -1,
        max_bytes: 1 << 20,
        topics: vec![TopicSnapshot {
            name: CLUSTER_METADATA_TOPIC.into(),
            partitions: vec![PartitionSnapshot {
                partition: 0,
                current_leader_epoch: leader_epoch,
                snapshot_id: SnapshotId {
                    end_offset: snapshot.0,
                    epoch: snapshot.1,
                    ..Default::default()
                },
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        cluster_id: None,
        ..Default::default()
    }
}

/// The `(end_offset, epoch)` of every `<end_offset>-<epoch>.checkpoint` file
/// under `dir`, which is how the controller names the snapshots it wrote.
fn checkpoint_ids(dir: &Path) -> Vec<(i64, i32)> {
    let mut ids = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return ids;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            ids.extend(checkpoint_ids(&path));
        } else if let Some((end_offset, epoch)) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".checkpoint"))
            .and_then(|name| name.split_once('-'))
            && let (Ok(end_offset), Ok(epoch)) = (end_offset.parse(), epoch.parse())
        {
            ids.push((end_offset, epoch));
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_serves_the_named_metadata_snapshot() {
    // The test owns the data directory, since the snapshot file it reads back
    // lives under it.
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, client) = support::start_with_dir(dir.path()).await;

    // Make the metadata image non-empty so the snapshot has real content.
    let resp = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("snap-topic").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);

    broker
        .trigger_snapshot_for_test()
        .await
        .expect("trigger snapshot");

    // A stale leader epoch is refused, and the refusal names the leader and
    // its epoch, which is how a fetcher learns the epoch to send.
    let refused = client
        .send(fetch((0, 0), 0, 0))
        .await
        .unwrap()
        .topics
        .remove(0)
        .partitions
        .remove(0);
    assert!(refused.error_code == FENCED_LEADER_EPOCH);
    let leader_epoch = refused.current_leader.leader_epoch;
    check!(refused.current_leader.leader_id == 1);
    assert!(leader_epoch > 0);

    // The trigger only schedules the snapshot; it completes asynchronously.
    // Wait for its file, whose name is the snapshot id a fetcher asks for.
    let deadline = Instant::now() + Duration::from_secs(30);
    let snapshot = loop {
        if let Some(id) = checkpoint_ids(dir.path()).into_iter().max() {
            break id;
        }
        assert!(
            Instant::now() <= deadline,
            "snapshot not written within 30s"
        );
        // intentional: snapshot production completes asynchronously in raft
        // and has no metadata-image or metric signal (the image was already
        // non-empty after CreateTopics, so it does not change when the
        // snapshot is written). The only observable is the checkpoint file,
        // so we poll for it under a bounded deadline.
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let out = client.send(fetch(snapshot, leader_epoch, 0)).await.unwrap();
    assert!(out.error_code == 0, "top-level error_code");
    let part = &out.topics[0].partitions[0];
    assert!(part.error_code == 0, "partition error_code");
    check!(part.index == 0);
    check!(part.snapshot_id.end_offset == snapshot.0);
    check!(part.snapshot_id.epoch == snapshot.1);
    check!(
        part.size > 0,
        "served snapshot reports a non-zero total size"
    );
    check!(
        part.unaligned_records.payload_len() > 0,
        "served snapshot page carries bytes"
    );
    let size = part.size;

    // Another snapshot id is not served in its place, and the end of the
    // snapshot is not a position to read from.
    for (what, request, expected) in [
        (
            "an id that no snapshot has",
            fetch((snapshot.0 + 1, snapshot.1), leader_epoch, 0),
            SNAPSHOT_NOT_FOUND,
        ),
        (
            "the end of the snapshot",
            fetch(snapshot, leader_epoch, size),
            POSITION_OUT_OF_RANGE,
        ),
    ] {
        let out = client.send(request).await.unwrap();
        check!(out.error_code == 0, "{what}");
        check!(out.topics[0].partitions[0].error_code == expected, "{what}");
    }

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_rejects_non_metadata_topic() {
    let env = support::start().await;

    let mut req = fetch((0, 0), 0, 0);
    req.topics[0].name = "not-metadata".into();
    let out = env.client.send(req).await.unwrap();
    assert!(out.error_code == 0, "top-level error_code is success");
    // UNKNOWN_TOPIC_OR_PARTITION (3) for any topic other than
    // __cluster_metadata, as Kafka's raft client answers it.
    assert!(out.topics[0].partitions[0].error_code == UNKNOWN_TOPIC_OR_PARTITION);

    env.broker.shutdown().await;
}
