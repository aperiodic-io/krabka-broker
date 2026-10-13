//! End-to-end snapshot generation and restart recovery for a single-voter
//! controller.
//!
//! The test proves that a committed metadata change survives a manual snapshot
//! and a full process-style restart. That restart rebuilds the image from the
//! on-disk checkpoint, and not from in-memory state.
//!
//! The scope here is the manual `trigger_snapshot` path and the recovery that
//! follows it. The engine's own byte- and interval-triggered snapshotting is
//! covered in `krabka_raft::kraft::controller`, and the cross-node catch-up
//! over KIP-595 `FetchSnapshot` is covered by the engine simulator.

use {
    assert2::check,
    krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord, NodeId, TopicRecord},
    krabka_raft::{
        BootstrapMode, Controller, ControllerConfig, deserialize_metadata_snapshot,
        deserialize_metadata_snapshot_image, serialize_metadata_snapshot,
    },
    krabka_units::prelude::{Time, millis},
    std::time::Duration,
    tempfile::TempDir,
    uuid::Uuid,
};

krabka_macros::topic_record_fixture!(single_partition_topic);

/// Single-voter elections are instant, and a short timeout keeps each boot well
/// inside the 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

#[test]
fn public_snapshot_codec_round_trips_metadata() {
    let cid = Uuid::new_v4();
    let record = MetadataRecord::V1Topic(TopicRecord {
        name: "snapshot-codec".into(),
        topic_id: Uuid::new_v4(),
        partitions: 0,
        replication_factor: 1,
    });
    let image = MetadataImage::from_records(cid, &[record]);

    let bytes = serialize_metadata_snapshot(&image, 1_700_000_000_000).unwrap();
    let decoded = deserialize_metadata_snapshot(&bytes).unwrap();

    check!(!decoded.is_empty());
    check!(MetadataImage::from_records(cid, &decoded) == image);
}

/// A broker-only observer installs a snapshot with its KIP-853 controls, so the
/// image it rebuilds names the voters and their endpoints, as the image the
/// controller wrote does. The restore path leaves the controls out.
#[test]
fn the_snapshot_image_keeps_the_quorum_controls() {
    use krabka_metadata::{
        KRaftVersionRange, KRaftVersionRecord, Voter, VoterEndpoint, VoterSet, VotersRecord,
    };

    let cid = Uuid::new_v4();
    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "snapshot-image".into(),
        topic_id: Uuid::new_v4(),
        partitions: 0,
        replication_factor: 1,
    });
    let controls = [
        MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
        MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::from_voters([Voter {
                id: NodeId(3001),
                directory_id: Uuid::from_u128(3001),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "controller.example".into(),
                    port: 9_592,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }]),
        }),
    ];
    let image = MetadataImage::from_records(cid, &[
        controls[0].clone(),
        controls[1].clone(),
        topic.clone(),
    ]);
    let bytes = serialize_metadata_snapshot(&image, 1_700_000_000_000).unwrap();

    let whole = deserialize_metadata_snapshot_image(&bytes).unwrap();
    let restored = deserialize_metadata_snapshot(&bytes).unwrap();

    check!(MetadataImage::from_records(cid, &whole) == image);
    check!(
        MetadataImage::from_records(cid, &restored) == MetadataImage::from_records(cid, &[topic])
    );
}

async fn wait_for_leader(controller: &krabka_raft::ControllerHandle) {
    let mut leader_watch = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), async {
        while leader_watch.borrow().is_none() {
            leader_watch.changed().await.expect("leader watch closed");
        }
    })
    .await
    .expect("no leader elected within 30s");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_then_restart_recovers_image() {
    let dir = TempDir::new().unwrap();
    let cid = Uuid::new_v4();

    // First boot: bootstrap a fresh single voter, commit a topic, then snapshot.
    {
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Bootstrap;
        let controller = Controller::start(cfg).await.expect("first boot start");
        wait_for_leader(&controller).await;

        // Commit real KIP-631 metadata-log records — a topic and a finalized
        // feature level — that must survive snapshot + restart.
        //
        // The controller voter set is NOT submitted here: it is Raft control
        // state, not a KIP-631 metadata-log record. Snapshot serialization writes
        // it as the leading KIP-853 VotersRecord alongside kraft.version 0.
        controller
            .submit_change(vec![
                MetadataRecord::V1Topic(single_partition_topic("t", Uuid::new_v4())),
                // KIP-584 finalized feature: must survive snapshot + restart.
                // A dropped feature level reverts metadata.version to UNKNOWN.
                MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: "metadata.version".into(),
                    level: 25,
                }),
            ])
            .await
            .expect("submit records");
        check!(
            (
                controller.current_image().topic("t").is_some(),
                controller.current_image().finalized_metadata_version(),
                controller.current_image().voters().contains(NodeId(1)),
            ) == (true, Some(25), true)
        );

        controller
            .trigger_snapshot()
            .await
            .expect("trigger snapshot");

        // The checkpoint is written synchronously inside the engine before
        // `trigger_snapshot` returns; confirm it landed on disk.
        assert2::assert!(has_checkpoint(&krabka_raft::metadata_partition_dir(
            dir.path()
        )));

        controller.shutdown().await;
    }

    // Restart from the SAME dir in Rejoin mode. The engine must rebuild the
    // image from the on-disk checkpoint + log.
    {
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.cluster_id = Some(cid);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let controller = Controller::start(cfg).await.expect("rejoin start");
        wait_for_leader(&controller).await;

        assert2::assert!(controller.current_image().topic("t").is_some());
        // The finalized feature + its epoch must be rebuilt from the on-disk
        // checkpoint, not silently dropped.
        let recovered = controller.current_image();
        check!(
            recovered.finalized_metadata_version() == Some(25),
            "finalized metadata.version must survive snapshot + restart"
        );
        check!(
            recovered.finalized_features_epoch() >= 0,
            "finalized-features epoch must survive snapshot + restart, got {}",
            recovered.finalized_features_epoch()
        );
        // The voter set is restored from the snapshot's KIP-853 control state
        // and mirrored into the image — it must be present after restart.
        check!(
            recovered.voters().contains(NodeId(1)),
            "voter set must survive snapshot + restart"
        );

        controller.shutdown().await;
    }
}

// A lagging follower catching up through the real KIP-595 `FetchSnapshot` is
// covered by `kraft_engine_sim::lagging_follower_catches_up_via_snapshot`.

fn has_checkpoint(meta_dir: &std::path::Path) -> bool {
    std::fs::read_dir(meta_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint"))
}
