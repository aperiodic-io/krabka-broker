//! In-process single-voter Controller. It validates the wiring of openraft,
//! `log_store`, `state_machine`, and the listener, and it needs no 3-node
//! cluster.

use {
    krabka_metadata::{MetadataRecord, NodeId},
    krabka_raft::{Controller, ControllerConfig, ControllerHandle},
    krabka_units::prelude::{Time, millis},
    std::time::Duration,
    tempfile::TempDir,
    uuid::Uuid,
};

/// Single-voter elections are instant, and a short timeout keeps each test well
/// inside its 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

krabka_macros::topic_record_fixture!(single_partition_topic);

fn single_voter_config() -> (TempDir, ControllerConfig) {
    let dir = TempDir::new().unwrap();
    let mut config = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    config.election_timeout = FAST_ELECTION_TIMEOUT;
    // Pin the controller listen addr to a real loopback port so the network
    // factory has something to dial when initialize wants to seed members.
    config.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    (dir, config)
}

async fn wait_for_leader(controller: &ControllerHandle) {
    let mut receiver = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), receiver.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_create_topic_round_trip() {
    let (_dir, config) = single_voter_config();
    let controller = Controller::start(config).await.expect("controller start");
    // Wait until openraft elects this single voter as leader.
    wait_for_leader(&controller).await;

    let topic = MetadataRecord::V1Topic(single_partition_topic("t", Uuid::new_v4()));
    controller.submit_change(vec![topic]).await.expect("submit");

    assert2::assert!(controller.current_image().topic("t").is_some());

    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_duplicate_topic_rejected() {
    let (_dir, config) = single_voter_config();
    let controller = Controller::start(config).await.unwrap();
    wait_for_leader(&controller).await;

    let topic = MetadataRecord::V1Topic(single_partition_topic("t", Uuid::new_v4()));
    controller.submit_change(vec![topic.clone()]).await.unwrap();
    let err = controller.submit_change(vec![topic]).await.unwrap_err();
    assert2::assert!(matches!(
        err,
        krabka_raft::RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))
    ));

    controller.shutdown().await;
}
