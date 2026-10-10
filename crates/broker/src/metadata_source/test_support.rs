//! Fixtures that more than one of this module's unit-test modules needs: a
//! throwaway topic record to submit, and a wait for a freshly started
//! controller to elect itself.

use krabka_metadata::MetadataRecord;
use uuid::Uuid;

pub(super) fn topic_record(name: &str) -> MetadataRecord {
    MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
        name,
        Uuid::new_v4(),
    ))
}

pub(super) async fn wait_for_controller_leader(ctrl: &krabka_raft::ControllerHandle) {
    let mut leader_rx = ctrl.watch_leader();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("controller should elect itself");
}

/// A fresh bootstrapped single-node controller, already elected.
pub(super) async fn start_controller() -> (tempfile::TempDir, krabka_raft::ControllerHandle) {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = krabka_raft::ControllerConfig {
        bootstrap_mode: krabka_raft::BootstrapMode::Bootstrap,
        ..krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(1), dir.path().to_path_buf())
    };
    let ctrl = krabka_raft::Controller::start(cfg)
        .await
        .expect("controller");
    wait_for_controller_leader(&ctrl).await;
    (dir, ctrl)
}
