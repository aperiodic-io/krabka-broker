//! A controller refuses to run at a feature level that it does not support.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws when a
//! record's level lies outside the controller's supported range, so a node
//! started with `unstable.feature.versions.enable` off never serves a log that
//! was finalized at an unstable `metadata.version`. The tests finalize
//! `4.4-IV2` (level 33), which is unstable in Kafka 4.3.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_metadata::{FeatureLevelRecord, MetadataRecord, NodeId};
use krabka_raft::{
    BootstrapMode, Controller, ControllerConfig, ControllerHandle, RaftError,
    UnstableFeatureVersions,
};
use krabka_units::prelude::{Time, millis};
use tempfile::TempDir;
use uuid::Uuid;

/// Single-voter elections are instant, and a short timeout keeps each boot well
/// inside the 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

krabka_macros::topic_record_fixture!(single_partition_topic);

const UNSTABLE_METADATA_VERSION: i16 = 33;

const REFUSAL: &str = "Tried to apply FeatureLevelRecord \
    FeatureLevelRecord(name='metadata.version', featureLevel=33), \
    but this controller only supports versions 7-30";

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FeatureControllerSetup {
    cluster_id: Uuid,
    #[default(BootstrapMode::Bootstrap)]
    mode: BootstrapMode,
    #[default(UnstableFeatureVersions::Disabled)]
    unstable: UnstableFeatureVersions,
}

fn config(dir: &TempDir, setup: FeatureControllerSetup) -> ControllerConfig {
    let FeatureControllerSetup {
        cluster_id,
        mode,
        unstable,
    } = setup;
    let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    cfg.election_timeout = FAST_ELECTION_TIMEOUT;
    cfg.cluster_id = Some(cluster_id);
    cfg.bootstrap_mode = mode;
    cfg.unstable_feature_versions = unstable;
    cfg
}

async fn wait_for_leader(controller: &ControllerHandle) {
    let mut rx = controller.watch_leader();
    tokio::time::timeout(Duration::from_secs(30), rx.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");
}

fn finalize_unstable_metadata_version() -> Vec<MetadataRecord> {
    vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: "metadata.version".into(),
        level: UNSTABLE_METADATA_VERSION,
    })]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_finalized_at_an_unstable_level_starts_only_with_the_flag_on() {
    let dir = TempDir::new().unwrap();
    let cluster_id = Uuid::new_v4();

    // The first boot has the flag on and finalizes the unstable level.
    let controller = Controller::start(config(
        &dir,
        FeatureControllerSetup {
            cluster_id,
            unstable: UnstableFeatureVersions::Enabled,
            ..Default::default()
        },
    ))
    .await
    .expect("first boot");
    wait_for_leader(&controller).await;
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("finalize the unstable level");
    controller.shutdown().await;

    // With the flag off the node refuses the log, with Kafka's message.
    let Err(refused) = Controller::start(config(
        &dir,
        FeatureControllerSetup {
            cluster_id,
            mode: BootstrapMode::Rejoin,
            ..Default::default()
        },
    ))
    .await
    else {
        panic!("a controller without the flag started over an unstable log");
    };
    check!(matches!(&refused, RaftError::FatalFault(message) if message == REFUSAL));
    check!(refused.to_string() == format!("Encountered fatal fault: {REFUSAL}"));

    // The refusal changed nothing on disk: the same log starts with the flag on.
    let controller = Controller::start(config(
        &dir,
        FeatureControllerSetup {
            cluster_id,
            mode: BootstrapMode::Rejoin,
            unstable: UnstableFeatureVersions::Enabled,
        },
    ))
    .await
    .expect("restart with the flag on");
    wait_for_leader(&controller).await;
    check!(controller.current_image().finalized_metadata_version() == Some(33));
    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_stops_when_it_replays_a_level_it_does_not_support() {
    let dir = TempDir::new().unwrap();
    let controller = Controller::start(config(
        &dir,
        FeatureControllerSetup {
            cluster_id: Uuid::new_v4(),
            ..Default::default()
        },
    ))
    .await
    .expect("boot");
    wait_for_leader(&controller).await;
    let fatal = controller.watch_fatal();
    check!(
        fatal.borrow().is_none(),
        "no fault before the level commits"
    );

    // The level commits and applies, since the check reads the image the engine
    // publishes. Then the controller stops, and no later change is accepted.
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit the unsupported level");
    let deadline = Instant::now() + Duration::from_secs(30);
    for probe in 0.. {
        let topic = MetadataRecord::V1Topic(single_partition_topic(
            &format!("probe-{probe}"),
            Uuid::new_v4(),
        ));
        if let Err(error) = controller.submit_change(vec![topic]).await {
            check!(matches!(error, RaftError::Shutdown), "{error}");
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "the controller kept accepting changes at an unsupported level"
        );
        // intentional: the stop follows the image publication on another task,
        // and nothing but a refused submit says that it has happened.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // The refusal is on the fatal channel by the time a submit fails: the host
    // that halts over it never sees the shutdown without the reason.
    check!(fatal.borrow().as_deref() == Some(REFUSAL));
    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_that_stops_for_any_other_reason_reports_no_fault() {
    let dir = TempDir::new().unwrap();
    let controller = Controller::start(config(
        &dir,
        FeatureControllerSetup {
            cluster_id: Uuid::new_v4(),
            unstable: UnstableFeatureVersions::Enabled,
            ..Default::default()
        },
    ))
    .await
    .expect("boot");
    wait_for_leader(&controller).await;
    // A level within the range is not a fault.
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit a supported level");
    let mut fatal = controller.watch_fatal();
    controller.shutdown().await;

    // The channel closes without ever carrying a value.
    check!(fatal.changed().await.is_err());
    check!(fatal.borrow().is_none());
}
