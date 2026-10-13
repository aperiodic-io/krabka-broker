//! Scenario 4: a lossy `metadata.version` downgrade in a mixed cluster.
//!
//! Kafka's `FeatureControlManager.updateMetadataVersion` refuses every
//! downgrade that crosses a level whose `didMetadataChange` is set, with
//! either downgrade type. The scenario drives `kafka-features.sh` rather than
//! the replication path, and it needs its own registration waits, so it does
//! not share a file with the truncation scenarios.

use {
    crate::{
        docker::{KAFKA_IMAGE_FEATURES, docker_run_kafka_tool_with_image},
        mixed_cluster::{MixedCluster, start_mixed_cluster},
        support,
        topic_admin::create_mixed_topic,
    },
    krabka_broker::BrokerHandle,
    std::time::{Duration, Instant},
};

fn run_features(bootstrap: &str, command: &[&str]) -> std::process::Output {
    let mut args = vec![
        "/opt/kafka/bin/kafka-features.sh",
        "--bootstrap-server",
        bootstrap,
    ];
    args.extend_from_slice(command);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_FEATURES, &args)
}

/// Block until every Krabka broker sees the JVM broker (id 3) advertise
/// `expected` as its `metadata.version` maximum. The `AdminClient` can route
/// `UpdateFeatures` to either Krabka broker, so both images must hold the
/// registration before the test sends a downgrade.
async fn wait_for_jvm_metadata_max(cluster: &MixedCluster, expected: i16) {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let observed = cluster
            .krabka
            .iter()
            .map(|(broker, _)| {
                broker
                    .controller_image_for_test()
                    .broker(krabka_broker::NodeId(3))
                    .and_then(|registration| {
                        registration
                            .features
                            .get(krabka_metadata::metadata_version::METADATA_VERSION_FEATURE)
                            .map(|(_, max)| *max)
                    })
            })
            .collect::<Vec<_>>();
        if observed.iter().all(|max| *max == Some(expected)) {
            return;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "JVM broker did not advertise metadata.version max {expected} on every Krabka \
             broker; observed {observed:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Block until every Krabka broker's image holds a controller registration
/// for every voter. `UpdateFeatures` rejects any update with "controller N has
/// not registered" before it looks at the downgrade itself. The test must not
/// send the downgrade before those registrations land, or it asserts on the
/// wrong rejection text.
async fn wait_for_voter_registrations(cluster: &MixedCluster) {
    for (broker, _) in &cluster.krabka {
        broker
            .wait_for_image(|image| {
                image
                    .voters()
                    .iter()
                    .all(|voter| image.controller(voter.id).is_some())
            })
            .await;
    }
}

/// 4.0-IV3 (25) to 3.7-IV1 (16) crosses 4.0-IV1 and 3.7-IV2, which both
/// changed metadata, so Kafka refuses it as a safe and as an unsafe downgrade,
/// with its own message for each, and nothing changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + published controller/data ports; Linux-bound"]
async fn metadata_version_downgrade_refuses_lossy_levels() {
    const EXISTING_TOPIC: &str = "krabka-mv-capability-existing";
    const UPPER_LEVEL: i16 = 25; // 4.0-IV3.

    let container = support::unique_container_name("krabka-mv-capability-jvm-broker");

    let cluster = start_mixed_cluster(&container, false).await;
    assert2::assert!(
        cluster.wait_for_brokers(3, Duration::from_mins(2)).await,
        "JVM broker never joined the mixed cluster"
    );
    wait_for_jvm_metadata_max(&cluster, UPPER_LEVEL).await;
    wait_for_voter_registrations(&cluster).await;
    create_mixed_topic(&cluster.bootstrap_all, EXISTING_TOPIC).await;
    // The JVM reports its replica directory asynchronously after topic
    // creation. Wait for that report before capturing the metadata baseline;
    // other replicas of this empty topic may retain an unassigned slot.
    for (broker, _) in &cluster.krabka {
        broker
            .wait_for_image(|image| {
                image.partition(EXISTING_TOPIC, 0).is_some_and(|partition| {
                    partition
                        .replicas
                        .iter()
                        .position(|id| id.0 == 3)
                        .and_then(|slot| partition.directories.get(slot))
                        .is_some_and(|directory| {
                            !directory.is_nil()
                                && image.broker(krabka_broker::NodeId(3)).is_some_and(
                                    |registration| registration.log_dirs.contains(directory),
                                )
                        })
                })
            })
            .await;
    }
    let state = |broker: &BrokerHandle| {
        let image = broker.controller_image_for_test();
        (
            image.finalized_metadata_version(),
            image
                .brokers()
                .map(|registration| (registration.node_id, registration.log_dirs.clone()))
                .collect::<Vec<_>>(),
            image
                .partition(EXISTING_TOPIC, 0)
                .expect("existing mixed topic")
                .directories
                .clone(),
        )
    };
    let before = cluster
        .krabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    for (kind, command, reason) in [
        (
            "safe",
            vec!["downgrade", "--metadata", "3.7-IV1"],
            "Refusing to perform the requested downgrade because it might delete metadata \
             information.",
        ),
        (
            "unsafe",
            vec!["downgrade", "--metadata", "3.7-IV1", "--unsafe"],
            "Unsafe metadata downgrade is not supported in this version.",
        ),
    ] {
        let output = run_features(&cluster.bootstrap_all, &command);
        let error = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert2::assert!(
            !output.status.success()
                && error.contains(&format!(
                    "Unsupported metadata.version downgrade from {UPPER_LEVEL} to 16. {reason}"
                )),
            "{kind} downgrade was not refused as lossy: {error}"
        );
    }

    let after = cluster
        .krabka
        .iter()
        .map(|(broker, _)| state(broker))
        .collect::<Vec<_>>();
    assert2::assert!(
        after == before,
        "a refused downgrade changed finalized or directory metadata"
    );
    cluster.shutdown().await;
}
