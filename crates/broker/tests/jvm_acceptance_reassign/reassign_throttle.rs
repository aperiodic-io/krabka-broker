//! `kafka-reassign-partitions --execute --throttle` and the `--verify` that
//! clears the throttle again.
//!
//! Beyond the move itself this asserts the broker-scoped throttle config the
//! tool writes is visible to `kafka-configs --describe` and is gone from the
//! metadata image once `--verify` has run.

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE_TXN, broker0_advertised};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_reassign_partitions_with_throttle_end_to_end() {
    const TOPIC: &str = "krabka-throttle-reassign-itest";

    let (cluster, _admin_props, admin_mount) =
        Box::pin(crate::cluster::reassignment_cluster(TOPIC)).await;

    let (initial, new_node, staying) = crate::cluster::assignment(&cluster.h1, TOPIC, false);
    let (_json_file, json_mount) =
        crate::cluster::execute_plan(&admin_mount, TOPIC, staying, new_node, true);

    // Verify throttle configs were applied via kafka-configs --describe.
    let desc = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE_TXN,
        mounts: &[&admin_mount],
        args: &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "brokers",
            "--entity-name",
            "1",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
        ..Default::default()
    })
    .output()
    .expect("spawn kafka-configs --describe");
    eprintln!(
        "KRABKA[test] kafka-configs describe status={} stdout={} stderr={}",
        desc.status,
        String::from_utf8_lossy(&desc.stdout),
        String::from_utf8_lossy(&desc.stderr),
    );
    let desc_stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        desc_stdout.contains("leader.replication.throttled.rate=1024"),
        "leader.replication.throttled.rate=1024 not visible in kafka-configs output: {desc_stdout}"
    );

    // Inject ISR including new_node so the background reassignment-completion
    // task can see the new broker in ISR without relying on inter-broker
    // replication (which is broken under WSL2 due to host-gateway routing;
    // the reassignment tests use the same technique).
    let pr_after = cluster
        .h1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record after execute");
    let removing_replica = crate::cluster::removed_replica(&pr_after, &initial);
    let injected = krabka_metadata::PartitionRecord {
        isr: vec![
            krabka_metadata::NodeId(staying),
            krabka_metadata::NodeId(new_node),
            removing_replica,
        ],
        ..pr_after.clone()
    };
    cluster
        .h1
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject ISR for reassignment completion");

    // Wait until the reassignment completes (adding/removing replicas drained
    // from the committed metadata image).
    cluster
        .h1
        .wait_for_image(|img| {
            img.partition(TOPIC, 0)
                .is_some_and(|pr| pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty())
        })
        .await;
    // After completion the replica set must be exactly {staying, new_node}.
    crate::cluster::assert_reassigned(&cluster.h1, TOPIC, staying, new_node);
    eprintln!("KRABKA[test] reassignment completed; running --verify");

    // --verify clears throttle configs and exits 0 (broker-scoped
    // IncrementalAlterConfigs is supported).
    let _ = crate::jvm_acceptance::verify_console_reassignment(&admin_mount, &json_mount);

    // Confirm throttle configs were cleared from the metadata image after --verify.
    cluster
        .h1
        .wait_for_image(|img| {
            img.broker_throttle_rate(
                krabka_metadata::NodeId(1),
                krabka_metadata::ThrottleKind::Leader,
            )
            .is_none()
        })
        .await;

    cluster.h1.shutdown().await;
    cluster.h2.shutdown().await;
    cluster.h3.shutdown().await;
}
