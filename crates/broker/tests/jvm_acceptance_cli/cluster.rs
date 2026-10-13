//! The `kafka-cluster` tool, whose `cluster-id` subcommand exercises
//! `DescribeCluster`.

use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image,
        nc_check_connectivity, start_host_broker,
    },
    assert2::assert,
};

/// `kafka-cluster cluster-id` exercises `DescribeCluster` (`api_key` 60).
///
/// Uses `cp-kafka:7.5.0` (= [`KAFKA_IMAGE_TXN`]) because:
/// - `cp-kafka:6.1.1` does not ship the `kafka-cluster` binary at all.
/// - `cp-kafka:7.5.0` ships it but the subcommand is `cluster-id`
///   (not `describe`; that alias does not exist in this version).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_cluster_describe() {
    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    let out = docker_run_kafka_tool_with_image(KAFKA_IMAGE_TXN, &[
        "kafka-cluster",
        "cluster-id",
        "--bootstrap-server",
        broker0_advertised(),
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    // `kafka-cluster cluster-id` prints a line like:
    //   "Cluster ID: <uuid>"
    assert!(
        s.contains("Cluster ID") || s.contains("cluster ID") || s.contains("00000000"),
        "cluster-id output missing cluster id: {s}"
    );
}
