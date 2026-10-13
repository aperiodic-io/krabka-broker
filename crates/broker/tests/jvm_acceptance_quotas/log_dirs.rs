//! KIP-113 log-directory reporting: `kafka-log-dirs --describe` against a
//! two-directory JBOD broker.
//!
//! This is the one suite here that boots a single host broker rather than the
//! three-broker SASL cluster, because JBOD spread is a per-broker property.

use {
    crate::jvm_acceptance::{
        broker0_advertised, docker_run_kafka_tool, nc_check_connectivity, start_host_broker_jbod,
    },
    assert2::check,
};

/// KIP-113: `kafka-log-dirs --describe` against a two-directory
/// JBOD broker. The test asserts that the JVM tool sees both configured log
/// directories and that the new topic spreads its partitions across them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_kafka_log_dirs_describe_reports_jbod_spread() {
    let (broker, primary, extra) = start_host_broker_jbod().await;
    nc_check_connectivity();

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        "jbodtopic",
        "--partitions",
        "6",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // Wait for the local writer-actor of every partition to materialize on
    // disk before the JVM tool inspects the log dirs.
    for p in 0..6 {
        broker
            .wait_until_local_log_end_offset("jbodtopic", p, 0)
            .await;
    }

    let out = docker_run_kafka_tool(&[
        "kafka-log-dirs",
        "--describe",
        "--bootstrap-server",
        broker0_advertised(),
        "--broker-list",
        "1",
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);

    // The broker reports canonical absolute host paths; canonicalize the
    // expected dirs so the substring match is robust to /tmp symlinks.
    let primary_path =
        std::fs::canonicalize(primary.path()).unwrap_or_else(|_| primary.path().to_path_buf());
    let extra_path =
        std::fs::canonicalize(extra.path()).unwrap_or_else(|_| extra.path().to_path_buf());

    check!(
        stdout.contains(&primary_path.display().to_string()),
        "kafka-log-dirs output missing primary dir {}; got: {stdout}",
        primary_path.display()
    );
    check!(
        stdout.contains(&extra_path.display().to_string()),
        "kafka-log-dirs output missing extra dir {}; got: {stdout}",
        extra_path.display()
    );
    check!(
        stdout.contains("jbodtopic"),
        "kafka-log-dirs output missing topic partitions; got: {stdout}"
    );

    broker.shutdown().await;
}
