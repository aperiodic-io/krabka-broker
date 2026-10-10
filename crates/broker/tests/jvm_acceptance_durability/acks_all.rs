//! The steady-state `acks=all` durability gate: 100 records written with
//! `--request-required-acks -1` must all read back from a third broker under
//! `read_committed`.
//!
//! It covers the high-watermark path with the cluster intact. The variant that
//! kills the partition leader mid-burst lives beside it in `leader_crash`.

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE, docker_run_kafka_tool};

// `acks=all` durability gate: 3-broker Krabka cluster, JVM
// `kafka-console-producer --request-required-acks -1` writes 100
// records, then `kafka-console-consumer --isolation-level
// read_committed` reads them all back. Confirms HW+acks=all works
// against an unmodified JVM client.
//
// Fixed ports above 10000 — the other multi-broker tests use 9092-9992;
// this test steps into 10000+ to dodge TIME_WAIT + raft-quorum collisions
// when JVM tests run sequentially via the nextest broker-jvm-acceptance test group.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acks_all_durability() {
    const TOPIC: &str = "krabka-acks-all-itest";

    crate::support::init_jvm_tracing("krabka_broker=debug,info");

    // Ports 10092/10192/10292 + 10093/10193/10293 — the next free hundred
    // above the transactional test (9792-9992). The other multi-broker
    // tests use the 9092-9992 range; we step into 10000+ to avoid TIME_WAIT
    // collisions.
    let client_ports = [10092u16, 10192, 10292];
    let controller_ports = [10093u16, 10193, 10293];

    let cluster = crate::support::start_jvm_cluster(client_ports, controller_ports, |_| {}).await;

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);

    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // Produce 100 records with --request-required-acks=-1.
    let producer_out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE,
        args: &[
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_1} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 10000"
            ),
        ],
        input: crate::support::ContainerInput::Attached,
        ..Default::default()
    })
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .output()
    .expect("spawn kafka-console-producer");
    eprintln!(
        "KRABKA[test] producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "kafka-console-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // intentional: wait for the produced records (acks=-1) to replicate to
    // node 3 and its high-watermark to advance before the read_committed
    // consume below. Follower high-watermark/LSO is not in the metadata image
    // and has no krabka awaiter/metric.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);
    let consume_out = crate::jvm_acceptance::consume_committed_at(&bootstrap_3, TOPIC, 100);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = crate::support::jvm_output_lines(&consume_out).len();
    assert!(
        line_count >= 100,
        "expected at least 100 records; got {line_count}: stdout={stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
