//! The `kafka-delete-records` tool, which trims a log through `DeleteRecords`
//! and reports the new low watermark.

use std::io::Write as _;

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, KAFKA_IMAGE_TXN, broker0_advertised, docker_run_kafka_tool_with_image,
    docker_run_kafka_tool_with_mount, nc_check_connectivity, start_host_broker,
    start_host_broker_in, wait_jvm_partition_leader, write_temp_file,
};

/// `kafka-delete-records --offset-json-file <(...)`: produce 20
/// records, trim to offset 10, expect success + `low_watermark`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trims_log() {
    const TOPIC: &str = "krabka-delete-recs-itest";

    let (_broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    create_topic(TOPIC);
    produce_lines(TOPIC, 20);

    let out = delete_records_to(TOPIC, 10);
    assert!(
        out.contains("low_watermark") || out.contains("10"),
        "delete-records output missing low_watermark: {out}"
    );
}

/// A trim that lands inside the active segment stays in force across a broker
/// restart, as the JVM tools see it.
///
/// The offsets before a `DeleteRecords` are not on disk anywhere once the
/// segment they live in is still open for append: nothing rolls, nothing is
/// deleted, and only the recorded log start offset says they are gone. This
/// case boots the second broker on the first one's log directory and asks
/// `kafka-get-offsets --time -2` again, which is the request a JVM consumer
/// makes when it resets to `earliest`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_delete_records_trim_survives_a_broker_restart() {
    const TOPIC: &str = "krabka-delete-recs-restart-itest";
    /// Inside the first (and only) segment, which 20 records do not fill.
    const TRIM_TO: i64 = 7;

    // The directory outlives both brokers, so the second boot reopens the log
    // the first one trimmed.
    let dir = tempfile::tempdir().expect("tempdir");

    let broker = start_host_broker_in(dir.path()).await;
    nc_check_connectivity();

    create_topic(TOPIC);
    wait_jvm_partition_leader(&broker, TOPIC, 0, 1).await;
    produce_lines(TOPIC, 20);
    delete_records_to(TOPIC, TRIM_TO);

    assert!(
        jvm_earliest_offset(TOPIC) == TRIM_TO,
        "earliest offset before the restart"
    );

    // The JVM broker binds fixed ports, and so does this one: the first broker
    // has to be all the way down before the second can take the listener.
    broker.shutdown().await;

    let broker = start_host_broker_in(dir.path()).await;
    wait_jvm_partition_leader(&broker, TOPIC, 0, 1).await;
    broker
        .wait_until_local_partition_leader(TOPIC, 0, krabka_broker::NodeId(1))
        .await;

    assert!(
        jvm_earliest_offset(TOPIC) == TRIM_TO,
        "earliest offset after the restart"
    );

    broker.shutdown().await;
}

/// One partition at replication factor one, which is all a single node hosts.
fn create_topic(topic: &str) {
    crate::jvm_acceptance::create_console_topic(
        crate::jvm_acceptance::KAFKA_IMAGE,
        &[],
        topic,
        1,
        1,
    );
}

/// Produce `count` records to `topic` through `kafka-console-producer`, one
/// line per record on the tool's stdin.
fn produce_lines(topic: &str, count: usize) {
    let mut child = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE,
        args: &[
            "kafka-console-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            topic,
        ],
        input: crate::support::ContainerInput::Attached,
        ..Default::default()
    })
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .expect("spawn producer");
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        for i in 0..count {
            writeln!(stdin, "msg-{i}").expect("write");
        }
    }
    drop(child.stdin.take());
    let prod_out = child.wait_with_output().expect("wait producer");
    assert!(
        prod_out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&prod_out.stdout),
        String::from_utf8_lossy(&prod_out.stderr),
    );
}

/// Trim `topic`-0 up to `offset` with `kafka-delete-records`, returning the
/// tool's stdout.
///
/// The offset-json file is built on the host and bind-mounted in. The cp-kafka
/// container runs as a non-root user, and `write_temp_file` is what relaxes the
/// `0600` a tempfile is created with to a mode that user can read; native Linux
/// CI enforces that strictly, where WSL and Docker Desktop do not.
fn delete_records_to(topic: &str, offset: i64) -> String {
    let json = format!(
        r#"{{"partitions":[{{"topic":"{topic}","partition":0,"offset":{offset}}}],"version":1}}"#
    );
    let file = write_temp_file("offsets.json", &json);
    let mount = format!("{}:/offsets.json:ro", file.host_path());

    let out = docker_run_kafka_tool_with_mount(
        &mount,
        &[
            "kafka-delete-records",
            "--bootstrap-server",
            broker0_advertised(),
            "--offset-json-file",
            "/offsets.json",
        ],
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// The earliest offset the JVM tools report for `topic`-0.
///
/// `kafka-get-offsets --time -2` is `ListOffsets` at the earliest timestamp,
/// which answers with the log start offset. The tool prints one
/// `topic:partition:offset` row per partition.
///
/// This one call needs [`KAFKA_IMAGE_TXN`] rather than the suite's default
/// [`KAFKA_IMAGE`]: the `kafka-get-offsets` wrapper script arrived in Kafka
/// 3.0, and `cp-kafka:6.1.1` is Kafka 2.7, where the container exits 127 with
/// `executable file not found in $PATH`. `jvm_barrier_markers.rs` reads
/// offsets off the newer image for the same reason. Only the tool container
/// changes; the broker under test is still the one this case booted.
fn jvm_earliest_offset(topic: &str) -> i64 {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-get-offsets",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic-partitions",
            &format!("{topic}:0"),
            "--time",
            "-2",
        ],
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let prefix = format!("{topic}:0:");
    let line = stdout
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("kafka-get-offsets printed no row for {topic}-0: {stdout}"));
    crate::support::jvm_parse_offset(line)
}
