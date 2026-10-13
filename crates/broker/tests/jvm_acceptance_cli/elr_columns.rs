//! KIP-966: the `Elr:` and `LastKnownElr:` columns of `kafka-topics
//! --describe`, compared against a real Apache Kafka broker.
//!
//! The columns are rendered by `TopicCommand.PartitionDescription`, which
//! prints `Elr: N/A` when `TopicPartitionInfo.elr()` is null and prints the
//! joined replica ids -- empty for an empty list -- otherwise. A broker that
//! sends null therefore makes `kafka-topics --describe` say "this broker does
//! not know" where a real Kafka broker says "none", and no operator reading
//! the report can tell the two apart.
//!
//! The test does not hard-code what Kafka prints. It starts a JVM broker from
//! the same image the CLI runs from, creates the same topic on both brokers,
//! and compares the rendered partition lines byte for byte. One tool version
//! renders both sides, so any difference is a difference in what the two
//! brokers answered.

use {
    crate::jvm_acceptance::{
        KAFKA_IMAGE_ELR, broker0_advertised, docker_run_kafka_tool_with_image,
        nc_check_connectivity, start_host_broker,
    },
    assert2::assert,
    std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    },
};

/// The topic both brokers get. The name is part of every rendered line, so the
/// two sides must use the same one for the comparison to be exact.
const TOPIC: &str = "krabka-elr-columns-itest";

/// Where `kafka-topics` lives in the Apache Kafka image. The image's entry
/// point does not put it on `PATH`.
const KAFKA_TOPICS: &str = "/opt/kafka/bin/kafka-topics.sh";

/// `kafka-topics` arguments, without the script path and without the
/// bootstrap address, for the two calls each broker gets.
fn create_args() -> Vec<&'static str> {
    vec![
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "2",
        "--replication-factor",
        "1",
    ]
}

fn describe_args() -> Vec<&'static str> { vec!["--describe", "--topic", TOPIC] }

/// Run `kafka-topics` inside the JVM broker's own container, against its
/// loopback listener. Nothing has to be published to the host for this side.
fn kafka_topics_in_container(container: &str, args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .args(["exec", container, KAFKA_TOPICS])
        .args(["--bootstrap-server", "localhost:9092"])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn docker exec")
}

/// Run `kafka-topics` from a throwaway container of the same image, against
/// the krabka broker on the host.
fn kafka_topics_against_krabka(args: &[&str]) -> std::process::Output {
    let mut command = vec![KAFKA_TOPICS, "--bootstrap-server", broker0_advertised()];
    command.extend_from_slice(args);
    docker_run_kafka_tool_with_image(KAFKA_IMAGE_ELR, &command)
}

/// Start a single-node `KRaft` Apache Kafka broker and wait until it answers
/// metadata requests.
async fn start_jvm_broker(container: &str) {
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container,
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
            "-e",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            KAFKA_IMAGE_ELR,
        ])
        .status()
        .expect("spawn docker run");
    assert!(status.success(), "start JVM broker {container}");

    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let out = kafka_topics_in_container(container, &["--list"]);
        if out.status.success() {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "JVM broker {container} never became ready: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Describe the topic on the krabka broker once every partition has a leader.
///
/// `--create` returns as soon as the controller has written the partition
/// records, and the first `--describe` after it can still render
/// `Leader: none`. Every column of the line is compared, so the report has to
/// be taken after the election rather than during it.
async fn describe_krabka_once_led() -> Vec<String> {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let out = kafka_topics_against_krabka(&describe_args());
        let lines = partition_lines(&String::from_utf8_lossy(&out.stdout));
        if !lines.is_empty() && !lines.iter().any(|line| line.contains("Leader: none")) {
            return lines;
        }
        assert!(
            Instant::now() <= deadline,
            "krabka never elected a leader for every partition: {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The indented per-partition lines of a `--describe` report, in order.
///
/// The topic summary line carries a `TopicId` and the broker's effective
/// `Configs`, neither of which this test is about. The partition lines carry
/// the ELR columns.
fn partition_lines(describe: &str) -> Vec<String> {
    describe
        .lines()
        .filter(|line| line.starts_with('\t'))
        .map(str::to_owned)
        .collect()
}

/// `kafka-topics --describe` must render the same partition line against
/// krabka as it does against Apache Kafka, ELR columns included.
///
/// A healthy partition has no eligible-leader replicas, so both brokers send
/// empty lists and the tool prints `Elr:` and `LastKnownElr:` with nothing
/// after them. A null on either field would print `N/A` on the krabka side
/// only, and this comparison would fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn kafka_topics_describe_renders_the_same_elr_columns_as_apache_kafka() {
    let container = format!("krabka-elr-columns-{}", std::process::id());
    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .output();

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();
    start_jvm_broker(&container).await;

    let jvm_create = kafka_topics_in_container(&container, &create_args());
    assert!(
        jvm_create.status.success(),
        "create on the JVM broker: {}",
        String::from_utf8_lossy(&jvm_create.stderr)
    );
    let jvm_describe = kafka_topics_in_container(&container, &describe_args());
    assert!(
        jvm_describe.status.success(),
        "describe on the JVM broker: {}",
        String::from_utf8_lossy(&jvm_describe.stderr)
    );
    let jvm_lines = partition_lines(&String::from_utf8_lossy(&jvm_describe.stdout));

    let krabka_create = kafka_topics_against_krabka(&create_args());
    assert!(
        krabka_create.status.success(),
        "create on krabka: {}",
        String::from_utf8_lossy(&krabka_create.stderr)
    );
    let krabka_lines = describe_krabka_once_led().await;

    let _ = Command::new("docker")
        .args(["rm", "-f", &container])
        .output();

    assert!(
        !jvm_lines.is_empty(),
        "the JVM broker rendered no partition lines"
    );
    assert!(
        jvm_lines.iter().all(|line| line.contains("\tElr: ")),
        "the JVM reference does not carry the ELR columns: {jvm_lines:?}"
    );
    assert!(
        krabka_lines == jvm_lines,
        "krabka rendered {krabka_lines:?}, Apache Kafka rendered {jvm_lines:?}"
    );

    broker.shutdown().await;
}
