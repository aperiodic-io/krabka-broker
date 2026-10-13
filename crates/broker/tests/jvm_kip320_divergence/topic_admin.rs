//! Topic administration and metadata observation through the JVM CLI tools.
//!
//! The scenarios gate their steps on what an *external* Kafka client sees,
//! rather than on Krabka's already-applied metadata image. This module holds
//! the `kafka-topics` calls that create the mixed-cluster topic, wait for a
//! named leader, and parse the ISR out of a `--describe` report.

use {
    crate::docker::{KAFKA_IMAGE, docker_run_kafka_tool_with_image},
    std::time::{Duration, Instant},
};

/// How long to wait for an external metadata request to report a leader.
///
/// This gates on a *resumed* JVM broker in two of the three call sites: the
/// container is sent `SIGSTOP`, so its `KRaft` session expires, and on unpause
/// it must re-register before the partition has any eligible leader at all. CI
/// caught the gap at 45s -- `Leader: none` with an empty ISR after the full
/// budget -- while the same commit passed elsewhere. The wait returns as soon as
/// the leader appears, so a higher ceiling costs a healthy run nothing.
pub const LEADER_WAIT: Duration = Duration::from_mins(2);

/// Wait until an external Kafka metadata request observes `expected` as the
/// partition leader. This gates producer/follower steps on the JVM broker's
/// view, rather than only on Krabka's already-applied metadata image.
pub async fn wait_for_described_leader(
    bootstrap: &str,
    topic: &str,
    expected: u64,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    let marker = format!("Leader: {expected}");
    loop {
        let output = describe_topic(bootstrap, topic);
        let description = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && description.contains(&marker) {
            return;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "external metadata never observed {topic} leader {expected}: {description}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub fn described_isr(description: &str) -> Vec<u64> {
    description
        .lines()
        .find_map(|line| line.split_once("Isr:").map(|(_, tail)| tail))
        .and_then(|tail| tail.split_whitespace().next())
        .into_iter()
        .flat_map(|ids| ids.split(','))
        .filter_map(|id| id.parse().ok())
        .collect()
}

/// Create the RF=3 mixed-cluster topic after all brokers have registered.
/// Registration and unfencing are separate `KRaft` transitions, so retry the
/// administrative request through the short window between them.
pub async fn create_mixed_topic(bootstrap: &str, topic: &str) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = docker_run_kafka_tool_with_image(KAFKA_IMAGE, &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "3",
            "--bootstrap-server",
            bootstrap,
        ]);
        if output.status.success() {
            return;
        }
        assert2::assert!(
            Instant::now() <= deadline,
            "create topic {topic} did not succeed after broker registration: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[test]
fn topic_description_isr_parser_ignores_ids_outside_isr_field() {
    let description =
        "Topic: krabka-kip320-3 Partition: 0 Leader: 1 Replicas: 1,2,3 Isr: 1,2 Elr: 3";
    assert2::assert!(described_isr(description) == vec![1, 2]);
}

/// Create the mixed topic and wait for the scenario's required JVM-visible ISR.
pub async fn create_and_wait_for_isr(
    bootstrap: &str,
    topic: &str,
    required: &[u64],
    context: &str,
) {
    create_mixed_topic(bootstrap, topic).await;
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let output = describe_topic(bootstrap, topic);
        let description = String::from_utf8_lossy(&output.stdout);
        let isr = described_isr(&description);
        if required.iter().all(|node| isr.contains(node)) {
            return;
        }
        assert2::assert!(Instant::now() <= deadline, "{context}: {description}");
        // External JVM fetch progress has no matching in-process awaiter, and needs this two-minute budget.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn describe_topic(bootstrap: &str, topic: &str) -> std::process::Output {
    docker_run_kafka_tool_with_image(KAFKA_IMAGE, &[
        "kafka-topics",
        "--describe",
        "--topic",
        topic,
        "--bootstrap-server",
        bootstrap,
    ])
}
