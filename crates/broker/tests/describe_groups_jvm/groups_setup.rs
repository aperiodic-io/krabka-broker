//! Readiness polling and consumer-group preparation, both driven by the JVM
//! admin tools on the container's own `PLAINTEXT` listener.
//!
//! The classic and next-generation images ship their command-line tools under
//! different names and paths, so each rebalance protocol gets its own
//! preparation function.

use {
    crate::{
        GROUP, NEXT_GEN_GROUP, TOPIC, TYPELESS_GROUP,
        groups_docker::{docker_logs, exec, exec_detached},
    },
    assert2::assert,
    std::time::{Duration, Instant},
};

pub(crate) fn wait_for_broker(api_versions_tool: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
    while Instant::now() < deadline {
        if exec(&[api_versions_tool, "--bootstrap-server", "localhost:9092"])
            .status
            .success()
        {
            eprintln!("CAPTURE broker READY");
            return;
        }
        // intentional: polls the external JVM cp-kafka container via its admin CLI; no in-process krabka metric/image to await.
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "Kafka never became ready within 120s.\ncontainer logs:\n{}",
        docker_logs()
    );
}

/// Wait until a group reports `Stable` with a member.
///
/// The check uses the JVM admin tool `kafka-consumer-groups --describe
/// --state`.
fn wait_for_group_stable(group: &str, consumer_groups_tool: &str) {
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut last = String::new();
    while Instant::now() < deadline {
        let out = exec(&[
            consumer_groups_tool,
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            group,
            "--state",
        ]);
        last = String::from_utf8_lossy(&out.stdout).into_owned();
        if last.contains("Stable") {
            eprintln!("CAPTURE group {group} STABLE:\n{last}");
            return;
        }
        // intentional: polls the external JVM broker's group state via kafka-consumer-groups; no in-process krabka metric/image to await.
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "group {group} never reached Stable within 60s.\nlast --state:\n{last}\nlogs:\n{}",
        docker_logs()
    );
}

pub(crate) fn prepare_classic_groups() {
    prepare_topic("kafka-topics", "kafka-console-producer");
    exec_detached(&format!(
        "kafka-console-consumer --bootstrap-server localhost:9092 --topic {TOPIC} --group {GROUP} \
         --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor \
         --from-beginning --timeout-ms 180000 > /tmp/consumer.out 2>&1"
    ));
    wait_for_group_stable(GROUP, "kafka-consumer-groups");

    let typeless = exec(&[
        "kafka-consumer-groups",
        "--bootstrap-server",
        "localhost:9092",
        "--group",
        TYPELESS_GROUP,
        "--topic",
        TOPIC,
        "--reset-offsets",
        "--to-earliest",
        "--execute",
    ]);
    assert!(
        typeless.status.success(),
        "create typeless group failed: {}",
        String::from_utf8_lossy(&typeless.stderr)
    );
}

pub(crate) fn prepare_next_gen_group() {
    prepare_topic(
        "/opt/kafka/bin/kafka-topics.sh",
        "/opt/kafka/bin/kafka-console-producer.sh",
    );
    exec_detached(&format!(
        "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {TOPIC} \
         --group {NEXT_GEN_GROUP} --consumer-property group.protocol=consumer \
         --from-beginning --timeout-ms 180000 > /tmp/consumer.out 2>&1"
    ));
    wait_for_group_stable(NEXT_GEN_GROUP, "/opt/kafka/bin/kafka-consumer-groups.sh");
}

/// Seed the same two-partition workload with each image's original tool paths.
fn prepare_topic(topics_tool: &str, producer_tool: &str) {
    let created = exec(&[
        topics_tool,
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        TOPIC,
        "--partitions",
        "2",
        "--replication-factor",
        "1",
    ]);
    assert!(
        created.status.success(),
        "create topic failed: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    let produced = exec(&[
        "bash",
        "-c",
        &format!(
            "printf 'r1\\nr2\\nr3\\nr4\\n' | {producer_tool} --bootstrap-server localhost:9092 --topic {TOPIC}"
        ),
    ]);
    assert!(
        produced.status.success(),
        "produce failed: {}",
        String::from_utf8_lossy(&produced.stderr)
    );
}
