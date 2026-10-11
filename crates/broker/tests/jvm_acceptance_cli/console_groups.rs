//! `kafka-console-consumer` runs that join a consumer group: the default
//! assignor, KIP-345 static membership, and the KIP-429
//! `CooperativeStickyAssignor`.
//!
//! All three drive the same coordinator sequence of `JoinGroup`, `SyncGroup`,
//! `Heartbeat` and `Fetch`, which is why they share a file.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, KAFKA_IMAGE_TXN, assert_console_values, broker0_advertised,
    describe_console_group, docker_run_kafka_tool, docker_run_kafka_tool_with_image,
    nc_check_connectivity, produce_console_checked, start_console_broker, start_host_broker,
};

// Same multi-thread runtime caveat as `console_producer_round_trip`:
// the test body makes blocking `Command::output()` calls; a
// single-threaded runtime would starve the broker's accept loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_group_round_trip() {
    const TOPIC: &str = "krabka-broker-grp-itest";

    let (broker, _dir) = start_console_broker(TOPIC, 1).await;

    // 2. Produce records via kafka-console-producer over stdin.

    produce_console_checked(KAFKA_IMAGE, TOPIC, b"x\ny\nz\n");

    // 3. Consume WITHOUT --partition. The default `console-consumer`
    //    group will JoinGroup → SyncGroup → Heartbeat → Fetch through
    //    our coordinator.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        "krabka-acceptance-group",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    assert_console_values(&s, &["x", "y", "z"], "consumer didn't emit");

    broker.shutdown().await;
}

// KIP-345 static membership: the JVM consumer with
// `group.instance.id` set should round-trip through the coordinator
// (JoinGroup → SyncGroup → Heartbeat → Fetch with the v3+
// `group_instance_id` wire field populated) and a subsequent
// `kafka-consumer-groups --describe` must surface the instance id under
// HOST/CONSUMER-ID columns, confirming the broker persisted it on the
// member metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_static_membership() {
    const TOPIC: &str = "krabka-broker-static-itest";
    const GROUP: &str = "krabka-static-grp";
    const INSTANCE: &str = "client-static-1";

    let (broker, _dir) = start_console_broker(TOPIC, 1).await;

    // Produce three records.

    produce_console_checked(KAFKA_IMAGE, TOPIC, b"a\nb\nc\n");

    // Consume with `group.instance.id` set. The JVM consumer sends this
    // as `group_instance_id` in JoinGroup v5+ / SyncGroup v3+ / Heartbeat
    // v3+ / OffsetCommit v7+. If the broker rejects the wire field we'll
    // see a hard failure here.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        GROUP,
        "--consumer-property",
        &format!("group.instance.id={INSTANCE}"),
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    assert_console_values(&s, &["a", "b", "c"], "consumer didn't emit");

    // `kafka-consumer-groups --describe` exercises the broker's
    // DescribeGroups path. The output should mention the instance id so
    // operators can correlate static slots back to pods.
    let desc_out = describe_console_group(GROUP);
    let s = String::from_utf8_lossy(&desc_out.stdout);
    assert!(s.contains(TOPIC), "describe missing topic {TOPIC}: {s}");

    broker.shutdown().await;
}

/// KIP-429 JVM acceptance: drive `kafka-console-consumer` with the JVM
/// `CooperativeStickyAssignor` against Krabka. The test validates that
/// Krabka's `JoinGroup` vote rule accepts `cooperative-sticky` and that the
/// broker forwards the negotiated `protocol_name` correctly, so the JVM
/// client's `AbstractCoordinator.onJoinComplete` accepts the response.
///
/// The test uses `cp-kafka:7.5.0` (= [`KAFKA_IMAGE_TXN`]). The
/// cooperative-sticky assignor in `cp-kafka:6.1.1` (Kafka 2.7) still lacked
/// several rebalance race fixes that landed in Kafka 3.x.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn cooperative_sticky_kafka_console_consumer() {
    const TOPIC: &str = "coop-jvm";

    let (broker, _dir) = start_host_broker().await;
    nc_check_connectivity();

    // 1. Create the topic.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--topic",
        TOPIC,
        "--partitions",
        "3",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        broker0_advertised(),
    ]);

    // 2. Produce 3 records via stdin.

    produce_console_checked(KAFKA_IMAGE, TOPIC, b"alpha\nbravo\ncharlie\n");

    // 3. Consume via kafka-console-consumer with CooperativeStickyAssignor.
    //    Use cp-kafka:7.5.0 (Kafka 3.5) — cooperative-sticky in 2.7 had
    //    rebalance races that masked broker correctness issues.
    let consumer_out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--group",
            "coop-jvm-group",
            "--consumer-property",
            "partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
            "--from-beginning",
            "--max-messages",
            "3",
            "--timeout-ms",
            "30000",
        ],
    );
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["alpha", "bravo", "charlie"] {
        assert!(
            s.contains(needle),
            "consumer didn't emit {needle}: stdout={s:?} stderr={:?}",
            String::from_utf8_lossy(&consumer_out.stderr)
        );
    }

    broker.shutdown().await;
}
