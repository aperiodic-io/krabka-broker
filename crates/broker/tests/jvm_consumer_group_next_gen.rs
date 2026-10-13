//! JVM-acceptance tests for KIP-848. They drive the GA Kafka 4.0 client
//! against an in-process Krabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.

mod support;

use {assert2::assert, support::jvm_docker_run as docker_run};

/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str { &support::jvm_listeners().advertised }

const KAFKA_IMAGE_NEXT_GEN: &str = "mirror.gcr.io/apache/kafka:4.0.0";
/// Kafka 4.3.1 is the oracle for broker-side subscription regexes: from 4.1 the
/// console consumer subscribes with `SubscriptionPattern` when
/// `group.protocol=consumer`, so `--include` reaches the broker as the
/// heartbeat's `SubscribedTopicRegex` instead of being compiled client-side.
const KAFKA_IMAGE_CLASSIC: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";

async fn start_host_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    support::start_jvm_single("krabka_broker=info,info", |_| {}).await
}
/// Pre-create a topic with the classic admin tooling. Krabka's broker does
/// not auto-create topics on the produce path, so tests must create them
/// explicitly. This matches the existing `jvm_acceptance.rs` convention.
fn create_topic(name: &str, partitions: i32) {
    let out = docker_run(KAFKA_IMAGE_CLASSIC, &[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        bootstrap_addr(),
        "--topic",
        name,
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        "1",
    ]);
    assert!(
        out.status.success(),
        "create topic {name} failed: stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Spawn a console-consumer container on a blocking thread and return a handle
/// that resolves to its stdout. This runs two overlapping consumers in the same
/// group. The caller awaits both handles, so the containers run concurrently
/// and not back-to-back. `--add-host` mirrors `docker_run`, so the
/// container can reach the host-process broker at `host.docker.internal`.
fn spawn_consumer(image: &'static str, script: String) -> tokio::task::JoinHandle<String> {
    tokio::task::spawn_blocking(move || {
        let out = std::process::Command::new("docker")
            .arg("run")
            .arg("--rm")
            .arg("--add-host=host.docker.internal:host-gateway")
            .arg(image)
            .arg("bash")
            .arg("-c")
            .arg(&script)
            .output()
            .expect("docker run");
        eprintln!(
            "KRABKA[test] consumer {image} status={} stderr={}",
            out.status,
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    })
}

/// Extract the set of partition numbers from console-consumer stdout produced
/// with `--property print.partition=true`. The `DefaultMessageFormatter`
/// emits one `Partition:<n>` token per record line, for example
/// `Partition:2\t<value>` when it prints no key. This function tolerates any
/// surrounding columns and pulls the integer after each `Partition:` marker.
fn parse_partitions(stdout: &str) -> std::collections::BTreeSet<i32> {
    let mut set = std::collections::BTreeSet::new();
    for line in stdout.lines() {
        for token in line.split(['\t', ' ']) {
            if let Some(rest) = token.strip_prefix("Partition:")
                && let Ok(n) = rest.trim().parse::<i32>()
            {
                set.insert(n);
            }
        }
    }
    set
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_single_consumer_round_trip() {
    let bootstrap = bootstrap_addr();
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-rt", 1);
    let produced = docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf 'a\\nb\\nc\\n' | kafka-console-producer --bootstrap-server {bootstrap} --topic kip848-rt --producer-property max.block.ms=10000"
        ),
    ]);
    assert!(produced.status.success(), "producer failed: {produced:?}");

    let consumed = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} --topic kip848-rt --group g-rt --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 3"
        ),
    ]);
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    assert!(
        stdout.contains('a') && stdout.contains('b') && stdout.contains('c'),
        "expected a/b/c, got {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_describe_group() {
    let bootstrap = bootstrap_addr();
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-d", 1);
    docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf '1\\n2\\n' | kafka-console-producer --bootstrap-server {bootstrap} --topic kip848-d"
        ),
    ]);
    let _ = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} --topic kip848-d --group g-d --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 2"
        ),
    ]);
    let described = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --describe --group g-d"
        ),
    ]);
    let stdout = String::from_utf8_lossy(&described.stdout);
    assert!(
        stdout.contains("g-d"),
        "expected group g-d in describe output, got {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_delete_group() {
    let bootstrap = bootstrap_addr();
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-del", 1);
    docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf 'x\\n' | kafka-console-producer --bootstrap-server {bootstrap} --topic kip848-del"
        ),
    ]);
    let _ = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} --topic kip848-del --group g-del --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 1"
        ),
    ]);
    let deleted = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --delete --group g-del"
        ),
    ]);
    assert!(deleted.status.success(), "delete failed: {deleted:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_coexists_with_classic() {
    let bootstrap = bootstrap_addr();
    let (_broker, _dir) = start_host_broker().await;
    create_topic("kip848-coex", 1);
    docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "printf 'p\\nq\\n' | kafka-console-producer --bootstrap-server {bootstrap} --topic kip848-coex"
        ),
    ]);
    let classic = docker_run(KAFKA_IMAGE_CLASSIC, &[
        "bash",
        "-c",
        &format!(
            "kafka-console-consumer --bootstrap-server {bootstrap} --topic kip848-coex --group g-classic --from-beginning --timeout-ms 10000 --max-messages 2"
        ),
    ]);
    let cs = String::from_utf8_lossy(&classic.stdout);
    assert!(cs.contains('p') && cs.contains('q'));

    let next_gen = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} --topic kip848-coex --group g-next --consumer-property group.protocol=consumer --from-beginning --timeout-ms 10000 --max-messages 2"
        ),
    ]);
    let ns = String::from_utf8_lossy(&next_gen.stdout);
    assert!(ns.contains('p') && ns.contains('q'));
}

/// Migration interop within a *single* consumer group, deterministically.
///
/// Phase 1: a classic (cp-kafka 7.4.0) consumer forms group `g-migrate` and
/// drains batch 1. That proves the classic protocol serves the group. Phase
/// 2: a next-gen (apache/kafka 4.0.0, `group.protocol=consumer`) consumer joins
/// the SAME group and drains a freshly-produced batch 2. Krabka's unified
/// coordinator runs the default `Bidirectional` policy with the consumer
/// rebalance protocol enabled, from `NextGenConfig::default`, because
/// `start_host_broker` does not override it. The consumer protocol therefore
/// serves the group in place, and the next-gen member reads batch 2 from the
/// offsets that the classic member committed. Both protocols work against the
/// same group, with offset continuity across the migration.
///
/// Each phase runs a SOLE member that owns all partitions, so the assignment is
/// fixed and there is no concurrency or rebalance race. An earlier concurrent
/// design flaked on CI, because the lone classic member drained every record
/// and committed offsets before the next-gen member joined, which starved it.
/// The in-process suite in `coordinator::unified` covers the live, concurrent
/// mixed-membership split deterministically: upgrade, downgrade, round-trip,
/// gap-free assignment, static membership, and committed-offset survival.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kip848_classic_and_consumer_in_one_group_migrate() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    create_topic("mig", 4);
    let group = "g-migrate";
    let all: std::collections::BTreeSet<i32> = (0..4).collect();

    // Produce one deterministic batch of 8 records (2 per partition). Kafka's
    // default partitioner is murmur2(key) % numPartitions, so keyed records land
    // on a fixed partition: "0"->0, "4"->1, "5"->2, "1"->3.
    let produce = |label: &str| {
        let out = docker_run(KAFKA_IMAGE_CLASSIC, &[
            "bash",
            "-c",
            &format!(
                "printf '0:a\\n4:b\\n5:c\\n1:d\\n0:e\\n4:f\\n5:g\\n1:h\\n' | \
                     kafka-console-producer --bootstrap-server {bootstrap} --topic mig \
                     --property parse.key=true --property key.separator=: \
                     --producer-property max.block.ms=15000"
            ),
        ]);
        assert!(out.status.success(), "{label} producer failed: {out:?}");
    };

    // Phase 1 — classic consumer drains batch 1 from all four partitions.
    produce("batch1");
    let classic_out = spawn_consumer(
        KAFKA_IMAGE_CLASSIC,
        format!(
            "kafka-console-consumer --bootstrap-server {bootstrap} --topic mig --group {group} \
             --from-beginning --property print.partition=true --timeout-ms 25000 --max-messages 8"
        ),
    )
    .await
    .unwrap();
    eprintln!("KRABKA[test] classic stdout:\n{classic_out}");
    let cp = parse_partitions(&classic_out);
    assert!(
        cp == all,
        "classic consumer must cover all partitions: {cp:?}\nstdout: {classic_out}"
    );

    // Phase 2 — a next-gen consumer joins the SAME group (in-place migration to
    // the consumer protocol) and drains batch 2 from the classic-committed
    // offsets, across all four partitions.
    produce("batch2");
    let nextgen_out = spawn_consumer(
        KAFKA_IMAGE_NEXT_GEN,
        format!(
            "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server {bootstrap} --topic mig \
             --group {group} --consumer-property group.protocol=consumer --from-beginning \
             --property print.partition=true --timeout-ms 25000 --max-messages 8"
        ),
    )
    .await
    .unwrap();
    eprintln!("KRABKA[test] nextgen stdout:\n{nextgen_out}");
    let np = parse_partitions(&nextgen_out);
    assert!(
        np == all,
        "next-gen consumer must cover all partitions after the migration: {np:?}\nstdout: {nextgen_out}"
    );

    // The migrated group must describe coherently to the JVM admin tooling.
    let describe = docker_run(KAFKA_IMAGE_NEXT_GEN, &[
        "bash",
        "-c",
        &format!(
            "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap} --describe --group {group}"
        ),
    ]);
    assert!(
        String::from_utf8_lossy(&describe.stdout).contains("mig"),
        "describe mentions topic mig: {}",
        String::from_utf8_lossy(&describe.stdout),
    );

    drop(broker);
}

// A `SubscribedTopicRegex` that does not compile is answered
// `INVALID_REGULAR_EXPRESSION` (128) before any member record is written, but
// there is no JVM-lane case for it: no stock JVM client sends an invalid
// pattern to the broker. `KafkaConsumer.subscribe(Pattern)` and
// `kafka-console-consumer --include` compile it locally with
// `java.util.regex`, so `apache/kafka:4.3.1` fails inside
// `ConsoleConsumer$ConsumerWrapper` with `PatternSyntaxException: Unclosed
// group` before opening a connection. Only a compiled 4.x application using
// the `SubscriptionPattern` overload sends the string through, and no image
// here carries both a JDK and the 4.x client jars. The broker behaviour is
// pinned over the wire instead, by
// `consumer_group_next_gen::an_invalid_subscribed_topic_regex_fails_the_heartbeat`,
// and in `coordinator::unified::actor::member_state`'s unit tests.
