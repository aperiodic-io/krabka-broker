//! Docker invocation of the Apache Kafka command-line tools.
//!
//! This module pins the cp-kafka images the suites run, wraps `docker run` so a
//! failed tool reports its captured output, and builds the host-side files that
//! those containers bind-mount.

use std::process::Stdio;

use assert2::assert;

use super::ports::host_port;

/// Create a topic through the JVM admin client, optionally with client properties.
pub(crate) fn create_console_topic(
    image: &str,
    mounts: &[&str],
    topic: &str,
    partitions: i32,
    replicas: i16,
) {
    let partitions = partitions.to_string();
    let replicas = replicas.to_string();
    let mut args = vec![
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        &partitions,
        "--replication-factor",
        &replicas,
        "--bootstrap-server",
        super::ports::broker0_advertised(),
    ];
    if !mounts.is_empty() {
        args.extend(["--command-config", "/client.properties"]);
    }
    docker_run_kafka_tool_with_image_and_mounts(image, mounts, &args);
}

/// Describe the topic overrides submitted by an application.
pub(crate) fn describe_console_topic_configs(bootstrap: &str, topic: &str) -> String {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--bootstrap-server",
            bootstrap,
        ],
    );
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    eprintln!("KRABKA[test] kafka-configs --describe {topic}:\n{text}");
    text
}

/// Create an application's single-partition topic at its own bootstrap endpoint.
pub(crate) fn create_console_topic_at(bootstrap: &str, topic: &str) {
    docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "1",
            "--replication-factor",
            "1",
            "--bootstrap-server",
            bootstrap,
        ],
    );
}

/// Produce the supplied console records, with the listener's optional client properties.
pub(crate) fn produce_console(
    image: &str,
    mounts: &[&str],
    topic: &str,
    legacy: bool,
    payload: &[u8],
) -> std::process::Output {
    let bootstrap_flag = if legacy {
        "--broker-list"
    } else {
        "--bootstrap-server"
    };
    let mut args = vec![
        "kafka-console-producer",
        bootstrap_flag,
        super::ports::broker0_advertised(),
        "--topic",
        topic,
    ];
    if !mounts.is_empty() {
        args.extend(["--producer.config", "/client.properties"]);
    }
    crate::support::jvm_stdin_output(
        &mut crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
            image,
            mounts,
            args: &args,
            input: crate::support::ContainerInput::Attached,
        }),
        payload,
    )
}

/// Run an authenticated topic round-trip and retain its properties file for the caller.
pub(crate) fn console_round_trip_with_props(
    image: &str,
    topic: &str,
    props: &str,
) -> ClientPropsFile {
    let props_file = write_client_props(props);
    let mount = props_file.mount_str();
    create_console_topic(image, &[&mount], topic, 1, 1);
    authenticated_console_round_trip(image, &[&mount], topic);
    props_file
}

/// Provision a TLS SCRAM user and its separate topic Read and Write ACLs.
pub(crate) fn provision_ssl_topic(
    admin: (&str, &str),
    user: (&str, &str),
    truststore_mount: &str,
    topic: &str,
    replicas: i16,
) -> (ClientPropsFile, ClientPropsFile) {
    let (admin_props, user_props) =
        provision_ssl_scram_sha512(admin.0, admin.1, user.0, user.1, truststore_mount);
    create_console_topic(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), truststore_mount],
        topic,
        1,
        replicas,
    );
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mounts(
            KAFKA_IMAGE_TXN,
            &[&admin_props.mount_str(), truststore_mount],
            &[
                "kafka-acls",
                "--add",
                "--allow-principal",
                &format!("User:{}", user.0),
                "--operation",
                op,
                "--topic",
                topic,
                "--bootstrap-server",
                super::ports::broker0_advertised(),
                "--command-config",
                "/client.properties",
            ],
        );
    }
    (admin_props, user_props)
}

/// Assert a console producer's two captured streams with the common diagnostic.
pub(crate) fn assert_console_produced(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A successful plain console produce, with its stderr-only diagnostic.
pub(crate) fn produce_console_checked(image: &str, topic: &str, payload: &[u8]) {
    let out = produce_console(image, &[], topic, false, payload);
    assert!(
        out.status.success(),
        "producer failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Exact expected values, independently supplied by the scenario that produced them.
pub(crate) fn assert_console_values(stdout: &str, expected: &[&str], label: &str) {
    for needle in expected {
        assert!(stdout.contains(needle), "{label} {needle}: {stdout:?}");
    }
}

/// One partition read, retaining the caller's timeout and extra consumer properties.
pub(crate) fn consume_console_partition(
    topic: &str,
    count: usize,
    timeout: u32,
    extra: &[&str],
) -> std::process::Output {
    let count = count.to_string();
    let timeout = timeout.to_string();
    let mut args = vec![
        "kafka-console-consumer",
        "--bootstrap-server",
        super::ports::broker0_advertised(),
        "--topic",
        topic,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        &count,
        "--timeout-ms",
        &timeout,
    ];
    args.extend_from_slice(extra);
    docker_run_kafka_tool(&args)
}

/// One group consumer read with the CLI's group-before-from-beginning ordering.
pub(crate) fn consume_console_group(
    topic: &str,
    group: &str,
    count: usize,
    timeout: u32,
) -> std::process::Output {
    let count = count.to_string();
    let timeout = timeout.to_string();
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        super::ports::broker0_advertised(),
        "--topic",
        topic,
        "--group",
        group,
        "--from-beginning",
        "--max-messages",
        &count,
        "--timeout-ms",
        &timeout,
    ])
}

/// Describe one consumer group through the common admin command.
pub(crate) fn describe_console_group(group: &str) -> std::process::Output {
    docker_run_kafka_tool(&[
        "kafka-consumer-groups",
        "--describe",
        "--group",
        group,
        "--bootstrap-server",
        super::ports::broker0_advertised(),
    ])
}

/// A numbered sequence of console records, with one record per line.
pub(crate) fn numbered_payload(prefix: &str, count: usize) -> String {
    use std::fmt::Write as _;

    (0..count).fold(String::new(), |mut payload, index| {
        writeln!(payload, "{prefix}-{index}").expect("write console record");
        payload
    })
}

/// PLAIN properties for the JVM admin tools.
pub(crate) fn write_plain_props(user: &str, password: &str) -> ClientPropsFile {
    write_client_props(&plain_client_properties(user, password))
}

/// `SASL_SSL` properties with the shared JVM truststore and optional non-idempotent producer.
pub(crate) fn write_ssl_sasl_props(
    mechanism: &str,
    jaas: &str,
    non_idempotent: bool,
) -> ClientPropsFile {
    let producer = if non_idempotent {
        "enable.idempotence=false\nacks=1\n"
    } else {
        ""
    };
    write_client_props(&format!(
        "security.protocol=SASL_SSL\nsasl.mechanism={mechanism}\nsasl.jaas.config={jaas}\nssl.truststore.location=/truststore.jks\nssl.truststore.password=changeit\nssl.endpoint.identification.algorithm=\n{producer}"
    ))
}

/// Provision the shared TLS SCRAM user and return both JVM clients' properties.
pub(crate) fn provision_ssl_scram_sha512(
    admin: &str,
    admin_password: &str,
    user: &str,
    password: &str,
    truststore_mount: &str,
) -> (ClientPropsFile, ClientPropsFile) {
    let admin_props = write_ssl_sasl_props(
        "PLAIN",
        &super::sasl::plain_jaas(admin, admin_password),
        false,
    );
    provision_console_scram(
        KAFKA_IMAGE_TXN,
        &[&admin_props.mount_str(), truststore_mount],
        user,
        password,
        "SCRAM-SHA-512",
    );
    let user_props = write_ssl_sasl_props(
        "SCRAM-SHA-512",
        &super::sasl::scram_jaas(user, password),
        true,
    );
    (admin_props, user_props)
}

/// Provision one SCRAM credential through the typed JVM admin API.
pub(crate) fn provision_console_scram(
    image: &str,
    mounts: &[&str],
    user: &str,
    password: &str,
    mechanism: &str,
) {
    docker_run_kafka_tool_with_image_and_mounts(
        image,
        mounts,
        &scram_credential_args(user, &format!("{mechanism}=[password={password}]")),
    );
}

/// Add the named operations to one principal's topic or group ACL.
pub(crate) fn add_console_acl(
    image: &str,
    mounts: &[&str],
    principal: &str,
    operations: &[&str],
    resource_flag: &str,
    resource: &str,
) {
    let mut args = vec![
        "kafka-acls",
        "--bootstrap-server",
        super::ports::broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        principal,
    ];
    for operation in operations {
        args.extend(["--operation", operation]);
    }
    args.extend([resource_flag, resource]);
    docker_run_kafka_tool_with_image_and_mounts(image, mounts, &args);
}

/// Consume one partition or group with the console client's optional properties.
pub(crate) fn consume_console(
    image: &str,
    mounts: &[&str],
    topic: &str,
    group: Option<&str>,
    count: usize,
    timeout_ms: u32,
    require_success: bool,
) -> std::process::Output {
    let count = count.to_string();
    let timeout = timeout_ms.to_string();
    let mut args = vec!["kafka-console-consumer"];
    if image == KAFKA_IMAGE_LEGACY {
        args.push("--new-consumer");
    }
    args.extend([
        "--bootstrap-server",
        super::ports::broker0_advertised(),
        "--topic",
        topic,
    ]);
    args.extend(group.map_or(["--partition", "0"], |group| ["--group", group]));
    args.extend([
        "--from-beginning",
        "--max-messages",
        &count,
        "--timeout-ms",
        &timeout,
    ]);
    if !mounts.is_empty() {
        args.extend(["--consumer.config", "/client.properties"]);
    }
    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image,
        mounts,
        args: &args,
        ..Default::default()
    })
    .output()
    .expect("console consumer");
    if require_success {
        assert!(
            out.status.success(),
            "console consumer failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    out
}

/// The authenticated ten-record console round-trip used by the SASL mechanisms.
pub(crate) fn authenticated_console_round_trip(image: &str, mounts: &[&str], topic: &str) {
    let out = produce_console(
        image,
        mounts,
        topic,
        false,
        numbered_payload("msg", 10).as_bytes(),
    );
    assert!(
        out.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let out = consume_console(image, mounts, topic, None, 10, 20_000, true);
    let stdout = String::from_utf8_lossy(&out.stdout);
    for index in 0..10 {
        let needle = format!("msg-{index}");
        assert!(
            stdout.contains(&needle),
            "consumer missing {needle}: {stdout:?}"
        );
    }
}

/// Address the Kafka CLI containers use for bootstrap AND that the broker
/// advertises in `Metadata`. [`docker_run_kafka_tool`] resolves it with
/// `--add-host=host.docker.internal:host-gateway`.
/// Bind to all interfaces so the Docker bridge can reach the broker at the
/// host gateway IP.
pub(crate) const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";

/// Newer Kafka image for tests that need tools or client APIs not bundled in
/// [`KAFKA_IMAGE`]. These tests use it:
///
/// - `kafka_cluster_describe`: `cp-kafka:6.1.1` has no `kafka-cluster`
///   binary, but `cp-kafka:7.5.0` has one.
///
/// - `transactional_console_producer_eos`: the image includes `javac` and the
///   Kafka 3.5 client jars used by the transactional Java helper.
pub(crate) const KAFKA_IMAGE_TXN: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";

/// Kafka 0.10.1 console tools from Confluent Platform 3.1.2. The
/// legacy-client acceptance tests (`jvm_legacy_010_*`) use them. The
/// 0.10.x-era producer emits v1 `MessageSet` records by default, with
/// KIP-32 per-message timestamps. The consumer negotiates Fetch v0–3. This
/// image exercises the broker's `kafka_3_6_2`-namespace handlers and the
/// legacy record up/down-conversion paths (#226).
pub(crate) const KAFKA_IMAGE_LEGACY: &str = "mirror.gcr.io/confluentinc/cp-kafka:3.1.2";

/// `KIP-405` topic configs (`remote.storage.enable`, `local.retention.bytes`)
/// landed in Apache Kafka 3.6 / Confluent Platform 7.6. The default
/// [`KAFKA_IMAGE`] (`mirror.gcr.io/confluentinc/cp-kafka:6.1.1` / Kafka 2.7)
/// and [`KAFKA_IMAGE_TXN`] (`mirror.gcr.io/confluentinc/cp-kafka:7.5.0` /
/// Kafka 3.5) both predate KIP-405. Their `TopicCommand` client validates
/// `--config` keys against the local `LogConfig.configNames` set and rejects
/// unknown ones before it sends the `CreateTopics` request, so the
/// tiered-storage test cannot reuse them.
/// `mirror.gcr.io/confluentinc/cp-kafka:7.8.8` ships Kafka 3.8, where
/// KIP-405 is GA.
pub(crate) const KAFKA_IMAGE_TIERED: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.8.8";

/// KIP-966 needs a `DescribeTopicPartitions` client and a `TopicCommand` that
/// renders the `Elr:` / `LastKnownElr:` columns. Neither [`KAFKA_IMAGE`] (Kafka
/// 2.7) nor [`KAFKA_IMAGE_TXN`] (Kafka 3.5) has either: their `kafka-topics`
/// still describes topics through a `Metadata` fan-out, and Kafka's `Metadata`
/// schema carries no ELR field in any version.
/// `mirror.gcr.io/apache/kafka:4.3.1` is the same image
/// `jvm_kip320_divergence` uses as a modern `AdminClient`, and it doubles as
/// the JVM broker the ELR columns are compared against. Its tools are not on
/// `PATH`; call them by their `/opt/kafka/bin` path.
pub(crate) const KAFKA_IMAGE_ELR: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// Verify TCP connectivity from inside a bridge-network container with
/// `--add-host=host.docker.internal:host-gateway`.
pub(crate) fn nc_check_connectivity() {
    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: "alpine",
        args: &[
            "sh",
            "-c",
            &format!(
                "apk add --no-cache netcat-openbsd >/dev/null 2>&1 && nc -zv {} {}",
                "host.docker.internal",
                host_port()
            ),
        ],
        ..Default::default()
    })
    .output()
    .expect("spawn nc check");
    eprintln!(
        "NC CHECK status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run `docker run --rm --add-host=host.docker.internal:host-gateway
/// <image> <args...>` and assert that it succeeds.
pub(crate) fn docker_run_kafka_tool(args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image(KAFKA_IMAGE, args)
}

/// Like [`docker_run_kafka_tool`] but lets the caller choose the image.
/// Use it when a test needs a newer image. For example, `cp-kafka:7.5.0`
/// bundles `kafka-cluster` and `6.1.1` does not.
pub(crate) fn docker_run_kafka_tool_with_image(image: &str, args: &[&str]) -> std::process::Output {
    let out = docker_run_kafka_tool_allowing_failure_with_image(image, args);
    assert!(
        out.status.success(),
        "docker run image={image} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Run a Kafka CLI tool the way [`docker_run_kafka_tool`] does, and hand back
/// what it did without asserting that it succeeded.
///
/// A case about a refusal needs this: the JVM tool exits non-zero and prints
/// the broker's error, which is the evidence, so the asserting helpers above
/// would fail the test on the very outcome it is checking. Pair it with
/// [`tool_output`] to search both streams as one text.
pub(crate) fn docker_run_kafka_tool_allowing_failure(args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_allowing_failure_with_image(KAFKA_IMAGE, args)
}

/// [`docker_run_kafka_tool_allowing_failure`] against a caller-chosen image.
pub(crate) fn docker_run_kafka_tool_allowing_failure_with_image(
    image: &str,
    args: &[&str],
) -> std::process::Output {
    crate::support::jvm_tool_output(image, args, "test")
}

/// A finished tool's stdout followed by its stderr, as one text.
///
/// The JVM tools split a failure across the two streams in ways that differ
/// per tool -- `kafka-configs` prints its own summary line on stdout and the
/// exception on stderr -- so a case about a refusal searches both.
pub(crate) fn tool_output(out: &std::process::Output) -> String {
    crate::support::combined_output(out)
}

pub(crate) const TRANSACTIONAL_PRODUCER_JAVA: &str = krabka_macros::java_string_producer_source!(
    r#"

public final class TransactionalProducer {
  public static void main(String[] args) throws Exception {
"#,
    "args[0]",
    r#"    config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "eos-tid");

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      producer.initTransactions();
      producer.beginTransaction();
      for (int i = 0; i < 6; i++) {
        producer.send(new ProducerRecord<>(args[1], "committed-" + i)).get();
      }
      producer.commitTransaction();

      // A second transaction on the same producer instance. A client below
      // EndTxn v5 (this cp-kafka 7.5.0 client speaks v3) keeps its epoch across
      // transactions, so the coordinator of a transaction.version 2 cluster must
      // not bump it on completion: the next AddPartitionsToTxn at the old epoch
      // would be fenced, and the producer would fail with ProducerFencedException.
      producer.beginTransaction();
      for (int i = 0; i < 2; i++) {
        producer.send(new ProducerRecord<>(args[1], "aborted-" + i)).get();
      }
      producer.abortTransaction();
    }

    config.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      producer.send(new ProducerRecord<>(args[1], "after-abort")).get();
    }
    System.out.println("TXNPROBE OK");
  }
}
"#
);

/// A compiled `KafkaStreams` topology, for the suite that runs the real
/// Streams runtime against krabka (`tests/jvm_streams_app.rs`).
///
/// The shape is chosen so that every internal-topic contract KIP-1071 and the
/// Streams runtime put on a broker is exercised by one run:
///
/// - `selectKey` then an explicitly named `repartition()`, so
///   `InternalTopicManager` creates `<application.id>-shuffle-repartition`
///   with `RepartitionTopicConfig`'s overrides (`cleanup.policy=delete`,
///   `retention.ms=-1`, `segment.bytes=52428800`).
/// - a time-windowed `count()` with the record cache disabled, so every
///   update is forwarded rather than the last per key per commit interval,
///   and with the window store named `krabka-counts` -- the name
///   `jvm_streams_app`'s `WINDOW_STORE_NAME` looks for -- so the changelog is
///   `<application.id>-krabka-counts-changelog`, created with
///   `WindowedChangelogTopicConfig`'s `cleanup.policy=compact,delete`. An
///   unnamed store would be `KSTREAM-AGGREGATE-STATE-STORE-<n>`; this one is
///   named because it has to be asked for in memory.
/// - `processing.guarantee=exactly_once_v2`, so the single stream thread runs
///   one transactional producer and every sink and changelog write is inside
///   a transaction the consumer only sees under `read_committed`.
///
/// Both configs also carry `message.timestamp.type=CreateTime` from
/// `InternalTopicConfig.INTERNAL_TOPIC_DEFAULT_OVERRIDES`, which is the
/// registration KIP-1071's internal topics depend on.
///
/// The app seeds its own input with a plain producer at FIXED record
/// timestamps before it starts the topology. That is what makes the expected
/// output exact: every seeded record lands in the same window, so the emitted
/// running counts are a function of the input order alone and not of when the
/// test happened to run.
///
/// Arguments: `<bootstrap> <input topic> <output topic> <application.id>
/// [<group.protocol>]`. The fifth argument is set as the raw `group.protocol`
/// key rather than through a `StreamsConfig` constant, because the class is
/// compiled against Kafka 3.5 jars, which have no KIP-1071 constant, and is
/// then also run against 4.3 jars, which do.
///
/// It prints `STREAMSPROBE OK` once the topology has emitted every expected
/// output record, and `STREAMSPROBE TIMEOUT remaining=<n>` (exit 1) if it has
/// not within the latch budget.
pub(crate) const STREAMS_APP_JAVA: &str = krabka_macros::java_string_producer_source!(
    r#"
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;

public final class StreamsApp {
  // A fixed record timestamp for every seeded input record. The window is ten
  // minutes wide and epoch-aligned, so all five records fall in the one window
  // whatever the wall clock says when the suite runs.
  private static final long BASE_TIMESTAMP = 1700000000000L;
  private static final Duration WINDOW = Duration.ofMinutes(10);
  private static final Duration RETENTION = Duration.ofMinutes(30);
  private static final List<String> WORDS =
      List.of("alpha", "alpha", "beta", "alpha", "beta");

  public static void main(String[] args) throws Exception {
    final String bootstrap = args[0];
    final String input = args[1];
    final String output = args[2];
    final String applicationId = args[3];
    final String groupProtocol = args.length > 4 ? args[4] : "classic";

    seed(bootstrap, input);

    final CountDownLatch emitted = new CountDownLatch(WORDS.size());

    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
        .selectKey((key, value) -> value)
        .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
            .withName("shuffle")
            .withNumberOfPartitions(1))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW))
        // An in-memory window store, not the default RocksDB one: the Apache
        // image is JRE-only and carries no `libstdc++`, so `librocksdbjni`
        // cannot load there and the topology would die on its first commit.
        // The store choice is a client-side one and changes nothing krabka
        // sees: a windowed store is still logged, so the changelog topic is
        // still created with `cleanup.policy=compact,delete`, which is what
        // this suite reads back.
        .count(Materialized.<String, Long>as(
                Stores.inMemoryWindowStore("krabka-counts", RETENTION, WINDOW, false))
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long()))
        .toStream()
        .map((windowedKey, count) ->
            new KeyValue<>(windowedKey.key(), windowedKey.key() + ":" + count))
        .peek((key, value) -> emitted.countDown())
        .to(output, Produced.with(Serdes.String(), Serdes.String()));

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
    config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
    // Forward every update rather than the last one per key per commit. The
    // record cache is 10MB by default, which collapses the running counts
    // this suite reads back into one record per key -- `alpha:3`, `beta:2` --
    // and the five expected outputs would never all arrive. The raw key is
    // used rather than the `StreamsConfig` constant because the class is
    // compiled against Kafka 3.5 jars and also runs against 4.3 ones.
    config.put("statestore.cache.max.bytes", 0);
    config.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/krabka-streams-state/" + applicationId);
    if (!"classic".equals(groupProtocol)) {
      config.put("group.protocol", groupProtocol);
    }

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    streams.setUncaughtExceptionHandler(error -> {
      System.err.println("STREAMSPROBE uncaught " + error);
      error.printStackTrace();
      return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
    });
    streams.start();
    boolean complete = emitted.await(180, TimeUnit.SECONDS);
    streams.close(Duration.ofSeconds(60));
    if (complete) {
      System.out.println("STREAMSPROBE OK");
    } else {
      System.out.println("STREAMSPROBE TIMEOUT remaining=" + emitted.getCount());
      System.exit(1);
    }
  }

  // Seed the input topic at fixed timestamps, with a plain producer, before
  // the topology starts. `auto.offset.reset` defaults to `earliest` under
  // Streams, so the run sees every one of them.
  private static void seed(String bootstrap, String input) throws Exception {
"#,
    "bootstrap",
    r#"    try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
      for (int i = 0; i < WORDS.size(); i++) {
        producer.send(new ProducerRecord<>(
            input, null, BASE_TIMESTAMP + i, "seed-" + i, WORDS.get(i))).get();
      }
    }
    System.out.println("STREAMSPROBE seeded=" + WORDS.size());
  }
}
"#
);

/// Write `props` to a `tempfile::NamedTempFile` and chmod it to `0644` on
/// unix, so the non-root user of the cp-kafka container can read it once it
/// is bind-mounted. `tempfile` creates files `0600` by default, which causes
/// a silent `IOException: client.properties (Permission denied)` inside the
/// JVM tool. The returned object holds the tempfile open. Drop it after the
/// last docker invocation that needs the mount.
pub(crate) fn write_client_props(props: &str) -> ClientPropsFile {
    let tmp = tempfile::NamedTempFile::new().expect("tmp");
    std::fs::write(tmp.path(), props).expect("write props");
    #[cfg(unix)]
    {
        crate::support::chmod_for_container(tmp.path(), 0o644, "chmod props");
    }
    ClientPropsFile { tmp }
}

/// Owns a `client.properties` tempfile + builds the `-v` mount spec for it.
pub(crate) struct ClientPropsFile {
    tmp: tempfile::NamedTempFile,
}

impl ClientPropsFile {
    /// `<host_path>:/client.properties:ro`, the second positional argument
    /// to `docker run -v`. Inside the container the file is always at
    /// `/client.properties`, so JVM tool flags can use a fixed path.
    pub(crate) fn mount_str(&self) -> String {
        format!("{}:/client.properties:ro", self.tmp.path().display())
    }
}

/// Run a cp-kafka tool with an extra `-v <mount>` bind. Otherwise identical
/// to [`docker_run_kafka_tool`]: it asserts success and captures
/// stdout+stderr.
pub(crate) fn docker_run_kafka_tool_with_mount(mount: &str, args: &[&str]) -> std::process::Output {
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE, mount, args)
}

/// Like [`docker_run_kafka_tool_with_mount`] but lets the caller choose the
/// image. The SCRAM-SHA-512 acceptance test uses it and needs
/// `cp-kafka:7.5.0`, because `kafka-configs --alter --entity-type users` on
/// `cp-kafka:6.1.1` (Kafka 2.7) sends `IncrementalAlterConfigs (api_key 44)`
/// rather than `AlterUserScramCredentials (51)`. Kafka 3.5+ uses the typed
/// KIP-554 request, which is what the broker implements.
pub(crate) fn docker_run_kafka_tool_with_image_and_mount(
    image: &str,
    mount: &str,
    args: &[&str],
) -> std::process::Output {
    let out = crate::support::docker_tool_command(
        image,
        &["-v", mount, "--add-host=host.docker.internal:host-gateway"],
    )
    .args(args)
    .output()
    .expect("spawn docker run");
    eprintln!(
        "KRABKA[test] docker_run image={image} mount={mount} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mount={mount} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// Like [`docker_run_kafka_tool_with_image_and_mount`] but supports multiple
/// bind mounts. The `SASL_SSL` test needs this, because it mounts both a
/// `client.properties` file and a JKS truststore into the same container.
pub(crate) fn docker_run_kafka_tool_with_image_and_mounts(
    image: &str,
    mounts: &[&str],
    args: &[&str],
) -> std::process::Output {
    let mut options = Vec::new();
    for mount in mounts {
        options.extend(["-v", *mount]);
    }
    options.push("--add-host=host.docker.internal:host-gateway");
    let mut cmd = crate::support::docker_tool_command(image, &options);
    cmd.args(args);
    let out = cmd.output().expect("spawn docker run");
    eprintln!(
        "KRABKA[test] docker_run image={image} mounts={mounts:?} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len(),
    );
    assert!(
        out.status.success(),
        "docker run image={image} mounts={mounts:?} {args:?} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

// ---------------------------------------------------------------------------
// Helper: write an arbitrary tempfile and return a TempFileMount that owns
// the NamedTempFile (so it stays alive as long as the returned value is alive)
// and exposes the host path for Docker `-v` mount specs.
// ---------------------------------------------------------------------------

pub(crate) struct TempFileMount {
    tmp: tempfile::NamedTempFile,
}

impl TempFileMount {
    /// `<host_path>:<container_path>`. The caller appends `:ro` if it wants
    /// a read-only mount.
    pub(crate) fn host_path(&self) -> String {
        self.tmp.path().display().to_string()
    }
}

pub(crate) fn write_temp_file(filename: &str, contents: &str) -> TempFileMount {
    let tmp = tempfile::Builder::new()
        .prefix(filename)
        .tempfile()
        .expect("tempfile");
    std::fs::write(tmp.path(), contents).expect("write tempfile");
    #[cfg(unix)]
    {
        crate::support::chmod_for_container(tmp.path(), 0o644, "chmod tempfile");
    }
    TempFileMount { tmp }
}

/// Verify the reassignment and let the JVM tool clear its throttle settings.
pub(crate) fn verify_console_reassignment(
    admin_mount: &str,
    json_mount: &str,
) -> std::process::Output {
    docker_run_kafka_tool_with_image_and_mounts(
        KAFKA_IMAGE_TXN,
        &[admin_mount, json_mount],
        &[
            "kafka-reassign-partitions",
            "--verify",
            "--reassignment-json-file",
            "/reassignment.json",
            "--bootstrap-server",
            super::ports::broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    )
}

/// PLAIN client configuration, also used by oracle-owned mounted files.
pub(crate) fn plain_client_properties(user: &str, password: &str) -> String {
    format!(
        "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config={}\n",
        super::sasl::plain_jaas(user, password)
    )
}

/// Describe a configured admin entity through the authenticated JVM tool.
pub(crate) fn describe_console_entity(
    mount: &str,
    entity: &str,
    name: &str,
) -> std::process::Output {
    let desc = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        mount,
        &[
            "kafka-configs",
            "--describe",
            "--entity-type",
            entity,
            "--entity-name",
            name,
            "--bootstrap-server",
            super::ports::broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        desc.status.success(),
        "describe failed: {}",
        String::from_utf8_lossy(&desc.stderr)
    );
    desc
}

/// Topic override command shared by the topic-config scenarios.
pub(crate) fn alter_console_topic_config(topic: &str, config: &str) {
    docker_run_kafka_tool(&[
        "kafka-configs",
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        topic,
        "--add-config",
        config,
        "--bootstrap-server",
        super::ports::broker0_advertised(),
    ]);
}

/// A one-record authenticated group consumer with the original denial-case timeout.
pub(crate) fn denied_console_consumer(
    mount: &str,
    topic: &str,
    group: &str,
    context: &str,
) -> std::process::Output {
    crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE_TXN,
        mounts: &[mount],
        args: &[
            "kafka-console-consumer",
            "--bootstrap-server",
            super::ports::broker0_advertised(),
            "--topic",
            topic,
            "--group",
            group,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "15000",
            "--consumer.config",
            "/client.properties",
        ],
        ..Default::default()
    })
    .stderr(Stdio::piped())
    .stdout(Stdio::piped())
    .output()
    .expect(context)
}

/// Provision a SCRAM credential through the single-mounted-file tool variant.
pub(crate) fn provision_plain_scram(mount: &str, user: &str, password: &str, mechanism: &str) {
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        mount,
        &scram_credential_args(user, &format!("{mechanism}=[password={password}]")),
    );
}

/// The older console-client image's rf=1 topic constructor at a supplied bootstrap.
pub(crate) fn create_plain_console_topic_at(bootstrap: &str, topic: &str) {
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        bootstrap,
    ]);
}

/// Read committed records through a selected broker, preserving the durability client's timeout.
pub(crate) fn consume_committed_at(
    bootstrap: &str,
    topic: &str,
    count: usize,
) -> std::process::Output {
    docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        &count.to_string(),
        "--timeout-ms",
        "20000",
    ])
}

fn scram_credential_args<'a>(user: &'a str, credential: &'a str) -> [&'a str; 12] {
    [
        "kafka-configs",
        "--alter",
        "--entity-type",
        "users",
        "--entity-name",
        user,
        "--add-config",
        credential,
        "--bootstrap-server",
        super::ports::broker0_advertised(),
        "--command-config",
        "/client.properties",
    ]
}
