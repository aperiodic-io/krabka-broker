//! A compiled Kafka Streams application, run against krabka.
//!
//! `jvm_streams_groups.rs` drives the KIP-1071 admin surface with the native
//! client and points the JVM `kafka-streams-groups.sh` tool at the result. It
//! cannot run a topology: `mirror.gcr.io/apache/kafka:4.1.0` is JRE-only. This
//! suite runs the real thing. [`STREAMS_APP_JAVA`] is compiled inside
//! [`KAFKA_IMAGE_TXN`], which ships `javac` beside the Kafka 3.5 jars, and the
//! resulting classes are then run against krabka.
//!
//! Two runs, one class file:
//!
//! - [`compiled_streams_app_runs_on_the_classic_protocol`] runs it in the same
//!   `cp-kafka:7.5.0` container it was compiled in, on the classic rebalance
//!   protocol. This is `StreamsPartitionAssignor` carrying Streams' own
//!   subscription and assignment userdata through `JoinGroup`/`SyncGroup`,
//!   `InternalTopicManager` creating the repartition and changelog topics, and
//!   -- under `processing.guarantee=exactly_once_v2` -- one transactional
//!   producer per stream thread writing every sink and changelog record inside
//!   a transaction.
//!
//! - [`compiled_streams_app_runs_on_the_streams_protocol`] mounts the same
//!   classes into `mirror.gcr.io/apache/kafka:4.3.1` and runs them with
//!   `group.protocol=streams`, which is KIP-1071: the app sends its topology in
//!   `StreamsGroupHeartbeat` rather than in an assignor's userdata. `cp-kafka:
//!   7.5.0` is Kafka 3.5 and has no such protocol, so the class has to cross
//!   images; it is compiled with `--release 11`, and `group.protocol` is set as
//!   a raw key, because Kafka 3.5 has no constant for it.
//!
//! The topology is `source -> selectKey -> repartition -> windowed count ->
//! sink`, so one run creates both internal topic shapes Kafka Streams asks a
//! broker for. What the suite then asserts on them is exactly what
//! `RepartitionTopicConfig`, `WindowedChangelogTopicConfig` and
//! `InternalTopicConfig` set:
//!
//! | topic | configs |
//! | --- | --- |
//! | `<app>-shuffle-repartition` | `cleanup.policy=delete`, `retention.ms=-1`, `message.timestamp.type=CreateTime` |
//! | `<app>-krabka-counts-changelog` | `cleanup.policy=compact,delete`, `message.timestamp.type=CreateTime` |
//!
//! Networking is the `jvm_acceptance` harness's: the broker binds an allocated
//! port on `0.0.0.0` and advertises `host.docker.internal:<port>`, which the
//! containers resolve through `--add-host=host.docker.internal:host-gateway`.
//!
//! Gated `#[ignore = "requires Docker"]`; the Bazel `docker` lane runs it with
//! `--ignored --test-threads=1`.

mod jvm_acceptance;
mod support;

use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use assert2::assert;
use jvm_acceptance::{
    KAFKA_IMAGE_TXN, STREAMS_APP_JAVA, broker0_advertised, docker_run_kafka_tool_with_image,
    rlmm_broker0_advertised, start_host_broker,
};

use crate::{
    jvm_acceptance::{create_console_topic_at, describe_console_topic_configs},
    support::client::connect_owned,
};

/// The Apache Kafka image whose jars carry KIP-1071. It is JRE-only, which is
/// why the class it runs is compiled elsewhere.
const KAFKA_IMAGE_STREAMS_PROTOCOL: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The classpath expression for the Confluent image, as
/// `jvm_acceptance_durability::transactional_eos` builds it.
const CP_CONFLUENT: &str = r"$(ls /usr/share/java/kafka/*.jar | tr '\n' ':')$(ls /usr/share/java/cp-base-new/*.jar | tr '\n' ':')";

/// The classpath expression for the Apache Kafka image, whose tarball layout
/// puts every jar in one directory.
const CP_APACHE: &str = r"$(ls /opt/kafka/libs/*.jar | tr '\n' ':')";

/// What the topology emits, in order, for the five seeded input records
/// (`alpha alpha beta alpha beta`, all at timestamps inside one ten-minute
/// window). A windowed `count()` with no suppression and the record cache
/// disabled emits the running count on every update, so the sink holds one
/// record per input record.
/// The window store the topology names in its `Materialized`, and so the
/// middle of its changelog topic's name. It is named because the topology asks
/// for an in-memory store; an unnamed one would be
/// `KSTREAM-AGGREGATE-STATE-STORE-<n>`.
const WINDOW_STORE_NAME: &str = "krabka-counts";

const EXPECTED_SINK: [&str; 5] = ["alpha:1", "alpha:2", "beta:1", "alpha:3", "beta:2"];

/// The JVM linkage failures that mean the Kafka-3.5-compiled class cannot run
/// against the 4.3 jars. The KIP-1071 case reports these rather than failing:
/// they say the cross-image compile did not work out, not that krabka
/// answered wrongly.
const LINKAGE_ERRORS: [&str; 4] = [
    "NoSuchMethodError",
    "NoClassDefFoundError",
    "NoSuchFieldError",
    "UnsupportedClassVersionError",
];

/// Compile [`STREAMS_APP_JAVA`] into `dest` on the host.
///
/// The compile happens inside [`KAFKA_IMAGE_TXN`] -- the only pinned image
/// that ships a JDK alongside Kafka jars -- with `dest` bind-mounted at
/// `/out`, so both runs below share one class file.
///
/// `--release 11` rather than a bare compile: the class has to load under the
/// Apache image's JRE 21 as well as run under the Confluent image's own JVM,
/// and 11 is the floor both Kafka 3.5 and Kafka 4.3 support. A class file
/// built for an older release loads on a newer JVM; the reverse does not.
fn compile_streams_app(dest: &Path) {
    let mut javac = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "-v",
            &format!("{}:/out", dest.display()),
            "--entrypoint",
            "bash",
            KAFKA_IMAGE_TXN,
            "-c",
            &format!(
                r#"set -e; cat >/tmp/StreamsApp.java; \
                   CP={CP_CONFLUENT}; \
                   javac --release 11 -cp "$CP" -d /out /tmp/StreamsApp.java; \
                   ls -l /out"#
            ),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn javac container");
    javac
        .stdin
        .as_mut()
        .expect("javac stdin")
        .write_all(STREAMS_APP_JAVA.as_bytes())
        .expect("write the Streams app source");
    drop(javac.stdin.take());
    let out = javac.wait_with_output().expect("wait for javac");
    eprintln!(
        "KRABKA[test] javac status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "compiling the Streams app failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Run the compiled class in `image`, with `classes` mounted read-only and
/// `classpath` naming where that image keeps its jars.
///
/// Both streams and stderr are echoed whatever the exit status: this is the
/// only place a Streams failure -- a refused internal topic, a transaction
/// that never initialised, an assignment that never arrived -- is written
/// down, and the caller decides what to make of it.
fn run_streams_app(
    image: &str,
    classes: &Path,
    classpath: &str,
    args: &[&str],
) -> std::process::Output {
    let mut cmd = Command::new("docker");
    cmd.args([
        "run",
        "--rm",
        "-v",
        &format!("{}:/classes:ro", classes.display()),
        "--add-host=host.docker.internal:host-gateway",
        "--entrypoint",
        "bash",
        image,
        "-c",
        &format!(r#"set -e; CP={classpath}; java -cp "/classes:$CP" StreamsApp "$@""#),
        "--",
    ])
    .args(args)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let out = cmd.output().expect("spawn the Streams app container");
    eprintln!(
        "KRABKA[test] StreamsApp image={image} args={args:?} status={}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// A host-side directory the container's non-root user can write class files
/// into. `tempfile` makes it `0700`, which the image's `appuser` cannot use.
fn shared_class_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    #[cfg(unix)]
    {
        use crate::support::chmod_for_container;

        chmod_for_container(dir.path(), 0o777, "chmod the class directory");
    }
    dir
}

/// Create a one-partition topic through the JVM `kafka-topics` tool.
///
/// Streams creates its own internal topics, but not the source and sink
/// topics of the topology: `builder.stream(...)` and `.to(...)` both require
/// the topic to exist already.
fn create_topic(bootstrap: &str, topic: &str) {
    create_console_topic_at(bootstrap, topic);
}

/// Every topic the broker holds, one per line.
fn list_topics(bootstrap: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &["kafka-topics", "--list", "--bootstrap-server", bootstrap],
    );
    support::jvm_output_lines(&out)
}

/// The one topic in `topics` that `matches` accepts, or a failure naming every
/// topic there was. A Streams run that never got as far as creating its
/// internal topics otherwise fails as an opaque index panic.
fn only_topic(topics: &[String], what: &str, matches: impl Fn(&str) -> bool) -> String {
    let found: Vec<&String> = topics
        .iter()
        .filter(|name| matches(name.as_str()))
        .collect();
    assert!(
        found.len() == 1,
        "expected exactly one {what} topic, found {found:?} among {topics:?}",
    );
    found[0].clone()
}

/// `kafka-configs --describe --entity-type topics` for one topic, as text.
///
/// The tool prints only the configs that were explicitly set, which is what
/// makes it the right reading: every key asserted below is one Kafka Streams
/// sent on the `CreateTopics` request, so a broker that dropped it renders
/// nothing rather than a default.
fn describe_topic_configs(bootstrap: &str, topic: &str) -> String {
    describe_console_topic_configs(bootstrap, topic)
}

/// Read the sink topic under `read_committed`, which is the isolation level
/// `exactly_once_v2` output is only visible at.
fn read_committed_sink(bootstrap: &str, topic: &str) -> Vec<String> {
    let out = docker_run_kafka_tool_with_image(
        KAFKA_IMAGE_TXN,
        &[
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            topic,
            "--isolation-level",
            "read_committed",
            "--from-beginning",
            "--max-messages",
            "5",
            "--timeout-ms",
            "60000",
        ],
    );
    support::jvm_output_lines(&out)
}

/// Finalize `streams.version` to level 1, so the KIP-1071 heartbeat handlers
/// stop answering `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE, the
/// same call `jvm_streams_groups.rs` makes.
async fn finalize_streams_version() {
    let client = connect_owned(
        rlmm_broker0_advertised(),
        "krabka-streams-app-test",
        "client build",
    )
    .await;
    support::streams::finalize_streams_version(&client).await;
}

/// The shared half of both cases: the app has run, so check what it left
/// behind -- the sink under `read_committed`, and the two internal topics with
/// the configs `RepartitionTopicConfig` and `WindowedChangelogTopicConfig`
/// give them.
fn assert_streams_run(bootstrap: &str, application_id: &str, output_topic: &str) {
    let sink = read_committed_sink(bootstrap, output_topic);
    assert!(
        sink == EXPECTED_SINK,
        "the read_committed sink must hold the windowed running counts in input order, \
         got {sink:?}",
    );

    let topics = list_topics(bootstrap);
    let repartition_topic = format!("{application_id}-shuffle-repartition");
    let repartition = only_topic(&topics, "repartition", |name| name == repartition_topic);
    // `<app>-<store>-changelog`, where the store is the one the topology names
    // in its `Materialized`. Streams generates a
    // `KSTREAM-AGGREGATE-STATE-STORE-<n>` name only for an unnamed store, and
    // this topology names its window store so it can ask for an in-memory one.
    let changelog = only_topic(&topics, "aggregate-store changelog", |name| {
        name == format!("{application_id}-{WINDOW_STORE_NAME}-changelog")
    });

    // `RepartitionTopicConfig`: `cleanup.policy=delete`, `retention.ms=-1`
    // ("infinity"), and `message.timestamp.type=CreateTime` from
    // `InternalTopicConfig.INTERNAL_TOPIC_DEFAULT_OVERRIDES`.
    let repartition_configs = describe_topic_configs(bootstrap, &repartition);
    for needle in ["retention.ms=-1", "message.timestamp.type=CreateTime"] {
        assert!(
            repartition_configs.contains(needle),
            "the repartition topic {repartition} must carry {needle}; \
             kafka-configs said:\n{repartition_configs}",
        );
    }

    // `WindowedChangelogTopicConfig`: the list-valued `compact,delete` policy,
    // which is the whole point of the changelog half -- a broker that accepts
    // only a single policy refuses the topic and the app never starts.
    let changelog_configs = describe_topic_configs(bootstrap, &changelog);
    for needle in [
        "cleanup.policy=compact,delete",
        "message.timestamp.type=CreateTime",
    ] {
        assert!(
            changelog_configs.contains(needle),
            "the windowed-store changelog {changelog} must carry {needle}; \
             kafka-configs said:\n{changelog_configs}",
        );
    }
}

/// A real `KafkaStreams` topology, on the classic rebalance protocol, with
/// `processing.guarantee=exactly_once_v2`, against krabka.
///
/// This is `StreamsPartitionAssignor` end to end: the members' subscription
/// and assignment userdata, `InternalTopicManager` creating the repartition
/// and changelog topics, the transactional producer the stream thread runs,
/// and the offsets it commits inside those transactions. The suite reads the
/// result back at `read_committed`, so an output record that was never
/// committed is not counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn compiled_streams_app_runs_on_the_classic_protocol() {
    const INPUT: &str = "krabka-streams-classic-in";
    const OUTPUT: &str = "krabka-streams-classic-out";
    const APPLICATION_ID: &str = "krabka-streams-classic";

    let (broker, _dir) = start_host_broker().await;
    let bootstrap = broker0_advertised();

    create_topic(bootstrap, INPUT);
    create_topic(bootstrap, OUTPUT);

    let classes = shared_class_dir();
    compile_streams_app(classes.path());
    let out = run_streams_app(
        KAFKA_IMAGE_TXN,
        classes.path(),
        CP_CONFLUENT,
        &[bootstrap, INPUT, OUTPUT, APPLICATION_ID],
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        stdout.contains("STREAMSPROBE OK"),
        "the Streams app did not emit every expected record; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );

    assert_streams_run(bootstrap, APPLICATION_ID, OUTPUT);
    broker.shutdown().await;
}

/// The same topology under KIP-1071: `group.protocol=streams`, so the app
/// sends its topology in `StreamsGroupHeartbeat` instead of in an assignor's
/// userdata, and the broker -- not the client's assignor -- resolves the
/// tasks.
///
/// The class is compiled against Kafka 3.5 and run against Kafka 4.3, because
/// no pinned image has both `javac` and KIP-1071. If that does not load, the
/// case says so and stops rather than reporting a krabka failure: a linkage
/// error is a statement about the two Kafka releases, not about the broker.
/// The classic case above still covers the compiled-topology half in that
/// event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn compiled_streams_app_runs_on_the_streams_protocol() {
    const INPUT: &str = "krabka-streams-kip1071-in";
    const OUTPUT: &str = "krabka-streams-kip1071-out";
    const APPLICATION_ID: &str = "krabka-streams-kip1071";

    let (broker, _dir) = start_host_broker().await;
    let bootstrap = broker0_advertised();

    finalize_streams_version().await;
    create_topic(bootstrap, INPUT);
    create_topic(bootstrap, OUTPUT);

    let classes = shared_class_dir();
    compile_streams_app(classes.path());
    let out = run_streams_app(
        KAFKA_IMAGE_STREAMS_PROTOCOL,
        classes.path(),
        CP_APACHE,
        &[bootstrap, INPUT, OUTPUT, APPLICATION_ID, "streams"],
    );
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();

    if let Some(error) = LINKAGE_ERRORS
        .iter()
        .find(|error| stderr.contains(*error) || stdout.contains(*error))
    {
        eprintln!(
            "KRABKA[test] the Kafka 3.5-compiled StreamsApp does not load against the Kafka 4.3 \
             jars ({error}), so the KIP-1071 run cannot be made from a cross-compiled class. \
             Nothing here is a krabka result; \
             `compiled_streams_app_runs_on_the_classic_protocol` covers the compiled topology. \
             stdout:\n{stdout}\nstderr:\n{stderr}"
        );
        broker.shutdown().await;
        return;
    }

    assert!(
        stdout.contains("STREAMSPROBE OK"),
        "the Streams app did not emit every expected record under group.protocol=streams; \
         stdout:\n{stdout}\nstderr:\n{stderr}",
    );

    assert_streams_run(bootstrap, APPLICATION_ID, OUTPUT);
    broker.shutdown().await;
}
