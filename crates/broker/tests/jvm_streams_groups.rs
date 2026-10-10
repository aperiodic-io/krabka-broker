//! JVM differential / interop test for KIP-1071 streams groups (the Streams
//! Rebalance Protocol).
//!
//! The test drives the REAL Apache Kafka 4.1.0 `kafka-streams-groups.sh` admin
//! tool, a `KafkaStreamsGroupsCommand` that wraps the JVM `AdminClient`. The
//! tool runs inside an `mirror.gcr.io/apache/kafka:4.1.0` container against an
//! in-process Krabka broker on the host. The container has a JRE-only Kafka
//! image with no `javac` or `jshell`, so we cannot compile a custom
//! `KafkaStreams` app. Instead we use the native `krabka-client-core` client to
//! make a streams group EXIST on Krabka. The client finalizes
//! `streams.version=1`, creates a source topic, and drives a
//! `StreamsGroupHeartbeat` so the group has a live member with an assignment.
//! We then point the bundled JVM admin tool at Krabka and prove it round-trips
//! the streams-group admin wire path.
//!
//! The compiled topology this suite cannot run lives in
//! `tests/jvm_streams_app.rs`: it builds a real `KafkaStreams` app in the
//! `cp-kafka:7.5.0` image, which does ship `javac`, and runs it against Krabka
//! on both the classic protocol and `group.protocol=streams`. This suite stays
//! the one that reads the KIP-1071 ADMIN wire path off a real
//! `AdminClient`'s DEBUG log.
//!
//! The real `apache-kafka-java` 4.1.0 `AdminClient` drives this flow, read
//! empirically from its DEBUG wire log:
//!
//! - `ApiVersions` negotiation (key 18): Krabka advertises api keys 88/89 and
//!   the finalized `streams.version` feature,
//! - `Metadata` (key 13) to discover the broker set,
//! - `ListGroups` (key 16, v5) with `typesFilter=[Streams]`, which is the
//!   KIP-1071 `ListGroupsOptions.forStreamsGroups()` filter, and Krabka returns
//!   the live streams group,
//! - `FindCoordinator` (key 10) for the group,
//! - `StreamsGroupDescribe` (key 89): Krabka returns the full `DescribedGroup`,
//!   which the JVM `DescribeStreamsGroupsHandler` accepts. That group holds the
//!   group state and epochs, the resolved topology, and the member with its
//!   active task assignment.
//!
//! The test is gated `#[ignore = "requires Docker"]`. Run it with `--ignored`.
//!
//! Networking mirrors `jvm_share_groups.rs`: the broker binds `0.0.0.0:9092`
//! and advertises `host.docker.internal:9092`. The container reaches it with
//! `--add-host=host.docker.internal:host-gateway`.

mod support;

use assert2::{assert, check};
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::owned::{
    common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
    streams_group_heartbeat_request::Topology,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

/// Port the broker binds on the host and that the container reaches through
/// `host.docker.internal`.
/// Ports for this test process, allocated once rather than fixed at 9092.
///
/// Every container suite used to hard-code 9092/9093, so two could not run at
/// the same time: the second to start lost the bind and reported `Address
/// already in use` as a test failure. That is why these targets ran one at a
/// time. A port per process lets them overlap.
///
/// `&'static str`, so these read as the constants they replaced.
fn bootstrap_addr() -> &'static str {
    &support::jvm_listeners().advertised
}

/// Official Apache Kafka image. It ships KIP-1071 streams groups plus the
/// `kafka-streams-groups.sh` admin tool (`StreamsGroupDescribe` / `ListGroups`).
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const STREAMS_GROUPS: &str = "/opt/kafka/bin/kafka-streams-groups.sh";
/// Boot one broker bound to `0.0.0.0:9092`. It advertises `host.docker.internal:
/// 9092` so the Docker container's post-Metadata connect targets a hostname it
/// can resolve. Mirrors `jvm_share_groups.rs::start_host_broker`.
async fn start_host_broker() -> (BrokerHandle, tempfile::TempDir) {
    let (broker, dir) = support::start_jvm_single("krabka_broker=info,info", |_| {}).await;
    broker.wait_until_group_coordinator_ready().await;
    (broker, dir)
}
/// Native client that connects to the broker's local loopback listener. The
/// container reaches the same broker through `host.docker.internal`.
async fn connect() -> Client {
    support::client::connect_owned(
        support::jvm_client_addr(),
        "krabka-streams-test",
        "client build",
    )
    .await
}

/// Create `topic` (`partitions` partitions) and wait until this broker leads
/// partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str, partitions: i32) {
    support::client::create_led_topic(
        broker,
        client,
        crate::support::topics::CreateTopicSetup {
            topic,
            num_partitions: crate::support::topics::TopicPartitionCount(partitions),
            ..Default::default()
        },
    )
    .await;
}

/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
use support::streams::{active_partition_count, finalize_streams_version, follow_up};

fn topology(source_topic: &str) -> Topology {
    support::streams::topology(source_topic, Vec::new())
}

async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    support::streams::streams_join_and_converge(client, group, topo, want_active, tries, true).await
}

/// Heartbeat once more to keep the live member's session fresh while the JVM
/// admin tool runs. The group then stays non-Empty with a member and an
/// assignment.
async fn keepalive(client: &Client, group: &str, member_id: &str, epoch: i32) {
    let active = Some(vec![ReqTaskIds {
        subtopology_id: "0".into(),
        partitions: vec![0, 1],
        ..Default::default()
    }]);
    let _ = client
        .send(follow_up(crate::support::streams::StreamsFollowUpSetup {
            group,
            member_id,
            epoch: crate::support::streams::StreamsMemberEpoch(epoch),
            active,
        }))
        .await;
}

/// Run a docker container against the host broker and return its output. The
/// admin tool may exit non-zero even on a successful round-trip, as the
/// `jvm_share_groups.rs` note records. So callers check stdout, not exit status.
fn docker_run(args: &[&str]) -> std::process::Output {
    support::jvm_docker_run(KAFKA_IMAGE, args)
}

/// A DEBUG-level log4j2 config, written into the container at `/tmp/d.yaml`, so
/// the JVM tool's `NetworkClient`/`KafkaAdminClient` logs every request and
/// response. The tool's own stdout is empty when no streams group surfaces. See
/// the test below. So the test reads the wire-level interop checkpoints it
/// asserts on from these DEBUG lines, captured with `2>&1`.
// NOTE: YAML is indentation-sensitive, so this is written WITHOUT Rust
// line-continuation (`\` at EOL eats the next line's leading spaces). Each
// `\n` is a real newline and the two/four/six-space indents are literal.
const TOOL_DEBUG_PREAMBLE: &str = concat!(
    "cat > /tmp/d.yaml <<'YAML'\n",
    "Configuration:\n",
    "  Appenders:\n",
    "    Console:\n",
    "      name: STDERR\n",
    "      target: SYSTEM_ERR\n",
    "      PatternLayout:\n",
    "        Pattern: \"%d %p %c %m%n\"\n",
    "  Loggers:\n",
    "    Root:\n",
    "      level: DEBUG\n",
    "      AppenderRef:\n",
    "        ref: STDERR\n",
    "YAML\n",
    "export KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/tmp/d.yaml'\n",
);

/// The headline differential test: make a KIP-1071 streams group live on Krabka
/// with the native `krabka-client-core` client (`StreamsGroupHeartbeat`, api
/// 88). Then drive the REAL Apache Kafka 4.1.0 `kafka-streams-groups.sh` admin
/// tool against Krabka and prove it round-trips the streams-group admin wire
/// path. That tool is the JVM `StreamsGroupCommand` that wraps `AdminClient`.
///
/// We assert these checkpoints, read from the JVM tool's own DEBUG wire logs:
///
///  1. The JVM `AdminClient` negotiated `ApiVersions` with Krabka. The response
///     advertised `StreamsGroupDescribe (apiKey=89)` plus the finalized
///     `streams.version` feature, so the KIP-1071 admin surface is visible to
///     the real client.
///  2. The tool issued `LIST_GROUPS apiVersion=5` with `typesFilter=[Streams]`,
///     the KIP-1071 `ListGroupsOptions.forStreamsGroups()` filter. Krabka
///     answered with the live streams group (`errorCode=0`).
///  3. `StreamsGroupCommand.describeGroups()` then resolved the coordinator and
///     issued `StreamsGroupDescribe` (api 89). Krabka returned the full group,
///     which the JVM `DescribeStreamsGroupsHandler` accepts. That group holds
///     the state and epochs, the resolved topology, and the member with its
///     active task assignment. The handler rejects a describe whose topology is
///     absent, so the topology must be populated. Checkpoint 3 guards against a
///     regression there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_streams_groups_admin_round_trips_krabka() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "streams-input";
    let group = "jvm-streams-g";

    let client = connect().await;
    finalize_streams_version(&client).await;
    create_topic(&broker, &client, topic, 2).await;

    // Make a streams group EXIST on Krabka: a lone member owns both partitions
    // of the single subtopology over `streams-input` (native StreamsGroupHeartbeat
    // / api 88). The broker runs Kafka's 3 s initial rebalance delay, so the
    // bound allows 10 s of 200 ms heartbeats.
    let (member_id, resp) = join_and_converge(&client, group, topology(topic), 2, 50).await;
    check!(
        resp.error_code == 0,
        "lone member must join cleanly, got member_id={member_id:?}, {resp:?}"
    );
    check!(
        !member_id.is_empty(),
        "lone member must get a broker-minted member id, got member_id={member_id:?}, {resp:?}"
    );
    check!(
        active_partition_count(&resp) == 2,
        "lone member must own both input partitions, got member_id={member_id:?}, {resp:?}"
    );
    let epoch = resp.member_epoch;
    keepalive(&client, group, &member_id, epoch).await;

    // Drive the JVM tool with DEBUG wire logging so we can read the actual
    // request/response frames it exchanges with Krabka. `--describe` exercises
    // the full KIP-1071 admin flow: ApiVersions (18) -> Metadata (13) ->
    // ListGroups (16, typesFilter=[Streams]) -> [StreamsGroupDescribe (89)].
    let described = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{TOOL_DEBUG_PREAMBLE}\
             {STREAMS_GROUPS} --bootstrap-server {bootstrap} --describe --group {group} 2>&1; \
             echo EXIT=$?"
        ),
    ]);
    // With `2>&1` the DEBUG wire log lands on the container's stdout.
    let wire = String::from_utf8_lossy(&described.stdout);
    eprintln!("KRABKA[test] streams-groups --describe (DEBUG wire log):\n{wire}");

    // Checkpoint 1: the ApiVersions handshake with Krabka advertised the
    // KIP-1071 StreamsGroupDescribe API (apiKey=89) and the finalized
    // streams.version feature — the streams-group admin surface is visible to a
    // real Apache Kafka 4.1.0 AdminClient.
    //
    // Checkpoint 2: the tool issued the KIP-1071 streams-group LIST_GROUPS
    // request (typesFilter=[Streams]) and Krabka answered it cleanly. This is
    // the real JVM streams-group admin client round-tripping against Krabka.
    //
    // Checkpoint 3: the streams group now surfaces in Krabka's ListGroups reply,
    // so `StreamsGroupCommand.describeGroups()` proceeds past `listStreamsGroups()`,
    // resolves the coordinator, and issues the KIP-1071 `StreamsGroupDescribe`
    // (api 89). Krabka answers with the full group — topology + member + active
    // task assignment — completing the JVM admin round-trip end to end. The
    // describe response must carry the resolved topology ("missing the topology
    // information" must NOT appear) — the real JVM `DescribeStreamsGroupsHandler`
    // logs an ERROR and rejects a describe whose topology is absent.
    let group_needle = format!("groupId='{group}'");
    // (needle, expected presence in the DEBUG wire log)
    let cases = [
        // Checkpoint 1.
        ("Received API_VERSIONS response", true),
        ("apiKey=89", true),
        ("FinalizedFeatureKey(name='streams.version'", true),
        // Checkpoint 2.
        ("Sending LIST_GROUPS request", true),
        ("typesFilter=[Streams]", true),
        ("Received LIST_GROUPS response", true),
        ("errorCode=0", true),
        // Checkpoint 3. The describe response must carry the resolved topology,
        // so "missing the topology information" must NOT appear.
        ("Received STREAMS_GROUP_DESCRIBE response", true),
        ("missing the topology information", false),
        ("subtopologyId='0'", true),
        (group_needle.as_str(), true),
    ];
    for (needle, expected) in cases {
        assert!(
            wire.contains(needle) == expected,
            "JVM streams-group admin round-trip checkpoint failed: wire log must {} \
             {needle:?}; wire log:\n{wire}",
            if expected { "contain" } else { "not contain" },
        );
    }
}
