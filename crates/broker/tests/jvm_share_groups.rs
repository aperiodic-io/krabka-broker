//! JVM differential / interop test for KIP-932 share groups.
//!
//! This test drives a REAL Apache Kafka 4.x
//! `kafka-console-share-consumer.sh`, which runs a `KafkaShareConsumer`,
//! inside a `mirror.gcr.io/apache/kafka:4.1.0` container. It runs against an
//! in-process Krabka broker on the host. This exercises Krabka's share-group
//! wire protocol end to end against the real JVM client:
//!
//! - `ApiVersions` negotiation (key 18; `share.version` advertised by Krabka),
//! - `FindCoordinator(GROUP/SHARE)` (key 10),
//! - `ShareGroupHeartbeat` (key 76) membership + assignment,
//! - `ShareFetch` (key 78) acquire + record bytes,
//! - `ShareAcknowledge` (key 79) implicit-ack on poll.
//!
//! Where a share partition starts is a GROUP config, not a client property:
//! `KafkaShareConsumer` has no `auto.offset.reset` of its own, so these tests
//! set `share.auto.offset.reset` with `kafka-configs.sh --entity-type groups`
//! before the consumer's first fetch, exactly as an operator would. The
//! `earliest` cases then read every produced record, the `by_duration` case
//! reads only the records inside its window, and the default (`latest`) case
//! reads none of the records produced before it joined.
//!
//! The test is gated with `#[ignore = "requires Docker"]`. Run it with
//! `--ignored`.
//!
//! The networking mirrors `jvm_consumer_group_next_gen.rs` and
//! `jvm_acceptance.rs`. The broker binds `0.0.0.0:9092` and advertises
//! `host.docker.internal:9092`. The container reaches it through
//! `--add-host=host.docker.internal:host-gateway`.

mod support;

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_client_core::Client;
use krabka_protocol::records::{Record, RecordBatch};

/// Port that the broker binds on the host, and that the container reaches
/// through `host.docker.internal`.
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

/// Official Apache Kafka image. It ships KIP-932 share groups, which are GA in
/// 4.x, and the `kafka-console-share-consumer.sh` and `kafka-share-groups.sh`
/// tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const SHARE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-share-consumer.sh";
const SHARE_GROUPS: &str = "/opt/kafka/bin/kafka-share-groups.sh";
const CONFIGS: &str = "/opt/kafka/bin/kafka-configs.sh";

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

use support::share::coordinator_key as share_coordinator_key;

/// Boots one broker bound to `0.0.0.0:9092` that advertises
/// `host.docker.internal:9092`. The Docker container's connect after Metadata
/// then targets a hostname it can resolve. This mirrors
/// `jvm_consumer_group_next_gen.rs::start_host_broker`.
async fn start_host_broker() -> (BrokerHandle, tempfile::TempDir) {
    support::start_jvm_single("krabka_broker=info,info", |_| {}).await
}
async fn connect() -> Client {
    support::client::connect_owned(
        support::jvm_client_addr(),
        "krabka-share-test",
        "client build",
    )
    .await
}

/// Creates `topic` with 1 partition and waits until this broker leads
/// partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str) -> uuid::Uuid {
    support::client::create_led_topic(
        broker,
        client,
        crate::support::topics::CreateTopicSetup {
            topic,
            ..Default::default()
        },
    )
    .await;
    support::share::topic_id(broker, topic)
}

/// Creates `__share_group_state` in advance, as a KIP-932 client would create
/// it lazily through `FindCoordinator(SHARE)`. It then waits until every state
/// partition is local, so the share coordinator can accept writes before the
/// JVM consumer drives `ShareFetch` and `ShareAcknowledge`.
async fn bootstrap_share_state(broker: &BrokerHandle, client: &Client, key: &str) {
    support::find_coordinator(client, support::KEY_TYPE_SHARE, key).await;
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Wall-clock milliseconds, the unit a record timestamp carries.
fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .expect("timestamp fits i64")
}

/// Produces `values` stamped with the current time.
async fn produce(client: &Client, topic: &str, tid: uuid::Uuid, values: &[&str]) {
    produce_at(client, topic, tid, values, now_ms()).await;
}

/// Produces the supplied `values` as one batch into `(topic, 0)`, every record
/// stamped `timestamp_ms`. It retries while the new partition still
/// materializes its leader.
///
/// The timestamp is what a `by_duration` share group resolves against, so a
/// test can place records inside or outside the window without waiting.
async fn produce_at(
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    values: &[&str],
    timestamp_ms: i64,
) {
    let records = values
        .iter()
        .enumerate()
        .map(|(i, value)| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
            ..Default::default()
        })
        .collect();
    support::share::produce_batch(
        client,
        RecordBatch {
            last_offset_delta: i32::try_from(values.len() - 1).unwrap(),
            base_timestamp: timestamp_ms,
            max_timestamp: timestamp_ms,
            records,
            ..Default::default()
        },
        crate::support::share::ShareProduceSetup {
            topic,
            topic_id: tid,
            ..Default::default()
        },
    )
    .await;
}

/// Runs a docker container against the host broker and returns its output. The
/// share consumer exits with a non-zero status on an idle timeout, even after
/// it consumed records, so callers check stdout and not the exit status.
fn docker_run(args: &[&str]) -> std::process::Output {
    support::jvm_docker_run(KAFKA_IMAGE, args)
}

/// Sets `share.auto.offset.reset` for `group` through the real
/// `kafka-configs.sh --alter --entity-type groups`, which drives
/// `IncrementalAlterConfigs` (`api_key` 44) against Krabka.
///
/// `KafkaShareConsumer` has no client-side `auto.offset.reset`; the group
/// config is the only way to move a share partition's start offset, and it has
/// to be in place before the group's first `ShareFetch` resolves it.
fn set_share_auto_offset_reset(bootstrap: &str, group: &str, value: &str) {
    let out = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{CONFIGS} --bootstrap-server {bootstrap} --alter \
                --entity-type groups --entity-name {group} \
                --add-config share.auto.offset.reset={value}"
        ),
    ]);
    assert!(
        out.status.success(),
        "kafka-configs --alter share.auto.offset.reset={value} failed: {}",
        String::from_utf8_lossy(&out.stderr),
    );
}

async fn prepare_share_topic(
    broker: &BrokerHandle,
    topic: &str,
    group: &str,
) -> (Client, uuid::Uuid) {
    let client = connect().await;
    let tid = create_topic(broker, &client, topic).await;
    bootstrap_share_state(broker, &client, &share_coordinator_key(group, tid, 0)).await;
    (client, tid)
}

/// The main differential test. A real JVM `KafkaShareConsumer` joins a fresh
/// Krabka share group, reads every produced record, and acknowledges
/// implicitly on poll. The test asserts that each produced value appears in
/// its stdout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_consumer_reads_krabka() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm";
    let group = "jvm-share-g";
    let values = ["share-alpha", "share-bravo", "share-charlie", "share-delta"];

    let (client, tid) = prepare_share_topic(&broker, topic, group).await;
    produce(&client, topic, tid, &values).await;

    // Drive the real JVM KafkaShareConsumer. The group config sets the
    // share-partition start to the log start offset, so it must read all
    // produced records. `--timeout-ms` makes it exit after the idle window
    // once it has drained the partition.
    set_share_auto_offset_reset(bootstrap, group, "earliest");
    let consumed = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {bootstrap} \
                --topic {topic} \
                --group {group} \
                --timeout-ms 20000 \
                --max-messages {}",
            values.len()
        ),
    ]);
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    eprintln!("KRABKA[test] share-consumer stdout:\n{stdout}");

    for v in values {
        assert!(
            stdout.contains(v),
            "JVM share consumer must read produced value {v:?}; got stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&consumed.stderr),
        );
    }
}

/// `kafka-share-groups.sh --describe --state` reports the share group after
/// the JVM consumer joined. That proves Krabka serves the share-group admin
/// path (`ShareGroupDescribe`, `api_key` 77) to the real JVM tooling. The tool
/// resolves the share coordinator, sends `ShareGroupDescribe`, and prints the
/// group's coordinator and state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_groups_describe_state() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-d";
    let group = "jvm-share-gd";
    let values = ["d-one", "d-two"];

    register_jvm_share_group(&broker, topic, group, &values).await;

    // `--describe --state` drives ShareGroupDescribe (api_key 77). Renders e.g.
    //   GROUP         COORDINATOR (ID)              STATE   #MEMBERS
    //   jvm-share-gd  host.docker.internal:9092 (1) Empty   0
    let state = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_GROUPS} --bootstrap-server {bootstrap} --describe --state --group {group}"
        ),
    ]);
    let state_out = String::from_utf8_lossy(&state.stdout);
    eprintln!("KRABKA[test] share-groups --describe --state stdout:\n{state_out}");
    assert!(
        state_out.contains(group),
        "share group {group} must appear in --describe --state output; got:\n{state_out}\nstderr:\n{}",
        String::from_utf8_lossy(&state.stderr),
    );
}

/// `kafka-share-groups.sh --list` drives `ListGroups` (`api_key` 16) with
/// `types_filter = ["share"]`. After a real JVM `KafkaShareConsumer` joins a
/// share group on the Krabka broker, the share group id must appear in the
/// tool's `--list` stdout.
///
/// Before the `ListGroups` share pass, the JVM tool's `types_filter=["share"]`
/// matched nothing and `--list` was EMPTY. This test asserts that the
/// regression is closed against the real Apache Kafka 4.1.0 tool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_groups_list() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-l";
    let group = "jvm-share-gl";
    let values = ["l-one", "l-two"];

    register_jvm_share_group(&broker, topic, group, &values).await;

    // `--list` drives ListGroups(16) with types_filter=["share"]. The share
    // group id must appear in stdout.
    let listed = docker_run(&[
        "bash",
        "-c",
        &format!("{SHARE_GROUPS} --bootstrap-server {bootstrap} --list"),
    ]);
    let list_out = String::from_utf8_lossy(&listed.stdout);
    eprintln!("KRABKA[test] share-groups --list stdout:\n{list_out}");
    assert!(
        list_out.contains(group),
        "share group {group} must appear in --list output; got:\n{list_out}\nstderr:\n{}",
        String::from_utf8_lossy(&listed.stderr),
    );
}

/// `share.auto.offset.reset=by_duration:PT1H` starts the share partition at
/// the first record inside the window. The test produces one batch stamped two
/// hours ago and one stamped now, then asserts that the real JVM
/// `KafkaShareConsumer` reads only the recent batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_consumer_by_duration_skips_records_outside_the_window() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-dur";
    let group = "jvm-share-gdur";
    let stale = ["dur-stale-one", "dur-stale-two"];
    let recent = ["dur-recent-one", "dur-recent-two"];

    let (client, tid) = prepare_share_topic(&broker, topic, group).await;
    let now = now_ms();
    produce_at(&client, topic, tid, &stale, now - 2 * 60 * 60 * 1_000).await;
    produce_at(&client, topic, tid, &recent, now).await;

    set_share_auto_offset_reset(bootstrap, group, "by_duration:PT1H");
    let consumed = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {bootstrap} \
                --topic {topic} \
                --group {group} \
                --timeout-ms 20000 \
                --max-messages {}",
            recent.len()
        ),
    ]);
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    eprintln!("KRABKA[test] by_duration share-consumer stdout:\n{stdout}");

    for v in recent {
        assert!(
            stdout.contains(v),
            "record inside the by_duration window must be delivered: {v:?}; got:\n{stdout}",
        );
    }
    for v in stale {
        assert!(
            !stdout.contains(v),
            "record older than the by_duration window must not be delivered: {v:?}; got:\n{stdout}",
        );
    }
}

/// The default strategy is `latest`: a fresh share group never sees the
/// records produced before its first fetch.
///
/// The group config is left untouched, so the broker's own default decides.
/// After the consumer's idle-timeout exit, `kafka-share-groups.sh --describe
/// --state` must still report the group, which is what proves the consumer
/// joined and fetched rather than failing to connect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_consumer_defaults_to_latest() {
    let bootstrap = bootstrap_addr();
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-latest";
    let group = "jvm-share-glatest";
    let values = ["latest-alpha", "latest-bravo"];

    let (client, tid) = prepare_share_topic(&broker, topic, group).await;
    produce(&client, topic, tid, &values).await;

    let consumed = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {bootstrap} \
                --topic {topic} \
                --group {group} \
                --timeout-ms 15000"
        ),
    ]);
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    eprintln!("KRABKA[test] default-latest share-consumer stdout:\n{stdout}");
    for v in values {
        assert!(
            !stdout.contains(v),
            "a `latest` share group must not read a record produced before it joined: {v:?}; \
             got:\n{stdout}",
        );
    }

    let state = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_GROUPS} --bootstrap-server {bootstrap} --describe --state --group {group}"
        ),
    ]);
    let state_out = String::from_utf8_lossy(&state.stdout);
    assert!(
        state_out.contains(group),
        "the share group must be registered, or the empty stdout above proves nothing; got:\n\
         {state_out}\nstderr:\n{}",
        String::from_utf8_lossy(&state.stderr),
    );
}

async fn register_jvm_share_group(
    broker: &BrokerHandle,
    topic: &str,
    group: &str,
    values: &[&str],
) {
    let (client, tid) = prepare_share_topic(broker, topic, group).await;
    produce(&client, topic, tid, values).await;
    let bootstrap = bootstrap_addr();
    set_share_auto_offset_reset(bootstrap, group, "earliest");
    let _ = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} --bootstrap-server {bootstrap} --topic {topic} --group {group} --timeout-ms 15000 --max-messages {}",
            values.len()
        ),
    ]);
}
