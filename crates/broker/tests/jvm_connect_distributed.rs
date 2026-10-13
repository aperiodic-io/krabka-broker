//! A Kafka Connect worker in DISTRIBUTED mode, against krabka.
//!
//! Standalone mode keeps its offsets in a local file and never speaks to a
//! coordinator, so it exercises nothing this suite is about. Distributed mode
//! backs all three of its stores -- config, offset and status -- with
//! `KafkaBasedLog`, and that is a specific contract on the broker:
//!
//! - `TopicAdmin` creates the three topics with `cleanup.policy=compact` and
//!   `CreateTopics`, treating `TOPIC_ALREADY_EXISTS` as success;
//! - each log reads its end offsets with `Admin.listOffsets`, then replays the
//!   topic from `seekToBeginning` to that offset before the worker will serve;
//! - every write goes through an idempotent producer (`acks=all`,
//!   `enable.idempotence=true`), so `InitProducerId` and the producer-id
//!   sequencing are on the startup path, not an optional extra;
//! - the `DistributedHerder` itself is a classic consumer group under the
//!   `connect` protocol, so `JoinGroup`/`SyncGroup` carry the worker's own
//!   assignment userdata.
//!
//! The worker is the stock `connect-distributed` of
//! `mirror.gcr.io/apache/kafka:4.3.1`. That image is JRE-only, which is fine
//! here -- nothing is compiled -- and its tarball carries both
//! `bin/connect-distributed.sh` and `libs/connect-file-*.jar`, so no new image
//! is pinned for this suite. `kafka-run-class.sh` deliberately excludes
//! `connect-file*.jar` from the worker classpath, so the jar is copied into a
//! `plugin.path` directory instead of being picked up from `libs`.
//!
//! The suite registers a `FileStreamSource` and a `FileStreamSink` over one
//! topic through the REST API, and then asserts the round trip: the worker
//! reports both connectors and their tasks `RUNNING`, the sink file ends up
//! matching the source file byte for byte, and the three internal topics exist
//! on krabka with `cleanup.policy=compact`.
//!
//! The REST calls are made from the test over a published port with a small
//! HTTP/1.1 client rather than with `curl` inside the container: the image is
//! Alpine with nothing but `bash` added, so no HTTP client is guaranteed to be
//! in it.
//!
//! Gated `#[ignore = "requires Docker"]`; the Bazel `docker` lane runs it with
//! `--ignored --test-threads=1`.

mod jvm_acceptance;
mod support;

use {
    crate::{
        jvm_acceptance::{create_console_topic_at, describe_console_topic_configs},
        support::{docker_logs, remove_container},
    },
    assert2::assert,
    jvm_acceptance::broker0_advertised,
    std::{
        io::{Read as _, Write as _},
        net::TcpStream,
        process::Command,
        time::Duration,
    },
};

/// The Apache Kafka image the worker runs from. Its tarball is the whole
/// release, so `connect-distributed.sh` and the `FileStream` connector jar are
/// both in it.
const CONNECT_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// The three `KafkaBasedLog`-backed stores, as this suite names them.
const CONFIG_TOPIC: &str = "krabka-connect-configs";
const OFFSET_TOPIC: &str = "krabka-connect-offsets";
const STATUS_TOPIC: &str = "krabka-connect-status";

/// The topic the `FileStream` source writes and the `FileStream` sink reads.
const DATA_TOPIC: &str = "krabka-connect-file";

/// Where the connectors' files live inside the worker container.
const SOURCE_FILE: &str = "/tmp/connect-data/source.txt";
const SINK_FILE: &str = "/tmp/connect-data/sink.txt";

/// What the source file holds, and therefore what the sink file must end up
/// holding. `FileStreamSourceConnector` emits one record per line and
/// `FileStreamSinkConnector` writes one line per record, so the round trip is
/// line for line.
const LINES: [&str; 3] = ["connect-line-1", "connect-line-2", "connect-line-3"];

/// How long the worker gets to boot far enough to serve its REST API. It has
/// to create and then fully replay three topics before the herder completes,
/// and it does that on a cold JVM.
const REST_BUDGET: Duration = Duration::from_secs(240);

/// How long both connectors get to reach `RUNNING` once they are registered.
const RUNNING_BUDGET: Duration = Duration::from_secs(180);

/// How long the sink file gets to match the source file. The sink flushes on
/// `offset.flush.interval.ms`, which the worker properties set to one second.
const ROUND_TRIP_BUDGET: Duration = Duration::from_secs(180);

/// A `connect-distributed` worker in a container of its own, with its REST
/// port published to the host.
struct ConnectWorker {
    container: String,
    rest: String,
}

impl Drop for ConnectWorker {
    fn drop(&mut self) { remove_container(&self.container); }
}

impl ConnectWorker {
    /// Start the worker against `bootstrap` and return once `docker run` has
    /// accepted it. The process inside is still booting; [`Self::wait_for_rest`]
    /// is what waits for it.
    fn start(bootstrap: &str) -> Self {
        let container = support::unique_container_name("krabka-connect");
        let rest_port = support::free_port();
        let source_lines = LINES.join("\\n");
        // Written as one script rather than a bind mount: the image runs as a
        // non-root user, and everything the worker needs -- the plugin
        // directory, the source file, the properties -- is container-local.
        //
        // `listeners` rather than the `rest.host.name`/`rest.port` pair,
        // which Kafka 4 no longer accepts.
        let entry = format!(
            "set -e\n\
             mkdir -p /tmp/connect-plugins /tmp/connect-data\n\
             cp /opt/kafka/libs/connect-file-*.jar /tmp/connect-plugins/\n\
             printf '{source_lines}\\n' > {SOURCE_FILE}\n\
             cat > /tmp/connect-distributed.properties <<'PROPS'\n\
             bootstrap.servers={bootstrap}\n\
             group.id=krabka-connect-cluster\n\
             key.converter=org.apache.kafka.connect.storage.StringConverter\n\
             value.converter=org.apache.kafka.connect.storage.StringConverter\n\
             config.storage.topic={CONFIG_TOPIC}\n\
             config.storage.replication.factor=1\n\
             offset.storage.topic={OFFSET_TOPIC}\n\
             offset.storage.replication.factor=1\n\
             offset.storage.partitions=1\n\
             status.storage.topic={STATUS_TOPIC}\n\
             status.storage.replication.factor=1\n\
             status.storage.partitions=1\n\
             offset.flush.interval.ms=1000\n\
             plugin.path=/tmp/connect-plugins\n\
             listeners=HTTP://0.0.0.0:8083\n\
             PROPS\n\
             exec /opt/kafka/bin/connect-distributed.sh /tmp/connect-distributed.properties\n"
        );
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container,
                "-p",
                &format!("{rest_port}:8083"),
                "--add-host=host.docker.internal:host-gateway",
                "--entrypoint",
                "bash",
                CONNECT_IMAGE,
                "-c",
                &entry,
            ])
            .status()
            .expect("spawn the connect-distributed container");
        assert!(
            status.success(),
            "docker run of the connect-distributed worker failed",
        );
        Self {
            container,
            rest: format!("127.0.0.1:{rest_port}"),
        }
    }

    /// Everything the worker has printed. Every failure below ends with this,
    /// because the worker's own log is where a refused `CreateTopics`, a
    /// `listOffsets` that never answered or a plugin that was not found is
    /// written down.
    fn logs(&self) -> String { docker_logs(&self.container) }

    /// One file inside the container, or `None` if it is not there yet. The
    /// sink file does not exist until the sink task's first flush.
    fn read_file(&self, path: &str) -> Option<String> {
        let out = Command::new("docker")
            .args(["exec", &self.container, "cat", path])
            .output()
            .expect("spawn docker exec cat");
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Block until the REST API answers, which is the worker saying it has
    /// created and replayed all three internal topics and completed the
    /// herder's first rebalance.
    async fn wait_for_rest(&self) {
        let deadline = tokio::time::Instant::now() + REST_BUDGET;
        loop {
            if let Ok((status, _)) = http(&self.rest, "GET", "/connectors", None)
                && status == 200
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the Connect worker never served its REST API within {REST_BUDGET:?}; \
                 worker logs:\n{}",
                self.logs(),
            );
            // intentional: a worker's readiness is a JVM-internal state with no
            // krabka-side awaiter -- polling its own REST API is the only
            // observation there is.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// `PUT /connectors/<name>/config`, which creates or replaces a connector.
    /// `PUT` rather than `POST /connectors`, so a retry is not a conflict.
    fn put_connector(&self, name: &str, config: &str) {
        let (status, body) = http(
            &self.rest,
            "PUT",
            &format!("/connectors/{name}/config"),
            Some(config),
        )
        .expect("PUT the connector config");
        assert!(
            (200..300).contains(&status),
            "registering the {name} connector failed with HTTP {status}: {body}\n\
             worker logs:\n{}",
            self.logs(),
        );
    }

    /// Block until `name` and every one of its tasks report `RUNNING`.
    ///
    /// A `FAILED` state ends the wait immediately: the task's own trace is in
    /// the status body, and waiting out the budget would only bury it.
    async fn wait_until_running(&self, name: &str, tasks: usize) {
        let deadline = tokio::time::Instant::now() + RUNNING_BUDGET;
        let path = format!("/connectors/{name}/status");
        loop {
            let body = match http(&self.rest, "GET", &path, None) {
                Ok((200, body)) => body,
                _ => String::new(),
            };
            assert!(
                !body.contains("\"state\":\"FAILED\""),
                "the {name} connector reported FAILED: {body}\nworker logs:\n{}",
                self.logs(),
            );
            // One `RUNNING` for the connector itself and one per task.
            if body.matches("\"state\":\"RUNNING\"").count() > tasks {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the {name} connector and its {tasks} task(s) did not reach RUNNING within \
                 {RUNNING_BUDGET:?}; last status: {body}\nworker logs:\n{}",
                self.logs(),
            );
            // intentional: connector and task state is herder-local, reached
            // through a rebalance the broker does not report on.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    /// Block until the sink file holds exactly the source file's lines.
    async fn wait_for_round_trip(&self) {
        let deadline = tokio::time::Instant::now() + ROUND_TRIP_BUDGET;
        loop {
            let sink: Vec<String> = self
                .read_file(SINK_FILE)
                .unwrap_or_default()
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect();
            if sink == LINES {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the FileStream sink file never matched the source file within \
                 {ROUND_TRIP_BUDGET:?}; sink held {sink:?}, source holds {LINES:?}\n\
                 worker logs:\n{}",
                self.logs(),
            );
            // intentional: the sink writes on its own flush interval, which is
            // connector state and not anything krabka can be awaited on.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}

/// One HTTP/1.1 exchange against `addr`, returning the status code and the
/// body.
///
/// `Connection: close` is what makes this small enough to hand-roll: the
/// server closes the socket at the end of the response, so the body is
/// everything up to EOF and neither `Content-Length` nor chunked framing has
/// to be parsed. Timeouts are set on both directions so a wedged worker fails
/// the surrounding wait rather than blocking the test forever.
fn http(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let payload = body.unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Accept: application/json\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {payload}",
        payload.len(),
    );
    stream.write_all(request.as_bytes())?;
    stream.flush()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let text = String::from_utf8_lossy(&response).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("no HTTP status line in the response: {text}"),
            )
        })?;
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, rest)| rest.to_owned());
    Ok((status, body))
}

/// `kafka-configs --describe --entity-type topics` for one topic, as text.
/// The tool prints only the configs that were explicitly set, so a
/// `cleanup.policy=compact` line here is `TopicAdmin`'s `CreateTopics` config
/// having survived on the broker rather than a default being echoed back.
fn describe_topic_configs(bootstrap: &str, topic: &str) -> String {
    describe_console_topic_configs(bootstrap, topic)
}

/// Create a one-partition topic through the JVM `kafka-topics` tool.
fn create_topic(bootstrap: &str, topic: &str) { create_console_topic_at(bootstrap, topic); }

/// A distributed Connect worker boots against krabka, runs a `FileStream`
/// source and sink over one topic, and the file survives the round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn connect_distributed_worker_round_trips_a_file_through_krabka() {
    let (broker, _dir) = jvm_acceptance::start_host_broker().await;
    let bootstrap = broker0_advertised();

    // The FileStream connectors do not create their topic, and the sink's
    // consumer would otherwise join a group over a topic that is not there.
    create_topic(bootstrap, DATA_TOPIC);

    let worker = ConnectWorker::start(bootstrap);
    worker.wait_for_rest().await;

    worker.put_connector(
        "krabka-file-source",
        &format!(
            r#"{{"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector",
                 "tasks.max":"1","file":"{SOURCE_FILE}","topic":"{DATA_TOPIC}"}}"#
        ),
    );
    worker.put_connector(
        "krabka-file-sink",
        &format!(
            r#"{{"connector.class":"org.apache.kafka.connect.file.FileStreamSinkConnector",
                 "tasks.max":"1","file":"{SINK_FILE}","topics":"{DATA_TOPIC}"}}"#
        ),
    );

    worker.wait_until_running("krabka-file-source", 1).await;
    worker.wait_until_running("krabka-file-sink", 1).await;
    worker.wait_for_round_trip().await;

    // The three `KafkaBasedLog` stores. Connect compacts all of them, because
    // each is a keyed log it replays in full on every worker start.
    for topic in [CONFIG_TOPIC, OFFSET_TOPIC, STATUS_TOPIC] {
        let configs = describe_topic_configs(bootstrap, topic);
        assert!(
            configs.contains("cleanup.policy=compact"),
            "the Connect store topic {topic} must carry cleanup.policy=compact; \
             kafka-configs said:\n{configs}\nworker logs:\n{}",
            worker.logs(),
        );
    }

    drop(worker);
    broker.shutdown().await;
}
