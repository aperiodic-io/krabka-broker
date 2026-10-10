//! Real JBOD exhaustion at the packaged-process boundary.
//!
//! Three packaged brokers host an RF=3 topic. Broker 1's two log directories
//! are real size-limited tmpfs volumes, held open so they survive the formatter
//! and broker containers. Produce first fills the extra directory, proving
//! partial JBOD failover and continued service, then fills the primary and
//! requires the all-directories-offline shutdown. Freeing both filesystems and
//! restarting must rebuild the replicas and preserve every acknowledged record.

mod support;

use std::{
    collections::BTreeSet,
    process::Command,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::Bytes;
use krabka_client_core::Client;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    assign_replicas_to_dirs_request::{
        AssignReplicasToDirsRequest, DirectoryData, PartitionData, TopicData,
    },
    create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
    describe_log_dirs_request::DescribeLogDirsRequest,
    fetch_request::FetchRequest,
};

use crate::support::fetch::{fetch_partition, single_partition_fetch};

const BROKER_IMAGE: &str = "docker.io/krabka-io/krabka-broker:dev";
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";
const ROOT: &str = "/var/lib/krabka";
const PRIMARY: &str = "/var/lib/krabka/data";
const EXTRA: &str = "/var/lib/krabka/enospc";
const TOPIC: &str = "jbod-real-enospc";
const PARTITIONS: i32 = 12;
const READY: Duration = Duration::from_secs(120);

fn image() -> String {
    std::env::var("KRABKA_BROKER_IMAGE").unwrap_or_else(|_| BROKER_IMAGE.to_owned())
}

fn docker(args: &[&str]) -> String {
    let output = Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("spawn docker {args:?}: {error}"));
    assert!(
        output.status.success(),
        "docker {args:?} exited {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("ephemeral address")
        .port()
}

fn metric_value(body: &str, name: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        (fields.next()? == name)
            .then(|| fields.next()?.parse().ok())
            .flatten()
    })
}

#[test]
fn prometheus_value_parser_ignores_help_and_other_metrics() {
    let body = "# HELP krabka_broker_offline_log_dirs count\n\
                another_metric 9\n\
                krabka_broker_offline_log_dirs 1\n";
    assert!(metric_value(body, "krabka_broker_offline_log_dirs") == Some(1.0));
    assert!(metric_value(body, "missing") == None);
}

struct TinyFs {
    volume: String,
    holder: String,
}

impl TinyFs {
    fn create(stem: &str, size: &str) -> Self {
        let volume = format!("{stem}-volume");
        let holder = format!("{stem}-holder");
        docker(&[
            "volume",
            "create",
            "--driver",
            "local",
            "--opt",
            "type=tmpfs",
            "--opt",
            "device=tmpfs",
            "--opt",
            &format!("o=size={size},mode=1777"),
            &volume,
        ]);
        let mount = format!("{volume}:/fs");
        docker(&[
            "run",
            "--detach",
            "--name",
            &holder,
            "--privileged",
            "--user",
            "0:0",
            "--volume",
            &mount,
            "--entrypoint",
            "/bin/sleep",
            KAFKA_IMAGE,
            "infinity",
        ]);
        Self { volume, holder }
    }

    fn free_topic_data(&self) {
        docker(&[
            "exec",
            &self.holder,
            "/bin/bash",
            "-c",
            "rm -rf /fs/jbod-real-enospc-*",
        ]);
    }

    fn grow(&self, size: &str) {
        docker(&[
            "exec",
            &self.holder,
            "/bin/mount",
            "-o",
            &format!("remount,size={size}"),
            "tmpfs",
            "/fs",
        ]);
    }
}

impl Drop for TinyFs {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.holder])
            .output();
        let _ = Command::new("docker")
            .args(["volume", "rm", "--force", &self.volume])
            .output();
    }
}

struct BrokerProcess {
    name: String,
    node_id: u32,
    client_port: u16,
    controller_port: u16,
    metrics_port: u16,
    root: tempfile::TempDir,
}

impl BrokerProcess {
    fn user(&self) -> String {
        use std::os::unix::fs::MetadataExt as _;
        let metadata = std::fs::metadata(self.root.path()).expect("stat broker root");
        format!("{}:{}", metadata.uid(), metadata.gid())
    }

    fn bootstrap(&self) -> String {
        format!("127.0.0.1:{}", self.client_port)
    }

    fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.metrics_port)
    }

    fn advertised_controller(&self) -> String {
        format!("host.docker.internal:{}", self.controller_port)
    }

    fn voter(&self) -> String {
        format!(
            "{}@{}:{}",
            self.node_id,
            self.advertised_controller(),
            directory_uuid(self.node_id)
        )
    }

    fn format(&self, cluster_id: &str, voters: &str, primary: Option<&TinyFs>) {
        let mount = format!("{}:{ROOT}", self.root.path().display());
        let user = self.user();
        let broker_image = image();
        let log_dir = format!("--log-dir={PRIMARY}");
        let cluster = format!("--cluster-id={cluster_id}");
        let initial = format!("--initial-controllers={voters}");
        let node = format!("--node-id={}", self.node_id);
        let directory = format!("--directory-id={}", directory_uuid(self.node_id));
        let mut args = vec!["run", "--rm", "--user", &user, "--volume", &mount];
        let primary_mount;
        if let Some(primary) = primary {
            primary_mount = format!("{}:{PRIMARY}", primary.volume);
            args.extend(["--volume", &primary_mount]);
        }
        args.extend([
            "--entrypoint",
            "/usr/bin/krabka-format",
            &broker_image,
            &log_dir,
            &cluster,
            &node,
            &directory,
            &initial,
            "--ignore-formatted",
        ]);
        docker(&args);
    }

    fn write_config(&self, voters: &str) {
        std::fs::create_dir_all(self.root.path().join("extra")).expect("create extra log dir");
        let extra = if self.node_id == 1 {
            EXTRA
        } else {
            "/var/lib/krabka/extra"
        };
        let config = format!(
            "broker_id = {id}\n\
             log_dir = \"{PRIMARY}\"\n\
             extra_log_dirs = [\"{extra}\"]\n\
             controller_quorum_voters = [{voters}]\n\n\
             [[listeners]]\n\
             name = \"PLAINTEXT\"\n\
             bind_addr = \"0.0.0.0:9092\"\n\
             advertised = \"host.docker.internal:{client}\"\n\
             protocol = \"Plaintext\"\n\n\
             [process]\n\
             roles = [\"controller\", \"broker\"]\n",
            id = self.node_id,
            client = self.client_port,
        );
        std::fs::write(self.root.path().join("broker.toml"), config).expect("write broker config");
    }

    fn run(&self, cluster_id: &str, primary: Option<&TinyFs>, extra: Option<&TinyFs>) {
        let root_mount = format!("{}:{ROOT}", self.root.path().display());
        let user = self.user();
        let client = format!("{}:9092", self.client_port);
        let controller = format!("{}:9093", self.controller_port);
        let metrics = format!("{}:9404", self.metrics_port);
        let broker_id = format!("--broker-id={}", self.node_id);
        let cluster_id = format!("--cluster-id={cluster_id}");
        let config = format!("--config-file={ROOT}/broker.toml");
        let broker_image = image();
        let mut args = vec![
            "run",
            "--detach",
            "--name",
            &self.name,
            "--user",
            &user,
            "--add-host",
            "host.docker.internal:host-gateway",
            "--volume",
            &root_mount,
        ];
        let primary_mount;
        if let Some(primary) = primary {
            primary_mount = format!("{}:{PRIMARY}", primary.volume);
            args.extend(["--volume", &primary_mount]);
        }
        let extra_mount;
        if let Some(extra) = extra {
            extra_mount = format!("{}:{EXTRA}", extra.volume);
            args.extend(["--volume", &extra_mount]);
        }
        args.extend([
            "--publish",
            &client,
            "--publish",
            &controller,
            "--publish",
            &metrics,
            &broker_image,
            &config,
            &broker_id,
            &cluster_id,
            "--metrics-listen-addr=0.0.0.0:9404",
            "--health-listen-addr=none",
        ]);
        docker(&args);
    }

    fn start_existing(&self) {
        docker(&["start", &self.name]);
    }

    fn is_running(&self) -> bool {
        docker(&["inspect", "--format", "{{.State.Running}}", &self.name]) == "true"
    }

    fn logs(&self) -> String {
        Command::new("docker")
            .args(["logs", "--tail", "80", &self.name])
            .output()
            .map_or_else(
                |error| format!("could not read logs: {error}"),
                |output| {
                    format!(
                        "{}{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    )
                },
            )
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        crate::support::remove_container_with_volumes(&self.name);
    }
}

struct Cluster {
    brokers: Vec<BrokerProcess>,
    primary: TinyFs,
    extra: TinyFs,
}

impl Cluster {
    fn start() -> Self {
        let stem = format!("krabka-jbod-enospc-{}", std::process::id());
        let primary = TinyFs::create(&format!("{stem}-primary"), "32m");
        let extra = TinyFs::create(&format!("{stem}-extra"), "16m");
        let cluster_id = uuid::Uuid::new_v4().to_string();
        let brokers = (1..=3)
            .map(|node_id| BrokerProcess {
                name: format!("{stem}-broker-{node_id}"),
                node_id,
                client_port: free_port(),
                controller_port: free_port(),
                metrics_port: free_port(),
                root: tempfile::TempDir::new().expect("broker root"),
            })
            .collect::<Vec<_>>();
        let initial = brokers
            .iter()
            .map(BrokerProcess::voter)
            .collect::<Vec<_>>()
            .join(",");
        let voters = brokers
            .iter()
            .map(|broker| format!("\"{}@{}\"", broker.node_id, broker.advertised_controller()))
            .collect::<Vec<_>>()
            .join(",");
        for broker in &brokers {
            broker.format(
                &cluster_id,
                &initial,
                (broker.node_id == 1).then_some(&primary),
            );
            broker.write_config(&voters);
        }
        for broker in &brokers {
            let limited = broker.node_id == 1;
            broker.run(
                &cluster_id,
                limited.then_some(&primary),
                limited.then_some(&extra),
            );
        }
        Self {
            brokers,
            primary,
            extra,
        }
    }

    fn bootstrap(&self) -> String {
        self.brokers
            .iter()
            .map(BrokerProcess::bootstrap)
            .collect::<Vec<_>>()
            .join(",")
    }
}

async fn create_topic(bootstrap: &str) {
    let deadline = Instant::now() + READY;
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(client) = Client::builder().bootstrap(bootstrap).build().await {
            match client
                .send(CreateTopicsRequest {
                    topics: vec![crate::support::topics::creatable_topic_with_configs(
                        crate::support::topics::ConfiguredTopicSetup {
                            name: TOPIC.into(),
                            partitions: crate::support::topics::TopicPartitionCount(PARTITIONS),
                            replicas: crate::support::topics::TopicReplicationFactor(3),
                            configs: vec![CreatableTopicConfig {
                                name: "min.insync.replicas".into(),
                                value: Some("2".into()),
                                ..Default::default()
                            }],
                        },
                    )],
                    timeout_ms: 10_000,
                    ..Default::default()
                })
                .await
            {
                Ok(response) if response.topics.iter().all(|topic| topic.error_code == 0) => return,
                Ok(response) => last = format!("{:?}", response.topics),
                Err(error) => last = error.to_string(),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("could not create {TOPIC} within {READY:?}: {last}");
}

async fn describe(
    bootstrap: &str,
) -> krabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResponse {
    crate::support::client::connect_with_context(bootstrap, None, "describe client")
        .await
        .send(DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .expect("DescribeLogDirs")
}

fn partitions_in(
    response: &krabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResponse,
    dir: &str,
) -> Vec<i32> {
    response
        .results
        .iter()
        .find(|result| result.log_dir == dir)
        .into_iter()
        .flat_map(|result| &result.topics)
        .filter(|topic| topic.name == TOPIC)
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .map(|partition| partition.partition_index)
        })
        .collect()
}

/// The metadata directory id that `node_id` is formatted with: fixed, so the
/// voter set can name it, and outside the 100 lowest ids, which Kafka reserves
/// and the broker refuses at startup.
fn directory_uuid(node_id: u32) -> uuid::Uuid {
    uuid::Uuid::from_u64_pair(1, u64::from(node_id))
}

fn directory_id(fs: &TinyFs) -> uuid::Uuid {
    let raw = docker(&["exec", &fs.holder, "/bin/cat", "/fs/meta.properties"]);
    krabka_format::MetaProperties::parse(raw.as_bytes())
        .expect("a Kafka meta.properties")
        .directory_id
        .expect("meta.properties directory.id")
        .into()
}

async fn assign_node1_dirs(cluster: &Cluster, primary: &[i32], extra: &[i32]) {
    let metadata = crate::support::client::connect_with_context(
        cluster.bootstrap(),
        None,
        "assignment metadata client",
    )
    .await
    .send(crate::support::discovery::named_topic_metadata(TOPIC))
    .await
    .expect("assignment Metadata");
    let controller = usize::try_from(metadata.controller_id - 1).expect("positive controller id");
    let topic_id = metadata.topics[0].topic_id;
    let directory = |id: uuid::Uuid, partitions: &[i32]| DirectoryData {
        id: krabka_protocol::primitives::uuid::Uuid(id.into_bytes()),
        topics: vec![TopicData {
            topic_id,
            partitions: partitions
                .iter()
                .map(|partition| PartitionData {
                    partition_index: *partition,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = crate::support::client::connect_with_context(
        cluster.brokers[controller].bootstrap(),
        None,
        "assignment controller client",
    )
    .await
    .send(AssignReplicasToDirsRequest {
        broker_id: 1,
        // -1 means "not provided" (KIP-903) and matches any
        // registration; the controller still checks that broker 1 is
        // registered at all (#636).
        broker_epoch: -1,
        directories: vec![
            directory(directory_id(&cluster.primary), primary),
            directory(directory_id(&cluster.extra), extra),
        ],
        ..Default::default()
    })
    .await
    .expect("AssignReplicasToDirs");
    assert!(
        response.error_code == 0
            && response.directories.iter().all(|directory| {
                directory.topics.iter().all(|topic| {
                    topic
                        .partitions
                        .iter()
                        .all(|partition| partition.error_code == 0)
                })
            }),
        "controller rejected node 1 directory assignments: {response:?}"
    );
}

async fn leaders(bootstrap: &str) -> Vec<i32> {
    let response = crate::support::client::connect_with_context(bootstrap, None, "metadata client")
        .await
        .send(crate::support::discovery::named_topic_metadata(TOPIC))
        .await
        .expect("Metadata");
    let mut leaders = vec![-1; usize::try_from(PARTITIONS).unwrap()];
    for partition in &response.topics[0].partitions {
        leaders[usize::try_from(partition.partition_index).unwrap()] = partition.leader_id;
    }
    leaders
}

async fn wait_isr(bootstrap: &str, partition: i32, expected: usize) {
    let deadline = Instant::now() + READY;
    loop {
        let response =
            crate::support::client::connect_with_context(bootstrap, None, "ISR metadata client")
                .await
                .send(crate::support::discovery::named_topic_metadata(TOPIC))
                .await
                .expect("ISR Metadata");
        if response.topics[0]
            .partitions
            .iter()
            .find(|candidate| candidate.partition_index == partition)
            .is_some_and(|candidate| candidate.isr_nodes.len() == expected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "partition {partition} did not reach ISR size {expected}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn record(partition: i32, key: String, bytes: usize) -> ProducerRecord {
    crate::support::producer::producer_record(crate::support::producer::ProducerRecordSetup {
        topic: (TOPIC).into(),
        partition: Some(krabka_ids::PartitionIndex(partition)),
        key: Some(key.into()),
        value: Some(Bytes::from(vec![b'x'; bytes])),
    })
}

async fn producer(bootstrap: &str) -> Producer {
    crate::support::producer::no_retry_acks_all_producer(bootstrap, Some(512 * 1024), "producer")
        .await
}

async fn wait_metric(url: &str, expected: f64, broker: &BrokerProcess) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + READY;
    while Instant::now() < deadline {
        if let Ok(response) = client.get(url).send().await
            && let Ok(body) = response.text().await
            && metric_value(&body, "krabka_broker_offline_log_dirs") == Some(expected)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!(
        "{} did not report offline_log_dirs={expected}: {}",
        broker.name,
        broker.logs()
    );
}

async fn wait_exited(broker: &BrokerProcess) {
    let deadline = Instant::now() + READY;
    while broker.is_running() {
        assert!(
            Instant::now() < deadline,
            "{} did not shut down after every log dir went offline: {}",
            broker.name,
            broker.logs()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn read_partition_keys(cluster: &Cluster, partition: i32) -> BTreeSet<String> {
    let metadata = crate::support::client::connect_with_context(
        cluster.bootstrap(),
        None,
        "read metadata client",
    )
    .await
    .send(crate::support::discovery::named_topic_metadata(TOPIC))
    .await
    .expect("read Metadata");
    let topic = &metadata.topics[0];
    let leader = topic
        .partitions
        .iter()
        .find(|candidate| candidate.partition_index == partition)
        .expect("read partition metadata")
        .leader_id;
    let client = crate::support::client::connect_with_context(
        cluster.brokers[usize::try_from(leader - 1).unwrap()].bootstrap(),
        None,
        "fetch client",
    )
    .await;
    let mut offset = 0;
    let mut keys = BTreeSet::new();
    loop {
        let response = client
            .send(FetchRequest {
                replica_id: -1,
                ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
                    topic: TOPIC.into(),
                    topic_id: topic.topic_id,
                    partition: fetch_partition(crate::support::fetch::FetchPartitionSetup {
                        partition: krabka_ids::PartitionIndex(partition),
                        offset: krabka_ids::Offset(offset),
                        maximum: crate::support::fetch::FetchByteLimit(1 << 24),
                    }),
                    limits: crate::support::fetch::FetchLimits::wait_for_data_with_maximum(
                        crate::support::fetch::RequestWaitMillis(0),
                        crate::support::fetch::FetchByteLimit(1 << 24),
                    ),
                })
            })
            .await
            .expect("Fetch");
        let fetched = &response.responses[0].partitions[0];
        assert!(fetched.error_code == 0, "Fetch failed: {fetched:?}");
        let Some(batches) = fetched.records.as_ref().and_then(|records| records.as_v2()) else {
            break;
        };
        if batches.is_empty() {
            break;
        }
        for batch in batches {
            offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
            for record in &batch.records {
                if let Some(key) = &record.key {
                    keys.insert(String::from_utf8_lossy(key).into_owned());
                }
            }
        }
    }
    keys
}

async fn wait_keys(cluster: &Cluster, partitions: &[i32], expected: &BTreeSet<String>) {
    let deadline = Instant::now() + READY;
    loop {
        let mut visible = BTreeSet::new();
        for partition in partitions {
            visible.extend(read_partition_keys(cluster, *partition).await);
        }
        let missing = expected.difference(&visible).collect::<Vec<_>>();
        if missing.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "acknowledged records did not become visible: {missing:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker and //packaging:image"]
async fn real_enospc_moves_leadership_and_preserves_acked_records_across_restart() {
    let cluster = Cluster::start();
    let bootstrap = cluster.bootstrap();
    create_topic(&bootstrap).await;

    let node1 = &cluster.brokers[0];
    let deadline = Instant::now() + READY;
    let (tiny_partitions, primary_partitions) = loop {
        let described = describe(&node1.bootstrap()).await;
        let tiny = partitions_in(&described, EXTRA);
        let primary = partitions_in(&described, PRIMARY);
        if !tiny.is_empty() && !primary.is_empty() {
            break (tiny, primary);
        }
        assert!(
            Instant::now() < deadline,
            "replicas did not spread across both node-1 dirs"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let before = leaders(&node1.bootstrap()).await;
    let doomed = *tiny_partitions
        .iter()
        .find(|partition| before[usize::try_from(**partition).unwrap()] == 1)
        .expect("a node-1-led partition must live on the tiny dir");
    let healthy = *primary_partitions
        .iter()
        .find(|partition| before[usize::try_from(**partition).unwrap()] == 1)
        .expect("a node-1-led partition must live on the primary dir");
    assign_node1_dirs(&cluster, &primary_partitions, &tiny_partitions).await;
    wait_isr(&bootstrap, doomed, 3).await;
    wait_isr(&bootstrap, healthy, 3).await;

    let fill = producer(&bootstrap).await;
    let mut acked = BTreeSet::new();
    fill_partition(&fill, doomed, "fill", 128, &mut acked).await;
    wait_metric(&node1.metrics_url(), 1.0, node1).await;

    let failed = describe(&node1.bootstrap()).await;
    let extra = failed
        .results
        .iter()
        .find(|result| result.log_dir == EXTRA)
        .expect("extra dir result");
    let primary = failed
        .results
        .iter()
        .find(|result| result.log_dir == PRIMARY)
        .expect("primary dir result");
    assert!(
        extra.error_code == 56,
        "ENOSPC must surface as KAFKA_STORAGE_ERROR: {extra:?}"
    );
    assert!(
        primary.error_code == 0,
        "primary directory stays online: {primary:?}"
    );

    let healthy_key = "healthy-primary".to_owned();
    let healthy_producer = producer(&bootstrap).await;
    assert!(
        healthy_producer
            .send(record(healthy, healthy_key.clone(), 16))
            .await
            .is_ok(),
        "a partition on node 1's healthy directory must remain writable"
    );
    acked.insert(healthy_key);

    let primary_fill = producer(&bootstrap).await;
    fill_partition(&primary_fill, healthy, "primary-fill", 256, &mut acked).await;
    wait_exited(node1).await;
    let deadline = Instant::now() + READY;
    loop {
        let now = leaders(&cluster.brokers[1].bootstrap()).await;
        if [doomed, healthy].into_iter().all(|partition| {
            let leader = now[usize::try_from(partition).unwrap()];
            leader > 0 && leader != 1
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "leaders for exhausted node-1 partitions did not move off node 1: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    wait_keys(&cluster, &[doomed, healthy], &acked).await;

    cluster.primary.free_topic_data();
    cluster.extra.free_topic_data();
    cluster.primary.grow("64m");
    cluster.extra.grow("64m");
    node1.start_existing();
    wait_metric(&node1.metrics_url(), 0.0, node1).await;
    let deadline = Instant::now() + READY;
    loop {
        let recovered = describe(&node1.bootstrap()).await;
        let primary = recovered
            .results
            .iter()
            .find(|result| result.log_dir == PRIMARY);
        let extra = recovered
            .results
            .iter()
            .find(|result| result.log_dir == EXTRA);
        if primary.is_some_and(|result| {
            result.error_code == 0 && !partitions_in(&recovered, PRIMARY).is_empty()
        }) && extra.is_some_and(|result| {
            result.error_code == 0 && !partitions_in(&recovered, EXTRA).is_empty()
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node 1 did not rebuild replicas on both recovered directories"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    wait_isr(&bootstrap, doomed, 3).await;
    wait_isr(&bootstrap, healthy, 3).await;
    wait_keys(&cluster, &[doomed, healthy], &acked).await;
}

async fn fill_partition(
    producer: &Producer,
    partition: i32,
    prefix: &str,
    attempts: usize,
    acked: &mut BTreeSet<String>,
) {
    for sequence in 0..attempts {
        let key = format!("{prefix}-{sequence}");
        match producer
            .send(record(partition, key.clone(), 256 * 1024))
            .await
        {
            Ok(_) => {
                acked.insert(key);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            _ => break,
        }
    }
}
