//! A krabka cluster of broker processes on the loopback interface, for the
//! local benchmark harness (`krabka-bench-local`).
//!
//! Every node runs the combined `controller,broker` roles from the release
//! binaries, against a static KIP-595 voter set. The cluster owns its child
//! processes and kills them when it drops, so a harness that fails part way
//! never leaves a broker holding a port.

use {
    anyhow::{Context, Result, bail},
    bon::Builder,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    std::{
        fs::{self, File},
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        time::Duration,
    },
    tokio::{
        net::TcpStream,
        time::{Instant, sleep},
    },
    tracing::info,
};

/// `TOPIC_ALREADY_EXISTS`.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// The fixed cluster id every local node is formatted with. A local run
/// starts from empty directories, so it never meets another cluster's data.
const CLUSTER_ID: &str = "DX4vWpscTB6KPyttHkyfEA";

/// The most bytes the benchmark topic keeps per partition replica.
const RETENTION_BYTES: u64 = 256 * 1024 * 1024;
/// The segment size of the benchmark topic, small enough that retention can
/// delete in steps well below [`RETENTION_BYTES`].
const SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
/// How often the brokers apply retention, as a duration flag value.
const RETENTION_CHECK_INTERVAL: &str = "2s";

/// How long the harness waits for the cluster, or a topic, to come up.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// The ports one node listens on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePorts {
    /// The client listener.
    pub client: u16,
    /// The controller (raft) listener.
    pub controller: u16,
    /// The Prometheus `/metrics` endpoint.
    pub metrics: u16,
}

impl NodePorts {
    /// The ports of node `index` (zero-based) for a cluster based at
    /// `base_port`. Each node takes a block of ten ports.
    #[must_use]
    pub fn of(base_port: u16, index: u16) -> Self {
        let block = base_port + index * 10;
        Self {
            client: block,
            controller: block + 1,
            metrics: block + 2,
        }
    }
}

/// How to lay out and start the local cluster.
#[derive(Debug, Clone, Builder)]
pub struct LocalClusterSpec {
    /// Directory holding `krabka-broker` and `krabka-format`.
    pub bin_dir: PathBuf,
    /// Root of the node data directories and logs. It is wiped first.
    pub work_dir: PathBuf,
    /// Number of nodes.
    #[builder(default = 3)]
    pub nodes: u16,
    /// First port of node 1's port block.
    #[builder(default = 19_100)]
    pub base_port: u16,
    /// Extra arguments passed to every broker.
    #[builder(default)]
    pub broker_args: Vec<String>,
    /// `RUST_LOG` for the brokers.
    #[builder(default = "warn".to_owned())]
    pub broker_log: String,
    /// Extra environment variables for every broker, as `KEY=VALUE`.
    #[builder(default)]
    pub broker_env: Vec<String>,
}

/// A running local cluster. Dropping it kills every node.
pub struct LocalCluster {
    children: Vec<Child>,
    bootstrap: String,
    nodes: u16,
    /// The node data directories, deleted when the cluster drops. A
    /// saturating run writes gigabytes; the logs and metrics beside them stay.
    data_dirs: Vec<PathBuf>,
}

impl LocalCluster {
    /// Formats every node's directory, starts the nodes, and waits until every
    /// broker has registered.
    ///
    /// # Errors
    ///
    /// When a binary is missing, a format fails, a node exits, or the cluster
    /// is not up within a minute.
    pub async fn start(spec: &LocalClusterSpec) -> Result<Self> {
        let broker_bin = spec.bin_dir.join("krabka-broker");
        let format_bin = spec.bin_dir.join("krabka-format");
        for bin in [&broker_bin, &format_bin] {
            if !bin.is_file() {
                bail!(
                    "{} is missing; build it with cargo build --release",
                    bin.display()
                );
            }
        }
        if spec.work_dir.exists() {
            fs::remove_dir_all(&spec.work_dir)
                .with_context(|| format!("clear {}", spec.work_dir.display()))?;
        }
        fs::create_dir_all(&spec.work_dir)?;

        let voters = (0..spec.nodes)
            .map(|index| {
                let ports = NodePorts::of(spec.base_port, index);
                format!("{}@127.0.0.1:{}", index + 1, ports.controller)
            })
            .collect::<Vec<_>>()
            .join(",");

        let mut cluster = Self {
            children: Vec::new(),
            bootstrap: (0..spec.nodes)
                .map(|index| format!("127.0.0.1:{}", NodePorts::of(spec.base_port, index).client))
                .collect::<Vec<_>>()
                .join(","),
            nodes: spec.nodes,
            data_dirs: (0..spec.nodes)
                .map(|index| spec.work_dir.join(format!("node-{}", index + 1)))
                .collect(),
        };
        for index in 0..spec.nodes {
            let child = start_node(spec, &broker_bin, &format_bin, index, &voters)?;
            cluster.children.push(child);
        }
        cluster.wait_registered().await?;
        Ok(cluster)
    }

    /// The comma-separated bootstrap list of every node's client listener.
    #[must_use]
    pub fn bootstrap(&self) -> &str { &self.bootstrap }

    /// Saves every node's `/metrics` page as `node-N.metrics` in the work
    /// directory, for reading after the run.
    ///
    /// # Errors
    ///
    /// When a page cannot be fetched or written.
    pub async fn save_metrics(&self, spec: &LocalClusterSpec) -> Result<()> {
        let http = reqwest::Client::new();
        for index in 0..self.nodes {
            let port = NodePorts::of(spec.base_port, index).metrics;
            let page = http
                .get(format!("http://127.0.0.1:{port}/metrics"))
                .send()
                .await?
                .text()
                .await?;
            fs::write(
                spec.work_dir.join(format!("node-{}.metrics", index + 1)),
                page,
            )?;
        }
        Ok(())
    }

    async fn admin(&self) -> Result<Client> {
        Client::builder()
            .bootstrap(self.bootstrap.clone())
            .client_id("bench-local-admin")
            .build()
            .await
            .context("admin client")
    }

    /// Whether every node's client listener accepts a connection, so the
    /// admin client is not built against a node that is still starting.
    async fn all_listening(&self) -> bool {
        for address in self.bootstrap.split(',') {
            if TcpStream::connect(address).await.is_err() {
                return false;
            }
        }
        true
    }

    fn check_alive(&mut self) -> Result<()> {
        for (index, child) in self.children.iter_mut().enumerate() {
            if let Some(status) = child.try_wait()? {
                bail!("node {} exited early with {status}", index + 1);
            }
        }
        Ok(())
    }

    async fn wait_registered(&mut self) -> Result<()> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            self.check_alive()?;
            if self.all_listening().await
                && let Ok(client) = self.admin().await
                && let Ok(metadata) = client.send(MetadataRequest::default()).await
                && metadata.brokers.len() == usize::from(self.nodes)
            {
                info!(nodes = self.nodes, "local cluster registered");
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("cluster did not register {} brokers in time", self.nodes);
            }
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Creates `topic` and waits until every partition has a leader and a
    /// full in-sync replica set.
    ///
    /// The topic keeps at most [`RETENTION_BYTES`] per partition, in
    /// [`SEGMENT_BYTES`] segments, so a saturating run does not fill the disk
    /// it shares with the build.
    ///
    /// # Errors
    ///
    /// When the broker refuses the topic or it is not ready within a minute.
    pub async fn create_topic(
        &mut self,
        topic: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        let client = self.admin().await?;
        let response = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: topic.to_owned(),
                    num_partitions: partitions,
                    replication_factor,
                    configs: vec![
                        CreatableTopicConfig {
                            name: "retention.bytes".to_owned(),
                            value: Some(RETENTION_BYTES.to_string()),
                            ..Default::default()
                        },
                        CreatableTopicConfig {
                            name: "segment.bytes".to_owned(),
                            value: Some(SEGMENT_BYTES.to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            })
            .await
            .context("CreateTopics")?;
        let code = response.topics.first().map_or(-1, |topic| topic.error_code);
        if code != 0 && code != TOPIC_ALREADY_EXISTS {
            bail!("CreateTopics {topic}: error code {code}");
        }
        let want_isr = usize::try_from(replication_factor).unwrap_or_default();
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            self.check_alive()?;
            let metadata = client
                .send(MetadataRequest {
                    topics: Some(vec![MetadataRequestTopic {
                        name: Some(topic.to_owned()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                })
                .await
                .context("Metadata")?;
            let ready = metadata.topics.first().is_some_and(|described| {
                described.error_code == 0
                    && described.partitions.len() == usize::try_from(partitions).unwrap_or_default()
                    && described.partitions.iter().all(|partition| {
                        partition.leader_id >= 0 && partition.isr_nodes.len() == want_isr
                    })
            });
            if ready {
                info!(topic, partitions, replication_factor, "topic ready");
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("topic {topic} did not become ready in time");
            }
            sleep(Duration::from_millis(250)).await;
        }
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
        }
        for child in &mut self.children {
            let _ = child.wait();
        }
        for dir in &self.data_dirs {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

/// Formats node `index` and starts it, with its output in `node-N.log`.
fn start_node(
    spec: &LocalClusterSpec,
    broker_bin: &Path,
    format_bin: &Path,
    index: u16,
    voters: &str,
) -> Result<Child> {
    let node_id = (index + 1).to_string();
    let ports = NodePorts::of(spec.base_port, index);
    let data_dir = spec.work_dir.join(format!("node-{node_id}"));
    let status = Command::new(format_bin)
        .arg("--log-dir")
        .arg(&data_dir)
        .args(["--node-id", &node_id, "--cluster-id", CLUSTER_ID])
        .stdout(Stdio::null())
        .status()
        .context("run krabka-format")?;
    if !status.success() {
        bail!("krabka-format for node {node_id} failed with {status}");
    }
    let log = File::create(spec.work_dir.join(format!("node-{node_id}.log")))?;
    let client_addr = format!("127.0.0.1:{}", ports.client);
    Command::new(broker_bin)
        .arg("--log-dir")
        .arg(&data_dir)
        .args([
            "--broker-id",
            &node_id,
            "--cluster-id",
            CLUSTER_ID,
            "--listen-addr",
            &client_addr,
            "--advertised-listener",
            &client_addr,
            "--controller-listen-addr",
            &format!("127.0.0.1:{}", ports.controller),
            "--controller-quorum-voters",
            voters,
            "--metrics-listen-addr",
            &format!("127.0.0.1:{}", ports.metrics),
            "--health-listen-addr",
            "none",
            "--log-retention-check-interval",
            RETENTION_CHECK_INTERVAL,
        ])
        .args(&spec.broker_args)
        .env("RUST_LOG", &spec.broker_log)
        .envs(
            spec.broker_env
                .iter()
                .filter_map(|pair| pair.split_once('=')),
        )
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()
        .with_context(|| format!("start node {node_id}"))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn node_ports_take_a_block_of_ten() {
        check!(
            NodePorts::of(19_100, 0)
                == NodePorts {
                    client: 19_100,
                    controller: 19_101,
                    metrics: 19_102,
                }
        );
        check!(
            NodePorts::of(19_100, 2)
                == NodePorts {
                    client: 19_120,
                    controller: 19_121,
                    metrics: 19_122,
                }
        );
    }
}
