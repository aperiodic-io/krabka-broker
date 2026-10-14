//! What the recovery suites share: keyed batches, a broker that restarts on
//! its own directory, the damage a crash leaves in a segment file, direct
//! connections to one broker of a cluster, and the cluster steps of a
//! failover.

use {
    crate::support::{
        self,
        client::connect_client,
        dedup::{
            PartitionContents, Rec, create_dedup_topic, keyed_batch, partition_metric, produce,
            read_partition,
        },
        discovery::named_topic_metadata,
        produce::{
            ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
            batch_request,
        },
        topic_on,
        topics::{create_topic_request, topic_configs},
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::{Client, ClientError, Connection, ConnectionOptions},
    krabka_metadata::PartitionRecord,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse,
        primitives::uuid::Uuid,
        records::{CRC_COVERAGE_START, HEADER_LEN, RecordBatch, RecordBatchHeader},
    },
    std::{
        net::SocketAddr,
        path::{Path, PathBuf},
    },
    tempfile::TempDir,
    zerocopy::FromBytes,
};

/// The window every suite uses unless it tests the window itself: far longer
/// than any scenario runs, so nothing expires by accident.
pub const WINDOW_MS: i64 = 600_000;

/// The file a graceful stop leaves in the log directory as its proof.
const CLEAN_SHUTDOWN_PROOF: &str = ".kafka_cleanshutdown";

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One batch holding a record for each of `keys`, in order, created now.
pub fn keyed(keys: &[&str]) -> RecordBatch {
    let records: Vec<Rec> = keys.iter().map(|key| Rec::key(key)).collect();
    keyed_batch().records(&records).call()
}

/// `(offset, key)` pairs as [`PartitionContents::offsets_and_keys`] reads
/// them.
pub fn at(pairs: &[(i64, &str)]) -> Vec<(i64, String)> {
    pairs
        .iter()
        .map(|(offset, key)| (*offset, (*key).to_owned()))
        .collect()
}

/// Every key that appears more than once in a partition, in offset order.
pub fn repeated_keys(contents: &PartitionContents) -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    contents
        .keys()
        .into_iter()
        .filter(|key| !seen.insert(key.clone()))
        .collect()
}

/// The two per-partition KFC-10 counters these suites read.
#[derive(Debug, PartialEq)]
pub struct DedupCounters {
    pub dropped: f64,
    pub index_rebuilds: f64,
}

pub async fn dedup_counters(broker: &BrokerHandle, topic: &str) -> DedupCounters {
    DedupCounters {
        dropped: partition_metric(broker, "dedup_records_dropped_total", topic, 0).await,
        index_rebuilds: partition_metric(broker, "dedup_index_rebuilds_total", topic, 0).await,
    }
}

// ---------------------------------------------------------------------------
// One broker on a directory it keeps across restarts
// ---------------------------------------------------------------------------

/// A broker booted on a caller-owned directory, its client, and the id of the
/// one topic under test.
pub struct SingleBroker {
    pub broker: BrokerHandle,
    pub client: Client,
    pub topic: String,
    pub topic_id: Uuid,
}

impl SingleBroker {
    /// Produce one batch to partition 0 with `acks=all`.
    pub async fn produce(&self, batch: RecordBatch) -> PartitionProduceResponse {
        produce()
            .client(&self.client)
            .topic(&self.topic)
            .topic_id(self.topic_id)
            .batch(batch)
            .call()
            .await
    }

    /// Everything partition 0 holds, as a consumer reads it.
    pub async fn read(&self) -> PartitionContents {
        read_partition(&self.client, &self.topic, self.topic_id, 0).await
    }

    pub async fn counters(&self) -> DedupCounters {
        dedup_counters(&self.broker, &self.topic).await
    }

    /// Wait until the produce path sees `config` as `value`.
    ///
    /// The produce handler reads the topic config from the same image this
    /// waits on, so the first produce after it runs under the new rule.
    pub async fn await_topic_config(&self, config: &str, value: Option<&str>) {
        let topic = self.topic.clone();
        self.broker
            .wait_for_image(|image| {
                image
                    .topic_config(&topic)
                    .and_then(|configs| configs.get(config))
                    .map(String::as_str)
                    == value
            })
            .await;
    }

    pub async fn shutdown(self) { self.broker.shutdown().await; }
}

/// Boot a broker on the empty directory `dir` and create `topic` on it.
#[bon::builder]
pub async fn first_boot(
    dir: &Path,
    topic: &str,
    #[builder(default = WINDOW_MS)] window_ms: i64,
    key_header: Option<&str>,
    #[builder(default = &[])] extra_configs: &[(&str, &str)],
) -> SingleBroker {
    let (broker, client) = support::start_with_dir(dir).await;
    let topic_id = create_dedup_topic()
        .broker(&broker)
        .client(&client)
        .topic(topic)
        .window_ms(window_ms)
        .maybe_key_header(key_header)
        .extra_configs(extra_configs)
        .call()
        .await;
    broker
        .wait_until_local_partition_leader(topic, 0, NodeId(1))
        .await;
    SingleBroker {
        broker,
        client,
        topic: topic.to_owned(),
        topic_id,
    }
}

/// Boot the broker again on `dir`, which already holds `topic`, and wait
/// until it leads the partition again.
pub async fn reboot(dir: &Path, topic: &str) -> SingleBroker {
    let (broker, client) = support::start_with_dir(dir).await;
    broker
        .wait_until_local_partition_leader(topic, 0, NodeId(1))
        .await;
    let topic_id = support::topic_id_for(&client, topic).await;
    SingleBroker {
        broker,
        client,
        topic: topic.to_owned(),
        topic_id,
    }
}

// ---------------------------------------------------------------------------
// The damage a crash leaves on disk
// ---------------------------------------------------------------------------

/// What a stop leaves behind for the next boot to recover from.
#[derive(Clone, Copy, Debug)]
pub enum Damage {
    /// Nothing: a graceful stop.
    None,
    /// The clean-shutdown proof is gone, as after a crash: the next boot
    /// registers as an unclean restart.
    NoCleanShutdownProof,
    /// The first half of the last batch is written again after it, as a
    /// write that power loss interrupted leaves it. Every complete batch
    /// survives, and recovery cuts the torn copy.
    TornCopy,
    /// The last batch itself is cut in half and its record is lost. The
    /// clean-shutdown proof goes too, as no graceful stop leaves a torn
    /// batch.
    LastBatchCut,
}

impl Damage {
    /// Damage the files of `topic`'s partition 0 under `log_dir`.
    pub fn apply(self, log_dir: &Path, topic: &str) {
        match self {
            Self::None => {}
            Self::NoCleanShutdownProof => remove_clean_shutdown_proof(log_dir),
            Self::TornCopy => {
                remove_clean_shutdown_proof(log_dir);
                tear_tail(&active_segment(log_dir, topic));
            }
            Self::LastBatchCut => {
                remove_clean_shutdown_proof(log_dir);
                cut_last_batch(&active_segment(log_dir, topic));
            }
        }
    }
}

fn remove_clean_shutdown_proof(log_dir: &Path) {
    std::fs::remove_file(log_dir.join(CLEAN_SHUTDOWN_PROOF))
        .expect("a graceful stop leaves the clean-shutdown proof");
}

/// One complete batch of a segment file, by byte range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SegmentBatch {
    start: usize,
    end: usize,
}

/// The complete, CRC-valid batches at the start of a segment file, and the
/// number of bytes after the last of them.
fn walk_batches(bytes: &[u8]) -> (Vec<SegmentBatch>, usize) {
    let mut batches = Vec::new();
    let mut at = 0usize;
    while let Some(header_bytes) = bytes.get(at..at + HEADER_LEN) {
        let Ok(header) = RecordBatchHeader::ref_from_bytes(header_bytes) else {
            break;
        };
        let Ok(declared) = usize::try_from(header.batch_length.get()) else {
            break;
        };
        // `batch_length` counts everything after itself, and it and
        // `base_offset` are the first twelve bytes.
        let end = at + 12 + declared;
        let Some(batch) = bytes.get(at..end).filter(|_| end >= at + HEADER_LEN) else {
            break;
        };
        if header.magic != 2 || crc32c::crc32c(&batch[CRC_COVERAGE_START..]) != header.crc.get() {
            break;
        }
        batches.push(SegmentBatch { start: at, end });
        at = end;
    }
    (batches, bytes.len() - at)
}

/// The newest `.log` segment of `topic`'s partition 0 under `log_dir`.
fn active_segment(log_dir: &Path, topic: &str) -> PathBuf {
    let partition_dir = log_dir.join(format!("{topic}-0"));
    let mut segments: Vec<PathBuf> = std::fs::read_dir(&partition_dir)
        .unwrap_or_else(|error| panic!("read {}: {error}", partition_dir.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .collect();
    segments.sort();
    segments
        .pop()
        .unwrap_or_else(|| panic!("no segment in {}", partition_dir.display()))
}

/// The last complete batch of a segment that ends on a batch boundary, and
/// the length of its first half.
fn last_batch_and_half(bytes: &[u8], segment: &Path) -> (SegmentBatch, usize) {
    let (batches, tail) = walk_batches(bytes);
    assert!(
        tail == 0,
        "{} must end on a batch boundary before it is damaged",
        segment.display()
    );
    let last = *batches.last().expect("the segment holds a batch");
    let half = (last.end - last.start) / 2;
    // Twelve bytes is `base_offset` plus `batch_length`, so what is left
    // states a length that nothing follows.
    assert!(
        half >= 12,
        "a batch of {} bytes is too short to tear",
        last.end - last.start
    );
    (last, half)
}

/// Write the first half of the segment's last complete batch again after
/// it, as `process_kill_durability` tears a tail: a header that promises more
/// bytes than follow it, built from bytes the broker wrote itself.
fn tear_tail(segment: &Path) {
    let bytes = std::fs::read(segment).expect("read the segment");
    let (last, half) = last_batch_and_half(&bytes, segment);
    let torn = [&bytes[..], &bytes[last.start..last.start + half]].concat();
    std::fs::write(segment, &torn).expect("write the torn segment");
    assert!(
        walk_batches(&torn).1 == half,
        "the tear leaves a partial batch"
    );
}

/// Cut the segment's last complete batch in half, so that its records are
/// lost and recovery ends the log at its base offset.
fn cut_last_batch(segment: &Path) {
    let bytes = std::fs::read(segment).expect("read the segment");
    let (last, half) = last_batch_and_half(&bytes, segment);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(segment)
        .expect("open the segment for the cut");
    file.set_len(u64::try_from(last.start + half).expect("segment size"))
        .expect("cut the last batch");
    let cut = std::fs::read(segment).expect("read the cut segment");
    assert!(
        walk_batches(&cut).1 == half,
        "the cut leaves a partial batch"
    );
}

// ---------------------------------------------------------------------------
// One broker of a cluster, over the wire
// ---------------------------------------------------------------------------

/// A connection to exactly one broker. A `Client` routes a request to the
/// least loaded broker it knows, which in a cluster need not be the leader.
pub async fn connect(addr: SocketAddr) -> Connection {
    Connection::connect(addr, ConnectionOptions {
        client_id: "dedup-recovery".into(),
        ..Default::default()
    })
    .await
    .unwrap_or_else(|error| panic!("connect to {addr}: {error}"))
}

/// The id of `topic`, from the broker behind `conn`.
pub async fn topic_id_on(conn: &Connection, topic: &str) -> Uuid {
    let response = conn
        .send(named_topic_metadata(topic))
        .await
        .expect("Metadata");
    response
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(topic))
        .map(|row| row.topic_id)
        .expect("the topic is in the metadata")
}

/// Produce one batch to partition 0 of the broker behind `conn`.
pub async fn produce_on(
    conn: &Connection,
    topic: &str,
    topic_id: Uuid,
    batch: RecordBatch,
    acks: ProduceAcknowledgements,
) -> Result<PartitionProduceResponse, ClientError> {
    let request = batch_request(batch, SinglePartitionProduceSetup {
        topic: topic.to_owned(),
        topic_id,
        acknowledgements: acks,
        timeout: ProduceTimeoutMillis(15_000),
        ..SinglePartitionProduceSetup::default()
    });
    let response = conn.send(request).await?;
    Ok(response.responses[0].partition_responses[0].clone())
}

/// [`read_partition`] against the broker at `addr`, which must lead the
/// partition.
///
/// The client is new and never sends a Metadata request, so it knows no
/// broker but its bootstrap one and sends every Fetch there.
pub async fn read_partition_on(addr: SocketAddr, topic: &str, topic_id: Uuid) -> PartitionContents {
    let client = connect_client(addr.to_string(), Some("dedup-recovery-reader")).await;
    read_partition(&client, topic, topic_id, 0).await
}

// ---------------------------------------------------------------------------
// A three-broker cluster
// ---------------------------------------------------------------------------

pub type Node = (BrokerHandle, BrokerConfig, TempDir);

/// Boot three registered brokers and create `topic` with one partition on
/// all three, broker 1 leading, `min.insync.replicas=2` and dedup on by key
/// within [`WINDOW_MS`]. Returns when broker 1 is ready to take a produce
/// and every replica is in the ISR.
pub async fn dedup_cluster(topic: &str) -> (Vec<Node>, Uuid) {
    let cluster = support::registered_cluster(3).await;
    let conn = connect(cluster[0].1.listen_addr).await;
    let window = WINDOW_MS.to_string();
    let mut creatable = topic_on(topic, &[&[1, 2, 3]]);
    creatable.configs = topic_configs([
        ("dedup.window.ms", window.as_str()),
        ("min.insync.replicas", "2"),
    ]);
    let response = conn
        .send(create_topic_request(creatable))
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics: {response:?}"
    );
    for (broker, _, _) in &cluster {
        broker.wait_until_partition_present(topic, 0).await;
    }
    let topic = topic.to_owned();
    wait_for_partition(&cluster[0].0, &topic, |partition| {
        partition.leader == NodeId(1) && partition.isr.len() == 3
    })
    .await;
    cluster[0]
        .0
        .wait_until_local_partition_leader(&topic, 0, NodeId(1))
        .await;
    let topic_id = topic_id_on(&conn, &topic).await;
    (cluster, topic_id)
}

/// Wait until `broker`'s image shows partition 0 of `topic` matching `ready`.
pub async fn wait_for_partition(
    broker: &BrokerHandle,
    topic: &str,
    ready: impl Fn(&PartitionRecord) -> bool,
) {
    broker
        .wait_for_image(|image| image.partition(topic, 0).is_some_and(&ready))
        .await;
}

/// The position of broker `node` in `cluster`.
pub fn position(cluster: &[Node], node: u64) -> usize {
    cluster
        .iter()
        .position(|(_, config, _)| config.node_id == NodeId(node))
        .unwrap_or_else(|| panic!("broker {node} is not running"))
}

/// The broker that leads partition 0 of `topic`, by `observer`'s image.
pub fn leader_of(observer: &BrokerHandle, topic: &str) -> u64 {
    observer
        .partition_leader_for_test(topic, 0)
        .expect("the partition has a leader")
}

/// Stop broker `node` gracefully and take it out of `cluster`. Returns its
/// config, set to rejoin, and its directory, for a restart.
pub async fn stop(cluster: &mut Vec<Node>, node: u64) -> (BrokerConfig, TempDir) {
    let (broker, mut config, dir) = cluster.remove(position(cluster, node));
    broker.shutdown().await;
    config.bootstrap_mode = BootstrapMode::Rejoin;
    (config, dir)
}

/// Start a stopped broker again on its own directory and addresses.
pub async fn restart(config: BrokerConfig, dir: TempDir) -> Node {
    let what = format!("restart broker {}", config.node_id.0);
    let broker = Box::pin(support::start_reusing_addrs(&config, &what)).await;
    (broker, config, dir)
}

/// Wait until the brokers left in `cluster` elected a partition leader other
/// than `victim` and that leader is ready to take a produce. Returns it.
pub async fn await_new_leader(cluster: &[Node], topic: &str, victim: u64) -> u64 {
    for (broker, _, _) in cluster {
        support::await_controller_replacement(
            broker,
            NodeId(victim),
            "no controller leader among the survivors",
        )
        .await;
    }
    cluster[0]
        .0
        .wait_until_partition_leader_changed(topic, 0, NodeId(victim))
        .await;
    let leader = leader_of(&cluster[0].0, topic);
    cluster[position(cluster, leader)]
        .0
        .wait_until_local_partition_leader(topic, 0, NodeId(leader))
        .await;
    leader
}

/// Wait until broker `node` leads partition 0 of `topic` and is ready to take
/// a produce.
pub async fn await_local_leader(cluster: &[Node], topic: &str, node: u64) {
    wait_for_partition(&cluster[0].0, topic, |partition| {
        partition.leader == NodeId(node)
    })
    .await;
    cluster[position(cluster, node)]
        .0
        .wait_until_local_partition_leader(topic, 0, NodeId(node))
        .await;
}

pub async fn shutdown_all(cluster: Vec<Node>) { support::shutdown_cluster(cluster).await; }
