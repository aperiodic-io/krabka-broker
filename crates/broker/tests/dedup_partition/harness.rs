//! What the partition scenarios share: the relay-linked cluster, a dedup
//! topic pinned to known replicas, produces to one named broker, the replica
//! logs read straight off each broker, and the waits for failover and
//! convergence.

use {
    crate::support::{
        admin::election_request,
        dedup::{Rec, keyed_batch, partition_metric},
        discovery::named_topic_metadata,
        linked_cluster::{NODES, PeerLinks, RelayedCluster},
        produce::{
            ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
            batch_request,
        },
        topic_on,
        topics::{create_topic_request, topic_configs},
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, NodeId},
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_metadata::PartitionRecord,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse, primitives::uuid::Uuid,
        records::RecordBatch,
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Display,
        net::SocketAddr,
        time::{Duration, Instant},
    },
};

/// The dedup window of every topic here: far longer than any scenario, so
/// nothing expires by accident and every key must be unique in the log.
pub const WINDOW_MS: i64 = 600_000;

/// The bound on one cluster step: an election, a catch-up, a convergence.
/// Other suites share the machine and the build lock, so it is generous; a
/// healthy cluster finishes each step in a few seconds.
pub const STEP: Duration = Duration::from_secs(120);

/// The bound on connecting to a broker, which is always reachable: the
/// relays front only what the peers dial.
const CONNECT: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// The cluster
// ---------------------------------------------------------------------------

/// Three combined controller and broker nodes, each of which can be cut off
/// from its peers in both directions while it keeps running. Brokers are
/// named by node id, 1 to 3.
pub struct Cluster(RelayedCluster<PeerLinks>);

impl Cluster {
    pub async fn start() -> Self { Self(RelayedCluster::start(|_, _| {}).await) }

    /// Every node id.
    pub fn nodes() -> impl Iterator<Item = u64> + Clone {
        (1..=NODES).map(|index| u64::try_from(index).expect("a node id"))
    }

    pub fn broker(&self, node: u64) -> &BrokerHandle { self.0.handle(position(node)) }

    /// The broker's real client listener, which a test client reaches however
    /// the peers are cut.
    pub fn addr(&self, node: u64) -> SocketAddr { self.0.addr(position(node)) }

    pub fn links(&self) -> &PeerLinks { self.0.network() }

    /// Check that every broker connection went through a relay that a cut
    /// reaches, then stop the cluster.
    pub async fn finish(self) {
        let unattributed = self.links().unattributed_client_ids();
        self.0.shutdown().await;
        assert!(
            unattributed.is_empty(),
            "these broker connections went around every cut: {unattributed:?}"
        );
    }
}

fn position(node: u64) -> usize {
    usize::try_from(node)
        .ok()
        .and_then(|node| node.checked_sub(1))
        .expect("a node id")
}

// ---------------------------------------------------------------------------
// Waiting
// ---------------------------------------------------------------------------

/// Poll `probe` until it answers, for at most [`STEP`].
pub async fn until<T>(what: impl Display, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + STEP;
    loop {
        if let Some(found) = probe() {
            return found;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {STEP:?}: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Partition 0 of `topic` as broker `node`'s metadata image has it.
pub fn partition_on(cluster: &Cluster, node: u64, topic: &str) -> Option<PartitionRecord> {
    cluster.broker(node).partition_record_for_test(topic, 0)
}

/// The leader of `topic` that the newest leader epoch any broker's image
/// holds names, as a producer that asks every broker for metadata finds it.
pub fn newest_leader(cluster: &Cluster, topic: &str) -> Option<u64> {
    Cluster::nodes()
        .filter_map(|node| partition_on(cluster, node, topic))
        .max_by_key(|partition| partition.leader_epoch.0)
        .map(|partition| partition.leader.0)
        .filter(|leader| *leader != 0)
}

/// Wait until every broker of `observers` agrees that `node` leads partition
/// 0 of `topic`, and `node` is ready to take a produce.
pub async fn await_leader(cluster: &Cluster, topic: &str, observers: &[u64], node: u64) {
    until(
        format!("{observers:?} see broker {node} lead {topic}"),
        || {
            observers
                .iter()
                .all(|observer| {
                    partition_on(cluster, *observer, topic)
                        .is_some_and(|partition| partition.leader == NodeId(node))
                })
                .then_some(())
        },
    )
    .await;
    cluster
        .broker(node)
        .wait_until_local_partition_leader(topic, 0, NodeId(node))
        .await;
}

/// Wait until the brokers of `observers` agree on one leader of `topic` other
/// than `deposed`, at one leader epoch, and it is ready. Returns it.
pub async fn await_new_leader(
    cluster: &Cluster,
    topic: &str,
    observers: &[u64],
    deposed: u64,
) -> u64 {
    let leader = until(
        format!("{observers:?} elect a leader of {topic} other than broker {deposed}"),
        || {
            let views: BTreeSet<(u64, i32)> = observers
                .iter()
                .filter_map(|observer| partition_on(cluster, *observer, topic))
                .map(|partition| (partition.leader.0, partition.leader_epoch.0))
                .collect();
            match views.into_iter().collect::<Vec<_>>().as_slice() {
                [(leader, _)] if *leader != 0 && *leader != deposed => Some(*leader),
                _ => None,
            }
        },
    )
    .await;
    await_leader(cluster, topic, observers, leader).await;
    leader
}

/// Wait until every broker of `observers` sees exactly `isr` as the in-sync
/// replicas of `topic`.
pub async fn await_isr(cluster: &Cluster, topic: &str, observers: &[u64], isr: &[u64]) {
    let expected: BTreeSet<u64> = isr.iter().copied().collect();
    until(
        format!("{observers:?} see the ISR of {topic} at {isr:?}"),
        || {
            observers
                .iter()
                .all(|observer| {
                    partition_on(cluster, *observer, topic).is_some_and(|partition| {
                        partition
                            .isr
                            .iter()
                            .map(|node| node.0)
                            .collect::<BTreeSet<_>>()
                            == expected
                    })
                })
                .then_some(())
        },
    )
    .await;
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

/// A topic under test: its name, and the id Produce and Fetch v13 name it by.
#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
    pub id: Uuid,
}

/// Create a one-partition topic deduplicated by key on `replicas`, the first
/// leading, and wait until every broker sees all of them in sync and the
/// leader takes produces.
#[bon::builder]
pub async fn dedup_topic(
    cluster: &Cluster,
    name: &str,
    replicas: &[u64],
    #[builder(default = 2)] min_insync: i32,
) -> Topic {
    let leader = replicas[0];
    let conn = connect(cluster, leader).await;
    let assignment: Vec<i32> = replicas
        .iter()
        .map(|node| i32::try_from(*node).expect("a broker id"))
        .collect();
    let mut creatable = topic_on(name, &[&assignment]);
    let window = WINDOW_MS.to_string();
    let min_insync = min_insync.to_string();
    creatable.configs = topic_configs([
        ("dedup.window.ms", window.as_str()),
        ("min.insync.replicas", min_insync.as_str()),
    ]);
    let response = conn
        .send(create_topic_request(creatable))
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics: {response:?}"
    );
    let all: Vec<u64> = Cluster::nodes().collect();
    await_isr(cluster, name, &all, replicas).await;
    await_leader(cluster, name, &all, leader).await;
    let metadata = conn
        .send(named_topic_metadata(name))
        .await
        .expect("Metadata");
    let id = metadata
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(name))
        .map(|row| row.topic_id)
        .expect("the topic is in the metadata");
    Topic {
        name: name.to_owned(),
        id,
    }
}

// ---------------------------------------------------------------------------
// Producing
// ---------------------------------------------------------------------------

/// A connection to broker `node`'s real listener, under a client id that
/// names no broker, so the fronts never mistake it for a peer.
pub async fn connect(cluster: &Cluster, node: u64) -> Connection {
    let addr = cluster.addr(node);
    let options = ConnectionOptions {
        client_id: "dedup-partition".into(),
        ..ConnectionOptions::default()
    };
    tokio::time::timeout(CONNECT, Connection::connect(addr, options))
        .await
        .unwrap_or_else(|_| panic!("connect to broker {node} timed out"))
        .unwrap_or_else(|error| panic!("connect to broker {node}: {error}"))
}

/// One batch holding a record for each of `keys`, in order, created now.
pub fn keys(keys: &[&str]) -> RecordBatch {
    keyed_batch()
        .records(&keys.iter().map(|key| Rec::key(key)).collect::<Vec<_>>())
        .call()
}

/// Send `batch` to partition 0 of `topic` on broker `node`, with the broker
/// told to answer within `timeout_ms`. A transport failure, or no answer
/// well past that, is an `Err`: either way the write was not acknowledged.
pub async fn send(
    cluster: &Cluster,
    node: u64,
    topic: &Topic,
    batch: RecordBatch,
    acks: ProduceAcknowledgements,
    timeout_ms: i32,
) -> Result<PartitionProduceResponse, String> {
    let conn = connect(cluster, node).await;
    let request = batch_request(batch, SinglePartitionProduceSetup {
        topic: topic.name.clone(),
        topic_id: topic.id,
        acknowledgements: acks,
        timeout: ProduceTimeoutMillis(timeout_ms),
        ..SinglePartitionProduceSetup::default()
    });
    let patience = Duration::from_millis(u64::try_from(timeout_ms).expect("a timeout"))
        + Duration::from_secs(30);
    match tokio::time::timeout(patience, conn.send(request)).await {
        Ok(Ok(response)) => Ok(response.responses[0].partition_responses[0].clone()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err(format!("no answer within {patience:?}")),
    }
}

/// [`send`] with `acks`, which must be acknowledged: returns the base offset.
pub async fn produce(
    cluster: &Cluster,
    node: u64,
    topic: &Topic,
    keys_: &[&str],
    acks: ProduceAcknowledgements,
) -> i64 {
    let row = send(cluster, node, topic, keys(keys_), acks, 30_000)
        .await
        .unwrap_or_else(|error| panic!("produce {keys_:?} to broker {node}: {error}"));
    assert!(
        row.error_code == 0,
        "produce {keys_:?} to broker {node}: {row:?}"
    );
    row.base_offset
}

/// Whether a [`send`] was acknowledged.
pub fn acknowledged(outcome: &Result<PartitionProduceResponse, String>) -> bool {
    matches!(outcome, Ok(row) if row.error_code == 0)
}

/// Ask broker `via` for a preferred election of `topic` until `preferred`
/// leads it and is ready to take a produce.
pub async fn elect_preferred(cluster: &Cluster, topic: &str, via: u64, preferred: u64) {
    let conn = connect(cluster, via).await;
    let deadline = Instant::now() + STEP;
    while partition_on(cluster, via, topic).is_none_or(|p| p.leader != NodeId(preferred)) {
        assert!(
            Instant::now() < deadline,
            "broker {preferred} was not elected leader of {topic} within {STEP:?}"
        );
        let response = conn
            .send(election_request().topic(topic.to_owned()).call())
            .await
            .expect("ElectLeaders");
        tracing::debug!(?response, "preferred election");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let all: Vec<u64> = Cluster::nodes().collect();
    await_leader(cluster, topic, &all, preferred).await;
}

/// Records partition 0 of `topic` has dropped as duplicates on broker `node`
/// since that broker started.
pub async fn dropped(cluster: &Cluster, node: u64, topic: &str) -> f64 {
    partition_metric(
        cluster.broker(node),
        "dedup_records_dropped_total",
        topic,
        0,
    )
    .await
}

// ---------------------------------------------------------------------------
// Replica logs
// ---------------------------------------------------------------------------

/// Every batch of broker `node`'s local copy of partition 0, above the high
/// watermark too, read under the log lock and not through the network.
pub fn replica_log(cluster: &Cluster, node: u64, topic: &str) -> Vec<RecordBatch> {
    cluster
        .broker(node)
        .local_batches_for_test(topic, 0)
        .unwrap_or_else(|| panic!("broker {node} holds {topic}-0"))
}

/// Every `(offset, key)` of `batches`, in offset order.
pub fn offsets_and_keys(batches: &[RecordBatch]) -> Vec<(i64, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            batch.records.iter().map(|record| {
                let key = record.key.as_deref().map_or_else(
                    || "<null>".to_owned(),
                    |key| String::from_utf8_lossy(key).into_owned(),
                );
                (batch.base_offset + i64::from(record.offset_delta), key)
            })
        })
        .collect()
}

/// `(offset, key)` pairs as [`offsets_and_keys`] gives them.
pub fn at(pairs: &[(i64, &str)]) -> Vec<(i64, String)> {
    pairs
        .iter()
        .map(|&(offset, key)| (offset, key.to_owned()))
        .collect()
}

/// The keys of `log` that it holds more than once, with their offsets.
pub fn repeated_keys(log: &[(i64, String)]) -> BTreeMap<String, Vec<i64>> {
    let mut offsets: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for (offset, key) in log {
        offsets.entry(key.clone()).or_default().push(*offset);
    }
    offsets.retain(|_, at| at.len() > 1);
    offsets
}

/// Wait until the three replicas of `topic` agree: one leader at one epoch
/// in every image, all three in sync, the same batches in every local log,
/// and the leader's high watermark at the end of them. Returns the log.
pub async fn converged(cluster: &Cluster, topic: &str) -> Vec<RecordBatch> {
    let (leader, log) = until(format!("the replicas of {topic} converge"), || {
        let views: BTreeSet<(u64, i32, usize)> = Cluster::nodes()
            .filter_map(|node| partition_on(cluster, node, topic))
            .map(|partition| {
                (
                    partition.leader.0,
                    partition.leader_epoch.0,
                    partition.isr.len(),
                )
            })
            .collect();
        let [(leader, _, NODES)] = views.into_iter().collect::<Vec<_>>()[..] else {
            return None;
        };
        let mut logs = Cluster::nodes().map(|node| replica_log(cluster, node, topic));
        let first = logs.next()?;
        logs.all(|log| log == first).then_some((leader, first))
    })
    .await;
    let end = log.last().map_or(0, |batch| {
        batch.base_offset + i64::from(batch.last_offset_delta) + 1
    });
    tokio::time::timeout(
        STEP,
        cluster
            .broker(leader)
            .wait_until_high_watermark(topic, 0, end),
    )
    .await
    .unwrap_or_else(|_| panic!("the high watermark of {topic} did not reach {end}"));
    log
}

/// Assert that every replica's dedup index holds exactly the identities its
/// log holds: one per distinct key, and none of a record that a truncation
/// removed.
///
/// A replica rebuilds an index that a truncation left stale before its next
/// append, not at once, so call this after an append that every replica
/// followed. The gauge is reported after each append and on the broker's
/// once-a-second tick, so this waits briefly for it to settle.
pub async fn assert_indexes_match(cluster: &Cluster, topic: &str, log: &[(i64, String)]) {
    let distinct = log
        .iter()
        .map(|(_, key)| key)
        .collect::<BTreeSet<_>>()
        .len();
    let expected = f64::from(u32::try_from(distinct).expect("a key count"));
    let deadline = Instant::now() + Duration::from_secs(20);
    for node in Cluster::nodes() {
        loop {
            let entries =
                partition_metric(cluster.broker(node), "dedup_index_entries", topic, 0).await;
            if entries.total_cmp(&expected).is_eq() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "broker {node}'s index of {topic} holds {entries} identities; its log holds \
                 {distinct} distinct keys"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
