//! The producers of a chaos run and the history they record.
//!
//! Each producer sends small keyed batches with `acks=all` to a random
//! partition and retries a batch, unchanged, until a leader acknowledges it.
//! Half of them are idempotent, with a sequence per partition and one batch
//! in flight, as a Kafka idempotent producer with
//! `max.in.flight.requests.per.connection=1` sends. Identities come from
//! small pools shared by every producer, so most records repeat one that an
//! earlier batch, an earlier record of the same batch, or another producer
//! sent. The plain producers also deliver an acknowledged batch a second
//! time now and then, unchanged, as an upstream system that replays does.
//!
//! Every attempt goes into the history with its outcome: the base offset a
//! leader acknowledged, or the error.

use {
    crate::{
        cluster::{STEP, bounded, dial},
        rng::Rng,
        support::{
            dedup::{Idempotence, Rec, keyed_batch},
            discovery::topic_metadata_request,
            produce::{
                ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
                batch_request,
            },
        },
        topics::{Identities, TOPICS, Target, targets},
    },
    krabka_client_core::Connection,
    krabka_ids::PartitionIndex,
    krabka_protocol::{
        owned::init_producer_id_request::InitProducerIdRequest, primitives::uuid::Uuid,
        records::RecordBatch,
    },
    std::{
        collections::BTreeMap,
        fmt,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::task::JoinHandle,
};

/// How many producers run, the even ones plain and the odd ones idempotent.
const PRODUCERS: u64 = 6;

/// How long a producer retries one batch before it gives up on it.
const BATCH_DEADLINE: Duration = Duration::from_secs(120);

/// How many acknowledged batches a plain producer keeps for replays.
const REPLAY_POOL: usize = 24;

/// The idempotent-producer errors after which a producer, as Kafka's does
/// for an idempotent producer since KIP-360, starts again with a new
/// producer id: `OUT_OF_ORDER_SEQUENCE_NUMBER`, `DUPLICATE_SEQUENCE_NUMBER`,
/// `INVALID_PRODUCER_EPOCH` and `UNKNOWN_PRODUCER_ID`.
const SEQUENCE_ERRORS: [i16; 4] = [45, 46, 47, 59];

/// One record as a producer sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sent {
    pub key: String,
    pub value: String,
    pub header: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Outcome {
    /// A leader acknowledged the batch at this base offset.
    Acked(i64),
    Failed(String),
}

/// One produce request and what came of it.
#[derive(Clone, Debug)]
pub struct Attempt {
    pub producer: u64,
    pub target: Target,
    /// The producer's own number of the batch; a retry or replay keeps it.
    pub batch: u64,
    pub replay: bool,
    pub records: Arc<[Sent]>,
    /// When the answer came, from the start of the run.
    pub at: Duration,
    pub outcome: Outcome,
}

impl Attempt {
    pub fn acked(&self) -> Option<i64> {
        match self.outcome {
            Outcome::Acked(base) => Some(base),
            Outcome::Failed(_) => None,
        }
    }
}

impl fmt::Display for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.records.iter().map(|sent| sent.key.as_str()).collect();
        write!(
            f,
            "producer {} batch {}{} to {} at +{}ms {keys:?}: {}",
            self.producer,
            self.batch,
            if self.replay { " (replay)" } else { "" },
            self.target,
            self.at.as_millis(),
            self.outcome
        )
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acked(base) => write!(f, "acknowledged at {base}"),
            Self::Failed(why) => write!(f, "failed: {why}"),
        }
    }
}

/// What one producer did.
#[derive(Debug, Default)]
pub struct ProducerReport {
    pub attempts: Vec<Attempt>,
    /// Batches never acknowledged within [`BATCH_DEADLINE`].
    pub abandoned: Vec<String>,
    /// Each new producer id an idempotent producer took after a sequence
    /// error, with that error.
    pub restarts: Vec<String>,
}

/// The whole history of a run.
#[derive(Debug, Default)]
pub struct History {
    pub attempts: Vec<Attempt>,
    pub abandoned: Vec<String>,
    pub restarts: Vec<String>,
}

/// The running producers.
pub struct Workload {
    stop: Arc<AtomicBool>,
    producers: Vec<JoinHandle<ProducerReport>>,
}

impl Workload {
    pub fn start(
        seed: u64,
        brokers: &[(u64, SocketAddr)],
        topic_ids: &[Uuid],
        started: Instant,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let producers = (0..PRODUCERS)
            .map(|index| {
                let producer = Producer {
                    index,
                    idempotent: index % 2 == 1,
                    rng: Rng::lane(seed, index + 1),
                    brokers: brokers.to_vec(),
                    topic_ids: topic_ids.to_vec(),
                    started,
                    stop: Arc::clone(&stop),
                    connections: BTreeMap::new(),
                    leaders: BTreeMap::new(),
                    identity: None,
                    sequences: BTreeMap::new(),
                    acked: Vec::new(),
                    fresh: 0,
                    report: ProducerReport::default(),
                };
                tokio::spawn(producer.run())
            })
            .collect();
        Self { stop, producers }
    }

    /// Stop after the batch each producer is sending, and collect the
    /// history.
    pub async fn finish(self) -> History {
        self.stop.store(true, Ordering::Release);
        let mut history = History::default();
        for producer in self.producers {
            let report = producer.await.expect("a producer");
            history.attempts.extend(report.attempts);
            history.abandoned.extend(report.abandoned);
            history.restarts.extend(report.restarts);
        }
        history.attempts.sort_by_key(|attempt| attempt.at);
        history
    }
}

/// An acknowledged batch a plain producer may replay.
struct Delivered {
    target: Target,
    batch: u64,
    records: Arc<[Sent]>,
    encoded: RecordBatch,
}

struct Producer {
    index: u64,
    idempotent: bool,
    rng: Rng,
    brokers: Vec<(u64, SocketAddr)>,
    topic_ids: Vec<Uuid>,
    started: Instant,
    stop: Arc<AtomicBool>,
    connections: BTreeMap<u64, Connection>,
    leaders: BTreeMap<Target, u64>,
    /// The idempotent producer id and epoch.
    identity: Option<(i64, i16)>,
    /// The next sequence of each partition, for the current producer id.
    sequences: BTreeMap<Target, i32>,
    acked: Vec<Delivered>,
    fresh: u64,
    report: ProducerReport,
}

impl Producer {
    async fn run(mut self) -> ProducerReport {
        let all = targets();
        let mut batch = 0;
        while !self.stop.load(Ordering::Acquire) {
            let replay = !self.idempotent && !self.acked.is_empty() && self.rng.chance(8);
            if replay {
                let pick = self.rng.index(self.acked.len());
                let old = &self.acked[pick];
                let (target, number, records, encoded) = (
                    old.target,
                    old.batch,
                    Arc::clone(&old.records),
                    old.encoded.clone(),
                );
                self.deliver(target, number, records, Some(encoded)).await;
            } else {
                let target = all[self.rng.index(all.len())];
                let records = self.records(target, batch);
                self.deliver(target, batch, records, None).await;
                batch += 1;
            }
            tokio::time::sleep(self.rng.millis(0, 25)).await;
        }
        self.report
    }

    /// One to six records for `target`, or four to twelve of fresh keys, so
    /// that the capacity topic passes its partition limit early. A record
    /// repeats the identity of an earlier record of the batch a quarter of
    /// the time; its value is new either way, so first-wins decides which
    /// value the log keeps.
    fn records(&mut self, target: Target, batch: u64) -> Arc<[Sent]> {
        let count = match target.spec().identities {
            Identities::Pool { .. } => self.rng.between(1, 6),
            Identities::Fresh => self.rng.between(4, 12),
        };
        let mut records: Vec<Sent> = Vec::new();
        for record in 0..count {
            let value = format!("p{}-b{batch}-r{record}", self.index);
            let sent = if !records.is_empty() && self.rng.chance(25) {
                let earlier = &records[self.rng.index(records.len())];
                Sent {
                    value,
                    ..earlier.clone()
                }
            } else {
                self.identity_for(target, value)
            };
            records.push(sent);
        }
        records.into()
    }

    fn identity_for(&mut self, target: Target, value: String) -> Sent {
        match target.spec().identities {
            Identities::Pool { keys, headers } => Sent {
                key: format!("k{:04}", self.rng.below(keys)),
                value,
                header: (headers > 0).then(|| format!("h{:04}", self.rng.below(headers))),
            },
            Identities::Fresh => {
                self.fresh += 1;
                Sent {
                    key: format!("c{}-{}", self.index, self.fresh),
                    value,
                    header: None,
                }
            }
        }
    }

    /// The wire batch for `records`, with this producer's sequence for
    /// `target` when it is idempotent.
    async fn encode(&mut self, target: Target, records: &[Sent]) -> RecordBatch {
        let idempotence = if self.idempotent {
            let (producer_id, producer_epoch) = self.identity().await;
            Some(Idempotence {
                producer_id,
                producer_epoch,
                base_sequence: *self.sequences.entry(target).or_default(),
            })
        } else {
            None
        };
        encode_records(records, idempotence)
    }

    /// Send a batch until a leader acknowledges it. A plain producer resends
    /// the same bytes, `encoded` when it replays; an idempotent one resends
    /// the same sequence, and takes a new producer id after a sequence error.
    async fn deliver(
        &mut self,
        target: Target,
        batch: u64,
        records: Arc<[Sent]>,
        encoded: Option<RecordBatch>,
    ) {
        let replay = encoded.is_some();
        let deadline = Instant::now() + BATCH_DEADLINE;
        let mut wire = match encoded {
            Some(encoded) => encoded,
            None => self.encode(target, &records).await,
        };
        loop {
            let outcome = self.attempt(target, &wire).await;
            self.report.attempts.push(Attempt {
                producer: self.index,
                target,
                batch,
                replay,
                records: Arc::clone(&records),
                at: self.started.elapsed(),
                outcome: outcome.clone().map_or_else(Outcome::Failed, Outcome::Acked),
            });
            match outcome {
                Ok(_) => {
                    self.delivered(target, batch, records, wire);
                    return;
                }
                Err(error) if self.idempotent && is_sequence_error(&error) => {
                    self.report
                        .restarts
                        .push(format!("producer {} on {target}: {error}", self.index));
                    self.identity = None;
                    self.sequences.clear();
                    wire = self.encode(target, &records).await;
                }
                Err(_) => {}
            }
            if Instant::now() > deadline {
                self.report.abandoned.push(format!(
                    "producer {} batch {batch} to {target}: no acknowledgement within {BATCH_DEADLINE:?}",
                    self.index
                ));
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn delivered(&mut self, target: Target, batch: u64, records: Arc<[Sent]>, wire: RecordBatch) {
        if self.idempotent {
            let count = i32::try_from(records.len()).expect("a batch size");
            *self.sequences.entry(target).or_default() += count;
            return;
        }
        if self.acked.len() == REPLAY_POOL {
            self.acked.remove(0);
        }
        self.acked.push(Delivered {
            target,
            batch,
            records,
            encoded: wire,
        });
    }

    /// One produce request to the leader of `target`.
    async fn attempt(&mut self, target: Target, batch: &RecordBatch) -> Result<i64, String> {
        let leader = self.leader(target).await?;
        let conn = self.connection(leader).await?;
        let answer = produce_to(&conn, target, self.topic_ids[target.topic], batch.clone()).await;
        match answer {
            Ok(Ok(base_offset)) => Ok(base_offset),
            Ok(Err(code)) => {
                self.leaders.remove(&target);
                Err(format!("broker {leader}: error {code}"))
            }
            Err(error) => {
                self.connections.remove(&leader);
                self.leaders.remove(&target);
                Err(format!("broker {leader}: {error}"))
            }
        }
    }

    async fn connection(&mut self, node: u64) -> Result<Connection, String> {
        if let Some(conn) = self.connections.get(&node) {
            return Ok(conn.clone());
        }
        let addr = self
            .brokers
            .iter()
            .find(|(id, _)| *id == node)
            .map(|(_, addr)| *addr)
            .ok_or_else(|| format!("no broker {node}"))?;
        let conn = dial(addr, "dedup-chaos-producer").await?;
        self.connections.insert(node, conn.clone());
        Ok(conn)
    }

    /// The leader of `target`, from the cache or from the first broker, in a
    /// random order, that answers a Metadata request with one.
    async fn leader(&mut self, target: Target) -> Result<u64, String> {
        if let Some(leader) = self.leaders.get(&target) {
            return Ok(*leader);
        }
        let first = self.rng.index(self.brokers.len());
        for offset in 0..self.brokers.len() {
            let node = self.brokers[(first + offset) % self.brokers.len()].0;
            let Ok(conn) = self.connection(node).await else {
                continue;
            };
            let Ok(Ok(metadata)) = bounded(conn.send(topic_metadata_request(None))).await else {
                self.connections.remove(&node);
                continue;
            };
            for row in &metadata.topics {
                let Some(topic) = TOPICS
                    .iter()
                    .position(|spec| row.name.as_deref() == Some(spec.name))
                else {
                    continue;
                };
                for partition in &row.partitions {
                    if let Ok(leader) = u64::try_from(partition.leader_id) {
                        self.leaders.insert(
                            Target {
                                topic,
                                partition: partition.partition_index,
                            },
                            leader,
                        );
                    }
                }
            }
            if let Some(leader) = self.leaders.get(&target) {
                return Ok(*leader);
            }
        }
        Err(format!("no broker named a leader of {target}"))
    }

    /// This producer's id and epoch, taken with `InitProducerId` from a
    /// broker that answers within [`BATCH_DEADLINE`].
    async fn identity(&mut self) -> (i64, i16) {
        if let Some(identity) = self.identity {
            return identity;
        }
        let deadline = Instant::now() + BATCH_DEADLINE;
        loop {
            let node = self.brokers[self.rng.index(self.brokers.len())].0;
            if let Ok(conn) = self.connection(node).await {
                let request = InitProducerIdRequest {
                    transactional_id: None,
                    ..Default::default()
                };
                match bounded(conn.send(request)).await {
                    Ok(Ok(response)) if response.error_code == 0 => {
                        let identity = (response.producer_id, response.producer_epoch);
                        self.identity = Some(identity);
                        return identity;
                    }
                    Ok(Ok(_)) => {}
                    _ => {
                        self.connections.remove(&node);
                    }
                }
            }
            assert2::assert!(
                Instant::now() < deadline,
                "producer {} found no broker to give it a producer id",
                self.index
            );
            tokio::time::sleep(STEP / 40).await;
        }
    }
}

/// Produce `batch` to `target` on the broker behind `conn` with `acks=all`.
/// The outer error is the transport's, the inner one the partition's error
/// code; `Ok(Ok(_))` is the acknowledged base offset.
pub async fn produce_to(
    conn: &Connection,
    target: Target,
    topic_id: Uuid,
    batch: RecordBatch,
) -> Result<Result<i64, i16>, String> {
    let request = batch_request(batch, SinglePartitionProduceSetup {
        topic: target.spec().name.to_owned(),
        topic_id,
        partition: PartitionIndex(target.partition),
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        timeout: ProduceTimeoutMillis(5_000),
        ..SinglePartitionProduceSetup::default()
    });
    let response = bounded(conn.send(request))
        .await?
        .map_err(|error| error.to_string())?;
    let row = &response.responses[0].partition_responses[0];
    Ok(if row.error_code == 0 {
        Ok(row.base_offset)
    } else {
        Err(row.error_code)
    })
}

/// The wire batch of `records`, without producer fields.
pub fn plain_batch(records: &[Sent]) -> RecordBatch { encode_records(records, None) }

fn encode_records(records: &[Sent], idempotence: Option<Idempotence>) -> RecordBatch {
    let recs: Vec<Rec> = records
        .iter()
        .map(|sent| {
            let rec = Rec::key(&sent.key).value(&sent.value);
            match &sent.header {
                Some(header) => rec.dedup_id(header),
                None => rec,
            }
        })
        .collect();
    keyed_batch()
        .records(&recs)
        .maybe_idempotence(idempotence)
        .call()
}

fn is_sequence_error(error: &str) -> bool {
    SEQUENCE_ERRORS
        .iter()
        .any(|code| error.ends_with(&format!("error {code}")))
}
