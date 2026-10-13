//! A protocol-level load generator for `krabka-bench-local`.
//!
//! The client-library workload in [`crate::workload`] spends several
//! microseconds of CPU per record: it builds, batches, encodes and settles
//! each one. On a small machine that shares its cores with the cluster, the
//! load generator then saturates before the brokers do, and the number it
//! reports is the client's. This generator keeps the per-record cost near
//! zero, so the number it reports is the cluster's.
//!
//! It produces the way a batching Kafka client does. Each leader gets
//! `producers` connections, and each connection keeps
//! [`IN_FLIGHT_PER_CONNECTION`] `Produce` requests outstanding, as
//! `max.in.flight.requests.per.connection` does. A request carries one record
//! batch for every partition that broker leads. The batches are encoded once
//! and only restamped per request: the batch timestamps and the first
//! record's value carry the send time, and the CRC is recomputed.
//!
//! A paced run sends one single-record request per token, round robin over
//! the partitions, so its latency is the latency of one record.
//!
//! The consumer fetches every partition from its leader and reads the send
//! time out of each batch's first record, which gives the end-to-end latency:
//! the time from send until the record is readable below the high watermark,
//! that is, replicated.

use {
    crate::{
        hist,
        rate::Pacer,
        scenario::{Acks, LoadMode, Scenario},
    },
    anyhow::{Context, Result, bail},
    bytes::{Buf, BufMut, Bytes, BytesMut},
    futures::{StreamExt as _, stream::FuturesUnordered},
    hdrhistogram::Histogram,
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_protocol::{
        Decode, Encode, ProtocolError, ProtocolRequest,
        owned::{
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::AbortedTransaction,
            metadata_request::{MetadataRequest, MetadataRequestTopic},
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        },
        primitives::{
            array::{get_array_len, get_nullable_array_len},
            fixed::{get_i16, get_i32, get_i64},
            string_bytes::{get_compact_nullable_bytes_owned, get_compact_string_owned},
            uuid::Uuid,
        },
        records::{Record, RecordBatch, RecordsPayload},
        tagged_fields::read_tagged_fields,
    },
    krabka_units::prelude::*,
    std::{
        collections::BTreeMap,
        net::{SocketAddr, ToSocketAddrs},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{task::JoinSet, time::Instant},
};

/// Produce requests each connection keeps outstanding, Kafka's default
/// `max.in.flight.requests.per.connection`.
pub const IN_FLIGHT_PER_CONNECTION: usize = 5;

/// Offset of the CRC in a v2 batch header, and of the bytes it covers.
const CRC_AT: usize = 17;
const CRC_FROM: usize = 21;
/// Offsets of the base and max timestamps in a v2 batch header.
const BASE_TIMESTAMP_AT: usize = 27;
const MAX_TIMESTAMP_AT: usize = 35;
/// Length of a v2 batch header; the first record starts here.
const BATCH_HEADER_LEN: usize = 61;

/// What one raw run measured.
#[derive(Debug)]
pub struct RawOutcome {
    /// Records acknowledged inside the measurement window.
    pub produced: u64,
    /// Records the consumer read that were sent inside the window.
    pub consumed: u64,
    /// Records that became readable -- replicated below the high watermark --
    /// while the window was open, whenever they were sent. Under a load the
    /// followers cannot keep up with, this is the replication throughput, and
    /// it is the rate the cluster can sustain.
    pub committed: u64,
    /// The measurement window.
    pub window: Time,
    /// Per-record acknowledgement latency, in microseconds.
    pub ack: Histogram<u64>,
    /// Per-record send-to-readable latency, in microseconds.
    pub end_to_end: Histogram<u64>,
    /// Produce errors: failed requests and partitions answered with an error.
    pub errors: u64,
    /// The first error seen, if any.
    pub first_error: Option<String>,
}

/// The partitions one broker leads, and a connection to it.
struct Leader {
    addr: SocketAddr,
    partitions: Vec<i32>,
}

/// The topic as the run addresses it.
#[derive(Clone)]
struct TopicRef {
    name: String,
    id: Uuid,
}

/// The shared tallies and the run's clock.
struct Shared {
    stop: AtomicBool,
    produced: AtomicU64,
    errors: AtomicU64,
    first_error: Mutex<Option<String>>,
    ack: Mutex<Histogram<u64>>,
    end_to_end: Mutex<Histogram<u64>>,
    consumed: AtomicU64,
    committed: AtomicU64,
    /// The measurement window, as epoch nanoseconds of the send time.
    window_start_ns: u64,
    window_end_ns: u64,
}

impl Shared {
    fn in_window(&self, sent_ns: u64) -> bool {
        sent_ns >= self.window_start_ns && sent_ns < self.window_end_ns
    }

    fn error(&self, message: String) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        let mut first = self.first_error.lock().expect("error slot");
        first.get_or_insert(message);
    }
}

fn now_ns() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
}

/// Runs `scenario` against the cluster at `bootstrap` with the raw generator.
///
/// # Errors
///
/// When the topic's metadata cannot be read or a connection cannot be made.
///
/// # Panics
///
/// If a tally lock is poisoned, which only a panicking task can do.
pub async fn run(scenario: &Scenario, bootstrap: &str, topic: &str) -> Result<RawOutcome> {
    let (topic, leaders) = discover(bootstrap, topic).await?;
    let start_ns = now_ns();
    let warmup_ns = u64::try_from(scenario.warmup.nanos_i64()).unwrap_or(0);
    let window_ns = u64::try_from(scenario.duration.nanos_i64()).unwrap_or(0);
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        produced: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        first_error: Mutex::new(None),
        ack: Mutex::new(hist::new()),
        end_to_end: Mutex::new(hist::new()),
        consumed: AtomicU64::new(0),
        committed: AtomicU64::new(0),
        window_start_ns: start_ns + warmup_ns,
        window_end_ns: start_ns + warmup_ns + window_ns,
    });
    let acks = match scenario.acks {
        Acks::None => 0,
        Acks::Leader => 1,
        Acks::All => -1,
    };

    let mut consumers = JoinSet::new();
    if scenario.consumers > 0 {
        for leader in &leaders {
            let connection = connect(leader.addr, "bench-raw-consumer").await?;
            consumers.spawn(consume(
                connection,
                topic.clone(),
                leader.partitions.clone(),
                Arc::clone(&shared),
            ));
        }
    }

    let mut producers = JoinSet::new();
    match scenario.mode {
        LoadMode::Saturate => {
            let records = records_per_batch(scenario);
            let template = batch_template(scenario.msg_size, records)?;
            for leader in &leaders {
                for _ in 0..scenario.producers.max(1) {
                    let connection = connect(leader.addr, "bench-raw-producer").await?;
                    for _ in 0..IN_FLIGHT_PER_CONNECTION {
                        producers.spawn(produce_saturate(SaturateTask {
                            connection: connection.clone(),
                            topic: topic.clone(),
                            partitions: leader.partitions.clone(),
                            template: template.clone(),
                            records,
                            acks,
                            shared: Arc::clone(&shared),
                        }));
                    }
                }
            }
        }
        LoadMode::FixedRate { rate } => {
            let template = batch_template(scenario.msg_size, 1)?;
            let mut routes = Vec::new();
            for leader in &leaders {
                let connection = connect(leader.addr, "bench-raw-producer").await?;
                for &partition in &leader.partitions {
                    routes.push((connection.clone(), partition));
                }
            }
            routes.sort_by_key(|(_, partition)| *partition);
            producers.spawn(produce_paced(PacedTask {
                routes,
                topic: topic.clone(),
                template,
                acks,
                rate,
                shared: Arc::clone(&shared),
            }));
        }
    }

    let run_for = Duration::from_nanos(warmup_ns + window_ns);
    tokio::time::sleep(run_for).await;
    shared.stop.store(true, Ordering::Relaxed);
    while producers.join_next().await.is_some() {}
    // Let the consumers read what was produced up to the end of the window.
    let drain_until = Instant::now() + Duration::from_secs(5);
    let produced = shared.produced.load(Ordering::Relaxed);
    while Instant::now() < drain_until && shared.consumed.load(Ordering::Relaxed) < produced {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    consumers.abort_all();
    while consumers.join_next().await.is_some() {}

    let ack = shared.ack.lock().expect("ack histogram").clone();
    let end_to_end = shared.end_to_end.lock().expect("e2e histogram").clone();
    let first_error = shared.first_error.lock().expect("error slot").clone();
    Ok(RawOutcome {
        produced,
        consumed: shared.consumed.load(Ordering::Relaxed),
        committed: shared.committed.load(Ordering::Relaxed),
        window: scenario.duration,
        ack,
        end_to_end,
        errors: shared.errors.load(Ordering::Relaxed),
        first_error,
    })
}

/// Records per batch: as many `msg_size` records as fit `batch_size`, and at
/// least one.
fn records_per_batch(scenario: &Scenario) -> usize {
    let per = scenario.batch_size.bytes_u64() / scenario.msg_size.bytes_u64().max(1);
    usize::try_from(per.max(1)).unwrap_or(1)
}

async fn connect(addr: SocketAddr, client_id: &str) -> Result<Connection> {
    Connection::connect(addr, ConnectionOptions {
        client_id: client_id.to_owned(),
        ..ConnectionOptions::default()
    })
    .await
    .with_context(|| format!("connect to {addr}"))
}

/// Reads the topic id and each partition's leader from the cluster.
async fn discover(bootstrap: &str, topic: &str) -> Result<(TopicRef, Vec<Leader>)> {
    let first = bootstrap
        .split(',')
        .next()
        .context("empty bootstrap")?
        .to_socket_addrs()?
        .next()
        .context("bootstrap does not resolve")?;
    let connection = connect(first, "bench-raw-metadata").await?;
    let metadata = connection
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(topic.to_owned()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .context("Metadata")?;
    let described = metadata.topics.first().context("no topic in metadata")?;
    if described.error_code != 0 {
        bail!("metadata for {topic}: error code {}", described.error_code);
    }
    let mut by_leader: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
    for partition in &described.partitions {
        by_leader
            .entry(partition.leader_id)
            .or_default()
            .push(partition.partition_index);
    }
    let mut leaders = Vec::new();
    for (leader_id, partitions) in by_leader {
        let broker = metadata
            .brokers
            .iter()
            .find(|broker| broker.node_id == leader_id)
            .with_context(|| format!("leader {leader_id} is not in the broker list"))?;
        let addr = (broker.host.as_str(), u16::try_from(broker.port)?)
            .to_socket_addrs()?
            .next()
            .context("broker address does not resolve")?;
        leaders.push(Leader { addr, partitions });
    }
    Ok((
        TopicRef {
            name: topic.to_owned(),
            id: described.topic_id,
        },
        leaders,
    ))
}

/// A batch of `records` records of `msg_size` bytes each, encoded once. Its
/// stamps are filled in per request by [`stamped`].
fn batch_template(msg_size: ByteSize, records: usize) -> Result<Bytes> {
    let value = Bytes::from(vec![0x5a; msg_size.bytes_usize().max(8)]);
    let batch = RecordBatch {
        last_offset_delta: i32::try_from(records - 1)?,
        records: (0..records)
            .map(|offset| Record {
                offset_delta: i32::try_from(offset).unwrap_or(i32::MAX),
                value: Some(value.clone()),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let mut wire = BytesMut::new();
    batch
        .encode(&mut wire)
        .context("encode the batch template")?;
    Ok(wire.freeze())
}

/// Reads one zigzag varint at `at`, and returns it with the offset after it.
fn varint(bytes: &[u8], mut at: usize) -> Option<(i64, usize)> {
    let mut raw: u64 = 0;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(at)?;
        at += 1;
        raw |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            let value = i64::try_from(raw >> 1).ok()? ^ -i64::try_from(raw & 1).ok()?;
            return Some((value, at));
        }
    }
    None
}

/// Where the first record's value starts in an encoded v2 batch.
fn first_value_at(batch: &[u8]) -> Option<usize> {
    let (_, at) = varint(batch, BATCH_HEADER_LEN)?; // record length
    let at = at + 1; // attributes
    let (_, at) = varint(batch, at)?; // timestamp delta
    let (_, at) = varint(batch, at)?; // offset delta
    let (key_len, at) = varint(batch, at)?;
    let at = at + usize::try_from(key_len.max(0)).ok()?;
    let (_, at) = varint(batch, at)?; // value length
    Some(at)
}

/// The send time a batch's first record carries, in epoch nanoseconds.
fn sent_ns(batch: &[u8]) -> Option<u64> {
    let at = first_value_at(batch)?;
    let stamp = batch.get(at..at + 8)?;
    Some(u64::from_be_bytes(stamp.try_into().ok()?))
}

/// `template` with its timestamps and its first record stamped with `sent`,
/// and the CRC recomputed.
fn stamped(template: &Bytes, value_at: usize, sent: u64) -> Bytes {
    let mut batch = BytesMut::from(&template[..]);
    let millis = i64::try_from(sent / 1_000_000).unwrap_or(i64::MAX);
    batch[BASE_TIMESTAMP_AT..BASE_TIMESTAMP_AT + 8].copy_from_slice(&millis.to_be_bytes());
    batch[MAX_TIMESTAMP_AT..MAX_TIMESTAMP_AT + 8].copy_from_slice(&millis.to_be_bytes());
    batch[value_at..value_at + 8].copy_from_slice(&sent.to_be_bytes());
    let crc = crc32c::crc32c(&batch[CRC_FROM..]);
    batch[CRC_AT..CRC_AT + 4].copy_from_slice(&crc.to_be_bytes());
    batch.freeze()
}

fn produce_request(topic: &TopicRef, acks: i16, batches: Vec<(i32, Bytes)>) -> ProduceRequest {
    ProduceRequest {
        acks,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.name.clone(),
            topic_id: topic.id,
            partition_data: batches
                .into_iter()
                .map(|(index, batch)| PartitionProduceData {
                    index,
                    records: Some(RecordsPayload::Raw(batch)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Sends `request` and tallies its records, its latency and its errors.
async fn send_and_tally(
    connection: &Connection,
    request: ProduceRequest,
    records: u64,
    sent: u64,
    shared: &Shared,
) {
    let started = Instant::now();
    let response = connection.send(request).await;
    let latency = started.elapsed();
    match response {
        Ok(response) => {
            let failed = response
                .responses
                .iter()
                .flat_map(|topic| &topic.partition_responses)
                .find(|partition| partition.error_code != 0);
            if let Some(partition) = failed {
                shared.error(format!(
                    "partition {} answered error code {}",
                    partition.index, partition.error_code
                ));
                return;
            }
            if shared.in_window(sent) {
                shared.produced.fetch_add(records, Ordering::Relaxed);
                let micros = u64::try_from(latency.as_micros())
                    .unwrap_or(u64::MAX)
                    .max(1);
                let mut ack = shared.ack.lock().expect("ack histogram");
                let high = ack.high();
                let _ = ack.record_n(micros.min(high), records);
            }
        }
        Err(error) => shared.error(format!("produce: {error}")),
    }
}

struct SaturateTask {
    connection: Connection,
    topic: TopicRef,
    partitions: Vec<i32>,
    template: Bytes,
    records: usize,
    acks: i16,
    shared: Arc<Shared>,
}

async fn produce_saturate(task: SaturateTask) {
    let SaturateTask {
        connection,
        topic,
        partitions,
        template,
        records,
        acks,
        shared,
    } = task;
    let Some(value_at) = first_value_at(&template) else {
        shared.error("the batch template has no first record".to_owned());
        return;
    };
    let records = u64::try_from(records * partitions.len()).unwrap_or(u64::MAX);
    while !shared.stop.load(Ordering::Relaxed) {
        let sent = now_ns();
        let batches = partitions
            .iter()
            .map(|&partition| (partition, stamped(&template, value_at, sent)))
            .collect();
        let request = produce_request(&topic, acks, batches);
        send_and_tally(&connection, request, records, sent, &shared).await;
    }
}

struct PacedTask {
    routes: Vec<(Connection, i32)>,
    topic: TopicRef,
    template: Bytes,
    acks: i16,
    rate: Frequency,
    shared: Arc<Shared>,
}

async fn produce_paced(task: PacedTask) {
    let PacedTask {
        routes,
        topic,
        template,
        acks,
        rate,
        shared,
    } = task;
    let Some(value_at) = first_value_at(&template) else {
        shared.error("the batch template has no first record".to_owned());
        return;
    };
    let mut pacer = Pacer::new(rate);
    // The sends run on this task rather than one spawned task each, so an ack
    // is settled where it lands and its latency carries no scheduling hop.
    let mut sends = FuturesUnordered::new();
    for (connection, partition) in routes.iter().cycle() {
        if shared.stop.load(Ordering::Relaxed) {
            break;
        }
        let token = pacer.await_token();
        tokio::pin!(token);
        loop {
            tokio::select! {
                biased;
                Some(()) = sends.next(), if !sends.is_empty() => {}
                () = &mut token => break,
            }
        }
        let sent = now_ns();
        let request = produce_request(&topic, acks, vec![(
            *partition,
            stamped(&template, value_at, sent),
        )]);
        let shared = &shared;
        sends.push(async move {
            send_and_tally(connection, request, 1, sent, shared).await;
        });
    }
    while sends.next().await.is_some() {}
}

/// Fetches `partitions` from their leader from offset zero, and tallies every
/// batch that was sent inside the window.
async fn consume(
    connection: Connection,
    topic: TopicRef,
    partitions: Vec<i32>,
    shared: Arc<Shared>,
) {
    let mut offsets: BTreeMap<i32, i64> = partitions.iter().map(|&p| (p, 0)).collect();
    loop {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: topic.name.clone(),
                topic_id: topic.id,
                partitions: offsets
                    .iter()
                    .map(|(&partition, &fetch_offset)| FetchPartition {
                        partition,
                        current_leader_epoch: -1,
                        fetch_offset,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 8 * 1024 * 1024,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let Ok(response) = connection.send(RawFetch(request)).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let now = now_ns();
        let read_in_window = shared.in_window(now);
        let mut tally = Vec::new();
        for fetched in &response.partitions {
            if fetched.error_code == OFFSET_OUT_OF_RANGE {
                // Retention deleted what this consumer had not read yet.
                offsets.insert(fetched.partition, fetched.log_start_offset);
                continue;
            }
            let Some(records) = &fetched.records else {
                continue;
            };
            for batch in BatchHeaders::of(records) {
                offsets.insert(fetched.partition, batch.next_offset);
                if read_in_window {
                    shared.committed.fetch_add(batch.records, Ordering::Relaxed);
                }
                if let Some(sent) = batch.sent_ns
                    && shared.in_window(sent)
                {
                    tally.push((now.saturating_sub(sent) / 1_000, batch.records));
                }
            }
        }
        if !tally.is_empty() {
            let mut end_to_end = shared.end_to_end.lock().expect("e2e histogram");
            for (micros, records) in tally {
                shared.consumed.fetch_add(records, Ordering::Relaxed);
                let high = end_to_end.high();
                let _ = end_to_end.record_n(micros.clamp(1, high), records);
            }
        }
    }
}

/// The fields of one fetched batch the consumer tallies, read from its header
/// and its first record without decoding the rest.
struct BatchHeader {
    /// The offset after the batch's last record.
    next_offset: i64,
    records: u64,
    sent_ns: Option<u64>,
}

/// The complete v2 batches in one partition's fetched records bytes. A
/// trailing partial batch, which a fetch's byte limit can cut, is skipped.
struct BatchHeaders<'a> {
    rest: &'a [u8],
}

impl<'a> BatchHeaders<'a> {
    fn of(records: &'a [u8]) -> Self { Self { rest: records } }
}

impl Iterator for BatchHeaders<'_> {
    type Item = BatchHeader;

    fn next(&mut self) -> Option<BatchHeader> {
        let header = self.rest.get(..BATCH_HEADER_LEN)?;
        let field = |at: usize, len: usize| header.get(at..at + len);
        let base_offset = i64::from_be_bytes(field(0, 8)?.try_into().ok()?);
        let length = i32::from_be_bytes(field(8, 4)?.try_into().ok()?);
        let last_offset_delta = i32::from_be_bytes(field(23, 4)?.try_into().ok()?);
        let records = i32::from_be_bytes(field(57, 4)?.try_into().ok()?);
        let total = 12 + usize::try_from(length).ok()?;
        let batch = self.rest.get(..total)?;
        self.rest = &self.rest[total..];
        Some(BatchHeader {
            next_offset: base_offset + i64::from(last_offset_delta) + 1,
            records: u64::try_from(records).unwrap_or(0),
            sent_ns: sent_ns(batch),
        })
    }
}

/// A consumer `Fetch` pinned to v12, whose response keeps every partition's
/// records as the bytes the broker sent. The typed response decodes every
/// record, which at small record sizes costs the generator more CPU than the
/// broker spends serving it.
struct RawFetch(FetchRequest);

impl Encode for RawFetch {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize { self.0.encoded_len(version) }
}

impl ProtocolRequest for RawFetch {
    type Response = RawFetched;

    const API_KEY: i16 = <FetchRequest as ProtocolRequest>::API_KEY;
    const FLEXIBLE_MIN: i16 = <FetchRequest as ProtocolRequest>::FLEXIBLE_MIN;
    const LATEST_STABLE_VERSION: i16 = RAW_FETCH_VERSION;
    const MAX_VERSION: i16 = RAW_FETCH_VERSION;
    const MIN_VERSION: i16 = RAW_FETCH_VERSION;
}

/// Fetch v12: the last version that names topics, and a flexible one.
const RAW_FETCH_VERSION: i16 = 12;

/// `OFFSET_OUT_OF_RANGE`.
const OFFSET_OUT_OF_RANGE: i16 = 1;

/// Each fetched partition and its records bytes.
struct RawFetched {
    partitions: Vec<FetchedPartition>,
}

/// One partition of a [`RawFetched`] response.
struct FetchedPartition {
    partition: i32,
    error_code: i16,
    log_start_offset: i64,
    records: Option<Bytes>,
}

impl Decode<'_> for RawFetched {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if version != RAW_FETCH_VERSION {
            return Err(ProtocolError::UnsupportedVersion {
                api_key: <FetchRequest as ProtocolRequest>::API_KEY,
                version,
            });
        }
        let _throttle_time_ms = get_i32(buf)?;
        let _error_code = get_i16(buf)?;
        let _session_id = get_i32(buf)?;
        let mut partitions = Vec::new();
        for _ in 0..get_array_len(buf, true)? {
            let _topic = get_compact_string_owned(buf)?;
            for _ in 0..get_array_len(buf, true)? {
                let partition = get_i32(buf)?;
                let error_code = get_i16(buf)?;
                let _high_watermark = get_i64(buf)?;
                let _last_stable_offset = get_i64(buf)?;
                let log_start_offset = get_i64(buf)?;
                if let Some(aborted) = get_nullable_array_len(buf, true)? {
                    for _ in 0..aborted {
                        AbortedTransaction::decode(buf, version)?;
                    }
                }
                let _preferred_read_replica = get_i32(buf)?;
                let records = get_compact_nullable_bytes_owned(buf)?;
                read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
                partitions.push(FetchedPartition {
                    partition,
                    error_code,
                    log_start_offset,
                    records,
                });
            }
            read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        }
        read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        Ok(Self { partitions })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn a_stamped_batch_decodes_with_its_send_time() {
        let template = batch_template(bytes(100), 3).expect("template");
        let value_at = first_value_at(&template).expect("first value");
        let batch = stamped(&template, value_at, 1_700_000_000_123_456_789);
        check!(sent_ns(&batch) == Some(1_700_000_000_123_456_789));
        let mut cursor: &[u8] = &batch;
        let decoded = RecordBatch::decode(&mut cursor).expect("the CRC matches");
        check!(decoded.records.len() == 3);
        check!(decoded.base_timestamp == 1_700_000_000_123);
    }

    #[test]
    fn batch_headers_walk_complete_batches_and_skip_a_cut_tail() {
        let template = batch_template(bytes(100), 4).expect("template");
        let value_at = first_value_at(&template).expect("first value");
        let mut first = BytesMut::from(&stamped(&template, value_at, 11)[..]);
        first[..8].copy_from_slice(&10_i64.to_be_bytes());
        let mut second = BytesMut::from(&stamped(&template, value_at, 22)[..]);
        second[..8].copy_from_slice(&14_i64.to_be_bytes());
        let mut records = first.clone();
        records.extend_from_slice(&second);
        records.extend_from_slice(&first[..20]);

        let headers = BatchHeaders::of(&records)
            .map(|batch| (batch.next_offset, batch.records, batch.sent_ns))
            .collect::<Vec<_>>();

        check!(headers == vec![(14, 4, Some(11)), (18, 4, Some(22))]);
    }
}
