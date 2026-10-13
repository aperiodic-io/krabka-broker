//! Sustained produce traffic against a running `profile_server`.
//!
//! Run `profile_server` in one shell and this in another. This process makes
//! the load and prints what it achieved; the other process is the one you
//! attach the profiler to, so the profile holds no client work.
//!
//! ```text
//! cargo run --release -p krabka-broker --example loadgen
//! ```
//!
//! The numbers this prints measure the pair of processes on one machine. They
//! are a profiling aid and not a benchmark: `bench/` is the harness that
//! measures a cluster. Nothing in CI runs this.
//!
//! # Environment
//!
//! | Variable | Default | What it sets |
//! | :--- | :--- | :--- |
//! | `LOAD_BOOTSTRAP` | `127.0.0.1:9092` | The broker address. |
//! | `LOAD_TOPIC` | `loadgen` | The topic. It is created if it is absent. |
//! | `LOAD_PARTITIONS` | `8` | The partition count of that topic. |
//! | `LOAD_PRODUCERS` | `4` | Concurrent producer tasks. |
//! | `LOAD_VALUE_BYTES` | `128` | The record value size. |
//! | `LOAD_SECONDS` | `20` | How long to run. |
//! | `LOAD_INFLIGHT` | `1000` | Unacknowledged sends held per producer. |
//! | `LOAD_ACKS` | `1` | `0`, `1` or `all`. |
//! | `LOAD_CONSUME` | `0` | `1` also runs one consumer group. |

use {
    bytes::Bytes,
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    krabka_client_core::Client,
    krabka_client_producer::{Acks, Producer, ProducerRecord},
    krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest},
    num_traits::ToPrimitive,
    std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
};

/// `TOPIC_ALREADY_EXISTS`. A second run against the same broker is expected.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// One environment variable, parsed, or `default` when it is absent or does
/// not parse.
fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A counter as an `f64`, for the rates printed at the end.
///
/// `to_f64` rather than an `as` cast: the cast is lossy above 2^53 and says
/// nothing about it, and this crate's lint set rejects it.
fn as_f64(value: u64) -> f64 { value.to_f64().expect("a u64 below 2^53 is exact as an f64") }

/// A byte count as a `u64`. `usize` is 64 bits on every target this runs on.
fn bytes_as_u64(value: usize) -> u64 { u64::try_from(value).unwrap_or(u64::MAX) }

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let bootstrap: String =
        std::env::var("LOAD_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic: String = std::env::var("LOAD_TOPIC").unwrap_or_else(|_| "loadgen".into());
    let partitions: i32 = env("LOAD_PARTITIONS", 8);
    let producers: usize = env("LOAD_PRODUCERS", 4);
    let value_bytes: usize = env("LOAD_VALUE_BYTES", 128);
    let seconds: u64 = env("LOAD_SECONDS", 20);
    let inflight: usize = env("LOAD_INFLIGHT", 1000);
    let acks_name: String = std::env::var("LOAD_ACKS").unwrap_or_else(|_| "1".into());
    let consume: bool = env::<u8>("LOAD_CONSUME", 0) == 1;

    let acks = match acks_name.as_str() {
        "0" => Acks::Zero,
        "all" => Acks::All,
        _ => Acks::One,
    };

    create_topic(&bootstrap, &topic, partitions).await;

    let value = Bytes::from(vec![0xABu8; value_bytes]);
    let sent = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let start = Instant::now();
    let mut tasks = Vec::new();
    for index in 0..producers {
        tasks.push(tokio::spawn(produce(Load {
            bootstrap: bootstrap.clone(),
            topic: topic.clone(),
            value: value.clone(),
            acks,
            inflight,
            index,
            sent: Arc::clone(&sent),
            acked: Arc::clone(&acked),
            stop: Arc::clone(&stop),
        })));
    }
    if consume {
        tasks.push(tokio::spawn(consume_group(
            bootstrap.clone(),
            topic.clone(),
            Arc::clone(&stop),
        )));
    }

    tokio::time::sleep(Duration::from_secs(seconds)).await;
    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = task.await;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let acked = acked.load(Ordering::Relaxed);
    let sent = sent.load(Ordering::Relaxed);
    let megabytes = as_f64(acked) * as_f64(bytes_as_u64(value_bytes)) / 1.0e6;
    println!(
        "loadgen: acks={acks_name} producers={producers} value={value_bytes}B \
         parts={partitions} | sent={sent} acked={acked} | {:.0} msg/s | {:.1} MB/s \
         over {elapsed:.1}s",
        as_f64(acked) / elapsed,
        megabytes / elapsed,
    );
}

/// Create the topic, and accept the broker that already has it.
async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("loadgen-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    let code = response.topics[0].error_code;
    assert2::assert!(code == 0 || code == TOPIC_ALREADY_EXISTS);
}

/// What one producer task needs. A struct rather than nine parameters.
struct Load {
    bootstrap: String,
    topic: String,
    value: Bytes,
    acks: Acks,
    inflight: usize,
    index: usize,
    sent: Arc<AtomicU64>,
    acked: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

/// One producer task: keep `inflight` sends outstanding until `stop` is set.
async fn produce(load: Load) {
    let Load {
        bootstrap,
        topic,
        value,
        acks,
        inflight,
        index,
        sent,
        acked,
        stop,
    } = load;
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("loadgen-{index}"))
        .enable_idempotence(false)
        .acks(acks)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let mut window = VecDeque::new();
    while !stop.load(Ordering::Relaxed) {
        while window.len() < inflight {
            let pending = producer
                .enqueue(ProducerRecord {
                    topic: topic.clone(),
                    value: Some(value.clone()),
                    ..Default::default()
                })
                .await;
            sent.fetch_add(1, Ordering::Relaxed);
            if let Ok(handle) = pending {
                window.push_back(handle);
            } else {
                break;
            }
        }
        if let Some(pending) = window.pop_front()
            && pending.await.is_ok()
        {
            acked.fetch_add(1, Ordering::Relaxed);
        }
    }
    let _ = producer.flush().await;
    for pending in window {
        if pending.await.is_ok() {
            acked.fetch_add(1, Ordering::Relaxed);
        }
    }
    producer.close().await.ok();
}

/// One consumer group, so the fetch path carries load as well.
async fn consume_group(bootstrap: String, topic: String, stop: Arc<AtomicBool>) {
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("loadgen-consumer")
        .group_id("loadgen-grp")
        .session_timeout(krabka_units::secs(30))
        .max_poll_interval(krabka_units::secs(5))
        .heartbeat_interval(krabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic])
        .build()
        .await
        .expect("consumer build");
    while !stop.load(Ordering::Relaxed) {
        let _ = consumer.poll(krabka_units::millis(200)).await;
    }
    consumer.close().await.ok();
}
