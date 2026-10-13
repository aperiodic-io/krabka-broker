//! KFC-10 memory bounds and stall freedom, driven through the Kafka protocol
//! against one in-process broker.
//!
//! `krabka-dedup` proves the per-call bounds of the index with counts. This
//! suite proves them end to end: the `dedup_max_entries_per_partition` and
//! `dedup_max_entries` limits hold as the broker's own gauges report them,
//! entries go back to the broker budget when a topic stops deduplicating, a
//! produce never fails or stalls when a limit is hit or the index grows, and
//! a rebuild after a restart does not hold up fetches of its partition.
//!
//! The timed assertions leave wide margins for a loaded machine running a
//! debug build. They catch a multi-second hang, a lock held across a whole
//! rebuild, or a cost proportional to the index, not a regression of a few
//! milliseconds. Each timed test prints what it measured. The long variants
//! are ignored; run them with
//! `cargo test -p krabka-broker --test dedup_bounds -- --ignored --nocapture`.

mod support;

#[path = "dedup_bounds/budget.rs"]
mod budget;
#[path = "dedup_bounds/latency.rs"]
mod latency;
#[path = "dedup_bounds/rebuild.rs"]
mod rebuild;

use {
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        fmt,
        ops::Range,
        time::{Duration, Instant},
    },
    support::dedup::{Rec, broker_metric, keyed_batch, partition_metric, produce},
};

// The window of every dedup topic in this suite: long enough that nothing
// expires while a test runs, so only the entry limits remove entries.
const HOUR_MS: i64 = 3_600_000;

// The spacing of `Arrival::Spread` record timestamps.
const SPREAD_STEP_MS: i64 = 100;

// The highest key a spread test uses. Key `n` is created
// `(SPREAD_KEYS - n) * SPREAD_STEP_MS` before now, inside `HOUR_MS`.
const SPREAD_KEYS: u64 = 30_000;

// One partition of a topic, as Produce v13 and Fetch v13 address it.
#[derive(Clone, Copy, Debug)]
struct Partition<'a> {
    topic: &'a str,
    topic_id: Uuid,
    index: i32,
}

// How the timestamps of a run of new identities are laid out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Arrival {
    // Every record is created now. More than a quarter of a full index then
    // shares one bucket of the capacity histogram, which cannot split it, so
    // a new identity at the limit is admitted unindexed.
    Burst,
    // Key `n` is created `SPREAD_STEP_MS` after key `n - 1`, as a steady
    // stream would create it. The capacity histogram can then find the oldest
    // quarter, and a new identity at the limit evicts the oldest entries.
    Spread,
}

// Records keyed `{prefix}-{n}` for each `n` in `keys`, each with a value of
// `value_bytes` bytes, or `value-of-{key}` when `value_bytes` is zero.
#[bon::builder]
fn numbered(
    #[builder(default = "key")] prefix: &str,
    keys: Range<u64>,
    #[builder(default = Arrival::Burst)] arrival: Arrival,
    #[builder(default)] value_bytes: usize,
) -> Vec<Rec> {
    keys.map(|n| {
        let mut record = Rec::key(&format!("{prefix}-{n}"));
        if value_bytes > 0 {
            record.value = vec![b'v'; value_bytes];
        }
        match arrival {
            Arrival::Burst => record,
            Arrival::Spread => {
                let steps = i64::try_from(SPREAD_KEYS.saturating_sub(n)).expect("spread steps");
                record.aged(steps * SPREAD_STEP_MS)
            }
        }
    })
    .collect()
}

// Produce `records` as one batch with `acks=all`, assert that the partition
// accepted it, and return how long the request took.
async fn timed_produce(client: &Client, target: Partition<'_>, records: &[Rec]) -> Duration {
    let batch = keyed_batch().records(records).call();
    let started = Instant::now();
    let row = produce()
        .client(client)
        .topic(target.topic)
        .topic_id(target.topic_id)
        .partition(target.index)
        .batch(batch)
        .call()
        .await;
    let took = started.elapsed();
    assert!(row.error_code == 0, "{target:?}");
    took
}

// A gauge or counter value. The broker sets these to whole numbers only.
fn whole(value: f64) -> u64 {
    value
        .to_string()
        .parse()
        .expect("a whole, non-negative metric value")
}

// The `dedup_index_entries` gauge of one partition: 0 when the series is
// absent.
async fn index_entries(broker: &BrokerHandle, target: Partition<'_>) -> u64 {
    counter(broker, "dedup_index_entries", target).await
}

// One per-partition KFC-10 series, 0 when it is absent.
async fn counter(broker: &BrokerHandle, name: &str, target: Partition<'_>) -> u64 {
    whole(partition_metric(broker, name, target.topic, target.index).await)
}

// One broker-wide KFC-10 gauge.
async fn broker_gauge(broker: &BrokerHandle, name: &str) -> u64 {
    whole(broker_metric(broker, name).await)
}

// Request latencies of one workload.
#[derive(Clone, Debug, Default)]
struct Latencies(Vec<Duration>);

impl Latencies {
    fn push(&mut self, took: Duration) { self.0.push(took); }

    // The latency below which `percent` percent of the requests finished.
    fn percentile(&self, percent: usize) -> Duration {
        let mut sorted = self.0.clone();
        sorted.sort_unstable();
        sorted
            .get(sorted.len().saturating_sub(1) * percent / 100)
            .copied()
            .unwrap_or_default()
    }

    fn median(&self) -> Duration { self.percentile(50) }

    fn p99(&self) -> Duration { self.percentile(99) }

    fn max(&self) -> Duration { self.percentile(100) }
}

impl fmt::Display for Latencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "n={} median={:?} p99={:?} max={:?}",
            self.0.len(),
            self.median(),
            self.p99(),
            self.max()
        )
    }
}
