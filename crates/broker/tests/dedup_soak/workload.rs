//! The records a soak cycle produces, and the producers that send them.
//!
//! Every cycle sends the same mix, so the latency of one cycle compares with
//! the latency of another:
//!
//! - **The steady load.** Several producers, each pacing itself to a fixed
//!   request rate, write batches of mostly unique identities to every
//!   partition. A batch also carries one identity twice, one record dated
//!   three windows back (expired on arrival, admitted unindexed) and one dated
//!   a window ahead (clamped to the broker clock). Every few requests a
//!   producer sends a burst of duplicates instead: the last batch again
//!   (every identity dropped), or half of it with half new (a partial drop,
//!   which rebuilds the batch). Every tenth request carries a few records with
//!   2 KiB keys and 24 KiB values.
//! - **The saturation.** One hot partition takes half again its entry limit
//!   of identities, so its own limit binds. On even cycles they are dated
//!   across the window, so the cursor's capacity histogram can find the
//!   oldest quarter and evicts it; on odd cycles they are all dated now, one
//!   bucket of the histogram that it cannot split, so the identities past the
//!   limit are admitted unindexed (fail-open). Then every partition at once
//!   takes its share of a quarter more than the broker limit, so the broker
//!   budget binds and each partition trims its own oldest entries.
//!
//! On a header-mode topic the identity is the `dedup-id` header and the keys
//! repeat on purpose; one record in fifty has no header and so no identity.

use {
    crate::support::{
        dedup::{Rec, keyed_batch, produce},
        produce::ProduceAcknowledgements,
    },
    futures_util::future::join_all,
    krabka_client_core::Client,
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        sync::{Arc, Mutex, PoisonError},
        time::{Duration, Instant},
    },
    tokio::time::{MissedTickBehavior, interval},
};

/// The bytes of a large record's key.
const LARGE_KEY_BYTES: usize = 2 * 1024;

/// The bytes of a large record's value.
const LARGE_VALUE_BYTES: usize = 24 * 1024;

/// The records of a large batch.
const LARGE_RECORDS: u64 = 4;

/// The records of one saturation batch.
const SATURATION_BATCH: u64 = 1_000;

/// Distinct keys of a header-mode topic: its identities are in the header.
const HEADER_MODE_KEYS: u64 = 64;

/// One record in this many has no `dedup-id` header on a header-mode topic.
const HEADERLESS_EVERY: u64 = 50;

/// Which part of a record a topic deduplicates on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
    /// The record key.
    Key,
    /// The `dedup-id` header.
    Header,
}

/// How a topic deduplicates: the identity rule and the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub identity: Identity,
    pub window_ms: i64,
}

/// One partition the producers write to, with a client of its leader.
#[derive(Clone)]
pub struct Target {
    pub topic: Arc<str>,
    pub topic_id: Uuid,
    pub partition: i32,
    pub rule: Rule,
    /// A client that knows only the partition's leader, so every request it
    /// sends goes there.
    pub leader: Client,
}

/// Produce latencies, which the producers add to and the recorder takes.
#[derive(Clone, Default)]
pub struct LatencySink(Arc<Mutex<Vec<Duration>>>);

impl LatencySink {
    fn push(&self, took: Duration) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(took);
    }

    /// Every latency added since the last call.
    pub fn take(&self) -> Vec<Duration> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

/// What a set of produce requests did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tally {
    pub requests: u64,
    pub records: u64,
    /// Each request a partition refused: the partition and its error code.
    pub failures: Vec<String>,
}

impl Tally {
    pub fn absorb(&mut self, other: Self) {
        self.requests += other.requests;
        self.records += other.records;
        self.failures.extend(other.failures);
    }
}

/// Send `records` to `target` as one batch, `acks=1`, and note the outcome.
///
/// `acks=1` keeps the latency the leader's: a follower that a restart or a
/// slow tick took out of the ISR does not stall the request.
async fn send(target: &Target, records: &[Rec], sink: &LatencySink, tally: &mut Tally) {
    let batch = keyed_batch().records(records).call();
    let started = Instant::now();
    let row = produce()
        .client(&target.leader)
        .topic(&target.topic)
        .topic_id(target.topic_id)
        .partition(target.partition)
        .batch(batch)
        .acks(ProduceAcknowledgements::Leader)
        .call()
        .await;
    sink.push(started.elapsed());
    tally.requests += 1;
    tally.records += u64::try_from(records.len()).expect("a record count");
    if row.error_code != 0 {
        tally.failures.push(format!(
            "{}-{} refused a batch of {} records with error {}",
            target.topic,
            target.partition,
            records.len(),
            row.error_code
        ));
    }
}

/// Identities no other record of the run carries.
struct Identities {
    prefix: String,
    next: u64,
}

impl Identities {
    fn new(cycle: usize, source: &str) -> Self {
        Self {
            prefix: format!("c{cycle}-{source}"),
            next: 0,
        }
    }

    fn next(&mut self) -> (u64, String) {
        let n = self.next;
        self.next += 1;
        (n, format!("{}-{n}", self.prefix))
    }
}

/// One record whose identity is `id` under `rule`, created `age_ms` before
/// now.
fn record(rule: Rule, n: u64, id: &str, age_ms: i64) -> Rec {
    let record = match rule.identity {
        Identity::Key => Rec::key(id),
        Identity::Header if n % HEADERLESS_EVERY == HEADERLESS_EVERY - 1 => {
            Rec::key(&format!("key-{}", n % HEADER_MODE_KEYS))
        }
        Identity::Header => Rec::key(&format!("key-{}", n % HEADER_MODE_KEYS)).dedup_id(id),
    };
    record.aged(age_ms)
}

/// A steady batch: `count` new identities, the first of them twice, one
/// record expired on arrival and one dated in the future.
fn unique_batch(rule: Rule, ids: &mut Identities, count: u64) -> Vec<Rec> {
    let mut records: Vec<Rec> = (0..count)
        .map(|_| {
            let (n, id) = ids.next();
            record(rule, n, &id, 0)
        })
        .collect();
    records.push(records[0].clone());
    let (n, id) = ids.next();
    records.push(record(rule, n, &id, 3 * rule.window_ms));
    let (n, id) = ids.next();
    records.push(record(rule, n, &id, -rule.window_ms));
    records
}

/// A few records with large keys and values.
fn large_batch(rule: Rule, ids: &mut Identities) -> Vec<Rec> {
    (0..LARGE_RECORDS)
        .map(|_| {
            let (n, id) = ids.next();
            let mut large = record(rule, n, &id, 0);
            let mut key = id.into_bytes();
            key.resize(LARGE_KEY_BYTES, b'k');
            large.key = Some(key);
            large.value = vec![b'v'; LARGE_VALUE_BYTES];
            large
        })
        .collect()
}

/// What the `seq`-th steady request of a producer carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    Unique,
    Replay,
    HalfReplay,
    Large,
}

impl Shape {
    fn of(seq: u64) -> Self {
        match seq % 10 {
            3 => Self::Replay,
            6 => Self::HalfReplay,
            9 => Self::Large,
            _ => Self::Unique,
        }
    }
}

/// The steady load of one producer.
#[derive(Clone, Copy, Debug)]
pub struct SteadyPlan {
    pub cycle: usize,
    pub producer: usize,
    pub duration: Duration,
    pub requests_per_second: u32,
    pub records_per_batch: u64,
}

/// Produce the steady load to `targets`, one request per tick, round robin.
pub async fn steady(plan: SteadyPlan, targets: Vec<Target>, sink: LatencySink) -> Tally {
    let mut ids = Identities::new(plan.cycle, &format!("p{}", plan.producer));
    let mut last: Vec<Vec<Rec>> = vec![Vec::new(); targets.len()];
    let mut tally = Tally::default();
    let mut ticks = interval(Duration::from_secs(1) / plan.requests_per_second);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let deadline = Instant::now() + plan.duration;
    let mut seq = 0_u64;
    while Instant::now() < deadline {
        ticks.tick().await;
        let slot = usize::try_from(seq).expect("a request count") % targets.len();
        let target = &targets[slot];
        let shape = match Shape::of(seq) {
            Shape::Replay | Shape::HalfReplay if last[slot].is_empty() => Shape::Unique,
            shape => shape,
        };
        let records = match shape {
            Shape::Unique => unique_batch(target.rule, &mut ids, plan.records_per_batch),
            Shape::Replay => last[slot].clone(),
            Shape::HalfReplay => {
                let half = last[slot].len() / 2;
                let fresh = unique_batch(target.rule, &mut ids, plan.records_per_batch / 2);
                last[slot][..half].iter().cloned().chain(fresh).collect()
            }
            Shape::Large => large_batch(target.rule, &mut ids),
        };
        send(target, &records, &sink, &mut tally).await;
        if shape == Shape::Unique {
            last[slot] = records;
        }
        seq += 1;
    }
    tally
}

/// Send `total` new identities to `target` in saturation batches, the
/// identities spread evenly over the last `spread_ms` milliseconds, oldest
/// first.
async fn flood(
    target: &Target,
    ids: &mut Identities,
    total: u64,
    spread_ms: i64,
    sink: &LatencySink,
) -> Tally {
    let mut tally = Tally::default();
    let total_ms = i64::try_from(total).expect("an identity count");
    let mut sent = 0;
    while sent < total {
        let count = SATURATION_BATCH.min(total - sent);
        let records: Vec<Rec> = (0..count)
            .map(|_| {
                let (n, id) = ids.next();
                let remaining = total_ms - i64::try_from(sent).expect("an identity count");
                sent += 1;
                record(target.rule, n, &id, spread_ms * remaining / total_ms)
            })
            .collect();
        send(target, &records, sink, &mut tally).await;
    }
    tally
}

/// Saturate the per-partition limit of `hot`: half again its limit of new
/// identities, dated across nine tenths of its window on an even `cycle`
/// and all dated now on an odd one.
pub async fn saturate_partition(
    cycle: usize,
    max_entries_per_partition: u64,
    hot: &Target,
    sink: &LatencySink,
) -> Tally {
    let mut ids = Identities::new(cycle, "hot");
    let spread_ms = if cycle.is_multiple_of(2) {
        hot.rule.window_ms * 9 / 10
    } else {
        0
    };
    flood(
        hot,
        &mut ids,
        max_entries_per_partition * 3 / 2,
        spread_ms,
        sink,
    )
    .await
}

/// Saturate the broker budget: every target at once takes its share of a
/// quarter more new identities than `max_entries`, all dated now.
pub async fn saturate_brokers(
    cycle: usize,
    max_entries: u64,
    targets: &[Target],
    sink: &LatencySink,
) -> Tally {
    let share = (max_entries * 5 / 4).div_ceil(u64::try_from(targets.len()).expect("count"));
    let floods = targets.iter().enumerate().map(|(i, target)| async move {
        let mut ids = Identities::new(cycle, &format!("spray{i}"));
        flood(target, &mut ids, share, 0, sink).await
    });
    let mut tally = Tally::default();
    for spray in join_all(floods).await {
        tally.absorb(spray);
    }
    tally
}

mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    const KEY_RULE: Rule = Rule {
        identity: Identity::Key,
        window_ms: 2_000,
    };

    const HEADER_RULE: Rule = Rule {
        identity: Identity::Header,
        window_ms: 2_000,
    };

    fn text(bytes: Option<&[u8]>) -> Option<String> {
        bytes.map(|bytes| String::from_utf8_lossy(bytes).into_owned())
    }

    fn dedup_ids(records: &[Rec]) -> Vec<Option<String>> {
        records
            .iter()
            .map(|r| {
                let header = r.headers.iter().find(|(name, _)| name == "dedup-id");
                header.and_then(|(_, value)| text(value.as_deref()))
            })
            .collect()
    }

    #[test]
    fn a_steady_batch_repeats_one_identity_and_dates_two_outside_the_window() {
        let batch = unique_batch(KEY_RULE, &mut Identities::new(4, "p1"), 3);
        let keys: Vec<_> = batch.iter().map(|r| text(r.key.as_deref())).collect();
        let ages: Vec<_> = batch.iter().map(|r| r.age_ms).collect();
        let expected = [
            "c4-p1-0", "c4-p1-1", "c4-p1-2", "c4-p1-0", "c4-p1-3", "c4-p1-4",
        ];
        assert!(keys == expected.map(|key| Some(key.to_owned())));
        assert!(ages == [0, 0, 0, 0, 6_000, -2_000]);
        assert!(dedup_ids(&batch) == [None, None, None, None, None, None]);
    }

    #[test]
    fn a_header_mode_record_carries_its_identity_in_the_header() {
        let batch = unique_batch(HEADER_RULE, &mut Identities::new(0, "p0"), 49);
        let ids = dedup_ids(&batch);
        assert!(ids[0].as_deref() == Some("c0-p0-0"));
        // The repeat of the first identity.
        assert!(ids[49].as_deref() == Some("c0-p0-0"));
        // The fiftieth identity, n = 49, goes out without a header.
        assert!(ids[50].is_none());
        assert!(ids[51].as_deref() == Some("c0-p0-50"));
        assert!(text(batch[1].key.as_deref()).as_deref() == Some("key-1"));
        assert!(text(batch[51].key.as_deref()).as_deref() == Some("key-50"));
    }

    #[test]
    fn a_large_batch_pads_keys_and_values() {
        let batch = large_batch(KEY_RULE, &mut Identities::new(0, "p0"));
        assert!(batch.len() == 4);
        assert!(
            batch
                .iter()
                .all(|r| r.key.as_ref().map(Vec::len) == Some(2_048))
        );
        assert!(batch.iter().all(|r| r.value.len() == 24 * 1_024));
    }

    #[yare::parameterized(
        first = { 0, Shape::Unique },
        replay = { 3, Shape::Replay },
        half = { 16, Shape::HalfReplay },
        large = { 29, Shape::Large },
    )]
    fn the_steady_mix_repeats_every_ten_requests(seq: u64, shape: Shape) {
        check!(Shape::of(seq) == shape);
    }
}
