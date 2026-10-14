//! Seeded flapping partitions under concurrent keyed producers.
//!
//! For a few tens of seconds a fault schedule, drawn from a seed, isolates
//! the partition leader or a random node, isolates two nodes at once so that
//! no quorum is left, cuts two nodes apart while both still reach the third,
//! or cuts one direction between two nodes, holds each fault for a random
//! while, and usually heals before the next. Meanwhile several producers write
//! batches of new keys mixed with deliberate repeats of keys some leader
//! already acknowledged, each with `acks=all`, and retry a batch at whichever
//! broker the newest metadata names until it is acknowledged. Then the
//! network heals for good, and the run is judged by [`violations`]:
//! durability of every acknowledged record at the offset its acknowledgement
//! named, every key once, every repeat dropped. On top of that every replica
//! must hold the same log and an index of exactly its keys, and the cluster
//! must take a new write.
//!
//! The schedule is a function of the seed, so a failure names the seed to
//! replay with `DEDUP_PARTITION_SEED`; the interleaving with the producers is
//! not, so a replay reproduces the faults rather than the exact run.

use {
    crate::{
        harness::{
            Cluster, Topic, assert_indexes_match, converged, dedup_topic, newest_leader,
            offsets_and_keys, partition_on, produce, send,
        },
        history::{Acked, Sent, violations},
        support::{
            cluster_lock,
            dedup::{Rec, keyed_batch},
            produce::ProduceAcknowledgements::AllReplicas,
        },
    },
    assert2::assert,
    std::{
        collections::BTreeSet,
        sync::{
            Mutex, OnceLock,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

/// How many producers write at once.
const PRODUCERS: u64 = 3;

/// How long the faults last by default, per seed. CI-sized: with the cluster
/// boot, the settling and the checks, one seed takes about half a minute.
const DEFAULT_SECS: u64 = 20;

/// How long a producer goes on retrying its last batch after the faults end.
const SETTLE: Duration = Duration::from_secs(45);

/// The share of records, in percent, that repeat an acknowledged key.
const REPEAT_PERCENT: u64 = 30;

/// How long a leader may hold an `acks=all` write before it answers.
const PRODUCE_TIMEOUT_MS: i32 = 3_000;

// ---------------------------------------------------------------------------
// Seeds
// ---------------------------------------------------------------------------

/// `SplitMix64`: small, fast, and fully determined by its seed.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..bound`.
    fn below(&mut self, bound: u64) -> u64 { self.next() % bound }

    /// A node id, 1 to 3.
    fn node(&mut self) -> u64 { 1 + self.below(3) }
}

fn env_number(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name}={value} is not a whole number")),
    )
}

/// The seeds to run and the seconds of faults per seed.
fn plan() -> (Vec<u64>, u64) {
    let secs = env_number("DEDUP_PARTITION_SECS").unwrap_or(DEFAULT_SECS);
    if let Some(seed) = env_number("DEDUP_PARTITION_SEED") {
        return (vec![seed], secs);
    }
    let count = env_number("DEDUP_PARTITION_SEEDS").unwrap_or(1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is past 1970");
    let mut seeds = SplitMix(u64::from(now.subsec_nanos()) ^ now.as_secs());
    ((0..count).map(|_| seeds.next()).collect(), secs)
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Cut whichever node leads the partition off from both peers.
    IsolateLeader,
    /// Cut a node off from both peers.
    Isolate(u64),
    /// Cut two nodes off, which leaves no quorum.
    IsolateTwo(u64, u64),
    /// Cut two nodes apart; both still reach the third.
    Split(u64, u64),
    /// Stop one node from dialing another, which can still dial it.
    OneWay(u64, u64),
    /// Leave the network as it is.
    Calm,
}

impl Fault {
    fn draw(rng: &mut SplitMix) -> Self {
        let (a, b) = {
            let a = rng.node();
            (a, a % 3 + 1)
        };
        match rng.below(100) {
            0..40 => Self::IsolateLeader,
            40..60 => Self::Isolate(a),
            60..70 => Self::IsolateTwo(a, b),
            70..85 => Self::Split(a, b),
            85..93 => Self::OneWay(a, b),
            _ => Self::Calm,
        }
    }

    /// Apply the fault, and return it with the leader it isolated named.
    fn apply(self, cluster: &Cluster, topic: &str) -> Self {
        let links = cluster.links();
        match self {
            Self::IsolateLeader => {
                if let Some(leader) = newest_leader(cluster, topic) {
                    return Self::Isolate(leader).apply(cluster, topic);
                }
            }
            Self::Isolate(node) => links.isolate(node),
            Self::IsolateTwo(a, b) => {
                links.isolate(a);
                links.isolate(b);
            }
            Self::Split(a, b) => links.cut_between(a, b),
            Self::OneWay(from, to) => links.cut_path(from, to),
            Self::Calm => {}
        }
        self
    }
}

/// Inject faults on `topic`'s cluster for `length`, then heal everything.
///
/// Each fault holds for one to six seconds: some outlast the two-second
/// broker session and the replica lag and move leadership, some do not.
/// Then the network usually heals, and otherwise the next fault lands on top
/// of it, which overlaps partitions. The first fault always isolates the
/// leader for long enough to depose it, so that every seed has a leader
/// truncate a suffix that producers then retry elsewhere.
async fn inject(cluster: &Cluster, topic: &str, seed: u64, length: Duration, record: &Record) {
    let mut rng = SplitMix(seed);
    let start = Instant::now();
    let elapsed = || start.elapsed().as_millis();
    let mut first = true;
    while start.elapsed() < length {
        let (fault, least) = if std::mem::take(&mut first) {
            (Fault::IsolateLeader, 4_000)
        } else {
            (Fault::draw(&mut rng), 1_000)
        };
        let fault = fault.apply(cluster, topic);
        let hold = Duration::from_millis(least + rng.below(5_000));
        record.note(format!("{:>6} ms  {fault:?} for {hold:?}", elapsed()));
        tokio::time::sleep(hold.min(length.saturating_sub(start.elapsed()))).await;
        if rng.below(100) < 70 {
            cluster.links().heal_all();
            record.note(format!("{:>6} ms  heal", elapsed()));
        }
    }
    cluster.links().heal_all();
    record.note(format!("{:>6} ms  heal, for good", elapsed()));
}

// ---------------------------------------------------------------------------
// Producers
// ---------------------------------------------------------------------------

/// What every producer of one run shares.
struct Run<'a> {
    cluster: &'a Cluster,
    topic: &'a Topic,
    /// Every key a leader acknowledged, for the producers to repeat.
    acknowledged: Mutex<Vec<String>>,
    /// Set when the faults end: no producer starts another batch.
    winding_down: AtomicBool,
    /// When a producer gives up on the batch it still has.
    give_up_at: OnceLock<Instant>,
}

/// What one producer did.
#[derive(Default)]
struct Delivery {
    acked: Vec<Acked>,
    abandoned: Vec<Sent>,
    attempts: usize,
}

impl Run<'_> {
    /// Write batches until the faults end. Each batch is retried until a
    /// leader acknowledges it or the settling time runs out.
    async fn produce(&self, producer: u64, mut rng: SplitMix) -> Delivery {
        let mut delivery = Delivery::default();
        let mut fresh = 0_u64;
        while !self.winding_down.load(Ordering::Acquire) {
            let mut sent = Sent {
                keys: vec![],
                repeats: vec![],
            };
            for _ in 0..=rng.below(3) {
                let repeat = (rng.below(100) < REPEAT_PERCENT)
                    .then(|| {
                        let acknowledged = self.acknowledged.lock().expect("acknowledged keys");
                        let count = u64::try_from(acknowledged.len()).expect("a key count");
                        let pick = usize::try_from(rng.below(count.max(1))).expect("an index");
                        acknowledged.get(pick).cloned()
                    })
                    .flatten();
                sent.repeats.push(repeat.is_some());
                sent.keys.push(repeat.unwrap_or_else(|| {
                    fresh += 1;
                    format!("p{producer}-{fresh:05}")
                }));
            }
            match self.deliver(&sent, &mut delivery.attempts).await {
                Some(base_offset) => {
                    let new = sent.keys.iter().zip(&sent.repeats);
                    self.acknowledged.lock().expect("acknowledged keys").extend(
                        new.filter(|(_, repeat)| !**repeat)
                            .map(|(key, _)| key.clone()),
                    );
                    delivery.acked.push(Acked { sent, base_offset });
                }
                None => delivery.abandoned.push(sent),
            }
        }
        delivery
    }

    /// Send `sent`, the same batch every time, until it is acknowledged.
    async fn deliver(&self, sent: &Sent, attempts: &mut usize) -> Option<i64> {
        let records: Vec<Rec> = sent.keys.iter().map(|key| Rec::key(key)).collect();
        let batch = keyed_batch().records(&records).call();
        loop {
            if self
                .give_up_at
                .get()
                .is_some_and(|at| Instant::now() >= *at)
            {
                return None;
            }
            if let Some(leader) = newest_leader(self.cluster, &self.topic.name) {
                *attempts += 1;
                let outcome = send(
                    self.cluster,
                    leader,
                    self.topic,
                    batch.clone(),
                    AllReplicas,
                    PRODUCE_TIMEOUT_MS,
                )
                .await;
                if let Ok(row) = outcome
                    && row.error_code == 0
                {
                    return Some(row.base_offset);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

// ---------------------------------------------------------------------------
// One seed
// ---------------------------------------------------------------------------

/// A seed's fault schedule and counts, printed when the run fails before it
/// is marked passed, and with `--nocapture` when it passes.
struct Record {
    seed: u64,
    lines: Mutex<Vec<String>>,
    passed: AtomicBool,
}

impl Record {
    fn note(&self, line: String) { self.lines.lock().expect("record lines").push(line); }

    fn dump(&self) -> String { self.lines.lock().expect("record lines").join("\n") }
}

impl Drop for Record {
    fn drop(&mut self) {
        let (seed, lines) = (self.seed, self.dump());
        if self.passed.load(Ordering::Acquire) {
            eprintln!("dedup_partition flapping seed {seed} passed:\n{lines}");
        } else {
            eprintln!(
                "dedup_partition flapping FAILED for seed {seed}; replay it with \
                 DEDUP_PARTITION_SEED={seed}\n{lines}"
            );
        }
    }
}

async fn run_seed(seed: u64, secs: u64) {
    let record = Record {
        seed,
        lines: Mutex::new(vec![]),
        passed: AtomicBool::new(false),
    };
    eprintln!("dedup_partition flapping: seed {seed}, {secs}s of faults");
    let started = Instant::now();
    let cluster = Cluster::start().await;
    let topic = dedup_topic()
        .cluster(&cluster)
        .name(&format!("flapping-{seed}"))
        .replicas(&[1, 2, 3])
        .call()
        .await;
    let run = Run {
        cluster: &cluster,
        topic: &topic,
        acknowledged: Mutex::new(vec![]),
        winding_down: AtomicBool::new(false),
        give_up_at: OnceLock::new(),
    };
    let faults = async {
        inject(
            &cluster,
            &topic.name,
            seed,
            Duration::from_secs(secs),
            &record,
        )
        .await;
        run.winding_down.store(true, Ordering::Release);
        let _ = run.give_up_at.set(Instant::now() + SETTLE);
    };
    let producers = futures_util::future::join_all(
        (1..=PRODUCERS)
            .map(|producer| run.produce(producer, SplitMix(seed ^ producer.rotate_left(32)))),
    );
    let ((), deliveries) = tokio::join!(faults, producers);

    let log = offsets_and_keys(&converged(&cluster, &topic.name).await);
    let acked: Vec<Acked> = deliveries
        .iter()
        .flat_map(|delivery| delivery.acked.iter().cloned())
        .collect();
    let sent: BTreeSet<String> = deliveries
        .iter()
        .flat_map(|delivery| {
            let acked = delivery.acked.iter().map(|batch| &batch.sent);
            acked.chain(&delivery.abandoned)
        })
        .flat_map(|batch| batch.keys.iter().cloned())
        .collect();
    let repeats: usize = acked
        .iter()
        .map(|batch| batch.sent.repeats.iter().filter(|repeat| **repeat).count())
        .sum();
    let epoch = partition_on(&cluster, 1, &topic.name).map(|partition| partition.leader_epoch.0);
    record.note(format!(
        "{} batches acknowledged ({} records, {repeats} deliberate repeats) in {} attempts; {} \
         abandoned; the converged log holds {} records; the leader epoch reached {epoch:?}",
        acked.len(),
        acked
            .iter()
            .map(|batch| batch.sent.keys.len())
            .sum::<usize>(),
        deliveries
            .iter()
            .map(|delivery| delivery.attempts)
            .sum::<usize>(),
        deliveries
            .iter()
            .map(|delivery| delivery.abandoned.len())
            .sum::<usize>(),
        log.len(),
    ));
    let found = violations(&log, &acked, &sent);
    assert!(found.is_empty(), "seed {seed}: {found:#?}");
    assert!(
        !acked.is_empty(),
        "seed {seed}: no batch was ever acknowledged"
    );

    // Liveness: the healed cluster takes a new write on every replica.
    let probe = format!("probe-{seed}");
    let leader = newest_leader(&cluster, &topic.name).expect("a leader after the heal");
    let probed = produce(&cluster, leader, &topic, &[&probe], AllReplicas).await;
    let after = offsets_and_keys(&converged(&cluster, &topic.name).await);
    let expected: Vec<(i64, String)> = log.into_iter().chain([(probed, probe)]).collect();
    assert!(after == expected, "seed {seed}");
    assert_indexes_match(&cluster, &topic.name, &after).await;
    cluster.finish().await;
    record.note(format!("took {:?}", started.elapsed()));
    record.passed.store(true, Ordering::Release);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flapping_partitions_keep_every_acknowledged_key_once_on_every_replica() {
    let _guard = cluster_lock().lock().await;
    let (seeds, secs) = plan();
    for seed in seeds {
        run_seed(seed, secs).await;
    }
}
