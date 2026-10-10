//! A multi-hour soak: three brokers, continuous traffic, and a failure when a
//! resource level drifts rather than only when something crashes.
//!
//! # What this lane is for
//!
//! The longest a krabka cluster had ever run in this repository was the length
//! of one integration test -- the slowest wall-clock bound anywhere under
//! `crates/broker/tests` is four minutes -- and nothing in the workspace had
//! ever read a resource level and asserted a bound on it. So the failure a
//! Kafka operator actually hits, a broker that is fine for a day and then OOMs
//! or hits `EMFILE` on day three, was one this repository had no evidence
//! about, and one an adopter would discover in their production rather than in
//! ours.
//!
//! `docs/operations/capacity.md` tells that operator to size file descriptors
//! as "one per client connection plus the segment files of every replica" and
//! to watch `krabka_broker_active_connections`, which counts only the first
//! half of that sum. This suite is what makes the second half checkable:
//! segment-file handles, fetch-session cache entries, per-partition metric
//! series, producer-id state and the log directory itself all have to come back
//! down over hours of steady traffic, and here they are sampled and judged.
//!
//! # How it runs
//!
//! Three brokers, each a real container process out of `//packaging`'s image
//! and formatted into one three-voter quorum -- see [`cluster`] for the
//! addressing that lets containers and the host reach the same broker. Three
//! topics carry the load, and their configurations are the reason ten cycles of
//! each kind are reachable inside the run rather than a hope:
//!
//! | topic | policy | segment | retention | what it exercises |
//! | --- | --- | --- | --- | --- |
//! | `soak-retention` | `delete` | `internal.segment.bytes=64 KiB`, `segment.ms=5s` | `retention.ms=60s`, `retention.bytes=4 MiB` | segment roll, then deletion of the sealed segments |
//! | `soak-compacted` | `compact` | `internal.segment.bytes=64 KiB`, `segment.ms=5s` | `min.cleanable.dirty.ratio=0.01`, `min.compaction.lag.ms=0` | the cleaner, sweeping every second |
//! | `soak-verify` | `delete` | default | unlimited | the acked records read back at the end |
//!
//! A 64 KiB segment is what makes the roll count reachable: at this suite's
//! produce rate each partition of `soak-retention` fills one every half-minute
//! or so, and the roll is driven by `internal.segment.bytes` on the append path.
//! `--cleaner-interval` and `--log-retention-check-interval` are both set to
//! one second (the broker defaults are thirty seconds and five minutes), so a
//! three-minute local run gets around 180 opportunities of each kind rather
//! than six and zero. Those two flags are what make ten cycles of each kind
//! reachable inside the run; see [`cluster`], which sets them. The cycle counts
//! below are asserted from counters and from the segment files themselves, not
//! inferred from the clock: a soak that silently never rolled a segment, never
//! swept or never trimmed fails.
//!
//! `segment.ms` and `retention.ms`/`retention.bytes` are applied by the
//! broker-wide local-retention sweep in `krabka_broker::log_retention`, which
//! dispatches `krabka_log::Log::tick` through each partition's writer actor on
//! `log.retention.check.interval.ms`. Until that sweep existed nothing in
//! `crates/broker` called `Log::tick`, so a sealed segment was never deleted
//! and the descriptor count climbed for as long as the producer ran -- which is
//! the failure this lane found on its first run and the reason it exists.
//!
//! `soak-compacted` has no `retention.ms`, exactly as a compacted Kafka topic
//! has none: the cleaner is what bounds it. That makes it the control against
//! `soak-retention`, and it is what caught the cleaner sweeping only the
//! partitions this broker led: a follower replica of a compacted topic had
//! nothing bounding its segment count at all, and its descriptors climbed for
//! the length of the run while the leader's stayed flat.
//!
//! # What fails it
//!
//! Four series per broker -- resident set, open descriptors, `/metrics`
//! cardinality and log-directory bytes -- are sampled on a fixed interval and
//! handed to [`drift::judge`], which fails a series that exceeds its ceiling
//! or that trends upward across the second half of the run. That judgement is
//! pure logic over a list of numbers and is unit-tested in [`drift`] against
//! series whose shape is known, including the segment-roll sawtooth that a
//! first-to-last comparison would misread as a leak.
//!
//! Then: every cycle count must have been reached, the cleaner must have failed
//! zero sweeps, and every record the client saw acked on `soak-verify` must
//! still be readable.
//!
//! # Running it
//!
//! Scheduled-only. The `#[ignore]` keeps it out of `cargo test`, and
//! `//crates/broker:soak_docker_test` is tagged `docker` and excluded from the
//! container-suite selection, so no pull request runs it. The `soak` job in
//! `.github/workflows/ci.yml` is what does, nightly, with
//! `KRABKA_SOAK_SECONDS=14400`.
//!
//! A developer exercises the whole thing in a few minutes:
//!
//! ```text
//! bazel run -c opt //packaging:image_load
//! cargo test -p krabka-broker --test soak -- --ignored --nocapture
//! ```
//!
//! which uses the default [`DEFAULT_SOAK_SECONDS`]-second run. Set
//! `KRABKA_SOAK_SECONDS` to lengthen it. The analysis and the parsing are
//! hermetic and need none of that:
//!
//! ```text
//! cargo test -p krabka-broker --test soak
//! ```

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use assert2::{assert, check};
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::Client;
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tokio::sync::Mutex;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `soak/` directory, which keeps the parts out of `tests/` where every `.rs`
// file would become another test binary.
mod support;

#[path = "soak/cluster.rs"]
mod cluster;
#[path = "soak/drift.rs"]
mod drift;
#[path = "soak/probe.rs"]
mod probe;

use crate::{
    cluster::SoakCluster,
    drift::{Bound, Series},
};

/// How long the load runs when `KRABKA_SOAK_SECONDS` is unset.
///
/// Three minutes: long enough for the sampler to take the sixteen samples
/// [`drift`] needs before it will judge a trend, and for every cycle count
/// below to be reached, so a developer exercises the same assertions the
/// nightly does without waiting four hours for them.
const DEFAULT_SOAK_SECONDS: u64 = 180;

/// Samples taken over the run, whatever its length.
///
/// The interval is derived from this rather than fixed, so a four-hour run and
/// a three-minute one are judged on series of the same shape -- and so the
/// second half always holds enough points for a slope.
const SAMPLES: u32 = 60;

/// The shortest interval the sampler will use.
///
/// Each sample scrapes three `/metrics` bodies and walks three log directories;
/// below this the sampling would start to be part of what it measures.
const MIN_SAMPLE_INTERVAL: Duration = Duration::from_secs(3);

/// Cycles of each kind the run must reach.
///
/// Ten, because one is an accident and ten is a rhythm: a leak that only shows
/// after a handful of segment deletions has ten chances to appear.
const MIN_CYCLES: u64 = 10;

const RETENTION_TOPIC: &str = "soak-retention";
const COMPACTED_TOPIC: &str = "soak-compacted";
const VERIFY_TOPIC: &str = "soak-verify";

/// Payload size for the retention topic's records.
const PAYLOAD_BYTES: usize = 256;

/// Distinct keys the compacted topic cycles through. Small, so the cleaner has
/// something to collapse on every sweep.
const COMPACTION_KEYS: u64 = 64;

/// Pause between records on the retention topic, per producer task.
const RETENTION_PACE: Duration = Duration::from_millis(20);

/// Pause between records on the compacted topic.
const COMPACTION_PACE: Duration = Duration::from_millis(50);

/// Pause between records on the verify topic.
///
/// One per second: the acked set is held in memory and read back in full at the
/// end, so a four-hour run has to leave a set that can be read back inside a
/// bounded poll.
const VERIFY_PACE: Duration = Duration::from_secs(1);

/// Resident set a broker under this load may not exceed.
const RSS_CEILING: f64 = 1.5 * 1024.0 * 1024.0 * 1024.0;

/// Open descriptors a broker under this load may not exceed.
///
/// The default `ulimit -n` on a container is 1024 and the image raises nothing,
/// so a broker approaching this is already close to `EMFILE`: the ceiling is
/// four times the soft limit precisely so that crossing it is a finding rather
/// than a crash nobody can attribute.
const FD_CEILING: f64 = 4_096.0;

/// `/metrics` cardinality a broker may not exceed.
///
/// The checked-in body under `docs/operations/` is a few hundred series; ten
/// partitions' worth of per-partition families is a few thousand. Twenty
/// thousand is the point at which a family has stopped dropping members.
const SERIES_CEILING: f64 = 20_000.0;

/// Bytes one broker's log directory may not exceed.
///
/// `soak-retention` is capped at 4 MiB per partition by `retention.bytes` over
/// six partitions, all three replicas of which land on every broker, and the
/// compacted and verify topics are far smaller. 512 MiB leaves an order of
/// magnitude of head-room over that and still catches retention that has
/// stopped working.
const LOG_DIR_CEILING: f64 = 512.0 * 1024.0 * 1024.0;

/// The metric the cleaner's sweeps are counted from.
const CLEANER_RUNS: &str = "krabka_broker_log_cleaner_runs_total";

/// The metric a failed sweep is counted from. `log_cleaner_runs_total` cannot
/// tell a sweep that compacted from one whose every partition errored.
const CLEANER_FAILURES: &str = "krabka_broker_log_cleaner_failures_total";

/// The per-partition counter a completed compaction increments.
const COMPACTIONS: &str = "krabka_broker_log_compactions_total";

/// How long the load runs, from the environment.
fn soak_duration() -> Duration {
    let seconds = std::env::var("KRABKA_SOAK_SECONDS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_SOAK_SECONDS);
    // The default is the floor, not merely the default. A shorter run reaches
    // fewer than the ten cycles below -- a two-minute run measured nine
    // compactions where a three-minute one measured comfortably more -- and
    // would fail on the clock rather than on the broker. Refusing it says so,
    // where letting it through would look like a finding.
    assert!(
        seconds >= DEFAULT_SOAK_SECONDS,
        "KRABKA_SOAK_SECONDS={seconds} is below the {DEFAULT_SOAK_SECONDS}s floor: \
         too short to reach {MIN_CYCLES} cycles of each kind, so a failure would \
         be the run's length rather than the broker's behaviour"
    );
    Duration::from_secs(seconds)
}

/// One record of `PAYLOAD_BYTES` bytes on `topic`.
fn record(topic: &str, key: String, value: Vec<u8>) -> ProducerRecord {
    crate::support::producer::producer_record(crate::support::producer::ProducerRecordSetup {
        topic: topic.to_owned(),
        key: Some(key.into()),
        value: Some(value.into()),
        ..Default::default()
    })
}

/// A producer for the load tasks.
async fn producer(bootstrap: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("build a producer")
}

/// Create the three topics the load runs over.
async fn create_topics(bootstrap: &str) {
    let topics = vec![
        topic(
            RETENTION_TOPIC,
            6,
            &[
                ("cleanup.policy", "delete"),
                ("internal.segment.bytes", "65536"),
                ("segment.ms", "5000"),
                ("retention.ms", "60000"),
                ("retention.bytes", "4194304"),
            ],
        ),
        topic(
            COMPACTED_TOPIC,
            3,
            &[
                ("cleanup.policy", "compact"),
                ("internal.segment.bytes", "65536"),
                ("segment.ms", "5000"),
                ("min.cleanable.dirty.ratio", "0.01"),
                ("min.compaction.lag.ms", "0"),
            ],
        ),
        topic(
            VERIFY_TOPIC,
            1,
            &[
                ("cleanup.policy", "delete"),
                ("retention.ms", "-1"),
                ("retention.bytes", "-1"),
            ],
        ),
    ];

    // A cluster that has just booted is still electing; the first attempts are
    // expected to fail on the connection or on a missing controller.
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last = String::new();
    while Instant::now() < deadline {
        if let Ok(admin) = Client::builder().bootstrap(bootstrap).build().await {
            match admin
                .send(CreateTopicsRequest {
                    topics: topics.clone(),
                    timeout_ms: 30_000,
                    ..Default::default()
                })
                .await
            {
                Ok(response) if response.topics.iter().all(|t| t.error_code == 0) => return,
                Ok(response) => last = format!("{:?}", response.topics),
                Err(error) => last = format!("{error}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("could not create the soak topics on {bootstrap}: {last}");
}

fn topic(name: &str, partitions: i32, configs: &[(&str, &str)]) -> CreatableTopic {
    CreatableTopic {
        name: name.into(),
        num_partitions: partitions,
        // Three replicas over three brokers: every broker holds every
        // partition, so each one's descriptors and log directory carry the
        // whole load rather than a third of it.
        replication_factor: 3,
        configs: crate::support::topics::topic_configs(configs.iter().copied()),
        ..Default::default()
    }
}

/// The state the load tasks share with the sampler.
struct Load {
    stop: Arc<AtomicBool>,
    produced: Arc<AtomicU64>,
    consumed: Arc<AtomicU64>,
    /// Keys the broker acked on [`VERIFY_TOPIC`].
    verified: Arc<Mutex<BTreeSet<String>>>,
}

impl Load {
    fn new() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            produced: Arc::new(AtomicU64::new(0)),
            consumed: Arc::new(AtomicU64::new(0)),
            verified: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn running(&self) -> bool {
        !self.stop.load(Ordering::Relaxed)
    }
}

/// Produce on `topic` at `pace` until the load stops.
///
/// Keys come from `key_space`: `None` produces a distinct key per record --
/// what the retention topic wants -- and `Some(n)` cycles through `n` keys,
/// which is what gives the cleaner something to collapse.
async fn produce_loop(
    load: Arc<Load>,
    bootstrap: String,
    topic: &'static str,
    pace: Duration,
    key_space: Option<u64>,
    record_ack: bool,
) {
    let producer = producer(&bootstrap).await;
    let mut sequence = 0u64;
    while load.running() {
        let key = match key_space {
            Some(space) => format!("{topic}-k{}", sequence % space),
            None => format!("{topic}-{sequence}"),
        };
        let value = vec![b'x'; PAYLOAD_BYTES];
        if producer
            .send(record(topic, key.clone(), value))
            .await
            .is_ok()
        {
            load.produced.fetch_add(1, Ordering::Relaxed);
            if record_ack {
                load.verified.lock().await.insert(key);
            }
        }
        sequence += 1;
        tokio::time::sleep(pace).await;
    }
}

/// Consume everything the producers write, continuously, until the load stops.
///
/// The consumer is here to keep fetch sessions, the group coordinator and the
/// read path busy for the whole run: those are where a leak the produce path
/// alone would never reach lives.
async fn consume_loop(load: Arc<Load>, bootstrap: String) {
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("soak-readers")
        .subscribe(vec![
            RETENTION_TOPIC.to_owned(),
            COMPACTED_TOPIC.to_owned(),
            VERIFY_TOPIC.to_owned(),
        ])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("build the soak consumer");
    while load.running() {
        match consumer.poll(krabka_units::millis(500)).await {
            Ok(records) => {
                let count = u64::try_from(records.len()).unwrap_or(0);
                load.consumed.fetch_add(count, Ordering::Relaxed);
            }
            // A rebalance or a leader move surfaces here; the soak is about
            // what the cluster does over hours, not about one poll.
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let _ = consumer.close().await;
}

/// The four series one broker contributes, plus its counters.
struct BrokerSamples {
    /// The container this broker runs as, for a failure message.
    name: String,
    rss: Series,
    fds: Series,
    series_count: Series,
    log_dir: Series,
    /// Every segment base offset seen in any partition directory, ever.
    segments_seen: std::collections::BTreeMap<String, BTreeSet<u64>>,
    /// The lowest base offset still present per partition, at the last sample.
    lowest_kept: std::collections::BTreeMap<String, u64>,
    /// How many times a partition's lowest retained segment moved up.
    retention_cycles: u64,
    cleaner_runs: f64,
    cleaner_failures: f64,
    compactions: f64,
}

impl BrokerSamples {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            rss: Series::new(format!("{name} resident set"), "bytes"),
            fds: Series::new(format!("{name} open descriptors"), "descriptors"),
            series_count: Series::new(format!("{name} /metrics series"), "series"),
            log_dir: Series::new(format!("{name} log directory"), "bytes"),
            segments_seen: std::collections::BTreeMap::new(),
            lowest_kept: std::collections::BTreeMap::new(),
            retention_cycles: 0,
            cleaner_runs: 0.0,
            cleaner_failures: 0.0,
            compactions: 0.0,
        }
    }

    /// Segments created since the first sample.
    fn segment_rolls(&self) -> u64 {
        // The first segment of each partition is not a roll: it is what the
        // partition was created with.
        self.segments_seen
            .values()
            .map(|offsets| u64::try_from(offsets.len()).unwrap_or(0).saturating_sub(1))
            .sum()
    }

    /// Fold one directory listing into the segment and retention tallies.
    fn observe_segments(&mut self, listing: &std::collections::BTreeMap<String, BTreeSet<u64>>) {
        for (partition, offsets) in listing {
            self.segments_seen
                .entry(partition.clone())
                .or_default()
                .extend(offsets.iter().copied());
            let Some(lowest) = offsets.iter().next().copied() else {
                continue;
            };
            match self.lowest_kept.get(partition) {
                // A partition whose lowest surviving segment moved up had a
                // sealed segment deleted: that is a retention cycle, observed
                // rather than assumed from the clock.
                Some(previous) if lowest > *previous => {
                    self.retention_cycles += 1;
                    self.lowest_kept.insert(partition.clone(), lowest);
                }
                Some(_) => {}
                None => {
                    self.lowest_kept.insert(partition.clone(), lowest);
                }
            }
        }
    }
}

/// Every acked verify-topic key, read back from the earliest offset.
async fn read_back(bootstrap: &str, want: usize) -> BTreeSet<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("soak-verify-readback")
        .subscribe(vec![VERIFY_TOPIC.to_owned()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("build the read-back consumer");
    let mut keys = BTreeSet::new();
    let deadline = Instant::now() + Duration::from_secs(180);
    while keys.len() < want && Instant::now() < deadline {
        let Ok(records) = consumer.poll(krabka_units::millis(1_000)).await else {
            continue;
        };
        for record in records {
            if let Some(key) = record.key {
                keys.insert(String::from_utf8_lossy(&key).into_owned());
            }
        }
    }
    let _ = consumer.close().await;
    keys
}

/// The bounds each series is judged against, in the order the samples are
/// collected.
fn bounds() -> [Bound; 4] {
    [
        // A resident set that climbs 15% over the second half of a four-hour
        // run is on course to double inside a day.
        Bound {
            ceiling: RSS_CEILING,
            drift_fraction: 0.15,
            floor: 64.0 * 1024.0 * 1024.0,
        },
        // Descriptors should be flat once the connection count is: a segment
        // handle per replica plus a connection per client, both bounded.
        Bound {
            ceiling: FD_CEILING,
            drift_fraction: 0.15,
            floor: 128.0,
        },
        // Cardinality is the tightest, because the partition and client sets
        // are fixed for the whole run: a family that keeps growing here is one
        // that never drops a label set.
        Bound {
            ceiling: SERIES_CEILING,
            drift_fraction: 0.10,
            floor: 200.0,
        },
        // The loosest, because this is the series that is a sawtooth by
        // construction -- it grows to `retention.bytes` and is cut back, over
        // and over.
        Bound {
            ceiling: LOG_DIR_CEILING,
            drift_fraction: 0.25,
            floor: 16.0 * 1024.0 * 1024.0,
        },
    ]
}

/// Four hours of continuous produce and consume across three brokers, and a
/// failure when any resource level drifts.
///
/// See the module documentation for the topics, the cycle counts and how a
/// developer runs a short version.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires Docker and //packaging:image; runs in the scheduled soak lane"]
async fn a_three_broker_cluster_holds_its_resources_over_a_soak() {
    let duration = soak_duration();
    let interval = (duration / SAMPLES).max(MIN_SAMPLE_INTERVAL);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build an HTTP client");

    let cluster = SoakCluster::start();
    cluster.wait_metrics_up(&http).await;
    let bootstrap = cluster.bootstrap();
    create_topics(&bootstrap).await;

    let load = Arc::new(Load::new());
    let mut tasks = vec![
        tokio::spawn(produce_loop(
            Arc::clone(&load),
            bootstrap.clone(),
            RETENTION_TOPIC,
            RETENTION_PACE,
            None,
            false,
        )),
        tokio::spawn(produce_loop(
            Arc::clone(&load),
            bootstrap.clone(),
            COMPACTED_TOPIC,
            COMPACTION_PACE,
            Some(COMPACTION_KEYS),
            false,
        )),
        tokio::spawn(produce_loop(
            Arc::clone(&load),
            bootstrap.clone(),
            VERIFY_TOPIC,
            VERIFY_PACE,
            None,
            true,
        )),
    ];
    tasks.push(tokio::spawn(consume_loop(
        Arc::clone(&load),
        bootstrap.clone(),
    )));

    let mut samples: Vec<BrokerSamples> = cluster
        .brokers
        .iter()
        .map(|broker| BrokerSamples::new(&broker.name))
        .collect();
    let mut baseline: Option<Vec<(f64, f64, f64)>> = None;

    let started = Instant::now();
    let mut taken = 0u32;
    while started.elapsed() < duration {
        tokio::time::sleep(interval).await;
        let elapsed = started.elapsed();
        let mut counters = Vec::new();
        for (broker, sample) in cluster.brokers.iter().zip(&mut samples) {
            let pid = broker.host_pid();
            let rss = probe::resident_bytes(pid)
                .unwrap_or_else(|| panic!("{} vanished mid-soak", broker.name));
            let fds = probe::open_descriptors(pid)
                .unwrap_or_else(|| panic!("{} vanished mid-soak", broker.name));
            let body = http
                .get(broker.metrics_url())
                .send()
                .await
                .expect("scrape /metrics")
                .text()
                .await
                .expect("read the /metrics body");
            let log_dir = broker.host_log_dir();

            sample.rss.push(elapsed, probe::sampled(rss));
            sample.fds.push(elapsed, probe::sampled_count(fds));
            sample
                .series_count
                .push(elapsed, probe::sampled_count(probe::count_series(&body)));
            sample
                .log_dir
                .push(elapsed, probe::sampled(probe::directory_bytes(&log_dir)));
            sample.observe_segments(&probe::segments(&log_dir));
            sample.cleaner_runs = probe::sum_metric(&body, CLEANER_RUNS);
            sample.cleaner_failures = probe::sum_metric(&body, CLEANER_FAILURES);
            sample.compactions = probe::sum_metric(&body, COMPACTIONS);
            counters.push((
                sample.cleaner_runs,
                sample.cleaner_failures,
                sample.compactions,
            ));
        }
        if baseline.is_none() {
            baseline = Some(counters);
        }
        taken += 1;
    }

    load.stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = task.await;
    }

    let produced = load.produced.load(Ordering::Relaxed);
    let consumed = load.consumed.load(Ordering::Relaxed);
    eprintln!(
        "soak: {duration:?} of load, {taken} samples at {interval:?}, \
         {produced} records acked, {consumed} delivered"
    );

    let verified = load.verified.lock().await.clone();
    let readable = read_back(&bootstrap, verified.len()).await;
    let lost: Vec<&String> = verified.difference(&readable).collect();
    assert!(
        lost.is_empty(),
        "{} of {} acked records on {VERIFY_TOPIC} are gone after the soak: {:?}",
        lost.len(),
        verified.len(),
        &lost[..lost.len().min(10)]
    );
    assert!(
        !verified.is_empty(),
        "the soak acked no records at all on {VERIFY_TOPIC}"
    );

    // The cycles happened. Read off counters rather than inferred from the
    // wall clock: a soak that silently never rolled a segment or never swept
    // must fail rather than report four quiet hours.
    let baseline = baseline.expect("the sampler took at least one sample");
    let mut sweeps = 0.0;
    let mut compactions = 0.0;
    for ((first_runs, first_failures, first_compactions), sample) in baseline.iter().zip(&samples) {
        sweeps += sample.cleaner_runs - first_runs;
        compactions += sample.compactions - first_compactions;
        // A counter, so any rise at all is a failed sweep: `< 1.0` is "the
        // counter did not move", written without a float equality.
        let failures = sample.cleaner_failures - first_failures;
        check!(
            failures < 1.0,
            "{}: the cleaner failed {failures} sweeps",
            sample.name
        );
    }
    let rolls: u64 = samples.iter().map(BrokerSamples::segment_rolls).sum();
    let retention: u64 = samples.iter().map(|s| s.retention_cycles).sum();

    let want = probe::sampled(MIN_CYCLES);
    check!(
        sweeps >= want,
        "the cleaner swept {sweeps} times across the cluster, fewer than {MIN_CYCLES}"
    );
    check!(
        compactions >= want,
        "{compactions} compactions completed, fewer than {MIN_CYCLES}"
    );
    check!(
        rolls >= MIN_CYCLES,
        "{rolls} segments rolled, fewer than {MIN_CYCLES}"
    );
    check!(
        retention >= MIN_CYCLES,
        "retention deleted a partition's oldest segment {retention} times, \
         fewer than {MIN_CYCLES}"
    );

    // And nothing drifted.
    let [rss, fds, series, log_dir] = bounds();
    let mut judged = Vec::new();
    for sample in samples {
        judged.push((sample.rss, rss));
        judged.push((sample.fds, fds));
        judged.push((sample.series_count, series));
        judged.push((sample.log_dir, log_dir));
    }
    let verdicts = drift::judge_all(&judged);
    for verdict in &verdicts {
        eprintln!("soak: {verdict}");
    }
    let failed: Vec<String> = verdicts
        .iter()
        .filter(|verdict| verdict.is_failure())
        .map(ToString::to_string)
        .collect();
    assert!(
        failed.is_empty(),
        "{} resource series did not hold:\n{}",
        failed.len(),
        failed.join("\n")
    );
}
