use {
    super::*,
    crate::{
        dedup::{DedupRuntime, DedupSettings, IdentityCollector},
        metrics::BrokerMetrics,
        time_util::now_ms,
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_dedup::IdentityDigest,
    krabka_log::LogConfig,
    krabka_protocol::records::{Record, RecordHeader},
    krabka_units::mebibytes,
    std::{
        sync::atomic::{AtomicBool, Ordering},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

const WINDOW_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;

fn runtime_with(max_entries_per_partition: usize, max_entries: usize) -> Arc<DedupRuntime> {
    Arc::new(DedupRuntime::new(
        max_entries_per_partition,
        max_entries,
        RecordDecompressionPolicy::default(),
        BrokerMetrics::new(),
    ))
}

fn runtime() -> Arc<DedupRuntime> { runtime_with(1_000, 1_000_000) }

fn settings(runtime: &Arc<DedupRuntime>, window_ms: i64, key_header: Option<&str>) -> DedupConfig {
    DedupConfig {
        settings: DedupSettings {
            window_ms,
            key_header: key_header.map(Arc::from),
        },
        runtime: Arc::clone(runtime),
    }
}

fn config(runtime: &Arc<DedupRuntime>, key_header: Option<&str>) -> DedupConfig {
    settings(runtime, WINDOW_MS, key_header)
}

fn windowed(runtime: &Arc<DedupRuntime>, window_ms: i64) -> DedupConfig {
    settings(runtime, window_ms, None)
}

// `records` in order as one batch, all created `age_ms` before now.
fn batch_of(records: impl Iterator<Item = Record>, age_ms: i64) -> RecordBatch {
    let base_timestamp = now_ms() - age_ms;
    let records: Vec<Record> = records
        .zip(0..)
        .map(|(record, offset_delta)| Record {
            offset_delta,
            ..record
        })
        .collect();
    RecordBatch {
        last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
        base_timestamp,
        max_timestamp: base_timestamp,
        records,
        ..RecordBatch::default()
    }
}

// One record for each key, `age_ms` before now, and an `h` header that holds
// the key in upper case.
#[bon::builder]
fn batch(keys: &[&str], #[builder(default = 1_000)] age_ms: i64) -> RecordBatch {
    let records = keys.iter().map(|key| Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value: Some(Bytes::from_static(b"value")),
        headers: vec![RecordHeader {
            key: "h".to_owned(),
            value: Some(Bytes::from(key.to_uppercase())),
        }],
        ..Record::default()
    });
    batch_of(records, age_ms)
}

// `count` records keyed `{prefix}-{first}` upwards, each with a value of
// `value_bytes` bytes, all created `age_ms` before now.
#[bon::builder]
fn numbered_batch(
    #[builder(default = "key")] prefix: &str,
    first: u64,
    count: u64,
    #[builder(default = 1_000)] age_ms: i64,
    #[builder(default = 8)] value_bytes: usize,
) -> RecordBatch {
    let value = Bytes::from(vec![b'v'; value_bytes]);
    let records = (first..first + count).map(|key| Record {
        key: Some(Bytes::from(format!("{prefix}-{key}"))),
        value: Some(value.clone()),
        ..Record::default()
    });
    batch_of(records, age_ms)
}

fn job(config: &DedupConfig, batch: &RecordBatch) -> DedupJob {
    let mut collector = IdentityCollector::new(
        config.settings.key_header.clone(),
        batch.base_timestamp,
        batch.records.len(),
    );
    for record in &batch.records {
        collector.visit_owned(record);
    }
    DedupJob {
        config: config.clone(),
        records: collector.finish().records,
    }
}

// Every record of a log as `(offset, key)`, and every batch's offset span.
struct Contents {
    records: Vec<(i64, String)>,
    spans: Vec<(i64, i64)>,
}

struct Replica {
    _dir: TempDir,
    log: Mutex<Log>,
    dedup: PartitionDedup,
}

impl Replica {
    fn new(config: Option<DedupConfig>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let log = Mutex::new(Log::open(dir.path(), LogConfig::default()).unwrap());
        let mut dedup = PartitionDedup::new(Arc::from("topic"), 0);
        dedup.configure(config);
        Self {
            _dir: dir,
            log,
            dedup,
        }
    }

    fn log_end(&self) -> Offset { self.log.lock().unwrap().log_end_offset() }

    fn active(&self) -> &ActiveIndex { self.dedup.active.as_ref().unwrap() }

    fn is_current(&self) -> bool { self.active().is_current(self.log_end()) }

    // The writer's leader path for one batch: catch up, classify, append,
    // finish. Returns the base offset the log assigned.
    fn produce(&mut self, batch: RecordBatch, job: Option<DedupJob>) -> Offset {
        self.produce_with(batch, job, true)
    }

    // `produce` with the job that the produce handler reads under `config`.
    fn produce_deduplicated(&mut self, config: &DedupConfig, batch: RecordBatch) -> Offset {
        let job = job(config, &batch);
        self.produce(batch, Some(job))
    }

    // `produce_deduplicated` of one record for each key.
    fn produce_keys(&mut self, config: &DedupConfig, keys: &[&str]) -> Offset {
        self.produce_deduplicated(config, batch().keys(keys).call())
    }

    fn produce_with(&mut self, batch: RecordBatch, job: Option<DedupJob>, succeed: bool) -> Offset {
        self.dedup.adopt(job.as_ref());
        self.dedup.make_current(&self.log, now_ms());
        self.append_classified(batch, job, succeed)
    }

    fn classify(&mut self, batch: RecordBatch, job: Option<DedupJob>) -> ClassifiedAppend {
        let log_end = self.log_end();
        self.dedup
            .classify(ProduceData::Owned(batch), job, log_end, None, now_ms())
    }

    // Classify, append when `succeed`, and finish, without adopting the job's
    // config first. Returns the base offset, or -1 for a failed append.
    fn append_classified(
        &mut self,
        batch: RecordBatch,
        job: Option<DedupJob>,
        succeed: bool,
    ) -> Offset {
        let ClassifiedAppend { data, pending } = self.classify(batch, job);
        let ProduceData::Owned(mut batch) = data else {
            panic!("an owned batch stays owned");
        };
        let base = if succeed {
            self.log.lock().unwrap().append(&mut batch).unwrap().0
        } else {
            Offset(-1)
        };
        let after = self.log_end();
        self.dedup.finish(pending, succeed, after, None, now_ms());
        base
    }

    // The follower path for one batch its leader stored, which the index
    // reads as `observed`.
    fn replicate(&mut self, mut batch: RecordBatch, observed: &ObservedBatch) {
        let before = self.log_end();
        let base = Offset(batch.base_offset);
        self.log
            .lock()
            .unwrap()
            .append_at(&mut batch, base)
            .unwrap();
        let after = self.log_end();
        self.dedup.follow(observed, before, after, now_ms());
    }

    // Every stored batch, read from offset 0 the way a follower fetches.
    fn stored_batches(&self) -> Vec<RecordBatch> {
        let log = self.log.lock().unwrap();
        let end = log.log_end_offset();
        let mut batches = Vec::new();
        let mut next = Offset(0);
        while next < end {
            let read = log.read(next, mebibytes(1)).unwrap();
            for batch in read.batches {
                let last = batch.base_offset + i64::from(batch.last_offset_delta);
                if last >= next.0 {
                    next = Offset(last + 1);
                    batches.push(batch);
                }
            }
        }
        batches
    }

    fn contents(&self) -> Contents {
        let batches = self.stored_batches();
        let records = batches
            .iter()
            .flat_map(|batch| {
                batch.records.iter().map(|record| {
                    (
                        batch.base_offset + i64::from(record.offset_delta),
                        String::from_utf8(record.key.clone().unwrap().to_vec()).unwrap(),
                    )
                })
            })
            .collect();
        let spans = batches
            .iter()
            .map(|batch| {
                (
                    batch.base_offset,
                    batch.base_offset + i64::from(batch.last_offset_delta),
                )
            })
            .collect();
        Contents { records, spans }
    }

    fn filter(&self) -> Option<&DedupWindowFilter> {
        self.dedup.active.as_ref().map(|active| &active.filter)
    }

    fn entries(&self) -> Vec<(IdentityDigest, i64)> {
        self.filter()
            .map(|filter| filter.iter().collect())
            .unwrap_or_default()
    }

    fn index_len(&self) -> usize { self.filter().map_or(0, DedupWindowFilter::len) }

    fn counters(&self) -> DedupCounters {
        self.filter()
            .map(DedupWindowFilter::counters)
            .unwrap_or_default()
    }

    fn sweep_visits(&self) -> u64 { self.filter().map_or(0, DedupWindowFilter::sweep_visits) }
}

// A replica that deduplicates by record key and holds one record for each
// of `keys`, with the config it holds them under.
fn replica_holding(keys: &[&str]) -> (DedupConfig, Replica) {
    let config = config(&runtime(), None);
    let mut replica = Replica::new(Some(config.clone()));
    replica.produce_keys(&config, keys);
    (config, replica)
}

fn keys(records: &[(i64, String)]) -> Vec<(i64, &str)> {
    records
        .iter()
        .map(|(offset, key)| (*offset, key.as_str()))
        .collect()
}

// Append `total` records keyed `{prefix}-0` upwards in batches of `per_batch`,
// through the writer's leader path.
#[bon::builder]
fn fill(
    replica: &mut Replica,
    #[builder(default = "key")] prefix: &str,
    total: u64,
    per_batch: u64,
    #[builder(default = 1_000)] age_ms: i64,
    #[builder(default = 8)] value_bytes: usize,
) {
    for first in (0..total).step_by(usize::try_from(per_batch).unwrap()) {
        let produced = numbered_batch()
            .prefix(prefix)
            .first(first)
            .count(per_batch.min(total - first))
            .age_ms(age_ms)
            .value_bytes(value_bytes)
            .call();
        replica.produce(produced, None);
    }
}

#[test]
fn duplicates_leave_offset_gaps_and_an_all_duplicate_batch_appends_empty() {
    let config = config(&runtime(), None);
    let mut replica = Replica::new(Some(config.clone()));

    let bases = [&["a", "b"][..], &["a", "c", "b", "d"], &["c", "d"]]
        .map(|keys| replica.produce_keys(&config, keys));

    let Contents { records, spans } = replica.contents();
    assert!(bases == [Offset(0), Offset(2), Offset(6)]);
    assert!(keys(&records) == [(0, "a"), (1, "b"), (3, "c"), (5, "d")]);
    assert!(spans == [(0, 1), (2, 5), (6, 7)]);
    assert!(replica.log_end() == Offset(8));
}

#[test]
fn a_failed_append_gives_its_identities_back() {
    let config = config(&runtime(), None);
    let mut replica = Replica::new(Some(config.clone()));
    let lost = batch().keys(&["x"]).call();

    replica.produce_with(lost.clone(), Some(job(&config, &lost)), false);
    let retried = replica.produce_keys(&config, &["x"]);

    assert!(retried == Offset(0));
    assert!(keys(&replica.contents().records) == [(0, "x")]);
}

#[test]
fn a_new_index_is_rebuilt_from_the_window_of_the_log() {
    let config = config(&runtime(), None);
    // The log holds records written before the index existed: one older than
    // the window, and one inside it.
    let mut replica = Replica::new(None);
    replica.produce(batch().keys(&["old"]).age_ms(2 * WINDOW_MS).call(), None);
    replica.produce(batch().keys(&["recent"]).call(), None);
    replica.dedup.configure(Some(config.clone()));

    replica.produce_keys(&config, &["old", "recent"]);

    assert!(keys(&replica.contents().records) == [(0, "old"), (1, "recent"), (2, "old")]);
}

#[test]
fn a_truncation_makes_the_index_forget_the_records_it_removed() {
    let (config, mut replica) = replica_holding(&["a"]);

    replica.log.lock().unwrap().truncate_to(Offset(0)).unwrap();
    replica.dedup.invalidate();
    let again = replica.produce_keys(&config, &["a"]);

    assert!(again == Offset(0));
    assert!(keys(&replica.contents().records) == [(0, "a")]);
}

#[test]
fn a_stale_index_classifies_nothing_until_it_is_rebuilt() {
    let (config, mut replica) = replica_holding(&["a"]);
    replica.dedup.invalidate();

    let first = batch().keys(&["a"]).call();
    let job = job(&config, &first);
    let ClassifiedAppend { pending, .. } = replica.classify(first, Some(job));

    assert!(matches!(pending, PendingAppend::Ignored));
}

#[test]
fn a_new_identity_rule_rebuilds_the_index_under_that_rule() {
    let (by_key, mut replica) = replica_holding(&["a"]);
    let by_header = config(&by_key.runtime, Some("h"));

    // Under the header rule the identity of "a" is its header value "A". A
    // record with another key and the same header value is a duplicate, and
    // the record key no longer matters.
    let mut same_header = batch().keys(&["z"]).call();
    same_header.records[0].headers[0].value = Some(Bytes::from_static(b"A"));
    let base = replica.produce_deduplicated(&by_header, same_header);

    assert!(base == Offset(1));
    assert!(keys(&replica.contents().records) == [(0, "a")]);
}

#[test]
fn a_job_read_under_other_settings_is_appended_whole_and_indexed() {
    let runtime = runtime();
    let by_key = config(&runtime, None);
    let by_header = config(&runtime, Some("h"));
    let mut replica = Replica::new(Some(by_key.clone()));
    replica.dedup.make_current(&replica.log, now_ms());
    let first = batch().keys(&["a"]).call();

    let header_job = job(&by_header, &first);
    replica.append_classified(first, Some(header_job), true);
    replica.produce_keys(&by_key, &["a"]);

    assert!(keys(&replica.contents().records) == [(0, "a")]);
}

#[test]
fn a_follower_holds_the_index_its_leader_holds() {
    let config = config(&runtime(), None);
    let mut leader = Replica::new(Some(config.clone()));
    let mut follower = Replica::new(Some(config.clone()));
    follower.dedup.make_current(&follower.log, now_ms());

    for keys in [&["a", "b"][..], &["b", "c", "a"], &["d"], &["d", "e"]] {
        leader.produce_keys(&config, keys);
    }
    for (index, batch) in leader.stored_batches().into_iter().enumerate() {
        // Half the batches arrive as wire bytes, the way the verbatim
        // replication path delivers them.
        let observed = if index % 2 == 0 {
            ObservedBatch::Owned(batch.clone())
        } else {
            let mut bytes = BytesMut::new();
            batch.encode(&mut bytes).unwrap();
            ObservedBatch::Bytes(bytes.freeze())
        };
        follower.replicate(batch, &observed);
    }

    assert!(follower.entries() == leader.entries());
    assert!(follower.is_current());
}

#[test]
fn a_follower_that_missed_an_append_goes_stale() {
    let mut follower = Replica::new(Some(config(&runtime(), None)));
    follower.dedup.make_current(&follower.log, now_ms());

    follower
        .dedup
        .follow(&ObservedBatch::Empty, Offset(5), Offset(6), now_ms());

    assert!(follower.active().covered_through.is_none());
}

#[test]
fn turning_dedup_off_frees_the_budget() {
    let (config, mut replica) = replica_holding(&["a", "b", "c"]);
    let budget = config.runtime.budget();
    let held = budget.entries();

    replica.dedup.configure(None);

    assert!(held == 3);
    assert!(budget.entries() == 0);
    assert!(!replica.dedup.is_enabled());
}

#[test]
fn a_narrower_window_keeps_the_index_and_a_wider_one_rebuilds_it() {
    let runtime = runtime();
    let narrow = windowed(&runtime, WINDOW_MS / 2);
    let wide = config(&runtime, None);
    for (name, from, to, stays_current) in [
        ("narrower", wide.clone(), narrow.clone(), true),
        ("wider", narrow, wide, false),
    ] {
        let mut replica = Replica::new(Some(from.clone()));
        replica.produce_keys(&from, &["a"]);

        replica.dedup.configure(Some(to));

        assert!(replica.is_current() == stays_current, "{name}");
    }
}

// One acquisition of the log lock by a reader: when it asked, and how long it
// waited.
struct LockProbe {
    asked: Instant,
    waited: Duration,
}

// Take and release the log lock, as a fetch does, until `stop` is set.
fn probe_log_lock(log: &Mutex<Log>, stop: &AtomicBool) -> Vec<LockProbe> {
    let mut probes = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let asked = Instant::now();
        let guard = log.lock().unwrap();
        let waited = asked.elapsed();
        drop(guard);
        probes.push(LockProbe { asked, waited });
        std::thread::sleep(Duration::from_micros(200));
    }
    probes
}

// The rebuild reads the log 1 MiB at a time and takes the log lock for each
// read only. A reader that takes the lock while it runs, as a fetch does,
// waits for one read at most, never for the whole rebuild.
#[test]
fn a_rebuild_releases_the_log_lock_between_reads() {
    const RECORDS: u64 = 160_000;
    let runtime = runtime_with(DedupWindowFilter::DEFAULT_MAX_ENTRIES, 16_000_000);
    let mut replica = Replica::new(None);
    fill()
        .replica(&mut replica)
        .total(RECORDS)
        .per_batch(2_000)
        .value_bytes(100)
        .call();
    replica.dedup.configure(Some(windowed(&runtime, HOUR_MS)));

    let stop = AtomicBool::new(false);
    let log = &replica.log;
    let dedup = &mut replica.dedup;
    let (started, finished, probes) = std::thread::scope(|scope| {
        let prober = scope.spawn(|| probe_log_lock(log, &stop));
        // Let the prober start before the rebuild does.
        std::thread::sleep(Duration::from_millis(20));
        let started = Instant::now();
        dedup.make_current(log, now_ms());
        let finished = Instant::now();
        stop.store(true, Ordering::Relaxed);
        (started, finished, prober.join().unwrap())
    });
    let rebuild = finished - started;
    let during: Vec<Duration> = probes
        .iter()
        .filter(|probe| probe.asked >= started && probe.asked < finished)
        .map(|probe| probe.waited)
        .collect();
    let longest = during.iter().max().copied().unwrap_or_default();
    eprintln!(
        "rebuild of {RECORDS} records took {rebuild:?}; {} lock acquisitions during it, longest \
         wait {longest:?}",
        during.len()
    );

    assert!(replica.index_len() == usize::try_from(RECORDS).unwrap());
    assert!(during.len() >= 20);
    assert!(longest * 4 < rebuild);
}

#[derive(Clone, Copy, Debug)]
enum Arrival {
    // Every identity created within one second.
    Burst,
    // Each step of identities created a minute after the one before.
    Spread,
}

impl Arrival {
    fn age_ms(self, step: u64, steps: u64) -> i64 {
        match self {
            Arrival::Burst => 1_000,
            Arrival::Spread => i64::try_from(steps - step).unwrap() * 60_000,
        }
    }
}

// The window holds five times the per-partition limit. The index stays at
// or under the limit while it is rebuilt from the log, while the leader
// classifies, and while a follower indexes what it replicates. Identities
// that arrive spread out over the window evict the oldest entries; a burst
// that the capacity histogram cannot split is admitted unindexed.
#[test]
fn the_index_never_exceeds_its_partition_limit_on_any_path() {
    const LIMIT: usize = 1_000;
    const STEPS: u64 = 50;
    const PER_STEP: u64 = 200;
    for arrival in [Arrival::Burst, Arrival::Spread] {
        let runtime = runtime_with(LIMIT, 1_000_000);
        let config = windowed(&runtime, HOUR_MS);
        let step_batch = |step: u64| {
            numbered_batch()
                .first(step * PER_STEP)
                .count(PER_STEP)
                .age_ms(arrival.age_ms(step, STEPS))
                .call()
        };
        let mut leader = Replica::new(None);
        for step in 0..STEPS / 2 {
            leader.produce(step_batch(step), None);
        }

        leader.dedup.configure(Some(config.clone()));
        leader.dedup.make_current(&leader.log, now_ms());
        let rebuilt = leader.index_len();
        let mut leader_sizes = Vec::new();
        for step in STEPS / 2..STEPS {
            leader.produce_deduplicated(&config, step_batch(step));
            leader_sizes.push(leader.index_len());
        }
        let mut follower = Replica::new(Some(config.clone()));
        follower.dedup.make_current(&follower.log, now_ms());
        let mut follower_sizes = Vec::new();
        for stored in leader.stored_batches() {
            follower.replicate(stored.clone(), &ObservedBatch::Owned(stored));
            follower_sizes.push(follower.index_len());
        }

        let leader_counters = leader.counters();
        let follower_counters = follower.counters();
        assert!(rebuilt <= LIMIT, "{arrival:?}");
        assert!(leader_sizes.iter().all(|&len| len <= LIMIT), "{arrival:?}");
        assert!(
            follower_sizes.iter().all(|&len| len <= LIMIT),
            "{arrival:?}"
        );
        assert!(
            leader_sizes.last() == Some(&leader.index_len()),
            "{arrival:?}"
        );
        assert!(runtime.budget().entries() == leader.index_len() + follower.index_len());
        match arrival {
            Arrival::Burst => {
                assert!(leader_counters.unindexed_records > 0);
                assert!(follower_counters.unindexed_records > 0);
            }
            Arrival::Spread => {
                assert!(leader_counters.capacity_evicted_entries > 0);
                assert!(follower_counters.capacity_evicted_entries > 0);
                // The oldest identities went first: the newest step is held.
                let again = leader.produce_deduplicated(&config, step_batch(STEPS - 1));
                let stored = leader.stored_batches();
                let last = stored.last().unwrap();
                assert!(Offset(last.base_offset) == again);
                assert!(last.records.is_empty());
            }
        }
    }
}

fn assert_budget_is_the_sum(runtime: &DedupRuntime, replicas: &[Replica], step: &str) {
    let sum: usize = replicas.iter().map(Replica::index_len).sum();
    let entries = runtime.budget().entries();
    assert!(entries == sum, "{step}");
    assert!(entries <= runtime.budget().max_entries(), "{step}");
}

// Four partitions share one broker budget that holds less than their
// per-partition limits together. The budget equals the sum of the indexes
// after every append, and after an index goes stale, is turned off, is
// dropped, and is rebuilt. A produce that carries the config turns a disabled
// index back on, as the writer adopts a produce job's config.
#[test]
fn the_broker_budget_is_the_sum_of_the_indexes_through_their_lifecycle() {
    const BROKER_LIMIT: usize = 2_500;
    let runtime = runtime_with(1_000, BROKER_LIMIT);
    let config = windowed(&runtime, HOUR_MS);
    let mut replicas: Vec<Replica> = (0..4).map(|_| Replica::new(Some(config.clone()))).collect();
    let produce_round = |replicas: &mut [Replica], round: u64| {
        for replica in replicas.iter_mut() {
            replica.produce_deduplicated(
                &config,
                numbered_batch().first(round * 100).count(100).call(),
            );
        }
    };

    for round in 0..15 {
        produce_round(&mut replicas, round);
        assert_budget_is_the_sum(&runtime, &replicas, &format!("round {round}"));
    }
    let full = runtime.budget().entries();
    replicas[0].dedup.invalidate();
    assert_budget_is_the_sum(&runtime, &replicas, "invalidate");
    replicas[1].dedup.configure(None);
    assert_budget_is_the_sum(&runtime, &replicas, "configure(None)");
    let disabled = (replicas[0].index_len(), replicas[1].index_len());
    drop(replicas.remove(2));
    assert_budget_is_the_sum(&runtime, &replicas, "drop");
    let released = runtime.budget().entries();
    let Replica { log, dedup, .. } = &mut replicas[0];
    dedup.make_current(log, now_ms());
    assert_budget_is_the_sum(&runtime, &replicas, "rebuild");
    for round in 15..20 {
        produce_round(&mut replicas, round);
        assert_budget_is_the_sum(&runtime, &replicas, &format!("round {round}"));
    }

    assert!(full >= BROKER_LIMIT * 9 / 10);
    assert!(released < full / 2);
    assert!(disabled == (0, 0));
    assert!(replicas[0].index_len() > 0);
    drop(replicas);
    assert!(runtime.budget().entries() == 0);
}

// A log whose old prefix is two hundred times its recent tail. A rebuild
// starts at the first offset inside the window, so with a one-hour window it
// indexes only the tail and visits only the tail's entries, and it takes a
// fraction of the time a window that covers the whole log takes.
#[test]
fn a_rebuild_reads_the_window_and_not_the_log_before_it() {
    const OLD: u64 = 100_000;
    const RECENT: u64 = 500;
    let runtime = runtime_with(DedupWindowFilter::DEFAULT_MAX_ENTRIES, 16_000_000);
    let mut replica = Replica::new(None);
    fill()
        .replica(&mut replica)
        .prefix("old")
        .total(OLD)
        .per_batch(2_000)
        .age_ms(2 * HOUR_MS)
        .value_bytes(64)
        .call();
    fill()
        .replica(&mut replica)
        .prefix("recent")
        .total(RECENT)
        .per_batch(100)
        .call();

    let mut rebuilt = Vec::new();
    for window_ms in [HOUR_MS, 3 * HOUR_MS] {
        replica.dedup.configure(Some(windowed(&runtime, window_ms)));
        let visits = replica.sweep_visits();
        let started = Instant::now();
        replica.dedup.make_current(&replica.log, now_ms());
        let took = started.elapsed();
        rebuilt.push((replica.index_len(), replica.sweep_visits() - visits, took));
    }
    let [
        (tail_len, tail_visits, tail_took),
        (whole_len, _, whole_took),
    ] = rebuilt[..]
    else {
        panic!("two rebuilds");
    };
    eprintln!("rebuild of the window: {tail_took:?}; of the whole log: {whole_took:?}");

    assert!(tail_len == usize::try_from(RECENT).unwrap());
    assert!(whole_len == usize::try_from(OLD + RECENT).unwrap());
    // The sweep visits at most eight entries for each identity it indexes.
    assert!(tail_visits <= 8 * RECENT);
    assert!(tail_took * 4 < whole_took);
}
