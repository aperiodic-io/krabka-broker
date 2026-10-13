//! Adversarial and stress cases for the writer-side dedup index: the shapes a
//! hostile or merely unlucky producer, operator, or log can hand the writer,
//! and what each one costs it.
//!
//! The cases drive [`PartitionDedup`] against a real [`Log`] through the
//! calls the partition writer makes: adopt the job's config, catch the index
//! up, classify, append, finish. Each case says what it measures and prints
//! its numbers, so a run with `--nocapture` doubles as a report.

use {
    super::{
        DedupConfig, DedupJob, DedupRuntime, DedupSettings, IdentityCollector, PartitionDedup,
        partition::ClassifiedAppend,
    },
    crate::{metrics::BrokerMetrics, partition::ProduceData, time_util::now_ms},
    assert2::assert,
    bytes::Bytes,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_dedup::DedupWindowFilter,
    krabka_log::{Log, LogConfig},
    krabka_protocol::records::{Attributes, Record, RecordBatch},
    std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

const HOUR_MS: i64 = 3_600_000;

fn config(window_ms: i64) -> DedupConfig {
    DedupConfig {
        settings: DedupSettings {
            window_ms,
            key_header: None,
        },
        runtime: Arc::new(DedupRuntime::new(
            DedupWindowFilter::DEFAULT_MAX_ENTRIES,
            16_000_000,
            RecordDecompressionPolicy::default(),
            BrokerMetrics::new(),
        )),
    }
}

/// One record of a [`batch`]: its key, the size of its value, and its
/// timestamp relative to the batch's base timestamp.
#[derive(Clone, Copy)]
struct Spec<'a> {
    key: &'a str,
    value_bytes: usize,
    timestamp_delta: i64,
}

impl<'a> Spec<'a> {
    fn key(key: &'a str) -> Self {
        Self {
            key,
            value_bytes: 8,
            timestamp_delta: 0,
        }
    }
}

/// One client batch holding `records` in order, created at `base_timestamp`.
#[bon::builder]
fn batch(
    records: &[Spec<'_>],
    #[builder(default = now_ms())] base_timestamp: i64,
    #[builder(default = CompressionType::None)] compression: CompressionType,
) -> RecordBatch {
    let records: Vec<Record> = records
        .iter()
        .zip(0..)
        .map(|(spec, offset_delta)| Record {
            offset_delta,
            timestamp_delta: spec.timestamp_delta,
            key: Some(Bytes::copy_from_slice(spec.key.as_bytes())),
            value: Some(Bytes::from(vec![b'v'; spec.value_bytes])),
            ..Record::default()
        })
        .collect();
    let max_delta = records
        .iter()
        .map(|record| record.timestamp_delta)
        .max()
        .unwrap_or(0);
    RecordBatch {
        attributes: Attributes::default().with_compression(compression),
        last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
        base_timestamp,
        max_timestamp: base_timestamp.saturating_add(max_delta),
        records,
        ..RecordBatch::default()
    }
}

fn keyed(keys: &[&str]) -> RecordBatch {
    let specs: Vec<Spec<'_>> = keys.iter().map(|key| Spec::key(key)).collect();
    batch().records(&specs).call()
}

/// The job the produce handler builds for `batch` under `config`.
fn job(config: &DedupConfig, batch: &RecordBatch) -> DedupJob {
    let header = config.settings.key_header.clone();
    let mut collector = IdentityCollector::new(header, batch.base_timestamp, batch.records.len());
    for record in &batch.records {
        collector.visit_owned(record);
    }
    let records = collector.finish().records;
    DedupJob {
        config: config.clone(),
        records,
    }
}

/// One partition replica under test: the index its writer keeps, its log, and
/// the directory that backs the log.
struct Harness {
    dedup: PartitionDedup,
    log: Mutex<Log>,
    _dir: TempDir,
}

/// A fresh replica, with dedup set to `config`.
fn harness(config: Option<DedupConfig>) -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let opened = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut dedup = PartitionDedup::new(Arc::from("stress"), 0);
    dedup.configure(config);
    Harness {
        dedup,
        log: Mutex::new(opened),
        _dir: dir,
    }
}

impl Harness {
    /// Append `batch` as a partition without dedup appends it.
    fn append_plain(&self, mut batch: RecordBatch) {
        self.log.lock().unwrap().append(&mut batch).unwrap();
    }

    /// The writer's leader path for one produce whose job was read under
    /// `job_config`. Returns how many records the log stored.
    fn produce(&mut self, job_config: &DedupConfig, batch: RecordBatch) -> usize {
        let job = job(job_config, &batch);
        let now = now_ms();
        self.dedup.adopt(Some(&job));
        self.dedup.make_current(&self.log, now);
        let before = self.log.lock().unwrap().log_end_offset();
        let ClassifiedAppend { data, pending } =
            self.dedup
                .classify(ProduceData::Owned(batch), Some(job), before, None, now);
        let ProduceData::Owned(mut stored) = data else {
            panic!("an owned batch stays owned");
        };
        let kept = stored.records.len();
        let after = {
            let mut log = self.log.lock().unwrap();
            log.append(&mut stored).unwrap();
            log.log_end_offset()
        };
        self.dedup.finish(pending, true, after, None, now);
        kept
    }
}

/// A rebuild reads the log 1 MiB at a time, and a stored batch can be larger
/// than that: Kafka's default `max.message.bytes` (1,048,588) admits a batch
/// of 1 MiB plus 12 bytes. Such a batch, and every batch after it, must be
/// indexed. A rebuild that stopped at it would report itself current and
/// admit the duplicates of all of them until the next rebuild.
#[test]
fn a_rebuild_indexes_batches_larger_than_its_read_size() {
    // Uncompressed, one record each: the stored size is the value plus about
    // 80 bytes of batch and record framing.
    let cases = [
        ("a batch of 2 MiB under a raised max.message.bytes", 2 << 20),
        (
            "a batch over 1 MiB and within Kafka's default max.message.bytes",
            1_048_506,
        ),
        ("a batch below 1 MiB behind a 3 KiB batch", 1_046_000),
    ];
    for (name, big_value) in cases {
        let config = config(HOUR_MS);
        let replica_config = config.clone();
        let mut replica = harness(None);
        let small: Vec<String> = (0..10).map(|n| format!("small-{n}")).collect();
        let small_specs: Vec<Spec<'_>> = small
            .iter()
            .map(|key| Spec {
                value_bytes: 290,
                ..Spec::key(key)
            })
            .collect();
        replica.append_plain(batch().records(&small_specs).call());
        replica.append_plain(
            batch()
                .records(&[Spec {
                    value_bytes: big_value,
                    ..Spec::key("big")
                }])
                .call(),
        );
        replica.append_plain(keyed(&["after-0", "after-1"]));
        // Turning dedup on makes the index stale; the first produce rebuilds
        // it from the window, which holds every batch above.
        replica.dedup.configure(Some(replica_config));

        let kept = replica.produce(
            &config,
            keyed(&["small-0", "big", "after-0", "after-1", "fresh"]),
        );

        assert!(
            kept == 1,
            "{name}: only the fresh key is new, but {kept} records were kept"
        );
    }
}

/// Take and release the log lock as a fetch does, until `stop` is set, and
/// return the longest wait among the acquisitions asked for in `[from, to)`.
fn longest_wait_during(log: &Mutex<Log>, stop: &AtomicBool) -> Vec<(Instant, Duration)> {
    let mut probes = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let asked = Instant::now();
        drop(log.lock().unwrap());
        probes.push((asked, asked.elapsed()));
        std::thread::sleep(Duration::from_micros(200));
    }
    probes
}

/// A rebuild takes the log lock for each 1 MiB read only to copy the wire
/// bytes. 1 MiB of a compressed batch of small records is tens of MiB and
/// millions of records once decoded, so decoding under the lock would make a
/// fetch of the partition wait for all of it.
///
/// Measures the longest wait of a lock prober during a rebuild of highly
/// compressible zstd batches, and bounds it at 250 ms: a fetch should never
/// wait for more than one small read. Decoding under the lock held it for
/// 1.2 s in one acquisition on a rebuild of 1.2M records from 24 zstd
/// batches.
#[test]
fn a_rebuild_of_compressed_batches_holds_the_log_lock_briefly() {
    const BATCHES: usize = 24;
    const RECORDS_PER_BATCH: usize = 50_000;
    let config = config(HOUR_MS);
    let mut replica = harness(None);
    let keys: Vec<String> = (0..RECORDS_PER_BATCH)
        .map(|n| format!("{}", n % 512))
        .collect();
    let specs: Vec<Spec<'_>> = keys
        .iter()
        .map(|key| Spec {
            value_bytes: 0,
            ..Spec::key(key)
        })
        .collect();
    for _ in 0..BATCHES {
        replica.append_plain(
            batch()
                .records(&specs)
                .compression(CompressionType::Zstd)
                .call(),
        );
    }
    let stored = replica.log.lock().unwrap().size();
    replica.dedup.configure(Some(config));

    let stop = AtomicBool::new(false);
    let log = &replica.log;
    let dedup = &mut replica.dedup;
    let (rebuild, probes, started) = std::thread::scope(|scope| {
        let prober = scope.spawn(|| longest_wait_during(log, &stop));
        std::thread::sleep(Duration::from_millis(20));
        let started = Instant::now();
        dedup.make_current(log, now_ms());
        let rebuild = started.elapsed();
        stop.store(true, Ordering::Relaxed);
        (rebuild, prober.join().unwrap(), started)
    });
    let during: Vec<Duration> = probes
        .iter()
        .filter(|(asked, _)| *asked >= started && *asked < started + rebuild)
        .map(|(_, waited)| *waited)
        .collect();
    let longest = during.iter().max().copied().unwrap_or_default();
    eprintln!(
        "rebuild of {} compressed records ({stored:?} on disk) took {rebuild:?}; {} lock \
         acquisitions during it, longest wait {longest:?}",
        BATCHES * RECORDS_PER_BATCH,
        during.len()
    );

    assert!(longest < Duration::from_millis(250));
}

/// The operator turned dedup off, so the broker's newest image reports the
/// topic has no window. A produce job prepared before that change still
/// carries the old, enabled config. When it reaches the writer, the index
/// must take the config the image reports now, not the job's, so a stale job
/// cannot turn deduplication back on.
///
/// Target: coordinator finding 4. [`PartitionDedup::adopt`] consults the
/// runtime's resolver (the metadata image) when the job's config differs from
/// the active one; this installs a resolver that reports the topic off.
#[test]
fn a_stale_job_cannot_reenable_dedup_the_image_turned_off() {
    let enabled = config(HOUR_MS);
    let mut replica = harness(Some(enabled.clone()));
    assert!(replica.produce(&enabled, keyed(&["a"])) == 1);

    // The operator turns dedup off; the broker's image now reports no window.
    replica.dedup.configure(None);
    enabled.runtime.set_resolver(|_topic| None);

    // The stale job still carries the enabled config, but adopting it must
    // not re-enable the index.
    let kept = replica.produce(&enabled, keyed(&["a"]));

    assert!(kept == 1);
    assert!(!replica.dedup.is_enabled());
}

/// Timestamps and windows at the edges of `i64`: a producer can send any
/// `CreateTime`, and `dedup.window.ms` accepts any positive `i64`. None of
/// these may panic on the leader path or a rebuild, and the index never holds
/// more than the two distinct identities it is shown. (The window is judged
/// against the broker clock, so records dated far outside it are not
/// deduplicated against each other; this test asserts crash-safety and index
/// consistency, not a drop count.)
#[test]
fn extreme_timestamps_and_windows_classify_without_panicking() {
    let now = now_ms();
    let cases: [(&str, i64, i64, [i64; 2]); 8] = [
        ("no timestamp (-1)", HOUR_MS, -1, [0, 0]),
        ("epoch zero", HOUR_MS, 0, [0, 0]),
        ("i64::MIN base", HOUR_MS, i64::MIN, [0, 0]),
        ("i64::MAX base, clamped", HOUR_MS, i64::MAX, [0, 0]),
        ("one hour ahead, clamped", HOUR_MS, now + HOUR_MS, [0, 0]),
        ("window of 1 ms", 1, now, [0, 0]),
        ("window of i64::MAX", i64::MAX, now, [0, 0]),
        ("deltas spanning i64", i64::MAX, i64::MIN, [0, i64::MAX]),
    ];
    for (name, window_ms, base_timestamp, deltas) in cases {
        let config = config(window_ms);
        let mut leader = harness(Some(config.clone()));
        let specs = [
            Spec {
                timestamp_delta: deltas[0],
                ..Spec::key("k")
            },
            Spec {
                timestamp_delta: deltas[0],
                ..Spec::key("k")
            },
            Spec {
                timestamp_delta: deltas[1],
                ..Spec::key("other")
            },
        ];
        let produced = batch()
            .records(&specs)
            .base_timestamp(base_timestamp)
            .call();

        let kept = leader.produce(&config, produced);
        // A rebuild reads the same records back from the log, then a fresh
        // produce runs against the rebuilt index. Neither may panic.
        leader.dedup.invalidate();
        leader.dedup.make_current(&leader.log, now_ms());
        let again = leader.produce(&config, keyed(&["k"]));

        // At least the two distinct identities are kept, at most all three
        // records; the fresh single-record produce keeps zero or one.
        assert!((2..=3).contains(&kept), "{name}: kept {kept}");
        assert!(again <= 1, "{name}: fresh produce kept {again}");
        eprintln!("{name}: kept {kept} of 3, fresh copy kept {again}");
    }
}
