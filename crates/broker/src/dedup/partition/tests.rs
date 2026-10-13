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
    tempfile::TempDir,
};

const WINDOW_MS: i64 = 60_000;

fn runtime() -> Arc<DedupRuntime> {
    Arc::new(DedupRuntime::new(
        1_000,
        1_000_000,
        RecordDecompressionPolicy::default(),
        BrokerMetrics::new(),
    ))
}

fn config(runtime: &Arc<DedupRuntime>, key_header: Option<&str>) -> DedupConfig {
    DedupConfig {
        settings: DedupSettings {
            window_ms: WINDOW_MS,
            key_header: key_header.map(Arc::from),
        },
        runtime: Arc::clone(runtime),
    }
}

/// One record for each key, `age_ms` before now, and an `h` header that holds
/// the key in upper case.
#[bon::builder]
fn batch(keys: &[&str], #[builder(default = 1_000)] age_ms: i64) -> RecordBatch {
    let base_timestamp = now_ms() - age_ms;
    let records: Vec<Record> = keys
        .iter()
        .zip(0..)
        .map(|(key, offset_delta)| Record {
            offset_delta,
            key: Some(Bytes::copy_from_slice(key.as_bytes())),
            value: Some(Bytes::from_static(b"value")),
            headers: vec![RecordHeader {
                key: "h".to_owned(),
                value: Some(Bytes::from(key.to_uppercase())),
            }],
            ..Record::default()
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

/// Every record of a log as `(offset, key)`, and every batch's offset span.
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

    /// The writer's leader path for one batch: catch up, classify, append,
    /// finish. Returns the base offset the log assigned.
    fn produce(&mut self, batch: RecordBatch, job: Option<DedupJob>) -> Offset {
        self.produce_with(batch, job, true)
    }

    fn produce_with(&mut self, batch: RecordBatch, job: Option<DedupJob>, succeed: bool) -> Offset {
        let now = now_ms();
        self.dedup.adopt(job.as_ref());
        self.dedup.make_current(&self.log, now);
        let before = self.log_end();
        let ClassifiedAppend { data, pending } =
            self.dedup
                .classify(ProduceData::Owned(batch), job, before, None, now);
        let ProduceData::Owned(mut batch) = data else {
            panic!("an owned batch stays owned");
        };
        let base = if succeed {
            Some(self.log.lock().unwrap().append(&mut batch).unwrap().0)
        } else {
            None
        };
        let after = self.log_end();
        self.dedup.finish(pending, succeed, after, None, now);
        base.unwrap_or(Offset(-1))
    }

    fn contents(&self) -> Contents {
        let read = self
            .log
            .lock()
            .unwrap()
            .read(Offset(0), mebibytes(1))
            .unwrap();
        let records = read
            .batches
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
        let spans = read
            .batches
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

    fn entries(&self) -> Vec<(IdentityDigest, i64)> {
        self.dedup
            .active
            .as_ref()
            .map(|active| active.filter.iter().collect())
            .unwrap_or_default()
    }
}

fn keys(records: &[(i64, String)]) -> Vec<(i64, &str)> {
    records
        .iter()
        .map(|(offset, key)| (*offset, key.as_str()))
        .collect()
}

#[test]
fn duplicates_leave_offset_gaps_and_an_all_duplicate_batch_appends_empty() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut replica = Replica::new(Some(config.clone()));

    let first = batch().keys(&["a", "b"]).call();
    let second = batch().keys(&["a", "c", "b", "d"]).call();
    let third = batch().keys(&["c", "d"]).call();
    let bases = [
        replica.produce(first.clone(), Some(job(&config, &first))),
        replica.produce(second.clone(), Some(job(&config, &second))),
        replica.produce(third.clone(), Some(job(&config, &third))),
    ];

    let Contents { records, spans } = replica.contents();
    assert!(bases == [Offset(0), Offset(2), Offset(6)]);
    assert!(keys(&records) == [(0, "a"), (1, "b"), (3, "c"), (5, "d")]);
    assert!(spans == [(0, 1), (2, 5), (6, 7)]);
    assert!(replica.log_end() == Offset(8));
}

#[test]
fn a_failed_append_gives_its_identities_back() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut replica = Replica::new(Some(config.clone()));
    let lost = batch().keys(&["x"]).call();

    replica.produce_with(lost.clone(), Some(job(&config, &lost)), false);
    let retried = replica.produce(lost.clone(), Some(job(&config, &lost)));

    assert!(retried == Offset(0));
    assert!(keys(&replica.contents().records) == [(0, "x")]);
}

#[test]
fn a_new_index_is_rebuilt_from_the_window_of_the_log() {
    let runtime = runtime();
    let config = config(&runtime, None);
    // The log holds records written before the index existed: one older than
    // the window, and one inside it.
    let mut replica = Replica::new(None);
    replica.produce(batch().keys(&["old"]).age_ms(2 * WINDOW_MS).call(), None);
    replica.produce(batch().keys(&["recent"]).call(), None);
    replica.dedup.configure(Some(config.clone()));

    let next = batch().keys(&["old", "recent"]).call();
    replica.produce(next.clone(), Some(job(&config, &next)));

    assert!(keys(&replica.contents().records) == [(0, "old"), (1, "recent"), (2, "old")]);
}

#[test]
fn a_truncation_makes_the_index_forget_the_records_it_removed() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut replica = Replica::new(Some(config.clone()));
    let first = batch().keys(&["a"]).call();
    replica.produce(first.clone(), Some(job(&config, &first)));

    replica.log.lock().unwrap().truncate_to(Offset(0)).unwrap();
    replica.dedup.invalidate();
    let again = replica.produce(first.clone(), Some(job(&config, &first)));

    assert!(again == Offset(0));
    assert!(keys(&replica.contents().records) == [(0, "a")]);
}

#[test]
fn a_stale_index_classifies_nothing_until_it_is_rebuilt() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut replica = Replica::new(Some(config.clone()));
    let first = batch().keys(&["a"]).call();
    replica.produce(first.clone(), Some(job(&config, &first)));
    replica.dedup.invalidate();

    let log_end = replica.log_end();
    let ClassifiedAppend { pending, .. } = replica.dedup.classify(
        ProduceData::Owned(first.clone()),
        Some(job(&config, &first)),
        log_end,
        None,
        now_ms(),
    );

    assert!(matches!(pending, PendingAppend::Ignored));
}

#[test]
fn a_new_identity_rule_rebuilds_the_index_under_that_rule() {
    let runtime = runtime();
    let by_key = config(&runtime, None);
    let by_header = config(&runtime, Some("h"));
    let mut replica = Replica::new(Some(by_key.clone()));
    let first = batch().keys(&["a"]).call();
    replica.produce(first.clone(), Some(job(&by_key, &first)));

    // Under the header rule the identity of "a" is its header value "A". A
    // record with another key and the same header value is a duplicate, and
    // the record key no longer matters.
    let mut same_header = batch().keys(&["z"]).call();
    same_header.records[0].headers[0].value = Some(Bytes::from_static(b"A"));
    let base = replica.produce(same_header.clone(), Some(job(&by_header, &same_header)));

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

    let log_end = replica.log_end();
    let ClassifiedAppend { data, pending } = replica.dedup.classify(
        ProduceData::Owned(first.clone()),
        Some(job(&by_header, &first)),
        log_end,
        None,
        now_ms(),
    );
    let ProduceData::Owned(mut appended) = data else {
        panic!("an owned batch stays owned");
    };
    replica.log.lock().unwrap().append(&mut appended).unwrap();
    let after = replica.log_end();
    replica.dedup.finish(pending, true, after, None, now_ms());
    let again = batch().keys(&["a"]).call();
    replica.produce(again.clone(), Some(job(&by_key, &again)));

    assert!(keys(&replica.contents().records) == [(0, "a")]);
}

#[test]
fn a_follower_holds_the_index_its_leader_holds() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut leader = Replica::new(Some(config.clone()));
    let mut follower = Replica::new(Some(config.clone()));
    follower.dedup.make_current(&follower.log, now_ms());

    for keys in [&["a", "b"][..], &["b", "c", "a"], &["d"], &["d", "e"]] {
        let produced = batch().keys(keys).call();
        leader.produce(produced.clone(), Some(job(&config, &produced)));
    }
    let stored = leader
        .log
        .lock()
        .unwrap()
        .read(Offset(0), mebibytes(1))
        .unwrap()
        .batches;
    for (index, mut batch) in stored.into_iter().enumerate() {
        let before = follower.log_end();
        let base = Offset(batch.base_offset);
        // Half the batches arrive as wire bytes, the way the verbatim
        // replication path delivers them.
        let observed = if index % 2 == 0 {
            ObservedBatch::Owned(batch.clone())
        } else {
            let mut bytes = BytesMut::new();
            batch.encode(&mut bytes).unwrap();
            ObservedBatch::Bytes(bytes.freeze())
        };
        follower
            .log
            .lock()
            .unwrap()
            .append_at(&mut batch, base)
            .unwrap();
        let after = follower.log_end();
        follower.dedup.follow(&observed, before, after, now_ms());
    }

    assert!(follower.entries() == leader.entries());
    assert!(
        follower
            .dedup
            .active
            .as_ref()
            .unwrap()
            .is_current(follower.log_end())
    );
}

#[test]
fn a_follower_that_missed_an_append_goes_stale() {
    let runtime = runtime();
    let mut follower = Replica::new(Some(config(&runtime, None)));
    follower.dedup.make_current(&follower.log, now_ms());

    follower
        .dedup
        .follow(&ObservedBatch::Empty, Offset(5), Offset(6), now_ms());

    assert!(
        follower
            .dedup
            .active
            .as_ref()
            .unwrap()
            .covered_through
            .is_none()
    );
}

#[test]
fn turning_dedup_off_frees_the_budget() {
    let runtime = runtime();
    let config = config(&runtime, None);
    let mut replica = Replica::new(Some(config.clone()));
    let first = batch().keys(&["a", "b", "c"]).call();
    replica.produce(first.clone(), Some(job(&config, &first)));
    let held = runtime.budget().entries();

    replica.dedup.configure(None);

    assert!(held == 3);
    assert!(runtime.budget().entries() == 0);
    assert!(!replica.dedup.is_enabled());
}

#[test]
fn a_narrower_window_keeps_the_index_and_a_wider_one_rebuilds_it() {
    let runtime = runtime();
    let narrow = DedupConfig {
        settings: DedupSettings {
            window_ms: WINDOW_MS / 2,
            key_header: None,
        },
        runtime: Arc::clone(&runtime),
    };
    let wide = config(&runtime, None);
    for (name, from, to, stays_current) in [
        ("narrower", wide.clone(), narrow.clone(), true),
        ("wider", narrow, wide, false),
    ] {
        let mut replica = Replica::new(Some(from.clone()));
        let first = batch().keys(&["a"]).call();
        replica.produce(first.clone(), Some(job(&from, &first)));

        replica.dedup.configure(Some(to));

        let log_end = replica.log_end();
        let current = replica.dedup.active.as_ref().unwrap().is_current(log_end);
        assert!(current == stays_current, "{name}");
    }
}
