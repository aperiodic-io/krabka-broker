//! One broker, stopped and started again on the directory it wrote.
//!
//! Nothing of the index survives a stop, so every case here checks that the
//! index the next boot rebuilds from the last window of the log holds what
//! that log holds, read with the topic's current rule: the records that
//! recovery kept are duplicates, the records it cut are not, and the records
//! older than the window at the time of the rebuild are not either.

use {
    crate::{
        harness::{
            Damage, DedupCounters, SingleBroker, WINDOW_MS, at, first_boot, keyed, reboot,
            repeated_keys,
        },
        support::{
            client::connect_client,
            cluster_lock,
            configs::{ConfigMutationOutcome, alter_topic_config},
            dedup::{DEDUP_HEADER, Rec, keyed_batch},
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            produce::expected_partition_success,
        },
    },
    assert2::assert,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
};

/// An `IncrementalAlterConfigs` that succeeded.
const ALTERED: ConfigMutationOutcome = ConfigMutationOutcome {
    error_code: 0,
    error_message: None,
};

/// A count of records as the counters report it.
fn count(records: usize) -> f64 { f64::from(u32::try_from(records).expect("a small count")) }

#[tokio::test]
async fn a_restarted_broker_rebuilds_its_index_from_the_log_and_keeps_exact_offsets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = first_boot()
        .dir(dir.path())
        .topic("restart-keeps")
        .call()
        .await;
    let written = [
        first.produce(keyed(&["a", "b"])).await,
        first.produce(keyed(&["c"])).await,
    ];
    first.shutdown().await;

    let broker = reboot(dir.path(), "restart-keeps").await;
    let after_boot = broker.counters().await;
    let duplicates = broker.produce(keyed(&["a", "d", "c"])).await;
    let after_first = broker.counters().await;
    let more = broker.produce(keyed(&["d", "e"])).await;
    let after_second = broker.counters().await;
    let contents = broker.read().await;

    assert!(
        written
            == [
                expected_partition_success(0, 0),
                expected_partition_success(2, 0)
            ]
    );
    // A booted broker holds no index yet: the first append rebuilds it.
    assert!(
        after_boot
            == DedupCounters {
                dropped: 0.0,
                index_rebuilds: 0.0
            }
    );
    assert!(duplicates == expected_partition_success(3, 0));
    assert!(
        after_first
            == DedupCounters {
                dropped: 2.0,
                index_rebuilds: 1.0
            }
    );
    // Every later append keeps the rebuilt index current.
    assert!(more == expected_partition_success(6, 0));
    assert!(
        after_second
            == DedupCounters {
                dropped: 3.0,
                index_rebuilds: 1.0
            }
    );
    assert!(contents.offsets_and_keys() == at(&[(0, "a"), (1, "b"), (2, "c"), (4, "d"), (7, "e")]));
    assert!(contents.high_watermark == 8);
    broker.shutdown().await;
}

#[tokio::test]
async fn the_rebuilt_index_holds_exactly_the_records_that_log_recovery_kept() {
    struct Case {
        damage: Damage,
        recovered_end: i64,
        base_offset: i64,
        expected: Vec<(i64, String)>,
    }

    // Two batches, `[a1 a2]` at 0 and `[b1 b2]` at 2. After the restart one
    // request sends `a1 b1 n`: a key of each batch and a new one.
    let survived = at(&[(0, "a1"), (1, "a2"), (2, "b1"), (3, "b2"), (6, "n")]);
    let cases = [
        Case {
            damage: Damage::None,
            recovered_end: 4,
            base_offset: 4,
            expected: survived.clone(),
        },
        Case {
            damage: Damage::NoCleanShutdownProof,
            recovered_end: 4,
            base_offset: 4,
            expected: survived.clone(),
        },
        Case {
            damage: Damage::TornCopy,
            recovered_end: 4,
            base_offset: 4,
            expected: survived,
        },
        // The cut loses `[b1 b2]`. The index must not remember `b1`: the
        // log no longer holds it, so a new `b1` is the first one.
        Case {
            damage: Damage::LastBatchCut,
            recovered_end: 2,
            base_offset: 2,
            expected: at(&[(0, "a1"), (1, "a2"), (3, "b1"), (4, "n")]),
        },
    ];

    for (index, case) in cases.into_iter().enumerate() {
        let topic = format!("tail-{index}");
        let dir = tempfile::tempdir().expect("tempdir");
        let first = first_boot().dir(dir.path()).topic(&topic).call().await;
        first.produce(keyed(&["a1", "a2"])).await;
        first.produce(keyed(&["b1", "b2"])).await;
        first.shutdown().await;
        case.damage.apply(dir.path(), &topic);

        let broker = reboot(dir.path(), &topic).await;
        let recovered_end = broker.broker.local_log_end_offset(&topic, 0);
        let row = broker.produce(keyed(&["a1", "b1", "n"])).await;
        let contents = broker.read().await;

        let damage = case.damage;
        assert!(recovered_end == Some(case.recovered_end), "{damage:?}");
        assert!(
            row == expected_partition_success(case.base_offset, 0),
            "{damage:?}"
        );
        assert!(contents.offsets_and_keys() == case.expected, "{damage:?}");
        assert!(
            contents.high_watermark == case.base_offset + 3,
            "{damage:?}"
        );
        broker.shutdown().await;
    }
}

#[tokio::test]
async fn a_restart_agrees_with_the_running_index_on_what_left_the_window() {
    // `old` is 35 s old and `fresh` 10 s, against a 30 s window. The window
    // is judged against the broker clock, so `old` is outside it the moment
    // it arrives and is never indexed, while `fresh` is. A rebuild starts at
    // the first record inside the window of the broker clock, which is
    // `fresh`. So a running broker and a restarted one hold the same index:
    // a copy of `old` that is a second younger is the first record of its
    // identity, while a copy of `fresh` is still a duplicate.
    struct Case {
        restart: bool,
        expected: Vec<(i64, String)>,
    }

    const WINDOW: i64 = 30_000;
    let cases = [false, true].map(|restart| Case {
        restart,
        expected: at(&[(0, "old"), (1, "fresh"), (2, "old")]),
    });

    for case in cases {
        let topic = format!("window-restart-{}", case.restart);
        let dir = tempfile::tempdir().expect("tempdir");
        let mut broker = first_boot()
            .dir(dir.path())
            .topic(&topic)
            .window_ms(WINDOW)
            .call()
            .await;
        broker
            .produce(
                keyed_batch()
                    .records(&[Rec::key("old").aged(35_000)])
                    .call(),
            )
            .await;
        broker
            .produce(
                keyed_batch()
                    .records(&[Rec::key("fresh").aged(10_000)])
                    .call(),
            )
            .await;
        if case.restart {
            broker.shutdown().await;
            broker = reboot(dir.path(), &topic).await;
        }
        let row = broker
            .produce(
                keyed_batch()
                    .records(&[Rec::key("old").aged(34_000), Rec::key("fresh").aged(10_000)])
                    .call(),
            )
            .await;
        let contents = broker.read().await;

        let restart = case.restart;
        assert!(row == expected_partition_success(2, 0), "restart={restart}");
        assert!(
            contents.offsets_and_keys() == case.expected,
            "restart={restart}"
        );
        assert!(contents.high_watermark == 4, "restart={restart}");
        broker.shutdown().await;
    }
}

/// When a topic's identity rule changes from the record key to
/// [`DEDUP_HEADER`].
#[derive(Clone, Copy, Debug)]
enum RuleChange {
    /// While the broker runs, before it stops, and with no append after it.
    BeforeShutdown,
    /// After the restart, before the first append.
    AfterRestart,
    /// After the restart and the first append, which rebuilt the index under
    /// the key rule.
    AfterFirstAppend,
}

#[tokio::test]
async fn the_rebuilt_index_reads_the_log_with_the_current_identity_rule() {
    // Each record carries both a key and the header. The last request sends
    // `k1` under a new header value and `kY` under `h2`: the header rule
    // keeps the first and drops the second, the key rule would do the
    // opposite. Wherever the change lands, the index ends up holding what the
    // header rule reads from the whole log.
    for change in [
        RuleChange::BeforeShutdown,
        RuleChange::AfterRestart,
        RuleChange::AfterFirstAppend,
    ] {
        let topic = format!("rule-{change:?}").to_lowercase();
        let dir = tempfile::tempdir().expect("tempdir");
        let first = first_boot().dir(dir.path()).topic(&topic).call().await;
        first
            .produce(
                keyed_batch()
                    .records(&[Rec::key("k1").dedup_id("h1"), Rec::key("k2").dedup_id("h2")])
                    .call(),
            )
            .await;
        if matches!(change, RuleChange::BeforeShutdown) {
            use_header_rule(&first).await;
        }
        first.shutdown().await;

        let broker = reboot(dir.path(), &topic).await;
        if matches!(change, RuleChange::AfterRestart) {
            use_header_rule(&broker).await;
        }
        let warm_up = broker
            .produce(
                keyed_batch()
                    .records(&[Rec::key("w").dedup_id("hw")])
                    .call(),
            )
            .await;
        if matches!(change, RuleChange::AfterFirstAppend) {
            use_header_rule(&broker).await;
        }
        let row = broker
            .produce(
                keyed_batch()
                    .records(&[Rec::key("k1").dedup_id("hX"), Rec::key("kY").dedup_id("h2")])
                    .call(),
            )
            .await;
        let contents = broker.read().await;

        assert!(warm_up == expected_partition_success(2, 0), "{change:?}");
        assert!(row == expected_partition_success(3, 0), "{change:?}");
        assert!(
            contents.offsets_and_keys() == at(&[(0, "k1"), (1, "k2"), (2, "w"), (3, "k1")]),
            "{change:?}"
        );
        assert!(contents.high_watermark == 5, "{change:?}");
        broker.shutdown().await;
    }
}

/// Switch the topic of `broker` to the [`DEDUP_HEADER`] identity rule and
/// wait until the produce path reads it.
async fn use_header_rule(broker: &SingleBroker) {
    let outcome = alter_topic_config()
        .client(&broker.client)
        .topic(&broker.topic)
        .name("dedup.key.header")
        .value(DEDUP_HEADER)
        .call()
        .await;
    assert!(outcome == ALTERED);
    broker
        .await_topic_config("dedup.key.header", Some(DEDUP_HEADER))
        .await;
}

/// Set the topic's `dedup.window.ms` and wait until the produce path reads it.
async fn set_window(broker: &SingleBroker, window: &str) {
    let outcome = alter_topic_config()
        .client(&broker.client)
        .topic(&broker.topic)
        .name("dedup.window.ms")
        .value(window)
        .call()
        .await;
    assert!(outcome == ALTERED);
    broker
        .await_topic_config("dedup.window.ms", Some(window))
        .await;
}

#[tokio::test]
async fn re_enabling_after_a_restart_indexes_what_was_written_while_disabled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = first_boot().dir(dir.path()).topic("toggle").call().await;
    let enabled = first.produce(keyed(&["a", "b"])).await;
    set_window(&first, "-1").await;
    let disabled = first.produce(keyed(&["a", "c"])).await;
    first.shutdown().await;

    let broker = reboot(dir.path(), "toggle").await;
    let disabled_after_restart = broker.produce(keyed(&["c", "d"])).await;
    set_window(&broker, &WINDOW_MS.to_string()).await;
    let re_enabled = broker.produce(keyed(&["a", "b", "c", "d", "e"])).await;
    let counters = broker.counters().await;
    let contents = broker.read().await;

    assert!(
        [enabled, disabled, disabled_after_restart, re_enabled]
            == [
                expected_partition_success(0, 0),
                expected_partition_success(2, 0),
                expected_partition_success(4, 0),
                expected_partition_success(6, 0),
            ]
    );
    // The records written while dedup was off are in the log, and so in the
    // rebuilt index: all four keys are duplicates, only `e` is new.
    assert!(
        counters
            == DedupCounters {
                dropped: 4.0,
                index_rebuilds: 1.0
            }
    );
    assert!(
        contents.offsets_and_keys()
            == at(&[
                (0, "a"),
                (1, "b"),
                (2, "a"),
                (3, "c"),
                (4, "c"),
                (5, "d"),
                (10, "e")
            ])
    );
    assert!(contents.high_watermark == 11);
    broker.shutdown().await;
}

#[tokio::test]
async fn every_key_stays_unique_across_repeated_restarts() {
    // Each cycle stops the broker, damages what it left in a way that keeps
    // every complete batch, boots it again, and sends every key written so
    // far together with two new ones.
    let damages = [
        Damage::None,
        Damage::NoCleanShutdownProof,
        Damage::TornCopy,
        Damage::None,
        Damage::TornCopy,
    ];
    let topic = "cycles";
    let dir = tempfile::tempdir().expect("tempdir");
    let mut broker = first_boot().dir(dir.path()).topic(topic).call().await;
    let row = broker.produce(keyed(&["c0-a", "c0-b"])).await;
    assert!(row == expected_partition_success(0, 0));
    let mut written: Vec<String> = vec!["c0-a".into(), "c0-b".into()];
    let mut expected = at(&[(0, "c0-a"), (1, "c0-b")]);
    let mut next_offset = 2_i64;

    for (cycle, damage) in (1..).zip(damages) {
        broker.shutdown().await;
        damage.apply(dir.path(), topic);
        broker = reboot(dir.path(), topic).await;

        let new = [format!("c{cycle}-a"), format!("c{cycle}-b")];
        let keys: Vec<&str> = written.iter().chain(&new).map(String::as_str).collect();
        let row = broker.produce(keyed(&keys)).await;
        let counters = broker.counters().await;
        let contents = broker.read().await;

        let first_new = next_offset + i64::try_from(written.len()).expect("offset");
        expected.push((first_new, new[0].clone()));
        expected.push((first_new + 1, new[1].clone()));
        assert!(
            row == expected_partition_success(next_offset, 0),
            "cycle {cycle}, {damage:?}"
        );
        assert!(
            counters
                == DedupCounters {
                    dropped: count(written.len()),
                    index_rebuilds: 1.0
                },
            "cycle {cycle}, {damage:?}"
        );
        assert!(
            repeated_keys(&contents).is_empty(),
            "cycle {cycle}, {damage:?}"
        );
        assert!(
            contents.offsets_and_keys() == expected,
            "cycle {cycle}, {damage:?}"
        );
        next_offset += i64::try_from(keys.len()).expect("offset");
        written.extend(new);
    }
    broker.shutdown().await;
}

/// The number of distinct keys of the backlog case, and how many go in one
/// batch. With 100-byte values a batch is about 600 KiB, so the rebuild reads
/// the log in several of its 1 MiB steps.
const BACKLOG_KEYS: usize = 50_000;
const BACKLOG_BATCH: usize = 5_000;

fn backlog_batch(chunk: usize) -> krabka_protocol::records::RecordBatch {
    let padding = "v".repeat(100);
    let records: Vec<Rec> = (chunk * BACKLOG_BATCH..(chunk + 1) * BACKLOG_BATCH)
        .map(|index| {
            let key = format!("key-{index:05}");
            let value = format!("{key}-{padding}");
            Rec::key(&key).value(&value)
        })
        .collect();
    keyed_batch().records(&records).call()
}

/// Fetch partition 0 from offset 0 until `stop` is set. Returns how many
/// fetches answered without an error and the slowest of them.
async fn fetch_until(
    addr: String,
    topic: String,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    stop: Arc<AtomicBool>,
) -> (usize, Duration) {
    let client = connect_client(addr, Some("dedup-recovery-fetcher")).await;
    let mut answered = 0;
    let mut slowest = Duration::ZERO;
    while !stop.load(Ordering::Acquire) {
        let request = single_partition_fetch()
            .topic(topic.clone())
            .topic_id(topic_id)
            .partition(fetch_partition().partition(0).offset(0).call())
            .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
            .call();
        let started = Instant::now();
        let response = client.send(request).await.expect("Fetch");
        slowest = slowest.max(started.elapsed());
        let row = &response.responses[0].partitions[0];
        assert!(
            row.error_code == 0,
            "fetch during the rebuild: {}",
            row.error_code
        );
        answered += 1;
    }
    (answered, slowest)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_backlog_rebuilds_promptly_and_every_identity_is_remembered() {
    // Heavy enough to starve a cluster test of the same binary.
    let _g = cluster_lock().lock().await;
    let chunks = BACKLOG_KEYS / BACKLOG_BATCH;
    let dir = tempfile::tempdir().expect("tempdir");
    let first = first_boot().dir(dir.path()).topic("backlog").call().await;
    for chunk in 0..chunks {
        let row = first.produce(backlog_batch(chunk)).await;
        let base = i64::try_from(chunk * BACKLOG_BATCH).expect("offset");
        assert!(row == expected_partition_success(base, 0));
    }
    first.shutdown().await;

    let broker = reboot(dir.path(), "backlog").await;
    let halt = Arc::new(AtomicBool::new(false));
    let fetcher = tokio::spawn(fetch_until(
        broker.broker.listen_addr().to_string(),
        broker.topic.clone(),
        broker.topic_id,
        Arc::clone(&halt),
    ));
    // The first append after the boot rebuilds the index from all 50,000
    // records before it classifies anything.
    let started = Instant::now();
    let first_row = broker.produce(backlog_batch(0)).await;
    let rebuild_and_append = started.elapsed();
    let mut rows = Vec::new();
    for chunk in 1..chunks {
        rows.push(broker.produce(backlog_batch(chunk)).await);
    }
    halt.store(true, Ordering::Release);
    let (answered_fetches, slowest_fetch) = fetcher.await.expect("fetcher");
    let counters = broker.counters().await;
    let contents = broker.read().await;
    eprintln!(
        "backlog: the first append after the restart took {rebuild_and_append:?}; {answered_fetches} \
         concurrent fetches, the slowest {slowest_fetch:?}"
    );

    let backlog = i64::try_from(BACKLOG_KEYS).expect("offset");
    assert!(rebuild_and_append < Duration::from_secs(20));
    assert!(first_row == expected_partition_success(backlog, 0));
    let expected_rows: Vec<_> = (1..chunks)
        .map(|chunk| {
            let base = backlog + i64::try_from(chunk * BACKLOG_BATCH).expect("offset");
            expected_partition_success(base, 0)
        })
        .collect();
    assert!(rows == expected_rows);
    // Every one of the 50,000 identities was a duplicate.
    assert!(
        counters
            == DedupCounters {
                dropped: count(BACKLOG_KEYS),
                index_rebuilds: 1.0
            }
    );
    assert!(contents.records.len() == BACKLOG_KEYS);
    assert!(repeated_keys(&contents).is_empty());
    assert!(contents.high_watermark == 2 * backlog);
    assert!(answered_fetches >= 1);
    broker.shutdown().await;
}
