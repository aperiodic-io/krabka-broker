//! Three brokers, one replicated partition, and leadership that moves.
//!
//! A follower indexes every batch it replicates with its leader's rule, so a
//! promoted follower deduplicates its first produce without reading the log
//! again. A replica that truncates a suffix its leader never committed must
//! forget the identities of that suffix. And a producer that retries through
//! a rolling restart must find each of its records in the log exactly once.

use {
    crate::{
        harness::{
            DedupCounters, Node, at, await_local_leader, await_new_leader, connect, dedup_cluster,
            dedup_counters, keyed, leader_of, position, produce_on, read_partition_on,
            repeated_keys, restart, shutdown_all, stop, wait_for_partition,
        },
        support::{
            admin::election_request,
            cluster_lock,
            dedup::StoredBatch,
            discovery::named_topic_metadata,
            produce::{
                ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
                batch_request, expected_partition_success,
            },
        },
    },
    assert2::assert,
    krabka_broker::NodeId,
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_protocol::{primitives::uuid::Uuid, records::RecordBatch},
    std::{
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    },
};

/// Traffic on broker 1 before it stops, and the produce after the failover.
struct FailoverCase {
    topic: &'static str,
    /// Each batch produced to broker 1 with `acks=all`, and its base offset.
    before: &'static [(&'static [&'static str], i64)],
    /// The batches the promoted follower holds, as a consumer reads them.
    replicated: Vec<StoredBatch>,
    after: &'static [&'static str],
    after_base: i64,
    /// Records the promoted follower drops from `after`.
    dropped: f64,
    expected: Vec<(i64, String)>,
}

/// Run `case`: produce to broker 1, wait until both followers hold and have
/// committed every batch, stop broker 1, and produce to the new leader.
async fn fail_over(case: FailoverCase) {
    let _g = cluster_lock().lock().await;
    let topic = case.topic;
    let (mut cluster, topic_id) = dedup_cluster(topic).await;
    let old_leader = connect(cluster[0].1.listen_addr).await;
    for (keys, base) in case.before {
        let row = produce_on(
            &old_leader,
            topic,
            topic_id,
            keyed(keys),
            ProduceAcknowledgements::AllReplicas,
        )
        .await
        .expect("Produce");
        assert!(row == expected_partition_success(*base, 0), "{keys:?}");
    }
    let end = case.replicated.last().expect("a batch").last_offset + 1;
    for (broker, _, _) in &cluster {
        broker.wait_until_local_log_end_offset(topic, 0, end).await;
        broker.wait_until_high_watermark(topic, 0, end).await;
    }
    drop(old_leader);
    let (_config, _dir) = stop(&mut cluster, 1).await;
    let leader = await_new_leader(&cluster, topic, 1).await;
    let new_leader = &cluster[position(&cluster, leader)];
    let conn = connect(new_leader.1.listen_addr).await;

    let replicated = read_partition_on(new_leader.1.listen_addr, topic, topic_id)
        .await
        .batches;
    let promoted = dedup_counters(&new_leader.0, topic).await;
    let row = produce_on(
        &conn,
        topic,
        topic_id,
        keyed(case.after),
        ProduceAcknowledgements::AllReplicas,
    )
    .await
    .expect("Produce");
    let counters = dedup_counters(&new_leader.0, topic).await;
    let contents = read_partition_on(new_leader.1.listen_addr, topic, topic_id).await;
    eprintln!(
        "{topic}: broker {leader} took over; it had rebuilt its index {} times as a follower",
        promoted.index_rebuilds
    );

    assert!(replicated == case.replicated);
    assert!(row == expected_partition_success(case.after_base, 0));
    // The promoted follower's index was current: the first produce dropped
    // the duplicates without a rebuild from the log.
    assert!(
        counters
            == DedupCounters {
                dropped: case.dropped,
                index_rebuilds: promoted.index_rebuilds,
            }
    );
    assert!(repeated_keys(&contents).is_empty());
    assert!(contents.offsets_and_keys() == case.expected);
    shutdown_all(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_follower_deduplicates_its_first_produce_without_a_rebuild() {
    fail_over(FailoverCase {
        topic: "failover",
        before: &[(&["a", "b", "c"], 0)],
        replicated: vec![StoredBatch {
            base_offset: 0,
            last_offset: 2,
            records: 3,
        }],
        after: &["a", "d", "b"],
        after_base: 3,
        dropped: 2.0,
        expected: at(&[(0, "a"), (1, "b"), (2, "c"), (4, "d")]),
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_index_partially_dropped_and_empty_batches_they_replicate() {
    // The second batch loses `a` and `b` and is re-encoded with offset gaps,
    // the third loses everything and is an empty batch. The followers
    // replicate both shapes and index the records that remain in them.
    fail_over(FailoverCase {
        topic: "failover-shapes",
        before: &[
            (&["a", "b"], 0),
            (&["a", "c", "b", "d"], 2),
            (&["c", "d"], 6),
        ],
        replicated: vec![
            StoredBatch {
                base_offset: 0,
                last_offset: 1,
                records: 2,
            },
            StoredBatch {
                base_offset: 2,
                last_offset: 5,
                records: 2,
            },
            StoredBatch {
                base_offset: 6,
                last_offset: 7,
                records: 0,
            },
        ],
        after: &["c", "d", "a", "b", "e"],
        after_base: 8,
        dropped: 4.0,
        expected: at(&[(0, "a"), (1, "b"), (3, "c"), (5, "d"), (12, "e")]),
    })
    .await;
}

/// Every key of partition 0 of `topic` on broker `node`'s local log, in
/// offset order, read under the log lock rather than through Fetch, so it
/// includes records above the high watermark.
fn local_keys(cluster: &[Node], node: u64, topic: &str) -> Vec<String> {
    cluster[position(cluster, node)]
        .0
        .local_batches_for_test(topic, 0)
        .expect("the partition is local")
        .iter()
        .flat_map(|batch| &batch.records)
        .map(|record| {
            record.key.as_deref().map_or_else(
                || "<null>".to_owned(),
                |key| String::from_utf8_lossy(key).into(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_divergent_suffix_that_was_truncated_is_forgotten() {
    // Broker 1 leads alone and takes `x` and `y` with `acks=1` while both
    // followers are down, so no other replica ever holds them. The followers
    // come back without it and elect broker 2, which takes `d` at the offset
    // `x` had. When broker 1 returns as a follower it truncates `x y` and
    // replicates `d`. Made leader again, it must admit `x` and `y` as new
    // records and still drop `a` and `d`.
    let _g = cluster_lock().lock().await;
    let topic = "diverge";
    let (mut cluster, topic_id) = dedup_cluster(topic).await;
    let first = connect(cluster[0].1.listen_addr).await;
    let committed = produce_on(
        &first,
        topic,
        topic_id,
        keyed(&["a", "b"]),
        ProduceAcknowledgements::AllReplicas,
    )
    .await
    .expect("Produce");
    for (broker, _, _) in &cluster {
        broker.wait_until_high_watermark(topic, 0, 2).await;
    }

    let (config_3, dir_3) = stop(&mut cluster, 3).await;
    wait_for_partition(&cluster[0].0, topic, |partition| {
        partition.isr.len() == 2 && !partition.isr.contains(&NodeId(3))
    })
    .await;
    let (config_2, dir_2) = stop(&mut cluster, 2).await;
    let divergent = produce_on(
        &first,
        topic,
        topic_id,
        keyed(&["x", "y"]),
        ProduceAcknowledgements::Leader,
    )
    .await
    .expect("Produce");
    let divergent_log = local_keys(&cluster, 1, topic);
    drop(first);
    let (config_1, dir_1) = stop(&mut cluster, 1).await;

    let (node_2, node_3) = tokio::join!(restart(config_2, dir_2), restart(config_3, dir_3));
    cluster.extend([node_2, node_3]);
    await_local_leader(&cluster, topic, 2).await;
    wait_for_partition(&cluster[0].0, topic, |partition| {
        partition.isr.len() == 2 && !partition.isr.contains(&NodeId(1))
    })
    .await;
    let second = connect(cluster[position(&cluster, 2)].1.listen_addr).await;
    let on_new_leader = produce_on(
        &second,
        topic,
        topic_id,
        keyed(&["d"]),
        ProduceAcknowledgements::AllReplicas,
    )
    .await
    .expect("Produce");
    drop(second);

    cluster.push(restart(config_1, dir_1).await);
    wait_for_partition(&cluster[0].0, topic, |partition| partition.isr.len() == 3).await;
    cluster[position(&cluster, 1)]
        .0
        .wait_until_local_log_end_offset_eq(topic, 0, 3)
        .await;
    let truncated_log = local_keys(&cluster, 1, topic);
    // Preferred election puts the first replica, broker 1, back in charge.
    let election = connect(cluster[0].1.listen_addr).await;
    election
        .send(election_request().topic(topic.to_owned()).call())
        .await
        .expect("ElectLeaders");
    await_local_leader(&cluster, topic, 1).await;
    let back = connect(cluster[position(&cluster, 1)].1.listen_addr).await;
    let row = produce_on(
        &back,
        topic,
        topic_id,
        keyed(&["x", "y", "a", "d", "z"]),
        ProduceAcknowledgements::AllReplicas,
    )
    .await
    .expect("Produce");
    let contents = read_partition_on(
        cluster[position(&cluster, 1)].1.listen_addr,
        topic,
        topic_id,
    )
    .await;

    assert!(
        [committed, divergent, on_new_leader]
            == [
                expected_partition_success(0, 0),
                expected_partition_success(2, 0),
                expected_partition_success(2, 0),
            ]
    );
    assert!(divergent_log == ["a", "b", "x", "y"]);
    assert!(truncated_log == ["a", "b", "d"]);
    assert!(row == expected_partition_success(3, 0));
    assert!(
        contents.offsets_and_keys()
            == at(&[(0, "a"), (1, "b"), (2, "d"), (3, "x"), (4, "y"), (7, "z")])
    );
    shutdown_all(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn followers_that_truncate_in_band_forget_the_suffix_they_lost() {
    // Every replica holds and has indexed `c` at offset 2 when the leader's
    // log is cut back to 2. The running followers learn of it from the
    // leader's diverging-epoch answer and truncate in band, with an index
    // that was current until then. The follower promoted next must admit `c`
    // as a new record.
    let _g = cluster_lock().lock().await;
    let topic = "truncate-in-band";
    let (mut cluster, topic_id) = dedup_cluster(topic).await;
    let first = connect(cluster[0].1.listen_addr).await;
    let mut rows = Vec::new();
    for keys in [&["a", "b"][..], &["c"]] {
        rows.push(
            produce_on(
                &first,
                topic,
                topic_id,
                keyed(keys),
                ProduceAcknowledgements::AllReplicas,
            )
            .await
            .expect("Produce"),
        );
    }
    for (broker, _, _) in &cluster {
        broker.wait_until_high_watermark(topic, 0, 3).await;
    }
    drop(first);
    cluster[0]
        .0
        .test_truncate_local_log(topic, 0, 2)
        .await
        .expect("truncate the leader");
    for node in [2, 3] {
        cluster[position(&cluster, node)]
            .0
            .wait_until_local_log_end_offset_eq(topic, 0, 2)
            .await;
    }
    let (_config, _dir) = stop(&mut cluster, 1).await;
    let leader = await_new_leader(&cluster, topic, 1).await;
    let conn = connect(cluster[position(&cluster, leader)].1.listen_addr).await;
    let row = produce_on(
        &conn,
        topic,
        topic_id,
        keyed(&["c", "a", "d"]),
        ProduceAcknowledgements::AllReplicas,
    )
    .await
    .expect("Produce");
    let contents = read_partition_on(
        cluster[position(&cluster, leader)].1.listen_addr,
        topic,
        topic_id,
    )
    .await;

    assert!(
        rows == [
            expected_partition_success(0, 0),
            expected_partition_success(2, 0)
        ]
    );
    assert!(row == expected_partition_success(2, 0));
    assert!(contents.offsets_and_keys() == at(&[(0, "a"), (1, "b"), (2, "c"), (4, "d")]));
    shutdown_all(cluster).await;
}

// ---------------------------------------------------------------------------
// Rolling restart under a retrying producer
// ---------------------------------------------------------------------------

/// A producer that sends two new keys per batch with `acks=all` and retries a
/// batch, unchanged, until a leader acknowledges it: at-least-once delivery,
/// which dedup turns into exactly-once in the log.
///
/// After each acknowledgement it also delivers again the batch it sent
/// [`REPLAY_LAG`] batches earlier, as an upstream system that replays from an
/// older position does. That copy reaches whichever broker leads by then,
/// usually after a leadership change, and must always be dropped.
struct RetryingProducer {
    brokers: Vec<(u64, SocketAddr)>,
    topic: String,
    topic_id: Uuid,
    acked: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

/// What a [`RetryingProducer`] delivered.
struct Delivered {
    /// Every acknowledged key, in send order, each once.
    keys: Vec<String>,
    /// How many batches it delivered a second time.
    replays: usize,
}

/// A bound on one network step, so a request to a broker that is stopping
/// fails and is retried instead of hanging.
const STEP: Duration = Duration::from_secs(10);

/// How many batches back the replayed batch is.
const REPLAY_LAG: usize = 10;

impl RetryingProducer {
    /// Produce until `stop` is set.
    async fn run(self) -> Delivered {
        let mut keys = Vec::new();
        let mut sent: Vec<RecordBatch> = Vec::new();
        while !self.stop.load(Ordering::Acquire) {
            let batch = sent.len();
            let pair = [format!("r{batch:04}-a"), format!("r{batch:04}-b")];
            let records = keyed(&[pair[0].as_str(), pair[1].as_str()]);
            self.deliver(&records, "batch", batch).await;
            sent.push(records);
            keys.extend(pair);
            if let Some(replayed) = batch.checked_sub(REPLAY_LAG) {
                self.deliver(&sent[replayed], "replay of batch", replayed)
                    .await;
            }
            self.acked.fetch_add(1, Ordering::AcqRel);
        }
        Delivered {
            keys,
            replays: sent.len().saturating_sub(REPLAY_LAG),
        }
    }

    /// Send `records` until a leader acknowledges them.
    async fn deliver(&self, records: &RecordBatch, what: &str, batch: usize) {
        let deadline = Instant::now() + Duration::from_secs(90);
        while let Err(why) = self.attempt(records.clone()).await {
            assert!(
                Instant::now() < deadline,
                "{what} {batch} was never acknowledged: {why}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn attempt(&self, batch: RecordBatch) -> Result<(), String> {
        let leader = self.leader().await?;
        let conn = dial(leader).await?;
        let request = batch_request(batch, SinglePartitionProduceSetup {
            topic: self.topic.clone(),
            topic_id: self.topic_id,
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            timeout: ProduceTimeoutMillis(5_000),
            ..SinglePartitionProduceSetup::default()
        });
        let response = tokio::time::timeout(STEP, conn.send(request))
            .await
            .map_err(|_| "produce timed out".to_owned())?
            .map_err(|error| error.to_string())?;
        match response.responses[0].partition_responses[0].error_code {
            0 => Ok(()),
            code => Err(format!("produce error {code}")),
        }
    }

    /// The address of the partition leader, from the first broker that
    /// answers a Metadata request.
    async fn leader(&self) -> Result<SocketAddr, String> {
        for (_, addr) in &self.brokers {
            let Ok(conn) = dial(*addr).await else {
                continue;
            };
            let Ok(Ok(response)) =
                tokio::time::timeout(STEP, conn.send(named_topic_metadata(self.topic.clone())))
                    .await
            else {
                continue;
            };
            let leader = response
                .topics
                .iter()
                .find(|row| row.name.as_deref() == Some(self.topic.as_str()))
                .and_then(|row| row.partitions.first())
                .map(|partition| partition.leader_id);
            if let Some(found) = leader
                .and_then(|id| u64::try_from(id).ok())
                .and_then(|id| self.brokers.iter().find(|(node, _)| *node == id))
            {
                return Ok(found.1);
            }
        }
        Err("no broker named a leader".to_owned())
    }
}

async fn dial(addr: SocketAddr) -> Result<Connection, String> {
    let options = ConnectionOptions {
        client_id: "dedup-recovery-producer".into(),
        ..Default::default()
    };
    tokio::time::timeout(STEP, Connection::connect(addr, options))
        .await
        .map_err(|_| format!("connect to {addr} timed out"))?
        .map_err(|error| error.to_string())
}

/// Wait until the producer acknowledged `more` batches beyond `from`.
async fn await_progress(acked: &AtomicUsize, from: usize, more: usize, during: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let now = acked.load(Ordering::Acquire);
        if now >= from + more {
            return now;
        }
        assert!(
            Instant::now() < deadline,
            "the producer stalled {during}: {now} batches"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_restart_under_a_retrying_producer_leaves_every_key_once() {
    let _g = cluster_lock().lock().await;
    let topic = "rolling";
    let (mut cluster, topic_id) = dedup_cluster(topic).await;
    let acked = Arc::new(AtomicUsize::new(0));
    let halt = Arc::new(AtomicBool::new(false));
    let producer = tokio::spawn(
        RetryingProducer {
            brokers: cluster
                .iter()
                .map(|(_, config, _)| (config.node_id.0, config.listen_addr))
                .collect(),
            topic: topic.to_owned(),
            topic_id,
            acked: Arc::clone(&acked),
            stop: Arc::clone(&halt),
        }
        .run(),
    );
    let mut progress = await_progress(&acked, 0, 5, "before the restarts").await;

    for node in [1, 2, 3] {
        let (config, dir) = stop(&mut cluster, node).await;
        progress = await_progress(
            &acked,
            progress,
            3,
            &format!("while broker {node} was down"),
        )
        .await;
        cluster.push(restart(config, dir).await);
        wait_for_partition(&cluster[0].0, topic, |partition| partition.isr.len() == 3).await;
        progress = await_progress(
            &acked,
            progress,
            3,
            &format!("after broker {node} returned"),
        )
        .await;
    }
    halt.store(true, Ordering::Release);
    let Delivered { keys, replays } = producer.await.expect("producer");

    let leader = leader_of(&cluster[0].0, topic);
    let contents = read_partition_on(
        cluster[position(&cluster, leader)].1.listen_addr,
        topic,
        topic_id,
    )
    .await;
    let mut dropped = 0.0;
    for (broker, _, _) in &cluster {
        dropped += dedup_counters(broker, topic).await.dropped;
    }
    eprintln!(
        "rolling: {} keys acknowledged, {replays} batches replayed; the brokers that are up \
         now dropped {dropped} records since they last started",
        keys.len()
    );

    // Each replay was dropped whole, which leaves an empty batch; a retry of
    // an append whose answer was lost can leave one more.
    let empty_batches = contents
        .batches
        .iter()
        .filter(|batch| batch.records == 0)
        .count();
    assert!(replays > 0);
    assert!(empty_batches >= replays);
    assert!(repeated_keys(&contents).is_empty());
    assert!(contents.keys() == keys);
    shutdown_all(cluster).await;
}
