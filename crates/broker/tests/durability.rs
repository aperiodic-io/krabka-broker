//! Integration tests for EOS durability: the high watermark and acks=all.
//!
//! These tests are gated off Windows, like the other multi-broker tests.
//! openraft and `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol under test.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_protocol::{
    owned::{
        fetch_request::FetchRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::RecordBatch,
};
use support::{
    client::metric_value,
    durability::{
        create_topic_on_replicas as create_topic, produce_acks, produce_batch,
        record_batch_with_values,
    },
    topic_id_for,
};
use tempfile::TempDir;

use crate::support::{
    client::{BatchProduceSetup, connect_client},
    durability::{ReplicatedTopicSetup, TopicReplicasSetup, replicated_topic_fixture},
    fetch::{SinglePartitionFetchSetup, single_partition_fetch},
    produce::{ProduceAcknowledgements, ProduceTimeoutMillis},
    producer::{ProducerRecordSetup, producer_record, transactional_producer},
    registered_cluster, shutdown_cluster,
    topics::{
        CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor, configured_topic_request,
    },
};

mod support;

pub use crate::support::boot_single;

/// An idempotent batch with an explicit `(producer_id, base_sequence)`, so
/// that a test can replay a "retry" deterministically by sending the same
/// batch again.
fn idempotent_batch(pid: i64, base_seq: i32, values: &[&str]) -> RecordBatch {
    let mut b = record_batch_with_values(values);
    b.producer_id = pid;
    b.producer_epoch = 0;
    b.base_sequence = base_seq;
    b
}

/// Bug D regression. A failover-rejoin divergence can TRUNCATE an idempotent
/// batch off the leader's log, which also reverts the producer state. A retry
/// of that same batch must then RE-APPEND. It must not deduplicate against the
/// now-truncated offset and stall its `acks=all` high-watermark gate forever.
///
/// Without the producer-state revert on truncation, the retry resolves to
/// `Decision::Duplicate{base_offset}` and waits for `HW >= base_offset + N`.
/// The truncated log can never reach that, so every attempt returns
/// `REQUEST_TIMED_OUT`, which is a permanent stall on the
/// cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_retry_reappends_after_truncation_instead_of_stalling() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "trunc",
            ..Default::default()
        },
    )
    .await;

    let base = produce_batch(
        &bootstrap,
        idempotent_batch(42, 0, &["a", "b", "c"]),
        BatchProduceSetup {
            topic: "trunc",
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            ..Default::default()
        },
    )
    .await
    .expect("first idempotent produce succeeds");

    // Drop the just-appended batch off the leader's log (a divergence
    // truncation also reverts the dedup state, like the replicator does).
    broker
        .test_truncate_local_log("trunc", 0, base)
        .await
        .expect("truncate local log");

    // The retry of the SAME idempotent batch must re-append and complete fast,
    // not stall. A short timeout makes a regression (dedup-against-truncated
    // stall) fail as Err(REQUEST_TIMED_OUT) rather than hang.
    let retry = produce_batch(
        &bootstrap,
        idempotent_batch(42, 0, &["a", "b", "c"]),
        BatchProduceSetup {
            topic: "trunc",
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            timeout: ProduceTimeoutMillis(3_000),
            ..Default::default()
        },
    )
    .await;
    assert!(
        retry.is_ok(),
        "idempotent retry after truncation must re-append (not dedup-stall); got {retry:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_backed_restart_recovers_idempotent_producer_state() {
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "restart-dedup",
            ..Default::default()
        },
    )
    .await;
    let produce_setup = BatchProduceSetup {
        topic: "restart-dedup",
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        ..Default::default()
    };
    let first = produce_batch(&bootstrap, idempotent_batch(42, 0, &["a"]), produce_setup)
        .await
        .expect("initial idempotent produce");
    assert!(first == 0);
    broker.shutdown().await;

    let mut config = BrokerConfig::for_tests(log_dir);
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    broker
        .wait_until_local_partition_leader(
            "restart-dedup",
            0,
            krabka_broker::NodeId(broker.node_id()),
        )
        .await;

    let duplicate = produce_batch(&bootstrap, idempotent_batch(42, 0, &["a"]), produce_setup)
        .await
        .expect("idempotent retry after restart");
    assert!(duplicate == 0, "retry must resolve to the original offset");

    let next = produce_batch(&bootstrap, idempotent_batch(42, 1, &["b"]), produce_setup)
        .await
        .expect("next sequence after restart");
    assert!(next == 1);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_one_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "ack1",
            ..Default::default()
        },
    )
    .await;
    let start = Instant::now();
    let offset = produce_acks(
        &bootstrap,
        &["a", "b", "c"],
        BatchProduceSetup {
            topic: "ack1",
            ..Default::default()
        },
    )
    .await
    .expect("ack=1 success");
    let elapsed = start.elapsed();
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "ackall",
            ..Default::default()
        },
    )
    .await;
    let start = Instant::now();
    let offset = produce_acks(
        &bootstrap,
        &["a", "b", "c"],
        BatchProduceSetup {
            topic: "ackall",
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            ..Default::default()
        },
    )
    .await
    .expect("ack=-1 success");
    let elapsed = start.elapsed();
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=-1 on rf=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_clamps_at_hw_when_followers_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "clamp",
            ..Default::default()
        },
    )
    .await;

    let offset = produce_acks(
        &bootstrap,
        &["x", "y", "z"],
        BatchProduceSetup {
            topic: "clamp",
            ..Default::default()
        },
    )
    .await
    .expect("produce ok");
    assert!(offset == 0);
    // The writer sends the acks=1 response after the local append and then
    // advances the rf=1 high watermark asynchronously. Wait on the actual HW
    // transition before asserting the Fetch response so this regression test
    // does not race that post-ack recompute.
    broker.wait_until_high_watermark("clamp", 0, 3).await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "clamp").await;
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            ..single_partition_fetch(SinglePartitionFetchSetup {
                topic: "clamp".into(),
                topic_id,
                ..Default::default()
            })
        })
        .await
        .expect("Fetch");
    let pd = &resp.responses[0].partitions[0];
    assert!(pd.error_code == 0);
    assert!(pd.high_watermark == 3, "HW should equal LEO for rf=1");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Regression-pin for the `min(HW, lso)` behavior, which is equivalent to
// plain `lso` for rf=1 (HW = LEO immediately). Previously flaked with the
// `INVALID_TXN_STATE` race fixed in `Producer::flush` (it now waits for
// in-flight Produce batches before `EndTxn`).
async fn read_committed_under_rf1_unchanged() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(
        &broker,
        &bootstrap,
        TopicReplicasSetup {
            topic: "rctxn",
            ..Default::default()
        },
    )
    .await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    broker.wait_until_transaction_coordinator_ready().await;

    let producer = transactional_producer(bootstrap.clone(), "rc-tid").await;
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["p", "q", "r"] {
        drop(
            producer
                .enqueue(producer_record(ProducerRecordSetup {
                    topic: ("rctxn").into(),
                    value: Some(Bytes::from(v.to_string())),
                    ..Default::default()
                }))
                .await
                .expect("record is queued"),
        );
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("rc-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["rctxn".to_string()])
        .build()
        .await
        .unwrap();

    // Await the records committed to the local log before polling the consumer
    // so the consumer poll returns promptly rather than racing the commit.
    broker.wait_until_local_log_end_offset("rctxn", 0, 3).await;

    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        for r in consumer.poll(krabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["p", "q", "r"]);
    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
    support::init_tracing();
    let (mut cluster, bootstrap_1) = replicated_topic_fixture(ReplicatedTopicSetup {
        topic: "shrink",
        ..Default::default()
    })
    .await;
    // Wait for all 3 replicas to join the ISR before killing broker 3
    // so the scenario genuinely exercises ISR shrink rather than racing the
    // initial ISR population.
    cluster[0].0.wait_until_isr_len("shrink", 0, 3).await;

    // Kill broker 3 — its absence forces ISR to shrink within
    // replica_lag_time_max (2s on CI), unblocking the acks=-1 produce.
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    let start = Instant::now();
    let offset = produce_acks(
        &bootstrap_1,
        &["x", "y", "z"],
        BatchProduceSetup {
            topic: "shrink",
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            timeout: ProduceTimeoutMillis(10_000),
            ..Default::default()
        },
    )
    .await
    .expect("acks=-1 success after shrink");
    let elapsed = start.elapsed();
    check!(offset == 0);
    check!(
        elapsed >= Duration::from_millis(1_500),
        "expected to wait for ISR shrink (~2s); took {elapsed:?}"
    );
    check!(
        elapsed < Duration::from_secs(5),
        "shrink + completion should be well under 5s; took {elapsed:?}"
    );
    shutdown_cluster(cluster).await;
}

/// A produce that waits on a stalled follower must land in the remote-time
/// histogram, not the local-time one.
///
/// This is the metric that separates a lagging follower from a slow disk. The
/// scenario is the one `acks_all_completes_via_isr_shrink_when_follower_dead`
/// drives: three brokers, all three in the ISR, then one is killed, so the
/// `acks=all` high-watermark gate waits out `replica_lag_time_max` before the
/// ISR shrinks and the produce completes. The leader's own append is a
/// millisecond of local disk, and the seconds the client waited are all in the
/// high-watermark gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_stalled_follower_wait_lands_in_remote_time_not_local_time() {
    support::init_tracing();
    let (mut cluster, bootstrap_1) = replicated_topic_fixture(ReplicatedTopicSetup {
        topic: "phase-stall",
        ..Default::default()
    })
    .await;
    cluster[0].0.wait_until_isr_len("phase-stall", 0, 3).await;

    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    let offset = produce_acks(
        &bootstrap_1,
        &["x", "y", "z"],
        BatchProduceSetup {
            topic: "phase-stall",
            acknowledgements: ProduceAcknowledgements::AllReplicas,
            timeout: ProduceTimeoutMillis(10_000),
            ..Default::default()
        },
    )
    .await
    .expect("acks=-1 success after shrink");
    check!(offset == 0);

    // The produce went to the first broker, so its registry is the one that
    // holds this request's phases.
    let leader = &cluster[0].0;
    let local = metric_value(
        leader,
        "krabka_broker_request_local_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;
    let remote = metric_value(
        leader,
        "krabka_broker_request_remote_duration_seconds_sum{api_key=\"Produce\"}",
    )
    .await;

    // The ISR shrink takes replica_lag_time_max (2s on CI). Assert well under
    // it so a fast shrink does not fail the test, and well over any plausible
    // local append.
    check!(
        remote >= 0.5,
        "the acks=all gate waited for the dead follower, so remote time must \
         have moved; local={local} remote={remote}"
    );
    // The local append is a write to a tmpfs-backed log: milliseconds, and
    // orders of magnitude below the gate. Comparing the two rather than
    // pinning an absolute bound keeps the assertion honest on a slow runner.
    check!(
        local * 4.0 < remote,
        "the wait was on the follower, not on this broker's log; \
         local={local} remote={remote}"
    );

    shutdown_cluster(cluster).await;
}

/// Partitions the overlapped-wait case produces to in one request. Wide
/// enough that N sequential `timeout.ms` waits and one overlapped wait are
/// far apart, and small enough to create and lead on a two-broker cluster.
const OVERLAP_PARTITIONS: i32 = 8;

/// The `timeout.ms` the overlapped-wait case sends. One of these bounds the
/// whole request; one per waiting partition would bound nothing useful.
const OVERLAP_TIMEOUT_MS: i32 = 1_000;

async fn create_topic_with_partitions(
    broker: &BrokerHandle,
    bootstrap: &str,
    setup: CreateTopicSetup<'_>,
) {
    let client = connect_client(bootstrap.to_string(), None).await;
    let resp = client
        .send(configured_topic_request(setup))
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    for index in 0..setup.num_partitions.0 {
        broker
            .wait_until_partition_present(setup.topic, index)
            .await;
    }
}

/// One Produce carrying every partition of `topic`, returning each row's
/// `(index, error_code)`.
async fn produce_every_partition(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    acks: i16,
    timeout_ms: i32,
) -> Vec<(i32, i16)> {
    let client = connect_client(bootstrap.to_string(), None).await;
    let topic_id = topic_id_for(&client, topic).await;
    let resp = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: (0..partitions)
                    .map(|index| PartitionProduceData {
                        index,
                        records: Some(record_batch_with_values(&["a"]).into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    resp.responses[0]
        .partition_responses
        .iter()
        .map(|row| (row.index, row.error_code))
        .collect()
}

/// One `acks=-1` request to many partitions waits for the high watermark once,
/// not once per partition.
///
/// Kafka appends every partition of a request to the local log and then
/// registers a single `DelayedProduce` covering all of them with the request's
/// `timeout.ms`, so the replication waits overlap and one timeout bounds the
/// request. A handler that instead awaits each partition's watermark inline,
/// before the next partition is appended, turns one round trip into N and one
/// timeout into N of them.
///
/// The scenario holds the follower's fetch back for good: the cluster is two
/// brokers, and the second one is stopped. Its replicas can never acknowledge,
/// and the surviving broker cannot shrink the ISR either, because a two-voter
/// quorum is gone with it. Every partition the survivor leads therefore waits
/// out the full `timeout.ms` and answers `REQUEST_TIMED_OUT`,
/// which is exactly the case where the difference between one wait and N of
/// them is a wall-clock fact rather than a scheduling detail.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_waits_for_every_partition_at_once_not_one_after_another() {
    support::init_tracing();
    let mut cluster = registered_cluster(2).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic_with_partitions(
        &cluster[0].0,
        &bootstrap_1,
        CreateTopicSetup {
            topic: "overlap",
            num_partitions: TopicPartitionCount(OVERLAP_PARTITIONS),
            replication_factor: TopicReplicationFactor(2),
            ..Default::default()
        },
    )
    .await;
    // Both replicas in the ISR before the follower goes away, so the wait
    // below is a follower that stopped acknowledging and not one that never
    // started.
    for index in 0..OVERLAP_PARTITIONS {
        cluster[0].0.wait_until_isr_len("overlap", index, 2).await;
    }

    let follower = cluster.pop().expect("2nd broker");
    follower.0.shutdown().await;

    let start = Instant::now();
    let rows = produce_every_partition(
        &bootstrap_1,
        "overlap",
        OVERLAP_PARTITIONS,
        -1,
        OVERLAP_TIMEOUT_MS,
    )
    .await;
    let elapsed = start.elapsed();

    // The partitions the surviving broker leads are the ones that appended and
    // then waited; the rest answer NOT_LEADER_OR_FOLLOWER without waiting at
    // all. Half the partitions is the round-robin assignment's answer, and the
    // assertion needs only that several of them waited.
    let waited = rows
        .iter()
        // `REQUEST_TIMED_OUT` (7): the append is on the leader's log and the
        // high watermark never reached it.
        .filter(|(_, code)| *code == 7)
        .count();
    check!(i32::try_from(rows.len()) == Ok(OVERLAP_PARTITIONS));
    check!(
        waited >= 2,
        "the surviving broker must lead several partitions for this to measure anything; rows={rows:?}"
    );

    let one_wait = Duration::from_millis(u64::try_from(OVERLAP_TIMEOUT_MS).unwrap());
    check!(
        elapsed >= one_wait,
        "the gate must actually have waited out the request timeout; took {elapsed:?}"
    );
    // Sequential waits would cost `waited` timeouts, so at least two here and
    // typically four. Two of them is the bound that separates the two shapes
    // while leaving room for the appends and the client round trip.
    check!(
        elapsed < one_wait * 2,
        "the {waited} acks=-1 waits must overlap: sequential would take about \
         {waited} x {OVERLAP_TIMEOUT_MS}ms, this took {elapsed:?}"
    );

    shutdown_cluster(cluster).await;
}
