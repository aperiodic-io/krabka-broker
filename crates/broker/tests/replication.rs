//! Spins up a 3-broker cluster on loopback, creates a topic with
//! replication-factor 3, produces records to the leader, and asserts
//! every follower's local log converges to the leader's
//! `log_end_offset`. It exercises the full replication path: the
//! supervisor reconcile, the follower Fetch loop, and
//! `Partition::replicate_batch`.

// Test-file pragmatism: casts turn 1-based `i` into broker ids.
// Hoisting these into named helpers would obscure the per-test narrative.

use {
    crate::support::{
        client::{ValueBatchSetup, ValueRecordCount, connect_client},
        offsets::{delete_records_partition, delete_records_request, delete_records_topic},
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        registered_cluster, shutdown_cluster,
        topics::{
            AssignedTopicSetup, CreateTopicSetup, TopicPartitionCount, TopicReplicationFactor,
            create_topic_request,
        },
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_protocol::{
        owned::create_topics_request::{CreatableTopic, CreatableTopicConfig},
        records::{Record, RecordBatch},
    },
    support::{client::scrape_metrics as scrape, cluster_lock},
    tempfile::TempDir,
};

mod support;

// Cargo compiles this file as its own test binary, so `#[path]` re-bases the
// part onto the sibling `replication/` directory, where it does not become a
// test binary of its own.
#[path = "replication/compaction_replicas.rs"]
mod compaction_replicas;
#[path = "replication/describe_producers_replicas.rs"]
mod describe_producers_replicas;
#[path = "replication/idempotent_failover.rs"]
mod idempotent_failover;

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

async fn create_replicated_partition(
    cluster: &Cluster,
    topic: CreatableTopic,
) -> (String, krabka_protocol::primitives::uuid::Uuid) {
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = connect_client(leader_addr.clone(), None).await;
    let name = topic.name.clone();
    let response = admin.send(create_topic_request(topic)).await.unwrap();
    assert!(response.topics[0].error_code == 0);
    // Produce v13 identifies topics by the CreateTopics-assigned UUID (KIP-516).
    let topic_id = response.topics[0].topic_id;
    for (handle, _, _) in cluster {
        handle.wait_until_partition_present(&name, 0).await;
    }
    (leader_addr, topic_id)
}

async fn metrics_cluster(topic: &str) -> (Cluster, std::net::SocketAddr) {
    let cluster = support::start_n_node_with(3, |_, config| {
        config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    })
    .await
    .expect("3-broker cluster");
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let admin = connect_client(cluster[0].1.listen_addr.to_string(), None).await;
    support::client::create_topic_with(&admin, CreateTopicSetup {
        topic,
        num_partitions: TopicPartitionCount(12),
        replication_factor: TopicReplicationFactor(3),
        ..Default::default()
    })
    .await;
    for (handle, _, _) in &cluster {
        for partition in 0..12 {
            handle.wait_until_partition_present(topic, partition).await;
        }
    }
    let metrics_addr = cluster[0]
        .0
        .metrics_addr()
        .expect("metrics server should be bound");
    (cluster, metrics_addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers() {
    let _g = cluster_lock().lock().await;
    let cluster = registered_cluster(3).await;
    let (leader_addr, topic_id) =
        create_replicated_partition(&cluster, support::topic_on("repl", &[&[1, 2, 3]])).await;

    let producer = connect_client(leader_addr, None).await;
    let batch = support::client::value_batch(ValueBatchSetup {
        records: ValueRecordCount(20),
    });
    produce_replicated(&producer, "repl", topic_id, batch).await;

    // Wait until every broker's local log shows log_end_offset >= 20.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("repl", 0, 20).await;
    }

    shutdown_cluster(cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_truncates_and_recovers() {
    let _g = cluster_lock().lock().await;
    let cluster = registered_cluster(3).await;
    let (leader_addr, topic_id) =
        create_replicated_partition(&cluster, support::topic_on("oor", &[&[1, 2, 3]])).await;

    // Produce 50 records in 50 separate single-record batches so the
    // leader's log holds them as discrete batches. A single 50-record
    // batch won't do, because `Fetch` returns the whole batch as the
    // smallest unit, so after advancing leader's `log_start` to 25 the
    // follower would still pull a batch with `base_offset=0` and reject
    // it with `OffsetMismatch`. Per-record batches let
    // `Segment::read(25, ...)` filter out the prefix cleanly.
    let producer = connect_client(leader_addr, None).await;
    for i in 0..50i32 {
        let batch = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            ..batch_from_records(vec![value_record(
                0,
                Some(bytes::Bytes::from(format!("v{i}"))),
            )])
        };
        produce_replicated(&producer, "oor", topic_id, batch).await;
    }

    // Wait for every broker's local log to catch up to 50.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("oor", 0, 50).await;
    }

    // Simulate broker 3 "falling behind past retention": truncate its
    // local log to 0 AND advance the leader's `log_start` to 25. After
    // this, broker 3's replicator will fetch at offset 0, leader will
    // return OFFSET_OUT_OF_RANGE with `log_start_offset=25`, and the
    // replicator's recovery path must `reset_to(25)` and re-fetch from
    // 25 to converge again.
    cluster[2]
        .0
        .test_truncate_local_log("oor", 0, 0)
        .await
        .expect("truncate broker 3");
    cluster[0]
        .0
        .test_advance_log_start("oor", 0, 25)
        .await
        .expect("advance leader log_start");

    // Wait for broker 3 to converge again — log_end_offset should reach
    // 50 once it has fetched records 25..50 from the leader.
    cluster[2]
        .0
        .wait_until_local_log_end_offset("oor", 0, 50)
        .await;

    shutdown_cluster(cluster).await;
}

/// A follower that catches up across sealed segments copies every batch of the
/// leader, in order, with no gap.
///
/// Each segment holds three batches of about 300 KB. A follower fetch of 1 MiB
/// reads to the end of one segment, goes on into the next, and stops inside it
/// with budget left. The leader once read on from that point into the segment
/// after, and served the first batch of that segment. The follower then
/// skipped every offset between the two. The throttled-reassignment system
/// test found that gap: one new replica held 43 of the 666 big batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_that_catches_up_across_segments_copies_every_batch() {
    const BATCHES: i64 = 12;
    let _lock_guard = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    for (handle, _, _) in &cluster {
        handle.wait_until_brokers_registered(3).await;
    }

    let (leader_addr, topic_id) = create_replicated_partition(&cluster, CreatableTopic {
        configs: vec![CreatableTopicConfig {
            name: "segment.bytes".into(),
            value: Some("1048576".into()),
            ..Default::default()
        }],
        ..support::topic_on("seams", &[&[1, 2]])
    })
    .await;

    let producer = connect_client(leader_addr, None).await;
    for _ in 0..BATCHES {
        let batch = batch_from_records(vec![Record {
            value: Some(bytes::Bytes::from(vec![b'x'; 300_000])),
            ..Default::default()
        }]);
        produce_replicated(&producer, "seams", topic_id, batch).await;
    }
    let leader = &cluster[0].0;
    let follower = &cluster[1].0;
    follower
        .wait_until_local_log_end_offset("seams", 0, BATCHES)
        .await;

    // Empty the follower, so that it copies the whole log again in fetches of
    // 1 MiB, from sealed segments that are already complete.
    follower
        .test_truncate_local_log("seams", 0, 0)
        .await
        .expect("truncate the follower");
    follower
        .wait_until_local_log_end_offset("seams", 0, BATCHES)
        .await;

    let every_batch: Vec<i64> = (0..BATCHES).collect();
    assert!(leader.local_batch_base_offsets_for_test("seams", 0) == Some(every_batch.clone()));
    assert!(follower.local_batch_base_offsets_for_test("seams", 0) == Some(every_batch));

    shutdown_cluster(cluster).await;
}

/// KIP-107 (#746): `DeleteRecords` moves the log start offset on every
/// replica, and answers only when every live follower has it. A follower reads
/// the leader's `log_start_offset` from each Fetch response and raises its own,
/// as Kafka's `ReplicaFetcherThread` does, and the leader's purgatory waits for
/// the followers to report it. So when the response arrives, no replica still
/// serves a deleted record, and a leader change cannot bring one back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_records_moves_every_replica_log_start_before_it_answers() {
    use krabka_protocol::owned::delete_records_response::DeleteRecordsPartitionResult;

    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(3).await;
    }

    // cluster[0] is node 1, and the topic pins it as the leader of partition 0.
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let client = connect_client(leader_addr, None).await;
    let topic_id = support::topics::create_assigned_partition(
        &client,
        AssignedTopicSetup::three_replicas("trimmed"),
        cluster.iter().map(|(broker, _, _)| broker),
    )
    .await;

    let batch = support::client::value_batch(ValueBatchSetup {
        records: ValueRecordCount(20),
    });
    produce_replicated(&client, "trimmed", topic_id, batch).await;
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("trimmed", 0, 20).await;
    }

    let deleted = client
        .send(delete_records_request(
            vec![delete_records_topic("trimmed", vec![
                delete_records_partition(0, 10),
            ])],
            30_000,
        ))
        .await
        .unwrap();
    assert!(
        deleted.topics[0].partitions
            == vec![DeleteRecordsPartitionResult {
                partition_index: 0,
                low_watermark: 10,
                error_code: 0,
                ..Default::default()
            }]
    );

    // The response came after every replica moved, so no wait is needed here.
    let log_starts: Vec<Option<i64>> = cluster
        .iter()
        .map(|(h, _, _)| h.partition_log_start_for_test("trimmed", 0))
        .collect();
    assert!(log_starts == vec![Some(10); 3]);

    shutdown_cluster(cluster).await;
}

/// KIP-227 + `num.replica.fetchers`: a follower folds every partition it
/// follows from one leader into one connection and one in-flight `Fetch`.
///
/// Before this, the follower spawned one task, one connection and one
/// single-partition sessionless `Fetch` per (topic, partition). A follower of
/// twelve partitions therefore held twelve connections to the leader and
/// parked twelve requests in its purgatory every round, which is the
/// reconnect storm and the request amplification #415 describes.
///
/// The leader's own KIP-227 session cache is what makes this observable: a
/// sessionless follower fetch caches nothing, so
/// `krabka_broker_incremental_fetch_sessions` counted zero however many
/// partitions were replicating. One session per *follower* -- not per
/// partition -- is exactly the shape being asserted, and it stays at two while
/// the partition count grows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_batches_every_partition_of_one_leader_into_one_session() {
    let _g = cluster_lock().lock().await;
    let (cluster, metrics_addr) = metrics_cluster("batched").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut sessions = -1_i64;
    while std::time::Instant::now() < deadline {
        sessions = scrape_gauge(metrics_addr, "krabka_broker_incremental_fetch_sessions").await;
        if sessions == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert!(
        sessions == 2,
        "one incremental fetch session per follower, not per partition; got {sessions}"
    );

    shutdown_cluster(cluster).await;
}

/// The batching's other half: a caught-up follower's request *rate* is bounded
/// by how many fetchers it runs, not by how many partitions it follows.
///
/// One task per partition meant at least `2P` Fetch requests per second per
/// follower on an idle cluster, each parked in the leader's purgatory and
/// decoded on its own. With one fetcher per leader it is a couple per second
/// however many partitions there are. A regression that makes the fetcher spin
/// -- a round that returns immediately and is re-sent at once -- shows up here
/// as a rate in the thousands, and nowhere else until a profiler is pointed at
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caught_up_follower_fetches_a_few_times_a_second_whatever_it_follows() {
    let _g = cluster_lock().lock().await;
    let (cluster, metrics_addr) = metrics_cluster("idle-rate").await;
    // Let the followers settle into their steady state before sampling.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let before = scrape_counter(metrics_addr, "krabka_broker_api_requests_total", "Fetch").await;
    let window = std::time::Duration::from_secs(3);
    tokio::time::sleep(window).await;
    let after = scrape_counter(metrics_addr, "krabka_broker_api_requests_total", "Fetch").await;

    // The counter is a request count over three seconds; it does not reach
    // the range where an `i64` stops fitting an `f64` exactly, and the
    // conversion says so rather than truncating silently.
    let observed = i32::try_from(after - before).expect("Fetch count over three seconds fits i32");
    let per_second = f64::from(observed) / window.as_secs_f64();
    // Two followers, each long-polling for `fetch_max_wait`, plus whatever
    // internal topics the cluster keeps warm. Generous by two orders of
    // magnitude against a spin, and far below the ~50/s the per-partition
    // shape produced for this topic alone.
    assert!(
        per_second < 40.0,
        "a caught-up follower must not busy-fetch; leader saw {per_second:.1} Fetch/s"
    );

    shutdown_cluster(cluster).await;
}

/// Reads one labelled counter out of a broker's `/metrics` body, summed over
/// every series whose label set contains `label_value`.
async fn scrape_counter(addr: std::net::SocketAddr, name: &str, label_value: &str) -> i64 {
    let body = scrape(addr).await;
    body.lines()
        .filter(|line| line.starts_with(name) && line.contains(label_value))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<i64>().ok())
        .sum()
}

/// Reads one label-free gauge out of a broker's `/metrics` body.
async fn scrape_gauge(addr: std::net::SocketAddr, name: &str) -> i64 {
    scrape(addr)
        .await
        .lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse::<i64>().ok())
        .unwrap_or(-1)
}

async fn produce_replicated(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    batch: RecordBatch,
) {
    let response = client
        .send(batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        }))
        .await
        .unwrap();
    assert!(response.responses[0].partition_responses[0].error_code == 0);
}
