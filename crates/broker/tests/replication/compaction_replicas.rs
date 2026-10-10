//! Log compaction on every replica of a partition.
//!
//! Kafka's cleaner keeps the batch that holds the last sequence of each active
//! producer, as an empty batch when compaction removes all of its records
//! (`Cleaner.cleanInto`, `isBatchLastRecordOfProducer`). It reads the active
//! producers from the producer state of the log
//! (`UnifiedLog.lastRecordsOfActiveProducers`), which a follower updates for
//! each batch that it replicates. A follower that compacts its log therefore
//! keeps the same batches as its leader.
//!
//! The produce path of this broker keeps a second copy of the producer state,
//! and a follower does not add its replicated data batches to that copy. A
//! cleaner that read that copy removed the last batch of an idempotent
//! producer on each follower. A new leader then had no batch that held the
//! sequence of the producer.

use std::net::SocketAddr;

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    owned::create_topics_request::CreatableTopicConfig,
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::{support, support::records::batch_from_records};

const TOPIC: &str = "compaction-replicas";

/// The `internal.segment.bytes` of the topic. Each batch of this test is 72
/// bytes, so a segment holds one batch and not two. Kafka refuses a batch that
/// is larger than a segment with `RECORD_LIST_TOO_LARGE`.
const SEGMENT_BYTES: u32 = 100;

/// The idempotent producer: `(id, epoch)`.
const IDEMPOTENT: (i64, i16) = (9_201, 0);

// One batch of a local log after compaction. The compared fields are the
// ones that carry the state of a producer, and the records.
krabka_macros::compacted_batch!(Kept, plain);

impl Kept {
    /// The one-record batch of a client with no idempotence at `offset`.
    fn plain(offset: i64, (key, value): (&'static str, &'static str)) -> Self {
        Self {
            base_offset: offset,
            last_offset: offset,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![(
                Some(Bytes::from_static(key.as_bytes())),
                Some(Bytes::from_static(value.as_bytes())),
            )],
        }
    }
}

use crate::support::records::now_ms;

/// One connection to the broker that binds `address`, so that a request
/// reaches that broker and no other.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(
        address,
        ConnectionOptions {
            client_id: "compaction-replicas".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("connect")
}

/// A one-record batch of `key` and `value`. `producer` is `(id, epoch,
/// base_sequence)`, or `None` for a client with no idempotence.
fn record(
    producer: Option<(i64, i16, i32)>,
    (key, value): (&'static str, &'static str),
    timestamp: i64,
) -> RecordBatch {
    let (producer_id, producer_epoch, base_sequence) = producer.unwrap_or((-1, -1, -1));
    RecordBatch {
        base_timestamp: timestamp,
        max_timestamp: timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        ..batch_from_records(vec![Record {
            key: Some(Bytes::from_static(key.as_bytes())),
            value: Some(Bytes::from_static(value.as_bytes())),
            ..Record::default()
        }])
    }
}

/// Send `batch` to the leader with `acks=-1`, and return the error code of
/// the partition row.
async fn produce(leader: &Connection, topic_id: WireUuid, batch: RecordBatch) -> i16 {
    let response = crate::support::produce::send_batch(
        &leader,
        batch,
        crate::support::produce::SinglePartitionProduceSetup {topic: (TOPIC).into(), topic_id, ..crate::support::produce::SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()},
    )
    .await;
    response.responses[0].partition_responses[0].error_code
}

/// Wait until the topic configuration of [`TOPIC`] is in the log of the
/// partition on `broker`. A batch that arrives before it goes into one large
/// segment, which no compaction pass rewrites.
async fn wait_for_the_topic_config(broker: &BrokerHandle) {
    broker
        .wait_for_metrics("the topic config reaches the partition log", |_| {
            broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| {
                    config.cleanup_policy == krabka_log::CleanupPolicy::Compact
                        && config.segment_size == krabka_units::bytes(SEGMENT_BYTES)
                })
        })
        .await;
}

/// An idempotent producer writes keys `a` and `b`, and a client with no
/// idempotence then writes both keys again, so compaction removes every
/// record of the producer. Each replica compacts its own log. Every replica
/// keeps the last batch of the producer as an empty batch, which holds the
/// sequence of the producer for a later leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_keeps_the_last_batch_of_an_active_producer() {
    let _g = crate::cluster_lock().lock().await;
    let cluster = crate::support::registered_cluster(3).await;

    let admin = crate::support::client::connect_with_context(
        cluster[0].1.listen_addr.to_string(),
        None,
        "admin client",
    )
    .await;
    // One batch for each segment, so that every batch except the last is in a
    // sealed segment that a pass rewrites.
    let mut topic = support::topic_on(TOPIC, &[&[1, 2, 3]]);
    topic.configs = [
        ("cleanup.policy", "compact".to_owned()),
        ("internal.segment.bytes", SEGMENT_BYTES.to_string()),
    ]
    .into_iter()
    .map(|(name, value)| CreatableTopicConfig {
        name: name.into(),
        value: Some(value),
        ..Default::default()
    })
    .collect();
    let topic_id = support::client::create_topic_spec(
        &admin,
        topic,
        crate::support::topics::CreateTopicRequestSetup::default(),
    )
    .await;
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(TOPIC, 0).await;
        wait_for_the_topic_config(handle).await;
    }
    cluster[0]
        .0
        .wait_until_local_partition_leader(TOPIC, 0, cluster[0].1.node_id)
        .await;
    let leader = connect(cluster[0].0.listen_addr()).await;

    let now = now_ms();
    let (producer_id, producer_epoch) = IDEMPOTENT;
    let last_of_the_producer = record(Some((producer_id, producer_epoch, 1)), ("b", "p-b"), now);
    let batches = [
        record(Some((producer_id, producer_epoch, 0)), ("a", "p-a"), now),
        last_of_the_producer.clone(),
        record(None, ("a", "a-2"), now),
        record(None, ("b", "b-3"), now),
        record(None, ("c", "c-4"), now),
    ];
    let mut errors = Vec::new();
    for batch in batches {
        errors.push(produce(&leader, topic_id, batch).await);
    }
    assert!(errors == vec![codes::NONE; 5]);
    for (handle, _, _) in &cluster {
        handle.wait_until_local_log_end_offset(TOPIC, 0, 5).await;
        handle.wait_until_high_watermark(TOPIC, 0, 5).await;
    }

    let mut kept = Vec::new();
    for (handle, _, _) in &cluster {
        handle
            .compact_local_log_for_test(TOPIC, 0)
            .await
            .expect("compact");
        let batches = handle
            .local_batches_for_test(TOPIC, 0)
            .expect("the local log");
        kept.push(batches.iter().map(Kept::of).collect::<Vec<_>>());
    }

    let producer_header = Kept {
        base_offset: 1,
        last_offset: 1,
        records: Vec::new(),
        ..Kept::of(&last_of_the_producer)
    };
    let expected = vec![
        producer_header,
        Kept::plain(2, ("a", "a-2")),
        Kept::plain(3, ("b", "b-3")),
        Kept::plain(4, ("c", "c-4")),
    ];
    assert!(kept == vec![expected; 3]);

    crate::support::shutdown_cluster(cluster).await;
}
