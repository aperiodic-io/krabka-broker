//! `DescribeProducers` on every replica of a partition.
//!
//! Kafka answers `DescribeProducers` from the producer state of the partition
//! log on every replica that hosts the partition:
//! `ReplicaManager.activeProducerState`, then `Partition.activeProducerState`,
//! then `UnifiedLog.activeProducers`. A follower updates that state for each
//! batch that it replicates (`UnifiedLog.appendAsFollower`). When a follower
//! has the log of its leader, it gives the same answer as its leader.
//!
//! The produce path of this broker keeps a second copy of the producer state.
//! A follower does not add its replicated data batches to that copy, so a
//! follower that answered from it reported only the producers whose
//! transaction markers it had replicated.

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{BrokerConfig, BrokerHandle, codes};
use krabka_client_core::{Connection, ConnectionOptions};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        describe_producers_request::{DescribeProducersRequest, TopicRequest},
        describe_producers_response::{
            DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
        },
        produce_request::ProduceRequest,
        produce_response::ProduceResponse,
        write_txn_markers_request::WriteTxnMarkersRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use tempfile::TempDir;

use crate::support;

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

const TOPIC: &str = "describe-producers-replicas";

/// The `transactional_id` of every transactional `Produce`.
const TRANSACTIONAL_ID: &str = "describe-producers-replicas";

/// The `max_timestamp` of the first batch. Each later batch adds one.
const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

/// The coordinator epoch that every transaction marker carries.
const COORDINATOR_EPOCH: i32 = 17;

/// The last `Produce` version before transaction version 2. With
/// `transaction.partition.verification.enable=false`, Kafka's leader appends
/// a transactional batch at this version without a call to the transaction
/// coordinator, so no coordinator takes part in this test.
const PRODUCE_V11: i16 = 11;

/// Kafka's `api_key` of `Produce`.
const PRODUCE_API_KEY: i16 = 0;

/// The idempotent producer.
const IDEMPOTENT: (i64, i16) = (9_001, 0);
/// The transactional producer that commits at its epoch (transaction version
/// 1).
const COMMITTED: (i64, i16) = (9_002, 2);
/// The transactional producer that aborts at the next epoch (transaction
/// version 2).
const ABORTED: (i64, i16) = (9_003, 4);
/// The transactional producer whose transaction stays open.
const OPEN: (i64, i16) = (9_004, 0);

use crate::support::records::now_ms;

/// One connection to the broker that binds `address`, so that a request
/// reaches that broker and no other.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(
        address,
        ConnectionOptions {
            client_id: "describe-producers-replicas".to_owned(),
            ..ConnectionOptions::default()
        },
    )
    .await
    .expect("connect")
}

/// Three brokers whose leaders append a transactional `Produce` below v12
/// without the transaction coordinator. A cluster start can split the vote
/// on a slow runner, so a failed start is tried again with new ports.
async fn start_cluster() -> Cluster {
    let mut last_error = None;
    for _ in 0..3 {
        match support::start_n_node_with(3, |_, config| {
            config.transaction_partition_verification_enable = false;
        })
        .await
        {
            Ok(cluster) => return cluster,
            Err(error) => {
                last_error = Some(error);
                // intentional: a new attempt needs the old ports released.
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts: {last_error:?}");
}

// A batch of `producer` (`(id, epoch)`) with `records` records from
// `base_sequence`.
krabka_macros::producer_batch_fixture!(
    batch,
    ::bytes::Bytes::from(format!("{producer_id}-{offset_delta}"))
);

fn produce_request(
    topic_id: WireUuid,
    transactional_id: Option<&str>,
    batch: RecordBatch,
) -> ProduceRequest {
    ProduceRequest {
        transactional_id: transactional_id.map(Into::into),
        ..crate::support::produce::batch_request(
            batch,
            crate::support::produce::SinglePartitionProduceSetup {topic: (TOPIC).into(), topic_id, ..crate::support::produce::SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()},
        )
    }
}

/// Send the idempotent `batch` to the leader at the negotiated version, and
/// return the error code of the partition row.
async fn produce(leader: &Connection, topic_id: WireUuid, batch: RecordBatch) -> i16 {
    let response = leader
        .send(produce_request(topic_id, None, batch))
        .await
        .expect("Produce");
    response.responses[0].partition_responses[0].error_code
}

/// Send the transactional `batch` to the leader as `Produce` v11, and return
/// the error code of the partition row.
async fn produce_transactional(leader: &Connection, batch: RecordBatch) -> i16 {
    let request = produce_request(WireUuid::default(), Some(TRANSACTIONAL_ID), batch);
    let mut body = BytesMut::new();
    request
        .encode(&mut body, PRODUCE_V11)
        .expect("encode Produce v11");
    let mut answer = leader
        .raw_request(PRODUCE_API_KEY, PRODUCE_V11, body.freeze())
        .await
        .expect("Produce v11");
    let response = ProduceResponse::decode(&mut answer, PRODUCE_V11).expect("decode Produce v11");
    response.responses[0].partition_responses[0].error_code
}

/// Write the marker that ends the transaction of `producer` (`(id, epoch)`)
/// on the leader, and return the error code of the partition row.
async fn end_transaction(
    leader: &Connection,
    (producer_id, producer_epoch): (i64, i16),
    commit: crate::support::transactions::TransactionOutcome,
    transaction_version: i8,
) -> i16 {
    let response = leader
        .send(WriteTxnMarkersRequest {
            markers: vec![crate::support::transactions::transaction_marker(
                crate::support::transactions::TransactionMarkerSetup {
                    producer: crate::support::transactions::ProducerIdentity::from_wire((
                        producer_id,
                        producer_epoch,
                    )),
                    outcome: commit,
                    coordinator_epoch: crate::support::transactions::CoordinatorEpoch(
                        COORDINATOR_EPOCH,
                    ),
                    transaction_version: crate::support::transactions::TransactionVersion(
                        transaction_version,
                    ),
                    topics: vec![crate::support::transactions::marker_topic(
                        TOPIC.into(),
                        vec![0],
                    )],
                },
            )],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    response.markers[0].topics[0].partitions[0].error_code
}

async fn describe_producers(broker: &BrokerHandle) -> DescribeProducersResponse {
    connect(broker.listen_addr())
        .await
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: TOPIC.into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers")
}

fn producer_row(
    (producer_id, producer_epoch): (i64, i16),
    last_sequence: i32,
    last_timestamp: i64,
    (coordinator_epoch, current_txn_start_offset): (i32, i64),
) -> ProducerState {
    ProducerState {
        producer_id,
        producer_epoch: i32::from(producer_epoch),
        last_sequence,
        last_timestamp,
        coordinator_epoch,
        current_txn_start_offset,
        ..Default::default()
    }
}

/// The `last_timestamp` that `answer` reports for `producer_id`, or `-1`
/// when it has no row for that producer. After a marker it is the broker
/// clock at that marker, which a test cannot know in advance.
fn reported_timestamp(answer: &DescribeProducersResponse, producer_id: i64) -> i64 {
    answer.topics[0].partitions[0]
        .active_producers
        .iter()
        .find(|row| row.producer_id == producer_id)
        .map_or(-1, |row| row.last_timestamp)
}

/// The leader appends batches of an idempotent producer and of three
/// transactional producers: one commits at its epoch, one aborts at the next
/// epoch, and one keeps its transaction open. Once every follower has the
/// whole log, each broker gives the same `DescribeProducers` answer: one row
/// for each producer, in producer id order, with the sequence, the timestamp
/// and the transaction fields of the producer state of its log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_replica_describes_the_producers_of_its_log() {
    let _g = crate::cluster_lock().lock().await;
    let cluster = start_cluster().await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin = crate::support::client::connect_with_context(
        cluster[0].1.listen_addr.to_string(),
        None,
        "admin client",
    )
    .await;
    let topic_id = support::client::create_topic_spec(
        &admin,
        support::topic_on(TOPIC, &[&[1, 2, 3]]),
        crate::support::topics::CreateTopicRequestSetup::default(),
    )
    .await;
    for (handle, _, _) in &cluster {
        handle.wait_until_partition_present(TOPIC, 0).await;
    }
    cluster[0]
        .0
        .wait_until_local_partition_leader(TOPIC, 0, cluster[0].1.node_id)
        .await;
    let leader = connect(cluster[0].0.listen_addr()).await;

    // Offsets 0 to 4: the idempotent producer, two batches.
    let idempotent = [
        produce(
            &leader,
            topic_id,
            batch(ProducerBatchSetup {
                producer: BatchProducer::from_wire(IDEMPOTENT),
                records: BatchRecordCount(3),
                max_timestamp: BatchTimestamp(BASE_TIMESTAMP),
                ..Default::default()
            }),
        )
        .await,
        produce(
            &leader,
            topic_id,
            batch(ProducerBatchSetup {
                producer: BatchProducer::from_wire(IDEMPOTENT),
                base_sequence: BatchSequence(3),
                records: BatchRecordCount(2),
                max_timestamp: BatchTimestamp(BASE_TIMESTAMP + 1),
                ..Default::default()
            }),
        )
        .await,
    ];
    // Offsets 5 and 6, 8, and 10 and 11: the transactional batches. Offsets 7
    // and 9 are the markers below.
    let committed = produce_transactional(
        &leader,
        batch(ProducerBatchSetup {
            producer: BatchProducer::from_wire(COMMITTED),
            records: BatchRecordCount(2),
            max_timestamp: BatchTimestamp(BASE_TIMESTAMP + 2),
            transaction: BatchTransactionMode::Transactional,
            ..Default::default()
        }),
    )
    .await;
    let before_markers = now_ms();
    let commit = end_transaction(
        &leader,
        COMMITTED,
        crate::support::transactions::TransactionOutcome::Commit,
        1,
    )
    .await;
    let aborted = produce_transactional(
        &leader,
        batch(ProducerBatchSetup {
            producer: BatchProducer::from_wire(ABORTED),
            max_timestamp: BatchTimestamp(BASE_TIMESTAMP + 3),
            transaction: BatchTransactionMode::Transactional,
            ..Default::default()
        }),
    )
    .await;
    let (aborted_id, aborted_epoch) = ABORTED;
    let abort = end_transaction(
        &leader,
        (aborted_id, aborted_epoch + 1),
        crate::support::transactions::TransactionOutcome::Abort,
        2,
    )
    .await;
    let after_markers = now_ms();
    let open = produce_transactional(
        &leader,
        batch(ProducerBatchSetup {
            producer: BatchProducer::from_wire(OPEN),
            records: BatchRecordCount(2),
            max_timestamp: BatchTimestamp(BASE_TIMESTAMP + 4),
            transaction: BatchTransactionMode::Transactional,
            ..Default::default()
        }),
    )
    .await;
    assert!(
        (idempotent, committed, commit, aborted, abort, open)
            == (
                [codes::NONE; 2],
                codes::NONE,
                codes::NONE,
                codes::NONE,
                codes::NONE,
                codes::NONE
            )
    );
    for (handle, _, _) in &cluster {
        handle.wait_until_local_log_end_offset(TOPIC, 0, 12).await;
    }

    let mut answers = Vec::new();
    for (handle, _, _) in &cluster {
        answers.push(describe_producers(handle).await);
    }

    // Kafka's `ProducerAppendInfo.appendEndTxnMarker` puts the timestamp of
    // the marker in `lastTimestamp`. The leader wrote both markers inside
    // this window, so its answer gives those values.
    let window = before_markers..=after_markers;
    let committed_at = reported_timestamp(&answers[0], COMMITTED.0);
    let aborted_at = reported_timestamp(&answers[0], aborted_id);
    let expected = DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: vec![TopicResponse {
            name: TOPIC.into(),
            partitions: vec![PartitionResponse {
                partition_index: 0,
                error_code: codes::NONE,
                error_message: None,
                active_producers: vec![
                    producer_row(IDEMPOTENT, 4, BASE_TIMESTAMP + 1, (-1, -1)),
                    producer_row(COMMITTED, 1, committed_at, (COORDINATOR_EPOCH, -1)),
                    producer_row(
                        (aborted_id, aborted_epoch + 1),
                        -1,
                        aborted_at,
                        (COORDINATOR_EPOCH, -1),
                    ),
                    producer_row(OPEN, 1, BASE_TIMESTAMP + 4, (-1, 10)),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        (
            answers,
            window.contains(&committed_at),
            window.contains(&aborted_at)
        ) == (vec![expected; 3], true, true)
    );

    crate::support::shutdown_cluster(cluster).await;
}
