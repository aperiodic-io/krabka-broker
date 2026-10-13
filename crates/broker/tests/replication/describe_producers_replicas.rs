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

use {
    crate::{
        cluster_lock,
        support::{
            client::connect_with_context,
            produce::{SinglePartitionProduceSetup, batch_request},
            shutdown_cluster,
            topics::CreateTopicRequestSetup,
            transactions::{ProducerEpoch, ProducerIdentity, TransactionOutcome},
            {self},
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::{BrokerConfig, BrokerHandle, codes},
    krabka_client_core::{Connection, ConnectionOptions},
    krabka_ids::ProducerId,
    krabka_protocol::{
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
    },
    std::{net::SocketAddr, time::Duration},
    tempfile::TempDir,
};

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
const IDEMPOTENT: ProducerIdentity = producer(9_001, 0);
/// The transactional producer that commits at its epoch (transaction version
/// 1).
const COMMITTED: ProducerIdentity = producer(9_002, 2);
/// The transactional producer that aborts at the next epoch (transaction
/// version 2).
const ABORTED: ProducerIdentity = producer(9_003, 4);
/// The transactional producer whose transaction stays open.
const OPEN: ProducerIdentity = producer(9_004, 0);

/// The producer with `id` at `epoch`.
const fn producer(id: i64, epoch: i16) -> ProducerIdentity {
    ProducerIdentity {
        id: ProducerId(id),
        epoch: ProducerEpoch(epoch),
    }
}

/// The batch fixture's view of `identity`.
const fn batch_producer(identity: ProducerIdentity) -> BatchProducer {
    BatchProducer {
        id: identity.id,
        epoch: BatchProducerEpoch(identity.epoch.0),
    }
}

/// The transaction fields of a `DescribeProducers` row.
#[derive(Clone, Copy)]
struct TransactionFields {
    coordinator_epoch: i32,
    current_txn_start_offset: i64,
}

/// A row for a producer that has no open transaction and no marker.
const NO_TRANSACTION: TransactionFields = TransactionFields {
    coordinator_epoch: -1,
    current_txn_start_offset: -1,
};

use crate::support::records::now_ms;

/// One connection to the broker that binds `address`, so that a request
/// reaches that broker and no other.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(address, ConnectionOptions {
        client_id: "describe-producers-replicas".to_owned(),
        ..ConnectionOptions::default()
    })
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

// A batch of `producer` with `records` records from
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
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (TOPIC).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated_with_thirty_second_timeout()
        })
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

/// Write the marker that ends the transaction of `producer` on the leader, and
/// return the error code of the partition row.
async fn end_transaction(
    leader: &Connection,
    producer: ProducerIdentity,
    commit: TransactionOutcome,
    transaction_version: i8,
) -> i16 {
    let response = leader
        .send(WriteTxnMarkersRequest {
            markers: vec![
                crate::support::transactions::transaction_marker()
                    .producer(producer)
                    .outcome(commit)
                    .coordinator_epoch(COORDINATOR_EPOCH)
                    .transaction_version(transaction_version)
                    .topics(vec![crate::support::transactions::marker_topic(
                        TOPIC.into(),
                        vec![0],
                    )])
                    .call(),
            ],
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
    producer: ProducerIdentity,
    last_sequence: i32,
    last_timestamp: i64,
    TransactionFields {
        coordinator_epoch,
        current_txn_start_offset,
    }: TransactionFields,
) -> ProducerState {
    ProducerState {
        producer_id: producer.id.0,
        producer_epoch: i32::from(producer.epoch.0),
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
    let _g = cluster_lock().lock().await;
    let cluster = start_cluster().await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin =
        connect_with_context(cluster[0].1.listen_addr.to_string(), None, "admin client").await;
    let topic_id = support::client::create_topic_spec(
        &admin,
        support::topic_on(TOPIC, &[&[1, 2, 3]]),
        CreateTopicRequestSetup::default(),
    )
    .await;
    support::partitions::wait_for_partition_on_all(
        cluster.iter().map(|(handle, _, _)| handle),
        TOPIC,
        0,
    )
    .await;
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
            batch()
                .producer(batch_producer(IDEMPOTENT))
                .records(3)
                .max_timestamp(BASE_TIMESTAMP)
                .call(),
        )
        .await,
        produce(
            &leader,
            topic_id,
            batch()
                .producer(batch_producer(IDEMPOTENT))
                .base_sequence(3)
                .records(2)
                .max_timestamp(BASE_TIMESTAMP + 1)
                .call(),
        )
        .await,
    ];
    // Offsets 5 and 6, 8, and 10 and 11: the transactional batches. Offsets 7
    // and 9 are the markers below.
    let committed = produce_transactional(
        &leader,
        batch()
            .producer(batch_producer(COMMITTED))
            .records(2)
            .max_timestamp(BASE_TIMESTAMP + 2)
            .transaction(BatchTransactionMode::Transactional)
            .call(),
    )
    .await;
    let before_markers = now_ms();
    let commit = end_transaction(&leader, COMMITTED, TransactionOutcome::Commit, 1).await;
    let aborted = produce_transactional(
        &leader,
        batch()
            .producer(batch_producer(ABORTED))
            .max_timestamp(BASE_TIMESTAMP + 3)
            .transaction(BatchTransactionMode::Transactional)
            .call(),
    )
    .await;
    let aborted_next_epoch = producer(ABORTED.id.0, ABORTED.epoch.0 + 1);
    let abort = end_transaction(&leader, aborted_next_epoch, TransactionOutcome::Abort, 2).await;
    let after_markers = now_ms();
    let open = produce_transactional(
        &leader,
        batch()
            .producer(batch_producer(OPEN))
            .records(2)
            .max_timestamp(BASE_TIMESTAMP + 4)
            .transaction(BatchTransactionMode::Transactional)
            .call(),
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
    let committed_marker = TransactionFields {
        coordinator_epoch: COORDINATOR_EPOCH,
        current_txn_start_offset: -1,
    };
    let committed_at = reported_timestamp(&answers[0], COMMITTED.id.0);
    let aborted_at = reported_timestamp(&answers[0], aborted_next_epoch.id.0);
    let expected = DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: vec![TopicResponse {
            name: TOPIC.into(),
            partitions: vec![PartitionResponse {
                partition_index: 0,
                error_code: codes::NONE,
                error_message: None,
                active_producers: vec![
                    producer_row(IDEMPOTENT, 4, BASE_TIMESTAMP + 1, NO_TRANSACTION),
                    producer_row(COMMITTED, 1, committed_at, committed_marker),
                    producer_row(aborted_next_epoch, -1, aborted_at, committed_marker),
                    producer_row(OPEN, 1, BASE_TIMESTAMP + 4, TransactionFields {
                        coordinator_epoch: -1,
                        current_txn_start_offset: 10,
                    }),
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

    shutdown_cluster(cluster).await;
}
