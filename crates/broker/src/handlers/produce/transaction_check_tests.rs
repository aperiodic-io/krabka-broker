//! #990: one transaction check per `Produce` request.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` collects every partition of
//! a request that starts a transaction and hands them to
//! `AddPartitionsToTxnManager.addOrVerifyTransaction` together, so the
//! coordinator sees one `AddPartitionsToTxn` for the whole request. At
//! `Produce` v12 that one call adds every partition with one
//! `__transaction_state` record.

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_protocol::{
    owned::{
        create_topics_request::{self},
        init_producer_id_request::InitProducerIdRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse},
    },
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};

use super::handle;
use crate::{
    codes,
    handlers::test_support::CreateTopicSetup,
    test_support::{
        decode_response, dispatch_context, encode_request, peer, principal,
        start_broker_no_audit_with,
    },
    txn::state::TopicPartition,
};

const TOPIC: &str = "orders";
const TXN_ID: &str = "one-check";
const PARTITIONS: i32 = 3;

#[tokio::test]
async fn a_produce_that_starts_a_transaction_on_many_partitions_makes_one_coordinator_call() {
    let (handle_, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    handle_.wait_until_controller_leader().await;
    handle_.wait_until_brokers_registered(1).await;
    let broker = handle_.broker_arc_for_test();
    handle_.wait_until_transaction_coordinator_ready().await;

    request_identity!(
        (user, address, ctx),
        principal("client"),
        client_id = "one-check"
    );
    let create = crate::handlers::test_support::configured_topic_request(CreateTopicSetup {
        topic: TOPIC,
        num_partitions: crate::handlers::test_support::TopicPartitionCount(PARTITIONS),
        ..Default::default()
    });
    dispatch_context(
        &broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&create, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
    handle_
        .wait_for_image(|img| (0..PARTITIONS).all(|p| img.partition(TOPIC, p).is_some()))
        .await;
    for partition in 0..PARTITIONS {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while broker
                .partitions
                .get(TOPIC, PartitionIndex(partition))
                .is_none()
            {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("topic partition becomes local");
    }

    let init_version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let init = InitProducerIdRequest {
        transactional_id: Some(TXN_ID.to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let init = crate::handlers::init_producer_id::handle(&broker, init, init_version, &ctx)
        .await
        .expect("InitProducerId");
    assert!(init.error_code == codes::NONE, "InitProducerId: {init:?}");

    let state_partition = broker
        .partitions
        .get(crate::txn::bootstrap::TOPIC, PartitionIndex(0))
        .expect("__transaction_state-0");
    let state_records_before = state_partition.log_end_offset();

    let version = 12;
    let request = ProduceRequest {
        transactional_id: Some(TXN_ID.to_string()),
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.to_string(),
            partition_data: (0..PARTITIONS)
                .map(|index| PartitionProduceData {
                    index,
                    records: Some(RecordsPayload::V2(vec![RecordBatch {
                        attributes: Attributes::default().with_transactional(true),
                        producer_id: init.producer_id,
                        producer_epoch: init.producer_epoch,
                        base_sequence: 0,
                        records: vec![Record {
                            value: Some(Bytes::from_static(b"v")),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let request_bytes = encode_request(&request, version);
    let response: ProduceResponse = decode_response(
        &handle(
            &broker,
            version,
            &request_bytes,
            request_bytes.clone(),
            &ctx,
        )
        .await
        .expect("handle produce"),
        version,
    );

    let rows: Vec<PartitionProduceResponse> = response
        .responses
        .into_iter()
        .flat_map(|topic| topic.partition_responses)
        .collect();
    let appended: Vec<PartitionProduceResponse> = (0..PARTITIONS)
        .map(|index| PartitionProduceResponse {
            index,
            base_offset: 0,
            log_append_time_ms: -1,
            log_start_offset: 0,
            ..Default::default()
        })
        .collect();
    assert!(rows == appended);

    // One registration record holds all three partitions.
    assert!(state_partition.log_end_offset() == state_records_before + 1);
    let entry = broker
        .txn_coordinator
        .get(TXN_ID)
        .expect("transaction entry");
    let enlisted = entry.lock().await.partitions.clone();
    let expected: Vec<TopicPartition> = (0..PARTITIONS)
        .map(|partition| TopicPartition {
            topic: TOPIC.to_string(),
            partition: PartitionIndex(partition),
        })
        .collect();
    assert!(
        expected
            .iter()
            .all(|partition| enlisted.contains(partition))
    );
    assert!(enlisted.len() == expected.len());
    handle_.shutdown().await;
}
