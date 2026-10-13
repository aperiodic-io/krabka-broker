//! #990: one transaction check per `Produce` request.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` collects every partition of
//! a request that starts a transaction and hands them to
//! `AddPartitionsToTxnManager.addOrVerifyTransaction` together, so the
//! coordinator sees one `AddPartitionsToTxn` for the whole request. At
//! `Produce` v12 that one call adds every partition with one
//! `__transaction_state` record.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{test_support::one_transactional_batch, *},
    crate::{
        codes,
        handlers::{
            init_producer_id::handle as init_producer_id_handle,
            test_support::configured_topic_request,
        },
        test_support::{
            decode_response, dispatch_context, encode_request, start_broker_no_audit_with,
        },
        txn::{bootstrap::TOPIC as TXN_BOOTSTRAP_TOPIC, state::TopicPartition},
    },
    assert2::assert,
    krabka_ids::PartitionIndex,
    krabka_protocol::owned::{
        create_topics_request::{self},
        init_producer_id_request::InitProducerIdRequest,
    },
};

const TOPIC: &str = "orders";
const TXN_ID: &str = "one-check";
const PARTITIONS: i32 = 3;

#[tokio::test]
async fn a_produce_that_starts_a_transaction_on_many_partitions_makes_one_coordinator_call() {
    let HandlerStartedBroker {
        broker: handle_,
        dir: _dir,
    } = start_broker_no_audit_with(|cfg| {
        cfg.transaction_state_num_partitions = 1;
        cfg.transaction_state_replication_factor = 1;
    })
    .await;
    handle_.wait_until_controller_leader().await;
    handle_.wait_until_brokers_registered(1).await;
    let broker = handle_.broker_arc_for_test();
    handle_.wait_until_transaction_coordinator_ready().await;

    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["client"];
        client ["one-check"];
    };
    let create = configured_topic_request()
        .topic(TOPIC)
        .num_partitions(PARTITIONS)
        .call();
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
    let init = init_producer_id_handle(&broker, init, init_version, &ctx)
        .await
        .expect("InitProducerId");
    assert!(init.error_code == codes::NONE, "InitProducerId: {init:?}");

    let state_partition = broker
        .partitions
        .get(TXN_BOOTSTRAP_TOPIC, PartitionIndex(0))
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
                    records: Some(
                        one_transactional_batch()
                            .producer_id(init.producer_id)
                            .producer_epoch(init.producer_epoch)
                            .call(),
                    ),
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
