//! The two transaction-scoped fields of a producer row,
//! `current_txn_start_offset` and `coordinator_epoch`, tracked across an open
//! transaction, the `WriteTxnMarkers` that completes it, and the next one.

use {
    crate::{
        producers_harness::{
            create_topic, init_transactional_producer, topic_id_for, transactional_batch,
        },
        support::{
            self,
            admin::describe_producers_request,
            produce::{SinglePartitionProduceSetup, batch_request},
            records::{ProducerSequence, ProducerValuesSetup},
        },
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        produce_request::ProduceRequest, write_txn_markers_request::WriteTxnMarkersRequest,
    },
};

#[tokio::test]
async fn transactional_fields_follow_open_and_completed_transactions() {
    let p = support::start().await;
    create_topic(&p.client, "transactions").await;
    let topic_id = topic_id_for(&p.client, "transactions").await;
    let producer = init_transactional_producer(&p, "describe-producers-tid").await;

    let produce_response = p
        .client
        .send(transaction_values_request(topic_id, ProducerValuesSetup {
            pid: producer.id,
            epoch: producer.epoch,
            values: &["first"],
            ..Default::default()
        }))
        .await
        .expect("transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    check_transaction_state(
        &p.client,
        "DescribeProducers during first transaction",
        TransactionState {
            current_txn_start_offset: 0,
            coordinator_epoch: -1,
        },
    )
    .await;

    let marker = p
        .client
        .send(WriteTxnMarkersRequest {
            markers: vec![
                crate::support::transactions::transaction_marker()
                    .producer(producer)
                    .coordinator_epoch(17)
                    .transaction_version(1)
                    .topics(vec![crate::support::transactions::marker_topic(
                        "transactions".into(),
                        vec![0],
                    )])
                    .call(),
            ],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    assert!(marker.markers[0].topics[0].partitions[0].error_code == 0);

    check_transaction_state(
        &p.client,
        "DescribeProducers after marker",
        TransactionState {
            current_txn_start_offset: -1,
            coordinator_epoch: 17,
        },
    )
    .await;

    let produce_response = p
        .client
        .send(transaction_values_request(topic_id, ProducerValuesSetup {
            pid: producer.id,
            epoch: producer.epoch,
            base_seq: ProducerSequence(1),
            values: &["second"],
        }))
        .await
        .expect("second transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    check_transaction_state(
        &p.client,
        "DescribeProducers during second transaction",
        TransactionState {
            current_txn_start_offset: 2,
            coordinator_epoch: 17,
        },
    )
    .await;

    p.broker.shutdown().await;
}

/// The transaction fields one active producer reports.
struct TransactionState {
    current_txn_start_offset: i64,
    coordinator_epoch: i32,
}

async fn check_transaction_state(
    client: &krabka_client_core::Client,
    context: &str,
    expected: TransactionState,
) {
    let describe = client
        .send(describe_producers_request("transactions".into(), vec![0]))
        .await
        .expect(context);
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == expected.current_txn_start_offset);
    check!(producer_row.coordinator_epoch == expected.coordinator_epoch);
}

fn transaction_values_request(
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    values: ProducerValuesSetup<'_>,
) -> ProduceRequest {
    ProduceRequest {
        transactional_id: Some("describe-producers-tid".into()),
        ..batch_request(
            transactional_batch()
                .pid(values.pid.0)
                .epoch(values.epoch.0)
                .base_seq(values.base_seq.0)
                .values(values.values)
                .call(),
            SinglePartitionProduceSetup {
                topic: ("transactions").into(),
                topic_id,
                ..SinglePartitionProduceSetup::replicated()
            },
        )
    }
}
