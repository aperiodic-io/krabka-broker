//! The two transaction-scoped fields of a producer row,
//! `current_txn_start_offset` and `coordinator_epoch`, tracked across an open
//! transaction, the `WriteTxnMarkers` that completes it, and the next one.

use assert2::{assert, check};
use krabka_protocol::owned::{
    produce_request::ProduceRequest, write_txn_markers_request::WriteTxnMarkersRequest,
};

use crate::{
    producers_harness::{
        create_topic, init_transactional_producer, topic_id_for, transactional_batch,
    },
    support,
};

#[tokio::test]
async fn transactional_fields_follow_open_and_completed_transactions() {
    let p = support::start().await;
    create_topic(
        &p.client,
        crate::support::topics::CreateTopicSetup {
            topic: "transactions",
            ..Default::default()
        },
    )
    .await;
    let topic_id = topic_id_for(&p.client, "transactions").await;
    let (pid, epoch) = init_transactional_producer(&p, "describe-producers-tid").await;

    let produce_response = p
        .client
        .send(transaction_values_request(
            topic_id,
            crate::support::records::ProducerValuesSetup {
                pid: krabka_ids::ProducerId(pid),
                epoch: crate::support::transactions::ProducerEpoch(epoch),
                values: &["first"],
                ..Default::default()
            },
        ))
        .await
        .expect("transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    check_transaction_state(
        &p.client,
        "DescribeProducers during first transaction",
        (0, -1),
    )
    .await;

    let marker = p
        .client
        .send(WriteTxnMarkersRequest {
            markers: vec![crate::support::transactions::transaction_marker(
                crate::support::transactions::TransactionMarkerSetup {
                    producer: crate::support::transactions::ProducerIdentity::from_wire((
                        pid, epoch,
                    )),
                    coordinator_epoch: crate::support::transactions::CoordinatorEpoch(17),
                    transaction_version: crate::support::transactions::TransactionVersion(1),
                    topics: vec![crate::support::transactions::marker_topic(
                        "transactions".into(),
                        vec![0],
                    )],
                    ..Default::default()
                },
            )],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    assert!(marker.markers[0].topics[0].partitions[0].error_code == 0);

    check_transaction_state(&p.client, "DescribeProducers after marker", (-1, 17)).await;

    let produce_response = p
        .client
        .send(transaction_values_request(
            topic_id,
            crate::support::records::ProducerValuesSetup {
                pid: krabka_ids::ProducerId(pid),
                epoch: crate::support::transactions::ProducerEpoch(epoch),
                base_seq: crate::support::records::ProducerSequence(1),
                values: &["second"],
            },
        ))
        .await
        .expect("second transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    check_transaction_state(
        &p.client,
        "DescribeProducers during second transaction",
        (2, 17),
    )
    .await;

    p.broker.shutdown().await;
}

async fn check_transaction_state(
    client: &krabka_client_core::Client,
    context: &str,
    expected: (i64, i32),
) {
    let describe = client
        .send(crate::support::admin::describe_producers_request(
            "transactions".into(),
            vec![0],
        ))
        .await
        .expect(context);
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == expected.0);
    check!(producer_row.coordinator_epoch == expected.1);
}

fn transaction_values_request(
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    values: crate::support::records::ProducerValuesSetup<'_>,
) -> ProduceRequest {
    ProduceRequest {
        transactional_id: Some("describe-producers-tid".into()),
        ..crate::support::produce::batch_request(
            transactional_batch(values),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("transactions").into(),
                topic_id,
                ..crate::support::produce::SinglePartitionProduceSetup::replicated()
            },
        )
    }
}
