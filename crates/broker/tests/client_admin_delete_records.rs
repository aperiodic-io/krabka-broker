mod support;

use assert2::assert;
use krabka_client_admin::DeleteRecordsOp;
use krabka_client_core::{ClientError, Connection, ConnectionOptions, fetch_partition};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_truncates_wal_and_maps_outcome() {
    let (_dir, _broker, bootstrap, mut admin) = crate::support::admin::standalone_admin().await;
    admin
        .create_topics(
            &[crate::support::admin::topic_spec("wal".to_string(), 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();

    let producer = crate::support::producer::default_producer(&bootstrap).await;
    for offset in 0..100 {
        producer
            .send(crate::support::producer::producer_record(
                crate::support::producer::ProducerRecordSetup {
                    topic: "wal".to_string(),
                    partition: Some(krabka_ids::PartitionIndex(0)),
                    value: Some(format!("frame-{offset}").into_bytes().into()),
                    ..Default::default()
                },
            ))
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();

    let outcomes = admin
        .delete_records(
            &[DeleteRecordsOp {
                topic: "wal".to_string(),
                partition: 0,
                offset: 50,
            }],
            krabka_units::secs(5),
        )
        .await
        .unwrap();

    assert!(
        outcomes
            == vec![krabka_client_admin::DeleteRecordsOutcome {
                topic: "wal".to_string(),
                partition: 0,
                error_code: 0,
                low_watermark: 50,
            }]
    );

    let topic_id = admin
        .metadata(&["wal"])
        .await
        .unwrap()
        .topics
        .into_iter()
        .find(|topic| topic.name == "wal")
        .and_then(|topic| topic.topic_id)
        .map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes()));

    let reader = connect_reader(&bootstrap).await;
    let fetch_error = fetch_partition(
        &reader,
        "wal",
        topic_id,
        0,
        0,
        krabka_units::millis(500),
        krabka_units::mebibytes(1),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(fetch_error, ClientError::Server { error_code: 1, .. }),
        "fetch below log start should return OFFSET_OUT_OF_RANGE, got {fetch_error:?}"
    );
}

async fn connect_reader(bootstrap: &str) -> Connection {
    let addr = tokio::net::lookup_host(bootstrap)
        .await
        .expect("resolve bootstrap")
        .next()
        .expect("bootstrap address");
    Connection::connect_with_options(
        addr,
        ConnectionOptions {
            client_id: "delete-records-test-reader".to_string(),
            ..Default::default()
        },
    )
    .await
    .expect("connect reader")
}
