//! The one scenario that produces no records at all: a transaction marker,
//! which has no representation in the v0 or v1 `MessageSet` format and must be
//! dropped from a down-converted Fetch response, leaving an empty record set.
//!
//! The marker is created by committing a real transaction with the Rust
//! producer, because Kafka forbids a client from producing a control batch
//! directly.

use assert2::assert;
use bytes::Bytes;
use krabka_client_producer::Producer;
use krabka_protocol::{
    Decode, kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
};

use crate::{
    harness::{create_topic, fetch_legacy_raw_at},
    support,
};

/// Control batches, that is txn markers, have no representation in the v0 and
/// v1 `MessageSet` format, so a down-converted Fetch response must drop them.
///
/// The test commits a real transaction, fetches from its marker offset at v3,
/// and confirms that the partition comes back with an empty record set and no
/// error.
/// This drives the `Ok(None)` arm of the Fetch handler's down-conversion loop
/// without violating Kafka's rule that clients cannot produce control batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_v3_drops_control_batch() {
    let p = support::start_legacy().await;
    create_topic(&p.client, "legacy_fetch_ctrl").await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    p.broker.wait_until_transaction_coordinator_ready().await;

    let addr = p.broker.listen_addr();
    let producer = Producer::builder()
        .bootstrap(addr.to_string())
        .transactional_id("legacy-fetch-control-marker")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let transaction = producer.begin_transaction().await.unwrap();
    producer
        .send(crate::support::producer::producer_record(
            crate::support::producer::ProducerRecordSetup {
                topic: ("legacy_fetch_ctrl").into(),
                value: Some(Bytes::from_static(b"data-before-marker")),
                ..Default::default()
            },
        ))
        .await
        .expect("transactional produce");
    transaction.commit().await.expect("commit transaction");
    p.broker
        .wait_until_local_log_end_offset("legacy_fetch_ctrl", 0, 2)
        .await;

    // Offset 0 is the data batch; offset 1 is the internally generated marker.
    let resp_body = fetch_legacy_raw_at(addr, "legacy_fetch_ctrl", 3, 1).await;
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );
    // An empty record set, not a null one. Kafka's schema types the field as
    // nullable and Kafka's broker never uses that, so krabka does not either:
    // a client that drops the connection on a negative length -- sarama does --
    // must still be able to read a response whose only batch was dropped.
    assert!(
        part.records
            .as_ref()
            .is_some_and(|records| records.payload_len() == 0),
        "control batch must be dropped, leaving an empty record set on the \
         wire, but the partition carried {:?}",
        part.records
    );

    producer.close().await.unwrap();
    p.broker.shutdown().await;
}
