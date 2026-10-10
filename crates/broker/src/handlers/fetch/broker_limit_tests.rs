use assert2::assert;
use krabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    records::RecordsPayload,
};
use krabka_units::bytes;

use crate::{
    authorizer::AllowAllAuthorizer,
    fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
};

#[tokio::test]
async fn broker_fetch_cap_bounds_consumer_fetch_and_keeps_first_batch_progress() {
    let (broker, _dir) = crate::handlers::test_support::start_broker_with(|config| {
        config.authorizer = std::sync::Arc::new(AllowAllAuthorizer);
        config.fetch_max_bytes = bytes(1);
    })
    .await;
    let topic = "broker-fetch-cap";
    let topic_id =
        crate::handlers::test_support::create_topic(&broker, "fetch-cap-test", topic, 1).await;
    crate::handlers::test_support::produce_records(&broker, topic, 0, 2).await;
    crate::handlers::test_support::produce_records(&broker, topic, 0, 2).await;

    let response = super::test_support::fetch_wire(
        &broker,
        krabka_protocol::owned::fetch_request::MAX_VERSION,
        "consumer",
        "consumer-client",
        &FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: i32::MAX,
            session_id: INVALID_SESSION_ID,
            session_epoch: FINAL_EPOCH,
            topics: vec![FetchTopic {
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: i32::MAX,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await;

    let row = &response.responses[0].partitions[0];
    let batches = row
        .records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .expect("records in first-batch progress response");
    assert!(batches.len() == 1);
    assert!(batches[0].records.len() == 2);
    broker.shutdown().await;
}
