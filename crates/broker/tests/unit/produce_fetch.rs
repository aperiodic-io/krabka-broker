//! The record round trip over one broker.
//!
//! A produce assigns base offsets, a fetch reads the batches back, and
//! `ListOffsets` reports the ends of an empty partition. All three run against
//! the same in-process broker.

use assert2::{assert, check};
use krabka_protocol::owned::list_offsets_request::ListOffsetsRequest;

/// Builds one `RecordBatch` that carries `n` empty records with sequential
/// offset deltas.
use crate::support::records::empty_record_batch as one_record_batch;
use crate::{
    harness::{create_topic, topic_id_for},
    support,
    support::{
        fetch::single_partition_fetch,
        offsets::{list_offset_partition, single_partition_list_offsets},
    },
};

#[tokio::test]
async fn produce_assigns_base_offsets() {
    let p = support::start().await;
    create_topic(&p, "prod", 1).await;
    let topic_id = topic_id_for(&p.client, "prod").await;

    // First produce: 3 records → base 0.
    let req = crate::support::produce::batch_request(
        one_record_batch(3),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("prod").into(),
            topic_id,
            ..Default::default()
        },
    );
    let resp = p.client.send(req).await.expect("Produce 1");
    assert!(resp.responses.len() == 1);
    assert!(resp.responses[0].partition_responses.len() == 1);
    check!(resp.responses[0].partition_responses[0].error_code == 0);
    check!(resp.responses[0].partition_responses[0].base_offset == 0);

    // Second produce: 2 records → base 3.
    let req2 = crate::support::produce::batch_request(
        one_record_batch(2),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("prod").into(),
            topic_id,
            ..Default::default()
        },
    );
    let resp2 = p.client.send(req2).await.expect("Produce 2");
    assert!(resp2.responses[0].partition_responses[0].error_code == 0);
    assert!(resp2.responses[0].partition_responses[0].base_offset == 3);

    p.broker.shutdown().await;
}

/// The test client negotiates Produce v13, which carries only the topic id.
/// A request with no id names no topic, so Kafka's
/// `KafkaApis.handleProduceRequest` answers `UNKNOWN_TOPIC_ID` (100). The name
/// path and its `UNKNOWN_TOPIC_OR_PARTITION` (3) belong to v12 and earlier.
#[tokio::test]
async fn produce_without_a_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let req = crate::support::produce::batch_request(
        one_record_batch(1),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("nope").into(),
            ..Default::default()
        },
    );
    let resp = p.client.send(req).await.expect("Produce unknown");
    assert!(resp.responses[0].partition_responses[0].error_code == 100);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_then_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "round", 1).await;
    let topic_id = topic_id_for(&p.client, "round").await;

    let prod = crate::support::produce::batch_request(
        one_record_batch(3),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("round").into(),
            topic_id,
            ..Default::default()
        },
    );
    let presp = p.client.send(prod).await.expect("Produce");
    assert!(presp.responses[0].partition_responses[0].error_code == 0);

    let fetch = single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
        topic: "round".into(),
        topic_id,
        limits: crate::support::fetch::FetchLimits::wait_for_data(
            crate::support::fetch::RequestWaitMillis(100),
        ),
        ..Default::default()
    });
    let fresp = p.client.send(fetch).await.expect("Fetch");
    assert!(fresp.responses.len() == 1);
    crate::support::fetch::check_record_count(&fresp.responses[0].partitions[0], 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let p = support::start().await;
    create_topic(&p, "empty", 1).await;

    let mk = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        ..single_partition_list_offsets("empty", list_offset_partition(0, ts))
    };

    let earliest = p.client.send(mk(-2)).await.expect("ListOffsets earliest");
    let latest = p.client.send(mk(-1)).await.expect("ListOffsets latest");
    for (label, resp) in [("earliest", &earliest), ("latest", &latest)] {
        check!(resp.topics[0].partitions[0].error_code == 0, "{label}");
        check!(resp.topics[0].partitions[0].offset == 0, "{label}");
    }

    p.broker.shutdown().await;
}
