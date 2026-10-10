//! KIP-516: Fetch by `topic_id` error semantics.
//!
//! The test client negotiates the broker's highest `Fetch` version, which
//! carries only the topic id. Kafka's `KafkaApis.handleFetchRequest` answers
//! `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id does not
//! resolve to a name. The zero id is such an id.
use assert2::assert;

use crate::support::{
    fetch::fetch_request_for,
    topics::{creatable_topic, create_topic_request},
};
mod support;

use bytes::Bytes;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::{FetchableTopicResponse, PartitionData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

#[tokio::test]
async fn fetch_unresolved_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    p.client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("f_known").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("create topic");

    let cases = [
        (
            "non-zero id",
            WireUuid(uuid::Uuid::from_u128(0xdead_beef).into_bytes()),
        ),
        ("zero id", WireUuid::ZERO),
    ];
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, topic_id) in cases {
        let partition = |partition| FetchPartition {
            partition,
            fetch_offset: 0,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        };
        let resp = p
            .client
            .send(fetch_request_for(
                vec![FetchTopic {
                    topic: String::new(), // v13+: name absent, id-only on the wire
                    topic_id,
                    partitions: vec![partition(0), partition(1)],
                    ..Default::default()
                }],
                crate::support::fetch::FetchLimits::wait_for_data(
                    crate::support::fetch::RequestWaitMillis(100),
                ),
            ))
            .await
            .expect("fetch");
        // Kafka's `FetchResponse.partitionResponse`.
        let refused = |partition_index| PartitionData {
            partition_index,
            error_code: UNKNOWN_TOPIC_ID,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: Some(Vec::new()),
            preferred_read_replica: -1,
            records: Some(RecordsPayload::Legacy(Bytes::new())),
            ..Default::default()
        };
        actual.push((label, resp.responses));
        expected.push((
            label,
            vec![FetchableTopicResponse {
                topic: String::new(),
                topic_id,
                partitions: vec![refused(0), refused(1)],
                ..Default::default()
            }],
        ));
    }
    assert!(actual == expected);
}
