//! KIP-516: Produce by `topic_id` error semantics.
//!
//! The test client negotiates the broker's highest `Produce` version, v13,
//! which carries only the topic id. Kafka's `KafkaApis.handleProduceRequest`
//! answers `UNKNOWN_TOPIC_ID` on every partition row of a topic whose id does
//! not resolve to a name. The zero id is such an id.
use assert2::assert;

use crate::support::topics::{creatable_topic, create_topic_request};
mod support;

use krabka_protocol::owned::{
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

#[tokio::test]
async fn produce_unresolved_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    p.client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("p_known").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("create topic");

    let cases = support::topics::unresolved_topic_ids(0x0bad_f00d);
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, topic_id) in cases {
        let resp = p
            .client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: String::new(), // v13: id-only on the wire
                    topic_id,
                    partition_data: vec![
                        PartitionProduceData {
                            index: 0,
                            records: None,
                            ..Default::default()
                        },
                        PartitionProduceData {
                            index: 1,
                            records: None,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("produce");
        let refused = |index| PartitionProduceResponse {
            index,
            error_code: UNKNOWN_TOPIC_ID,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            ..Default::default()
        };
        actual.push((label, resp));
        expected.push((
            label,
            ProduceResponse {
                responses: vec![TopicProduceResponse {
                    name: String::new(),
                    topic_id,
                    partition_responses: vec![refused(0), refused(1)],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ));
    }
    assert!(actual == expected);
}
