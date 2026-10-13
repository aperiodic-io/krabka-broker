//! Handler tests for Kafka's `LogValidator.validateKey`: a topic whose
//! `cleanup.policy` holds `compact` refuses a batch that holds a record with no
//! key, with `INVALID_RECORD` and one record error per keyless record.

use {
    crate::{
        authorizer::AllowAllAuthorizer, broker::BrokerHandle, codes,
        handlers::test_support::produce_wire, test_support::start_broker_with_authorizer_no_audit,
    },
    assert2::check,
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::{
                BatchIndexAndErrorMessage, PartitionProduceResponse, ProduceResponse,
                TopicProduceResponse,
            },
        },
        records::{Record, RecordBatch, RecordsPayload},
    },
    std::sync::Arc,
};

/// Produce v12 names the topic and carries `record_errors` and
/// `error_message` (v8 and later).
const VERSION: i16 = 12;

async fn create_topic(broker: &BrokerHandle, name: &str, cleanup_policy: &str) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("compacted-key-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![CreatableTopicConfig {
                    name: "cleanup.policy".into(),
                    value: Some(cleanup_policy.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    check!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(name, 0).await;
}

/// A batch whose records carry `keys`, in order.
fn batch(keys: &[Option<&'static [u8]>]) -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        last_offset_delta: i32::try_from(keys.len()).expect("small batch") - 1,
        records: keys
            .iter()
            .zip(0..)
            .map(|(key, offset_delta)| Record {
                offset_delta,
                key: key.map(Bytes::from_static),
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }])
}

async fn produce(broker: &BrokerHandle, topic: &str, records: RecordsPayload) -> ProduceResponse {
    let part_data = PartitionProduceData {
        index: 0,
        records: Some(records),
        ..Default::default()
    };
    let request = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![part_data],
            ..Default::default()
        }],
        ..Default::default()
    };
    produce_wire(broker, VERSION, &request).await
}

fn response(topic: &str, partition: PartitionProduceResponse) -> ProduceResponse {
    ProduceResponse {
        responses: vec![TopicProduceResponse {
            name: topic.to_string(),
            partition_responses: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn appended(record_count: i64) -> (PartitionProduceResponse, i64) {
    (
        PartitionProduceResponse {
            index: 0,
            error_code: codes::NONE,
            base_offset: 0,
            log_append_time_ms: -1,
            log_start_offset: 0,
            ..Default::default()
        },
        record_count,
    )
}

/// The row Kafka answers for a batch whose records at `indices` have no key.
fn refused(topic: &str, indices: &[i32]) -> (PartitionProduceResponse, i64) {
    let message =
        format!("Compacted topic cannot accept message without key in topic partition {topic}-0");
    let shown = indices
        .iter()
        .map(|index| format!("RecordError(batchIndex={index}, message='{message}')"))
        .collect::<Vec<_>>()
        .join(", ");
    (
        PartitionProduceResponse {
            index: 0,
            error_code: codes::INVALID_RECORD,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: 0,
            record_errors: indices
                .iter()
                .map(|&batch_index| BatchIndexAndErrorMessage {
                    batch_index,
                    batch_index_error_message: Some(message.clone()),
                    ..Default::default()
                })
                .collect(),
            error_message: Some(format!(
                "One or more records have been rejected due to {} record errors in total, and \
                 only showing the first three errors at most: [{shown}]",
                indices.len()
            )),
            ..Default::default()
        },
        0,
    )
}

#[tokio::test]
async fn a_compacted_topic_refuses_a_record_without_a_key() {
    const KEYED: &[Option<&[u8]>] = &[Some(b"k0"), Some(b"k1")];
    const KEYLESS: &[Option<&[u8]>] = &[None, Some(b"k1"), None];

    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let cases = [
        ("delete", KEYED),
        ("delete", KEYLESS),
        ("compact", KEYED),
        ("compact", KEYLESS),
        ("compact,delete", KEYED),
        ("compact,delete", KEYLESS),
    ];
    for (row, (policy, keys)) in cases.into_iter().enumerate() {
        let topic = format!("keys-{row}");
        create_topic(&broker, &topic, policy).await;

        let actual = produce(&broker, &topic, batch(keys)).await;

        let record_count = i64::try_from(keys.len()).expect("small batch");
        let (partition, log_end) = if policy.contains("compact") && keys == KEYLESS {
            refused(&topic, &[0, 2])
        } else {
            appended(record_count)
        };
        check!(actual == response(&topic, partition), "{policy} {keys:?}");
        let stored = broker
            .broker_arc_for_test()
            .partitions
            .get(&topic, krabka_ids::PartitionIndex(0))
            .expect("partition")
            .log
            .lock()
            .expect("log")
            .log_end_offset();
        check!(stored == krabka_ids::Offset(log_end), "{policy} {keys:?}");
    }
    broker.shutdown().await;
}
