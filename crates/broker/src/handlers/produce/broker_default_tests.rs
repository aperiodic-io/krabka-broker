//! Handler tests for the broker-wide defaults of the topic keys `Produce`
//! enforces.
//!
//! `message.max.bytes`, `log.message.timestamp.*`, `compression.type`,
//! `log.cleanup.policy` and the other broker synonyms of a topic key are
//! dynamic broker configs. `kafka-configs --entity-type brokers` sets them for
//! the cluster (`--entity-default`) or for one node, and Kafka's
//! `DynamicLogConfig` applies the value to every log at once, so the next
//! produce validates against it. A topic that overrides the key keeps its own
//! value.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord};
use krabka_protocol::{
    owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};

use crate::{
    authorizer::AllowAllAuthorizer, broker::BrokerHandle, codes,
    test_support::start_broker_with_authorizer_no_audit,
};

/// The `Produce` version these tests speak.
const VERSION: i16 = 9;

async fn create_topic(broker: &BrokerHandle, name: &str) {
    crate::handlers::test_support::create_topic(
        broker,
        crate::handlers::test_support::ClientTopicSetup {
            client_id: "broker-default-test",
            name,
            ..Default::default()
        },
    )
    .await;
}

/// One dynamic config override; default to a cluster-wide two-MiB message cap.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct BrokerConfigSetup<'a> {
    #[default(DEFAULT_BROKER_CONFIG_NODE_ID)]
    node: krabka_metadata::NodeId,
    #[default("message.max.bytes")]
    name: &'a str,
    #[default("2097152")]
    value: &'a str,
}

/// The stored value of one dynamic broker config, on `node` (the cluster
/// default for [`DEFAULT_BROKER_CONFIG_NODE_ID`]).
async fn set_broker_config(broker: &BrokerHandle, setup: BrokerConfigSetup<'_>) {
    let BrokerConfigSetup { node, name, value } = setup;
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: node,
            config_name: name.to_owned(),
            config_value: Some(value.to_owned()),
        })])
        .await
        .expect("submit the broker config");
}

/// The error code `Produce` answers for one record of `value_bytes` bytes and
/// no key, sent to partition 0 of `topic`.
async fn produce_error_code(broker: &BrokerHandle, topic: &str, value_bytes: usize) -> i16 {
    produce_error_code_compressed(broker, topic, value_bytes, CompressionType::None).await
}

/// [`produce_error_code`] for a batch the producer compressed with `codec`.
async fn produce_error_code_compressed(
    broker: &BrokerHandle,
    topic: &str,
    value_bytes: usize,
    codec: CompressionType,
) -> i16 {
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![RecordBatch {
                    attributes: Attributes::default().with_compression(codec),
                    records: vec![Record {
                        value: Some(Bytes::from(vec![0_u8; value_bytes])),
                        ..Default::default()
                    }],
                    ..Default::default()
                }])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = crate::handlers::test_support::produce_wire(broker, VERSION, &request).await;
    response.responses[0].partition_responses[0].error_code
}

/// A cluster-wide `message.max.bytes` raises the cap `Produce` enforces on a
/// topic that sets none, and a node's own value beats the cluster's. Each
/// batch is 1.5 MiB, over the built-in 1048588-byte cap.
#[tokio::test]
async fn a_dynamic_message_max_bytes_governs_a_topic_that_sets_no_cap() {
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic(&broker, "orders").await;
    let node = krabka_metadata::NodeId(broker.broker_arc_for_test().config.node_id.0);
    let batch = 1_572_864;

    // The built-in default refuses it.
    assert!(produce_error_code(&broker, "orders", batch).await == codes::MESSAGE_TOO_LARGE);

    // A cluster-wide default of 2 MiB lets it through.
    set_broker_config(&broker, BrokerConfigSetup::default()).await;
    assert!(produce_error_code(&broker, "orders", batch).await == codes::NONE);

    // This node's own value wins over the cluster's.
    set_broker_config(
        &broker,
        BrokerConfigSetup {
            node,
            value: "1100000",
            ..Default::default()
        },
    )
    .await;
    assert!(produce_error_code(&broker, "orders", batch).await == codes::MESSAGE_TOO_LARGE);

    broker.shutdown().await;
}

/// Kafka trunk's `max.decompressed.message.bytes` refuses a compressed record
/// whose decompressed body is over it, as `INVALID_RECORD`, and only a
/// compressed one: an uncompressed record of the same size is bounded by
/// `max.message.bytes` alone. The limit is a dynamic broker config too, so a
/// cluster-wide value governs a topic that sets none and a node's own value
/// beats the cluster's.
#[tokio::test]
async fn a_dynamic_max_decompressed_message_bytes_refuses_an_oversized_compressed_record() {
    let (broker, _dir) = crate::test_support::start_broker_no_audit_with(|config| {
        config.authorizer = Arc::new(AllowAllAuthorizer);
        config.features.unstable_api_versions = crate::api_catalog::UnstableApiVersions::Enabled;
    })
    .await;
    create_topic(&broker, "orders").await;
    let node = krabka_metadata::NodeId(broker.broker_arc_for_test().config.node_id.0);
    let key = "max.decompressed.message.bytes";
    let record = 4_096;
    let gzip = |value_bytes| {
        produce_error_code_compressed(&broker, "orders", value_bytes, CompressionType::Gzip)
    };

    // The default is no limit.
    assert!(gzip(record).await == codes::NONE);

    // A cluster-wide 512 refuses the compressed record and not the small one,
    // and not the same record sent uncompressed.
    set_broker_config(
        &broker,
        BrokerConfigSetup {
            name: key,
            value: "512",
            ..Default::default()
        },
    )
    .await;
    assert!(gzip(record).await == codes::INVALID_RECORD);
    assert!(gzip(64).await == codes::NONE);
    assert!(produce_error_code(&broker, "orders", record).await == codes::NONE);

    // This node's own value wins over the cluster's.
    set_broker_config(
        &broker,
        BrokerConfigSetup {
            node,
            name: key,
            value: "8192",
        },
    )
    .await;
    assert!(gzip(record).await == codes::NONE);

    broker.shutdown().await;
}

/// Kafka 4.3.1 has no `max.decompressed.message.bytes`, so a broker that does
/// not serve trunk's keys ignores a stored value.
#[tokio::test]
async fn a_default_broker_ignores_max_decompressed_message_bytes() {
    let (broker, _dir) = start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic(&broker, "orders").await;
    set_broker_config(
        &broker,
        BrokerConfigSetup {
            name: "max.decompressed.message.bytes",
            value: "512",
            ..Default::default()
        },
    )
    .await;

    let code = produce_error_code_compressed(&broker, "orders", 4_096, CompressionType::Gzip).await;

    assert!(code == codes::NONE);
    broker.shutdown().await;
}
