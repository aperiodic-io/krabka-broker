//! The shared fixtures the verbatim-passthrough tests drive the broker with: a
//! single-broker boot, `CreateTopics` with and without topic configs, v2
//! `RecordBatch` builders, and the Produce and Fetch round trips.
//!
//! The builders are kept here rather than in each scenario module because
//! every scenario needs the same batch shape; only the codec, the record count,
//! and the producer identity differ between them.

use assert2::assert;
use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::FetchRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{RecordBatch, RecordsPayload},
};

pub use crate::support::topic_id_for;
use crate::support::{
    client::connect_client,
    fetch::{fetch_partition, single_partition_fetch},
    produce::single_partition_produce,
    records::value_record,
};

/// Build a single v2 `RecordBatch` that carries `n` copies of `value`, with the
/// given codec. The encoder compresses the body when the codec is not `None`.
pub fn batch(codec: CompressionType, n: usize, value: &[u8]) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: i32::try_from(n).unwrap() - 1,
        max_timestamp: 12_345,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(codec);
    for i in 0..n {
        b.records.push(value_record(
            i32::try_from(i).unwrap(),
            Some(Bytes::copy_from_slice(value)),
        ));
    }
    b
}

pub fn idempotent_lz4_batch(producer_id: i64, base_sequence: i32, n: usize) -> RecordBatch {
    let mut batch = batch(CompressionType::Lz4, n, &[b'I'; 1024]);
    batch.max_timestamp = 77;
    batch.producer_id = producer_id;
    batch.producer_epoch = 0;
    batch.base_sequence = base_sequence;
    batch
}

pub fn encode_batch(b: &RecordBatch) -> Bytes {
    let mut buf = bytes::BytesMut::new();
    b.encode(&mut buf).unwrap();
    buf.freeze()
}

pub async fn create_topic(broker: &krabka_broker::BrokerHandle, bootstrap: &str, name: &str) {
    create_topic_with_configs(broker, bootstrap, name, vec![]).await;
}

pub use crate::support::partitions::wait_for_compression;

pub async fn create_topic_with_configs(
    broker: &krabka_broker::BrokerHandle,
    bootstrap: &str,
    name: &str,
    configs: Vec<CreatableTopicConfig>,
) {
    let client = connect_client(bootstrap.to_string(), None).await;
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![crate::support::topics::creatable_topic_with_configs(
                crate::support::topics::ConfiguredTopicSetup {
                    name: name.into(),
                    configs,
                    ..Default::default()
                },
            )],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    broker.wait_until_partition_present(name, 0).await;
}

/// Produce a single batch to `topic` partition 0 with `acks=1`, and return the
/// assigned base offset.
pub async fn produce_one(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    b: RecordBatch,
) -> Result<i64, i16> {
    produce_batches(client, topic, topic_id, vec![b]).await
}

pub async fn produce_batches(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    batches: Vec<RecordBatch>,
) -> Result<i64, i16> {
    produce_payload(client, topic, topic_id, RecordsPayload::V2(batches)).await
}

pub async fn produce_payload(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    records: RecordsPayload,
) -> Result<i64, i16> {
    let resp = client
        .send(single_partition_produce(
            crate::support::produce::SinglePartitionProduceSetup {
                topic: (topic).into(),
                topic_id,
                records: Some(records),
                ..Default::default()
            },
        ))
        .await
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

/// Fetch partition 0 from offset 0 and return the first decoded batch.
///
/// `n` is the number of records already produced to partition 0. With a single
/// broker at RF=1 the high-watermark follows the local log end offset, but the
/// writer updates it after the append acknowledgement. Wait for the actual
/// high-watermark so one deterministic fetch cannot race that update.
pub async fn fetch_first_batch(
    broker: &krabka_broker::BrokerHandle,
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: WireUuid,
    n: i64,
) -> RecordBatch {
    broker.wait_until_high_watermark(topic, 0, n).await;
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
                topic: topic.into(),
                topic_id,
                partition: fetch_partition(crate::support::fetch::FetchPartitionSetup {
                    maximum: crate::support::fetch::FetchByteLimit(8 << 20),
                    ..Default::default()
                }),
                limits: crate::support::fetch::FetchLimits::wait_for_data_with_maximum(
                    crate::support::fetch::RequestWaitMillis(1_000),
                    crate::support::fetch::FetchByteLimit(8 << 20),
                ),
            })
        })
        .await
        .expect("Fetch");
    let pd = &resp.responses[0].partitions[0];
    assert!(pd.error_code == 0, "fetch error: {pd:?}");
    let payload = pd.records.as_ref().expect("records present");
    let batches = payload.as_v2().expect("v2 payload");
    batches.first().cloned().expect("at least one batch")
}

pub async fn boot() -> (krabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
        dir.path().to_path_buf(),
    ))
    .await
    .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}
