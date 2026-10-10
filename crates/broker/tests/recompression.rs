// rustc 1.95 clippy ICEs on `clippy::pedantic` in test files (same
// upstream bug as `tests/compaction.rs` / `tests/mtls.rs`).

//! Broker-side recompression.
//!
//! The test produces a gzip-compressed batch to a topic configured with
//! `compression.type=lz4`, fetches it back, and asserts the served
//! batch's attributes report `lz4`. This proves the broker re-encoded
//! the batch before it wrote it. The test also verifies the record
//! payload survives the round-trip intact. The broker decompresses the
//! gzip bytes and compresses them with lz4, and the client then
//! decompresses the lz4 bytes.
//!
//! The test is gated to non-Windows. This matches the multi-broker test
//! convention of the other replication and compaction tests.

mod kafka_wire;

mod support;

use std::net::SocketAddr;

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_compression::CompressionType;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        fetch_request::FetchRequest,
        fetch_response::FetchResponse,
        metadata_response::MetadataResponse,
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Attributes, RecordBatch},
};

use crate::support::{
    fetch::single_partition_fetch,
    records::{batch_from_records, value_record},
};

const CLIENT_ID: &str = "krabka-recompression-test";

async fn start_broker() -> (BrokerHandle, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    std::mem::forget(log_dir);
    (handle, addr)
}

async fn create_topic_with_compression(addr: SocketAddr, topic: &str, codec: &str) {
    let req = CreateTopicsRequest {
        topics: vec![crate::support::topics::creatable_topic_with_configs(
            crate::support::topics::ConfiguredTopicSetup {
                name: topic.into(),
                configs: vec![CreatableTopicConfig {
                    name: "compression.type".into(),
                    value: Some(codec.into()),
                    ..Default::default()
                }],
                ..Default::default()
            },
        )],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let version: i16 = 7;
    let (_, r): (usize, CreateTopicsResponse) =
        kafka_wire::request_once(addr, &req, (19, version), CLIENT_ID, (1, true)).await;
    assert!(
        r.topics[0].error_code == 0,
        "CreateTopics must succeed for compression.type={codec}: {:?}",
        r.topics[0]
    );
}

async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = crate::support::discovery::named_topic_metadata(topic);
    let version: i16 = 12;
    let (_, r): (usize, MetadataResponse) =
        kafka_wire::request_once(addr, &req, (3, version), CLIENT_ID, (1, true)).await;
    r.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response")
}

async fn produce_gzip(addr: SocketAddr, topic: &str, topic_id: Uuid, value: &[u8]) {
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Gzip),
        ..batch_from_records(vec![value_record(0, Some(Bytes::copy_from_slice(value)))])
    };
    let req = crate::support::produce::batch_request(
        batch,
        crate::support::produce::SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            ..crate::support::produce::SinglePartitionProduceSetup::replicated()
        },
    );
    let version: i16 = 9;
    let (_, r): (usize, ProduceResponse) =
        kafka_wire::request_once(addr, &req, (0, version), CLIENT_ID, (1, true)).await;
    let part = &r.responses[0].partition_responses[0];
    assert!(part.error_code == 0, "Produce must succeed: {part:?}");
}

async fn fetch_first_batch(addr: SocketAddr, topic: &str, topic_id: Uuid) -> RecordBatch {
    let req = FetchRequest {
        replica_id: -1,
        ..single_partition_fetch(crate::support::fetch::SinglePartitionFetchSetup {
            topic: topic.into(),
            topic_id,
            ..Default::default()
        })
    };
    let version: i16 = 12;
    let (_, r): (usize, FetchResponse) =
        kafka_wire::request_once(addr, &req, (1, version), CLIENT_ID, (1, true)).await;
    let part = &r.responses[0].partitions[0];
    assert!(part.error_code == 0, "Fetch error: {}", part.error_code);
    part.records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(|batches| batches.first().cloned())
        .expect("Fetch returned at least one v2 batch")
}

use crate::support::partitions::wait_for_compression;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_lz4_recompresses_producer_gzip_batch() {
    const TOPIC: &str = "recompress-target";
    let (handle, addr) = start_broker().await;

    create_topic_with_compression(addr, TOPIC, "lz4").await;
    // The CreateTopics path → metadata → replicator-supervisor
    // reconcile loop pushes the LogConfig override into the partition
    // writer's log. Wait until the writer has applied it; otherwise
    // the produce can land before the override and the broker passes
    // gzip through unmodified.
    wait_for_compression(&handle, TOPIC, Some(CompressionType::Lz4)).await;

    let topic_id = get_topic_id(addr, TOPIC).await;
    let payload = b"broker-side recompression smoke";
    produce_gzip(addr, TOPIC, topic_id, payload).await;

    let served = fetch_first_batch(addr, TOPIC, topic_id).await;
    check!(
        served.attributes.compression() == CompressionType::Lz4,
        "broker must re-encode the gzip batch to lz4 before write"
    );
    assert!(served.records.len() == 1);
    check!(
        served.records[0].value.as_deref() == Some(payload.as_slice()),
        "record payload must survive the recompress round-trip"
    );

    handle.shutdown().await;
}

/// Sanity check: when `compression.type=producer` (the Kafka default),
/// the broker does NOT recompress. The served batch keeps the
/// producer's gzip flag verbatim. Without this guard a regression that
/// always recompresses would still satisfy the lz4 happy-path test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_producer_preserves_producer_gzip() {
    const TOPIC: &str = "passthrough";
    let (handle, addr) = start_broker().await;

    create_topic_with_compression(addr, TOPIC, "producer").await;
    wait_for_compression(&handle, TOPIC, None).await;

    let topic_id = get_topic_id(addr, TOPIC).await;
    let payload = b"passthrough payload";
    produce_gzip(addr, TOPIC, topic_id, payload).await;

    let served = fetch_first_batch(addr, TOPIC, topic_id).await;
    assert!(
        served.attributes.compression() == CompressionType::Gzip,
        "compression.type=producer must preserve the producer's gzip flag"
    );
    assert!(served.records[0].value.as_deref() == Some(payload.as_slice()));

    handle.shutdown().await;
}

#[test]
fn stored_owned_batches_honor_every_codec_level() {
    use krabka_log::{Log, LogConfig};
    use krabka_units::gibibytes;

    for (codec, levels) in [
        (CompressionType::Gzip, [1, 9]),
        (CompressionType::Lz4, [9, 17]),
        (CompressionType::Zstd, [-5, 12]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for level in levels {
            log.set_config(LogConfig {
                compression_gzip_level: if codec == CompressionType::Gzip {
                    level
                } else {
                    -1
                },
                compression_lz4_level: if codec == CompressionType::Lz4 {
                    level
                } else {
                    9
                },
                compression_zstd_level: if codec == CompressionType::Zstd {
                    level
                } else {
                    3
                },
                ..LogConfig::default()
            });
            let value: Vec<u8> = (0..16 * 1024u32)
                .map(|i| u8::try_from((i * 7 + i / 13) % 251).unwrap())
                .collect();
            let mut batch = RecordBatch {
                attributes: Attributes::default().with_compression(codec),
                ..batch_from_records(vec![value_record(0, Some(Bytes::from(value)))])
            };
            let (base, _) = log.append(&mut batch).unwrap();
            let expected = encoded_at_level(&batch, CompressionLevel(level));
            let stored = log
                .read_raw(base, log.log_end_offset(), gibibytes(1))
                .unwrap();
            check!(stored.bytes == expected, "{codec:?} level={level}");
        }
    }
}

krabka_macros::compression_bytes_fixture!(encoded_at_level, CompressionLevel);
