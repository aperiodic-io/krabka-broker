//! The typed requests that set the scenario up: creating the compacted topic
//! with its config overrides, resolving its `topic_id`, and producing one
//! keyed record.
//!
//! Each helper encodes a request body, hands it to [`kafka_wire::round_trip`], and decodes
//! the matching response, so the API versions this suite pins are all stated
//! in one file.

use std::net::SocketAddr;

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_response::CreateTopicsResponse, metadata_response::MetadataResponse,
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use tokio::net::TcpStream;

use crate::{
    CLIENT_ID,
    kafka_wire::{self, ExchangeSetup as WireExchangeSetup},
    support::{
        discovery::named_topic_metadata,
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        topics::{CreateTopicSetup, configured_topic_request},
        wire::HeaderEncoding as RequestHeaderEncoding,
    },
};

/// Create a topic with config overrides, on PLAINTEXT and with no SASL.
pub(crate) async fn create_topic_with_configs(addr: SocketAddr, setup: CreateTopicSetup<'_>) {
    let topic = setup.topic;
    let req = configured_topic_request(setup);

    let version: i16 = 7; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode CreateTopics");
    let resp_bytes = kafka_wire::round_trip(
        &mut stream,
        WireExchangeSetup {
            api_key: WireApiKey(19),
            version: WireApiVersion(version),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, version).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Get `topic_id` with Metadata. Produce and Fetch v9+ need it.
pub(crate) async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = named_topic_metadata(topic.to_string());
    let version: i16 = 12; // flexible
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let resp: MetadataResponse = kafka_wire::exchange(
        &mut stream,
        &req,
        WireExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(version),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
    )
    .await
    .expect("Metadata round-trip");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

/// One keyed record for the compacted single-partition topic.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ProduceRecordSetup<'a> {
    #[default("compacted")]
    pub topic: &'a str,
    pub topic_id: Uuid,
    #[default(b"k")]
    pub key: &'a [u8],
    #[default(b"v")]
    pub value: &'a [u8],
}

/// Produce one record with an explicit key and value to (topic, partition 0).
pub(crate) async fn produce_record(addr: SocketAddr, setup: ProduceRecordSetup<'_>) {
    let ProduceRecordSetup {
        topic,
        topic_id,
        key,
        value,
    } = setup;
    let record = Record {
        key: Some(Bytes::copy_from_slice(key)),
        ..value_record(0, Some(Bytes::copy_from_slice(value)))
    };
    let batch = RecordBatch {
        last_offset_delta: 0,
        ..batch_from_records(vec![record])
    };

    let req = batch_request(
        batch,
        SinglePartitionProduceSetup {
            topic: topic.to_string(),
            topic_id,
            ..Default::default()
        },
    );

    let version: i16 = 9; // flexible, pre-KIP-516 (no topic_id required on the wire at v9)
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let resp: ProduceResponse = kafka_wire::exchange(
        &mut stream,
        &req,
        WireExchangeSetup {
            api_key: WireApiKey(0),
            version: WireApiVersion(version),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
    )
    .await
    .expect("Produce round-trip");
    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "Produce must succeed: error_code={}",
        part.error_code
    );
}
