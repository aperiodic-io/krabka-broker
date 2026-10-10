//! The raw-socket plumbing the legacy Fetch tests need: a non-flexible request
//! and response exchange, a modern flexible `ProduceRequest` v9 that seeds the
//! partition with a v2 batch, and the legacy `FetchRequest` encoder that asks
//! for the down-converted `MessageSet`.
//!
//! The suite talks to the broker over a `TcpStream` rather than through the
//! Rust client because the client always negotiates the highest Fetch version,
//! and these tests need to pin v0 and v3 exactly.

use assert2::assert;
use bytes::{Buf, BytesMut};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    kafka_3_6_2::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    owned::produce_response::ProduceResponse,
    records::{RecordBatch, RecordsPayload},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::support::{
    topics::{creatable_topic, create_topic_request},
    wire::CorrelationId as WireCorrelationId,
};

/// Verify successful down-conversion and extract the exact legacy bytes.
///
/// # Panics
/// Panics on a partition error, absent records, or a non-legacy payload.
pub fn legacy_bytes(
    error_code: i16,
    records: Option<&RecordsPayload>,
    mismatch: &str,
) -> bytes::Bytes {
    assert!(error_code == 0, "fetch partition error: {}", error_code);
    match records.expect("records field should be Some") {
        RecordsPayload::Legacy(bytes) => bytes.clone(),
        _ => panic!("{mismatch}"),
    }
}

// ── Wire helpers ──────────────────────────────────────────────────────────────

/// One length-prefixed request/response exchange with non-flexible v1 request
/// and v0 response headers; see [`kafka_wire::round_trip`]. Panics on an I/O
/// error.
pub use crate::kafka_wire::legacy_round_trip as round_trip_nonflexible;

// ── Topic helpers ─────────────────────────────────────────────────────────────

/// Creates a single-partition topic with the modern client and asserts that it
/// succeeds.
pub async fn create_topic(client: &krabka_client_core::Client, name: &str) {
    create_topic_with_partitions(client, name, 1).await;
}

/// Creates a topic of `num_partitions` partitions with the modern client and
/// asserts that it succeeds.
pub async fn create_topic_with_partitions(
    client: &krabka_client_core::Client,
    name: &str,
    num_partitions: i32,
) {
    let cr = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: (name).into(),
                partitions: crate::support::topics::TopicPartitionCount(num_partitions),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert!(
        cr.topics[0].error_code == 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );
}

/// Produces a single v2 batch to (topic, partition=0) with a modern flexible
/// `ProduceRequest`, version 9. Returns `Ok(())` on success.
pub async fn produce_batch(addr: std::net::SocketAddr, topic: &str, batch: RecordBatch) {
    produce_batch_to(addr, topic, 0, batch).await;
}

/// Produces a single v2 batch to `(topic, partition)` with a modern flexible
/// `ProduceRequest`, version 9.
pub async fn produce_batch_to(
    addr: std::net::SocketAddr,
    topic: &str,
    partition: i32,
    batch: RecordBatch,
) {
    const PRODUCE_VERSION: i16 = 9;
    let req = crate::support::produce::batch_request(
        batch,
        crate::support::produce::SinglePartitionProduceSetup {
            topic: (topic).into(),
            partition: krabka_ids::PartitionIndex(partition),
            ..Default::default()
        },
    );
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .expect("encode ProduceRequest v9");

    let mut stream = TcpStream::connect(addr).await.expect("connect for produce");
    stream.set_nodelay(true).ok();
    // ProduceRequest v9 is flexible (FLEXIBLE_MIN = 9).
    let frame = crate::support::wire::request_frame(crate::support::wire::WireFrameSetup {
        api_key: krabka_ids::ApiKey(0),
        version: krabka_ids::ApiVersion(PRODUCE_VERSION),
        correlation: crate::support::wire::CorrelationId(99),
        header: crate::support::wire::HeaderEncoding::Flexible,
        client_id: "legacy-fetch-produce",
        body: &body,
        ..Default::default()
    });

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .expect("write produce frame length");
    stream.write_all(&frame).await.expect("write produce frame");
    stream.flush().await.expect("flush produce");

    let resp_len = stream.read_u32().await.expect("read produce resp len");
    let mut resp = vec![0u8; resp_len as usize];
    stream
        .read_exact(&mut resp)
        .await
        .expect("read produce resp");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _tagged = cur.get_u8(); // flexible response header tagged fields
    let produce_resp =
        ProduceResponse::decode(&mut cur, PRODUCE_VERSION).expect("decode ProduceResponse v9");
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part_resp.error_code == 0,
        "produce error: {}",
        part_resp.error_code
    );
}

/// Sends a Fetch request at the given legacy `version` for (topic,
/// partition=0) from offset 0, and returns the raw response body bytes with
/// the `correlation_id` stripped.
///
/// Encoding at a low version drops the fields that version does not have, for
/// example `max_bytes`, which is v3+. One struct therefore works for v0 to
/// v3.
pub async fn fetch_legacy_raw(addr: std::net::SocketAddr, topic: &str, version: i16) -> Vec<u8> {
    fetch_legacy_raw_at(addr, topic, version, 0).await
}

pub async fn fetch_legacy_raw_at(
    addr: std::net::SocketAddr,
    topic: &str,
    version: i16,
    fetch_offset: i64,
) -> Vec<u8> {
    fetch_legacy_raw_partitions(addr, topic, version, &[0], fetch_offset).await
}

/// Sends one legacy Fetch covering several partitions of the same topic, so
/// the response carries more than one row for that topic entry.
///
/// The single-partition helpers cannot reach the down-conversion loop's
/// per-partition body more than once, which is where the topic name is turned
/// into a metric label.
pub async fn fetch_legacy_raw_partitions(
    addr: std::net::SocketAddr,
    topic: &str,
    version: i16,
    partitions: &[i32],
    fetch_offset: i64,
) -> Vec<u8> {
    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: partitions
                .iter()
                .map(|&partition| FetchPartition {
                    partition,
                    fetch_offset,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode legacy FetchRequest");

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect for legacy fetch");
    stream.set_nodelay(true).ok();
    round_trip_nonflexible(
        &mut stream,
        crate::kafka_wire::LegacyExchangeSetup {
            api_key: WireApiKey(1),
            version: WireApiVersion(version),
            correlation: WireCorrelationId(42),
            ..Default::default()
        },
        &body,
    )
    .await
}
