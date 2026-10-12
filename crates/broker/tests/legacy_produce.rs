//! End-to-end test. A hand-crafted Produce v0 request goes through
//! up-conversion and lands on disk as a v2 `RecordBatch`. A fetch back through
//! the typed client, at whatever version it negotiates, must return the key and
//! value that the test sent.

use assert2::assert;

use crate::{
    kafka_wire::LegacyExchangeSetup,
    support::{
        fetch::{SinglePartitionFetchSetup, single_partition_fetch},
        topics::{ConfiguredTopicSetup, creatable_topic, create_topic_request},
    },
};
mod kafka_wire;
mod support;

use bytes::{Bytes, BytesMut};
use krabka_ids::Offset;
use krabka_protocol::{
    Decode, Encode,
    kafka_3_6_2::owned::{
        produce_request::{
            PartitionProduceData as LegacyPartitionProduceData,
            ProduceRequest as LegacyProduceRequest, TopicProduceData as LegacyTopicProduceData,
        },
        produce_response::ProduceResponse as LegacyProduceResponse,
    },
    records::RecordsPayload,
};
use krabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set};
use support::topic_id_for;
use tokio::net::TcpStream;

// ── Raw TCP wire helpers ──────────────────────────────────────────────────────

/// Build a v0 `MessageSet` that contains one record per `(key, value)` pair.
/// It uses `krabka_records_legacy::encode_flat_message_set`, so this file does
/// not hand-roll the CRC logic.
fn build_v0_messageset(pairs: &[(&str, &str)]) -> Bytes {
    let records: Vec<ParsedRecord> = pairs
        .iter()
        .enumerate()
        .map(|(i, (k, v))| ParsedRecord {
            offset: Offset(i64::try_from(i).expect("index fits in i64")),
            timestamp: None, // v0 has no timestamps
            key: Some(Bytes::copy_from_slice(k.as_bytes())),
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
        })
        .collect();
    let mut buf = BytesMut::new();
    encode_flat_message_set(records, Magic::V0, &mut buf);
    buf.freeze()
}

/// One length-prefixed request/response exchange with non-flexible v1 request
/// and v0 response headers; see [`kafka_wire::round_trip`]. Panics on an I/O
/// error.
pub use crate::kafka_wire::legacy_round_trip as round_trip_v0;

// ── Topic helpers ─────────────────────────────────────────────────────────────

// ── Test ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn produce_v0_upconverts_and_is_readable_via_fetch() {
    let p = support::start_legacy().await;

    // 1. Create topic "legacy_v0" with 1 partition using the typed client.
    let topic_setup = ConfiguredTopicSetup {
        name: "legacy_v0".into(),
        ..Default::default()
    };
    let cr = p
        .client
        .send(create_topic_request(creatable_topic(topic_setup)))
        .await
        .expect("CreateTopics");
    assert!(
        cr.topics[0].error_code == 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );

    // 2. Open a raw TCP connection to the broker to send the v0 Produce.
    let addr = p.broker.listen_addr();
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect for v0 Produce");
    stream.set_nodelay(true).ok();

    // 3. Build the v0 MessageSet with key="k", value="v".
    let messageset_bytes = build_v0_messageset(&[("k", "v")]);

    // 4. Build the kafka_3_6_2 ProduceRequest at v0 and encode it.
    let legacy_req = LegacyProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![LegacyTopicProduceData {
            name: "legacy_v0".to_string(),
            partition_data: vec![LegacyPartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::Legacy(messageset_bytes)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    legacy_req
        .encode(&mut body, 0)
        .expect("encode ProduceRequest v0");

    // 5. Round-trip over raw TCP.
    // Produce v0, correlation ID 1 and legacy headers are the setup defaults.
    let resp_body = round_trip_v0(
        &mut stream,
        LegacyExchangeSetup {
            client_id: "legacy-produce-test",
            ..Default::default()
        },
        &body,
    )
    .await;

    // 6. Decode the ProduceResponse v0.
    let mut cur: &[u8] = &resp_body;
    let produce_resp =
        LegacyProduceResponse::decode(&mut cur, 0).expect("decode ProduceResponse v0");
    assert!(
        produce_resp.responses.len() == 1,
        "expected 1 topic in response"
    );
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part_resp.error_code == 0,
        "produce v0 error_code: {}",
        part_resp.error_code
    );

    // 7. Fetch via the typed client (which negotiates the highest supported
    //    Fetch version). The data stored on disk is a v2 RecordBatch.
    let topic_id = topic_id_for(&p.client, "legacy_v0").await;
    let fetch_resp = p
        .client
        .send(single_partition_fetch(SinglePartitionFetchSetup {
            topic: "legacy_v0".to_owned(),
            topic_id,
            ..Default::default()
        }))
        .await
        .expect("Fetch");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch error_code: {}",
        part.error_code
    );

    let batch = part
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(<[_]>::first)
        .expect("v2 RecordBatch present after up-converted produce");

    assert!(
        batch.records.len() == 1,
        "expected 1 record in fetched batch"
    );
    let rec = &batch.records[0];
    assert!(
        rec.key.as_deref() == Some(b"k".as_ref()),
        "key mismatch: {:?}",
        rec.key
    );
    assert!(
        rec.value.as_deref() == Some(b"v".as_ref()),
        "value mismatch: {:?}",
        rec.value
    );

    p.broker.shutdown().await;
}
