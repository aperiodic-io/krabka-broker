//! Wire drivers for the data path the throttle actually caps: a PLAINTEXT
//! Produce that fills the partition, and a replica Fetch that measures how
//! many bytes come back.
//!
//! The Fetch driver frames its own request instead of reusing
//! `kafka_wire::round_trip`, because the assertion under test is the *size* of
//! the raw response and that has to be captured before decoding.

use std::net::SocketAddr;

use assert2::assert;
use bytes::{Buf, BytesMut};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{Decode, Encode};
use tokio::net::TcpStream;

use crate::{
    CLIENT_ID,
    kafka_wire::{self, ExchangeSetup as WireExchangeSetup},
    support::{
        fetch::{FetchLimits, RequestWaitMillis, named_topic_fetch},
        produce::{SinglePartitionProduceSetup, single_partition_produce},
        records::{batch_from_records, value_record},
        wire::{
            HeaderEncoding as RequestHeaderEncoding, WireFrameSetup, read_frame,
            request_body_capacity, request_frame, write_frame,
        },
    },
};

/// Produce `count` records of `record_bytes` bytes each to `(topic, 0)` over
/// a PLAINTEXT connection. Asserts `error_code=0` on the partition row.
pub async fn produce_plaintext(addr: SocketAddr, topic: &str, record_bytes: usize, count: usize) {
    const VERSION: i16 = 9; // flexible, pre-KIP-516 (no topic_id needed)

    use krabka_protocol::{
        owned::produce_response::ProduceResponse,
        records::{Record, RecordBatch},
    };

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| {
            value_record(
                i32::try_from(i).unwrap(),
                Some(bytes::Bytes::copy_from_slice(&value)),
            )
        })
        .collect();

    let req = single_partition_produce(
        // leader ack only (rf=1 topic)
        SinglePartitionProduceSetup {
            topic: topic.to_string(),
            records: Some(
                RecordBatch {
                    last_offset_delta: i32::try_from(count - 1).unwrap(),
                    ..batch_from_records(records)
                }
                .into(),
            ),
            ..Default::default()
        },
    );

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    // A partition record that the test has just changed reaches the broker's
    // partition a moment after the image shows it, and a producer retries the
    // leadership errors of that window as any Kafka client does.
    let mut attempts = 0;
    let error_code = loop {
        let resp_bytes = kafka_wire::round_trip(
            &mut stream,
            WireExchangeSetup {
                api_key: WireApiKey(0),
                version: WireApiVersion(VERSION),
                client_id: CLIENT_ID,
                header: RequestHeaderEncoding::Flexible,
                ..Default::default()
            },
            &body,
        )
        .await
        .expect("Produce round-trip");
        let mut cur: &[u8] = &resp_bytes;
        let resp = ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse");
        let error_code = resp.responses[0].partition_responses[0].error_code;
        attempts += 1;
        // LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER
        if !matches!(error_code, 5 | 6) || attempts == 100 {
            break error_code;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(
        error_code == 0,
        "Produce must succeed: error_code={error_code}"
    );
}

/// Issue a single Fetch request with `replica_id` over a PLAINTEXT
/// connection. A value `>= 0` means an inter-broker replica fetch, which the
/// leader-side throttle applies to. Returns the raw response payload byte
/// length.
pub async fn fetch_plaintext_replica(addr: SocketAddr, topic: &str, replica_id: i32) -> usize {
    const VERSION: i16 = 12; // flexible

    use krabka_protocol::owned::{fetch_request::FetchRequest, fetch_response::FetchResponse};

    let req = FetchRequest {
        replica_id,
        ..named_topic_fetch(topic, FetchLimits::one_mebibyte(RequestWaitMillis(0)))
    };

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Fetch");

    // Send raw frame and capture the full raw response (before decode) so we
    // can measure response bytes.
    let frame = request_frame(WireFrameSetup {
        api_key: krabka_ids::ApiKey(1),
        version: krabka_ids::ApiVersion(VERSION),
        header: RequestHeaderEncoding::Flexible,
        client_id: "krabka-throttle-test",
        body: &body,
        capacity: Some(request_body_capacity(&body)),
        ..Default::default()
    });

    write_frame(&mut stream, &frame, None).await.unwrap();

    let resp = read_frame(&mut stream).await.unwrap();

    // Decode to assert no transport error and no partition error.
    let mut cur: &[u8] = &resp[4..]; // skip corr_id
    let _tagged = cur.get_u8(); // v1 header tagged-fields
    let decoded = FetchResponse::decode(&mut cur, VERSION).expect("decode FetchResponse");
    // A refused row is small too, so a throttle check on the size alone would
    // pass for a fetch the leader never served.
    assert!(
        decoded.responses[0].partitions[0].error_code == 0,
        "replica fetch refused: {decoded:?}"
    );

    resp.len()
}
