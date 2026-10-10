//! Down-conversion of a zstd-compressed v2 batch, which the legacy
//! `MessageSet` format cannot express and the broker therefore re-compresses
//! as snappy.
//!
//! The test reads the codec bits straight out of the wrapper message's
//! attributes byte, so it pins the codec id (2) that a legacy consumer sees
//! rather than only the decoded records.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_compression::CompressionType;
use krabka_protocol::{
    Decode,
    kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
    records::{Attributes, Record, RecordBatch},
};
use krabka_records_legacy::decode_message_set;

use crate::{
    harness::{fetch_legacy_raw, produce_batch},
    support,
    support::topics::{creatable_topic, create_topic_request},
};

#[tokio::test]
async fn fetch_v3_recompresses_zstd_as_snappy() {
    let p = support::start_legacy().await;

    // 1. Create topic.
    let cr = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("legacy_fetch_zstd").into(),
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

    // 2. Produce 50 zstd-compressed records so compression is exercised.
    let records: Vec<Record> = (0i32..50)
        .map(|i| Record {
            offset_delta: i,
            timestamp_delta: i64::from(i) * 1000,
            key: Some(Bytes::from(format!("key-{i:04}"))),
            value: Some(Bytes::from(format!(
                "val-{i:04} hello world this is a repeated test value"
            ))),
            ..Default::default()
        })
        .collect();
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Zstd),
        last_offset_delta: 49,
        records,
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_zstd", batch).await;

    // 3. Fetch v3 via raw TCP.
    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_zstd", 3).await;

    // 4. Decode as LegacyFetchResponse.
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    let part = &fetch_resp.responses[0].partitions[0];
    let legacy_bytes = crate::harness::legacy_bytes(
        part.error_code,
        part.records.as_ref(),
        "expected Legacy MessageSet in Fetch v3 response, got non-Legacy payload",
    );

    // 6. The outer wrapper message's attributes byte should carry snappy (2).
    // MessageSet format: offset(8) + message_size(4) + crc(4) + magic(1) + attributes(1)
    // So attributes byte is at index 17.
    assert!(
        legacy_bytes.len() > 17,
        "expected non-empty legacy bytes, got len={}",
        legacy_bytes.len()
    );
    let codec = legacy_bytes[17] & 0x07;
    assert!(
        codec == 2,
        "expected snappy codec id (2) in wrapper message attributes, got {codec}"
    );

    // 7. Verify the records decode correctly by round-tripping through decode_message_set.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len())
        .expect("decode_message_set on snappy-recompressed payload");
    assert!(
        recs.len() == 50,
        "expected 50 records after snappy decompression"
    );
    check!(
        recs[0].key.as_deref() == Some(b"key-0000".as_ref()),
        "first record key mismatch"
    );
    check!(
        recs[49].key.as_deref() == Some(b"key-0049".as_ref()),
        "last record key mismatch"
    );

    p.broker.shutdown().await;
}
