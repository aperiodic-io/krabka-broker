//! The two record-preserving down-conversions: Fetch v3, which yields a
//! `MessageSet` whose key and value pairs survive the rewrite, and Fetch v0,
//! whose `Magic::V0` framing has no timestamp field at all.
//!
//! The v0 case asserts the magic byte at index 16 of the `MessageSet` directly,
//! because a v1 framing would decode without error yet carry a create-time the
//! v0 client cannot represent.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::metrics::TopicLabel;
use krabka_protocol::{
    Decode,
    kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
    records::{Record, RecordBatch, RecordsPayload},
};
use krabka_records_legacy::decode_message_set;

use crate::{
    harness::{
        create_topic, create_topic_with_partitions, fetch_legacy_raw, fetch_legacy_raw_partitions,
        produce_batch, produce_batch_to,
    },
    support,
    support::topics::{creatable_topic, create_topic_request},
};

#[tokio::test]
async fn fetch_v3_downconverts_v2_batch_to_v0_messageset() {
    let p = support::start_legacy().await;

    // 1. Create topic.
    let cr = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("legacy_fetch_basic").into(),
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

    // 2. Produce 2 records via modern path (ProduceRequest v9).
    let batch = RecordBatch {
        records: vec![
            Record {
                offset_delta: 0,
                key: Some(Bytes::from_static(b"key0")),
                value: Some(Bytes::from_static(b"val0")),
                ..Default::default()
            },
            Record {
                offset_delta: 1,
                key: Some(Bytes::from_static(b"key1")),
                value: Some(Bytes::from_static(b"val1")),
                ..Default::default()
            },
        ],
        last_offset_delta: 1,
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_basic", batch).await;

    // 3. Fetch v3 via raw TCP.
    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_basic", 3).await;

    // 4. Decode as LegacyFetchResponse (Fetch v3 is non-flexible).
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    assert!(
        fetch_resp.responses.len() == 1,
        "expected 1 topic in fetch response"
    );
    let part = &fetch_resp.responses[0].partitions[0];
    let legacy_bytes = crate::harness::legacy_bytes(
        part.error_code,
        part.records.as_ref(),
        "expected Legacy MessageSet in Fetch v3 response, got non-Legacy payload",
    );

    // 6. Decode the MessageSet and verify key/value pairs.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");

    assert!(
        recs.len() == 2,
        "expected 2 records in MessageSet; got {}",
        recs.len()
    );
    for (i, key, value) in [
        (0usize, b"key0" as &[u8], b"val0" as &[u8]),
        (1, b"key1", b"val1"),
    ] {
        check!(
            recs[i].key.as_deref() == Some(key),
            "record {i} key mismatch"
        );
        check!(
            recs[i].value.as_deref() == Some(value),
            "record {i} value mismatch"
        );
    }

    p.broker.shutdown().await;
}

/// Fetch v0 maps to `Magic::V0`, which has no per-message timestamp.
///
/// The test produces a batch with timestamps through the modern path, then
/// fetches at v0 and confirms that the down-converted `MessageSet` strips
/// them. This drives the `request_version < 2` branch of
/// `down_convert_for_fetch` through the full Fetch handler, not through the
/// unit helper.
#[tokio::test]
async fn fetch_v0_downconverts_to_magic_v0_without_timestamps() {
    let p = support::start_legacy().await;
    create_topic(&p.client, "legacy_fetch_v0").await;

    // base_timestamp + per-record delta give a non-zero create-time that
    // a v1 MessageSet would carry but v0 must drop.
    let batch = RecordBatch {
        base_timestamp: 1_700_000_000,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            timestamp_delta: 500,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_v0", batch).await;

    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_v0", 0).await;
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 0).expect("decode LegacyFetchResponse v0");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );

    let legacy_bytes = match part.records.as_ref().expect("records field should be Some") {
        RecordsPayload::Legacy(b) => b.clone(),
        _ => {
            panic!("expected Legacy MessageSet in Fetch v0 response")
        }
    };

    // MessageSet layout: offset(8) + message_size(4) + crc(4) + magic(1).
    // The magic byte sits at index 16 and must be 0 for a v0 MessageSet.
    assert!(legacy_bytes.len() > 16, "legacy bytes too short");
    assert!(legacy_bytes[16] == 0, "expected v0 MessageSet magic byte 0");

    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");
    assert!(recs.len() == 1, "expected 1 record");
    check!(recs[0].key.as_deref() == Some(b"k".as_ref()));
    check!(recs[0].value.as_deref() == Some(b"v".as_ref()));
    check!(
        recs[0].timestamp == None,
        "v0 MessageSet must carry no timestamp"
    );

    p.broker.shutdown().await;
}

/// A legacy Fetch that reads several partitions of one topic converts, and
/// meters, every one of them.
///
/// `downconvert_legacy_responses` resolves the topic name once per topic entry
/// and then walks that entry's partitions, recording one
/// `fetch_message_conversions` observation per converted partition under a
/// single `topic` label. Every other case in this suite fetches one partition,
/// so none of them can tell a per-topic resolution from a per-partition one,
/// nor catch a loop that meters only the first row.
#[tokio::test]
async fn fetch_v3_meters_every_converted_partition_of_a_topic() {
    const TOPIC: &str = "legacy_fetch_multi";
    const PARTITIONS: [i32; 3] = [0, 1, 2];

    let p = support::start_legacy().await;
    create_topic_with_partitions(&p.client, TOPIC, 3).await;
    let addr = p.broker.listen_addr();

    for partition in PARTITIONS {
        p.broker
            .wait_until_partition_present(TOPIC, partition)
            .await;
        let batch = RecordBatch {
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::from(format!("key{partition}"))),
                value: Some(Bytes::from(format!("val{partition}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        produce_batch_to(addr, TOPIC, partition, batch).await;
        p.broker
            .wait_until_high_watermark(TOPIC, partition, 1)
            .await;
    }

    let before = p
        .broker
        .metrics()
        .fetch_message_conversions
        .get_or_create(&TopicLabel {
            topic: TOPIC.into(),
        })
        .get();

    let resp_body = fetch_legacy_raw_partitions(addr, TOPIC, 3, &PARTITIONS, 0).await;
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    assert!(
        fetch_resp.responses.len() == 1,
        "the three partitions belong to one topic entry"
    );
    let rows = &fetch_resp.responses[0].partitions;
    assert!(
        rows.len() == PARTITIONS.len(),
        "expected one row per requested partition; got {}",
        rows.len()
    );

    // Every row came back down-converted and carrying its own record, so
    // every row is one the conversion counter must have seen.
    for row in rows {
        check!(
            row.error_code == 0,
            "partition {} error: {}",
            row.partition_index,
            row.error_code
        );
        let legacy_bytes = match row.records.as_ref().expect("records field should be Some") {
            RecordsPayload::Legacy(b) => b.clone(),
            _ => panic!(
                "expected Legacy MessageSet for partition {}",
                row.partition_index
            ),
        };
        let mut ms_cur: &[u8] = &legacy_bytes;
        let recs = decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");
        check!(recs.len() == 1, "partition {}", row.partition_index);
        check!(
            recs[0].value.as_deref() == Some(format!("val{}", row.partition_index).as_bytes()),
            "partition {} carries another partition's record",
            row.partition_index
        );
    }

    let after = p
        .broker
        .metrics()
        .fetch_message_conversions
        .get_or_create(&TopicLabel {
            topic: TOPIC.into(),
        })
        .get();
    assert!(
        after - before == PARTITIONS.len() as u64,
        "one conversion per converted partition under the one topic label; \
         counter moved by {}",
        after - before
    );

    p.broker.shutdown().await;
}
