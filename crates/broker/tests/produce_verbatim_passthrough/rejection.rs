//! The two batches the broker must refuse before either append path runs: a
//! client-authored control batch, and a structurally malformed record body
//! whose CRC has been recomputed so the checksum alone cannot vouch for it.
//!
//! Both cases assert `INVALID_RECORD` (87) and a log end offset that is still
//! 0, which is what proves the rejection happened ahead of the append.

use {
    crate::{
        harness::{
            batch, boot, create_topic, encode_batch, produce_one, produce_payload, topic_id_for,
        },
        support::{client::connect_client, records::value_record},
    },
    assert2::check,
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_protocol::records::{
        Attributes, CRC_COVERAGE_START, HEADER_LEN, Record, RecordBatch, RecordsPayload,
    },
};

/// Control batches are broker-internal transaction markers. A client-authored
/// control batch must be rejected before either the verbatim or owned append
/// path can write it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_control_batch_is_rejected() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "ctrl").await;

    let client = connect_client(bootstrap.clone(), None).await;
    let topic_id = topic_id_for(&client, "ctrl").await;

    // A (non-compressed) control batch with one marker-shaped record.
    let mut b = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 99,
        producer_id: -1,
        ..RecordBatch::default()
    };
    b.attributes = Attributes::default().with_control(true);
    b.records.push(Record {
        key: Some(Bytes::from_static(&[0, 0, 0, 0])),
        ..value_record(0, Some(Bytes::from_static(&[0, 0, 0, 0])))
    });

    let error = produce_one(&client, "ctrl", topic_id, b)
        .await
        .expect_err("clients cannot append control batches");
    check!(error == 87);
    check!(broker.local_log_end_offset("ctrl", 0) == Some(0));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crc_valid_malformed_record_body_is_rejected() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&broker, &bootstrap, "malformed-body").await;

    let client = connect_client(bootstrap, None).await;
    let topic_id = topic_id_for(&client, "malformed-body").await;
    let mut wire = encode_batch(&batch(CompressionType::None, 1, b"valid")).to_vec();
    wire[HEADER_LEN] = 0; // zero-length first record body
    let crc = crc32c::crc32c(&wire[CRC_COVERAGE_START..]);
    wire[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());

    let error = produce_payload(
        &client,
        "malformed-body",
        topic_id,
        RecordsPayload::Raw(Bytes::from(wire)),
    )
    .await
    .expect_err("CRC alone cannot make a malformed record body valid");
    check!(error == 87);
    check!(broker.local_log_end_offset("malformed-body", 0) == Some(0));

    broker.shutdown().await;
}
