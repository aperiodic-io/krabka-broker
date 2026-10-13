//! Produce up-conversion from v0/v1 `MessageSet` to v2.
//!
//! The broker's Produce handler accepts a `RecordsPayload::Legacy` arm. It
//! passes incoming v0/v1 `MessageSet` bytes through
//! `krabka_records_legacy::legacy_to_v2`, and gives the resulting v2 batch to
//! the existing log-append path.
//!
//! These tests exercise the conversion without the wire protocol's version
//! negotiation. They build a Legacy payload directly, assert that the broker
//! stores the up-converted records, and then fetch those records back in the
//! modern v2 form.

use {
    crate::support::{
        client::create_topic,
        discovery::named_topic_metadata,
        fetch::single_partition_fetch,
        produce::{SinglePartitionProduceSetup, single_partition_produce},
    },
    assert2::assert,
};
mod support;

use {
    bytes::{Bytes, BytesMut},
    krabka_ids::Offset,
    krabka_protocol::{
        owned::fetch_request::FetchRequest, primitives::uuid::Uuid, records::RecordsPayload,
    },
    krabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set},
};

async fn topic_id_for(p: &support::InProcess, name: &str) -> Uuid {
    let resp = p
        .client
        .send(named_topic_metadata(name))
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response")
}

/// Builds a flat, uncompressed v1 `MessageSet` that carries `values` as
/// successive records at offsets 0 to N-1.
fn build_v1_message_set(values: &[&[u8]]) -> Bytes {
    let recs: Vec<ParsedRecord> = values
        .iter()
        .enumerate()
        .map(|(i, v)| ParsedRecord {
            offset: Offset(i64::try_from(i).expect("offset fits in i64")),
            timestamp: Some(1_700_000_000 + i64::try_from(i).expect("ts offset fits in i64")),
            key: None,
            value: Some(Bytes::copy_from_slice(v)),
        })
        .collect();
    let mut buf = BytesMut::new();
    encode_flat_message_set(recs, Magic::V1, &mut buf);
    buf.freeze()
}

async fn produce_legacy(
    p: &support::InProcess,
    topic: &str,
    topic_id: Uuid,
    records: Bytes,
) -> i16 {
    let req = single_partition_produce(SinglePartitionProduceSetup {
        topic: topic.into(),
        topic_id,
        records: Some(RecordsPayload::Legacy(records)),
        ..Default::default()
    });
    let resp = p.client.send(req).await.expect("Produce");
    resp.responses[0].partition_responses[0].error_code
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_v1_message_set_is_upconverted_and_round_trips() {
    let p = support::start().await;
    create_topic(&p.client, "legacy", 1).await;
    let topic_id = topic_id_for(&p, "legacy").await;

    let legacy_bytes = build_v1_message_set(&[b"alpha", b"beta", b"gamma"]);

    let err = produce_legacy(&p, "legacy", topic_id, legacy_bytes).await;
    assert!(err == 0, "Produce up-conversion must succeed: {err}");

    // Fetch the stored records back and verify they survived the
    // up-conversion. The wire response is v2, so the fetched batch
    // carries the same values.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let batch = loop {
        let fr = p
            .client
            .send(FetchRequest {
                replica_id: -1,
                ..single_partition_fetch()
                    .topic("legacy".into())
                    .topic_id(topic_id)
                    .call()
            })
            .await
            .expect("Fetch");
        let part = &fr.responses[0].partitions[0];
        assert!(part.error_code == 0);
        if let Some(batch) = part
            .records
            .as_ref()
            .and_then(|p| p.as_v2())
            .and_then(<[_]>::first)
        {
            break batch.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Fetch did not return the committed v2 batch within 5s"
        );
        tokio::task::yield_now().await;
    };
    assert!(batch.records.len() == 3);
    let values: Vec<&[u8]> = batch
        .records
        .iter()
        .map(|r| r.value.as_deref().unwrap_or(&[]))
        .collect();
    assert!(values == vec![&b"alpha"[..], &b"beta"[..], &b"gamma"[..]]);

    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_malformed_legacy_bytes_returns_invalid_record() {
    let p = support::start().await;
    create_topic(&p.client, "bad", 1).await;
    let topic_id = topic_id_for(&p, "bad").await;

    // 100 bytes of garbage that look superficially like a legacy
    // MessageSet (byte 16 != 2 → routed to Legacy arm) but fail CRC
    // when parsed. The handler must surface INVALID_RECORD (87), not
    // panic or wedge. The size field frames the whole set: a field below
    // Kafka's 14-byte minimum is `CORRUPT_MESSAGE` before anything is parsed.
    let mut garbage = vec![0u8; 100];
    garbage[8..12].copy_from_slice(&88_i32.to_be_bytes());
    let err = produce_legacy(&p, "bad", topic_id, Bytes::from(garbage)).await;
    assert!(
        err == 87,
        "malformed legacy bytes must surface INVALID_RECORD (87): {err}"
    );

    p.broker.shutdown().await;
}
