//! Integration test for `Consumer::seek`.
//!
//! A seek issued before the first poll takes effect once the partition is
//! assigned, so the consumer resumes from the sought offset instead of from
//! `auto.offset.reset`. This test proves that the seek wins over the
//! post-assignment prime, that it drops no pre-seek records, and that it skips
//! none above the sought offset.

mod support;

use krabka_client_consumer::{AutoOffsetReset, Consumer};

use crate::support::{
    client::connect_client,
    topics::{creatable_topic, create_topic_request},
};

async fn produce_n(bootstrap: &str, topic: &str, n: u32) {
    let producer = crate::support::producer::default_producer(bootstrap).await;
    for i in 0..n {
        producer
            .send(crate::support::producer::producer_record(
                crate::support::producer::ProducerRecordSetup {
                    topic: (topic).into(),
                    partition: Some(krabka_ids::PartitionIndex(0)),
                    key: Some(format!("k{i}").into()),
                    value: Some(format!("v{i}").into()),
                },
            ))
            .await
            .unwrap();
    }
    producer.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_before_first_poll_resumes_from_sought_offset() {
    let (_dir, _broker, bootstrap, _admin) = crate::support::client::standalone_topic("s").await;

    // Offsets 0..=4 on partition 0.
    produce_n(&bootstrap, "s", 5).await;

    // Fresh group with Earliest: without a seek this would read from offset 0.
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("seek-group")
        .subscribe(vec!["s".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    // Seek to offset 2 *before* the first poll — i.e. before the partition is
    // even guaranteed assigned. The consumer must hold this pending and apply
    // it after assignment, before any fetch.
    consumer.seek("s", 0, 2).await.unwrap();

    // Collect until we have the 3 expected records (offsets 2,3,4).
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while got.len() < 3 && std::time::Instant::now() < deadline {
        let recs = consumer.poll(krabka_units::millis(500)).await.unwrap();
        got.extend(recs);
    }

    let offsets: Vec<i64> = got.iter().map(|r| r.offset).collect();
    // No pre-seek record (offset 0 or 1) is ever delivered, and nothing above
    // the seek is skipped: exactly 2, 3, 4.
    assert2::assert!(offsets == vec![2, 3, 4]);

    consumer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_rejects_negative_offset() {
    let (_dir, broker) = crate::support::standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();

    let admin = connect_client(&bootstrap, None).await;
    admin
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("n").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();

    let consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("neg-group")
        .subscribe(vec!["n".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    let err = consumer.seek("n", 0, -1).await;
    assert2::assert!(err.is_err());

    // Offset 0 is a valid seek target (re-read from the beginning): the reject
    // boundary is strictly `offset < 0`, so 0 must be accepted.
    assert2::assert!(consumer.seek("n", 0, 0).await.is_ok());

    consumer.close().await.unwrap();
}
