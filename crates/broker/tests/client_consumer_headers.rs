mod support;

use krabka_client_consumer::{AutoOffsetReset, Consumer, Header as ConsumerHeader};
use krabka_client_producer::{Header, ProducerRecord};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_record_carries_headers() {
    let (_dir, _broker, bootstrap, _admin) = crate::support::client::standalone_topic("h").await;

    let producer = crate::support::producer::default_producer(&bootstrap).await;
    producer
        .send(ProducerRecord {
            headers: vec![Header {
                key: "trace".into(),
                value: Some("abc".into()),
            }],
            ..crate::support::producer::producer_record(
                crate::support::producer::ProducerRecordSetup {
                    topic: ("h").into(),
                    value: Some("v".into()),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    producer.flush().await.unwrap();
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("g")
        .subscribe(vec!["h".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let recs = loop {
        let r = consumer.poll(krabka_units::secs(2)).await.unwrap();
        if !r.is_empty() {
            break r;
        }
    };
    assert2::assert!(
        recs[0].headers
            == vec![ConsumerHeader {
                key: "trace".into(),
                value: Some("abc".into()),
            }]
    );
}
