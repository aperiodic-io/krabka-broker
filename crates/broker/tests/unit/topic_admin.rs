//! `CreateTopics` and `DeleteTopics` over one broker.
//!
//! The suite covers the admin round trip and the three definitions the broker
//! refuses: no partition, a name that already exists, and a replication factor
//! above the number of brokers.

use assert2::{assert, check};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
};

use crate::{
    support,
    support::topics::{creatable_topic, create_topic_request},
};

#[tokio::test]
async fn create_then_delete_topic_round_trip() {
    let p = support::start().await;

    let create = create_topic_request(creatable_topic(
        crate::support::topics::ConfiguredTopicSetup {
            name: ("alpha").into(),
            partitions: crate::support::topics::TopicPartitionCount(2),
            ..Default::default()
        },
    ));
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].error_code == 0);
    check!(resp.topics[0].num_partitions == 2);

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("alpha".into()),
            ..Default::default()
        }],
        topic_names: vec!["alpha".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let dresp = p.client.send(delete).await.expect("DeleteTopics");
    assert!(dresp.responses.len() == 1);
    assert!(dresp.responses[0].error_code == 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_topic_with_zero_partitions_errors() {
    let p = support::start().await;
    let create = create_topic_request(creatable_topic(
        crate::support::topics::ConfiguredTopicSetup {
            name: ("zero").into(),
            partitions: crate::support::topics::TopicPartitionCount(0),
            ..Default::default()
        },
    ));
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert!(resp.topics[0].error_code == 37); // INVALID_PARTITIONS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn duplicate_create_returns_topic_already_exists() {
    let p = support::start().await;
    let req = || {
        create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("dup").into(),
                ..Default::default()
            },
        ))
    };
    let r1 = p.client.send(req()).await.expect("CreateTopics 1");
    assert!(r1.topics[0].error_code == 0);
    let r2 = p.client.send(req()).await.expect("CreateTopics 2");
    assert!(r2.topics[0].error_code == 36); // TOPIC_ALREADY_EXISTS
    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_topics_rf_too_high_returns_invalid_replication_factor() {
    let p = support::start().await; // single-voter broker
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "boom".into(),
                num_partitions: 1,
                replication_factor: 5, // single broker → RF=5 is invalid
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        resp.topics[0].error_code == 38 /* INVALID_REPLICATION_FACTOR */
    );
    p.broker.shutdown().await;
}
