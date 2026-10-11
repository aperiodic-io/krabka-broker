//! The two paths that take data out of a topic, which a freeze also refuses.
//!
//! An operator who froze a topic for a disaster-recovery promotion needs the
//! frozen prefix byte-identical between sites, and both `DeleteRecords` and
//! `DeleteTopics` destroy exactly that. A freeze that stopped new writes and
//! let a trim through would be worse than no freeze at all.

use assert2::check;
use krabka_broker::codes;
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::freeze::PATTERN_TYPE_LITERAL,
    owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
};

use crate::{
    control_plane::freeze_scope,
    support::offsets::{delete_records_partition, delete_records_request, delete_records_topic},
    wire::{CONTROL, controlled_fixture, produce},
};

/// Trim `topic` up to `offset`, and hand back the partition row's error code.
async fn delete_records(client: &Client, topic: &str, offset: i64) -> i16 {
    let response = client
        .send(delete_records_request(
            vec![delete_records_topic(
                topic,
                vec![delete_records_partition(0, offset)],
            )],
            5_000,
        ))
        .await
        .expect("DeleteRecords");
    response.topics[0].partitions[0].error_code
}

/// Delete `topic`, and hand back the topic row's error code.
async fn delete_topic(client: &Client, topic: &str) -> i16 {
    let response = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(topic.to_owned()),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteTopics");
    response.responses[0].error_code
}

/// A freeze refuses both paths that take data out of the topic.
///
/// A freeze that stopped new writes and let a trim through would be worse than
/// no freeze at all: an operator who froze a topic for a disaster-recovery
/// promotion needs the frozen prefix byte-identical between sites, and both of
/// these paths destroy exactly that. The break-glass workflow is deliberately
/// off on this broker, so nothing but the freeze can produce the refusal, and
/// the control topic proves both paths still work when nothing is frozen.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_and_delete_topics_refuse_on_a_frozen_topic() {
    let (p, frozen, control) = Box::pin(controlled_fixture("orders")).await;
    for _ in 0..2 {
        check!(produce(&p.client, "orders", frozen).await.error_code == codes::NONE);
        check!(produce(&p.client, CONTROL, control).await.error_code == codes::NONE);
    }

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "promotion").await;

    check!(delete_records(&p.client, "orders", 1).await == codes::POLICY_VIOLATION);
    check!(p.broker.partition_log_start_for_test("orders", 0) == Some(0));
    check!(p.broker.local_log_end_offset("orders", 0) == Some(2));

    check!(delete_topic(&p.client, "orders").await == codes::POLICY_VIOLATION);
    check!(p.broker.partition_exists_for_test("orders", 0));

    // The control topic takes both, so the two refusals above are the freeze
    // and not a broker that refuses every deletion.
    check!(delete_records(&p.client, CONTROL, 1).await == codes::NONE);
    check!(delete_topic(&p.client, CONTROL).await == codes::NONE);

    p.broker.shutdown().await;
}
