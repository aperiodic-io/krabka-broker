//! Tests for finding deleted topics and for tombstoning their offsets in every
//! owned group.

use std::collections::HashMap;

use assert2::check;
use krabka_metadata::{DeleteTopicRecord, MetadataRecord, TopicRecord};
use uuid::Uuid;

use super::{deleted_topics, on_topics_deleted};
use crate::coordinator::{
    test_support::{metadata_delta_image as image, offset_entry as entry},
    unified::{
        classic_state::ClassicGroup,
        group::{CoordinatorGroup, GroupKind},
        test_support::make_coord_with_log,
    },
};

krabka_macros::delete_topic_request!(delete_request);

fn topic(name: &str, id: u128) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::from_u128(id),
        partitions: 1,
        replication_factor: 1,
    })
}

fn delete(name: &str) -> MetadataRecord {
    MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name: name.into() })
}

#[test]
fn deleted_topics_compares_topic_ids() {
    type Row = (&'static str, Vec<MetadataRecord>, Vec<(String, Uuid)>);
    let base = [topic("orders", 10), topic("payments", 20)];
    let orders = || vec![("orders".to_string(), Uuid::from_u128(10))];
    let rows: [Row; 4] = [
        ("no change", vec![], vec![]),
        ("a new topic", vec![topic("refunds", 30)], vec![]),
        ("one topic deleted", vec![delete("orders")], orders()),
        (
            "deleted and created again with the same name",
            vec![delete("orders"), topic("orders", 11)],
            orders(),
        ),
    ];
    for (name, changes, expected) in rows {
        let previous = image(&base);
        let next = image(&[base.as_slice(), changes.as_slice()].concat());
        check!(deleted_topics(&previous, &next) == expected, "{name}");
    }
}

/// One image can delete a topic and move a `__consumer_offsets` partition to
/// this broker. The load of that partition then applies the remembered
/// deletion, unless the topic name exists again: replayed offsets carry no
/// topic id, so they may belong to the new topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_partition_applies_the_remembered_deletions() {
    let rows: [(&str, Vec<MetadataRecord>, usize); 2] = [
        ("the name is gone", vec![], 1),
        ("the name exists again", vec![topic("orders", 11)], 0),
    ];
    for (name, current, expected_groups) in rows {
        let (coordinator, _log) = make_coord_with_log();
        super::remember_deletions(&coordinator, &[("orders".to_string(), Uuid::from_u128(10))]);
        let group = CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(ClassicGroup::new("g")),
            HashMap::from([(("orders".to_string(), 0), entry(1))]),
        );
        coordinator.seed_classic("g", Box::new(group));

        let changed = super::after_partition_load(&coordinator, &image(&current), |_| true).await;

        check!(changed.len() == expected_groups, "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_topics_deleted_tombstones_offsets_in_every_owned_group() {
    let (coordinator, log) = make_coord_with_log();
    for group_id in ["a", "b", "not-owned"] {
        let group = CoordinatorGroup::seeded(
            group_id,
            GroupKind::Classic(ClassicGroup::new(group_id)),
            HashMap::from([
                (("orders".to_string(), 0), entry(1)),
                (("payments".to_string(), 0), entry(2)),
            ]),
        );
        coordinator.seed_classic(group_id, Box::new(group));
    }

    let changed = on_topics_deleted(
        &coordinator,
        |group_id| group_id != "not-owned",
        &[("orders".to_string(), Uuid::from_u128(10))],
    )
    .await;

    check!(
        changed
            == vec![
                ("a".to_string(), vec![("orders".to_string(), 0)]),
                ("b".to_string(), vec![("orders".to_string(), 0)]),
            ]
    );
    check!(log.batches().await.len() == 2);
}

/// Through a running broker: a consumer commits an offset, the topic is
/// deleted and created again with the same name, and `OffsetFetch` then
/// answers no committed offset, as Kafka does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recreated_topic_does_not_inherit_the_old_committed_offsets() {
    use std::{sync::Arc, time::Duration};

    use krabka_protocol::owned::offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };

    use crate::{
        broker::Broker,
        codes,
        test_support::{peer, principal, request_context},
    };

    const TOPIC: &str = "orders";
    const GROUP: &str = "recreated-topic-group";
    /// The name-keyed versions, so the test does not track topic ids.
    const COMMIT_VERSION: i16 = 8;
    const FETCH_VERSION: i16 = 7;

    async fn fetched_offset(broker: &Broker) -> i64 {
        crate::coordinator::test_support::fetch_offset(broker, GROUP, TOPIC, FETCH_VERSION).await
    }

    let (handle, _dir) = crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
        crate::authorizer::AllowAllAuthorizer,
    ))
    .await;
    handle.wait_until_group_coordinator_ready().await;
    let broker = handle.broker_arc_for_test();
    let client = krabka_client_core::Client::builder()
        .bootstrap(handle.listen_addr().to_string())
        .client_id("topic-deletion-test")
        .build()
        .await
        .expect("client build");
    let create = || {
        crate::handlers::test_support::configured_topic_request(
            crate::handlers::test_support::CreateTopicSetup {
                topic: TOPIC,
                ..Default::default()
            },
        )
    };

    let created = client.send(create()).await.expect("CreateTopics");
    check!(created.topics[0].error_code == codes::NONE);
    handle.wait_until_partition_present(TOPIC, 0).await;
    let commit = OffsetCommitRequest {
        group_id: GROUP.into(),
        generation_id_or_member_epoch: -1,
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.into(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                committed_leader_epoch: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let principal_admin = principal("admin");
    let peer_addr = peer();
    let ctx = request_context(&principal_admin, &peer_addr, "consumer");
    let commit_offset = |offset: i64| {
        let mut request = commit.clone();
        request.topics[0].partitions[0].committed_offset = offset;
        let broker = Arc::clone(&broker);
        let ctx = &ctx;
        async move {
            let committed =
                crate::handlers::offset_commit::handle(&broker, request, COMMIT_VERSION, ctx)
                    .await
                    .expect("OffsetCommit");
            committed.topics[0].partitions[0].error_code
        }
    };
    check!(commit_offset(42).await == codes::NONE);
    check!(fetched_offset(&broker).await == 42);
    let old_id = handle
        .controller_image_for_test()
        .topic(TOPIC)
        .expect("topic in the image")
        .topic_id;

    let deleted = client
        .send(delete_request(TOPIC))
        .await
        .expect("DeleteTopics");
    check!(deleted.responses[0].error_code == codes::NONE);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while handle.controller_image_for_test().topic(TOPIC).is_some() {
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "topic never left the image"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let recreated = client.send(create()).await.expect("CreateTopics again");
    check!(recreated.topics[0].error_code == codes::NONE);
    handle.wait_until_partition_present(TOPIC, 0).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let offset = fetched_offset(&broker).await;
        if offset == -1 {
            break;
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "the recreated topic still reports offset {offset}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // An offset committed to the new topic survives a deletion of the old
    // topic that reaches the group late: it carries the new topic id.
    check!(commit_offset(7).await == codes::NONE);
    let changed = super::on_topics_deleted(
        &broker.group_coordinator,
        |_| true,
        &[(TOPIC.to_string(), old_id)],
    )
    .await;
    check!(changed.is_empty());
    check!(fetched_offset(&broker).await == 7);
    handle.shutdown().await;
}
