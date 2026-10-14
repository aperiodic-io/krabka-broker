//! Tests for finding the changed topics of an image and for the refresh
//! request that goes to the groups this broker coordinates.

use {
    super::{changed_topics, on_metadata_update, regex_resolution_may_change},
    crate::{
        coordinator::{
            test_support::metadata_delta_image as image,
            unified::{
                actor::test_support::rpc::consumer_request_as_client as heartbeat,
                test_support::{
                    SwitchableMetadata, consumer_join_request, make_coord_with_metadata,
                    snapshot_of,
                },
            },
        },
        test_support::allow_acl,
    },
    assert2::check,
    krabka_metadata::{
        BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, NodeId, PartitionRecord,
        TopicConfigRecord, TopicRecord,
    },
    krabka_protocol::owned::{
        common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
    },
    std::collections::BTreeMap,
    uuid::Uuid,
};

fn topic(name: &str, id: u128) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::from_u128(id),
        partitions: 0,
        replication_factor: 2,
    })
}

fn partition(name: &str, index: i32, leader: u64) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: name.into(),
        partition: index,
        leader: NodeId(leader),
        replicas: vec![NodeId(1), NodeId(2)],
        isr: vec![NodeId(1), NodeId(2)],
        ..Default::default()
    })
}

/// Kafka's `TopicsDelta.changedTopics` holds every topic with a
/// `TopicRecord`, `PartitionRecord` or `PartitionChangeRecord` in the delta,
/// and `deletedTopicIds` the deleted ones. A configuration or a broker is not
/// a topic change.
#[test]
fn changed_topics_are_the_created_changed_and_deleted_topics() {
    struct Row {
        name: &'static str,
        changes: Vec<MetadataRecord>,
        topics: Vec<&'static str>,
    }
    let base = [
        topic("orders", 10),
        partition("orders", 0, 1),
        topic("payments", 20),
        partition("payments", 0, 1),
    ];
    let rows: [Row; 8] = [
        Row {
            name: "no change",
            changes: vec![],
            topics: vec![],
        },
        Row {
            name: "a topic is created",
            changes: vec![topic("refunds", 30), partition("refunds", 0, 1)],
            topics: vec!["refunds"],
        },
        Row {
            name: "a topic grows",
            changes: vec![partition("orders", 1, 2)],
            topics: vec!["orders"],
        },
        Row {
            name: "a partition gets a new leader",
            changes: vec![partition("orders", 0, 2)],
            topics: vec!["orders"],
        },
        Row {
            name: "a topic is deleted",
            changes: vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "payments".into(),
            })],
            topics: vec!["payments"],
        },
        Row {
            name: "a topic is deleted and created again",
            changes: vec![
                MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: "orders".into(),
                }),
                topic("orders", 11),
                partition("orders", 0, 1),
            ],
            topics: vec!["orders"],
        },
        Row {
            name: "a topic configuration changes",
            changes: vec![orders_retention_config()],
            topics: vec![],
        },
        Row {
            name: "a broker registers",
            changes: vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    broker_epoch: 3,
                    incarnation_id: Uuid::from_u128(3),
                    host: "broker-3".into(),
                    rack: Some("rack-c".into()),
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(3))
                },
            )],
            topics: vec![],
        },
    ];
    let mut found = Vec::new();
    let mut expected = Vec::new();
    let previous = image(&base);
    for Row {
        name,
        changes,
        topics,
    } in rows
    {
        let next = image(&[base.as_slice(), changes.as_slice()].concat());
        found.push((name, changed_topics(&previous, &next)));
        expected.push((name, topics.into_iter().map(str::to_owned).collect()));
    }
    check!(found == expected);
}

fn describe_acl(topic: &str) -> MetadataRecord {
    MetadataRecord::V1AccessControlEntry(
        allow_acl()
            .resource_name(topic)
            .operation(krabka_metadata::AclOperation::Describe)
            .call(),
    )
}

/// A created topic and a changed ACL can change what a regular expression
/// resolves to. A deleted topic, a grown one, a new leader and a
/// configuration cannot: the assignment follows those through the metadata
/// hash, and the next resolution drops a deleted topic.
#[test]
fn a_new_topic_or_a_changed_acl_can_change_a_resolution() {
    struct Row {
        name: &'static str,
        changes: Vec<MetadataRecord>,
        may_change: bool,
    }
    let base = [
        topic("orders", 10),
        partition("orders", 0, 1),
        topic("payments", 20),
        partition("payments", 0, 1),
        describe_acl("orders"),
    ];
    let rows: [Row; 9] = [
        Row {
            name: "no change",
            changes: vec![],
            may_change: false,
        },
        Row {
            name: "a topic is created",
            changes: vec![topic("refunds", 30), partition("refunds", 0, 1)],
            may_change: true,
        },
        Row {
            name: "a topic is deleted and created again with a new id",
            changes: vec![
                MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: "orders".into(),
                }),
                topic("orders", 11),
                partition("orders", 0, 1),
            ],
            may_change: true,
        },
        Row {
            name: "an ACL is granted",
            changes: vec![describe_acl("payments")],
            may_change: true,
        },
        Row {
            name: "an ACL is deleted",
            changes: vec![MetadataRecord::V1DeleteAccessControlEntry(
                krabka_metadata::AclEntryFilter {
                    resource_type: Some(krabka_metadata::ResourceType::Topic),
                    resource_name: Some("orders".into()),
                    ..Default::default()
                },
            )],
            may_change: true,
        },
        Row {
            name: "a topic is deleted",
            changes: vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "payments".into(),
            })],
            may_change: false,
        },
        Row {
            name: "a topic grows",
            changes: vec![partition("orders", 1, 2)],
            may_change: false,
        },
        Row {
            name: "a partition gets a new leader",
            changes: vec![partition("orders", 0, 2)],
            may_change: false,
        },
        Row {
            name: "a topic configuration changes",
            changes: vec![orders_retention_config()],
            may_change: false,
        },
    ];
    let mut found = Vec::new();
    let mut expected = Vec::new();
    let previous = image(&base);
    for Row {
        name,
        changes,
        may_change,
    } in rows
    {
        let next = image(&[base.as_slice(), changes.as_slice()].concat());
        found.push((name, regex_resolution_may_change(&previous, &next)));
        expected.push((name, may_change));
    }
    check!(found == expected);
}

/// Only the groups whose `__consumer_offsets` partition this broker leads
/// refresh their metadata, as Kafka applies an image to its active shards
/// only. Each group has one member, which subscribed to `orders` before the
/// topic existed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metadata_update_refreshes_the_groups_this_broker_coordinates() {
    let metadata = SwitchableMetadata::new(snapshot_of(&[]));
    let coordinator = make_coord_with_metadata(metadata.clone());
    let groups = ["a", "b", "not-owned"];
    for group_id in groups {
        let handle = coordinator.get_or_create_consumer(group_id);
        let joined = heartbeat(
            &handle,
            consumer_join_request()
                .group_id(group_id)
                .topics(&["orders"])
                .call(),
        )
        .await;
        check!(joined.member_epoch == 2, "{group_id}");
    }

    metadata.set(snapshot_of(&[("orders", 1, 2)]));
    on_metadata_update(&coordinator, |group_id| group_id != "not-owned", &[
        "orders".to_string(),
    ])
    .await;

    let mut answers = Vec::new();
    for group_id in groups {
        let handle = coordinator.find(group_id).expect("the group's actor");
        let answer = heartbeat(&handle, ConsumerGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch: 2,
            rebalance_timeout_ms: -1,
            ..Default::default()
        })
        .await;
        answers.push((group_id, answer));
    }

    let answer = |member_epoch, assignment: Option<Vec<i32>>| ConsumerGroupHeartbeatResponse {
        member_id: Some("m1".into()),
        member_epoch,
        heartbeat_interval_ms: 5_000,
        assignment: assignment.map(|partitions| Assignment {
            topic_partitions: vec![TopicPartitions {
                topic_id: krabka_protocol::primitives::uuid::Uuid([1; 16]),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    check!(
        answers
            == vec![
                ("a", answer(3, Some(vec![0, 1]))),
                ("b", answer(3, Some(vec![0, 1]))),
                ("not-owned", answer(2, None)),
            ]
    );
}

fn orders_retention_config() -> MetadataRecord {
    MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides: BTreeMap::from([("retention.ms".into(), "1000".into())]),
    })
}
