//! Handler tests for how `OffsetCommit` fences a commit against the group,
//! as Kafka's `OffsetMetadataManager.validateOffsetCommit` does through
//! `ClassicGroup.validateOffsetCommit` and `ConsumerGroup.validateOffsetCommit`,
//! and how it refuses a partition whose metadata is too large.

use std::sync::Arc;

use assert2::assert;
use krabka_metadata::ResourceType;
use krabka_protocol::owned::{
    create_topics_request::{self},
    offset_commit_request::{
        OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    },
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
};

use super::WireUuid;
use crate::{
    authorizer::{AllowAllAuthorizer, AuthorizationResult},
    broker::{Broker, BrokerHandle},
    codes,
    coordinator::unified::{
        GroupSeed,
        actor::{GroupActorMessage, GroupKindTag},
        classic_state::{ClassicGroup, GroupState},
        group::{CoordinatorGroup, GroupKind},
        persistence_next_gen::{
            AssignedTopicPartitions, ClassicMemberMetadata, CurrentMemberAssignmentValue,
            CurrentTopicPartitions, MemberAssignmentState, MemberMetadataValue,
            TargetAssignmentMemberValue,
        },
    },
    handlers::test_support::CreateTopicSetup,
    test_support::{dispatch_context, encode_request, peer, principal},
};

const TOPIC: &str = "group-validation";
const MEMBER: &str = "m1";
const GENERATION: i32 = 3;

/// The group that one row commits to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    /// No group of that id exists.
    Missing,
    /// A `Stable` classic group that holds `MEMBER` at `GENERATION`.
    ClassicWithMember,
    /// A consumer (KIP-848) group with no members.
    EmptyConsumer,
}

/// One row: the group, the request version, member id and generation, and
/// the error code Kafka answers on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    group: Group,
    version: i16,
    member_id: &'static str,
    generation: i32,
    error_code: i16,
}

/// The partitions of `TOPIC` in the KIP-1251 test: partition 2 exists but is
/// assigned to no member.
const KIP_1251_TOPIC_PARTITIONS: i32 = 3;

async fn create_topic(broker: &Broker) {
    create_topic_with_partitions(broker, 1).await;
}

async fn create_topic_with_partitions(broker: &Broker, num_partitions: i32) {
    request_identity!(
        (admin, address, ctx),
        principal("admin"),
        client_id = "group-validation-admin"
    );
    let request = crate::handlers::test_support::configured_topic_request(CreateTopicSetup {
        topic: TOPIC,
        num_partitions: crate::handlers::test_support::TopicPartitionCount(num_partitions),
        ..Default::default()
    });
    dispatch_context(
        broker,
        create_topics_request::API_KEY,
        create_topics_request::MAX_VERSION,
        &encode_request(&request, create_topics_request::MAX_VERSION),
        &ctx,
    )
    .await;
}

fn seed(broker: &Broker, group_id: &str, group: Group) {
    match group {
        Group::Missing => {}
        Group::ClassicWithMember => {
            let mut state = ClassicGroup::new(group_id);
            state.protocol_type = Some("consumer".into());
            state.add_member(
                crate::coordinator::unified::actor::test_support::classic_member(MEMBER),
            );
            state.state = GroupState::Stable;
            state.generation_id = GENERATION;
            let seeded = CoordinatorGroup::seeded(
                group_id,
                GroupKind::Classic(state),
                std::collections::HashMap::new(),
            );
            broker
                .group_coordinator
                .seed_classic(group_id, Box::new(seeded));
        }
        Group::EmptyConsumer => {
            let _ = broker
                .group_coordinator
                .get_or_create_group(group_id, GroupKindTag::Consumer);
        }
    }
}

/// Runs one row in a group of its own, and returns the response and whether
/// the group exists afterwards.
async fn drive(broker: &BrokerHandle, row: usize, case: Case) -> (OffsetCommitResponse, bool) {
    let shared = broker.broker_arc_for_test();
    let group_id = format!("group-validation-{row}");
    seed(&shared, &group_id, case.group);
    let request = OffsetCommitRequest {
        group_id: group_id.clone(),
        generation_id_or_member_epoch: case.generation,
        member_id: case.member_id.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    request_identity!(
        (user, address, ctx),
        principal("consumer"),
        client_id = "consumer-client"
    );
    let response = crate::test_support::dispatch_wire(
        &shared,
        krabka_protocol::owned::offset_commit_request::API_KEY,
        case.version,
        &request,
        &ctx,
    )
    .await;
    (response, shared.group_coordinator.find(&group_id).is_some())
}

#[tokio::test]
async fn commit_is_fenced_by_kafka_group_rule() {
    let cases = [
        // A group that does not exist is created as a simple group for a
        // negative generation.
        Case {
            group: Group::Missing,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::NONE,
        },
        // Otherwise it is not found from v9 on, and an illegal generation
        // before, and it is not created.
        Case {
            group: Group::Missing,
            version: 9,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::GROUP_ID_NOT_FOUND,
        },
        Case {
            group: Group::Missing,
            version: 8,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::ILLEGAL_GENERATION,
        },
        // The admin client may not move the offsets of a classic group that
        // has members.
        Case {
            group: Group::ClassicWithMember,
            version: 8,
            member_id: "",
            generation: -1,
            error_code: codes::UNKNOWN_MEMBER_ID,
        },
        Case {
            group: Group::ClassicWithMember,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::UNKNOWN_MEMBER_ID,
        },
        Case {
            group: Group::ClassicWithMember,
            version: 9,
            member_id: MEMBER,
            generation: GENERATION,
            error_code: codes::NONE,
        },
        // The admin client may move the offsets of a consumer group that has
        // no members.
        Case {
            group: Group::EmptyConsumer,
            version: 9,
            member_id: "",
            generation: -1,
            error_code: codes::NONE,
        },
    ];
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    create_topic(&broker.broker_arc_for_test()).await;
    broker.wait_until_partition_present(TOPIC, 0).await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (row, case) in cases.into_iter().enumerate() {
        let (response, exists) = drive(&broker, row, case).await;
        actual.push((case.group, case.version, case.member_id, response, exists));
        expected.push((
            case.group,
            case.version,
            case.member_id,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: TOPIC.to_string(),
                    partitions: vec![OffsetCommitResponsePartition {
                        partition_index: 0,
                        error_code: case.error_code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            case.group != Group::Missing || case.error_code == codes::NONE,
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Allows everything but `Read` on the topic `DENIED_TOPIC`.
#[derive(Debug)]
struct DenyOneTopic;

const DENIED_TOPIC: &str = "denied-topic";

test_authorizer!(DenyOneTopic, (self, _source, req), {
    if req.resource_type == ResourceType::Topic && req.resource_name == DENIED_TOPIC {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

/// Kafka checks topic `Read` in `KafkaApis` before it calls the coordinator,
/// so the coordinator's error goes on the allowed topics only, and a request
/// with no allowed topic never reaches the coordinator.
#[tokio::test]
async fn topic_read_is_checked_before_the_group() {
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(DenyOneTopic)).await;
    let shared = broker.broker_arc_for_test();
    create_topic(&shared).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    seed(&shared, "authz-mixed", Group::ClassicWithMember);

    let row = |name: &str| OffsetCommitRequestTopic {
        name: name.to_string(),
        partitions: vec![OffsetCommitRequestPartition {
            partition_index: 0,
            committed_offset: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    let answer = |name: &str, error_code| OffsetCommitResponseTopic {
        name: name.to_string(),
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    let cases = [
        // One denied topic and one allowed topic with a wrong generation.
        (
            "authz-mixed",
            vec![row(DENIED_TOPIC), row(TOPIC)],
            vec![
                answer(DENIED_TOPIC, codes::TOPIC_AUTHORIZATION_FAILED),
                answer(TOPIC, codes::ILLEGAL_GENERATION),
            ],
        ),
        // Every topic denied on a group that does not exist: the coordinator
        // is not called, so neither GROUP_ID_NOT_FOUND nor a new group.
        (
            "authz-all-denied",
            vec![row(DENIED_TOPIC)],
            vec![answer(DENIED_TOPIC, codes::TOPIC_AUTHORIZATION_FAILED)],
        ),
    ];
    request_identity!(
        (user, address, ctx),
        principal("consumer"),
        client_id = "consumer-client"
    );
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (group_id, topics, answers) in cases {
        let request = OffsetCommitRequest {
            group_id: group_id.to_string(),
            generation_id_or_member_epoch: GENERATION + 1,
            member_id: MEMBER.to_string(),
            topics,
            ..Default::default()
        };
        let response: OffsetCommitResponse = crate::test_support::dispatch_wire(
            &shared,
            krabka_protocol::owned::offset_commit_request::API_KEY,
            9,
            &request,
            &ctx,
        )
        .await;
        actual.push((
            group_id,
            response,
            shared.group_coordinator.find(group_id).is_some(),
        ));
        expected.push((
            group_id,
            OffsetCommitResponse {
                topics: answers,
                ..Default::default()
            },
            group_id == "authz-mixed",
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A partition whose metadata is longer than `offset.metadata.max.bytes`
/// answers `OFFSET_METADATA_TOO_LARGE` and leaves no committed offset.
#[tokio::test]
async fn oversized_metadata_is_refused_through_the_handler() {
    const GROUP_ID: &str = "oversized-metadata";
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let shared = broker.broker_arc_for_test();
    create_topic(&shared).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let max = usize::try_from(shared.config.offset_metadata_max_bytes).unwrap();
    let request = OffsetCommitRequest {
        group_id: GROUP_ID.to_string(),
        topics: vec![OffsetCommitRequestTopic {
            name: TOPIC.to_string(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                committed_metadata: Some("m".repeat(max + 1)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    request_identity!(
        (user, address, ctx),
        principal("consumer"),
        client_id = "consumer-client"
    );
    let response: OffsetCommitResponse = crate::test_support::dispatch_wire(
        &shared,
        krabka_protocol::owned::offset_commit_request::API_KEY,
        9,
        &request,
        &ctx,
    )
    .await;

    let actor = shared
        .group_coordinator
        .find(GROUP_ID)
        .expect("simple group");
    let (reply, offsets) = tokio::sync::oneshot::channel();
    actor
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .expect("send FetchOffsets");
    let committed = offsets.await.expect("FetchOffsets reply").committed;

    let expected = OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: TOPIC.to_string(),
            partitions: vec![OffsetCommitResponsePartition {
                partition_index: 0,
                error_code: codes::OFFSET_METADATA_TOO_LARGE,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!((response, committed.is_empty()) == (expected, true));
    broker.shutdown().await;
}

/// The consumer group that one KIP-1251 row commits to: `native` and
/// `classic` at member epoch 5, each assigned partition 0 at epoch 3 and
/// partition 1 at epoch 5, restored from records as a coordinator failover
/// restores them.
fn consumer_group_seed(topic_id: WireUuid) -> GroupSeed {
    let metadata = |classic: bool| MemberMetadataValue {
        instance_id: None,
        rack_id: None,
        client_id: "client".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: vec![TOPIC.into()],
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout_ms: 60_000,
        classic: classic.then(|| ClassicMemberMetadata {
            session_timeout_ms: 45_000,
            supported_protocols: vec![("range".into(), bytes::Bytes::new())],
        }),
    };
    let current = CurrentMemberAssignmentValue {
        member_epoch: 5,
        previous_member_epoch: 4,
        state: MemberAssignmentState::Stable,
        assigned_partitions: vec![CurrentTopicPartitions {
            topic_id,
            partitions: vec![0, 1],
            assignment_epochs: Some(vec![3, 5]),
        }],
        partitions_pending_revocation: vec![],
    };
    let target = TargetAssignmentMemberValue {
        topic_partitions: vec![AssignedTopicPartitions {
            topic_id,
            partitions: vec![0, 1],
        }],
    };
    GroupSeed {
        group_epoch: 5,
        target_epoch: 5,
        assignment_timestamp_ms: 0,
        members: [
            ("native".to_string(), metadata(false)),
            ("classic".to_string(), metadata(true)),
        ]
        .into(),
        target_per_member: [
            ("native".to_string(), target.clone()),
            ("classic".to_string(), target),
        ]
        .into(),
        current_per_member: [
            ("native".to_string(), current.clone()),
            ("classic".to_string(), current),
        ]
        .into(),
        ..GroupSeed::default()
    }
}

/// Issue #800's table through the `OffsetCommit` handler at v9: Kafka 4.3.1's
/// `ConsumerGroup.validateOffsetCommit` with the KIP-1251 per-partition
/// assignment-epoch validator. A refusal answers every committed row.
#[tokio::test]
async fn consumer_group_commit_follows_kip_1251() {
    struct Row {
        member_id: &'static str,
        epoch: i32,
        version: i16,
        partitions: &'static [i32],
        error_code: i16,
    }
    let rows = [
        // Native member.
        Row {
            member_id: "native",
            epoch: 5,
            version: 9,
            partitions: &[0, 1],
            error_code: codes::NONE,
        },
        Row {
            member_id: "native",
            epoch: 6,
            version: 9,
            partitions: &[0],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[0],
            error_code: codes::NONE,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[1],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 4,
            version: 9,
            partitions: &[2],
            error_code: codes::STALE_MEMBER_EPOCH,
        },
        Row {
            member_id: "native",
            epoch: 5,
            version: 8,
            partitions: &[0],
            error_code: codes::UNSUPPORTED_VERSION,
        },
        // Classic-protocol member.
        Row {
            member_id: "classic",
            epoch: 6,
            version: 9,
            partitions: &[0],
            error_code: codes::ILLEGAL_GENERATION,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 9,
            partitions: &[0],
            error_code: codes::NONE,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 9,
            partitions: &[1],
            error_code: codes::ILLEGAL_GENERATION,
        },
        Row {
            member_id: "classic",
            epoch: 4,
            version: 8,
            partitions: &[0],
            error_code: codes::NONE,
        },
    ];
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let shared = broker.broker_arc_for_test();
    create_topic_with_partitions(&shared, KIP_1251_TOPIC_PARTITIONS).await;
    for partition in 0..KIP_1251_TOPIC_PARTITIONS {
        broker.wait_until_partition_present(TOPIC, partition).await;
    }
    let topic_id = WireUuid(
        shared
            .controller
            .current_image()
            .topic(TOPIC)
            .expect("topic")
            .topic_id
            .into_bytes(),
    );
    request_identity!(
        (user, address, ctx),
        principal("consumer"),
        client_id = "consumer-client"
    );

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let group_id = format!("kip-1251-{i}");
        let actor = shared.group_coordinator.get_or_create_consumer(&group_id);
        actor
            .tx
            .send(GroupActorMessage::Seed(consumer_group_seed(topic_id)))
            .await
            .expect("seed");
        let request = OffsetCommitRequest {
            group_id,
            generation_id_or_member_epoch: row.epoch,
            member_id: row.member_id.into(),
            topics: vec![OffsetCommitRequestTopic {
                name: TOPIC.into(),
                partitions: row
                    .partitions
                    .iter()
                    .map(|&partition_index| OffsetCommitRequestPartition {
                        partition_index,
                        committed_offset: 42,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response: OffsetCommitResponse = crate::test_support::dispatch_wire(
            &shared,
            krabka_protocol::owned::offset_commit_request::API_KEY,
            row.version,
            &request,
            &ctx,
        )
        .await;
        actual.push((row.member_id, row.epoch, row.version, response));
        expected.push((
            row.member_id,
            row.epoch,
            row.version,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: TOPIC.into(),
                    partitions: row
                        .partitions
                        .iter()
                        .map(|&partition_index| OffsetCommitResponsePartition {
                            partition_index,
                            error_code: row.error_code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}
