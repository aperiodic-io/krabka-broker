//! Handler tests for how `OffsetCommit` answers a topic row by request version
//! and by the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleOffsetCommitRequest` resolves each `topic_id` at
//! v10 and later. A row whose name stays empty answers `UNKNOWN_TOPIC_ID` on
//! every partition row, the zero id included, and the coordinator commits
//! nothing for it. The check runs after the group authorization and before the
//! topic authorization. After the topic authorization, a topic or a partition
//! that the image does not hold answers `UNKNOWN_TOPIC_OR_PARTITION`.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::{
    owned::{
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_commit_response::{
            OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AllowAllAuthorizer, Authorizer},
    broker::BrokerHandle,
    codes,
    coordinator::unified::actor::GroupActorMessage,
    handlers::test_support::CreateTopicSetup,
    test_support::{DenyAll, peer, principal, request_context},
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The name of the topic that exists in these tests.
const KNOWN_NAME: &str = "offsets-resolution";

use crate::handlers::test_support::{DenyTopics, TopicRef, TopicResolutionCase as Case};

/// The actual or the expected outcome of one [`Case`]: the response, and the
/// `(topic, partition)` keys that the group holds committed offsets for.
type Outcome = (i16, TopicRef, OffsetCommitResponse, Vec<(String, i32)>);

async fn create_known_topic(broker: &BrokerHandle) -> WireUuid {
    created_topic_fixture!(
        (client, response),
        broker,
        "offset-commit-resolution-test",
        crate::handlers::test_support::configured_topic_request(CreateTopicSetup {
            topic: KNOWN_NAME,
            ..Default::default()
        })
    );
    broker.wait_until_partition_present(KNOWN_NAME, 0).await;
    let image = broker.controller_image_for_test();
    let topic = image.topic(KNOWN_NAME).expect("known topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// The committed `(topic, partition)` keys of `group`, sorted. A group that
/// has no actor holds none.
async fn committed_keys(broker: &BrokerHandle, group: &str) -> Vec<(String, i32)> {
    let shared = broker.broker_arc_for_test();
    let Some(actor) = shared.group_coordinator.find(group) else {
        return Vec::new();
    };
    let (reply, offsets) = oneshot::channel();
    actor
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .expect("send FetchOffsets");
    let mut keys: Vec<_> = offsets
        .await
        .expect("FetchOffsets reply")
        .committed
        .into_keys()
        .collect();
    keys.sort();
    keys
}

macro_rules! consumer_commit {
    (($shared:ident, $user:ident, $address:ident, $context:ident, $actual:ident), $broker:expr, $version:expr, $request:ident) => {
        let $shared = $broker.broker_arc_for_test();
        request_identity!(($user, $address), principal("consumer"));
        let $context = request_context(&$user, &$address, "consumer-client");
        let $actual: OffsetCommitResponse = crate::test_support::dispatch_wire(
            &$shared,
            krabka_protocol::owned::offset_commit_request::API_KEY,
            $version,
            &$request,
            &$context,
        )
        .await;
    };
}

/// Send one single-row `OffsetCommit` at `case.version` for `case.topic`, in
/// a group of its own. Return the actual and the expected outcome.
///
/// The expected topic row carries the name and the id as the wire carries
/// them at `case.version`: the name before v10, the id at v10. Only a row that
/// answers `NONE` leaves a committed offset, under the topic name.
async fn drive(
    broker: &BrokerHandle,
    known_id: WireUuid,
    row: usize,
    case: Case,
) -> (Outcome, Outcome) {
    let (name, topic_id) = case
        .topic
        .wire_reference((KNOWN_NAME, known_id), (UNKNOWN_NAME, UNKNOWN_ID));
    let group = format!("resolution-{row}");
    // An empty member id and generation -1 commit as a simple consumer, so
    // the membership check passes.
    let request = OffsetCommitRequest {
        group_id: group.clone(),
        topics: vec![OffsetCommitRequestTopic {
            name: name.to_string(),
            topic_id,
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    consumer_commit!(
        (shared, user, address, ctx, actual),
        broker,
        case.version,
        request
    );
    let actual_keys = committed_keys(broker, &group).await;

    let id_only = case.version >= super::FIRST_TOPIC_ID_VERSION;
    let expected = OffsetCommitResponse {
        topics: vec![OffsetCommitResponseTopic {
            name: if id_only {
                String::new()
            } else {
                name.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partitions: vec![OffsetCommitResponsePartition {
                partition_index: 0,
                error_code: case.error_code,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected_keys = if case.error_code == codes::NONE {
        let committed_name = if case.topic == TopicRef::KnownId {
            KNOWN_NAME
        } else {
            name
        };
        vec![(committed_name.to_string(), 0)]
    } else {
        Vec::new()
    };
    (
        (case.version, case.topic, actual, actual_keys),
        (case.version, case.topic, expected, expected_keys),
    )
}

async fn run_table(authorizer: Arc<dyn Authorizer>, cases: &[Case]) {
    let (broker, _dir) = crate::test_support::start_group_broker_no_audit(authorizer).await;
    // A table that names no existing topic can run under an authorizer that
    // refuses `CreateTopics`.
    let needs_known = cases
        .iter()
        .any(|case| matches!(case.topic, TopicRef::KnownName | TopicRef::KnownId));
    let known_id = if needs_known {
        create_known_topic(&broker).await
    } else {
        WireUuid::ZERO
    };
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (row, case) in cases.iter().enumerate() {
        let (got, want) = drive(&broker, known_id, row, *case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    run_table(
        Arc::new(AllowAllAuthorizer),
        &[
            Case::new(9, TopicRef::KnownName, codes::NONE),
            Case::new(2, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
            Case::new(9, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
            Case::new(10, TopicRef::KnownId, codes::NONE),
            Case::new(10, TopicRef::UnknownId, codes::UNKNOWN_TOPIC_ID),
            Case::new(10, TopicRef::ZeroId, codes::UNKNOWN_TOPIC_ID),
        ],
    )
    .await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no topic `Read` grant sees 100 for an id that does not
/// resolve, and 29 for a name.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    run_table(
        Arc::new(DenyTopics),
        &[
            Case::new(9, TopicRef::UnknownName, codes::TOPIC_AUTHORIZATION_FAILED),
            Case::new(10, TopicRef::KnownId, codes::TOPIC_AUTHORIZATION_FAILED),
            Case::new(10, TopicRef::UnknownId, codes::UNKNOWN_TOPIC_ID),
            Case::new(10, TopicRef::ZeroId, codes::UNKNOWN_TOPIC_ID),
        ],
    )
    .await;
}

/// Kafka authorizes the group before it resolves any topic id, so a group
/// denial answers `GROUP_AUTHORIZATION_FAILED` on every row.
#[tokio::test]
async fn group_authorization_answers_before_topic_resolution() {
    run_table(
        Arc::new(DenyAll),
        &[
            Case::new(10, TopicRef::UnknownId, codes::GROUP_AUTHORIZATION_FAILED),
            Case::new(10, TopicRef::ZeroId, codes::GROUP_AUTHORIZATION_FAILED),
        ],
    )
    .await;
}

/// One v10 request that mixes a known id with the zero id and an unknown id.
/// The refused rows come first, and the committed row follows, as Kafka's
/// `OffsetCommitResponse.Builder.merge` orders them. Only the known topic
/// commits.
#[tokio::test]
async fn refused_rows_precede_the_committed_row() {
    const VERSION: i16 = 10;
    const GROUP: &str = "resolution-mixed";
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let known_id = create_known_topic(&broker).await;
    let row = |topic_id| OffsetCommitRequestTopic {
        topic_id,
        partitions: vec![OffsetCommitRequestPartition {
            partition_index: 0,
            committed_offset: 42,
            ..Default::default()
        }],
        ..Default::default()
    };
    let request = OffsetCommitRequest {
        group_id: GROUP.to_string(),
        topics: vec![row(known_id), row(WireUuid::ZERO), row(UNKNOWN_ID)],
        ..Default::default()
    };

    consumer_commit!(
        (shared, user, address, ctx, actual),
        broker,
        VERSION,
        request
    );

    let answer = |topic_id, error_code| OffsetCommitResponseTopic {
        topic_id,
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected = OffsetCommitResponse {
        topics: vec![
            answer(WireUuid::ZERO, codes::UNKNOWN_TOPIC_ID),
            answer(UNKNOWN_ID, codes::UNKNOWN_TOPIC_ID),
            answer(known_id, codes::NONE),
        ],
        ..Default::default()
    };
    assert!(
        (actual, committed_keys(&broker, GROUP).await)
            == (expected, vec![(KNOWN_NAME.to_string(), 0)])
    );
    broker.shutdown().await;
}

/// A partition index that the topic does not have answers
/// `UNKNOWN_TOPIC_OR_PARTITION` on its own row at every version, and the other
/// partitions of the same topic commit. The refused row comes first, and the
/// coordinator's row for the same topic merges into it, as Kafka's
/// `Builder.addPartition` and `Builder.merge` order them.
#[tokio::test]
async fn unknown_partition_answers_on_its_own_row() {
    const MISSING_PARTITION: i32 = 7;
    let (broker, _dir) =
        crate::test_support::start_group_broker_no_audit(Arc::new(AllowAllAuthorizer)).await;
    let known_id = create_known_topic(&broker).await;
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("consumer"),
        client_id = "consumer-client"
    );
    let partition = |partition_index| OffsetCommitRequestPartition {
        partition_index,
        committed_offset: 42,
        ..Default::default()
    };
    let answer = |partition_index, error_code| OffsetCommitResponsePartition {
        partition_index,
        error_code,
        ..Default::default()
    };

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for version in [2_i16, 9, 10] {
        let group = format!("unknown-partition-v{version}");
        let id_only = version >= super::FIRST_TOPIC_ID_VERSION;
        let (name, topic_id) = if id_only {
            ("", known_id)
        } else {
            (KNOWN_NAME, WireUuid::ZERO)
        };
        let request = OffsetCommitRequest {
            group_id: group.clone(),
            topics: vec![OffsetCommitRequestTopic {
                name: name.to_string(),
                topic_id,
                partitions: vec![partition(MISSING_PARTITION), partition(0)],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response: OffsetCommitResponse = crate::test_support::dispatch_wire(
            &shared,
            krabka_protocol::owned::offset_commit_request::API_KEY,
            version,
            &request,
            &ctx,
        )
        .await;
        actual.push((version, response, committed_keys(&broker, &group).await));
        expected.push((
            version,
            OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    name: name.to_string(),
                    topic_id,
                    partitions: vec![
                        answer(MISSING_PARTITION, codes::UNKNOWN_TOPIC_OR_PARTITION),
                        answer(0, codes::NONE),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
            vec![(KNOWN_NAME.to_string(), 0)],
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}
