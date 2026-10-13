//! Handler tests for how `ShareFetch` answers a partition row by the kind of
//! topic id that the row carries.
//!
//! Kafka's `ErroneousAndValidPartitionData` answers `UNKNOWN_TOPIC_ID` for
//! every partition of the share session whose topic id does not resolve, the
//! zero id included. It does this before the topic `Read` authorization. When
//! the request carries acknowledgements,
//! `KafkaApis.getAcknowledgeBatchesFromShareFetchRequest` also answers
//! `UNKNOWN_TOPIC_ID` as the acknowledge error of those partitions.

use {
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer},
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            StartedBroker, create_topic as create_topic_from_test_support, share_fetch_wire,
        },
        test_support::start_broker_no_audit_with,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            share_fetch_request::{AcknowledgementBatch, ShareFetchRequest},
            share_fetch_response::{
                PartitionData, ShareFetchResponse, ShareFetchableTopicResponse,
            },
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordsPayload,
    },
    std::sync::Arc,
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// The acquisition lock timeout of the test broker's share-group config.
const LOCK_TIMEOUT_MS: i32 = 30_000;

use crate::handlers::test_support::{DenyTopicRead, TopicIdRef as TopicRef};

/// One row of a table: the request version, the topic id, and the error code
/// that Kafka puts on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    topic: TopicRef,
    error_code: i16,
}

/// The actual or the expected outcome of one [`Case`].
#[derive(Debug, PartialEq)]
struct Outcome {
    version: i16,
    topic: TopicRef,
    response: ShareFetchResponse,
}

async fn start(authorizer: Arc<dyn Authorizer>) -> StartedBroker {
    let (broker, dir) = start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await;
    StartedBroker { broker, dir }
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("share-fetch-resolution-test")
        .name(name)
        .call()
        .await
}

/// A request for partition 0 of `topic_id`, in the share session of `member`.
/// With `acknowledge`, the row carries one acknowledgement batch.
use crate::handlers::test_support::ShareSessionEpoch;

#[derive(Clone, Copy, Default)]
enum AcknowledgementPresence {
    #[default]
    None,
    AcceptFirstRecord,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TopicResolutionSetup<'a> {
    #[default("member")]
    member: &'a str,
    #[default(ShareSessionEpoch(0))]
    epoch: ShareSessionEpoch,
    #[default(WireUuid::ZERO)]
    topic_id: WireUuid,
    acknowledgements: AcknowledgementPresence,
}

#[bon::builder]
fn request(
    #[builder(default = "member")] member: &'_ str,
    #[builder(into, default = ShareSessionEpoch (0))] epoch: ShareSessionEpoch,
    #[builder(default = WireUuid :: ZERO)] topic_id: WireUuid,
    #[builder(default)] acknowledgements: AcknowledgementPresence,
) -> ShareFetchRequest {
    let setup = TopicResolutionSetup {
        member,
        epoch,
        topic_id,
        acknowledgements,
    };
    let TopicResolutionSetup {
        member,
        epoch,
        topic_id,
        acknowledgements,
    } = setup;
    let acknowledgement_batches = match acknowledgements {
        AcknowledgementPresence::None => Vec::new(),
        AcknowledgementPresence::AcceptFirstRecord => vec![AcknowledgementBatch {
            first_offset: 0,
            last_offset: 0,
            acknowledge_types: vec![1],
            ..Default::default()
        }],
    };
    super::test_fetch_request()
        .group_id("resolution-group".into())
        .member_id(member.into())
        .topic_id(topic_id.0)
        .share_session_epoch(epoch.0)
        .acknowledgement_batches(acknowledgement_batches)
        .call()
}

async fn share_fetch(
    broker: &BrokerHandle,
    version: i16,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    share_fetch_wire()
        .broker(broker)
        .request(request)
        .version(version)
        .call()
        .await
}

/// The empty record set, as a client decodes it. Kafka's
/// `ShareFetchResponse.partitionResponse` sets `MemoryRecords.EMPTY` on every
/// row.
fn no_records() -> RecordsPayload { RecordsPayload::Legacy(Bytes::new()) }

/// A response with one topic row for partition 0.
fn one_row(topic_id: WireUuid, partition: PartitionData) -> ShareFetchResponse {
    ShareFetchResponse {
        acquisition_lock_timeout_ms: LOCK_TIMEOUT_MS,
        responses: vec![ShareFetchableTopicResponse {
            topic_id,
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The outcomes that `drive` got and the ones it expected.
struct DrivenCases {
    actual: Vec<Outcome>,
    expected: Vec<Outcome>,
}

/// Run every case in its own share session on `broker`. `known` is the id of
/// a topic that exists.
async fn drive(broker: &BrokerHandle, known: WireUuid, cases: &[Case]) -> DrivenCases {
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (row, case) in cases.iter().enumerate() {
        let topic_id = match case.topic {
            TopicRef::Known => known,
            TopicRef::Unknown => UNKNOWN_ID,
            TopicRef::Zero => WireUuid::ZERO,
        };
        let member = format!("member-{row}");
        let response = share_fetch(
            broker,
            case.version,
            &request().member(&member).topic_id(topic_id).call(),
        )
        .await;
        actual.push(Outcome {
            version: case.version,
            topic: case.topic,
            response,
        });
        let partition = PartitionData {
            partition_index: 0,
            error_code: case.error_code,
            records: Some(no_records()),
            ..Default::default()
        };
        expected.push(Outcome {
            version: case.version,
            topic: case.topic,
            response: one_row(topic_id, partition),
        });
    }
    DrivenCases { actual, expected }
}

/// The cases for every supported version: `known` is the error code for a
/// topic that exists.
fn cases(known: i16) -> Vec<Case> {
    let mut cases = Vec::new();
    for version in [1, krabka_protocol::owned::share_fetch_request::MAX_VERSION] {
        cases.extend([
            Case {
                version,
                topic: TopicRef::Known,
                error_code: known,
            },
            Case {
                version,
                topic: TopicRef::Unknown,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
            Case {
                version,
                topic: TopicRef::Zero,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
        ]);
    }
    cases
}

#[tokio::test]
async fn partition_row_error_follows_topic_id() {
    initialized_share_topic!(
        (broker, _dir, known),
        start(Arc::new(AllowAllAuthorizer)),
        "share-resolution",
        "resolution-group"
    );

    let DrivenCases { actual, expected } = drive(&broker, known, &cases(codes::NONE)).await;

    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no topic `Read` grant sees 29 for a topic that exists and
/// 100 for an id that does not resolve.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    initialized_share_topic!(
        (broker, _dir, known),
        start(Arc::new(DenyTopicRead)),
        "share-resolution",
        "resolution-group"
    );

    let DrivenCases { actual, expected } =
        drive(&broker, known, &cases(codes::TOPIC_AUTHORIZATION_FAILED)).await;

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The share session keeps a partition whose id does not resolve. A later
/// fetch that acknowledges records on it gets 100 as the fetch error and as
/// the acknowledge error.
#[tokio::test]
async fn a_piggybacked_acknowledgement_on_an_unresolved_id_answers_unknown_topic_id() {
    let version = krabka_protocol::owned::share_fetch_request::MAX_VERSION;
    let StartedBroker { broker, dir: _dir } = start(Arc::new(AllowAllAuthorizer)).await;

    let mut actual = Vec::new();
    for topic_id in [UNKNOWN_ID, WireUuid::ZERO] {
        let member = format!("member-{}", topic_id.0[0]);
        let opened = share_fetch(
            &broker,
            version,
            &request().member(&member).topic_id(topic_id).call(),
        )
        .await;
        let acknowledged = share_fetch(
            &broker,
            version,
            &request()
                .member(&member)
                .epoch(1)
                .topic_id(topic_id)
                .acknowledgements(AcknowledgementPresence::AcceptFirstRecord)
                .call(),
        )
        .await;
        actual.push((topic_id, opened, acknowledged));
    }

    let expected: Vec<_> = [UNKNOWN_ID, WireUuid::ZERO]
        .into_iter()
        .map(|topic_id| {
            (
                topic_id,
                one_row(topic_id, PartitionData {
                    partition_index: 0,
                    records: Some(no_records()),
                    error_code: codes::UNKNOWN_TOPIC_ID,
                    ..Default::default()
                }),
                one_row(topic_id, PartitionData {
                    partition_index: 0,
                    records: Some(no_records()),
                    error_code: codes::UNKNOWN_TOPIC_ID,
                    acknowledge_error_code: codes::UNKNOWN_TOPIC_ID,
                    ..Default::default()
                }),
            )
        })
        .collect();
    assert!(actual == expected);
    broker.shutdown().await;
}
