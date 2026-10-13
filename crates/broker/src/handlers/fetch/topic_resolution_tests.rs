//! Handler tests for how `Fetch` answers a topic row by request version and by
//! the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleFetchRequest` resolves the topic of every row
//! through `metadataCache.topicIdsToNames()` at version 13 and later. A row
//! whose id gives no name, the zero id included, gets `UNKNOWN_TOPIC_ID` on
//! every partition row, before the topic `Read` authorization. Versions 12 and
//! earlier name the topic, and a name that does not resolve goes to the
//! authorization gate and then to the partition gate.
//!
//! The tests encode each response the way the wire carries it and decode it
//! again, so the expected structs hold only what a client sees.

use {
    super::{
        FIRST_TOPIC_ID_VERSION, test_support::expected_refused_partition as refused_partition,
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        fetch_session::{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID},
        handlers::test_support::{
            check_cases, check_denied_topic_cases, create_topic as create_topic_from_test_support,
            seed_partition_replicas,
        },
        test_support::{DenyAll, KafkaErrorCode, start_broker_with_authorizer_no_audit as start},
    },
    assert2::assert,
    bytes::Bytes,
    krabka_protocol::{
        owned::{
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        },
        primitives::uuid::Uuid as WireUuid,
        records::RecordsPayload,
    },
    std::sync::Arc,
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A second id that no topic in these tests has.
const OTHER_UNKNOWN_ID: WireUuid = WireUuid([0x0c; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The highest `Fetch` version that the broker serves.
const MAX_VERSION: i16 = krabka_protocol::owned::fetch_request::MAX_VERSION;

use crate::handlers::test_support::{
    TopicRef, TopicResolutionCase as Case, unauthorized_topic_cases,
};

/// The actual or the expected outcome of one [`Case`].
type Outcome = (i16, TopicRef, FetchResponse);

/// The empty record set, as a client decodes it.
fn no_records() -> RecordsPayload { RecordsPayload::Legacy(Bytes::new()) }

/// The partition row of an empty topic that the consumer may read, as it
/// decodes at `version`. The wire carries `log_start_offset` from v5 on. An
/// older version decodes the field's default, -1.
fn empty_partition(version: i16) -> PartitionData {
    PartitionData {
        partition_index: 0,
        error_code: codes::NONE,
        high_watermark: 0,
        last_stable_offset: 0,
        log_start_offset: if version >= 5 { 0 } else { -1 },
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(no_records()),
        ..Default::default()
    }
}

/// One request row for partition 0 of the topic that `name` and `topic_id`
/// name.
fn fetch_topic(name: &str, topic_id: WireUuid) -> FetchTopic {
    FetchTopic {
        topic: name.to_owned(),
        topic_id,
        partitions: vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("fetch-resolution-test")
        .name(name)
        .call()
        .await
}

/// Send one `Fetch` at `version` and return the response as a client decodes
/// it.
async fn fetch(broker: &BrokerHandle, version: i16, request: &FetchRequest) -> FetchResponse {
    super::test_support::fetch_wire()
        .broker(broker)
        .request(request)
        .version(version)
        .user("consumer")
        .client_id("consumer-client")
        .call()
        .await
}

/// Send one sessionless single-row `Fetch` at `case.version` for `case.topic`.
/// Return the decoded response and the response that the case expects.
///
/// `known` names a topic that exists, with its id. The expected topic row
/// carries the name and the id as the wire carries them at `case.version`: the
/// name at v12 and earlier, the id at v13 and later.
async fn drive(
    broker: &BrokerHandle,
    known: Option<(&str, WireUuid)>,
    case: Case,
) -> (Outcome, Outcome) {
    let (known_name, known_id) = known.unwrap_or(("", WireUuid::ZERO));
    let (name, topic_id) = case
        .topic
        .wire_reference((known_name, known_id), (UNKNOWN_NAME, UNKNOWN_ID));
    let request = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![fetch_topic(name, topic_id)],
        ..Default::default()
    };
    let actual = fetch(broker, case.version, &request).await;

    let partition = if case.error_code == codes::NONE {
        empty_partition(case.version)
    } else {
        refused_partition(
            krabka_ids::PartitionIndex(0),
            KafkaErrorCode(case.error_code),
        )
    };
    let representation =
        super::test_support::TopicRepresentation::for_version(krabka_ids::ApiVersion(case.version));
    let expected =
        super::test_support::expected_single_topic(representation, (name, topic_id), partition);
    (
        (case.version, case.topic, actual),
        (case.version, case.topic, expected),
    )
}

#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    let mut cases = vec![
        Case::new(4, TopicRef::KnownName, codes::NONE),
        Case::new(4, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
        Case::new(12, TopicRef::KnownName, codes::NONE),
        Case::new(12, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
    ];
    for version in [FIRST_TOPIC_ID_VERSION, MAX_VERSION] {
        cases.extend([
            Case::new(version, TopicRef::KnownId, codes::NONE),
            Case::new(version, TopicRef::UnknownId, codes::UNKNOWN_TOPIC_ID),
            Case::new(version, TopicRef::ZeroId, codes::UNKNOWN_TOPIC_ID),
        ]);
    }
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let known_id = create_topic(&broker, "resolution").await;

    check_cases(cases, async |case| {
        drive(&broker, Some(("resolution", known_id)), case).await
    })
    .await;
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no `Read` grant sees 100 for an id that does not resolve,
/// and 29 for a name that does not resolve.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    let cases = unauthorized_topic_cases(12, FIRST_TOPIC_ID_VERSION);
    check_denied_topic_cases(cases, start(Arc::new(DenyAll)), async |broker, case| {
        drive(broker, None, case).await
    })
    .await;
}

/// Each topic whose id does not resolve keeps its own topic row. Kafka's
/// `FetchResponse.toMessage` fails with a `NullPointerException` when two such
/// rows are adjacent and answers `UNKNOWN_SERVER_ERROR` for the whole request.
/// The broker answers every row instead, as Kafka does for one such row.
#[tokio::test]
async fn every_unresolved_id_keeps_its_own_topic_row() {
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let request = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        topics: vec![
            fetch_topic("", UNKNOWN_ID),
            fetch_topic("", WireUuid::ZERO),
            fetch_topic("", OTHER_UNKNOWN_ID),
        ],
        ..Default::default()
    };

    let actual = fetch(&broker, MAX_VERSION, &request).await;

    let refused_topic = |topic_id| FetchableTopicResponse {
        topic: String::new(),
        topic_id,
        partitions: vec![refused_partition(
            krabka_ids::PartitionIndex(0),
            crate::test_support::KafkaErrorCode(codes::UNKNOWN_TOPIC_ID),
        )],
        ..Default::default()
    };
    let expected = FetchResponse {
        responses: vec![
            refused_topic(UNKNOWN_ID),
            refused_topic(WireUuid::ZERO),
            refused_topic(OTHER_UNKNOWN_ID),
        ],
        ..Default::default()
    };
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A fetch session keeps a partition whose id does not resolve. Kafka's
/// `FullFetchContext` caches it, and `CachedPartition.maybeUpdateResponseData`
/// puts every partition with an error into each incremental response. So the
/// first response and every incremental response carry 100 for it.
#[tokio::test]
async fn a_fetch_session_repeats_unknown_topic_id() {
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let opened = fetch(&broker, MAX_VERSION, &FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        session_id: INVALID_SESSION_ID,
        session_epoch: INITIAL_EPOCH,
        topics: vec![fetch_topic("", UNKNOWN_ID)],
        ..Default::default()
    })
    .await;
    let session_id = opened.session_id;
    assert!(session_id != INVALID_SESSION_ID, "{opened:?}");

    let mut actual = vec![(INITIAL_EPOCH, opened)];
    for epoch in [1, 2] {
        let incremental = FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id,
            session_epoch: epoch,
            ..Default::default()
        };
        actual.push((epoch, fetch(&broker, MAX_VERSION, &incremental).await));
    }

    let expected_response = FetchResponse {
        session_id,
        responses: vec![FetchableTopicResponse {
            topic: String::new(),
            topic_id: UNKNOWN_ID,
            partitions: vec![refused_partition(
                krabka_ids::PartitionIndex(0),
                crate::test_support::KafkaErrorCode(codes::UNKNOWN_TOPIC_ID),
            )],
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected: Vec<_> = [INITIAL_EPOCH, 1, 2]
        .into_iter()
        .map(|epoch| (epoch, expected_response.clone()))
        .collect();
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A partition the metadata holds and this broker does not host, after a
/// reassignment took its replica away for example, is the read's own refusal:
/// Kafka's `ReplicaManager.getPartitionOrError` answers `NOT_LEADER_OR_FOLLOWER`
/// so the client refreshes its metadata, and from v16 the row names the
/// leader (KIP-951) so it can re-route without that round trip. Only a
/// partition the metadata does not hold is `UNKNOWN_TOPIC_OR_PARTITION`, which
/// a client may take for a topic that was deleted.
#[tokio::test]
async fn a_partition_the_metadata_holds_and_this_broker_does_not_host_is_not_leader_or_follower() {
    use krabka_protocol::owned::fetch_response::LeaderIdAndEpoch;

    const KIP_951_VERSION: i16 = 16;
    const UNHOSTED: i32 = 0;
    const ABSENT: i32 = 7;

    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    // Node 1, this broker, is not a replica: node 2 leads and node 3 follows.
    let topic_id = uuid::Uuid::from_u128(0x51);
    seed_partition_replicas()
        .broker(&broker)
        .topic("moved")
        .topic_id(topic_id)
        .leader(2)
        .replicas(&[krabka_audit::NodeId(2), krabka_audit::NodeId(3)])
        .leader_epoch(4)
        .call()
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while broker
            .controller_image_for_test()
            .partition("moved", UNHOSTED)
            .is_none()
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the image holds the partition");
    let wire_id = WireUuid(topic_id.into_bytes());

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for version in [12, KIP_951_VERSION, MAX_VERSION] {
        let by_id = version >= FIRST_TOPIC_ID_VERSION;
        let name = if by_id { String::new() } else { "moved".into() };
        let id = if by_id { wire_id } else { WireUuid::ZERO };
        let request = FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id: INVALID_SESSION_ID,
            session_epoch: FINAL_EPOCH,
            topics: vec![FetchTopic {
                topic: name.clone(),
                topic_id: id,
                partitions: [UNHOSTED, ABSENT]
                    .into_iter()
                    .map(|partition| FetchPartition {
                        partition,
                        fetch_offset: 0,
                        partition_max_bytes: 1_048_576,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        actual.push((version, fetch(&broker, version, &request).await.responses));

        let hosted_elsewhere = PartitionData {
            partition_index: UNHOSTED,
            error_code: codes::NOT_LEADER_OR_FOLLOWER,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: None,
            preferred_read_replica: -1,
            records: Some(no_records()),
            current_leader: if version >= KIP_951_VERSION {
                LeaderIdAndEpoch {
                    leader_id: 2,
                    leader_epoch: 4,
                    ..Default::default()
                }
            } else {
                LeaderIdAndEpoch::default()
            },
            ..Default::default()
        };
        expected.push((version, vec![FetchableTopicResponse {
            topic: name,
            topic_id: id,
            partitions: vec![
                hosted_elsewhere,
                refused_partition(
                    krabka_ids::PartitionIndex(ABSENT),
                    crate::test_support::KafkaErrorCode(codes::UNKNOWN_TOPIC_OR_PARTITION),
                ),
            ],
            ..Default::default()
        }]));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}
