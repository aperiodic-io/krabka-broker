//! Handler tests for how `Produce` answers a topic row by request version and
//! by the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleProduceRequest` answers `UNKNOWN_TOPIC_ID` on
//! every partition row when an id-only version (v13 and later) names a topic
//! whose id does not resolve, the zero id included. It does this before the
//! topic `Write` authorization. Versions 12 and earlier name the topic, and a
//! name that does not resolve goes to the authorization gate and then to the
//! partition gate.

use std::sync::Arc;

use krabka_protocol::{
    owned::{
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use super::{FIRST_TOPIC_ID_VERSION, handle};
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    handlers::test_support::{
        ClientTopicSetup, check_cases, check_denied_topic_cases,
        create_topic as create_topic_from_test_support,
    },
    test_support::{
        DenyAll, decode_response, encode_request, peer, principal,
        start_broker_with_authorizer_no_audit as start,
    },
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

use crate::handlers::test_support::{
    TopicRef, TopicResolutionCase as Case, unauthorized_topic_cases,
};

/// The actual or the expected outcome of one [`Case`].
type Outcome = (i16, TopicRef, ProduceResponse);

/// One v2 batch with one record. A leader of a fresh topic appends it at
/// offset 0.
fn one_record_batch() -> RecordsPayload {
    RecordsPayload::V2(vec![crate::test_support::repeated_records_batch(
        crate::test_support::RepeatedRecordsSetup::default(),
    )])
}

/// The partition row that Kafka's `PartitionResponse(error)` constructor
/// builds for a row that failed before the append.
fn refused_partition(error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        ..Default::default()
    }
}

/// The partition row for a one-record append to a fresh topic, as it decodes
/// at `version`. The wire carries `log_start_offset` from v5 on. An older
/// version decodes the field's default, -1.
fn appended_partition(version: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset: 0,
        log_append_time_ms: -1,
        log_start_offset: if version >= 5 { 0 } else { -1 },
        ..Default::default()
    }
}

async fn create_topic(broker: &BrokerHandle, name: &str) {
    create_topic_from_test_support(
        broker,
        ClientTopicSetup {
            client_id: "produce-resolution-test",
            name,
            ..Default::default()
        },
    )
    .await;
}

/// Send one single-row `Produce` at `case.version` for `case.topic`. Return
/// the decoded response and the response that the case expects.
///
/// `known` is the name of a topic that exists and that no earlier case wrote
/// to. The expected topic row carries the name and the id as the wire carries
/// them at `case.version`: the name at v12 and earlier, the id at v13 and
/// later.
async fn drive(broker: &BrokerHandle, known: Option<&str>, case: Case) -> (Outcome, Outcome) {
    let known_id = known.map_or(WireUuid::ZERO, |name| {
        let image = broker.controller_image_for_test();
        let topic = image.topic(name).expect("known topic in the image");
        WireUuid(topic.topic_id.into_bytes())
    });
    let (name, topic_id) = case.topic.wire_reference(
        (known.unwrap_or_default(), known_id),
        (UNKNOWN_NAME, UNKNOWN_ID),
    );
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: name.to_string(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, address, ctx),
        principal("producer"),
        client_id = "producer-client"
    );
    let request_bytes = encode_request(&request, case.version);
    let response_bytes = handle(
        &shared,
        case.version,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    let actual: ProduceResponse = decode_response(&response_bytes, case.version);

    let partition = if case.error_code == codes::NONE {
        appended_partition(case.version)
    } else {
        refused_partition(case.error_code)
    };
    let id_only = case.version >= FIRST_TOPIC_ID_VERSION;
    let expected = ProduceResponse {
        responses: vec![TopicProduceResponse {
            name: if id_only {
                String::new()
            } else {
                name.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partition_responses: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    (
        (case.version, case.topic, actual),
        (case.version, case.topic, expected),
    )
}

#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    let cases = [
        Case::new(3, TopicRef::KnownName, codes::NONE),
        Case::new(3, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
        Case::new(12, TopicRef::KnownName, codes::NONE),
        Case::new(12, TopicRef::UnknownName, codes::UNKNOWN_TOPIC_OR_PARTITION),
        Case::new(13, TopicRef::KnownId, codes::NONE),
        Case::new(13, TopicRef::UnknownId, codes::UNKNOWN_TOPIC_ID),
        Case::new(13, TopicRef::ZeroId, codes::UNKNOWN_TOPIC_ID),
    ];
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;

    check_cases(cases.into_iter().enumerate(), async |(row, case)| {
        // Each row retains its own topic so an appended row starts at offset 0.
        let known = format!("resolution-{row}");
        create_topic(&broker, &known).await;
        drive(&broker, Some(&known), case).await
    })
    .await;
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no `Write` grant sees 100 for an id that does not resolve,
/// and 29 for a name that does not resolve.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    let cases = unauthorized_topic_cases(12, 13);
    check_denied_topic_cases(cases, start(Arc::new(DenyAll)), async |broker, case| {
        drive(broker, None, case).await
    })
    .await;
}
