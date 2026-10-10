//! The tests for the handler entry point: the per-topic `Describe` gate and
//! the response a denied topic receives.

use std::sync::Arc;

use assert2::assert;
use bytes::BytesMut;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::{
            self, ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
        },
    },
};

use super::{
    handle,
    sentinels::{EARLIEST_TIMESTAMP, LATEST_TIMESTAMP},
    test_support::{decode_response, encode_request},
};
use crate::{
    codes,
    handlers::{list_offsets::test_support::test_context, test_support::CreateTopicSetup},
    test_support::{
        DenyAll, peer, start_broker_with_authorizer_no_audit as start_broker, test_ctx,
    },
};

#[test]
fn topic_describe_denied_yields_topic_authorization_failed_rows() {
    use krabka_protocol::owned::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
    };

    empty_acl_fixture!(
        (authorizer, image),
        (principal, peer, ctx),
        crate::test_support::principal("ANONYMOUS"),
        client_id = "client-a",
        connection_id = "connection-a"
    );
    assert!(crate::handlers::acl_denied(
        &authorizer,
        &image,
        &ctx,
        ResourceType::Topic,
        "t",
        AclOperation::Describe,
    ));

    // The denied-topic shape the handler emits: every partition row
    // carries TOPIC_AUTHORIZATION_FAILED.
    let resp = ListOffsetsResponse {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "t".into(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                timestamp: -1,
                offset: -1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(list_offsets_response::MAX_VERSION));
    resp.encode(&mut buf, list_offsets_response::MAX_VERSION)
        .expect("encode");
    let mut cur: &[u8] = &buf;
    let decoded =
        ListOffsetsResponse::decode(&mut cur, list_offsets_response::MAX_VERSION).unwrap();
    assert!(decoded.topics[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
}

macro_rules! answer_request {
    (($request:ident, $bytes:ident, $response:ident), $broker:ident, $version:ident, $ctx:ident) => {
        let $request = encode_request(&$request, $version);
        let $bytes = handle(&$broker, $version, &$request, &$ctx)
            .await
            .expect("handle");
        let $response = decode_response(&$bytes, $version);
    };
}

#[tokio::test]
async fn denied_handler_preserves_topic_and_partition_response_fields() {
    let version = krabka_protocol::owned::list_offsets_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        deny_all,
        context(ctx, "alice")
    );
    let req = ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopic {
            name: "orders".into(),
            partitions: vec![
                ListOffsetsPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: LATEST_TIMESTAMP,
                    ..Default::default()
                },
                ListOffsetsPartition {
                    partition_index: 2,
                    current_leader_epoch: -1,
                    timestamp: EARLIEST_TIMESTAMP,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };
    answer_request!((req, bytes, resp), broker, version, ctx);

    let denied_row = |partition_index: i32| {
        tagged_wire!(ListOffsetsPartitionResponse {
            partition_index,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            timestamp: -1,
            offset: -1,
            leader_epoch: -1,
        })
    };
    let expected = unthrottled_wire!(ListOffsetsResponse {
        topics: vec![tagged_wire!(ListOffsetsTopicResponse {
            name: "orders".to_string(),
            partitions: vec![denied_row(0), denied_row(2)],
        })],
    });
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// Authorizer that denies `Describe` on a fixed set of topic names and
/// allows everything else. Drives the mixed authorized/denied scenarios
/// below without needing real ACL records in the metadata image.
use crate::handlers::list_offsets::test_support::DenyNamed;

/// Kafka's `handleListOffsetRequest` splits topics into authorized and
/// unauthorized up front, processes only the authorized ones, and appends
/// the unauthorized rows after them -- it never interleaves them in request
/// order. This runs the same three-topic request with the denied topic in
/// each position and checks that the denied row always lands last,
/// regardless of where it sat in the request.
#[tokio::test]
async fn denied_topic_rows_are_appended_after_authorized_rows_regardless_of_request_order() {
    struct Case {
        name: &'static str,
        request_topics: [&'static str; 3],
        denied: &'static str,
    }

    let version = krabka_protocol::owned::list_offsets_response::MAX_VERSION;

    let cases = [
        Case {
            name: "denied first",
            request_topics: ["denied", "b", "c"],
            denied: "denied",
        },
        Case {
            name: "denied middle",
            request_topics: ["a", "denied", "c"],
            denied: "denied",
        },
        Case {
            name: "denied last",
            request_topics: ["a", "b", "denied"],
            denied: "denied",
        },
    ];

    for case in cases {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::new(DenyNamed(std::collections::HashSet::from([
                case.denied
            ]))))
        );
        test_ctx!(ctx, "alice");

        let req = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: case
                .request_topics
                .iter()
                .map(|name| ListOffsetsTopic {
                    name: (*name).to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp: LATEST_TIMESTAMP,
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            timeout_ms: 30_000,
            ..Default::default()
        };
        answer_request!((req, bytes, resp), broker, version, ctx);

        let mut expected_order: Vec<&str> = case
            .request_topics
            .iter()
            .copied()
            .filter(|&name| name != case.denied)
            .collect();
        expected_order.push(case.denied);

        let actual_order: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert!(
            actual_order == expected_order,
            "{}: got {actual_order:?}, want {expected_order:?}",
            case.name,
        );

        let denied_topic = resp
            .topics
            .iter()
            .find(|t| t.name == case.denied)
            .expect("denied topic row present");
        assert!(
            denied_topic.partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{}: {denied_topic:?}",
            case.name,
        );

        broker_handle.shutdown().await;
    }
}

#[tokio::test]
async fn duplicate_partitions_get_invalid_request_on_every_row() {
    use super::test_support::client_for;

    const TOPIC: &str = "list-offsets-duplicate";

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let client = client_for(&broker_handle).await;
    client
        .send(crate::handlers::test_support::configured_topic_request(
            CreateTopicSetup {
                topic: TOPIC,
                num_partitions: crate::handlers::test_support::TopicPartitionCount(2),
                ..Default::default()
            },
        ))
        .await
        .expect("CreateTopics");
    broker_handle.wait_until_partition_present(TOPIC, 0).await;
    broker_handle.wait_until_partition_present(TOPIC, 1).await;

    let request = |partition_indexes: &[i32]| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: TOPIC.to_string(),
            partitions: partition_indexes
                .iter()
                .map(|&partition_index| ListOffsetsPartition {
                    partition_index,
                    current_leader_epoch: -1,
                    timestamp: LATEST_TIMESTAMP,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };

    // Kafka's duplicate check runs regardless of the answer a partition would
    // otherwise resolve to, so a "no duplicates" request first measures what
    // resolving partitions 0 and 1 actually returns; later cases reuse those
    // rows exactly for the partition that stays unduplicated, and only assert
    // `INVALID_REQUEST` for the ones the request names twice.
    let baseline = client
        .send(request(&[0, 1]))
        .await
        .expect("ListOffsets baseline");
    let resolved0 = baseline.topics[0].partitions[0].clone();
    let resolved1 = baseline.topics[0].partitions[1].clone();
    assert!(resolved0.error_code == codes::NONE, "{resolved0:?}");
    assert!(resolved1.error_code == codes::NONE, "{resolved1:?}");

    let invalid = |partition_index: i32| ListOffsetsPartitionResponse {
        partition_index,
        error_code: codes::INVALID_REQUEST,
        timestamp: -1,
        offset: -1,
        ..Default::default()
    };

    let cases: Vec<(&str, Vec<i32>, Vec<ListOffsetsPartitionResponse>)> = vec![
        (
            "no duplicates",
            vec![0, 1],
            vec![resolved0.clone(), resolved1.clone()],
        ),
        (
            "one partition duplicated",
            vec![0, 0],
            vec![invalid(0), invalid(0)],
        ),
        (
            "mixed: one partition duplicated, one resolved normally",
            vec![0, 0, 1],
            vec![invalid(0), invalid(0), resolved1.clone()],
        ),
    ];

    for (name, partition_indexes, expected) in cases {
        let response = client
            .send(request(&partition_indexes))
            .await
            .expect("ListOffsets");
        assert!(response.topics[0].partitions == expected, "{name}");
    }

    drop(client);
    broker_handle.shutdown().await;
}
