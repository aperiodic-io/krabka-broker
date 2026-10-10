//! A group name or a topic name of more than 32767 bytes reaches a barrier
//! handler as a compact string, and no `__barrier_state` record or marker can
//! carry it, because their strings have an `i16` length. Each handler that
//! takes a name answers `INVALID_REQUEST` (42) for one of 32768 bytes, and it
//! takes one of 32767 bytes past the check.
//!
//! The refusal must be an answer and not a panic in the connection task, and it
//! must leave no group behind.

use std::sync::Arc;

use assert2::check;
use krabka_protocol::krabka::barrier::{
    AlterBarrierGroupsRequest, AlterBarrierGroupsResponse, AlterableBarrierGroup,
    WritableBarrierPartition, WritableBarrierTopic, WriteBarrierMarkersRequest,
    WriteBarrierMarkersResponse,
};

use crate::{
    authorizer::AllowAllAuthorizer,
    barrier::handlers::{alter_groups, write_markers},
    codes,
    coordinator::unified::persistence::MAX_STRING_BYTES,
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

/// The only version of both requests.
const VERSION: i16 = 0;

#[derive(Clone, Copy, Default)]
enum GroupMutation {
    #[default]
    Upsert,
    Delete,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct AlterGroupSetup<'a> {
    #[default("short")]
    group: &'a str,
    #[default("orders")]
    topic: &'a str,
    mutation: GroupMutation,
}

fn entry(setup: AlterGroupSetup<'_>) -> AlterableBarrierGroup {
    let AlterGroupSetup {
        group,
        topic,
        mutation,
    } = setup;
    AlterableBarrierGroup {
        group: group.to_owned(),
        topics: vec![topic.to_owned()],
        interval_ms: -1,
        retained_cuts: 4,
        delete: matches!(mutation, GroupMutation::Delete),
        ..AlterableBarrierGroup::default()
    }
}

#[tokio::test]
async fn alter_barrier_groups_refuses_a_name_of_32768_bytes_and_creates_nothing() {
    let (handle, _dir, broker) = anonymous_broker().await;
    let (principal, peer) = (principal("ANONYMOUS"), peer());
    let long = "n".repeat(MAX_STRING_BYTES + 1);
    let request = AlterBarrierGroupsRequest {
        groups: vec![
            entry(AlterGroupSetup {
                group: &long,
                ..Default::default()
            }),
            entry(AlterGroupSetup {
                topic: &long,
                ..Default::default()
            }),
            entry(AlterGroupSetup {
                group: &long,
                mutation: GroupMutation::Delete,
                ..Default::default()
            }),
        ],
        ..AlterBarrierGroupsRequest::default()
    };

    let answer = alter_groups::handle(
        &broker,
        VERSION,
        1,
        &encode_request(&request, VERSION),
        &request_context(&principal, &peer, "barrier-string-bounds"),
    )
    .await
    .expect("the request is answered");

    let response: AlterBarrierGroupsResponse = decode_response(&answer, VERSION);
    let rows: Vec<(usize, i16)> = response
        .results
        .iter()
        .map(|row| (row.group.len(), row.error_code))
        .collect();
    check!(
        rows == vec![
            (long.len(), codes::INVALID_REQUEST),
            ("short".len(), codes::INVALID_REQUEST),
            (long.len(), codes::INVALID_REQUEST),
        ]
    );
    check!(
        broker
            .barrier_coordinator
            .describe_groups(&[])
            .await
            .is_empty()
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn write_barrier_markers_refuses_a_group_of_32768_bytes_on_every_partition() {
    let (handle, _dir, broker) = anonymous_broker().await;
    let (principal, peer) = (principal("ANONYMOUS"), peer());
    // The broker leads no `orders` partition, so a request that passes the
    // name check answers NOT_LEADER_OR_FOLLOWER for it.
    let cases = [
        (MAX_STRING_BYTES, codes::NOT_LEADER_OR_FOLLOWER),
        (MAX_STRING_BYTES + 1, codes::INVALID_REQUEST),
    ];
    for (length, expected) in cases {
        let request = WriteBarrierMarkersRequest {
            group: "g".repeat(length),
            epoch: 1,
            triggered_at: 1_000,
            topics: vec![WritableBarrierTopic {
                topic: "orders".to_owned(),
                partitions: vec![WritableBarrierPartition {
                    partition: 0,
                    expected_leader_epoch: 3,
                    ..WritableBarrierPartition::default()
                }],
                ..WritableBarrierTopic::default()
            }],
            ..WriteBarrierMarkersRequest::default()
        };

        let answer = write_markers::handle(
            &broker,
            VERSION,
            1,
            &encode_request(&request, VERSION),
            &request_context(&principal, &peer, "barrier-string-bounds"),
        )
        .await
        .expect("the request is answered");

        let response: WriteBarrierMarkersResponse = decode_response(&answer, VERSION);
        let codes_by_partition: Vec<i16> = response
            .topics
            .iter()
            .flat_map(|topic| topic.partitions.iter().map(|row| row.error_code))
            .collect();
        check!(codes_by_partition == vec![expected], "{length} bytes");
    }
    handle.shutdown().await;
}

async fn anonymous_broker() -> (
    crate::broker::BrokerHandle,
    tempfile::TempDir,
    Arc<crate::broker::Broker>,
) {
    let (handle, dir) =
        start_broker_with(|cfg| cfg.authorizer = Arc::new(AllowAllAuthorizer)).await;
    let broker = handle.broker_arc_for_test();
    (handle, dir, broker)
}
