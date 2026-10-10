//! Static membership (`instance_id`) in a streams group, as Kafka 4.3.1 serves
//! it: not at all.
//!
//! Kafka 4.3.1's `GroupCoordinatorService.streamsGroupHeartbeat` runs
//! `throwIfStreamsGroupHeartbeatRequestIsUsingUnsupportedFeatures` before it
//! schedules the write, and that check refuses any request with an instance id
//! with `INVALID_REQUEST` and "Static membership is not yet supported.". The
//! coordinator never sees the request, so it writes no record, creates no
//! group and changes no member. Its own static paths (`streamsGroupLeave` and
//! `streamsGroupHeartbeat`) throw `UnsupportedOperationException` and are
//! unreachable. The broker's default configuration serves 4.3.1.

use assert2::{assert, check};
use krabka_protocol::owned::{
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

use crate::streams_harness::{
    boot, connect, create_topic, describe, finalize_streams_version, first_join, follow_up,
    join_and_converge, topology,
};

/// The Kafka error code `INVALID_REQUEST`.
const INVALID_REQUEST: i16 = 42;
/// The Kafka error code `GROUP_ID_NOT_FOUND`.
const GROUP_ID_NOT_FOUND: i16 = 69;

/// Every static membership case of KIP-1071 is refused with the same answer,
/// and leaves the group and its member as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_static_member_request_is_refused_and_changes_nothing() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "static-input", 2).await;
    finalize_streams_version(&client).await;

    let (member_id, joined) = join_and_converge(
        &client,
        "static-app",
        topology("static-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(joined.error_code == 0, "join error: {joined:?}");
    let epoch = joined.member_epoch;
    let before = describe(&client, "static-app").await;

    let instance = |request: StreamsGroupHeartbeatRequest| StreamsGroupHeartbeatRequest {
        instance_id: Some("instance-1".into()),
        ..request
    };
    let join = || first_join("static-app", topology("static-input", vec![]));
    // (row, the request)
    let rows = [
        ("a first join with an instance id", instance(join())),
        (
            "a rejoin of the known member with an instance id",
            instance(StreamsGroupHeartbeatRequest {
                member_id: member_id.clone(),
                ..join()
            }),
        ),
        (
            "a heartbeat of the known member with an instance id",
            instance(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: "static-app",
                member_id: &member_id,
                epoch: crate::support::streams::StreamsMemberEpoch(epoch),
                ..Default::default()
            })),
        ),
        (
            "a heartbeat with an instance id and another member id",
            instance(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: "static-app",
                member_id: "another-member",
                epoch: crate::support::streams::StreamsMemberEpoch(epoch),
                ..Default::default()
            })),
        ),
        (
            "a temporary leave at epoch -2",
            instance(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: "static-app",
                member_id: &member_id,
                epoch: crate::support::streams::StreamsMemberEpoch(-2),
                ..Default::default()
            })),
        ),
        (
            "a permanent leave at epoch -1 with an instance id",
            instance(follow_up(crate::support::streams::StreamsFollowUpSetup {
                group: "static-app",
                member_id: &member_id,
                epoch: crate::support::streams::StreamsMemberEpoch(-1),
                ..Default::default()
            })),
        ),
        (
            "a first join with an instance id to a group that does not exist",
            instance(first_join(
                "static-absent",
                topology("static-input", vec![]),
            )),
        ),
    ];
    let refused = StreamsGroupHeartbeatResponse {
        error_code: INVALID_REQUEST,
        error_message: Some("Static membership is not yet supported.".into()),
        ..Default::default()
    };
    for (row, request) in rows {
        let response = client.send(request).await.expect("StreamsGroupHeartbeat");
        check!(response == refused, "{row}");
    }

    // No row reached the coordinator: the group and its member are as they
    // were, and no group was created.
    check!(describe(&client, "static-app").await == before);
    let absent = describe(&client, "static-absent").await;
    check!(
        absent
            .groups
            .iter()
            .map(|g| g.error_code)
            .collect::<Vec<_>>()
            == vec![GROUP_ID_NOT_FOUND]
    );
}
