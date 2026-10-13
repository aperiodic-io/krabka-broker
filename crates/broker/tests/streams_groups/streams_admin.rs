//! The admin-visibility scenarios for a live streams group: what
//! `StreamsGroupDescribe` reports about it, and how `ListGroups` tags and
//! filters it.
//!
//! These are the two requests the JVM `kafka-streams-groups.sh` tool issues, so
//! they are grouped here rather than with the membership scenarios that assert
//! on heartbeat responses.

use {
    crate::streams_harness::{
        boot, connect, create_topic, describe, finalize_streams_version, join_and_converge,
        topology,
    },
    assert2::{assert, check},
    krabka_protocol::owned::list_groups_request::ListGroupsRequest,
};

/// After a member joins, `StreamsGroupDescribe` reports exactly one group row
/// for the group id with a clean error code, the member present, and a sane
/// group-state string.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_returns_the_group() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "desc-input", 2).await;

    let (member_id, resp) = join_and_converge(
        &client,
        "streams-app-3",
        topology("desc-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");
    assert!(!member_id.is_empty());

    let desc = describe(&client, "streams-app-3").await;
    assert!(
        desc.groups.len() == 1,
        "expected exactly one described group, got {}",
        desc.groups.len()
    );
    let g = &desc.groups[0];
    check!(g.error_code == 0, "describe error: {:?}", g.error_code);
    check!(
        g.group_id == "streams-app-3",
        "described group id mismatch: {:?}",
        g.group_id
    );
    check!(
        !g.members.is_empty(),
        "described group must list the joined member"
    );
    check!(
        g.members.iter().any(|m| m.member_id == member_id),
        "described group must contain member {member_id}, got {:?}",
        g.members.iter().map(|m| &m.member_id).collect::<Vec<_>>()
    );
    check!(
        !g.group_state.is_empty(),
        "group_state must be a non-empty phase string, got {:?}",
        g.group_state
    );
}

/// `ListGroups` surfaces a live streams group with `group_type = "streams"`
/// and honors `types_filter = ["streams"]`. That is the exact path the JVM
/// `kafka-streams-groups.sh` `AdminClient` uses with
/// `listGroups(typesFilter=[Streams])` before it issues
/// `StreamsGroupDescribe`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_surfaces_streams_group() {
    let (_b, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "list-input", 2).await;

    let (_member_id, resp) = join_and_converge(
        &client,
        "streams-app-5",
        topology("list-input", vec![]),
        2,
        10,
    )
    .await;
    assert!(resp.error_code == 0, "join error: {resp:?}");

    // Filtered list, as the JVM streams-groups admin tool issues it.
    let listed = client
        .send(ListGroupsRequest {
            types_filter: vec!["streams".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups");
    assert!(listed.error_code == 0, "ListGroups error: {listed:?}");
    let g = listed
        .groups
        .iter()
        .find(|g| g.group_id == "streams-app-5")
        .unwrap_or_else(|| panic!("streams group not listed: {:?}", listed.groups));
    assert!(
        g.group_type == "streams",
        "group_type must be 'streams', got {:?}",
        g.group_type
    );

    // A non-streams type filter must exclude it.
    let consumer_only = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups consumer");
    assert!(
        !consumer_only
            .groups
            .iter()
            .any(|g| g.group_id == "streams-app-5"),
        "streams group must not appear under a consumer type filter"
    );
}
