//! Coverage for the `ListGroups` view of a share group, which is the request
//! `kafka-share-groups.sh --list` issues.
//!
//! The scenario lives in its own module because it exercises `ListGroups`
//! rather than the share-group RPCs, and asserts on the `group_type` tagging
//! and on the `types_filter` behaviour.

use {
    crate::share_group_harness::{boot, connect, create_topic, heartbeat},
    assert2::assert,
    krabka_protocol::owned::list_groups_request::ListGroupsRequest,
};

/// `kafka-share-groups.sh --list` sends `ListGroups` (`api_key` 16) with
/// `types_filter = ["share"]`. A live share group must appear in that response
/// tagged `group_type == "share"`, and must NOT appear when the request filters
/// on `["consumer"]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_share_group() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t7", 2).await;

    // Join a share group so it is registered in the coordinator.
    let mut join = heartbeat("g7", &uuid::Uuid::new_v4().to_string(), 0);
    join.subscribed_topic_names = Some(vec!["t7".into()]);
    let r = client.send(join).await.unwrap();
    assert!(r.error_code == 0, "join failed: {:?}", r.error_code);

    // types_filter = ["share"] → contains g7 tagged "share".
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["share".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[share]");
    assert!(resp.error_code == 0, "list error: {:?}", resp.error_code);
    let share_row = resp.groups.iter().find(|g| g.group_id == "g7");
    let share_row = share_row.unwrap_or_else(|| {
        panic!(
            "share group g7 missing from ListGroups[share], got {:?}",
            resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
        )
    });
    assert!(
        share_row.group_type == "share",
        "expected group_type=share, got {:?}",
        share_row.group_type
    );

    // types_filter = ["consumer"] → g7 must NOT appear.
    let resp = client
        .send(ListGroupsRequest {
            types_filter: vec!["consumer".into()],
            ..Default::default()
        })
        .await
        .expect("ListGroups[consumer]");
    assert!(
        !resp.groups.iter().any(|g| g.group_id == "g7"),
        "share group g7 must be excluded under types_filter=[consumer], got {:?}",
        resp.groups.iter().map(|g| &g.group_id).collect::<Vec<_>>()
    );

    // No filter → still contains g7 tagged "share".
    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("ListGroups[all]");
    let row = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g7")
        .expect("share group g7 present with no filter");
    assert!(
        row.group_type == "share",
        "unfiltered list must tag g7 as share, got {:?}",
        row.group_type
    );
}
