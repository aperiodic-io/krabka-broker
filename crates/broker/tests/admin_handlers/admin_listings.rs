//! The two read-only listing APIs of this suite: `ListConfigResources`
//! (`api_key` 74, KIP-1142), whose default set covers every topic and broker, and
//! `ListGroups` (`api_key` 16). Both check the dispatch glue, the ACL gate's
//! allow path, and the response encoding, so they share one file.

use {
    crate::{
        RESOURCE_TYPE_TOPIC, admin_harness::create_topic_helper, support::start_n_node_client,
    },
    assert2::assert,
    krabka_protocol::owned::{
        list_config_resources_request::ListConfigResourcesRequest,
        list_groups_request::ListGroupsRequest,
    },
};

/// `ListConfigResources` v1 with empty `resource_types` returns the default
/// set: every topic and every broker, plus empty client-metrics. This test
/// verifies the dispatch glue, the ACL gate's allow path, and the response
/// encoding. The pure `collect_resources` helper has its own unit tests in
/// `crates/broker/src/handlers/list_config_resources.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_config_resources_default_set_includes_topics_and_brokers() {
    const RESOURCE_TYPE_BROKER: i8 = 4;

    let (_cluster, client) = start_n_node_client(1, "admin-handlers-test").await;

    create_topic_helper(&client, "t-lcr-a", 1).await;
    create_topic_helper(&client, "t-lcr-b", 1).await;

    let resp = client
        .send(ListConfigResourcesRequest::default())
        .await
        .expect("list_config_resources");
    assert!(resp.error_code == 0, "list_config_resources error_code");

    // Default set on a 1-broker cluster with two topics: 2 topic entries
    // (type 2) + 1 broker entry (type 4) + 0 client-metrics entries.
    let topics: Vec<&str> = resp
        .config_resources
        .iter()
        .filter(|r| r.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|r| r.resource_name.as_str())
        .collect();
    assert!(
        topics.contains(&"t-lcr-a") && topics.contains(&"t-lcr-b"),
        "expected both seeded topics in response, got {topics:?}"
    );
    let brokers: Vec<&str> = resp
        .config_resources
        .iter()
        .filter(|r| r.resource_type == RESOURCE_TYPE_BROKER)
        .map(|r| r.resource_name.as_str())
        .collect();
    assert!(
        brokers == vec!["1"],
        "expected exactly broker '1', got {brokers:?}"
    );
}

/// `ListGroups` includes a group that the test helper injected directly into
/// the `GroupManager`. The test does not run a full `JoinGroup` /
/// `SyncGroup` exchange.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_groups_includes_freshly_created_group() {
    let (cluster, client) = start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    // Seed the group manager directly with a new group.
    broker.group_create_for_test("test-group-listed");

    let resp = client
        .send(ListGroupsRequest::default())
        .await
        .expect("list_groups");
    assert!(resp.error_code == 0, "list_groups error_code");

    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    assert!(
        ids.contains(&"test-group-listed"),
        "expected `test-group-listed` in group list, got {ids:?}"
    );
}
