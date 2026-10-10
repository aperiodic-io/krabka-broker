//! Tests 1 and 2: `IncrementalAlterConfigs` writes a throttle key and the
//! committed metadata image reports it back.
//!
//! Both run on the `SASL_PLAINTEXT` cluster, because a config alter is an
//! authorized operation and the suite wants a named super user driving it.
//! One test sets the broker-scoped rate, the other the topic-scoped replica
//! list.

use assert2::assert;

use crate::{
    cluster::{
        create_topic_as_admin, start_single_broker_sasl_plaintext_with_users, wait_partition_exists,
    },
    configs::drive_incremental_alter_configs,
};

/// Test 1: `IncrementalAlterConfigs` with `resource_type=Broker` sets the
/// `leader.replication.throttled.rate` key. The value must be visible in
/// the metadata image through `controller_image_for_test`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn broker_scoped_alter_persists_in_image() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    let node_id = handle.node_id();

    let err = drive_incremental_alter_configs(
        addr,
        "admin",
        "admin-secret",
        vec![(
            4, // resource_type = Broker
            node_id.to_string(),
            vec![(
                "leader.replication.throttled.rate".into(),
                Some("2048".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "alter should succeed; got error_code={err}");

    // Await until the config is visible (absorb raft commit latency).
    handle
        .wait_for_image(|img| {
            img.broker_throttle_rate(
                krabka_metadata::NodeId(node_id),
                krabka_metadata::ThrottleKind::Leader,
            ) == Some(krabka_units::bytes_per_sec(2048))
        })
        .await;
    handle.shutdown().await;
}

/// Test 2: `IncrementalAlterConfigs` with `resource_type=Topic` sets
/// `leader.replication.throttled.replicas`. `TopicThrottle::for_topic`
/// returns the partitions the list throttles on each broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_throttle_config_propagates() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    create_topic_as_admin(
        addr,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: crate::CLIENT_ID,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: ("foo").into(),
                ..Default::default()
            },
        },
    )
    .await;
    wait_partition_exists(&handle, "foo", 0).await;

    let err = drive_incremental_alter_configs(
        addr,
        "admin",
        "admin-secret",
        vec![(
            2, // resource_type = Topic
            "foo".into(),
            vec![(
                "leader.replication.throttled.replicas".into(),
                Some("0:1,0:2".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "topic alter should succeed; got error_code={err}");

    // Allow raft commit to propagate. Each broker keeps the entries that name
    // it: brokers 1 and 2 throttle partition 0, and broker 3 does not.
    handle
        .wait_for_image(|img| {
            let throttles_partition_0 = |broker| {
                krabka_broker::throttle::TopicThrottle::for_topic(
                    img,
                    "foo",
                    krabka_broker::NodeId(broker),
                )
                .leader
                .contains(0)
            };
            throttles_partition_0(1) && throttles_partition_0(2) && !throttles_partition_0(3)
        })
        .await;
    handle.shutdown().await;
}
