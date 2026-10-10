//! Broker-side integration tests for KIP-599 `controller_mutation_rate`.
//!
//! Tests:
//! 1. `controller_mutation_rate_throttles_create_topics`. Set rate=2.0 for
//!    alice. Let one strict request cross the limit, then assert the next is
//!    rejected with `THROTTLING_QUOTA_EXCEEDED`.
//! 2. `unthrottled_create_topics_unaffected`. No quota. Create a topic.
//!    Assert `throttle_time_ms` == 0.
//! 3. `controller_mutation_rate_throttles_delete_topics`. Pre-create a topic
//!    with 10 partitions. Set rate=2.0 for alice. Alice deletes. Assert
//!    `throttle_time_ms` > 0.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `controller_mutation_quota/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "controller_mutation_quota/cluster.rs"]
mod cluster;
mod kafka_wire;
mod support;
#[path = "controller_mutation_quota/topic_admin.rs"]
mod topic_admin;

/// The client id every request header in this suite carries, which is what the
/// broker reads when it looks the mutation quota up.
const CLIENT_ID: &str = "krabka-mutation-quota-test";

use assert2::{assert, check};
use krabka_metadata::AclOperation;

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────
use crate::support::client::metric_value;
use crate::{
    cluster::start_single_broker_sasl_plaintext_with_users,
    support::acl::{ClusterAclSetup, TopicAclSetup},
    topic_admin::{drive_create_topics_sasl, drive_delete_topics_sasl},
};

/// Install Alice's shared mutation quota and return its complete wire response.
async fn set_controller_mutation_quota(
    addr: std::net::SocketAddr,
) -> Vec<(crate::kafka_wire::quotas::QuotaEntity, i16)> {
    crate::kafka_wire::quotas::alter_user_quota(
        addr,
        crate::kafka_wire::quotas::UserQuotaSetup {
            authentication: crate::kafka_wire::PlainAuthenticationSetup::admin(crate::CLIENT_ID),
            key: "controller_mutation_rate",
            value: crate::kafka_wire::quotas::QuotaValue(2.0),
            ..Default::default()
        },
    )
    .await
}

/// Test 1: Set `controller_mutation_rate=2.0` for alice. A strict v7 request
/// may cross the limit, but the following mutation is rejected while debt
/// remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_create_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed an ACL granting alice Cluster Create — this also disables the
    // compat shim (allow-all when no ACLs present in image).
    let admin_acl = crate::support::acl::cluster_acl_record(ClusterAclSetup {
        operation: AclOperation::Create,
        ..Default::default()
    });
    handle
        .submit_metadata_record_for_test(admin_acl)
        .await
        .expect("seed ACL");

    // Set controller_mutation_rate=2.0 for (user=alice).
    let alter = set_controller_mutation_quota(addr).await;
    assert!(alter[0].1 == 0, "alter should succeed");

    // Wait until the controller_mutation_rate quota is committed to this
    // broker's metadata image. The CreateTopics handler reads the rate
    // straight from the image on the first consume (the bucket is created
    // lazily with that rate; the refresh task only re-rates existing buckets),
    // so image visibility — not the refresh task — is the real precondition.
    handle
        .wait_for_image(|img| {
            img.client_quotas()
                .values()
                .any(|configs| configs.contains_key("controller_mutation_rate"))
        })
        .await;

    // This operation crosses the limit but is accepted under strict quota
    // semantics because the bucket was not already exhausted.
    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "alice", "alice-secret", "throttled-topic", 10).await;
    check!(
        err_code == 0,
        "create-topics should succeed (alice has Cluster Create ACL)"
    );
    check!(throttle_ms == 0);

    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "alice", "alice-secret", "rejected-topic", 1).await;
    check!(
        err_code == krabka_broker::codes::THROTTLING_QUOTA_EXCEEDED,
        "expected strict quota rejection, got error {err_code}"
    );
    check!(
        throttle_ms > 0,
        "expected throttle_time_ms > 0, got {throttle_ms}"
    );
}

/// Test 2: No quota configured. Create a topic. Assert
/// `throttle_time_ms` == 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_create_topics_unaffected() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;
    // No controller_mutation_rate quota configured.
    // admin is super_user, no ACL seeding needed.
    let _ = handle; // keep alive

    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "admin", "admin-secret", "unthrottled-topic", 10).await;
    assert!(err_code == 0);
    assert!(throttle_ms == 0);
}

/// Test 3: Pre-create a topic as admin with no quota. Set rate=2.0 for
/// alice. Alice deletes. Assert `throttle_time_ms` > 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_delete_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed a dummy ACL to disable the compat shim (allow-all when no ACLs present).
    // Use an unrelated ACL; the real alice ACLs come below.
    let shim_disable = crate::support::acl::topic_acl_record(TopicAclSetup {
        topic: "__compat_shim_disable__",
        principal: "User:admin",
        ..Default::default()
    });
    handle
        .submit_metadata_record_for_test(shim_disable)
        .await
        .expect("seed compat shim disable ACL");
    // Wait until the shim-disable ACL is visible in the metadata image so the
    // allow-all compat shim is actually off before the scenario proceeds.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "__compat_shim_disable__")
        })
        .await;

    // Pre-create topic as admin (no quota for admin) with 10 partitions.
    let (_, ec) = drive_create_topics_sasl(addr, "admin", "admin-secret", "to-delete", 10).await;
    assert!(ec == 0);

    // Grant alice Topic Delete on "to-delete".
    let alice_delete_acl = crate::support::acl::topic_acl_record(TopicAclSetup {
        topic: "to-delete",
        operation: AclOperation::Delete,
        ..Default::default()
    });
    handle
        .submit_metadata_record_for_test(alice_delete_acl)
        .await
        .expect("seed alice Delete ACL");
    // Wait until alice's Delete ACL on "to-delete" is visible in the image so
    // the later delete is authorized.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "to-delete" && a.principal == "User:alice")
        })
        .await;

    // Now set the quota for alice and delete.
    let alter = set_controller_mutation_quota(addr).await;
    assert!(alter[0].1 == 0);
    // Wait for alice's controller_mutation_rate quota to land in the image
    // before deleting; DeleteTopics reads the rate from the image on consume.
    handle
        .wait_for_image(|img| {
            img.client_quotas()
                .values()
                .any(|configs| configs.contains_key("controller_mutation_rate"))
        })
        .await;

    let (throttle_ms, err_code) =
        drive_delete_topics_sasl(addr, "alice", "alice-secret", "to-delete").await;
    assert!(err_code == 0);
    assert!(
        throttle_ms > 0,
        "expected throttle_time_ms > 0, got {throttle_ms}"
    );

    // The broker sleeps on the KIP-599 delay, so it is an applied throttle and
    // has to be visible as one: an operator watching DeleteTopics latency rise
    // must be able to tell "alice is over her mutation quota" from "the
    // controller is wedged".
    let applied_seconds = f64::from(throttle_ms) / 1000.0;
    let by_quota = metric_value(
        &handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"ControllerMutation\"}",
    )
    .await;
    check!(
        by_quota >= applied_seconds,
        "controller_mutation_rate must be credited with the applied throttle \
         ({applied_seconds}s); got {by_quota}"
    );
    let phase = metric_value(
        &handle,
        "krabka_broker_request_throttle_duration_seconds_sum{api_key=\"DeleteTopics\"}",
    )
    .await;
    check!(
        phase >= applied_seconds,
        "the throttle phase must cover the delay the response reported \
         ({applied_seconds}s); got {phase}"
    );
}
