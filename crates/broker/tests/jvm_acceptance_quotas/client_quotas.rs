//! KIP-546 client-quota administration through `kafka-configs`: the alter,
//! describe, and delete round-trip for a user-scoped `producer_byte_rate`, an
//! `ip`-scoped `connection_creation_rate`, and a user-scoped
//! `controller_mutation_rate`.
//!
//! Each test ends by waiting for the deletion to reach the committed metadata
//! image, which is what proves the JVM tool's write went through raft rather
//! than only through the broker that served it.

use assert2::assert;

use crate::jvm_acceptance::{
    ADMIN, ADMIN_PASS, KAFKA_IMAGE_TXN, broker0_advertised,
    docker_run_kafka_tool_with_image_and_mount,
};

/// JVM acceptance: `kafka-configs --entity-type users` client quota round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe, and delete on a user-scoped `producer_byte_rate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_client_quota_end_to_end() {
    Box::pin(user_quota_round_trip(
        "producer_byte_rate",
        "1024",
        "producer_byte_rate=1024",
    ))
    .await;
}

/// JVM acceptance: `kafka-configs --entity-type ips` KIP-612 round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe (stdout substring), and delete-config on the
/// `connection_creation_rate` of `ip=127.0.0.1`. This test does not exercise
/// wall-time enforcement, because a single connection does not trigger the
/// rate limit. The Rust integration test in `tests/ip_quotas.rs` covers that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_ip_quota_end_to_end() {
    Box::pin(quota_round_trip(QuotaCase {
        entity: "ips",
        name: "127.0.0.1",
        metadata_entity: "ip",
        quota: "connection_creation_rate",
        value: "2.0",
        expected: "connection_creation_rate=2",
        label: "ip quota",
        users: &[],
    }))
    .await;
}

/// JVM acceptance: `kafka-configs --entity-type users controller_mutation_rate` round-trip.
///
/// Three-broker SASL/PLAINTEXT cluster. The JVM admin CLI runs alter,
/// describe (stdout substring), and delete-config on the
/// `controller_mutation_rate` of `user=alice`. This test does not check
/// wall-time enforcement: a single `kafka-topics --create` is one request,
/// with a maximum throttle of 1 s. The Rust integration test in
/// `tests/controller_mutation_quota.rs` covers enforcement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_alter_controller_mutation_rate_end_to_end() {
    Box::pin(user_quota_round_trip(
        "controller_mutation_rate",
        "2.0",
        "controller_mutation_rate=2",
    ))
    .await;
}

struct QuotaCase<'a> {
    entity: &'a str,
    name: &'a str,
    metadata_entity: &'a str,
    quota: &'a str,
    value: &'a str,
    expected: &'a str,
    label: &'a str,
    users: &'a [(&'a str, &'a str)],
}

async fn user_quota_round_trip(quota: &str, value: &str, expected: &str) {
    Box::pin(quota_round_trip(QuotaCase {
        entity: "users",
        name: "alice",
        metadata_entity: "user",
        quota,
        value,
        expected,
        label: "quota",
        users: &[("alice", "alice-secret")],
    }))
    .await;
}

async fn quota_round_trip(case: QuotaCase<'_>) {
    let QuotaCase {
        entity,
        name,
        metadata_entity,
        quota,
        value,
        expected,
        label,
        users,
    } = case;

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        Box::pin(crate::jvm_acceptance::start_registered_sasl_cluster(
            crate::jvm_acceptance::SaslClusterSetup {
                extra_users: users,
                ..Default::default()
            },
        ))
        .await;

    let admin_props = crate::jvm_acceptance::write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    // Set the user quota through the JVM admin client.
    let out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            entity,
            "--entity-name",
            name,
            "--add-config",
            &format!("{quota}={value}"),
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    eprintln!(
        "KRABKA[test] alter status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "alter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Describe — confirm visibility.
    // api_key 50 (DescribeUserScramCredentials) is implemented,
    // so the JVM tool exits 0 cleanly. Use the helper which asserts success.
    let desc = crate::jvm_acceptance::describe_console_entity(&admin_mount, entity, name);
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains(expected),
        "expected {label} in describe output: {stdout}"
    );

    // Delete the config.
    let del_out = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            entity,
            "--entity-name",
            name,
            "--delete-config",
            quota,
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        del_out.status.success(),
        "delete-config failed: {}",
        String::from_utf8_lossy(&del_out.stderr)
    );

    // Confirm the quota was cleared from the committed metadata image.
    h1.wait_for_image(|img| {
        let key: krabka_metadata::EntityKey =
            vec![(metadata_entity.to_string(), Some(name.to_string()))];
        img.client_quotas()
            .get(&key)
            .and_then(|m| m.get(quota))
            .is_none()
    })
    .await;

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
