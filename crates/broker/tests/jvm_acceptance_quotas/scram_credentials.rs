//! The KIP-554 read half, `api_key` 50: `kafka-configs --describe
//! --entity-type users` over a SCRAM credential the same tool provisioned.

use assert2::assert;

use crate::jvm_acceptance::{
    ADMIN, ADMIN_PASS, KAFKA_IMAGE_TXN, broker0_advertised,
    docker_run_kafka_tool_with_image_and_mount, nc_check_connectivity,
    start_three_broker_sasl_plaintext_jvm_cluster_with_users,
};

/// JVM acceptance: `kafka-configs --describe --entity-type users` round-trip for
/// SCRAM credentials (KIP-554 read half, `api_key` 50).
///
/// Three-broker SASL/PLAINTEXT cluster. The test provisions alice's
/// SCRAM-SHA-512 credential with `kafka-configs --alter --add-config
/// SCRAM-SHA-512=[...]`, then describes it and asserts exit 0 and
/// `SCRAM-SHA-512` in stdout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_configs_describe_users_scram_credentials_end_to_end() {
    let (h1, _h2, _h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_users(
            crate::jvm_acceptance::SaslClusterSetup::default(),
        )
        .await;
    nc_check_connectivity();

    let admin_props = crate::jvm_acceptance::write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    // Provision a SCRAM user via kafka-configs --alter (hits AlterUserScramCredentials, api_key 51).
    let alter = docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-configs",
            "--alter",
            "--entity-type",
            "users",
            "--entity-name",
            "alice",
            "--add-config",
            "SCRAM-SHA-512=[iterations=4096,password=alice-secret]",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
    );
    assert!(
        alter.status.success(),
        "alter SCRAM failed: {}",
        String::from_utf8_lossy(&alter.stderr)
    );

    // Describe — should exit 0 cleanly (api_key 50 now implemented).
    let desc = crate::jvm_acceptance::describe_console_entity(&admin_mount, "users", "alice");
    let stdout = String::from_utf8_lossy(&desc.stdout);
    assert!(
        stdout.contains("SCRAM-SHA-512"),
        "expected SCRAM-SHA-512 in describe output: {stdout}"
    );

    let _ = h1; // keep alive
}
