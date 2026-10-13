//! The KIP-48 delegation-token lifecycle through `kafka-delegation-tokens`:
//! create, authenticate a producer with the minted credentials, describe, and
//! expire.
//!
//! The token producer runs through a directly spawned `docker run`, not the
//! shared helper, because `kafka-console-producer` needs its records on stdin.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, KAFKA_IMAGE_TXN, broker0_advertised, create_console_topic,
        docker_run_kafka_tool_with_image_and_mount, extract_jvm_kv, nc_check_connectivity,
        produce_console, start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens,
        wait_three_brokers_registered, write_client_props, write_plain_props,
    },
    assert2::assert,
};

/// JVM acceptance: KIP-48 delegation-token round-trip through the official
/// `kafka-delegation-tokens` admin CLI.
///
/// 3-broker `SASL_PLAINTEXT` cluster with both `PLAIN` (admin auth) and
/// `SCRAM-SHA-256` (token auth) mechanisms enabled, plus a master
/// delegation-token HMAC key. The flow:
///
/// 1. Admin (PLAIN) calls `kafka-delegation-tokens --create`. The broker
///    mints a token, replicates `V1DelegationToken` through raft, and
///    returns `(TokenID, HMAC, …)`.
/// 2. Build a `token.properties` that references those credentials with
///    `sasl.mechanism=SCRAM-SHA-256`.
/// 3. `kafka-console-producer --producer.config token.properties` produces
///    one record. It authenticates against the token-fallback path of the
///    SCRAM handler.
/// 4. `kafka-delegation-tokens --describe --owner-principal User:admin`
///    lists the token. The test matches a substring on `TokenID`.
/// 5. `kafka-delegation-tokens --expire --expiry-time-period -1 --hmac
///    <hmac>` deletes the token.
///
/// `#[ignore = "requires Docker"]`, so run with `--ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_delegation_tokens_end_to_end() {
    const TOPIC: &str = "krabka-deleg-token-itest";
    const SECRET: &[u8] = b"jvm-master-key";

    let (h1, h2, h3, _cfg1, _cfg2, _cfg3, _d1, _d2, _d3) =
        start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens(
            ADMIN, ADMIN_PASS, SECRET,
        )
        .await;
    nc_check_connectivity();

    wait_three_brokers_registered(&h1, &h2, &h3, 3).await;

    // Admin properties: PLAIN, super-user — used for create/describe/expire.
    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    // 1. Create the token. `--max-life-time-period -1` ⇒ use the broker's
    //    configured `delegation.token.max.lifetime.ms` default.
    let create_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--create",
        "--max-life-time-period",
        "-1",
    ]);
    let create_stdout = String::from_utf8_lossy(&create_out.stdout).to_string();
    eprintln!("KRABKA[test] --create stdout:\n{create_stdout}");

    let token_id = extract_jvm_kv(&create_stdout, "TOKENID");
    let hmac = extract_jvm_kv(&create_stdout, "HMAC");
    assert!(
        !token_id.is_empty(),
        "empty TOKENID; stdout: {create_stdout}"
    );
    assert!(!hmac.is_empty(), "empty HMAC; stdout: {create_stdout}");

    // 2. Build token.properties referencing the new credentials via
    //    SCRAM-SHA-256 (the JVM client SASL mechanism for delegation
    //    tokens per KIP-48).
    let token_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-256\n\
         sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
         tokenauth=true \
         username=\"{token_id}\" password=\"{hmac}\";\n\
         enable.idempotence=false\n\
         acks=1\n",
    ));
    let token_mount = token_props.mount_str();

    // 3. Create the topic as admin so the token producer can target it.
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin_mount], TOPIC, 1, 1);

    // 4. Produce one message authenticated as the delegation token.

    let producer_out = produce_console(KAFKA_IMAGE_TXN, &[&token_mount], TOPIC, false, b"hello\n");
    assert!(
        producer_out.status.success(),
        "token producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // 5. Describe — confirm the token is visible to the owner principal.
    let desc_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--describe",
        "--owner-principal",
        "User:admin",
    ]);
    let desc_stdout = String::from_utf8_lossy(&desc_out.stdout);
    assert!(
        desc_stdout.contains(&token_id),
        "--describe stdout missing token_id={token_id}: {desc_stdout}",
    );

    // 6. Expire the token; `--expiry-time-period -1` deletes immediately.
    let exp_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-delegation-tokens",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--expire",
        "--expiry-time-period",
        "-1",
        "--hmac",
        &hmac,
    ]);
    assert!(
        exp_out.status.success(),
        "--expire failed: stdout={} stderr={}",
        String::from_utf8_lossy(&exp_out.stdout),
        String::from_utf8_lossy(&exp_out.stderr),
    );

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
