//! The SCRAM-SHA-512 and SCRAM-SHA-256 produce-and-consume round-trips over a
//! `SASL_PLAINTEXT` listener.
//!
//! Both digests share a file because they run the same two-stage scenario --
//! a PLAIN super-user provisions the credential through
//! `AlterUserScramCredentials`, then the provisioned user drives the RFC 5802
//! state machine -- and differ only in the digest they name.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, KAFKA_IMAGE_TXN, authenticated_console_round_trip,
        broker0_advertised, create_console_topic, docker_run_kafka_tool_with_image_and_mount,
        nc_check_connectivity, provision_plain_scram, scram_jaas, start_dual_mech_broker,
        start_dual_mech_broker_with_reauth, write_client_props, write_plain_props,
    },
    assert2::assert,
};

/// End-to-end `SASL_PLAINTEXT` + SCRAM-SHA-512 drive of the JVM tools
/// against a Rust broker. Exercises two distinct authentication paths in a
/// single run:
///
/// 1. **PLAIN as super-user.** The admin user authenticates with PLAIN and
///    runs `kafka-configs --alter --entity-type users --add-config
///    'SCRAM-SHA-512=[password=...]'`. On `cp-kafka:7.5.0` (Kafka 3.5+) the
///    JVM tool translates this to `AlterUserScramCredentials (api_key 51)`,
///    the KIP-554 typed request, which is what the broker's handler
///    accepts. On the older `cp-kafka:6.1.1` / Kafka 2.7 image the same
///    CLI invocation falls back to `IncrementalAlterConfigs (44)` with
///    `entity_type=USER`, which the broker does not implement.
///
/// 2. **SCRAM-SHA-512 as the provisioned user.** Alice then drives
///    `kafka-topics`, `kafka-console-producer`, and `kafka-console-consumer`
///    with `sasl.mechanism=SCRAM-SHA-512`. This exercises the RFC 5802 state
///    machine end-to-end through the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha512_produce_consume() {
    scram_round_trip("krabka-sasl-scram-itest", "SCRAM-SHA-512").await;
}

/// SHA-256 analog of `jvm_sasl_scram_sha512_produce_consume`.
/// The test provisions alice's credential with `kafka-configs --add-config
/// 'SCRAM-SHA-256=[password=...]'` (KIP-554 wire byte 1), then drives
/// produce + consume with `sasl.mechanism=SCRAM-SHA-256`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha256_produce_consume() {
    scram_round_trip("krabka-sasl-scram256-itest", "SCRAM-SHA-256").await;
}

/// KIP-368 in-band re-authentication under a two-second
/// `connections.max.reauth.ms`, driven by the JVM client's own re-auth code.
///
/// `kafka-verifiable-producer --throughput 1 --max-messages 10` keeps one
/// connection open for about ten seconds, which spans five re-auth windows.
/// The JVM `SaslClientAuthenticator` re-authenticates in band whenever it is
/// past the window the broker reported in `session_lifetime_ms`, so a run
/// that produces all ten records with no error is the client and the broker
/// agreeing on the window and on the SCRAM re-auth exchange. Without the
/// re-auth path, the broker would close the connection at the two-second
/// mark and the tool would report send errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_scram_sha512_in_band_reauth_under_max_reauth_window() {
    const TOPIC: &str = "krabka-sasl-scram-reauth-itest";

    let (broker, _dir) =
        start_dual_mech_broker_with_reauth(ADMIN, ADMIN_PASS, Some(krabka_units::secs(2))).await;
    nc_check_connectivity();

    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    provision_plain_scram(&admin_props.mount_str(), ALICE, ALICE_PASS, "SCRAM-SHA-512");
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin_props.mount_str()], TOPIC, 1, 1);
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_props.mount_str(), &[
        "kafka-acls",
        "--add",
        "--allow-principal",
        &format!("User:{ALICE}"),
        "--operation",
        "Write",
        "--topic",
        TOPIC,
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
    ]);

    // Ten records at one per second: about ten seconds on one connection,
    // five times the two-second re-auth window.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=SCRAM-SHA-512\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let out =
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &alice_props.mount_str(), &[
            "kafka-verifiable-producer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--max-messages",
            "10",
            "--throughput",
            "1",
            "--producer.config",
            "/client.properties",
        ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let acked = stdout.matches("producer_send_success").count();
    assert!(
        acked == 10,
        "expected 10 acked records across the re-auth windows, got {acked}: {stdout}"
    );
    assert!(
        !stdout.contains("producer_send_error"),
        "producer reported send errors under in-band re-auth: {stdout}"
    );

    broker.shutdown().await;
}

async fn scram_round_trip(topic: &str, mechanism: &str) {
    let (broker, _dir) = start_dual_mech_broker(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    provision_plain_scram(&admin_props.mount_str(), ALICE, ALICE_PASS, mechanism);

    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism={mechanism}\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        scram_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // Create the topic as admin.
    create_console_topic(KAFKA_IMAGE_TXN, &[&admin_props.mount_str()], topic, 1, 1);

    // Grant alice Read + Write on the topic. ACL implications cover
    // Describe.
    for op in ["Read", "Write"] {
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_props.mount_str(), &[
            "kafka-acls",
            "--add",
            "--allow-principal",
            &format!("User:{ALICE}"),
            "--operation",
            op,
            "--topic",
            topic,
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ]);
    }

    // Produce 10 records.

    authenticated_console_round_trip(KAFKA_IMAGE_TXN, &[&alice_mount], topic);

    broker.shutdown().await;
}
