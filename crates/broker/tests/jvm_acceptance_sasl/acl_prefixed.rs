//! The prefixed topic ACL case: a `PREFIXED` pattern grants exactly the topics
//! that carry the prefix and nothing else.
//!
//! The prefix pattern gets its own file because the case needs both outcomes
//! from one grant -- an allowed topic and a denied one -- and so seeds two
//! topics and runs two consumers, which no other ACL case does.

use {
    crate::jvm_acceptance::{
        ADMIN, ADMIN_PASS, ALICE, ALICE_PASS, KAFKA_IMAGE_TXN, broker0_advertised,
        create_console_topic, denied_console_consumer, docker_run_kafka_tool_with_image_and_mount,
        nc_check_connectivity, plain_jaas, produce_console,
        start_sasl_plaintext_broker_with_super_user, write_client_props, write_plain_props,
    },
    assert2::assert,
};

/// JVM acceptance: a prefixed topic ACL grants exactly the prefix.
///
/// Admin provisions:
/// - `Allow Read Topic PREFIXED "team-"` for alice (Describe implied by Read)
/// - `Allow Read Group LITERAL "cg-prefixed"` for alice (Describe implied by Read)
///
/// Admin then creates two topics: `team-foo`, which the prefix covers, and
/// `other-foo`, which it does NOT cover. Admin seeds one record into each.
/// Admin is a super-user, so it bypasses authorize.
///
/// Alice's consumer:
/// 1. `--topic team-foo` succeeds and reads the seeded record. This
///    exercises the PREFIXED Read path in `authorize`.
/// 2. `--topic other-foo` fails with `TopicAuthorizationException`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_prefixed_topic_acl_works() {
    const PREFIX: &str = "team-";
    const TOPIC_OK: &str = "team-foo";
    const TOPIC_DENIED: &str = "other-foo";
    const GROUP: &str = "cg-prefixed";

    let (broker, _dir) = start_sasl_plaintext_broker_with_super_user(ADMIN, &[
        (ADMIN, ADMIN_PASS),
        (ALICE, ALICE_PASS),
    ])
    .await;
    nc_check_connectivity();

    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    let admin_mount = admin_props.mount_str();

    // Pre-create both topics.
    for topic in [TOPIC_OK, TOPIC_DENIED] {
        create_console_topic(KAFKA_IMAGE_TXN, &[&admin_mount], topic, 1, 1);
    }

    // Prefixed Read on `team-*` for alice. ACL implications grant Describe from
    // Read on the same topic resource.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--resource-pattern-type",
        "prefixed",
        "--topic",
        PREFIX,
    ]);

    // Literal Read on group `cg-prefixed`. ACL implications grant Describe from
    // Read on the same group resource.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &admin_mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--group",
        GROUP,
    ]);

    // Seed one record into each topic as admin (super-user bypasses authorize).
    let admin_producer_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_producer_mount = admin_producer_props.mount_str();

    for topic in [TOPIC_OK, TOPIC_DENIED] {
        let seed_out = produce_console(
            KAFKA_IMAGE_TXN,
            &[&admin_producer_mount],
            topic,
            false,
            format!("seed-{topic}\n").as_bytes(),
        );
        assert!(
            seed_out.status.success(),
            "admin seed producer failed for {topic}: stderr={}",
            String::from_utf8_lossy(&seed_out.stderr),
        );
    }

    // ---- Alice: consume team-foo (allowed by prefix).
    let alice_props = write_plain_props(ALICE, ALICE_PASS);
    let alice_mount = alice_props.mount_str();

    let consumer_out =
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &alice_mount, &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC_OK,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ]);
    let stdout = String::from_utf8_lossy(&consumer_out.stdout);
    let needle = format!("seed-{TOPIC_OK}");
    assert!(
        stdout.contains(&needle),
        "alice should read {needle} from prefixed topic; got: {stdout}",
    );

    // ---- Alice: consume other-foo (denied — no matching prefix).
    let denied_out = denied_console_consumer(
        &alice_mount,
        TOPIC_DENIED,
        GROUP,
        "spawn alice denied consumer",
    );
    let denied_stderr = String::from_utf8_lossy(&denied_out.stderr);
    let denied_stdout = String::from_utf8_lossy(&denied_out.stdout);
    eprintln!(
        "KRABKA[test] alice denied consumer status={} stderr={denied_stderr} stdout={denied_stdout}",
        denied_out.status,
    );
    assert!(
        denied_stderr.contains("TopicAuthorizationException"),
        "alice should be denied on {TOPIC_DENIED}; stderr={denied_stderr} stdout={denied_stdout}",
    );

    broker.shutdown().await;
}
