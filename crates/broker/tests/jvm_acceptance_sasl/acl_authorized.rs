//! The authorized produce-and-consume round-trip for a principal that holds
//! literal topic and group ACLs.
//!
//! It is the positive half of the ACL enforcement cases, and it stands alone
//! because it is the only one that walks the whole consumer-group path --
//! `JoinGroup`, `OffsetFetch` and `OffsetCommit` -- through the authorize
//! preamble to a successful read.

use {
    crate::jvm_acceptance::{
        ALICE, ALICE_PASS, KAFKA_IMAGE_TXN, add_console_acl, assert_console_produced,
        broker0_advertised, docker_run_kafka_tool_with_image_and_mount, numbered_payload,
        plain_jaas, produce_console, start_plain_acl_topic_with_ops, write_client_props,
    },
    assert2::assert,
};

/// JVM acceptance: authorized produce + consume round-trip.
///
/// Admin (PLAIN super-user) provisions alice with:
/// - `Allow Read+Write Topic LITERAL "foo"`
/// - `Allow Read Group LITERAL "cg-foo"`
///
/// ACL implications grant Describe from Read/Write on the same resource, so
/// the test seeds no explicit Describe ACL. The Metadata per-topic check
/// relies on the implication path.
///
/// Alice (PLAIN, no super-user, no cluster perms) then drives
/// `kafka-console-producer` and `kafka-console-consumer --group cg-foo`
/// against the broker. This exercises the full `Produce`/`Fetch`/
/// `JoinGroup`/`OffsetFetch`/`OffsetCommit` authorize preambles end-to-end.
///
/// The test deliberately avoids topic auto-creation. Admin creates `foo`
/// before it grants alice access, so the Produce path does not have to
/// short-circuit on a missing topic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_authorized_produce_consume() {
    const TOPIC: &str = "foo";
    const GROUP: &str = "cg-foo";

    let (broker, _dir, admin_props) =
        start_plain_acl_topic_with_ops(TOPIC, ALICE, ALICE_PASS, &["Read", "Write"]).await;
    let admin_mount = admin_props.mount_str();

    // Allow Read on Group cg-foo for User:alice. ACL implications grant Describe
    // from Read on the same group resource, so no explicit Describe is
    // needed.
    add_console_acl(
        KAFKA_IMAGE_TXN,
        &[&admin_mount],
        "User:alice",
        &["Read"],
        "--group",
        GROUP,
    );

    // ---- Alice step: produce + consume over PLAIN as an ordinary user.
    //
    // `enable.idempotence=false` is required: cp-kafka 7.5 producers default
    // to idempotent mode, which sends `InitProducerId` without a
    // transactional id and so checks `Cluster IdempotentWrite` — a
    // cluster-scoped ACL that alice (a non-super-user with only topic +
    // group ACLs) doesn't hold. Falling back to the non-idempotent path
    // keeps alice's required ACL set bounded to what the plan calls out.
    let alice_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n\
         enable.idempotence=false\n\
         acks=1\n",
        plain_jaas(ALICE, ALICE_PASS),
    ));
    let alice_mount = alice_props.mount_str();

    // Produce 10 records via stdin.

    let payload: String = numbered_payload("msg", 10);
    let producer_out = produce_console(
        KAFKA_IMAGE_TXN,
        &[&alice_mount],
        TOPIC,
        false,
        payload.as_bytes(),
    );
    assert_console_produced(&producer_out);

    // Consume via `--group cg-foo --from-beginning` (the group-coordinator
    // path; exercises JoinGroup/OffsetFetch/OffsetCommit authorize).
    let consumer_out =
        docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &alice_mount, &[
            "kafka-console-consumer",
            "--bootstrap-server",
            broker0_advertised(),
            "--topic",
            TOPIC,
            "--group",
            GROUP,
            "--from-beginning",
            "--max-messages",
            "10",
            "--timeout-ms",
            "30000",
            "--consumer.config",
            "/client.properties",
        ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..10 {
        let needle = format!("msg-{i}");
        assert!(s.contains(&needle), "consumer missing {needle}: {s:?}");
    }

    broker.shutdown().await;
}
