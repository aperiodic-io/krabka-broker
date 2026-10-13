//! `kafka-topics --delete` against a cluster whose two-person rule has no
//! approved proposal for the delete.
//!
//! The create and the list on either side of the refusal are the control: the
//! same binary against the same broker still creates a topic and still lists
//! the one the gate saved.

use {
    crate::{
        host_broker::{gate_on, start_jvm_broker},
        jvm_tool::run_tool,
        vocabulary::{
            ACTION_DELETE_TOPIC, DOOMED_TOPIC, POLICY_VIOLATION_EXCEPTION, no_proposal_refusal,
        },
    },
    assert2::check,
};

/// `kafka-topics --delete` with no break-glass proposal fails, and carries the
/// broker's own refusal.
///
/// The tool holds every right Kafka asks for and the cluster is healthy, so
/// nothing in the Kafka protocol explains the failure except the message. That
/// makes the message the feature, and a response that dropped `error_message`
/// would still carry the right code while telling the operator nothing.
///
/// The create and the list around the delete are the control. The same binary,
/// against the same broker, must still create a topic and still list it
/// afterwards -- so a refusal here is the gate, and the topic really did
/// survive it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_topics_delete_carries_the_brokers_break_glass_refusal() {
    let broker = start_jvm_broker(gate_on).await;
    let bootstrap = broker.container.as_str();

    let created = run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--create",
        "--topic",
        DOOMED_TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
    ]);
    check!(
        created.succeeded(),
        "creating a topic is not gated, got {created:?}"
    );

    let deleted = run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--delete",
        "--topic",
        DOOMED_TOPIC,
    ]);
    let refusal = no_proposal_refusal(ACTION_DELETE_TOPIC, DOOMED_TOPIC);
    check!(
        !deleted.succeeded(),
        "a gated delete with no approval must exit non-zero"
    );
    check!(
        deleted.says(POLICY_VIOLATION_EXCEPTION),
        "the delete must name the policy violation, got {deleted:?}"
    );
    check!(
        deleted.says(&refusal),
        "the delete must print {refusal:?}, got {deleted:?}"
    );

    let listed = run_tool(None, None, &[
        "kafka-topics",
        "--bootstrap-server",
        bootstrap,
        "--list",
    ]);
    check!(listed.succeeded(), "listing is not gated, got {listed:?}");
    check!(
        listed.says(DOOMED_TOPIC),
        "the refusal must have left the topic in place, got {listed:?}"
    );
}
