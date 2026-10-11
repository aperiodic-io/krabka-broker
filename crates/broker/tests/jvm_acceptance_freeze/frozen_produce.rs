//! What a stock `kafka-console-producer` makes of a topic that a write freeze
//! covers, and of one that no freeze covers.
//!
//! The one case here walks three topics through the same tool: a topic frozen
//! by its own name, a topic frozen by the namespace above it, and the unfrozen
//! control topic that keeps the other two rows honest.

use assert2::check;
use krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED};

use crate::{
    control_plane::{freeze, plain_topic_fixture},
    jvm_tool::jvm_produce,
    vocabulary::{
        CONTROL_TOPIC, LITERAL_REASON, LITERAL_TOPIC, POLICY_VIOLATION_EXCEPTION, PREFIX_REASON,
        PREFIX_SCOPE, PREFIX_TOPIC, freeze_refusal,
    },
};

/// A produce to a frozen topic reaches `kafka-console-producer` as a
/// `PolicyViolationException`, and an unfrozen topic is untouched.
///
/// This is the case KFC-9's compatibility section turns on. A stock producer
/// must fail the batch, must not retry, and must surface the broker's own
/// sentence -- which names the scope that matched, because a topic can be
/// frozen by its own name or by a prefix over a thousand topics and the thaw
/// is a different command in each case.
///
/// The three rows differ only in the topic the same tool writes to. The
/// prefixed row matters on its own: its topic appears in no registry entry by
/// name, so it exercises the prefix index rather than the literal one. The
/// control row is what keeps the other two honest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn the_jvm_console_producer_reads_a_freeze_as_a_policy_violation() {
    let (broker, client) = plain_topic_fixture(&[LITERAL_TOPIC, PREFIX_TOPIC, CONTROL_TOPIC]).await;
    freeze(&client, LITERAL_TOPIC, PATTERN_TYPE_LITERAL, LITERAL_REASON).await;
    freeze(&client, PREFIX_SCOPE, PATTERN_TYPE_PREFIXED, PREFIX_REASON).await;

    for (label, topic, refusal) in [
        (
            "a topic frozen by its own name",
            LITERAL_TOPIC,
            Some(freeze_refusal("literal", LITERAL_TOPIC, LITERAL_REASON)),
        ),
        (
            "a topic frozen by the namespace above it",
            PREFIX_TOPIC,
            Some(freeze_refusal("prefixed", PREFIX_SCOPE, PREFIX_REASON)),
        ),
        ("the unfrozen control topic", CONTROL_TOPIC, None),
    ] {
        let run = jvm_produce(&broker.container, topic, None, "a record");
        if let Some(message) = refusal {
            check!(
                !run.succeeded(),
                "{label}: a refused synchronous send must exit non-zero"
            );
            check!(
                run.says(POLICY_VIOLATION_EXCEPTION),
                "{label}: the producer must name the policy violation, got {run:?}"
            );
            check!(
                run.says(&message),
                "{label}: the producer must print {message:?}, got {run:?}"
            );
        } else {
            check!(
                run.succeeded(),
                "{label}: an unfrozen topic still takes writes, got {run:?}"
            );
            check!(
                !run.says(POLICY_VIOLATION_EXCEPTION),
                "{label}: no freeze covers this topic, got {run:?}"
            );
        }
    }
}
