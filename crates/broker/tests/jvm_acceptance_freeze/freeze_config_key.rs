//! What `kafka-configs` can read of a freeze, and what it cannot do to one.
//!
//! The synthesised `write.freeze` topic config is the only place an operator
//! holding nothing but the JVM tools can see the scope that froze a topic. The
//! three alter rows are the other half of that rule: the key takes no write,
//! whichever operation the tool sends.

use {
    crate::{
        control_plane::{freeze, plain_topic_fixture},
        jvm_tool::{jvm_describe_configs, run_tool},
        vocabulary::{
            CONFIGS_REASON, CONFIGS_TOPIC, INVALID_CONFIG_EXCEPTION, WRITE_FREEZE_ALTER_REFUSAL,
            write_freeze_value,
        },
    },
    assert2::check,
    krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL,
};

/// `kafka-configs` reads the freeze and cannot write it.
///
/// KFC-9 synthesises a read-only `write.freeze` topic config so that an
/// operator holding only the JVM tools can see a freeze at all: they cannot
/// call `DescribeTopicFreezes`, and the value is the one place the scope that
/// froze the topic is legible to them. The other half of the rule is that the
/// key is never writable, because a key that could be set through
/// `AlterConfigs` would put the freeze registry behind an ordinary topic-config
/// ACL and let a snapshot restore resurrect a stale freeze.
///
/// The three alter rows differ only in the operation the same tool sends: set
/// a freeze, clear one, and delete the key. All three are refused with one
/// wording, and the `--delete-config` row reaches the broker at all only
/// because the frozen topic reports the key as a dynamic config, which is what
/// `kafka-configs` checks before it sends a delete.
///
/// The describe after the loop is the proof that the refusals refused. Three
/// rejected alters that had quietly changed the registry would pass every
/// assertion above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_configs_reads_the_freeze_and_cannot_write_it() {
    let (broker, client) = plain_topic_fixture(&[CONFIGS_TOPIC]).await;
    freeze(&client, CONFIGS_TOPIC, PATTERN_TYPE_LITERAL, CONFIGS_REASON).await;

    let frozen = write_freeze_value(CONFIGS_TOPIC);
    let described = jvm_describe_configs(&broker.container);
    check!(
        described.succeeded(),
        "describing a frozen topic is not gated, got {described:?}"
    );
    check!(
        described.says(&frozen),
        "the describe must show {frozen:?}, got {described:?}"
    );

    for (label, flag, value) in [
        (
            "set a freeze through the config key",
            "--add-config",
            "write.freeze=true",
        ),
        (
            "clear a freeze through the config key",
            "--add-config",
            "write.freeze=false",
        ),
        (
            "delete the config key outright",
            "--delete-config",
            "write.freeze",
        ),
    ] {
        let run = run_tool(None, None, &[
            "kafka-configs",
            "--bootstrap-server",
            &broker.container,
            "--entity-type",
            "topics",
            "--entity-name",
            CONFIGS_TOPIC,
            "--alter",
            flag,
            value,
        ]);
        check!(!run.succeeded(), "{label}: must exit non-zero");
        check!(
            run.says(INVALID_CONFIG_EXCEPTION),
            "{label}: must name the invalid configuration, got {run:?}"
        );
        check!(
            run.says(WRITE_FREEZE_ALTER_REFUSAL),
            "{label}: must name the command that does set the key, got {run:?}"
        );
    }

    let after = jvm_describe_configs(&broker.container);
    check!(
        after.says(&frozen),
        "the refusals must have left the freeze exactly as it was, got {after:?}"
    );
}
