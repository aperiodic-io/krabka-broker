//! Running one Kafka CLI tool in a cp-kafka container, and keeping every part
//! of what it did.
//!
//! Three of the four cases are about a tool that fails, so a run is captured
//! rather than asserted on: [`ToolRun`] carries the exit status beside both
//! output streams, and each case decides what that means. The rest of the
//! module is the argument list of each tool the suite drives.

use {
    crate::{
        jvm_acceptance::{ClientPropsFile, KAFKA_IMAGE_TXN, tool_output},
        support,
        vocabulary::{CONFIGS_TOPIC, CONTAINER_PROPS, ELECT_TOPIC},
    },
    std::process::{ExitStatus, Output},
};

/// One JVM tool run: how it exited, and everything it printed.
#[derive(Debug)]
pub(super) struct ToolRun {
    /// The container's exit status. Three of the four cases here are about a
    /// non-zero one, which is why this suite cannot use
    /// [`crate::jvm_acceptance::docker_run_kafka_tool_with_image`]: that helper
    /// asserts success.
    status: ExitStatus,
    /// stdout followed by stderr. The JVM tools split a failure across the two
    /// in ways that differ per tool -- `kafka-topics` prints the message on
    /// stdout and the stack trace on stderr, an uncaught exception goes only
    /// to stderr -- so both are searched as one text.
    output: String,
}

impl ToolRun {
    /// Merge one finished `docker run` into a run, and echo it for
    /// `--nocapture`.
    fn from_output(out: &Output, args: &[&str]) -> Self {
        let output = tool_output(out);
        let status = out.status;
        eprintln!("KRABKA[test] jvm tool {args:?} status={status}\n{output}");
        Self { status, output }
    }

    /// Whether the tool exited zero.
    pub(super) fn succeeded(&self) -> bool { self.status.success() }

    /// Whether the tool printed `needle` on either stream.
    pub(super) fn says(&self, needle: &str) -> bool { self.output.contains(needle) }
}

/// Run one Kafka CLI tool in a cp-kafka container, and hand back what it did.
///
/// This mirrors [`crate::jvm_acceptance::docker_run_kafka_tool_with_image`] -- same
/// image, same `--add-host` mapping onto the bridge gateway, same capture of
/// both streams -- and differs in the one way this suite needs: it does not
/// assert that the tool succeeded.
///
/// `stdin`, when given, adds `-i` and feeds the text in. Without it the
/// container has no stdin at all, and `kafka-console-producer` then reads EOF
/// immediately, produces nothing, and exits zero -- which would pass the
/// frozen-topic case for entirely the wrong reason.
///
/// The container is named rather than anonymous so that a run killed
/// mid-flight leaves something an operator can find and remove.
pub(super) fn run_tool(
    props: Option<&ClientPropsFile>,
    stdin: Option<&str>,
    args: &[&str],
) -> ToolRun {
    let name = support::unique_container_name("kfc9-jvm");
    let mount = props.map(ClientPropsFile::mount_str);
    let mut options = vec!["--name", name.as_str()];
    if stdin.is_some() {
        options.push("-i");
    }
    options.push("--add-host=host.docker.internal:host-gateway");
    if let Some(mount) = mount.as_deref() {
        options.extend(["-v", mount]);
    }
    let mut command = support::docker_tool_command(KAFKA_IMAGE_TXN, &options);
    command.args(args);

    let out = support::docker_output(&mut command, stdin, "write the record to the tool's stdin");
    ToolRun::from_output(&out, args)
}

/// Produce one record to `topic` with `kafka-console-producer`.
///
/// `--sync` is load-bearing, and not a stylistic choice. Without it the
/// console producer hands each send to an error-logging callback, prints the
/// failure, and exits zero; with it the producer awaits the future, the
/// broker's refusal surfaces as an `ExecutionException`, and the tool exits
/// one. An exit code is the only part of this that a runbook can branch on.
pub(super) fn jvm_produce(
    bootstrap: &str,
    topic: &str,
    props: Option<&ClientPropsFile>,
    record: &str,
) -> ToolRun {
    let mut args = vec![
        "kafka-console-producer",
        "--bootstrap-server",
        bootstrap,
        "--topic",
        topic,
        "--sync",
    ];
    if props.is_some() {
        args.extend_from_slice(&["--producer.config", CONTAINER_PROPS]);
    }
    run_tool(props, Some(&format!("{record}\n")), &args)
}

/// Ask `kafka-leader-election` for an unclean election of partition 0.
///
/// Preferred election is not gated, so the election type is the whole of what
/// puts this request in front of the two-person rule.
pub(super) fn jvm_unclean_election(bootstrap: &str, props: &ClientPropsFile) -> ToolRun {
    run_tool(Some(props), None, &[
        "kafka-leader-election",
        "--bootstrap-server",
        bootstrap,
        "--election-type",
        "unclean",
        "--topic",
        ELECT_TOPIC,
        "--partition",
        "0",
        "--admin.config",
        CONTAINER_PROPS,
    ])
}

/// Read one topic's dynamic configs with `kafka-configs --describe`.
pub(super) fn jvm_describe_configs(bootstrap: &str) -> ToolRun {
    run_tool(None, None, &[
        "kafka-configs",
        "--bootstrap-server",
        bootstrap,
        "--entity-type",
        "topics",
        "--entity-name",
        CONFIGS_TOPIC,
        "--describe",
    ])
}
