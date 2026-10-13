//! Docker plumbing shared by every KIP-320 divergence scenario.
//!
//! The module holds the container images the suite pins, the `docker` CLI
//! wrappers that start, pause, and remove containers, and the two helpers that
//! reach a Kafka CLI tool: a throwaway tool container and a JVM
//! `kafka-console-producer` fed over stdin. Every scenario needs them, so they
//! live apart from any one scenario.

use {
    crate::support::{bridge_gateway, jvm_acks_all_producer, jvm_tool_output},
    base64::Engine as _,
    std::process::Command,
    uuid::Uuid,
};

/// cp-kafka 6.1.1 (Kafka 2.7) ships the standard Apache Kafka CLI tools used
/// for produce / topic admin / `kafka-dump-log`. NOTE: its bundled consumer
/// only negotiates Fetch up to v11 and predates client-side KIP-320 position
/// validation, so these tests do NOT use it for the Fetch-v12+
/// wire-conformance probe. That probe needs [`KAFKA_IMAGE_MODERN`].
pub const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:6.1.1";
/// cp-kafka 7.5.0 (Kafka 3.5) is the modern client image. Its consumer
/// negotiates Fetch v12+ and runs the full KIP-320 client path
/// (`OffsetForLeaderEpoch` position validation + tagged `diverging_epoch` /
/// `current_leader` decode), and it ships a JDK with `javac`. These tests use
/// it to compile and run the wire-conformance Java helper.
pub const KAFKA_IMAGE_MODERN: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.5.0";
/// mirror.gcr.io/apache/kafka:4.0.0 is the `KRaft`-native broker used as the JVM member of the
/// mixed metadata quorum (same image as `jvm_static_quorum_spike.rs`).
pub const KAFKA_IMAGE_KRAFT: &str = "mirror.gcr.io/apache/kafka:4.0.0";
/// Newer CLI image used only as an `AdminClient`. Its `kafka-features.sh`
/// exposes the explicit safe and unsafe `metadata.version` downgrade commands.
pub const KAFKA_IMAGE_FEATURES: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// Kafka encodes a 16-byte UUID cluster id as URL-safe base64 with no
/// padding. The JVM `--cluster-id` string and Krabka's `uuid::Uuid` must wrap
/// the *same* 16 bytes or the two sides reject each other on cluster-id
/// mismatch. This helper is lifted verbatim from `jvm_static_quorum_spike.rs`.
pub fn kafka_cluster_id_string(id: Uuid) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(id.as_bytes())
}

pub fn docker_rm(name: &str) { let _ = Command::new("docker").args(["rm", "-f", name]).output(); }

pub fn set_container_paused(name: &str, paused: bool) {
    let action = if paused { "pause" } else { "unpause" };
    let status = Command::new("docker")
        .args([action, name])
        .status()
        .unwrap_or_else(|error| panic!("{action} JVM broker: {error}"));
    assert2::assert!(status.success(), "{action} JVM broker failed");
}

/// Address of the default Docker bridge as seen by both host processes and
/// containers. Mixed-cluster broker endpoints must work from both sides:
/// `host.docker.internal` is container-only on Linux, while this numeric
/// gateway is routable from Krabka and the JVM/tool containers alike.
pub fn docker_bridge_gateway() -> String {
    let output = Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            // Both fields, separated by a character neither can contain. A
            // space would not do: `Gateway` can render empty, and
            // `split_whitespace` would then hand back the subnet as if it were
            // the gateway.
            "{{(index .IPAM.Config 0).Gateway}}|{{(index .IPAM.Config 0).Subnet}}",
        ])
        .output()
        .expect("docker network inspect bridge");
    assert2::assert!(output.status.success(), "inspect Docker bridge gateway");
    let rendered = String::from_utf8(output.stdout).expect("Docker bridge gateway is UTF-8");
    bridge_gateway(&rendered).unwrap_or_else(|| {
        panic!("no Docker bridge gateway in `docker network inspect bridge`: {rendered:?}")
    })
}

/// Run a bundled Kafka CLI tool in a throwaway cp-kafka container on the
/// default bridge with `host.docker.internal` wired to the host gateway.
/// Mirrors `jvm_acceptance.rs::docker_run_kafka_tool_with_image`.
pub fn docker_run_kafka_tool_with_image(image: &str, args: &[&str]) -> std::process::Output {
    jvm_tool_output(image, args, "kip320")
}

/// Produce `lines` to `topic` partition 0 with the JVM `kafka-console-producer`
/// at `acks=all`, one record per line. Panics on producer failure.
pub fn produce_lines_via_jvm(bootstrap: &str, topic: &str, lines: &[String]) {
    let mut child = jvm_acks_all_producer(KAFKA_IMAGE, bootstrap, topic);
    {
        use std::io::Write as _;
        let stdin = child.stdin.as_mut().expect("stdin");
        for l in lines {
            writeln!(stdin, "{l}").expect("write line");
        }
    }
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait producer");
    assert2::assert!(
        out.status.success(),
        "JVM producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Seed a committed prefix before the scenario waits for its external follower.
pub fn produce_committed_prefix(bootstrap: &str, topic: &str, prefix: &str, count: usize) {
    produce_lines_via_jvm(
        bootstrap,
        topic,
        &(0..count)
            .map(|i| format!("{prefix}-{i}"))
            .collect::<Vec<_>>(),
    );
}
