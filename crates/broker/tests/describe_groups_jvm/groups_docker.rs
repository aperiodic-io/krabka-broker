//! Lifecycle of the single-node JVM Kafka container the capture runs against:
//! pulling the image, booting it with the dual-listener layout, running
//! commands inside it, and removing it again.

use {
    crate::{CONTAINER, HOST_PORT},
    assert2::assert,
    std::process::Command,
};

pub(crate) fn docker_pull(image: &str) {
    eprintln!("CAPTURE docker pull {image} (large; may take minutes)...");
    let out = Command::new("docker")
        .args(["pull", image])
        .output()
        .expect("spawn docker pull");
    assert!(
        out.status.success(),
        "docker pull {image} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn docker_rm_f(name: &str) { let _ = Command::new("docker").args(["rm", "-f", name]).output(); }

/// Boot single-node Kafka in `KRaft` mode with the dual listener layout.
///
/// Docker publishes the `EXTERNAL` listener to the fixed host port, so the host
/// `Client` can dial it directly.
pub(crate) fn docker_run_kafka(image: &str, enable_consumer_protocol: bool) {
    docker_rm_f(CONTAINER);
    let mut command = Command::new("docker");
    command.args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "-p",
            &format!("{HOST_PORT}:{HOST_PORT}"),
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            &format!(
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:{HOST_PORT},CONTROLLER://0.0.0.0:9093"
            ),
            "-e",
            &format!(
                "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://localhost:{HOST_PORT}"
            ),
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e",
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
        ]);
    if enable_consumer_protocol {
        command.args([
            "-e",
            "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer",
        ]);
    }
    let out = command.arg(image).output().expect("spawn docker run kafka");
    assert!(
        out.status.success(),
        "docker run kafka failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    eprintln!(
        "CAPTURE kafka container started id={}",
        String::from_utf8_lossy(&out.stdout).trim()
    );
}

/// Run a command inside the broker container and return its `Output`.
pub(crate) fn exec(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .arg("exec")
        .arg(CONTAINER)
        .args(args)
        .output()
        .expect("spawn docker exec")
}

/// Detach a long-running command inside the container with `docker exec -d`.
pub(crate) fn exec_detached(script: &str) {
    let out = Command::new("docker")
        .args(["exec", "-d", CONTAINER, "bash", "-c", script])
        .output()
        .expect("spawn docker exec -d");
    assert!(
        out.status.success(),
        "docker exec -d failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
}

pub(crate) fn docker_logs() -> String {
    let out = Command::new("docker")
        .args(["logs", CONTAINER])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    )
}

pub(crate) struct ContainerGuard;
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(CONTAINER);
        eprintln!("CAPTURE removed container {CONTAINER}");
    }
}
