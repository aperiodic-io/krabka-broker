//! Reading the acknowledged ledger back, with two clients that share no code.
//!
//! [`assert_rust_readback`] runs while the object store is still blocked, so
//! what it reads can only have come from the WAL quorum on the survivors. It
//! fetches the one known partition directly rather than through a consumer
//! group: a group coordinator is an unrelated subsystem, and the broker that
//! hosted it is the one the schedule just killed.
//!
//! [`assert_jvm_differential`] runs the stock `kafka-console-consumer` in a
//! container and compares its stdout byte for byte with the values that were
//! acknowledged. The Rust fetch and the Rust broker share a decoder and a
//! record model; the JVM client shares neither, so agreement between them says
//! the bytes on the wire are Kafka's and not this project's idea of them.

use {
    crate::{
        TOPIC,
        history::{AckedRecord, ledger_values},
        support::client::connect_owned,
    },
    assert2::assert,
    krabka_client_core::{FetchMinBytes, IsolatedFetch},
    std::{
        process::Stdio,
        time::{Duration, Instant},
    },
};

/// The image the JVM console consumer runs from.
const JVM_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";

/// The deadline for the `docker run` of the JVM console consumer.
///
/// It covers the container start and the consumer's own `--timeout-ms`. It
/// also covers a pull of the roughly 380 MB `JVM_IMAGE`, because a developer
/// who runs this test outside CI has no preload step.
const JVM_CONSUME_TIMEOUT: Duration = Duration::from_mins(5);

/// Fetch the partition directly and require the exact acknowledged ledger.
pub(crate) async fn assert_rust_readback(bootstrap: &str, ledger: &[AckedRecord]) {
    let observed = consume_ledger(bootstrap, ledger.len()).await;
    let expected = ledger_values(ledger);
    assert!(
        observed == expected,
        "Rust consumer lost or reordered acknowledged records: expected={expected:?} observed={observed:?}"
    );
}

async fn consume_ledger(bootstrap: &str, expected: usize) -> Vec<(i64, Vec<u8>)> {
    let client = connect_owned(bootstrap, "diskless-jepsen-consumer", "direct fetch client").await;
    let metadata = client.refresh_metadata().await.expect("fetch metadata");
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some(TOPIC))
        .expect("diskless topic metadata");
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .expect("diskless partition metadata");
    let broker_id = partition.leader_id;
    let topic_id = topic.topic_id;

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut observed = Vec::new();
    let mut offset = 0;
    while observed.len() < expected && Instant::now() < deadline {
        let records = client
            .fetch_partition_with_isolation_on(broker_id, IsolatedFetch {
                topic: TOPIC,
                topic_id,
                partition: 0,
                fetch_offset: offset,
                max_wait: krabka_units::millis(500),
                max: krabka_units::mebibytes(4),
                partition_max: krabka_units::mebibytes(4),
                fetch_min: FetchMinBytes::default(),
                isolation_level: 0,
            })
            .await
            .expect("direct partition fetch");
        for record in records {
            offset = record.offset + 1;
            observed.push((
                record.offset,
                record.value.map_or_else(Vec::new, |value| value.to_vec()),
            ));
        }
    }
    client.close();
    observed
}

/// Run the JVM console consumer against `bootstrap` and require its stdout to
/// be the acknowledged values, in order, one per line.
pub(crate) async fn assert_jvm_differential(bootstrap: &str, ledger: &[AckedRecord]) {
    let observed = jvm_consume_exact(bootstrap, ledger.len()).await;
    let expected = ledger_values(ledger)
        .iter()
        .flat_map(|(_, value)| value.iter().copied().chain(std::iter::once(b'\n')))
        .collect::<Vec<_>>();
    assert!(
        observed == expected,
        "JVM byte differential mismatch: expected={:?} observed={:?}",
        String::from_utf8_lossy(&expected),
        String::from_utf8_lossy(&observed),
    );
}

async fn jvm_consume_exact(bootstrap: &str, expected: usize) -> Vec<u8> {
    let max_messages = expected.to_string();
    let output = docker_output(
        &[
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            JVM_IMAGE,
            "kafka-console-consumer",
            "--bootstrap-server",
            bootstrap,
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--from-beginning",
            "--max-messages",
            &max_messages,
            "--timeout-ms",
            "30000",
        ],
        JVM_CONSUME_TIMEOUT,
        "JVM console consumer",
    )
    .await;
    assert!(
        output.status.success(),
        "JVM consumer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output.stdout
}

/// Run `docker` under a deadline and kill the child when the deadline passes.
///
/// `std::process::Command::output` blocks with no deadline of its own, so a
/// stalled image pull holds the test open until the CI job wall stops it. The
/// tokio child gives the wait an async deadline, and `kill_on_drop` stops the
/// `docker` client when the timeout drops the future.
async fn docker_output(args: &[&str], deadline: Duration, what: &str) -> std::process::Output {
    let child = tokio::process::Command::new("docker")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap_or_else(|err| panic!("spawn {what}: {err}"));
    tokio::time::timeout(deadline, child.wait_with_output())
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {deadline:?}"))
        .unwrap_or_else(|err| panic!("run {what}: {err}"))
}
