//! The compressed legacy batches, where v0 and v1 wrap the whole batch in one
//! outer compressed message rather than compressing each record.
//!
//! Both tests drive `legacy_to_v2`, which has to decompress the outer wrapper
//! and re-emit a v2 `RecordBatch` carrying the same compression marker; gzip
//! and snappy reach different codec paths inside it.

use assert2::assert;

use crate::jvm_acceptance::{KAFKA_IMAGE_LEGACY, broker0_advertised, docker_run_kafka_tool};

/// Test 4: gzip-compressed legacy round-trip.
///
/// A Kafka 0.10.1 console-producer with `compression.type=gzip`
/// sends ~50 records as a single outer-wrapped gzip `MessageSet`. That
/// is how v0/v1 represents compressed batches. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back. The test validates
/// the gzip path through `legacy_to_v2`, which decompresses the legacy
/// batch and re-emits it as a v2 `RecordBatch` with the same compression
/// marker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_compressed_round_trip() {
    compressed_round_trip("legacy-010-compressed-round-trip", "gzip").await;
}

/// Snappy legacy-compression follow-up: snappy-compressed legacy round-trip.
///
/// A Kafka 0.10.1 console-producer with `compression.type=snappy` sends
/// ~50 records as a single outer-wrapped snappy `MessageSet`. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back. The test validates
/// the snappy path through `legacy_to_v2`, which converts xerial-framed
/// snappy to a v2 `RecordBatch`.
///
/// NOTE: 0.10.x-era snappy-java framing is fragile against newer JVMs. For
/// that reason the legacy-compression work stream deferred this test and exercised only gzip live.
/// This test stays here as the documented follow-up. If it proves flaky in
/// CI, pin a specific snappy-java version rather than delete it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_snappy_round_trip() {
    compressed_round_trip("legacy-010-snappy-round-trip", "snappy").await;
}

async fn compressed_round_trip(topic: &str, compression: &str) {
    let (broker, _dir) = crate::jvm_acceptance::start_legacy_console_broker(topic).await;

    // 50 newline-separated records to give gzip something to compress.
    let mut input = String::with_capacity(50 * 12);
    {
        use std::fmt::Write as _;
        for i in 0..50 {
            writeln!(input, "record-{i:03}").unwrap();
        }
    }

    // Produce via legacy with gzip.
    let mut child_command = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE_LEGACY,
        args: &[
            "kafka-console-producer",
            "--broker-list",
            broker0_advertised(),
            "--topic",
            topic,
            "--producer-property",
            &format!("compression.type={compression}"),
            "--producer-property",
            "batch.size=131072", // 128 KiB — enough to batch all 50 records together
            "--producer-property",
            "linger.ms=100", // give the producer time to batch
        ],
        input: crate::support::ContainerInput::Attached,
        ..Default::default()
    });
    let producer_out = crate::support::jvm_stdin_output(&mut child_command, input.as_bytes());
    assert!(
        producer_out.status.success(),
        "legacy {compression} producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Consume all 50 via modern.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        broker0_advertised(),
        "--topic",
        topic,
        "--partition",
        "0",
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "15000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for i in 0..50 {
        let needle = format!("record-{i:03}");
        assert!(
            s.contains(&needle),
            "modern consumer didn't emit {needle} after legacy {compression} produce"
        );
    }

    broker.shutdown().await;
}
