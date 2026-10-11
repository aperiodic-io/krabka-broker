//! The pure-legacy round trip, in which a Kafka 0.10.1 producer and a Kafka
//! 0.10.1 consumer talk to the same modern broker.
//!
//! This is the only case here that exercises up-conversion in the `Produce`
//! handler and down-conversion in the `Fetch` handler within one test, so it
//! stands on its own.

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE_LEGACY, consume_console, produce_console, start_legacy_console_broker,
};

/// Test 1: pure-legacy round-trip.
///
/// A Kafka 0.10.1 console-producer (cp-kafka:3.1.2) sends 3 records
/// with Produce v0–2 and v1 `MessageSet` records. A Kafka 0.10.1
/// console-consumer reads them back with Fetch v0–3. The test exercises
/// both up-conversion in the Produce handler and down-conversion in the
/// Fetch handler, end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_round_trip() {
    run_round_trip(
        "legacy-010-round-trip",
        KAFKA_IMAGE_LEGACY,
        KAFKA_IMAGE_LEGACY,
    )
    .await;
}

pub(crate) async fn run_round_trip(topic: &str, producer_image: &str, consumer_image: &str) {
    let (broker, _dir) = start_legacy_console_broker(topic).await;
    let producer = produce_console(
        producer_image,
        &[],
        topic,
        producer_image == KAFKA_IMAGE_LEGACY,
        b"alpha\nbravo\ncharlie\n",
    );
    assert!(
        producer.status.success(),
        "producer failed: stdout={} stderr={}",
        String::from_utf8_lossy(&producer.stdout),
        String::from_utf8_lossy(&producer.stderr)
    );
    let consumer = consume_console(
        consumer_image,
        &[],
        topic,
        None,
        3,
        10_000,
        consumer_image != KAFKA_IMAGE_LEGACY,
    );
    let stdout = String::from_utf8_lossy(&consumer.stdout);
    let stderr = String::from_utf8_lossy(&consumer.stderr);
    for value in ["alpha", "bravo", "charlie"] {
        assert!(
            stdout.contains(value),
            "consumer didn't emit {value}: status={} stdout={stdout:?} stderr={stderr:?}",
            consumer.status
        );
    }
    broker.shutdown().await;
}
