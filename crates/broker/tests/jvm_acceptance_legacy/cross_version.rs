//! The two mixed-vintage paths, in which one side of the round trip is a Kafka
//! 0.10.1 tool and the other is a Kafka 2.6 tool.
//!
//! Together they pin the two conversions separately: a modern consumer proves
//! the up-converted v2 `RecordBatch` on disk is well formed, and a 0.10.1
//! consumer proves the down-converted v0/v1 `MessageSet` is.

use crate::{
    jvm_acceptance::{KAFKA_IMAGE, KAFKA_IMAGE_LEGACY},
    legacy_round_trip::run_round_trip,
};

/// Test 2: legacy producer, modern consumer.
///
/// A Kafka 0.10.1 console-producer sends 3 records. A Kafka 2.6
/// console-consumer (cp-kafka:6.1.1) reads them back with Fetch v11+.
/// The test validates that the up-conversion writes a well-formed v2
/// `RecordBatch` to the log that a modern client can decode, and not
/// only bytes that a Krabka broker accepts on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_legacy_010_produce_modern_consume() {
    run_round_trip(
        "legacy-010-produce-modern-consume",
        KAFKA_IMAGE_LEGACY,
        KAFKA_IMAGE,
    )
    .await;
}

/// Test 3: modern producer, legacy consumer.
///
/// A Kafka 2.6 console-producer (cp-kafka:6.1.1) sends 3 records with
/// Produce v9. A Kafka 0.10.1 console-consumer (cp-kafka:3.1.2) reads
/// them with Fetch v0–3. The test validates that a real Kafka 0.10.x
/// client can parse the bytes `down_convert_for_fetch` emits as a v0/v1
/// `MessageSet`. That is the load-bearing concern for down-conversion
/// correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_modern_produce_legacy_010_consume() {
    run_round_trip(
        "modern-produce-legacy-010-consume",
        KAFKA_IMAGE,
        KAFKA_IMAGE_LEGACY,
    )
    .await;
}
