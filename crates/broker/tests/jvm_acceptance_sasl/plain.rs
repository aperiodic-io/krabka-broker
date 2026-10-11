//! The SASL/PLAIN produce-and-consume round-trip over a `SASL_PLAINTEXT`
//! listener.
//!
//! PLAIN keeps its own file because it is the only mechanism here that needs
//! no credential-provisioning step: the broker starts with the user already
//! configured, so the case is a single authenticated round-trip through the
//! JVM tools.

use crate::jvm_acceptance::{
    KAFKA_IMAGE, console_round_trip_with_props, nc_check_connectivity, plain_jaas,
    start_sasl_plaintext_broker,
};

/// End-to-end `SASL_PLAINTEXT` + PLAIN drive of the JVM `kafka-topics`,
/// `kafka-console-producer`, and `kafka-console-consumer` tools against a
/// Rust broker with a `SASL_PLAINTEXT` listener and a single provisioned
/// PLAIN user. The test verifies the produce/consume round-trip end-to-end
/// through the official Kafka client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_plain_produce_consume() {
    const TOPIC: &str = "krabka-sasl-plain-itest";
    const USER: &str = "alice";
    const PASS: &str = "wonderland";

    let (broker, _dir) = start_sasl_plaintext_broker(&[(USER, PASS)]).await;
    nc_check_connectivity();

    // 1. Write client.properties for the JVM tools.
    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(USER, PASS),
    );
    let _props_file = console_round_trip_with_props(KAFKA_IMAGE, TOPIC, &props);

    broker.shutdown().await;
}
