//! The SASL/OAUTHBEARER produce-and-consume round-trip over a
//! `SASL_PLAINTEXT` listener.
//!
//! OAUTHBEARER keeps its own file because it is the only mechanism whose
//! principal comes from a bearer token rather than from a stored credential:
//! the JVM client mints an `alg:none` JWS and the broker derives the principal
//! from the RFC 7628 client initial response.

use crate::jvm_acceptance::{
    KAFKA_IMAGE, console_round_trip_with_props, nc_check_connectivity, oauthbearer_jaas,
    start_oauthbearer_broker,
};

/// End-to-end `SASL_PLAINTEXT` + OAUTHBEARER drive of the JVM
/// `kafka-topics` / `kafka-console-producer` / `kafka-console-consumer`
/// tools. The JVM client uses the built-in unsecured login module to mint an
/// `alg:none` JWS for `sub=admin`. Krabka parses the RFC 7628 client initial
/// response, validates the token, and derives `User:admin`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_sasl_oauthbearer_produce_consume() {
    const TOPIC: &str = "krabka-sasl-oauthbearer-itest";
    const USER: &str = "admin";

    let (broker, _dir) = start_oauthbearer_broker().await;
    nc_check_connectivity();

    let props = format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=OAUTHBEARER\n\
         sasl.login.callback.handler.class=\
         org.apache.kafka.common.security.oauthbearer.internals.unsecured.\
         OAuthBearerUnsecuredLoginCallbackHandler\n\
         sasl.jaas.config={}\n",
        oauthbearer_jaas(USER),
    );
    let _props_file = console_round_trip_with_props(KAFKA_IMAGE, TOPIC, &props);

    broker.shutdown().await;
}
