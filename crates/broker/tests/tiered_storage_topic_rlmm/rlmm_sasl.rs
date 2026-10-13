//! The copy-then-fetch round trip over a broker whose only listener is
//! `SASL_PLAINTEXT/PLAIN`.
//!
//! This is the one test that proves the manager's internal metadata client
//! authenticates as the inter-broker PLAIN principal, so it keeps the SASL
//! client-security setup to itself.

use {
    crate::{
        rlmm_cluster::{await_activation, build_client_secured, start_sasl_broker_with_topic_rlmm},
        rlmm_round_trip::copy_then_fetch_round_trip,
        run_broker_test,
    },
    krabka_client_core::security::{ClientSecurity, SaslCredentials},
    krabka_security::ListenerProtocol,
};

/// The full copy→metadata→read round-trip, but the broker's only listener
/// is `SASL_PLAINTEXT/PLAIN`. The RLMM's internal metadata client must
/// authenticate as the inter-broker PLAIN principal to bootstrap the
/// topic, publish/consume `CopySegment` events, and serve the read-back. This
/// proves the secured metadata client works end-to-end. The test's own
/// client authenticates with the same credentials.
#[test]
fn topic_rlmm_sasl_loopback_copy_then_fetch_round_trip() {
    run_broker_test(topic_rlmm_sasl_loopback_copy_then_fetch_round_trip_case());
}

async fn topic_rlmm_sasl_loopback_copy_then_fetch_round_trip_case() {
    const TOPIC: &str = "tiered-topic-rlmm-sasl-itest";
    let (broker, _log_dir, remote_dir) = start_sasl_broker_with_topic_rlmm().await;
    await_activation(&broker).await;

    let security = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "rlmm".into(),
            password: "rlmm-secret".into(),
        }),
        sasl_host: None,
    };
    let client = build_client_secured(&broker, Some(security)).await;
    copy_then_fetch_round_trip(&broker, &client, remote_dir.path(), TOPIC).await;
    // Close the test client before broker shutdown for the same reason as
    // `topic_rlmm_copy_then_fetch_round_trip`.
    drop(client);
    broker.shutdown().await;
}
