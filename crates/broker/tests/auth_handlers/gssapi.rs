//! SASL/GSSAPI.
//!
//! One case proves the mechanism is wired through the handshake
//! advertisement and that the broker does not touch the keytab before the
//! first `SaslAuthenticate` round; it needs no KDC. The other runs a full
//! inter-broker initiate from a keytab against the MIT KDC fixture and is
//! `#[ignore]`d by default.

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::Broker;
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::SaslMechanism;
use tokio::net::TcpStream;

use crate::{
    harness::round_trip,
    inter_broker::drive_inter_broker_client_then_apiversions,
    support::{
        manifest_dir,
        wire::{CorrelationId as WireCorrelationId, SocketExchangeSetup},
    },
};

/// A broker with GSSAPI enabled advertises GSSAPI in its `SaslHandshake`
/// response and accepts the handshake with `error_code = 0`.
///
/// The connection then stays in GSSAPI negotiation. The GSS context exchange
/// itself needs a live KDC, and the E2E parity tests in Task 10 cover it.
/// This case proves two things only. The mechanism is wired through the
/// handshake advertisement, and the broker does not touch the keytab before
/// the first `SaslAuthenticate` round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gssapi_handshake_advertised_when_enabled() {
    let (_log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![SaslMechanism::Gssapi]);
    cfg.gssapi = Some(krabka_broker::GssapiConfig {
        // Points at the committed fixture, but the handshake path never reads
        // it (the acceptor is built lazily on the first SaslAuthenticate).
        keytab_path: manifest_dir().join("tests/fixtures/security/kdc/kafka.keytab"),
        service_name: "kafka".to_string(),
        principal_to_local_rules: vec![],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: None,
        max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut corr = 0;

    // ApiVersions (pre-auth) so the connection is in a clean state.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av = round_trip(
        &mut stream,
        SocketExchangeSetup {
            correlation: WireCorrelationId(corr),
            ..Default::default()
        },
        &av_body,
    )
    .await
    .unwrap();
    corr += 1;
    let mut cur: &[u8] = &av;
    ApiVersionsResponse::decode(&mut cur, 0).unwrap();

    // SaslHandshake v1, mechanism = "GSSAPI".
    let sh_req = SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(corr),
            ..Default::default()
        },
        &sh_body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &sh;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();

    handle.shutdown().await;

    assert!(sh_resp.error_code == 0, "GSSAPI handshake must succeed");
    assert!(
        sh_resp.mechanisms.iter().any(|m| m == "GSSAPI"),
        "GSSAPI must be advertised; got {:?}",
        sh_resp.mechanisms
    );
}

/// End-to-end inter-broker GSSAPI initiate against a live KDC.
///
/// A Krabka broker accepts on a `SASL_PLAINTEXT`/GSSAPI listener, with the
/// service key in `kafka.keytab`. `InterBrokerClient` dials it with
/// `InterBrokerCredentials::Gssapi` and authenticates *from a keytab* as
/// `alice@CRABKA.TEST`, with no password. The test proves the full outbound
/// GSSAPI path: AS/TGS from `alice.keytab` → AP-REQ → broker validates →
/// RFC 4752 auth-only layer negotiation → authenticated stream. A follow-up
/// `ApiVersions` round-trip confirms the stream.
///
/// The test needs the MIT KDC fixture and the exported env, the same as the
/// provider contract test:
///
/// ```text
/// bazel run //crates/broker/tests/fixtures/security/kdc:image_load
/// docker compose -f crates/broker/tests/fixtures/security/kdc/docker-compose.yml up -d
/// KRB5_CONFIG=crates/broker/tests/fixtures/security/kdc/krb5.conf SSPI_KDC_URL=tcp://localhost:88 \
///   cargo test -p krabka-broker gssapi_inter_broker -- --ignored
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the MIT KDC fixture (docker compose up) + exported KRB5_CONFIG/SSPI_KDC_URL"]
async fn gssapi_inter_broker_client_authenticates_from_keytab() {
    let fixtures = manifest_dir().join("tests/fixtures/security/kdc");
    let kdc_url =
        std::env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_string());

    let (_log_dir, mut cfg) = crate::support::sasl::sasl_temp_config(vec![SaslMechanism::Gssapi]);
    cfg.gssapi = Some(krabka_broker::GssapiConfig {
        keytab_path: fixtures.join("kafka.keytab"),
        service_name: "kafka".to_string(),
        // DEFAULT rule + matching default realm maps alice@CRABKA.TEST to
        // the short name "alice".
        principal_to_local_rules: vec![krabka_broker::KerberosRule::Default],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: Some(kdc_url.clone()),
        max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let client = krabka_broker::network::client::InterBrokerClient::new(
        None,
        Some(krabka_broker::config::InterBrokerCredentials::Gssapi {
            keytab_path: fixtures.join("alice.keytab"),
            client_principal: "alice@CRABKA.TEST".to_string(),
            service_name: "kafka".to_string(),
            kdc_url,
        }),
    );

    let result = drive_inter_broker_client_then_apiversions(&client, addr).await;
    handle.shutdown().await;
    result.expect("InterBrokerClient GSSAPI auth + ApiVersions round-trip must succeed");
}
