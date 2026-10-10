// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files,
// matching the convention in auth_handlers.rs / elect_leaders.rs.

//! mTLS client authentication.
//!
//! Drives the full stack: rustls client cert handshake → broker
//! verifies the cert chain against `client_ca_path` → dispatch layer
//! extracts the cert's Subject DN as the connection's `Principal` →
//! authorizer reads that name when checking ACLs.
//!
//! The test exercises the principal-derivation path. It sets the
//! cert DN as a super-user and then sends a request that the
//! authorizer would refuse for any other principal. A successful round
//! trip proves that the broker resolved the connection to the cert DN
//! and not to `ANONYMOUS`.
//!
//! The test is gated to non-Windows. There is no multi-broker dependency, but
//! the dev cert fixture path resolution and tempfile semantics are easier to
//! keep consistent with the existing TLS integration tests.

mod kafka_wire;

mod support;

use std::sync::Arc;

use assert2::assert;
use bytes::BytesMut;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{Decode, Encode, owned::create_topics_response::CreateTopicsResponse};
use krabka_security::{ClientAuthMode, TlsConfig};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
    },
};

use crate::{
    kafka_wire::ExchangeSetup as WireExchangeSetup,
    support::{
        topics::{creatable_topic, create_topic_request},
        wire::HeaderEncoding as RequestHeaderEncoding,
    },
};

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-mtls-test";

use crate::support::tls::{
    CLIENT_PRINCIPAL, DEV_CERT, DEV_CLIENT_CA, DEV_CLIENT_CERT, DEV_CLIENT_KEY, DEV_KEY,
};

fn write_fixture(dir: &std::path::Path, name: &str, contents: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

/// Build a rustls `ClientConfig` that:
/// - skips server-cert verification (the broker presents the self-issued
///   `dev_cert` fixture, which rustls's default verifier rejects as
///   `CaUsedAsEndEntity`),
/// - presents the fixture client cert and private key on the
///   `CertificateRequest` callback.
fn client_config_with_pinned_server_and_client_cert(
    broker_cert: CertificateDer<'static>,
) -> Arc<ClientConfig> {
    let client_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(DEV_CLIENT_CERT.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse client cert PEM");
    let client_key = PrivateKeyDer::from_pem_slice(DEV_CLIENT_KEY.as_bytes())
        .expect("parse client private key PEM");
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(crate::support::tls::PinnedCertVerifier {
            pinned: broker_cert,
            schemes: crate::support::tls::fixture_signature_schemes(),
            mismatch: "presented server cert does not match pinned dev cert",
        }))
        .with_client_auth_cert(client_certs, client_key)
        .expect("rustls accepts client cert + key");
    Arc::new(cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_principal_is_cert_dn_and_super_user_bypass_works() {
    // Provider registration is shared with auth_handlers.rs; tolerate
    // an earlier installer.
    let _ = rustls::crypto::ring::default_provider().install_default();

    // The cert's Subject DN is the principal name. Set it as a
    // super-user so the authorizer permits CreateTopics; with no
    // super-users + no ACLs the compat shim would allow everything
    // regardless of principal, which would mask the principal-derivation
    // path under test.
    let (_log_dir, _pem_dir, handle, addr) = mtls_super_user(ClientAuthMode::Required).await;

    // Build the test TLS client: pin the broker's self-issued cert,
    // present the fixture client cert + key.
    let server_cert_der = server_certificate();
    let client_cfg = client_config_with_pinned_server_and_client_cert(server_cert_der);
    let connector = TlsConnector::from(client_cfg);

    let mut tls = mtls_connect(addr, connector, "mTLS handshake must succeed").await;

    // Send CreateTopics. Authorize gate: Cluster Create on the
    // super-user path. Any non-super-user principal (including
    // ANONYMOUS, which is what a non-mTLS connection would see) would
    // get CLUSTER_AUTHORIZATION_FAILED.
    let body = create_topics_body("mtls-smoke");
    let resp_bytes = kafka_wire::round_trip(
        &mut tls,
        WireExchangeSetup {
            api_key: WireApiKey(19),
            version: WireApiVersion(7),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .unwrap();
    check_create_topics_success(
        &resp_bytes,
        "CreateTopics must succeed for the cert-DN super-user",
    );

    handle.shutdown().await;
}

/// A presented certificate whose Subject DN matches no
/// `ssl.principal.mapping.rules` rule must close the connection.
///
/// Kafka's `SslPrincipalMapper.getName` throws `NoMatchingRule` there and the
/// channel never builds. The two ways this could regress are both privilege
/// promotions: admitting the peer under its raw DN, or falling through to
/// `ANONYMOUS`. Either would turn an operator's exhaustive rule list into a
/// door that anyone holding a CA-signed cert walks through.
///
/// The client is set up exactly as the passing test above -- same fixture
/// cert, same DN in `super_users` -- so the only difference is the rule list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unmappable_certificate_dn_closes_the_connection() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (_log_dir, _pem_dir, mut cfg) = mtls_fixture(ClientAuthMode::Required);
    // Matches only OU=ServiceUsers; the fixture DN has no matching rule.
    cfg.listeners[0].principal_mapper =
        krabka_broker::SslPrincipalMapper::parse(&["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"])
            .expect("the rule list parses");
    // Both regressions would produce a *response* rather than a closed
    // connection: a DN pass-through authorizes as this super-user and
    // succeeds, an ANONYMOUS fall-through comes back
    // CLUSTER_AUTHORIZATION_FAILED. Only the refusal ends the connection with
    // no reply at all, which is what this test reads.
    cfg.super_users = maplit::hashset! {CLIENT_PRINCIPAL.to_string()};

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let server_cert_der = server_certificate();
    let connector = TlsConnector::from(client_config_with_pinned_server_and_client_cert(
        server_cert_der,
    ));

    let mut tls = mtls_connect(addr, connector, "mTLS handshake must succeed").await;

    let body = create_topics_body("unmappable-dn");

    let outcome =
        timed_create_topics(&mut tls, &body, "the broker must close rather than hang").await;

    assert!(
        outcome.is_err(),
        "an unmappable cert DN must not be served -- got a response: {outcome:?}"
    );

    handle.shutdown().await;
}

/// A TLS listener that does not require a client certificate still has to
/// serve the peer that presents none.
///
/// This is the other half of the refusal above: `peer_cert_principal` returns
/// `Ok(None)` here -- the `ANONYMOUS` session -- and `Err(())` there, and the
/// two must not collapse into one another. If "no certificate" started
/// closing the connection, every `ClientAuthMode::Optional` listener would
/// stop serving plain TLS clients; the refusal test above holds the opposite
/// direction, so between them the branch cannot be flattened either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_with_no_certificate_is_served_rather_than_closed() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let (_log_dir, _pem_dir, handle, addr) = mtls_super_user(ClientAuthMode::Optional).await;

    let server_cert_der = server_certificate();
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(crate::support::tls::PinnedCertVerifier {
            pinned: server_cert_der,
            schemes: crate::support::tls::fixture_signature_schemes(),
            mismatch: "presented server cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));

    let mut tls = mtls_connect(
        addr,
        connector,
        "a certificate-less TLS handshake must succeed on an Optional listener",
    )
    .await;

    let body = create_topics_body("anonymous-tls");

    // The broker answers, which is the whole claim: the session was built.
    // The refusal test above sends the same request over the same listener
    // shape and gets no answer at all.
    let resp_bytes = timed_create_topics(&mut tls, &body, "the broker must not hang")
        .await
        .expect("the broker must answer a certificate-less TLS connection");
    check_create_topics_success(&resp_bytes, "the ANONYMOUS session must be served");

    handle.shutdown().await;
}

async fn timed_create_topics<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    body: &[u8],
    timeout_context: &str,
) -> std::io::Result<Vec<u8>> {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        kafka_wire::round_trip(
            stream,
            WireExchangeSetup {
                api_key: WireApiKey(19),
                version: WireApiVersion(7),
                client_id: CLIENT_ID,
                header: RequestHeaderEncoding::Flexible,
                ..Default::default()
            },
            body,
        ),
    )
    .await
    .expect(timeout_context)
}

fn check_create_topics_success(bytes: &[u8], context: &str) {
    let mut cursor = bytes;
    let response =
        CreateTopicsResponse::decode(&mut cursor, 7).expect("decode CreateTopicsResponse");
    assert!(response.topics.len() == 1);
    assert!(
        response.topics[0].error_code == 0,
        "{context} — got {:?}",
        response.topics[0]
    );
}

fn mtls_fixture(
    client_auth: ClientAuthMode,
) -> (tempfile::TempDir, tempfile::TempDir, BrokerConfig) {
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let server_cert_path = write_fixture(pem_dir.path(), "server.pem", DEV_CERT);
    let server_key_path = write_fixture(pem_dir.path(), "server.key", DEV_KEY);
    let client_ca_path = write_fixture(pem_dir.path(), "client_ca.pem", DEV_CLIENT_CA);

    let cfg = crate::support::tls::ssl_config(
        log_dir.path().to_path_buf(),
        TlsConfig {
            cert_chain_path: server_cert_path.clone(),
            private_key_path: server_key_path,
            trust_roots_path: None,
            client_ca_path: Some(client_ca_path),
            client_auth,
        },
    );
    (log_dir, pem_dir, cfg)
}

fn server_certificate() -> CertificateDer<'static> {
    CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("dev server cert present")
        .expect("dev server cert parses")
        .clone()
}

fn create_topics_body(topic: &str) -> BytesMut {
    let request = create_topic_request(creatable_topic(
        crate::support::topics::ConfiguredTopicSetup {
            name: (topic).into(),
            ..Default::default()
        },
    ));
    let mut body = BytesMut::new();
    request.encode(&mut body, 7).expect("encode CreateTopics");
    body
}

async fn mtls_connect(
    addr: std::net::SocketAddr,
    connector: TlsConnector,
    context: &str,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from("krabka-dev").unwrap();
    connector.connect(server_name, tcp).await.expect(context)
}

async fn mtls_super_user(
    client_auth: ClientAuthMode,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    BrokerHandle,
    std::net::SocketAddr,
) {
    let (log_dir, pem_dir, mut config) = mtls_fixture(client_auth);
    config.super_users = maplit::hashset! {CLIENT_PRINCIPAL.to_string()};
    let handle = Broker::start(config).await.expect("broker must start");
    let addr = handle.listen_addr();
    (log_dir, pem_dir, handle, addr)
}
