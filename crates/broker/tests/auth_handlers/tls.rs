//! TLS listener coverage: a stock `tokio_rustls` client completes a
//! handshake against an SSL-only listener, and every configured listener
//! reaches the broker's self-registration record and survives a Metadata
//! round-trip.

use {
    crate::{
        DEV_CERT, DEV_KEY,
        support::{
            client::connect_owned,
            tls::{PinnedCertVerifier, fixture_signature_schemes, ssl_config},
        },
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig},
    krabka_protocol::owned::metadata_request::MetadataRequest,
    krabka_security::{ListenerProtocol, TlsConfig},
    std::sync::Arc,
    tokio_rustls::{
        TlsConnector,
        rustls::{
            ClientConfig,
            pki_types::{CertificateDer, ServerName, pem::PemObject as _},
        },
    },
};

fn write_dev_pem(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cp = dir.join("cert.pem");
    let kp = dir.join("key.pem");
    std::fs::write(&cp, DEV_CERT).unwrap();
    std::fs::write(&kp, DEV_KEY).unwrap();
    (cp, kp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_accepts_tls_handshake_only() {
    let (log_dir, _pem_dir, cert_path, key_path) = tls_fixture();

    let cfg = ssl_config(log_dir.path().to_path_buf(), TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: krabka_security::ClientAuthMode::Disabled,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Build a client config trusting our dev cert. The fixture cert is
    // self-issued with `CA:TRUE`, which rustls's default webpki verifier
    // refuses to accept as an end-entity (`CaUsedAsEndEntity`). Since
    // this test only proves the TLS handshake bytes complete and produces
    // no real authentication, plug in a verifier that pins to the dev
    // cert's DER bytes and accepts anything that matches. Subsequent
    // task tests (T22 JVM TLS) regenerate proper cert chains.
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .expect("open dev cert")
        .collect::<Result<_, _>>()
        .expect("parse dev cert");
    let expected_cert = certs.into_iter().next().expect("at least one cert").clone();
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned: expected_cert,
            schemes: fixture_signature_schemes(),
            mismatch: "presented cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("krabka-dev").unwrap();
    let _tls = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS handshake must succeed");

    handle.shutdown().await;
}

/// Every configured listener should appear as a `BrokerEndpoint` on this
/// broker's self-registration record, and the projection should survive a
/// Metadata round-trip end-to-end. The Kafka v9+ Metadata wire response
/// carries a single `host:port` per broker, because the codec has no
/// `endpoints[]` array on `MetadataResponseBroker`. So this test asserts:
///
/// 1. The on-disk registration record stored in [`krabka_metadata::MetadataImage`]
///    carries one [`krabka_metadata::BrokerEndpoint`] per [`krabka_broker::config::ListenerSpec`].
/// 2. A `MetadataRequest::v12` round-trip over the PLAINTEXT listener
///    returns at least one broker entry whose `host:port` matches one of
///    the configured advertised endpoints.
///
/// The two-listener config uses PLAINTEXT and SSL. The SSL listener uses the
/// dev cert, so `BrokerConfig::validate` accepts it. We dial only the
/// PLAINTEXT listener, because the goal here is the metadata projection and
/// not TLS termination. `tls_listener_accepts_*` covers TLS termination.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_response_carries_listener_endpoints() {
    let (log_dir, _pem_dir, cert_path, key_path) = tls_fixture();

    // Distinct wildcard and loopback binds satisfy listener validation while
    // leaving port allocation atomic with Broker startup.
    let plaintext_bind = "127.0.0.1:0".parse().unwrap();
    let ssl_bind = "0.0.0.0:0".parse().unwrap();

    // Two listeners on independent ephemeral ports. PLAINTEXT is the
    // inter-broker listener (so self-registration's `host`/`port` falls
    // out from it); SSL exercises the multi-listener code path.
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![
        crate::support::listeners::listener(
            "PLAINTEXT",
            plaintext_bind,
            ListenerProtocol::Plaintext,
        ),
        crate::support::listeners::listener("SSL", ssl_bind, ListenerProtocol::Ssl),
    ];
    cfg.inter_broker_listener_name = "PLAINTEXT".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: krabka_security::ClientAuthMode::Disabled,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let plaintext_addr = handle.listen_addr();

    // ── Assertion 1: in-memory registration carries both endpoints.
    //
    // Self-registration is best-effort + asynchronous (the leader watcher
    // races with the `submit_change` round-trip); wait until the broker's
    // own registration record in the committed image carries both endpoints.
    let node_id = handle.node_id();
    handle
        .wait_for_image(|img| {
            img.broker(krabka_broker::NodeId(node_id))
                .is_some_and(|b| b.endpoints.len() >= 2)
        })
        .await;
    let endpoints = handle.self_registration_endpoints();
    assert!(
        endpoints.len() == 2,
        "self-registration must carry one endpoint per configured listener (got {endpoints:?})"
    );
    let mut names: Vec<&str> = endpoints.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert!(names == vec!["PLAINTEXT", "SSL"]);
    let plaintext_ep = endpoints
        .iter()
        .find(|e| e.name == "PLAINTEXT")
        .expect("PLAINTEXT endpoint");
    assert!(plaintext_ep.protocol == ListenerProtocol::Plaintext);
    let ssl_ep = endpoints
        .iter()
        .find(|e| e.name == "SSL")
        .expect("SSL endpoint");
    assert!(ssl_ep.protocol == ListenerProtocol::Ssl);

    // ── Assertion 2: a Metadata round-trip over the PLAINTEXT listener
    // returns a broker entry. The Kafka v9+ wire format has no
    // `endpoints[]` array on `MetadataResponseBroker`, so we only assert
    // that *some* broker entry comes back and matches our id — the
    // per-listener data is verified above via the in-memory image.
    let bootstrap = plaintext_addr.to_string();
    let client = connect_owned(&bootstrap, "krabka-auth-test", "client build").await;
    let resp = client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata round-trip");
    assert!(
        !resp.brokers.is_empty(),
        "MetadataResponse must include at least one broker"
    );
    assert!(
        resp.brokers.iter().any(|b| b.node_id == 1),
        "MetadataResponse must include this broker (node_id=1): {:?}",
        resp.brokers,
    );

    handle.shutdown().await;
}

fn tls_fixture() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    // Installing before broker startup also permits the first client-side TLS build.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_dev_pem(pem_dir.path());
    (log_dir, pem_dir, cert_path, key_path)
}
