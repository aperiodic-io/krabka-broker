//! `CreateDelegationToken` over a connection that never authenticated.
//!
//! Kafka's `KafkaApis.allowTokenRequests` refuses the token RPCs when the
//! security protocol is PLAINTEXT, or SSL without a client certificate (the
//! principal is then `KafkaPrincipal.ANONYMOUS`). Membership in
//! `super.users` does not lift that refusal, which is why
//! `super_users = ["ANONYMOUS"]` is never a way to let an unauthenticated
//! reconcile loop mint tokens — it is only a cluster-wide authorization
//! hole. `BrokerConfig::validate` rejects that value outright, and this file
//! pins the wire behavior that justifies the rejection.

use {
    crate::{
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        rpc::send_create_delegation_token,
        support::{
            sasl::delegation_token_defaults,
            tls::{
                CertificateDer, ClientAuthMode, ClientConfig, PemObject as _, PinnedCertVerifier,
                ServerName, TcpStream, TlsConfig, TlsConnector,
            },
        },
    },
    assert2::check,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, SslPrincipalMapper, config::ListenerSpec},
    krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest,
    krabka_security::ListenerProtocol,
    std::{net::SocketAddr, sync::Arc},
    tempfile::TempDir,
};

const DEV_CERT: &str = include_str!("../fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("../fixtures/security/dev_key.pem");

/// Boots a single-broker cluster whose only listener speaks `protocol`, with
/// the delegation-token master key set — so the handler reaches the admission
/// check instead of answering `DELEGATION_TOKEN_AUTH_DISABLED` — and with
/// `super_users` seeded from the caller.
///
/// The TLS listener sets `client_auth: Disabled`, so a client that presents
/// no certificate completes the handshake and reaches the broker as
/// `ANONYMOUS`: the one-way-SSL case Kafka refuses.
async fn start_broker(
    protocol: ListenerProtocol,
    super_users: &[&str],
) -> (BrokerHandle, TempDir, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let cert_path = pem_dir.path().join("cert.pem");
    let key_path = pem_dir.path().join("key.pem");
    std::fs::write(&cert_path, DEV_CERT).unwrap();
    std::fs::write(&key_path, DEV_KEY).unwrap();

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "PUBLIC".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "PUBLIC".to_string();
    if protocol == ListenerProtocol::Ssl {
        cfg.tls_config = Some(TlsConfig {
            cert_chain_path: cert_path,
            private_key_path: key_path,
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        });
    }
    for user in super_users {
        cfg.super_users.insert((*user).to_string());
    }
    delegation_token_defaults(&mut cfg, b"anon-master-key");

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, pem_dir, addr)
}

/// Opens a one-way TLS session — no client certificate — to `addr`.
async fn tls_connect_without_client_cert(
    addr: SocketAddr,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let pinned = CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("dev cert holds one PEM block")
        .expect("dev cert parses");
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
            pinned,
            schemes: tokio_rustls::rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes(),
            mismatch: "presented cert does not match pinned dev cert",
        }))
        .with_no_client_auth();
    let tcp = TcpStream::connect(addr).await.expect("TCP connect");
    TlsConnector::from(Arc::new(client_cfg))
        .connect(ServerName::try_from("krabka-dev").unwrap(), tcp)
        .await
        .expect("TLS handshake must succeed without a client certificate")
}

/// The token-request refusal holds on both unauthenticated listener shapes,
/// with and without a (non-ANONYMOUS) super-user configured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unauthenticated_create_delegation_token_is_refused() {
    // The broker installs the rustls crypto provider in `Broker::start`, but
    // the client side below needs one too, and it builds its config first.
    // `.ok()` swallows `AlreadySet`.
    let _ = rustls::crypto::ring::default_provider().install_default();

    for protocol in [ListenerProtocol::Plaintext, ListenerProtocol::Ssl] {
        for super_users in [&[][..], &["operator"][..]] {
            let (handle, _log_dir, _pem_dir, addr) = start_broker(protocol, super_users).await;
            let req = CreateDelegationTokenRequest {
                max_lifetime_ms: -1,
                ..Default::default()
            };
            let error_code = if protocol == ListenerProtocol::Ssl {
                let mut stream = tls_connect_without_client_cert(addr).await;
                send_create_delegation_token(&mut stream, 1, &req)
                    .await
                    .expect("CreateDelegationToken round trip")
                    .error_code
            } else {
                let mut stream = TcpStream::connect(addr).await.expect("TCP connect");
                send_create_delegation_token(&mut stream, 1, &req)
                    .await
                    .expect("CreateDelegationToken round trip")
                    .error_code
            };
            check!(
                error_code == DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
                "{protocol:?} listener with super_users={super_users:?} must refuse \
                 CreateDelegationToken from an unauthenticated principal"
            );
            handle.shutdown().await;
        }
    }
}
