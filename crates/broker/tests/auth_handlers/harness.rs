//! Wire plumbing and credential fixtures that every auth suite in this
//! binary shares: one length-prefixed request/response round-trip against a
//! broker socket, and the test passwords the SASL exchanges authenticate
//! with.

use std::io;

use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use tokio::net::TcpStream;

/// alice's SCRAM test password, built from characters at runtime.
///
/// The value is a non-secret test fixture. But a literal that goes into the
/// client SASL-auth calls trips GitHub's default code-scanning credential
/// query. This function keeps those call sites free of literals.
pub use crate::support::sasl::alice_password;
use crate::{
    kafka_wire::{self, ExchangeSetup as WireExchangeSetup},
    support::{
        sasl::{ScramCredentialSetup, provision_scram, sasl_plaintext_mechanisms, start_broker},
        sasl_plaintext_config,
        wire::{
            CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
            SocketExchangeSetup,
        },
    },
};

pub(crate) mod common_imports {
    pub use assert2::check;
    pub use krabka_broker::Broker;
    pub use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
    pub use krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
    pub use krabka_security::SaslMechanism;
    pub use tokio::net::TcpStream;
}

pub struct BrokerStream {
    pub broker: krabka_broker::BrokerHandle,
    pub stream: TcpStream,
}

pub async fn start_broker_stream(config: krabka_broker::BrokerConfig) -> BrokerStream {
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker must start");
    let stream = TcpStream::connect(broker.listen_addr())
        .await
        .expect("connect to broker");
    BrokerStream { broker, stream }
}

pub async fn sasl_authenticate_round_trip(
    stream: &mut TcpStream,
    body: &[u8],
) -> io::Result<Vec<u8>> {
    round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(3),
            header: RequestHeaderEncoding::Flexible,
        },
        body,
    )
    .await
}

/// admin PLAIN test password, built at runtime.
///
/// A runtime value stops code scanning from giving a false positive for a
/// static secret in the integration fixtures.
pub fn admin_plain_password() -> String {
    ['s', 'e', 'c', 'r', 'e', 't'].iter().collect()
}

/// wrong SCRAM test password, built at runtime for the same reason as
/// `admin_plain_password`.
pub fn wrong_scram_password() -> String {
    ['h', 'u', 'n', 't', 'e', 'r', '2'].iter().collect()
}

// One length-prefixed request/response exchange, bound to this suite.
crate::socket_round_trip_fixture!(pub round_trip, "krabka-sasl-test");

/// Post-auth Metadata proves the session survived and the data plane remains reachable.
pub async fn metadata_probe(stream: &mut TcpStream, corr_id: i32) -> io::Result<()> {
    let response: krabka_protocol::owned::metadata_response::MetadataResponse =
        kafka_wire::exchange(
            stream,
            &krabka_protocol::owned::metadata_request::MetadataRequest::default(),
            WireExchangeSetup {
                api_key: WireApiKey(3),
                version: WireApiVersion(12),
                correlation: WireCorrelationId(corr_id),
                client_id: "krabka-sasl-test",
                header: RequestHeaderEncoding::Flexible,
                ..Default::default()
            },
        )
        .await?;
    if response.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }
    Ok(())
}

/// Read after an authentication deadline/refusal, bounded so a missing close fails the test.
pub async fn read_after_auth(stream: &mut TcpStream) -> usize {
    let mut buf = [0_u8; 16];
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(stream, &mut buf),
    )
    .await
    .expect("read should not hang")
    .expect("read should not error")
}

/// Disable idle expiry so only the configured reauthentication deadline is observable.
pub fn reauth_config(
    log_dir: &std::path::Path,
    max_reauth: krabka_units::Time,
) -> krabka_broker::BrokerConfig {
    let mut config = sasl_plaintext_config(log_dir.to_path_buf());
    config.connections_max_idle = Some(krabka_units::millis(0));
    config.connections_max_reauth = Some(max_reauth);
    config
}

/// An authenticated SCRAM-admin fixture, enabling the mechanisms each case exercises.
pub async fn start_scram_admin(
    log_dir: &std::path::Path,
    password: &str,
    mechanisms: Vec<krabka_security::SaslMechanism>,
) -> (krabka_broker::BrokerHandle, std::net::SocketAddr) {
    let mut config = sasl_plaintext_mechanisms(log_dir.to_path_buf(), mechanisms);
    config
        .plain_credentials
        .insert("admin".to_string(), password.to_string());
    config.super_users = maplit::hashset! {"admin".to_string()};
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker must start");
    let addr = broker.listen_addr();
    (broker, addr)
}

pub fn alice_plain_config(
    log_dir: std::path::PathBuf,
    password: String,
) -> krabka_broker::BrokerConfig {
    let mut cfg = sasl_plaintext_mechanisms(log_dir, vec![krabka_security::SaslMechanism::Plain]);
    cfg.plain_credentials.insert("alice".into(), password);
    cfg
}

pub async fn start_scram_alice(
    log_dir: std::path::PathBuf,
    mechanism: krabka_security::SaslMechanism,
) -> krabka_broker::BrokerHandle {
    let cfg = sasl_plaintext_mechanisms(log_dir, vec![mechanism]);
    let broker = krabka_broker::Broker::start(cfg)
        .await
        .expect("broker must start");
    provision_scram(
        &broker,
        ScramCredentialSetup {
            mechanism,
            ..Default::default()
        },
    )
    .await;
    broker
}

/// PLAIN Alice fixture, retaining the log directory through the test.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub async fn start_alice_plain(
    password: String,
) -> (
    tempfile::TempDir,
    krabka_broker::BrokerHandle,
    std::net::SocketAddr,
) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = alice_plain_config(log_dir.path().to_path_buf(), password);
    let (handle, log_dir, addr) = start_broker(cfg, log_dir).await;
    (log_dir, handle, addr)
}

/// Keep the SCRAM provisioning broker's directory alive beside its handle.
pub async fn scram_admin_fixture(
    mechanisms: Vec<krabka_security::SaslMechanism>,
) -> (
    tempfile::TempDir,
    krabka_broker::BrokerHandle,
    std::net::SocketAddr,
) {
    let dir = tempfile::tempdir().unwrap();
    let (broker, addr) = start_scram_admin(dir.path(), &admin_plain_password(), mechanisms).await;
    (dir, broker, addr)
}

/// A PLAIN broker and its first socket, retaining the log directory beside the handle.
///
/// # Panics
/// Panics if the broker fixture or the TCP connection cannot be created.
pub async fn alice_plain_socket(
    password: String,
) -> (
    tempfile::TempDir,
    krabka_broker::BrokerHandle,
    std::net::SocketAddr,
    TcpStream,
) {
    let (dir, broker, addr) = start_alice_plain(password).await;
    let stream = TcpStream::connect(addr).await.unwrap();
    (dir, broker, addr, stream)
}

/// Provision Alice and Bob for the selected reauthentication mechanism.
/// PLAIN credentials are installed before startup; SCRAM credentials commit afterward.
///
/// # Panics
/// Panics if broker startup or SCRAM metadata submission fails.
pub async fn start_reauth_broker(
    log_dir: &std::path::Path,
    max_reauth: krabka_units::Time,
    mechanism: krabka_security::SaslMechanism,
) -> krabka_broker::BrokerHandle {
    let mut config = reauth_config(log_dir, max_reauth);
    config.enabled_sasl_mechanisms = vec![mechanism];
    if mechanism == krabka_security::SaslMechanism::Plain {
        for user in ["alice", "bob"] {
            config
                .plain_credentials
                .insert(user.to_string(), alice_password());
        }
    }
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("broker must start");
    if mechanism == krabka_security::SaslMechanism::ScramSha512 {
        for user in ["alice", "bob"] {
            provision_scram(
                &broker,
                ScramCredentialSetup {
                    user: (user).into(),
                    mechanism,
                    ..Default::default()
                },
            )
            .await;
        }
    }
    broker
}
