//! Canonical loopback SASL listener fixtures for integration tests.

use {
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{net::SocketAddr, path::PathBuf},
    tempfile::TempDir,
};

#[derive(Clone, Copy)]
pub struct ScramMechanismCode(pub i8);

#[derive(Clone, Copy, Default)]
pub enum ScramAlgorithm {
    Sha256,
    #[default]
    Sha512,
    Unrecognized(ScramMechanismCode),
}

impl ScramAlgorithm {
    fn wire_code(self) -> i8 {
        match self {
            Self::Sha256 => 1,
            Self::Sha512 => 2,
            Self::Unrecognized(code) => code.0,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ScramIterations(pub i32);

impl Default for ScramIterations {
    fn default() -> Self { Self(4096) }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ScramUpsertionSetup {
    #[default("alice".into())]
    pub name: String,
    pub mechanism: ScramAlgorithm,
    pub iterations: ScramIterations,
    #[default(bytes::Bytes::from(vec![0u8; 16]))]
    pub salt: bytes::Bytes,
    #[default(bytes::Bytes::from(vec![0u8; 64]))]
    pub salted_password: bytes::Bytes,
}

pub fn scram_upsertion(
    setup: ScramUpsertionSetup,
) -> krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialUpsertion {
    krabka_protocol::owned::alter_user_scram_credentials_request::ScramCredentialUpsertion {
        name: setup.name,
        mechanism: setup.mechanism.wire_code(),
        iterations: setup.iterations.0,
        salt: setup.salt,
        salted_password: setup.salted_password,
        ..Default::default()
    }
}

/// A single ephemeral SASL/PLAINTEXT listener, also used for inter-broker traffic.
/// Mechanisms, credentials and authorization remain explicit at the call site.
pub fn sasl_plaintext_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.listeners = vec![crate::support::listeners::loopback_listener(
        "SASL_PLAINTEXT",
        ListenerProtocol::SaslPlaintext,
    )];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg
}

/// An ephemeral SASL/PLAINTEXT listener with the case's exact enabled mechanisms.
pub fn sasl_plaintext_mechanisms(log_dir: PathBuf, mechanisms: Vec<SaslMechanism>) -> BrokerConfig {
    let mut config = sasl_plaintext_config(log_dir);
    config.enabled_sasl_mechanisms = mechanisms;
    config
}

/// Retain an ephemeral log directory beside its mechanism-specific configuration.
///
/// # Panics
/// Panics if the temporary directory cannot be created.
pub fn sasl_temp_config(mechanisms: Vec<SaslMechanism>) -> (TempDir, BrokerConfig) {
    let dir = tempfile::tempdir().unwrap();
    let config = sasl_plaintext_mechanisms(dir.path().to_path_buf(), mechanisms);
    (dir, config)
}

/// The KIP-48 fixture's secret, absolute lifetime ceiling and initial renew window.
pub fn delegation_token_defaults(config: &mut BrokerConfig, key: &[u8]) {
    config.delegation_token_secret_key = Some(krabka_security::SecretBytes::new(key.to_vec()));
    config.delegation_token_max_lifetime = krabka_units::days(7);
    config.delegation_token_default_renew_period = krabka_units::hours(24);
}

/// Enable PLAIN and seed a fixture's users and sole super-user.
pub fn sasl_plaintext_with_users(
    log_dir: PathBuf,
    super_user: &str,
    users: &[(&str, &str)],
) -> BrokerConfig {
    let mut cfg = sasl_plaintext_mechanisms(log_dir, vec![SaslMechanism::Plain]);
    cfg.plain_credentials = users
        .iter()
        .map(|(name, pass)| ((*name).to_string(), (*pass).to_string()))
        .collect();
    cfg.super_users = std::iter::once(super_user.to_string()).collect();
    cfg
}

/// Start a configured fixture, retaining its log directory beside the handle.
pub async fn start_broker(
    cfg: BrokerConfig,
    log_dir: TempDir,
) -> (BrokerHandle, TempDir, SocketAddr) {
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// PLAIN fixture with the default authorizer.
pub fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    start_configured_users(super_user, users, |_| {})
}

/// PLAIN fixture with ACL authorization and an anonymous controller heartbeat.
pub fn start_sasl_plaintext_with_acl_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    start_configured_users(super_user, users, |config| {
        config.authorizer =
            std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
                config
                    .super_users
                    .iter()
                    .cloned()
                    .chain(std::iter::once("ANONYMOUS".to_string()))
                    .collect(),
            ));
    })
}

/// Hash and persist a SCRAM fixture through the controller, outside the wire admin API.
#[derive(Clone, Copy)]
pub struct HashIterations(pub u32);

impl Default for HashIterations {
    fn default() -> Self { Self(4096) }
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ScramCredentialSetup {
    #[default("alice".into())]
    pub user: String,
    #[default(alice_password().into_bytes())]
    pub password: Vec<u8>,
    #[default(SaslMechanism::ScramSha512)]
    pub mechanism: SaslMechanism,
    pub iterations: HashIterations,
}

/// Seed the metadata image with the configured SCRAM credential.
///
/// # Panics
/// Panics if hashing rejects the mechanism or metadata submission fails.
pub async fn provision_scram(broker: &BrokerHandle, setup: ScramCredentialSetup) {
    let credential =
        krabka_security::hash_scram_password(&setup.password, setup.mechanism, setup.iterations.0);
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: setup.user,
                mechanism: setup.mechanism,
                salt: credential.salt,
                stored_key: credential.stored_key,
                server_key: credential.server_key,
                iterations: credential.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");
}

/// Start a bare PLAINTEXT broker, with optional caller-owned configuration.
///
/// # Panics
/// Panics if the temporary directory or broker cannot be created.
pub fn start_plaintext_configured(
    customize: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    customize(&mut cfg);
    start_broker(cfg, log_dir)
}

/// Start a single-broker PLAINTEXT cluster without authentication.
pub async fn start_single_broker_plaintext() -> (BrokerHandle, TempDir, SocketAddr) {
    start_plaintext_configured(|_| {}).await
}

/// Retain the directory and apply authorization before creating the broker-start future.
fn start_configured_users(
    super_user: &str,
    users: &[(&str, &str)],
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let dir = tempfile::tempdir().unwrap();
    let mut config = sasl_plaintext_with_users(dir.path().to_path_buf(), super_user, users);
    configure(&mut config);
    start_broker(config, dir)
}

/// Encode PLAIN's authentication request at version 2 without choosing handshake order.
///
/// # Panics
/// Panics if the protocol encoder rejects the request.
pub fn plain_authentication_body(user: &str, password: &[u8]) -> bytes::BytesMut {
    use krabka_protocol::Encode as _;
    let request = krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
        auth_bytes: plain_payload(user, password),
        ..Default::default()
    };
    let mut body = bytes::BytesMut::new();
    request.encode(&mut body, 2).unwrap();
    body
}

pub fn plain_payload(user: &str, password: &[u8]) -> bytes::Bytes {
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    bytes::Bytes::from(payload)
}

/// The authentication fixtures' runtime-constructed Alice password.
pub fn alice_password() -> String {
    ['w', 'o', 'n', 'd', 'e', 'r', 'l', 'a', 'n', 'd']
        .iter()
        .collect()
}
