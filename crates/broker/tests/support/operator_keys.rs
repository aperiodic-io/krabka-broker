//! Operator keys, and the brokers that trust them, for the KFC-9 suites.
//!
//! [`mint_operator_key`] writes one Ed25519 pair under a directory and keeps
//! the private half in process, which mirrors the operator whose `--sign-with`
//! file never leaves their machine. The `start_with_operator_*` helpers boot a
//! broker that trusts such keys and names a break-glass approver set, over a
//! plaintext listener or, where a case needs two distinct principals, over
//! `SASL_PLAINTEXT`.

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_core::Client;

use crate::support::client::connect_owned;

/// One operator key pair on disk, in the forms a test and a broker need.
///
/// The broker is handed only `public`; the private half stays here and signs
/// in process. That mirrors the operator's real position, where `--sign-with`
/// reads a PKCS#8 file that never leaves their machine.
///
/// `crates/guard-cli/tests/guard_cli.rs` carries its own copy of this fixture.
/// Cargo cannot share a `tests/` helper across crates, and the alternative --
/// a `#[cfg(feature)]` seam in the broker library purely for a test key -- puts
/// key-minting code in the shipped artifact to save a duplicated fixture.
pub struct OperatorKey {
    /// The PKCS#8 private key. Signs in process; the broker never sees it.
    pub pkcs8: Vec<u8>,
    /// The raw 32-byte public key, on disk, as `[[operator_keys]]` names it.
    pub public_path: std::path::PathBuf,
    /// The key id the signature carries.
    pub key_id: String,
    /// The principal this key is bound to, in the Kafka form both the freeze
    /// path and the break-glass path use.
    pub principal: String,
}

impl OperatorKey {
    /// The signing key, rebuilt per call because `Ed25519KeyPair` is not `Sync`.
    pub fn pair(&self) -> ring::signature::Ed25519KeyPair {
        ring::signature::Ed25519KeyPair::from_pkcs8(&self.pkcs8).expect("parse pkcs8")
    }

    /// The trust-set entry that names this key.
    pub fn entry(&self) -> krabka_broker::operator_keys::OperatorKeyEntry {
        krabka_broker::operator_keys::OperatorKeyEntry {
            key_id: self.key_id.clone(),
            principal: self.principal.clone(),
            public_key_path: self.public_path.clone(),
        }
    }
}

/// Mint one operator key pair under `dir`, bound to `principal`.
pub fn mint_operator_key(dir: &std::path::Path, key_id: &str, principal: &str) -> OperatorKey {
    use ring::signature::KeyPair as _;

    let rng = ring::rand::SystemRandom::new();
    let der = ring::signature::Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    let pair = ring::signature::Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse pkcs8");

    let public_path = dir.join(format!("{key_id}.pub"));
    std::fs::write(&public_path, pair.public_key().as_ref()).expect("write public key");

    OperatorKey {
        pkcs8: der.as_ref().to_vec(),
        public_path,
        key_id: key_id.to_owned(),
        principal: principal.to_owned(),
    }
}

/// The principal a plaintext listener authenticates, in the Kafka form.
///
/// [`start_with_operator_key`] binds its key to this name and lists it as the
/// one break-glass approver, so a single entry serves both paths. A suite that
/// needs two distinct principals -- a proposer and an approver -- wants
/// [`start_with_operator_keys_sasl`] instead.
pub const ANONYMOUS: &str = "User:ANONYMOUS";

#[derive(Clone, Copy, Default)]
pub struct OperatorKeysSetup<'a> {
    pub keys: &'a [&'a OperatorKey],
    pub approvers: &'a [&'a str],
}

#[derive(Clone, Copy, Default)]
pub struct OperatorSaslSetup<'a> {
    pub trust: OperatorKeysSetup<'a>,
    pub users: &'a [(&'a str, &'a str)],
}

/// Boot one broker on `dir` that trusts `keys` and takes `approvers` as its
/// break-glass approver set.
///
/// The returned [`BrokerConfig`] is what makes a restart possible: hand it back
/// to [`start_reusing_addrs`] after a `shutdown` and the node comes up on the
/// same addresses and the same trust set. Set `bootstrap_mode` to
/// [`BootstrapMode::Rejoin`] before doing so -- unlike [`start_with_dir`], this
/// helper cannot infer it for you on the second boot, and a node that
/// re-bootstraps comes back with an empty freeze registry.
///
/// [`start_reusing_addrs`]: super::start_reusing_addrs
/// [`BootstrapMode::Rejoin`]: krabka_broker::BootstrapMode::Rejoin
/// [`start_with_dir`]: super::start_with_dir
pub async fn start_with_operator_keys(
    dir: &std::path::Path,
    setup: OperatorKeysSetup<'_>,
) -> (BrokerHandle, Client, BrokerConfig) {
    let config = operator_config(dir, setup);

    let broker = Broker::start(config.clone()).await.expect("broker start");
    let client = connect_owned(
        broker.listen_addr().to_string(),
        "krabka-broker-test",
        "client build",
    )
    .await;
    (broker, client, config)
}

/// [`start_with_operator_keys`] with the single-key, single-approver shape the
/// freeze cases want.
pub async fn start_with_operator_key(
    dir: &std::path::Path,
    key: &OperatorKey,
) -> (BrokerHandle, Client, BrokerConfig) {
    start_with_operator_keys(
        dir,
        OperatorKeysSetup {
            keys: &[key],
            approvers: &[ANONYMOUS],
        },
    )
    .await
}

/// Boot one broker on `dir` behind a `SASL_PLAINTEXT` listener, so a suite can
/// speak to it as more than one principal.
///
/// The break-glass workflow needs that: a proposal is consumed by two distinct
/// approving principals, and the proposer may not approve their own. Over a
/// plaintext listener every connection authenticates as the same
/// [`ANONYMOUS`], which can prove a refusal but never a completion.
///
/// `users` are `(name, password)` pairs for the PLAIN credential store. Each
/// authenticates as `User:<name>`, which is the spelling `approvers` and
/// `[[operator_keys]]` must use.
pub async fn start_with_operator_keys_sasl(
    dir: &std::path::Path,
    setup: OperatorSaslSetup<'_>,
) -> (BrokerHandle, String, BrokerConfig) {
    let OperatorSaslSetup { trust, users } = setup;
    let mut config = operator_config(dir, trust);
    config.listeners = vec![crate::support::listeners::loopback_listener(
        "SASL_PLAINTEXT",
        krabka_security::ListenerProtocol::SaslPlaintext,
    )];
    "SASL_PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![krabka_security::SaslMechanism::Plain];
    for (name, pass) in users {
        config
            .plain_credentials
            .insert((*name).to_owned(), (*pass).to_owned());
    }

    let broker = Broker::start(config.clone()).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, config)
}

/// Client-side `SASL_PLAINTEXT`/`PLAIN` security for one `(user, pass)` pair.
///
/// Pair it with [`start_with_operator_keys_sasl`] to get a client that the
/// broker authenticates as `User:<user>`.
pub fn sasl_plain_security(user: &str, pass: &str) -> krabka_client_core::security::ClientSecurity {
    krabka_client_core::security::ClientSecurity {
        protocol: krabka_security::ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(krabka_client_core::security::SaslCredentials::Plain {
            username: user.to_owned(),
            password: pass.to_owned(),
        }),
        sasl_host: None,
    }
}

/// A client authenticated as `user` against `bootstrap`.
pub async fn sasl_client(bootstrap: &str, user: &str, pass: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("krabka-broker-test")
        .security(sasl_plain_security(user, pass))
        .build()
        .await
        .expect("client build")
}

fn operator_config(dir: &std::path::Path, setup: OperatorKeysSetup<'_>) -> BrokerConfig {
    let OperatorKeysSetup { keys, approvers } = setup;
    let entries: Vec<_> = keys.iter().map(|key| key.entry()).collect();
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = krabka_broker::operator_keys::OperatorKeys::load(&entries)
        .expect("load the operator trust set");
    config.break_glass.approvers = approvers
        .iter()
        .map(|principal| (*principal).to_owned())
        .collect();
    config
}
