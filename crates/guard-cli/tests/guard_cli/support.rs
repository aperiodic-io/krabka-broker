//! The fixtures every case shares: a broker, an operator key, and the in-process
//! call that stands in for running the binary.
//!
//! It also holds the exit codes the cases assert on, so one file names the
//! contract a runbook branches on rather than each case repeating it.

use {
    krabka_broker::{
        Broker, BrokerConfig, BrokerHandle,
        operator_keys::{OperatorKeyEntry, OperatorKeys},
    },
    ring::signature::{Ed25519KeyPair, KeyPair as _},
    std::path::{Path, PathBuf},
};

/// The exit code for a request the broker refused.
pub(super) const REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
pub(super) const UNREACHABLE: i32 = 2;
/// The exit code for an action that needs an approval which does not exist.
pub(super) const NO_APPROVAL: i32 = 4;
/// The exit code for a signature that did not verify.
pub(super) const BAD_SIGNATURE: i32 = 5;

/// The principal a plaintext listener authenticates, in the Kafka form both the
/// freeze path and the break-glass path use. It binds the operator key and it
/// is the one entry in `break_glass.approvers`.
pub(super) const PRINCIPAL: &str = "User:ANONYMOUS";
/// The operator key id the cases sign under.
pub(super) const KEY_ID: &str = "alice-yubi";

pub(super) const TOPIC: &str = "orders";
pub(super) const PREFIX: &str = "tenant-a.";

/// One operator key, on disk, in the three forms the tool and the broker need.
pub(super) struct Key {
    /// The PKCS#8 private key that `--sign-with` reads. It never leaves this
    /// machine, and the broker never sees it.
    pub(super) pkcs8: PathBuf,
    /// The raw 32-byte public key that `[[operator_keys]]` names.
    public: PathBuf,
    /// The TOML file that `--operator-keys` reads, in the shape `broker.toml`
    /// writes it.
    pub(super) trust_file: PathBuf,
}

/// Mint one operator key pair under `dir`, bound to `principal`.
pub(super) fn mint_key(dir: &Path, key_id: &str, principal: &str) -> Key {
    let rng = ring::rand::SystemRandom::new();
    let der = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    let pair = Ed25519KeyPair::from_pkcs8(der.as_ref()).expect("parse pkcs8");

    let pkcs8 = dir.join(format!("{key_id}.pk8"));
    std::fs::write(&pkcs8, der.as_ref()).expect("write private key");
    let public = dir.join(format!("{key_id}.pub"));
    std::fs::write(&public, pair.public_key().as_ref()).expect("write public key");
    let trust_file = dir.join(format!("{key_id}.toml"));
    std::fs::write(
        &trust_file,
        format!(
            "[[operator_keys]]\nkey_id = \"{key_id}\"\nprincipal = \"{principal}\"\n\
             public_key_path = \"{}\"\n",
            public.display()
        ),
    )
    .expect("write trust file");
    Key {
        pkcs8,
        public,
        trust_file,
    }
}

/// A booted broker, the directory it lives in, and its bootstrap address.
struct RunningBroker {
    handle: BrokerHandle,
    dir: tempfile::TempDir,
    bootstrap: String,
}

/// Boot a single-node broker that trusts `key` and lists this connection's
/// principal as its one break-glass approver.
///
/// The `TempDir` is returned so the log directory outlives the broker.
async fn broker(dir: tempfile::TempDir, key: &Key) -> RunningBroker {
    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.operator_keys = OperatorKeys::load(&[OperatorKeyEntry {
        key_id: KEY_ID.to_owned(),
        principal: PRINCIPAL.to_owned(),
        public_key_path: key.public.clone(),
    }])
    .expect("load the operator trust set");
    config.break_glass.approvers = vec![PRINCIPAL.to_owned()];
    let handle = Broker::start(config).await.expect("broker starts");
    let bootstrap = handle.listen_addr().to_string();
    RunningBroker {
        handle,
        dir,
        bootstrap,
    }
}

/// A broker, a key, and the directory both live in.
pub(super) async fn cluster() -> (BrokerHandle, tempfile::TempDir, String, Key) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let key = mint_key(dir.path(), KEY_ID, PRINCIPAL);
    let RunningBroker {
        handle,
        dir,
        bootstrap,
    } = broker(dir, &key).await;
    (handle, dir, bootstrap, key)
}

/// Run the tool, returning its exit code.
pub(super) async fn cli(bootstrap: &str, args: &[&str]) -> i32 {
    let mut line = vec!["krabka-guard", "--bootstrap-server", bootstrap];
    line.extend_from_slice(args);
    krabka_guard::run_from_args(line).await
}

/// The signing flags a freeze command takes, as a borrowed argument list.
pub(super) fn signed_as<'a>(key: &'a Key, principal: &'a str) -> [&'a str; 6] {
    [
        "--sign-with",
        key.pkcs8.to_str().expect("utf-8 path"),
        "--key-id",
        KEY_ID,
        "--principal",
        principal,
    ]
}

/// Place the signed topic freeze used by the thaw and registry cases.
pub(super) async fn set_signed_freeze(bootstrap: &str, signing: &[&str]) -> i32 {
    let mut set = vec!["freeze", "set", "--topic", TOPIC, "--reason", "DR cutover"];
    set.extend_from_slice(signing);
    cli(bootstrap, &set).await
}

/// Read and verify the registry against one local trust file.
pub(super) async fn verify_registry(bootstrap: &str, trust_file: &str) -> i32 {
    cli(bootstrap, &[
        "freeze",
        "list",
        "--verify-signatures",
        "--operator-keys",
        trust_file,
    ])
    .await
}

/// The id of the one proposal the cluster holds.
///
/// The tool prints the id on stdout, which an in-process case cannot read, so
/// this asks the broker directly. It exercises no tool code, only the setup a
/// later `approve` or `withdraw` case needs.
pub(super) async fn only_proposal(bootstrap: &str) -> uuid::Uuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .client_id("guard-cli-test")
        .build()
        .await
        .expect("client connects");
    let response = client
        .send(krabka_protocol::krabka::break_glass::DescribeBreakGlassRequest::default())
        .await
        .expect("describe break-glass");
    let stored = response.proposals.first().expect("the cluster holds one");
    uuid::Uuid::from_bytes(stored.proposal_id.0)
}

pub(super) async fn frozen_cluster_with_stranger()
-> (BrokerHandle, tempfile::TempDir, String, Key, Key) {
    let (broker, dir, bootstrap, key) = cluster().await;
    let stranger = mint_key(dir.path(), "mallory-yubi", "User:mallory");
    let signing = signed_as(&key, PRINCIPAL);
    assert2::assert!(set_signed_freeze(&bootstrap, &signing).await == 0);
    (broker, dir, bootstrap, key, stranger)
}
