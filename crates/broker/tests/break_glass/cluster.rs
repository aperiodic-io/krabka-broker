//! Boots the clusters the suite drives, and the clients that speak to them.
//!
//! Two shapes are needed. Every case about the rule itself runs on one broker
//! behind a `SASL_PLAINTEXT` listener, so that four credentials authenticate as
//! four distinct principals. The durability and background-recovery cases need
//! a quorum instead, and a `PLAINTEXT` listener is enough there because those
//! two write their approvals into the metadata log rather than over the wire.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{
    Broker, BrokerConfig, BrokerHandle, NodeId, config::BackgroundUncleanRecovery,
    operator_keys::OperatorKeys,
};
use krabka_client_core::Client;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

use crate::{
    principals::{APPROVERS, USERS, principal},
    support,
    support::client::connect_owned,
};

/// A live broker behind SASL, the operator keys it trusts, and the directory
/// both live in.
///
/// The `TempDir` is held so the log directory and the public key files outlive
/// the broker, exactly as `guard_cli.rs` holds its own.
pub(super) struct Cluster {
    pub(super) broker: BrokerHandle,
    bootstrap: String,
    keys: Vec<support::OperatorKey>,
    _dir: TempDir,
}

impl Cluster {
    /// A client the broker authenticates as `User:<user>`.
    pub(super) async fn client(&self, user: &str) -> Client {
        let password = USERS
            .iter()
            .find(|(name, _)| *name == user)
            .map(|(_, password)| *password)
            .expect("a configured credential");
        support::sasl_client(&self.bootstrap, user, password).await
    }

    /// The operator key bound to `user`.
    pub(super) fn key(&self, user: &str) -> &support::OperatorKey {
        let want = principal(user);
        self.keys
            .iter()
            .find(|key| key.principal == want)
            .expect("a minted operator key")
    }
}

/// Connect the requested principals in order before creating their shared topic.
macro_rules! client_fixture {
    ($cluster:ident = $boot:expr; $($client:ident => $user:expr),+; topic($owner:ident, $topic:expr, $partitions:expr)) => {
        let $cluster = $boot.await;
        $(let $client = $cluster.client($user).await;)+
        $crate::topics::create_topic(&$owner, $topic, $partitions).await;
    };
}
pub(super) use client_fixture;

/// Mint one operator key per approver under `dir`.
fn mint_keys(dir: &std::path::Path) -> Vec<support::OperatorKey> {
    APPROVERS
        .iter()
        .map(|user| support::mint_operator_key(dir, &format!("{user}-yubi"), &principal(user)))
        .collect()
}

/// Boot the suite's single-node broker on the shared helper.
///
/// `break_glass` keeps its defaults: two required approvals, a thirty-minute
/// lifetime, and no signed action. Every case that needs one of those changed
/// says so at its own call site.
pub(super) async fn boot() -> Cluster {
    let dir = TempDir::new().expect("tempdir");
    let keys = mint_keys(dir.path());
    let borrowed: Vec<&support::OperatorKey> = keys.iter().collect();
    let approvers: Vec<String> = APPROVERS.iter().copied().map(principal).collect();
    let approver_refs: Vec<&str> = approvers.iter().map(String::as_str).collect();
    let (broker, bootstrap, _config) = support::start_with_operator_keys_sasl(
        &dir.path().join("data"),
        crate::support::OperatorSaslSetup {
            trust: crate::support::OperatorKeysSetup {
                keys: &borrowed,
                approvers: &approver_refs,
            },
            users: USERS,
        },
    )
    .await;
    Cluster {
        broker,
        bootstrap,
        keys,
        _dir: dir,
    }
}

/// [`boot`], with `break_glass.signed_actions` naming `actions`.
///
/// The broker reads `signed_actions` when it starts, and
/// [`support::start_with_operator_keys_sasl`] takes no hook for it, so the one
/// case that needs a signed action rebuilds the same SASL broker here. Keeping
/// the field at its default in the shared helper is right: a suite that changed
/// it there would make every other case demand signatures it does not test.
pub(super) async fn boot_with_signed_actions(actions: &[&str]) -> Cluster {
    let dir = TempDir::new().expect("tempdir");
    let keys = mint_keys(dir.path());
    let entries: Vec<_> = keys.iter().map(support::OperatorKey::entry).collect();

    let mut config = BrokerConfig::for_tests(dir.path().join("data"));
    config.operator_keys = OperatorKeys::load(&entries).expect("load the operator trust set");
    config.break_glass.approvers = APPROVERS.iter().copied().map(principal).collect();
    config.break_glass.signed_actions = actions.iter().map(|a| (*a).to_owned()).collect();
    config.listeners = vec![crate::support::listeners::loopback_listener(
        "SASL_PLAINTEXT",
        ListenerProtocol::SaslPlaintext,
    )];
    "SASL_PLAINTEXT".clone_into(&mut config.inter_broker_listener_name);
    config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, password) in USERS {
        config
            .plain_credentials
            .insert((*name).to_owned(), (*password).to_owned());
    }

    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    Cluster {
        broker,
        bootstrap,
        keys,
        _dir: dir,
    }
}

// ── multi-node ───────────────────────────────────────────────────────────────

/// A client on a `PLAINTEXT` listener, which authenticates as `User:ANONYMOUS`.
pub(super) async fn plain_client(bootstrap: &str) -> Client {
    connect_owned(bootstrap, "break-glass-test", "client build").await
}

/// Boot an `n`-node cluster whose every node runs the two-person rule.
///
/// [`support::start_n_node_with_retry`] takes no configuration hook, so this
/// wraps [`support::start_n_node_with`] in the same retry: short raft timings
/// split-vote on a slow runner, and a fresh port set usually wins on the second
/// attempt.
pub(super) async fn start_gated_cluster(
    n: u64,
    background: BackgroundUncleanRecovery,
) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    for attempt in 1..=3_u32 {
        let started = support::start_n_node_with(n, |_, config| {
            config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
            config.break_glass.background_unclean_recovery = background;
        })
        .await;
        match started {
            Ok(cluster) => return cluster,
            Err(error) => eprintln!("{n}-node cluster attempt {attempt} failed: {error:?}"),
        }
    }
    panic!("no {n}-node break-glass cluster after 3 attempts")
}

/// Where `node` sits in `cluster`.
pub(super) fn index_of(cluster: &[(BrokerHandle, BrokerConfig, TempDir)], node: NodeId) -> usize {
    cluster
        .iter()
        .position(|(_, config, _)| config.node_id == node)
        .expect("the node is one of the cluster's")
}

/// Await a controller leader that is not `gone`.
pub(super) async fn wait_for_new_leader(handle: &BrokerHandle, gone: NodeId) -> NodeId {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(leader) = handle.controller_leader_id()
            && leader != gone
        {
            return leader;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no controller leader replaced {gone:?} within 30s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
