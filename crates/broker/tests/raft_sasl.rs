//! Inbound raft listener auth tests.
//!
//! These tests exercise the controller listener under `SaslPlaintext`.
//! They prove that the inbound and the outbound path work together. On
//! the inbound path, broker A accepts auth'd raft frames from broker B.
//! On the outbound path, `InterBrokerDialer` dials with SASL credentials.

mod support;

use std::{net::SocketAddr, time::Duration};

use assert2::assert;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, config::InterBrokerCredentials,
};
use krabka_raft::NodeId;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

fn oauth_token() -> String {
    format!(
        "{}.{}.",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(br#"{"sub":"broker","exp":4000000000}"#)
    )
}

use crate::support::init_tracing;

#[derive(Clone, Copy, Default)]
struct BrokerSlot(usize);

#[derive(Clone, Copy, PartialEq, Eq)]
enum ControllerAuthorization {
    Allowed,
    Denied,
}

/// Build a `SASL_PLAINTEXT` data-plane listener config for broker `i`
/// (0-indexed) and parameterized `controller_listener_protocol`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct SaslBrokerSetup<'a> {
    slot: BrokerSlot,
    #[default(data_listen_addr())]
    data_addr: SocketAddr,
    #[default((ListenerProtocol::SaslPlaintext, data_listen_addr()))]
    controller: (ListenerProtocol, SocketAddr),
    voters: &'a [(NodeId, SocketAddr)],
    #[default(BootstrapMode::Bootstrap)]
    mode: BootstrapMode,
    #[default(("raft-user", "raft-password"))]
    credentials: (&'a str, &'a str),
}

fn sasl_broker_config(log_dir: &std::path::Path, setup: SaslBrokerSetup<'_>) -> BrokerConfig {
    let SaslBrokerSetup {
        slot,
        data_addr,
        controller,
        voters,
        mode,
        credentials,
    } = setup;
    let (ctrl, ctrl_addr) = controller;
    let (plain_user, plain_pass) = credentials;
    let mut cfg = crate::support::node_config(crate::support::NodeIndex(slot.0), log_dir);
    cfg.listen_addr = data_addr;
    cfg.advertised_listener = data_addr.to_string();
    cfg.controller_listen_addr = ctrl_addr;
    cfg.controller_quorum_voters = voters
        .iter()
        .map(|(id, address)| (*id, address.to_string()))
        .collect();
    cfg.bootstrap_mode = mode;
    cfg.listeners = vec![crate::support::listeners::listener(
        "SASL_PLAINTEXT",
        data_addr,
        ListenerProtocol::SaslPlaintext,
    )];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.controller_listener_protocol = ctrl;
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert(plain_user.to_string(), plain_pass.to_string());
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: plain_user.to_string(),
        password: plain_pass.to_string(),
    });
    cfg
}

/// Bind two ephemeral loopback controller listeners and return them
/// alongside their addresses. The caller hands the live listeners to
/// `Broker::start_with_controller_listener`, which adopts them directly
/// instead of re-binding the address.
///
/// This defeats the bind-and-drop TOCTOU race. The classic pattern reads
/// an ephemeral port, then *drops* the probe socket before the broker
/// re-binds it. That leaves a window in which another process on the
/// runner can claim the port, and `Broker::start` then fails with
/// `AddrInUse`. This helper keeps the socket bound and hands it over,
/// which removes that window.
async fn reserve_ctrl_listeners() -> ([SocketAddr; 2], [tokio::net::TcpListener; 2]) {
    let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a0 = l0.local_addr().unwrap();
    let a1 = l1.local_addr().unwrap();
    ([a0, a1], [l0, l1])
}

/// Data-plane bind address for these tests: `127.0.0.1:0` lets the OS
/// assign an ephemeral port at `Broker::start`, so there's no probe/drop
/// gap to race on. Convergence here uses the controller listener. No
/// test dials the data plane, so nothing reads the bound port back.
fn data_listen_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Boot two brokers with the supplied data-plane and controller listener
/// configurations. This helper uses the deterministic bootstrap-then-join
/// pattern. The two "converging" tests call it. The mismatched-creds test
/// below inlines its own setup because it must spawn the joiner
/// asynchronously. That test never gets a leader.
async fn start_two_brokers_with_controller_protocol(
    ctrl: ListenerProtocol,
    plain_user: &str,
    plain_pass: &str,
) -> (BrokerHandle, BrokerHandle, TempDir, TempDir) {
    init_tracing();
    let (ctrl_addrs, [ctrl_l0, ctrl_l1]) = reserve_ctrl_listeners().await;
    let voters: Vec<(NodeId, SocketAddr)> =
        vec![(NodeId(1), ctrl_addrs[0]), (NodeId(2), ctrl_addrs[1])];

    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();

    let cfg0 = sasl_broker_config(
        dir0.path(),
        SaslBrokerSetup {
            controller: (ctrl, ctrl_addrs[0]),
            voters: &voters,
            credentials: (plain_user, plain_pass),
            ..Default::default()
        },
    );
    let cfg1 = sasl_broker_config(
        dir1.path(),
        SaslBrokerSetup {
            slot: BrokerSlot(1),
            controller: (ctrl, ctrl_addrs[1]),
            voters: &voters,
            credentials: (plain_user, plain_pass),
            ..Default::default()
        },
    );

    // KIP-595 static-quorum bootstrap: both brokers boot with the same
    // static voter set and elect among themselves over the (SASL/plaintext)
    // controller wire — no add_learner / change_membership (KIP-853 dynamic voter reconfiguration).
    let cfg1_for_spawn = cfg1.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1_for_spawn, Some(ctrl_l1)).await
    });
    let broker0 = Broker::start_with_controller_listener(cfg0, Some(ctrl_l0))
        .await
        .expect("start broker 0");

    let broker1 = join.await.expect("join spawn").expect("start broker 1");
    (broker0, broker1, dir0, dir1)
}

/// Independent single-voter clusters cannot merge when authentication or authorization fails.
/// Start broker 1 then broker 2, retain both directories, and observe the original 3s window.
async fn assert_disconnected_controllers(
    credentials: [(&str, &str); 2],
    authorization: ControllerAuthorization,
    failure: &str,
) {
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();
    // A shared two-voter quorum would never elect with these failures and would
    // block Broker::start on its leader wait. Each node instead bootstraps itself.
    let config = |index: usize, dir: &TempDir| {
        let mut config = sasl_broker_config(
            dir.path(),
            SaslBrokerSetup {
                slot: BrokerSlot(index),
                controller: (ListenerProtocol::SaslPlaintext, ctrl_addrs[index]),
                voters: &[(NodeId(u64::try_from(index).unwrap() + 1), ctrl_addrs[index])],
                credentials: credentials[index],
                ..Default::default()
            },
        );
        if authorization == ControllerAuthorization::Denied {
            // Valid SASL credentials are still denied CLUSTER_ACTION without
            // super users or ACLs. Construct each authorizer independently.
            config.authorizer =
                std::sync::Arc::new(krabka_broker::authorizer::SimpleAclAuthorizer::new(
                    std::collections::HashSet::new(),
                ));
        }
        config
    };
    let c1 = config(0, &dir1);
    let c2 = config(1, &dir2);
    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");
    let b2 = Broker::start_with_controller_listener(c2, Some(ctrl_l2))
        .await
        .expect("start b2");
    // Intentional negative observation: no awaiter can assert that state stays put.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(b1.broker_count() < 2, "{failure}");
    let _ = &b2;
    b2.shutdown().await;
    b1.shutdown().await;
}

// Exercises follower → leader `submit_change` forwarding under SASL.
//
// With `controller_listener_protocol = SaslPlaintext`, broker 1 elects itself,
// b1.add_learner + b1.change_membership replicate via the dialer (SASL OK),
// b2's Broker::start returns when it sees the leader — and b2 then calls
// `controller.submit_change(self_reg)` which forwards to the leader via
// `krabka_raft::controller::forward_submit_to`. T9b routes that helper
// through the injected `OutboundDialer` so the SASL handshake runs before
// `API_KEY_SUBMIT_CHANGE` hits the wire, and b1 accepts the registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_two_broker_quorum() {
    let (b1, b2, _d1, _d2) = Box::pin(start_two_brokers_with_controller_protocol(
        ListenerProtocol::SaslPlaintext,
        "broker",
        "secret",
    ))
    .await;
    // Wait until both brokers see two registered peers in the metadata image.
    // Event-driven: each awaiter observes `img.brokers().count() >= 2` (the
    // same signal `broker_count()` reads) and panics if convergence stalls.
    b1.wait_until_brokers_registered(2).await;
    b2.wait_until_brokers_registered(2).await;
    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_oauthbearer_two_broker_quorum() {
    init_tracing();
    let (controller_addrs, [controller_0, controller_1]) = reserve_ctrl_listeners().await;
    let voters = vec![
        (NodeId(1), controller_addrs[0]),
        (NodeId(2), controller_addrs[1]),
    ];
    let dir0 = TempDir::new().unwrap();
    let dir1 = TempDir::new().unwrap();
    let token_dir = TempDir::new().unwrap();
    let token_path = token_dir.path().join("oauth-token");
    std::fs::write(&token_path, oauth_token()).unwrap();

    let mut cfg0 = sasl_broker_config(
        dir0.path(),
        SaslBrokerSetup {
            controller: (ListenerProtocol::SaslPlaintext, controller_addrs[0]),
            voters: &voters,
            credentials: ("unused", "unused"),
            ..Default::default()
        },
    );
    let mut cfg1 = sasl_broker_config(
        dir1.path(),
        SaslBrokerSetup {
            slot: BrokerSlot(1),
            controller: (ListenerProtocol::SaslPlaintext, controller_addrs[1]),
            voters: &voters,
            credentials: ("unused", "unused"),
            ..Default::default()
        },
    );
    for config in [&mut cfg0, &mut cfg1] {
        config.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
        config.plain_credentials.clear();
        config.inter_broker_credentials = Some(InterBrokerCredentials::OAuthBearer {
            token_path: token_path.clone(),
        });
    }

    let join0 = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg0, Some(controller_0)).await
    });
    let join1 = tokio::spawn(async move {
        Broker::start_with_controller_listener(cfg1, Some(controller_1)).await
    });
    let broker0 = join0.await.expect("broker 0 task").expect("start broker 0");
    let broker1 = join1.await.expect("broker 1 task").expect("start broker 1");

    broker0.wait_until_brokers_registered(2).await;
    broker1.wait_until_brokers_registered(2).await;
    broker0.shutdown().await;
    broker1.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_plaintext_rejects_mismatched_creds() {
    // Neither broker has the other's password, so authentication fails both ways.
    Box::pin(assert_disconnected_controllers(
        [("alice", "wonderland"), ("bob", "burgers")],
        ControllerAuthorization::Allowed,
        "mismatched creds must not converge",
    ))
    .await;
}

// H-1: authentication is not authorization. Here both brokers present
// *valid, matching* SASL credentials (so the SASL handshake succeeds), but
// the controller listener is gated by a `SimpleAclAuthorizer` with NO
// super-users and NO ACLs — so the authenticated principal is DENIED
// `CLUSTER_ACTION` on `Cluster("kafka-cluster")`. The listener keeps the
// connection and refuses every raft and metadata RPC on it with
// `CLUSTER_AUTHORIZATION_FAILED` (#684), so the two single-voter clusters can
// never exchange controller RPCs to merge. b1 must still see only itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_sasl_denies_unauthorized_principal() {
    // Both credentials authenticate; the empty authorizers deny controller RPCs.
    Box::pin(assert_disconnected_controllers(
        [("broker", "secret"), ("broker", "secret")],
        ControllerAuthorization::Denied,
        "unauthorized principal must not be able to drive controller RPCs",
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_listener_plaintext_legacy_path_unchanged() {
    // Default `controller_listener_protocol = Plaintext` and the default
    // `AllowAllAuthorizer`: every peer is `ANONYMOUS` and every request is
    // allowed. Two brokers converge over the plaintext path.
    init_tracing();
    let (ctrl_addrs, [ctrl_l1, ctrl_l2]) = reserve_ctrl_listeners().await;
    let voters: Vec<(u64, SocketAddr)> = vec![(1, ctrl_addrs[0]), (2, ctrl_addrs[1])];

    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    // Plain (no SASL) configs: don't use sasl_broker_config because we
    // want zero auth on either listener (legacy path).
    let mut c1 = crate::support::node_config(crate::support::NodeIndex(0), dir1.path());
    c1.listen_addr = data_listen_addr();
    c1.advertised_listener = data_listen_addr().to_string();
    c1.controller_listen_addr = ctrl_addrs[0];
    c1.controller_quorum_voters = crate::support::controller_voters(&voters);
    c1.bootstrap_mode = BootstrapMode::Bootstrap;
    c1.controller_listener_protocol = ListenerProtocol::Plaintext;

    let mut c2 = crate::support::node_config(crate::support::NodeIndex(1), dir2.path());
    c2.listen_addr = data_listen_addr();
    c2.advertised_listener = data_listen_addr().to_string();
    c2.controller_listen_addr = ctrl_addrs[1];
    c2.controller_quorum_voters = crate::support::controller_voters(&voters);
    c2.bootstrap_mode = BootstrapMode::Bootstrap;
    c2.controller_listener_protocol = ListenerProtocol::Plaintext;

    // Static bootstrap: both brokers boot with the same voter set and elect
    // over the plaintext controller wire — no add_learner / change_membership.
    let c2_for_spawn = c2.clone();
    let join = tokio::spawn(async move {
        Broker::start_with_controller_listener(c2_for_spawn, Some(ctrl_l2)).await
    });
    let b1 = Broker::start_with_controller_listener(c1, Some(ctrl_l1))
        .await
        .expect("start b1");

    let b2 = join.await.expect("join spawn").expect("start b2");

    // Event-driven convergence: both brokers observe two registered peers in
    // the metadata image (same signal `broker_count()` reads).
    b1.wait_until_brokers_registered(2).await;
    b2.wait_until_brokers_registered(2).await;
    b1.shutdown().await;
    b2.shutdown().await;
}
