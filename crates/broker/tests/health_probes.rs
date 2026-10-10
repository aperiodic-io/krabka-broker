// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as `tests/mtls.rs` etc).

//! The `/healthz` and `/readyz` probes across a broker's startup.
//!
//! The interesting claim is the transition: `/readyz` must answer 503 while
//! the broker is coming up and 200 once it is serving, and `/healthz` must
//! answer 200 throughout, because a liveness probe that failed mid-recovery
//! would have the kubelet kill the node and restart the recovery.
//!
//! Nothing here waits out a window. The probes are served over the same
//! `HealthState` the broker is handed, so the test holds the broker in the
//! not-ready state simply by not having started it yet, and the 200 is
//! asserted against a handle that has already come back from
//! `Broker::start_with_health`.

mod support;

use std::{net::SocketAddr, time::Duration};

use assert2::{assert, check};
use krabka_broker::{
    Broker, BrokerConfig,
    config::DEFAULT_READINESS_MAX_METADATA_LAG,
    health::{HealthState, router},
};
use krabka_security::ListenerProtocol;
use tokio::net::TcpListener;

/// One HTTP/1.1 GET, returned as `(status line, body)`.
async fn get(addr: SocketAddr, path: &str) -> (String, String) {
    let raw = crate::support::client::http_get(addr, path, true).await;
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    let status = head.lines().next().unwrap_or_default().to_string();
    (status, body.to_string())
}

/// Serves the probe routes over `state` on an ephemeral port, the way the
/// broker binary does before it calls `Broker::start`.
async fn serve_probes(state: HealthState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    bound
}

fn loopback_config(log_dir: &std::path::Path) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![crate::support::listeners::loopback_listener(
        "PLAINTEXT",
        ListenerProtocol::Plaintext,
    )];
    cfg.inter_broker_listener_name = "PLAINTEXT".into();
    cfg
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_is_503_during_startup_and_200_once_the_broker_is_up() {
    let log_dir = tempfile::tempdir().unwrap();
    let state = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
    let probes = serve_probes(state.clone()).await;

    // Before the broker starts, the process is alive and the node is not
    // ready, and the body says which condition is holding it back.
    let (status, body) = get(probes, "/healthz").await;
    check!(
        status.contains("200 OK"),
        "healthz during startup: {status}"
    );
    check!(body == "ok\n");
    let (status, body) = get(probes, "/readyz").await;
    check!(
        status.contains("503 Service Unavailable"),
        "readyz during startup: {status}"
    );
    check!(
        body.starts_with("not ready: log_dir_recovery: "),
        "readyz body during startup: {body}"
    );

    let handle = Broker::start_with_health(loopback_config(log_dir.path()), state)
        .await
        .unwrap();

    // Startup returned, so every condition it is responsible for is marked.
    let (status, body) = get(probes, "/readyz").await;
    check!(status.contains("200 OK"), "readyz once started: {status}");
    check!(body == "ready\n");
    let (status, body) = get(probes, "/healthz").await;
    check!(status.contains("200 OK"), "healthz once started: {status}");
    check!(body == "ok\n");

    handle.shutdown().await;
}

/// The 503 body names the condition rather than reporting a generic failure,
/// and it names the first one that is unmet, so a partly-started node does not
/// report the condition it already satisfied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_names_each_condition_as_startup_advances() {
    let state = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
    let probes = serve_probes(state.clone()).await;

    let (_, body) = get(probes, "/readyz").await;
    check!(body.starts_with("not ready: log_dir_recovery: "), "{body}");

    state.mark_log_dir_recovery_complete();
    let (_, body) = get(probes, "/readyz").await;
    check!(body.starts_with("not ready: listeners_bound: "), "{body}");

    state.mark_listeners_bound();
    let (status, body) = get(probes, "/readyz").await;
    assert!(status.contains("503 Service Unavailable"));
    check!(
        body.starts_with("not ready: metadata_quorum_unreached: "),
        "{body}"
    );
}

async fn running_probe_fixture() -> (
    tempfile::TempDir,
    HealthState,
    std::net::SocketAddr,
    krabka_broker::BrokerHandle,
) {
    let dir = tempfile::tempdir().unwrap();
    let state = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
    let probes = serve_probes(state.clone()).await;
    let handle = Broker::start_with_health(loopback_config(dir.path()), state.clone())
        .await
        .unwrap();
    (dir, state, probes, handle)
}

/// A broker that is up stays ready: the metadata condition is evaluated live
/// on every request, so a probe some way past startup must not start failing
/// on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_stays_200_while_the_broker_runs() {
    let (_log_dir, _state, probes, handle) = running_probe_fixture().await;

    for _ in 0..3 {
        let (status, _) = get(probes, "/readyz").await;
        check!(status.contains("200 OK"), "{status}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_is_503_when_shutting_down() {
    let (_log_dir, state, probes, handle) = running_probe_fixture().await;

    let (status, body) = get(probes, "/readyz").await;
    check!(status.contains("200 OK"), "{status}");
    check!(body == "ready\n");

    state.mark_shutting_down();

    let (status, body) = get(probes, "/readyz").await;
    assert!(status.contains("503 Service Unavailable"));
    check!(body.starts_with("not ready: shutting_down: "), "{body}");

    handle.shutdown().await;
}

/// A draining node reports the drain ahead of every other pending condition.
///
/// A roll script polls `/readyz` to decide when the previous node has left
/// rotation. A broker that answered `log_dir_recovery` while it was in fact
/// handing leadership away would tell that script the node is still coming
/// up, not going down, and the two call for opposite actions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutting_down_outranks_every_other_not_ready_condition() {
    let state = HealthState::new(DEFAULT_READINESS_MAX_METADATA_LAG);
    let probes = serve_probes(state.clone()).await;

    // A freshly built state is not ready for a startup reason, so this is the
    // case where two conditions hold at once.
    let (status, body) = get(probes, "/readyz").await;
    check!(status.contains("503 Service Unavailable"), "{status}");
    check!(!body.starts_with("not ready: shutting_down"), "{body}");

    state.mark_shutting_down();

    let (status, body) = get(probes, "/readyz").await;
    check!(status.contains("503 Service Unavailable"), "{status}");
    check!(body.starts_with("not ready: shutting_down: "), "{body}");
}
