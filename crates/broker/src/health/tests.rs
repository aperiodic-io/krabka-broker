//! The readiness rule and the two routes, driven with a stub metadata
//! authority so no quorum has to exist.

use {
    super::*,
    assert2::assert,
    axum::{body::Body, http::Request},
    tower::ServiceExt as _,
};

struct StubProgress {
    node: i64,
    quorum: i64,
}

impl MetadataProgress for StubProgress {
    fn node_metadata_offset(&self) -> i64 { self.node }

    fn quorum_committed_offset(&self) -> i64 { self.quorum }
}

/// How far a case has taken the broker through startup.
#[derive(Clone, Copy)]
enum Phase {
    Nothing,
    LogDirsRecovered,
    ListenersBound,
    /// Quorum reached, with this node's offset and the quorum's committed
    /// offset.
    QuorumReached(i64, i64),
}

fn state_at(phase: Phase, max_lag: u64) -> HealthState {
    let state = HealthState::new(max_lag);
    if matches!(phase, Phase::Nothing) {
        return state;
    }
    state.mark_log_dir_recovery_complete();
    if matches!(phase, Phase::LogDirsRecovered) {
        return state;
    }
    state.mark_listeners_bound();
    if let Phase::QuorumReached(node, quorum) = phase {
        state.install_metadata_progress(Arc::new(StubProgress { node, quorum }));
    }
    state
}

/// A probe answer: the status and the body.
struct Probe {
    status: StatusCode,
    body: String,
}

async fn get(state: HealthState, path: &str) -> Probe {
    let resp = router(state)
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
        .await
        .unwrap();
    Probe {
        status,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

#[test]
fn readiness_names_the_first_unmet_condition() {
    let cases = [
        (Phase::Nothing, Err(NotReady::LogDirRecovery)),
        (Phase::LogDirsRecovered, Err(NotReady::ListenersBound)),
        (
            Phase::ListenersBound,
            Err(NotReady::MetadataQuorumUnreached),
        ),
        // Exactly at the bound is still ready; one past it is not.
        (Phase::QuorumReached(900, 1000), Ok(())),
        (
            Phase::QuorumReached(899, 1000),
            Err(NotReady::MetadataLag {
                lag: 101,
                max_lag: 100,
            }),
        ),
        // A node that has applied past the last offset it heard the quorum
        // commit is caught up, not 100 records ahead of ready.
        (Phase::QuorumReached(1001, 1000), Ok(())),
        // Both at the pre-first-record sentinel: a freshly bootstrapped
        // cluster is caught up, not unknown.
        (Phase::QuorumReached(-1, -1), Ok(())),
    ];
    for (phase, expected) in cases {
        assert!(state_at(phase, 100).readiness() == expected);
    }
}

#[tokio::test]
async fn healthz_is_200_at_every_startup_phase() {
    for phase in [
        Phase::Nothing,
        Phase::LogDirsRecovered,
        Phase::ListenersBound,
        Phase::QuorumReached(0, 1_000_000),
    ] {
        let Probe { status, body } = get(state_at(phase, 100), "/healthz").await;
        assert!(status == StatusCode::OK);
        assert!(body == "ok\n");
    }
}

#[tokio::test]
async fn readyz_body_names_the_failing_condition() {
    let cases = [
        (Phase::Nothing, "log_dir_recovery"),
        (Phase::LogDirsRecovered, "listeners_bound"),
        (Phase::ListenersBound, "metadata_quorum_unreached"),
        (Phase::QuorumReached(0, 1000), "metadata_lag"),
    ];
    for (phase, condition) in cases {
        let Probe { status, body } = get(state_at(phase, 100), "/readyz").await;
        assert!(status == StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            body.starts_with(&format!("not ready: {condition}: ")),
            "{body}"
        );
    }
}

#[tokio::test]
async fn readyz_is_200_once_every_condition_holds() {
    let Probe { status, body } =
        get(state_at(Phase::QuorumReached(1000, 1000), 100), "/readyz").await;
    assert!(status == StatusCode::OK);
    assert!(body == "ready\n");
}

#[tokio::test]
async fn shutting_down_flips_readyz_to_503() {
    let state = state_at(Phase::QuorumReached(1000, 1000), 100);
    assert!(state.readiness() == Ok(()));

    state.mark_shutting_down();
    assert!(state.readiness() == Err(NotReady::ShuttingDown));

    let Probe { status, body } = get(state, "/readyz").await;
    assert!(status == StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.starts_with("not ready: shutting_down: "));
}

#[test]
fn metadata_progress_is_installed_once() {
    let state = HealthState::new(100);
    state.mark_log_dir_recovery_complete();
    state.mark_listeners_bound();
    state.install_metadata_progress(Arc::new(StubProgress {
        node: 0,
        quorum: 1000,
    }));
    // A second install must not replace the first: the authority this node
    // joined is the one its readiness is measured against, and a later one
    // that reported no lag would mask a node that is genuinely behind.
    state.install_metadata_progress(Arc::new(StubProgress {
        node: 1000,
        quorum: 1000,
    }));
    assert!(
        state.readiness()
            == Err(NotReady::MetadataLag {
                lag: 1000,
                max_lag: 100,
            })
    );
}

/// A scraped answer: the status line and the body.
struct Scraped {
    status_line: String,
    body: String,
}

/// One HTTP/1.1 GET against a served socket.
async fn scrape(addr: std::net::SocketAddr, path: &str) -> Scraped {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
    Scraped {
        status_line: head.lines().next().unwrap_or_default().to_string(),
        body: body.to_string(),
    }
}

/// `serve` answers on the socket it reports, and stops when its token is
/// cancelled.
///
/// The binary calls this before it starts the broker, so the probes have to be
/// live from the socket alone -- an orchestrator polling a port that binds only
/// once the broker is up learns nothing that a TCP probe on 9092 did not
/// already tell it. Port 0 is why the bound address comes back at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_answers_on_the_reported_port_until_it_is_cancelled() {
    let shutdown = CancellationToken::new();
    let state = HealthState::new(100);
    let bound = serve(
        "127.0.0.1:0".parse().unwrap(),
        state.clone(),
        shutdown.clone(),
    )
    .await
    .expect("bind the health listener");
    assert!(bound.port() != 0);

    // Nothing has started, so this is the state a kubelet meets first.
    let Scraped {
        status_line: status,
        body,
    } = scrape(bound, "/healthz").await;
    assert!(status.contains("200 OK"), "{status}");
    assert!(body == "ok\n");
    let Scraped {
        status_line: status,
        body,
    } = scrape(bound, "/readyz").await;
    assert!(status.contains("503 Service Unavailable"), "{status}");
    assert!(body.starts_with("not ready: log_dir_recovery: "), "{body}");

    // The same server reflects the state as the broker marks its phases: it
    // holds the caller's clone, not a copy taken at bind time.
    state.mark_log_dir_recovery_complete();
    state.mark_listeners_bound();
    state.install_metadata_progress(Arc::new(StubProgress {
        node: 1000,
        quorum: 1000,
    }));
    let Scraped {
        status_line: status,
        body,
    } = scrape(bound, "/readyz").await;
    assert!(status.contains("200 OK"), "{status}");
    assert!(body == "ready\n");

    shutdown.cancel();
    // The graceful shutdown closes the listener, so a later connect finds
    // nothing there. Retry briefly: the accept loop stops on its own task.
    let mut refused = false;
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(bound).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(refused, "the health server should stop when cancelled");
}
