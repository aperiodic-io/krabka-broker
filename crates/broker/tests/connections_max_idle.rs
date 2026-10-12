//! `connections.max.idle.ms` on the accept path.
//!
//! Kafka reclaims a connection that goes `connections.max.idle.ms` without a
//! complete request frame, on every listener and whatever the connection's
//! auth state. The four scenarios here cover what that buys:
//!
//! 1. A PLAINTEXT connection that completes the TCP handshake and then sends
//!    nothing at all is closed once the window passes, and the close is
//!    counted under `connection_closes{reason="idle"}`. This is the
//!    fd-exhaustion shape: the peer never authenticates, so no ACL or SASL
//!    gate stands between it and an indefinitely held connection.
//! 2. A connection that sends a request every half-window is never closed, so
//!    the deadline really does reset on each frame read rather than capping a
//!    connection's total lifetime.
//! 3. A per-listener override wins over the broker-wide value for the
//!    listener it names.
//! 4. On a TLS listener the window covers the handshake too, so a peer that
//!    opens the socket and never sends a `ClientHello` -- the cheapest version
//!    of scenario 1, because it costs the peer no crypto at all -- is
//!    reclaimed rather than parked forever in `TlsAcceptor::accept`.
//!
//! Scenarios 1 to 3 drive the deadline with `tokio::time::pause()` and
//! `advance()`. Each pauses *after* the broker has started, not with
//! `start_paused = true`, because the broker's own start-up timers -- raft
//! heartbeats, disk scans -- need real wall-clock progress or `Broker::start`
//! hangs. The dispatch loop arms its idle deadline against the real tokio
//! clock as it enters the frame read, so a later `advance` jumps past that
//! `Instant` -- but only once the loop has got that far, which is why each
//! waits on the `active_connections` gauge before it advances. The gauge is
//! incremented in the same poll that arms the deadline, so seeing it move is
//! proof the timer is running.
//!
//! Scenario 4 cannot use that gauge, because the connection it describes never
//! reaches the serve loop that increments it. It runs on a real, short window
//! instead, and the bounded read it blocks on gives that window twenty times
//! its length to fire.

mod support;

use std::{io, net::SocketAddr, path::Path, time::Duration};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use krabka_broker::{
    Broker, BrokerConfig, BrokerHandle,
    metrics::{ConnectionCloseReason, ConnectionCloseReasonLabel},
};
use krabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};
use krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig};
use krabka_units::Time;
use support::listeners::loopback_listener;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// The dev server certificate scenario 4's TLS listener presents. Nothing
/// verifies it -- the peer under test never sends a `ClientHello` -- but the
/// listener will not bind without one.
const DEV_CERT: &str = include_str!("fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("fixtures/security/dev_key.pem");

/// Writes one PEM fixture into `dir` and returns its path.
fn write_pem(dir: &Path, name: &str, body: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).expect("write PEM fixture");
    path
}

/// A single-broker cluster serving one PLAINTEXT listener, plus the log
/// directory that has to outlive it.
struct Fixture {
    handle: BrokerHandle,
    addr: SocketAddr,
    _log_dir: TempDir,
}

impl Fixture {
    /// Boots a broker over `cfg`, whose listener list this function fills in.
    async fn start(mut cfg: BrokerConfig, log_dir: TempDir, listener: &str) -> Self {
        cfg.listeners = vec![loopback_listener(listener, ListenerProtocol::Plaintext)];
        cfg.inter_broker_listener_name = listener.to_string();
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        Self {
            handle,
            addr,
            _log_dir: log_dir,
        }
    }

    /// Opens a connection and returns once the serve loop has armed its idle
    /// deadline for it, which the `active_connections` gauge reports.
    async fn connect(&self) -> TcpStream {
        let before = self.handle.metrics().active_connections.get();
        let stream = TcpStream::connect(self.addr).await.expect("connect");
        self.handle
            .wait_for_metrics("the new connection to reach the serve loop", |metrics| {
                metrics.active_connections.get() > before
            })
            .await;
        stream
    }

    /// How many connections this broker has closed for being idle.
    fn idle_closes(&self) -> u64 {
        self.handle
            .metrics()
            .connection_closes
            .get_or_create(&ConnectionCloseReasonLabel {
                reason: ConnectionCloseReason::Idle,
            })
            .get()
    }
}

/// A test broker whose PLAINTEXT listener is held to `idle`.
fn config_with_idle_window(idle: Time) -> (BrokerConfig, TempDir) {
    let log_dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.connections_max_idle = Some(idle);
    (cfg, log_dir)
}

/// Sends one `ApiVersions` v0 request and reads its response. Returning the
/// decoded response is what proves the connection is still serving requests.
async fn api_versions(stream: &mut TcpStream, corr_id: i32) -> io::Result<ApiVersionsResponse> {
    let mut body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut body, 0)
        .map_err(|error| io::Error::other(format!("ApiVersions encode: {error}")))?;

    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(18);
    frame.put_i16(0);
    frame.put_i32(corr_id);
    frame.put_i16(-1); // null client_id
    frame.put_slice(&body);
    stream
        .write_u32(u32::try_from(frame.len()).expect("frame length fits u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let len = stream.read_u32().await?;
    let mut response = vec![0_u8; len as usize];
    stream.read_exact(&mut response).await?;
    let mut cur: &[u8] = &response;
    let _correlation_id = cur.get_i32();
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|error| io::Error::other(format!("ApiVersions decode: {error}")))
}

/// Jumps the tokio clock forward without letting the wall clock move.
async fn advance(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    tokio::time::resume();
}

/// Reads one byte with a bounded wait and reports whether the peer closed.
async fn is_closed(stream: &mut TcpStream) -> bool {
    let mut buf = [0_u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read must not hang")
        .expect("read must not error");
    read == 0
}

async fn assert_connection_closed(stream: &mut TcpStream) {
    assert!(is_closed(stream).await);
}

/// Scenario 1: a connection that sends nothing is closed once the window
/// passes, and the close lands on the idle reason of the close counter.
#[tokio::test(flavor = "current_thread")]
async fn silent_plaintext_connection_is_closed_past_the_idle_window() {
    let (idle_cfg, log_dir) = config_with_idle_window(krabka_units::secs(60));
    let fixture = Fixture::start(idle_cfg, log_dir, "PLAINTEXT").await;

    let mut stream = fixture.connect().await;
    assert!(fixture.idle_closes() == 0);

    advance(Duration::from_secs(61)).await;

    assert_connection_closed(&mut stream).await;
    assert!(fixture.idle_closes() == 1);

    fixture.handle.shutdown().await;
}

/// Scenario 2: a connection that speaks every half-window is never closed.
/// Four half-windows put twice the idle window behind the connection, so a
/// deadline that did not reset on each frame read would have fired.
#[tokio::test(flavor = "current_thread")]
async fn a_connection_speaking_each_half_window_is_never_closed() {
    let (cfg, log_dir) = config_with_idle_window(krabka_units::secs(60));
    let fixture = Fixture::start(cfg, log_dir, "PLAINTEXT").await;

    let mut stream = fixture.connect().await;
    for corr_id in 0..4 {
        advance(Duration::from_secs(30)).await;
        let response = api_versions(&mut stream, corr_id)
            .await
            .expect("connection must still serve requests");
        assert!(response.error_code == 0, "round trip {corr_id}");
    }

    assert!(fixture.idle_closes() == 0);
    fixture.handle.shutdown().await;
}

/// Scenario 3: a per-listener override governs the listener it names, over
/// the broker-wide value. `EXTERNAL` carries a 20-second window while the
/// broker-wide value stays at Kafka's ten-minute default, so a connection
/// silent for 21 seconds is gone. The fall-back for a listener the override
/// map does not name is covered by the `BrokerConfig` unit tests.
#[tokio::test(flavor = "current_thread")]
async fn a_per_listener_override_expires_its_listener_before_the_broker_wide_window() {
    let log_dir = tempfile::tempdir().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.connections_max_idle_overrides =
        std::iter::once(("EXTERNAL".to_string(), krabka_units::secs(20))).collect();
    let fixture = Fixture::start(cfg, log_dir, "EXTERNAL").await;

    let mut stream = fixture.connect().await;
    advance(Duration::from_secs(21)).await;

    assert_connection_closed(&mut stream).await;
    assert!(fixture.idle_closes() == 1);

    fixture.handle.shutdown().await;
}

/// Scenario 4: a TLS listener holds the handshake to the same window.
///
/// `apache/kafka:4.3.1` run with `connections.max.idle.ms=20000` and an SSL
/// listener closes a socket that sends no `ClientHello` after 20 seconds:
/// Kafka registers the channel with its `Selector` at accept time, so idle
/// expiry covers a connection that has not finished, or even begun,
/// negotiating. Without the same bound the broker would park a task and an fd
/// in `TlsAcceptor::accept` for as long as the peer cared to hold them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tls_listener_closes_a_socket_that_never_starts_its_handshake() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().expect("tempdir");
    let pem_dir = tempfile::tempdir().expect("tempdir");
    let cert_path = write_pem(pem_dir.path(), "cert.pem", DEV_CERT);
    let key_path = write_pem(pem_dir.path(), "key.pem", DEV_KEY);

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.connections_max_idle = Some(krabka_units::millis(250));
    cfg.listeners = vec![loopback_listener("SSL", ListenerProtocol::Ssl)];
    cfg.inter_broker_listener_name = "SSL".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let mut stream = TcpStream::connect(handle.listen_addr())
        .await
        .expect("connect");

    // No `ClientHello`, ever. `is_closed` waits five seconds, twenty windows.
    assert_connection_closed(&mut stream).await;
    assert!(
        handle
            .metrics()
            .connection_closes
            .get_or_create(&ConnectionCloseReasonLabel {
                reason: ConnectionCloseReason::Idle,
            })
            .get()
            == 1
    );

    handle.shutdown().await;
}
