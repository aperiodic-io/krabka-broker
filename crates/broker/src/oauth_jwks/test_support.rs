//! Fixtures shared by the unit tests of the `oauth_jwks` module tree.
//!
//! The module holds the JWKS test servers -- plaintext, HTTPS with a freshly
//! generated self-signed certificate, and a request-counting variant -- the
//! `JwksRefresher` builders that wire one to an injected timer, and the
//! polling helper that replaces a real-time sleep in a test.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
};

use krabka_security::{Jwks, JwksHandle};
use krabka_units::{Time, hours, millis, secs};
use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::JwksRefresher;

/// Returns a timer that holds every future deadline open indefinitely.
///
/// The manual clock behind it is dropped here and never handed out, so
/// nothing in the process can advance it. Every deadline the refresher arms
/// on the returned timer therefore stays pending for the whole test -- the
/// timer keeps its own handle to the clock's timeline, so the clock value
/// itself does not have to be kept alive to hold the registration open.
///
/// The one deadline this does not hold back is the loop's zero-duration
/// first tick: a deadline that is already due when it is armed completes
/// immediately, on a manual clock exactly as on a real one, so the t=0 fetch
/// still happens. Everything after it is dormant.
pub fn dormant_timer() -> Arc<dyn Timer> {
    ManualMonotonicClock::new_shared().new_timer()
}

/// Yield-polls until `cond` holds. A bounded hang-guard makes a real stall
/// fail the test deterministically instead of spinning forever.
pub(crate) use crate::test_support::await_until;

/// Serves a fixed body at `/jwks` on an ephemeral port. It returns the
/// bound address and a shutdown token for the server task.
pub async fn serve_jwks(body: &'static str) -> (SocketAddr, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let app = axum::Router::new().route("/jwks", axum::routing::get(move || async move { body }));
    let srv_shutdown = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
            .await
            .unwrap();
    });
    (addr, shutdown)
}

pub const JWKS_BODY: &str = r#"{"keys":[{"kty":"EC","crv":"P-256","kid":"k1","x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU","y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"}]}"#;

/// Picks the slots that matter for on-demand refresh out of a
/// `JwksRefresher`, so that the simple refresher tests stay short. These
/// tests supply `signal_rx` but never send on it. `min_on_demand_pause`
/// does not apply. Each test has its own timestamps.
#[derive(krabka_macros::FieldDefaults)]
pub struct RefresherSetup {
    #[default("http://127.0.0.1:1/jwks".into())]
    pub endpoint: String,
    #[default(millis(50))]
    pub interval: Time,
    pub tls_trust: Option<PathBuf>,
    #[default(dormant_timer())]
    pub timer: Arc<dyn Timer>,
}

pub fn test_refresher(
    handle: JwksHandle,
    shutdown: CancellationToken,
    setup: RefresherSetup,
) -> JwksRefresher {
    let RefresherSetup {
        endpoint,
        interval,
        tls_trust,
        timer,
    } = setup;
    let (_tx, rx) = mpsc::channel::<()>(1);
    JwksRefresher {
        endpoint,
        handle,
        interval,
        http_timeout: millis(37),
        shutdown,
        tls_trust,
        signal_rx: rx,
        min_on_demand_pause: secs(1),
        last_successful_fetch_ms: Arc::new(AtomicI64::new(0)),
        cache_generation: Arc::new(AtomicU64::new(0)),
        last_on_demand_refresh_ms: Arc::new(AtomicI64::new(0)),
        ignore_key_use: false,
        timer,
    }
}

/// Serves a fixed JSON body over TLS on an ephemeral port, with a newly
/// generated self-signed cert that carries `127.0.0.1` as a SAN. It
/// returns the bound address, a shutdown token, and the PEM path of the
/// cert, which the client can use as a trust bundle.
pub async fn serve_jwks_https(
    body: &'static str,
) -> (std::net::SocketAddr, CancellationToken, std::path::PathBuf) {
    use tokio::io::AsyncWriteExt as _;

    crate::test_support::serve_loopback_tls(move |mut tls| async move {
        use tokio::io::AsyncReadExt as _;
        // Drain a minimal request line + headers (we
        // don't parse — just ignore until empty line).
        // Then write a fixed JSON reply.
        let mut buf = [0u8; 1024];
        let _ = tls.read(&mut buf).await;
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len(),
        );
        let _ = tls.write_all(header.as_bytes()).await;
        let _ = tls.write_all(body.as_bytes()).await;
        let _ = tls.shutdown().await;
    })
    .await
}

/// Serves a fixed body and counts how many HTTP requests reached the
/// `/jwks` route. It returns a shared `AtomicUsize`, so the test can
/// assert on the call count after it drives the refresher.
pub async fn serve_jwks_counting(
    body: &'static str,
) -> (
    SocketAddr,
    CancellationToken,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter_cl = counter.clone();
    let app = axum::Router::new().route(
        "/jwks",
        axum::routing::get(move || {
            let c = counter_cl.clone();
            async move {
                c.fetch_add(1, Ordering::Relaxed);
                body
            }
        }),
    );
    let srv_shutdown = shutdown.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_shutdown.cancelled().await })
            .await
            .unwrap();
    });
    (addr, shutdown, counter)
}

/// Builds a refresher with a 1-hour periodic interval, so that only
/// on-demand signals matter for the test. It returns the shared signal
/// sender, the rate-limit timestamp, success timestamp, and cache generation.
pub type SignalRefresher = (
    JwksRefresher,
    mpsc::Sender<()>,
    Arc<AtomicI64>,
    Arc<AtomicI64>,
    Arc<AtomicU64>,
    CancellationToken,
    JwksHandle,
);

pub fn make_signal_refresher(endpoint: String, min_on_demand_pause: Time) -> SignalRefresher {
    let (signal_tx, signal_rx) = mpsc::channel::<()>(1);
    let shutdown = CancellationToken::new();
    let last_successful = Arc::new(AtomicI64::new(0));
    let last_on_demand = Arc::new(AtomicI64::new(0));
    let cache_generation = Arc::new(AtomicU64::new(0));
    let handle = JwksHandle::new_with_refresher_handles(
        Jwks::empty(),
        last_successful.clone(),
        signal_tx.clone(),
    );
    let refresher = JwksRefresher {
        endpoint,
        handle: handle.clone(),
        interval: hours(1),
        http_timeout: millis(37),
        shutdown: shutdown.clone(),
        tls_trust: None,
        signal_rx,
        min_on_demand_pause,
        last_successful_fetch_ms: last_successful.clone(),
        cache_generation: cache_generation.clone(),
        last_on_demand_refresh_ms: last_on_demand.clone(),
        ignore_key_use: false,
        // Signal tests isolate the on-demand arm; periodic refreshes have
        // dedicated manual-timeline coverage above. The one-hour interval is
        // armed on a timer nothing advances, so the loop's first tick is the
        // only periodic fetch a signal test ever sees.
        timer: dormant_timer(),
    };
    (
        refresher,
        signal_tx,
        last_successful,
        last_on_demand,
        cache_generation,
        shutdown,
        handle,
    )
}
