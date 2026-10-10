//! Behaviour tests for the refresh loop: the immediate first fetch, HTTPS with
//! a custom trust bundle, the on-demand signal and its rate limit, the two
//! shared timestamps a validator reads, and the shutdown a dead ticker forces.

use assert2::{assert, check};
use krabka_units::{millis, minutes};
use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, StdTimer};

use super::*;
use crate::{
    oauth_jwks::test_support::{
        JWKS_BODY, await_until, dormant_timer, make_signal_refresher, serve_jwks,
        serve_jwks_counting, serve_jwks_https, test_refresher,
    },
    test_support::{BrokenTimer, TimerFailure},
};

async fn check_first_fetch_and_stop(
    label: &str,
    handle: &JwksHandle,
    shutdown: &CancellationToken,
    task: tokio::task::JoinHandle<()>,
    server_shutdown: &CancellationToken,
) {
    await_until(label, || !handle.load().is_empty()).await;
    assert!(handle.load().len() == 1);
    shutdown.cancel();
    task.await.unwrap();
    server_shutdown.cancel();
}

#[tokio::test]
async fn refresher_populates_handle_then_stops_on_shutdown() {
    let (addr, srv_shutdown) = serve_jwks(JWKS_BODY).await;
    let handle = JwksHandle::default();
    assert!(handle.load().is_empty());
    let shutdown = CancellationToken::new();
    let refresher = test_refresher(
        handle.clone(),
        shutdown.clone(),
        crate::oauth_jwks::test_support::RefresherSetup { endpoint: format!("http://{addr}/jwks"), timer: // The production timer, so that one test covers the real thing
        // driving this loop. `StdTimer` needs no Tokio runtime of its own.
        Arc::new(StdTimer::new()), ..Default::default() },
    );
    let task = tokio::spawn(refresher.run());

    // Poll until the immediate first fetch lands.
    check_first_fetch_and_stop(
        "first JWKS fetch populates handle",
        &handle,
        &shutdown,
        task,
        &srv_shutdown,
    )
    .await;
}

#[tokio::test]
async fn refresher_fetches_jwks_over_https_with_custom_trust() {
    let (addr, srv_shutdown, ca_path) = serve_jwks_https(JWKS_BODY).await;
    let handle = JwksHandle::default();
    let shutdown = CancellationToken::new();
    let refresher = test_refresher(
        handle.clone(),
        shutdown.clone(),
        crate::oauth_jwks::test_support::RefresherSetup { endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()), tls_trust: Some(ca_path), timer: // This test asserts on the t=0 fetch alone, so the periodic tick is
        // parked rather than re-fetching over TLS every 50 ms of real time.
        dormant_timer(), ..Default::default() },
    );
    let task = tokio::spawn(refresher.run());
    check_first_fetch_and_stop(
        "first HTTPS JWKS fetch populates handle",
        &handle,
        &shutdown,
        task,
        &srv_shutdown,
    )
    .await;
}

#[tokio::test]
async fn refresher_https_fetch_fails_when_custom_trust_doesnt_match_server_cert() {
    // Server presents cert A; trust bundle is an unrelated cert B. Every
    // refresh fails TLS verification, so the handle must never populate.
    //
    // Driven on a manual timeline instead of a wall-clock timer: the first
    // fetch fires immediately (t=0 tick), then the loop parks on the
    // refresh-interval deadline. Advancing the timeline fires each subsequent
    // fetch deterministically — exact and instant, with no flaky timing.
    let (addr, srv_shutdown, _server_cert_path) = serve_jwks_https(JWKS_BODY).await;

    let dir = tempfile::tempdir().unwrap();
    let params = rcgen::CertificateParams::new(vec!["unrelated.example".to_string()]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let cert = params.self_signed(&key).unwrap();
    let bogus_ca = dir.path().join("bogus-ca.pem");
    std::fs::write(&bogus_ca, cert.pem()).unwrap();

    let handle = JwksHandle::default();
    let shutdown = CancellationToken::new();
    let interval = millis(50);
    let clock = ManualMonotonicClock::new_shared();
    let refresher = test_refresher(
        handle.clone(),
        shutdown.clone(),
        crate::oauth_jwks::test_support::RefresherSetup {
            endpoint: format!("https://127.0.0.1:{}/jwks", addr.port()),
            interval,
            tls_trust: Some(bogus_ca),
            timer: clock.new_timer(),
        },
    );
    let task = tokio::spawn(refresher.run());

    // Drive several refresh intervals. Each round blocks (bounded real time,
    // hang-guard only) until the loop has armed the next interval deadline —
    // which confirms the prior fetch attempt completed — and advances to that
    // deadline in the same locked step, so a deadline the loop has reached but
    // not yet polled cannot satisfy the wait on its own. Then assert the
    // failing fetch left the handle empty. The wait runs on a blocking thread
    // so it never stalls the current-thread runtime that must drive the
    // refresher's HTTPS attempt.
    for _ in 0..3 {
        let armed = Arc::clone(&clock);
        let advanced = tokio::task::spawn_blocking(move || {
            armed.advance_to_next_deadline_after_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            advanced.is_some(),
            "refresher should park on the interval deadline between fetches",
        );
        assert!(
            handle.load().is_empty(),
            "fetch should fail verification and leave handle empty",
        );
    }

    shutdown.cancel();
    task.await.unwrap();
    srv_shutdown.cancel();
}

// ---- On-demand refresh + cache-expiry timestamp -------------

#[tokio::test]
async fn refresher_signal_triggers_on_demand_refresh_when_pause_elapsed() {
    let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (
        refresher,
        signal_tx,
        _last_successful,
        last_on_demand,
        _cache_generation,
        shutdown,
        handle,
    ) = make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    let task = tokio::spawn(refresher.run());

    // Drive at least one signal refresh.
    signal_tx.send(()).await.unwrap();
    // Poll until the on-demand timestamp moves and the handle has been
    // populated by the on-demand fetch.
    await_until(
        "on-demand fetch advances timestamp and populates handle",
        || last_on_demand.load(Ordering::Relaxed) > 0 && !handle.load().is_empty(),
    )
    .await;
    check!(
        last_on_demand.load(Ordering::Relaxed) > 0,
        "on-demand timestamp should have advanced past sentinel 0",
    );
    check!(
        count.load(Ordering::Relaxed) >= 1,
        "server should have served the on-demand request"
    );
    check!(
        handle.load().len() == 1,
        "refresher must store the fetched key set"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_signal_dropped_when_within_min_pause_window() {
    let (addr, srv_shutdown, count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    // 60s pause — second signal MUST be rate-limited.
    let (
        refresher,
        signal_tx,
        _last_successful,
        last_on_demand,
        _cache_generation,
        shutdown,
        _handle,
    ) = make_signal_refresher(endpoint, minutes(1));
    let task = tokio::spawn(refresher.run());

    // First signal: fires. Wait on the HTTP counter (the strict
    // happens-after of refresh_and_swap) rather than the timestamp
    // store, which the select! arm performs BEFORE the HTTP call —
    // otherwise CI races between timestamp-set and request-arrival.
    //
    // Two requests are expected, not one: the loop's t=0 tick fetches
    // immediately whatever the timer, and the signal fetches on top of it.
    // The periodic tick then re-arms on a timer nothing advances, so the
    // count settles at two and any later increment can only be a signal.
    signal_tx.send(()).await.unwrap();
    for _ in 0..100 {
        if count.load(Ordering::Relaxed) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let first_ts = last_on_demand.load(Ordering::Relaxed);
    assert!(first_ts > 0, "first signal must have fired a refresh");
    let count_after_first = count.load(Ordering::Relaxed);
    assert!(count_after_first >= 2);

    // Second signal within the 60s pause: dropped.
    signal_tx.send(()).await.unwrap();
    // Yield the runtime a few times to let the select! arm process.
    for _ in 0..10 {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let actual = (
        last_on_demand.load(Ordering::Relaxed),
        count.load(Ordering::Relaxed),
    );
    assert!(
        actual == (first_ts, count_after_first),
        "second signal within min_pause must change neither timestamp nor HTTP request count"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_successful_refresh_updates_last_successful_fetch_timestamp() {
    let (addr, srv_shutdown, _count) = serve_jwks_counting(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (
        refresher,
        signal_tx,
        last_successful,
        _last_on_demand,
        cache_generation,
        shutdown,
        _handle,
    ) = make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);

    // Read the sentinel before the loop starts: the t=0 tick is a successful
    // refresh of its own, and it advances the timestamp as soon as the task
    // is polled.
    assert!(last_successful.load(Ordering::Relaxed) == 0);
    let task = tokio::spawn(refresher.run());
    signal_tx.send(()).await.unwrap();
    await_until(
        "successful fetch advances last_successful timestamp",
        || last_successful.load(Ordering::Relaxed) > 0,
    )
    .await;
    assert!(
        last_successful.load(Ordering::Relaxed) > 0,
        "last_successful_fetch_ms must advance after a successful fetch",
    );
    assert!(
        cache_generation.load(Ordering::Acquire) == 2,
        "successful fetch must publish one stable even generation",
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_failed_refresh_does_not_advance_last_successful_fetch() {
    // Endpoint that always returns 500 ⇒ fetch fails, timestamp must stay
    // at the sentinel 0.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv_shutdown = CancellationToken::new();
    let srv_token = srv_shutdown.clone();
    let app = axum::Router::new().route(
        "/jwks",
        axum::routing::get(|| async { (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { srv_token.cancelled().await })
            .await
            .unwrap();
    });

    let endpoint = format!("http://{addr}/jwks");
    let (
        refresher,
        signal_tx,
        last_successful,
        last_on_demand,
        cache_generation,
        shutdown,
        _handle,
    ) = make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    let task = tokio::spawn(refresher.run());

    signal_tx.send(()).await.unwrap();
    // Wait for the failed refresh attempt to complete & log; the on-demand
    // rate-limit timestamp advances even when the fetch itself fails.
    await_until(
        "failed on-demand fetch still advances rate-limit timestamp",
        || last_on_demand.load(Ordering::Relaxed) > 0,
    )
    .await;
    // On-demand timestamp advances regardless (rate-limit accounting);
    // success timestamp must stay at 0.
    assert!(
        last_on_demand.load(Ordering::Relaxed) > 0,
        "on-demand rate-limit timestamp updates even when the fetch itself fails",
    );
    assert!(
        last_successful.load(Ordering::Relaxed) == 0,
        "failed fetch must leave last_successful_fetch_ms at sentinel 0"
    );
    assert!(
        cache_generation.load(Ordering::Acquire) == 0,
        "failed fetch must not publish a cache generation",
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_generation_exhaustion_preserves_the_prior_cache() {
    let (addr, srv_shutdown) = serve_jwks(JWKS_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (
        refresher,
        _signal_tx,
        last_successful,
        _last_on_demand,
        cache_generation,
        _shutdown,
        handle,
    ) = make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    cache_generation.store(u64::MAX - 1, Ordering::Release);

    refresher.refresh_and_swap(&reqwest::Client::new()).await;

    assert!(
        handle.load().is_empty(),
        "exhausted cache must keep its prior keys"
    );
    assert!(
        last_successful.load(Ordering::Acquire) == 0,
        "exhausted cache must keep its prior success timestamp",
    );
    assert!(
        cache_generation.load(Ordering::Acquire) == u64::MAX - 1,
        "generation exhaustion must not enter the write phase",
    );
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_passes_ignore_key_use_through_to_jwks_parser() {
    // JWKS body has an RSA key marked `use=enc`. With ignore_key_use=true
    // the refresher should still install it; with false it would be filtered
    // out (yielding an empty key set).
    const ENC_KEY_BODY: &str =
        r#"{"keys":[{"kty":"RSA","kid":"enc-kid","use":"enc","n":"AQAB","e":"AQAB"}]}"#;
    let (addr, srv_shutdown, _count) = serve_jwks_counting(ENC_KEY_BODY).await;
    let endpoint = format!("http://{addr}/jwks");
    let (
        mut refresher,
        signal_tx,
        _last_successful,
        _last_on_demand,
        _cache_generation,
        shutdown,
        handle,
    ) = make_signal_refresher(endpoint, <Time as TimeExt>::ZERO);
    refresher.ignore_key_use = true;
    let task = tokio::spawn(refresher.run());

    signal_tx.send(()).await.unwrap();
    await_until("on-demand fetch installs the use=enc key", || {
        !handle.load().is_empty()
    })
    .await;
    assert!(
        handle.load().len() == 1,
        "ignore_key_use=true must keep the use=enc key in the installed set"
    );

    shutdown.cancel();
    let _ = task.await;
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_stops_without_fetching_when_the_first_deadline_is_refused() {
    check_first_deadline_failure(TimerFailure::Registration).await;
}

#[tokio::test]
async fn refresher_stops_when_the_first_deadline_is_armed_but_never_completes() {
    check_first_deadline_failure(TimerFailure::Completion).await;
}

fn dead_timer_refresher(
    addr: std::net::SocketAddr,
    handle: &JwksHandle,
    timer: &Arc<BrokenTimer>,
) -> JwksRefresher {
    test_refresher(
        handle.clone(),
        CancellationToken::new(),
        crate::oauth_jwks::test_support::RefresherSetup {
            endpoint: format!("http://{addr}/jwks"),
            timer: timer.injectable(),
            ..Default::default()
        },
    )
}

async fn check_first_deadline_failure(failure: TimerFailure) {
    let (addr, srv_shutdown, requests) = serve_jwks_counting(JWKS_BODY).await;
    let handle = JwksHandle::default();
    let timer = BrokenTimer::dead(failure);
    // No caller cancels this token. A refused registration or a failed first
    // deadline is therefore the only reason the refresher can stop.
    let refresher = dead_timer_refresher(addr, &handle, &timer);
    tokio::spawn(refresher.run())
        .await
        .expect("refresher task exits");
    check!(handle.load().is_empty());
    check!(requests.load(Ordering::Relaxed) == 0);
    check!(timer.registrations() == 1);
    srv_shutdown.cancel();
}

#[tokio::test]
async fn refresher_fetches_once_and_stops_when_the_interval_cannot_be_re_armed() {
    let (addr, srv_shutdown, requests) = serve_jwks_counting(JWKS_BODY).await;
    let handle = JwksHandle::default();
    let timer = BrokenTimer::dead_after(1, TimerFailure::Registration);
    let refresher = dead_timer_refresher(addr, &handle, &timer);

    // The start-up deadline is honoured, so the t=0 fetch lands and the keys
    // reach the handle. The interval the loop re-arms afterwards is refused,
    // and the task stops rather than retrying it: two registrations in all,
    // not a climbing count.
    tokio::spawn(refresher.run())
        .await
        .expect("refresher task exits");
    check!(handle.load().len() == 1);
    check!(requests.load(Ordering::Relaxed) == 1);
    check!(timer.registrations() == 2);
    srv_shutdown.cancel();
}
