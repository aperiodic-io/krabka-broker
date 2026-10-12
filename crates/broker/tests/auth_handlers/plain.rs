//! SASL/PLAIN end-to-end: the happy path, a wrong password that closes the
//! connection, and the per-mechanism authentication counters those two
//! sessions tick on the `/metrics` scrape.

use std::{io, net::SocketAddr};

use assert2::assert;

use crate::{
    harness::{
        alice_password, alice_plain_config, common_imports::*, metadata_probe, round_trip,
        start_alice_plain, start_reauth_broker, wrong_scram_password,
    },
    kafka_wire::{
        PlainAuthenticationSetup, sasl_plain_authenticate_on, sasl_plain_reauthenticate_on,
    },
    reauth_cases::{
        capped_session_closes, different_principal_closes, repeated_expired_reauth,
        same_principal_reopens,
    },
    support::{
        client::scrape_metrics as scrape_metrics_client,
        sasl::plain_authentication_body,
        wire::{CorrelationId, HeaderEncoding as RequestHeaderEncoding, SocketExchangeSetup},
    },
};

/// Happy-path drive of a SASL/PLAIN session: `ApiVersions` → `SaslHandshake`
/// → `SaslAuthenticate` → Metadata.
///
/// The test asserts that the connection survives every step and that the
/// final Metadata response carries this broker. The dial side sends raw
/// bytes over `TcpStream` and not `Client`, because `Client` does not speak
/// SASL yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path() {
    let (_log_dir, handle, addr) = start_alice_plain(alice_password()).await;
    let result = drive_sasl_plain_session(addr, "alice", alice_password().as_bytes()).await;
    handle.shutdown().await;
    result.expect("SASL/PLAIN session must succeed end-to-end");
}

/// SASL PLAIN metrics: one happy-path session and one wrong-password session
/// tick both the `successful_authentication_total` and the
/// `failed_authentication_total` per-mechanism counters on the `/metrics`
/// scrape.
///
/// The test checks the end-to-end wire path from the `SaslAuthenticate`
/// dispatch site to the rendered Prometheus text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_authentication_metrics_tick_for_success_and_failure() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = alice_plain_config(log_dir.path().to_path_buf(), alice_password());
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // 1. Happy path — must tick `successful_authentication_total`.
    drive_sasl_plain_session(addr, "alice", alice_password().as_bytes())
        .await
        .expect("happy-path PLAIN session");
    // 2. Wrong password — must tick `failed_authentication_total`.
    let bad = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    assert!(bad.is_err(), "wrong password must fail: {bad:?}");

    let body = scrape_metrics(metrics_addr).await;
    handle.shutdown().await;

    let success_needle = "krabka_broker_successful_authentication_total{mechanism=\"PLAIN\"} 1";
    let failed_needle = "krabka_broker_failed_authentication_total{mechanism=\"PLAIN\"} 1";
    assert!(
        body.contains(success_needle),
        "missing or wrong-value {success_needle} in:\n{body}"
    );
    assert!(
        body.contains(failed_needle),
        "missing or wrong-value {failed_needle} in:\n{body}"
    );
}

/// Send an HTTP GET `/metrics` to `addr` and return the response body.
///
/// The returned body holds no HTTP head.
async fn scrape_metrics(addr: SocketAddr) -> String {
    scrape_metrics_client(addr).await
}

/// Negative path: with a wrong password, `SaslAuthenticate` responds with
/// `error_code = SASL_AUTHENTICATION_FAILED` (58) and the broker closes the
/// connection.
///
/// `drive_sasl_plain_session` reports the failure as an `Err` in two cases.
/// The first case is a non-zero `error_code` on the auth response. The second
/// case is an EOF on the Metadata read that follows, when the peer closed the
/// connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection() {
    let (_log_dir, handle, addr) = start_alice_plain(alice_password()).await;
    let result = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail the SASL session: {result:?}"
    );
}

/// Drive a complete SASL/PLAIN session against a `SASL_PLAINTEXT` listener.
///
/// On success, this helper returns `Ok(())` after a successful post-auth
/// Metadata round-trip. It returns `Err` when any step fails: frame I/O,
/// response decode, a non-zero error code on a SASL response, or EOF before
/// Metadata.
///
/// This helper handles these wire-protocol mechanics inline, without the
/// `Client` API:
/// - Request headers: v1, non-flexible, for `ApiVersions v0` and
///   `SaslHandshake v1`. v2, flexible with a trailing `0x00` tagged-fields
///   byte, for `SaslAuthenticate v2` and `Metadata v12`.
/// - Response headers: always v0, which holds only `correlation_id`, for
///   `ApiVersions`, whatever the body flexibility. v1, which holds `corr_id`
///   plus a `0x00` tagged byte, for every other flexible response. v0 for
///   non-flexible.
/// - Length framing: a 4-byte big-endian length prefix on every frame in both
///   directions, the same as `krabka_broker::network::codec`.
async fn drive_sasl_plain_session(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    sasl_plain_authenticate_on(
        &mut stream,
        &PlainAuthenticationSetup {
            client_id: "krabka-sasl-test",
            user,
            password: (password).to_vec(),
        },
    )
    .await?;
    metadata_probe(&mut stream, 4).await
}

/// Opens a PLAIN session on `stream` and returns the `SaslAuthenticate`
/// response, whose `session_lifetime_ms` is the KIP-368 window. `corr` keeps
/// correlation ids unique across the initial authentication and any later
/// in-band re-auth on the same connection.
pub async fn plain_authenticate(
    stream: &mut TcpStream,
    corr: &mut CorrelationId,
    setup: &PlainAuthenticationSetup<'_>,
) -> Result<SaslAuthenticateResponse, io::Error> {
    sasl_plain_reauthenticate_on(stream, corr, setup).await
}

/// KIP-368: with `connections.max.reauth.ms` set, a PLAIN session reports the
/// window as `session_lifetime_ms`, and a data-plane request that arrives after
/// it elapses without an in-band re-authentication closes the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_session_capped_by_connections_max_reauth_then_closes() {
    capped_session_closes(SaslMechanism::Plain).await;
}

/// KIP-368: an in-band re-authentication with the same principal succeeds and
/// re-opens the data plane on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_in_band_reauth_same_principal_reopens_data_plane() {
    same_principal_reopens(SaslMechanism::Plain).await;
}

/// KIP-368 forbids a principal switch mid-connection: a PLAIN re-auth that
/// authenticates a different user answers `SASL_AUTHENTICATION_FAILED` (58)
/// and the broker closes the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_in_band_reauth_with_different_principal_closes() {
    different_principal_closes(SaslMechanism::Plain).await;
}

/// A `SaslAuthenticate` before any handshake closes the connection with no
/// response, as Kafka's `SaslServerAuthenticator.handleKafkaRequest` does
/// (`InvalidRequestException`), rather than authenticating anyone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_authenticate_without_handshake_closes_the_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle =
        start_reauth_broker(log_dir.path(), krabka_units::secs(30), SaslMechanism::Plain).await;
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let auth_body = plain_authentication_body("alice", alice_password().as_bytes());
    check!(
        round_trip(
            &mut stream,
            SocketExchangeSetup {
                api_key: WireApiKey(36),
                version: WireApiVersion(2),
                header: RequestHeaderEncoding::Flexible,
                ..Default::default()
            },
            &auth_body
        )
        .await
        .is_err(),
        "a SaslAuthenticate before a handshake must close with no response"
    );

    handle.shutdown().await;
}

/// KIP-368: a PLAIN connection re-authenticates round after round, and each
/// round may begin after its window has already elapsed.
///
/// A JVM client re-authenticates only when it next has a request to write, so
/// on its own send cadence it opens the exchange past the deadline routinely.
/// The window here is short and the test sleeps past it before every round, so
/// each of the three rounds starts expired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_reauth_after_the_window_elapsed_keeps_serving_round_after_round() {
    repeated_expired_reauth(SaslMechanism::Plain, krabka_units::millis(300), 200..=300).await;
}
