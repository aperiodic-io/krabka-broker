//! SASL/SCRAM end-to-end for both SHA-256 and SHA-512: the RFC 5802
//! two-round exchange against a credential provisioned through the
//! controller, and the wrong-password path that closes the connection.

use {
    crate::{
        harness::{alice_password, metadata_probe, start_scram_alice, wrong_scram_password},
        kafka_wire::{
            ScramAuthenticationSetup, sasl_authenticate_on, sasl_handshake_on,
            sasl_scram_authenticate_on,
        },
        reauth_cases::{
            capped_session_closes, different_principal_closes, repeated_expired_reauth,
            same_principal_reopens,
        },
        support::wire::CorrelationId,
    },
    assert2::assert,
    krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
    krabka_security::SaslMechanism,
    std::{io, net::SocketAddr},
    tokio::net::TcpStream,
};

/// Happy-path drive of a SASL/SCRAM-SHA-512 session.
///
/// The test provisions a credential for "alice" with the shared test
/// password. It goes through the controller directly and not through the
/// public `AlterUserScramCredentials` handler. It then runs the two-round
/// RFC 5802 exchange end-to-end and asserts that the post-auth Metadata
/// request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path() {
    let (_log_dir, handle, addr) = scram_fixture(SaslMechanism::ScramSha512).await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha512)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM session must succeed end-to-end");
}

/// Negative path: with a wrong password, `SaslAuthenticate` round 2 responds
/// with `error_code = 58`, which is `SASL_AUTHENTICATION_FAILED`, and the
/// broker closes the connection.
///
/// `drive_sasl_scram_session` reports the failure as a non-zero error code on
/// the auth response, or as an EOF when the Metadata read that follows
/// returns no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_wrong_password_closes_connection() {
    let (_log_dir, handle, addr) = scram_fixture(SaslMechanism::ScramSha512).await;
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha512,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SCRAM session: {result:?}"
    );
}

/// SASL/SCRAM-SHA-256 happy path.
///
/// The test is a copy of the SHA-512 test, but it provisions a SHA-256
/// credential and configures the listener to enable only SHA-256. This proves
/// that the new mechanism is wired end-to-end, and that it does not use the
/// SHA-512 code by accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_happy_path() {
    let (_log_dir, handle, addr) = scram_fixture(SaslMechanism::ScramSha256).await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha256)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM-SHA-256 session must succeed end-to-end");
}

/// Negative path for SHA-256: a wrong password must close the connection,
/// the same as in the SHA-512 variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_wrong_password_closes_connection() {
    let (_log_dir, handle, addr) = scram_fixture(SaslMechanism::ScramSha256).await;
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha256,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SHA-256 SCRAM session: {result:?}"
    );
}

/// Drive a complete SASL/SCRAM session against a `SASL_PLAINTEXT` listener.
///
/// This helper works for both SHA-256 and SHA-512. It passes the mechanism
/// through to the handshake and to the client state machine.
///
/// On success it returns `Ok(())` after a successful post-auth Metadata
/// round-trip. It returns `Err` when any step fails: a non-zero error code on
/// either of the two `SaslAuthenticate` rounds, a server-final signature
/// mismatch in the client-side proof, or EOF before Metadata returns.
pub async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
    mechanism: krabka_security::SaslMechanism,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    sasl_scram_authenticate_on(&mut stream, &ScramAuthenticationSetup {
        client_id: "krabka-sasl-test",
        user,
        password: (password).into(),
        mechanism,
    })
    .await?;
    metadata_probe(&mut stream, 5).await
}

/// Runs the two RFC 5802 rounds on `stream` and returns the round-2
/// response, whose `session_lifetime_ms` is the KIP-368 window.
pub async fn scram_authenticate(
    stream: &mut TcpStream,
    corr: &mut CorrelationId,
    setup: &ScramAuthenticationSetup<'_>,
) -> Result<SaslAuthenticateResponse, io::Error> {
    corr.0 += 1;
    sasl_handshake_on()
        .stream(stream)
        .client_id(setup.client_id)
        .correlation(*corr)
        .mechanism(setup.mechanism.wire_name())
        .call()
        .await?;

    let (client_first, client) =
        krabka_macros::scram_client_first!(setup.user, setup.password.as_str(), setup.mechanism)?;
    corr.0 += 1;
    let first = sasl_authenticate_on()
        .stream(stream)
        .auth_bytes(bytes::Bytes::from(client_first))
        .client_id(setup.client_id)
        .correlation(*corr)
        .call()
        .await?;
    if first.error_code != 0 {
        return Ok(first);
    }

    let (client_final, _client) = client
        .step(&first.auth_bytes)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    corr.0 += 1;
    sasl_authenticate_on()
        .stream(stream)
        .auth_bytes(bytes::Bytes::from(client_final))
        .client_id(setup.client_id)
        .correlation(*corr)
        .call()
        .await
}

/// KIP-368: a SCRAM session under `connections.max.reauth.ms` reports the
/// window as `session_lifetime_ms`, and a data-plane request that arrives after
/// it elapses without an in-band re-authentication closes the connection.
///
/// The close happens on that request and not on a timer, which is where Kafka
/// puts it (`Processor.processCompletedReceives`): a connection that has fallen
/// silent is reclaimed by `connections.max.idle.ms`, not by this window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_session_capped_by_connections_max_reauth_then_closes() {
    capped_session_closes(SaslMechanism::ScramSha512).await;
}

/// KIP-368: a SCRAM re-auth for the same principal runs both rounds in the
/// `Reauthenticating` state and re-arms the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_same_principal_reopens_data_plane() {
    same_principal_reopens(SaslMechanism::ScramSha512).await;
}

/// KIP-368 forbids a principal switch mid-connection: a SCRAM re-auth as a
/// different user answers `SASL_AUTHENTICATION_FAILED` (58) and closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_in_band_reauth_with_different_principal_closes() {
    different_principal_closes(SaslMechanism::ScramSha512).await;
}

/// KIP-368: a connection re-authenticates round after round for the whole life
/// of the connection, and it may begin each round *after* its window has
/// already elapsed.
///
/// That lateness is not an edge case. A JVM client picks its re-auth point at
/// 85–95% of the advertised `session_lifetime_ms`, but acts on it only when it
/// next has a request to write, so a producer on its own send cadence opens the
/// exchange past the deadline as a matter of course. Kafka serves it: the
/// broker runs no timer, and an expired session is closed only by a request
/// that is not part of a re-authentication.
///
/// The window here is short and the test sleeps past it before every round, so
/// each of the three rounds starts expired. A broker that closed on the deadline
/// itself fails on the first one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scram_reauth_after_the_window_elapsed_keeps_serving_round_after_round() {
    repeated_expired_reauth(
        SaslMechanism::ScramSha512,
        krabka_units::secs(1),
        900..=1_000,
    )
    .await;
}

async fn scram_fixture(
    mechanism: SaslMechanism,
) -> (tempfile::TempDir, krabka_broker::BrokerHandle, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_scram_alice(log_dir.path().to_path_buf(), mechanism).await;
    let addr = handle.listen_addr();
    (log_dir, handle, addr)
}
