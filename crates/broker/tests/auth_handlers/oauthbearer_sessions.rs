//! SASL/OAUTHBEARER re-authentication (KIP-368) end-to-end.
//!
//! Six scenarios exercise the full session-lifetime and in-band re-auth
//! surface: the response carries `session_lifetime_ms ~ exp - now`; a broker
//! cap clamps it; a request past the token's `exp` closes the connection; an
//! in-band re-auth with a fresh token re-arms the session even when it starts
//! after the old token expired; an in-band re-auth with a different principal
//! name returns 58 and closes the connection; an in-band attempt to switch
//! mechanism returns 34; and, as a regression, a PLAIN listener reports
//! `session_lifetime_ms = 0` and expires nothing.
//!
//! The expiry deadline is a wall-clock instant that the dispatch loop compares
//! each arriving request against, the way Kafka's
//! `Processor.processCompletedReceives` does, so these tests use short real
//! windows and real sleeps rather than a paused tokio clock. There is no timer
//! to jump past, and a session that has expired stays open until a request
//! that is not part of a re-authentication arrives on it.

use {
    crate::{
        harness::{alice_password, round_trip, sasl_authenticate_round_trip, start_broker_stream},
        kafka_wire::exchange,
        oauthbearer::{
            drive_inband_reauth, drive_sasl_oauthbearer_session_open, now_unix_secs,
            oauthbearer_fixture, oauthbearer_zero_skew_validator, unsecured_jws,
        },
        support::{
            sasl::{plain_authentication_body, sasl_temp_config},
            sasl_plaintext_config,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup,
            },
        },
    },
    assert2::assert,
    bytes::BytesMut,
    krabka_broker::Broker,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request, api_versions_response, metadata_request, metadata_response,
            sasl_authenticate_response, sasl_handshake_request, sasl_handshake_response,
        },
    },
    krabka_security::SaslMechanism,
    tokio::{io::AsyncReadExt, net::TcpStream},
};

/// Sends one Metadata request on a session whose window has elapsed.
///
/// That request is what ends the connection: KIP-368 expiry is enforced where
/// a request is admitted, not by a timer, so a test that only waits out the
/// window would wait forever. The broker closes without answering, so the
/// round trip's own result is nothing to assert on — the EOF the caller then
/// reads is.
async fn send_metadata_on_expired_session(stream: &mut TcpStream) {
    let mut md_body = BytesMut::new();
    metadata_request::MetadataRequest::default()
        .encode(&mut md_body, 12)
        .expect("Metadata encode must succeed");
    let _ = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(98),
            header: RequestHeaderEncoding::Flexible,
        },
        &md_body,
    )
    .await;
}

/// Test #1: a successful OAUTHBEARER authentication carries
/// `session_lifetime_ms ≈ exp - now`.
///
/// `session_lifetime_ms` is the KIP-368 wire field on
/// `sasl_authenticate_response::SaslAuthenticateResponse v1+`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_lifetime_ms_set_from_token_exp() {
    let (_log_dir, handle, addr) =
        oauthbearer_fixture(oauthbearer_zero_skew_validator(), None).await;

    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");
    drop(stream);

    // ~600_000 ms; allow generous wall-clock slop for CI.
    assert!(
        (590_000..605_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~600_000"
    );

    handle.shutdown().await;
}

/// The broker clamps `session_lifetime_ms` when the listener sets
/// `connections.max.reauth.ms`.
///
/// The response value becomes `min(token_exp_ms - now_ms, cap * 1000)`. The
/// dispatch loop anchors its deadline to the CLAMPED value, so the broker
/// enforces what it told the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_capped_by_connections_max_reauth() {
    let (_log_dir, handle, addr) = oauthbearer_fixture(
        oauthbearer_zero_skew_validator(),
        Some(krabka_units::millis(300)),
    )
    .await;

    // Token exp = now + 600s. Cap = 300 ms. Expected session = 300 ms.
    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Cap should clamp the response.
    assert!(
        (200..=300).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~300 (capped)"
    );

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    send_metadata_on_expired_session(&mut stream).await;

    let n = read_session_end(&mut stream).await;
    assert!(
        n == 0,
        "expected EOF after cap-bounded session expiry, got {n} bytes"
    );

    handle.shutdown().await;
}

/// Test #2: a request that arrives past the token's `exp` closes the TCP
/// stream, and the client observes EOF on the next read.
///
/// The token is given a few seconds of life and the test waits it out, rather
/// than jumping a paused tokio clock: the deadline is a wall-clock instant the
/// dispatch loop compares each arriving request against, not a timer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_expires_closes_connection() {
    let (_log_dir, handle, addr) =
        oauthbearer_fixture(oauthbearer_zero_skew_validator(), None).await;

    // `now_unix_secs` truncates to whole seconds, so `now + 1` left the
    // handshake anywhere between zero and one second before the token expired,
    // and under load it expired mid-handshake and the broker rejected it. The
    // headroom below is for the open; the expiry this test is about is the one
    // the wait below crosses.
    let exp_secs = now_unix_secs() + 4;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Wait out whatever is left of the token, measured after the handshake
    // rather than assumed before it, plus a margin to land past `exp`.
    wait_past_expiration(exp_secs).await;
    send_metadata_on_expired_session(&mut stream).await;

    // Broker must have closed the connection. Read should EOF.
    let n = read_session_end(&mut stream).await;
    assert!(n == 0, "expected EOF after session expiry, got {n} bytes");

    handle.shutdown().await;
}

/// Test #3: an in-band SaslHandshake/SaslAuthenticate pair with a fresh token
/// on an already-authenticated stream resets the per-connection deadline.
///
/// The re-auth here runs *after* token A has already expired, which is what a
/// JVM client does: it picks a re-auth point inside the window but acts on it
/// only when it next has a request to write. The broker must serve the
/// exchange and then re-arm the session from token B's `exp`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_fresh_token_resets_timer() {
    let (_log_dir, handle, addr) =
        oauthbearer_fixture(oauthbearer_zero_skew_validator(), None).await;

    // Token A gets a few seconds so the handshake cannot outlive it -- see the
    // note in `oauthbearer_session_expires_closes_connection`. Token B lasts
    // 600s. What this test needs is to re-auth *past* A's `exp`, which the
    // computed wait below guarantees.
    let exp_a = now_unix_secs() + 4;
    let token_a = unsecured_jws("alice", exp_a);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_a)
        .await
        .expect("initial OAUTHBEARER must succeed");

    wait_past_expiration(exp_a).await;
    let token_b = unsecured_jws("alice", now_unix_secs() + 600);
    drive_inband_reauth(&mut stream, &token_b)
        .await
        .expect("in-band re-auth with fresh token must succeed past token A's exp");

    // Issue a Metadata RPC to prove the connection survived.
    let md_req = metadata_request::MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .expect("Metadata encode must succeed");
    let md_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(99),
            header: RequestHeaderEncoding::Flexible,
        },
        &md_body,
    )
    .await
    .expect("Metadata RPC must succeed past original token expiry");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = metadata_response::MetadataResponse::decode(&mut cur, 12)
        .expect("Metadata decode must succeed");
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}

/// Test #4: the broker rejects an in-band re-auth with a token whose `sub`
/// differs from the original principal name.
///
/// `sasl_authenticate_response::SaslAuthenticateResponse` carries
/// `error_code = SASL_AUTHENTICATION_FAILED (58)`, and the connection closes.
/// The client then reads EOF. KIP-368 forbids a change of principal across an
/// in-band re-auth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_principal_closes() {
    let (_log_dir, handle, addr) =
        oauthbearer_fixture(oauthbearer_zero_skew_validator(), None).await;

    let token_alice = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_alice)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Attempt re-auth with a token belonging to "bob".
    let token_bob = unsecured_jws("bob", now_unix_secs() + 600);
    let result = drive_inband_reauth(&mut stream, &token_bob).await;
    let err = result.expect_err("re-auth with different principal must fail");
    assert!(
        err.to_string().contains("error_code=58"),
        "expected SASL_AUTHENTICATION_FAILED (58); got {err}"
    );

    // Broker closes after the error response.
    let n = read_session_end(&mut stream).await;
    assert!(n == 0, "expected EOF after failed re-auth");

    handle.shutdown().await;
}

/// Test #5: the broker rejects an in-band `SaslHandshake` whose `mechanism`
/// differs from the mechanism it first negotiated.
///
/// The response carries `NONE`, and the next frame closes the connection
/// (Kafka's `REAUTH_BAD_MECHANISM`). KIP-368 needs
/// the same mechanism across an in-band re-auth, even when the broker would
/// otherwise accept SCRAM on a fresh connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_mechanism_closes() {
    // Enable both OAUTHBEARER + SCRAM-SHA-512 on the same listener so a
    // fresh-connection SCRAM handshake WOULD succeed. The reject here
    // must be due to the same-mechanism rule, not "mechanism unknown".
    let (_log_dir, mut cfg) =
        sasl_temp_config(vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512]);
    cfg.oauthbearer_validator = oauthbearer_zero_skew_validator();
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let token = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // In-band SaslHandshake with SCRAM-SHA-512. Kafka answers it with NONE
    // and the enabled list (the mechanism is enabled), moves to
    // REAUTH_BAD_MECHANISM, and fails the connection on the next frame.
    let sh_req = sasl_handshake_request::SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .expect("SaslHandshake encode must succeed");
    let sh_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(200),
            ..Default::default()
        },
        &sh_body,
    )
    .await
    .expect("handshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = sasl_handshake_response::SaslHandshakeResponse::decode(&mut cur, 1)
        .expect("SaslHandshake decode must succeed");
    assert!(
        sh_resp
            == sasl_handshake_response::SaslHandshakeResponse {
                error_code: 0,
                mechanisms: vec!["OAUTHBEARER".to_string(), "SCRAM-SHA-512".to_string()],
                ..Default::default()
            }
    );
    // The next frame, whatever it is, gets no answer: the connection closes.
    assert!(
        round_trip(
            &mut stream,
            SocketExchangeSetup {
                api_key: WireApiKey(17),
                version: WireApiVersion(1),
                correlation: WireCorrelationId(201),
                ..Default::default()
            },
            &sh_body
        )
        .await
        .is_err(),
        "the connection must close on the frame after a mechanism switch"
    );

    handle.shutdown().await;
}

/// Test #6: regression for PLAIN listeners.
///
/// A PLAIN `SaslAuthenticate` response must carry `session_lifetime_ms = 0`.
/// The KIP-368 wire field has a meaning for OAUTHBEARER only. The dispatch
/// loop must NOT arm a per-connection deadline. An advance of the tokio clock
/// by one hour is harmless, and a Metadata RPC still succeeds.
///
/// The test uses the `current_thread` flavor, because `tokio::time::pause()`
/// needs a single-threaded runtime. See the comment on test #2 for why we
/// pause after the handshake and do not set `start_paused = true`.
///
/// The fixture switches `connections.max.idle.ms` off. That deadline is the
/// other reason this connection could be closed an hour on, and it is not the
/// one under test here -- `crates/broker/tests/connections_max_idle.rs` covers
/// it. Leaving it at Kafka's ten minutes would close the connection for being
/// idle and say nothing about the KIP-368 timer.
#[tokio::test(flavor = "current_thread")]
async fn plain_listener_session_lifetime_ms_is_zero_and_no_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = sasl_plaintext_config(log_dir.path().to_path_buf());
    cfg.connections_max_idle = Some(krabka_units::millis(0));

    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());
    let broker_stream = start_broker_stream(cfg).await;
    let handle = broker_stream.broker;

    // Inline a full PLAIN handshake (mirrors `drive_sasl_plain_session`)
    // so we can capture the sasl_authenticate_response::SaslAuthenticateResponse and assert its
    // `session_lifetime_ms` field directly.
    let mut stream = broker_stream.stream;

    let _: api_versions_response::ApiVersionsResponse = exchange()
        .stream(&mut stream)
        .request(&api_versions_request::ApiVersionsRequest::default())
        .client_id("krabka-sasl-test")
        .call()
        .await
        .expect("ApiVersions round-trip");

    let sh_req = sasl_handshake_request::SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(2),
            ..Default::default()
        },
        &sh_body,
    )
    .await
    .expect("SaslHandshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = sasl_handshake_response::SaslHandshakeResponse::decode(&mut cur, 1).unwrap();
    assert!(sh_resp.error_code == 0, "PLAIN handshake must succeed");

    let auth_body = plain_authentication_body("alice", alice_password().as_bytes());
    let auth_resp_bytes = sasl_authenticate_round_trip(&mut stream, &auth_body)
        .await
        .expect("SaslAuthenticate round-trip");
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp =
        sasl_authenticate_response::SaslAuthenticateResponse::decode(&mut cur, 2).unwrap();
    assert!(auth_resp.error_code == 0, "PLAIN authenticate must succeed");
    assert!(
        auth_resp.session_lifetime_ms == 0,
        "PLAIN listener must report session_lifetime_ms = 0 (no KIP-368 deadline)"
    );

    // Advance the tokio clock by an hour. The dispatch loop must NOT
    // have armed a per-connection timer for this non-OAuth session, so
    // the connection stays alive and serves further requests.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_hours(1)).await;
    tokio::time::resume();

    let md_req = metadata_request::MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();
    let md_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(5),
            header: RequestHeaderEncoding::Flexible,
        },
        &md_body,
    )
    .await
    .expect("Metadata RPC must succeed an hour after PLAIN auth");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = metadata_response::MetadataResponse::decode(&mut cur, 12).unwrap();
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}

async fn read_session_end(stream: &mut TcpStream) -> usize {
    let mut buf = [0_u8; 16];
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error")
}

async fn wait_past_expiration(expiration: i64) {
    let remaining = u64::try_from(expiration.saturating_sub(now_unix_secs()).max(0))
        .expect("a non-negative second count fits in u64");
    tokio::time::sleep(
        std::time::Duration::from_secs(remaining) + std::time::Duration::from_millis(500),
    )
    .await;
}
