//! Shared SASL/OAUTHBEARER plumbing (KIP-255 / RFC 7628).
//!
//! The module mints the bearer tokens, starts a broker whose only mechanism
//! is OAUTHBEARER, and drives the handshake, the authenticate round, and the
//! KIP-368 in-band re-authentication pair. The OAUTHBEARER suites in
//! `oauthbearer_tokens` and `oauthbearer_sessions` build on these.

use {
    crate::{
        harness::round_trip,
        kafka_wire::{decode_named, encode_named},
        support::{
            sasl::sasl_plaintext_mechanisms,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup,
            },
        },
    },
    base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64},
    krabka_broker::{Broker, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    krabka_security::SaslMechanism,
    std::{io, net::SocketAddr},
    tokio::net::TcpStream,
};

/// Build an unsecured JWS (`alg:none`) bearer token with a `sub` principal and
/// an `exp` in Unix seconds.
///
/// The signature segment is empty. This matches what the JVM
/// `OAuthBearerUnsecuredLoginCallbackHandler` produces.
pub fn unsecured_jws(sub: &str, exp_unix_secs: i64) -> String {
    let header = B64.encode(b"{\"alg\":\"none\"}");
    let claims = B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_unix_secs}}}").as_bytes());
    format!("{header}.{claims}.")
}

/// RFC 7628 client initial response that carries `token` with an empty
/// authzid.
pub fn oauthbearer_initial(token: &str) -> bytes::Bytes {
    bytes::Bytes::from(format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes())
}

pub fn now_unix_secs() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs(),
    )
    .expect("seconds fit in i64")
}

/// Start a single `SASL_PLAINTEXT` broker that enables only OAUTHBEARER, with
/// the given validator.
pub fn start_oauthbearer_broker(
    log_dir: &std::path::Path,
    validator: krabka_security::OAuthBearerValidator,
) -> impl std::future::Future<Output = krabka_broker::BrokerHandle> {
    start_oauthbearer_broker_with_cap(log_dir, validator, None)
}

/// Same as [`start_oauthbearer_broker`], but with the listener's KIP-368
/// `connections.max.reauth.ms` window set.
///
/// `Some(window)` clamps `session_lifetime_ms` to
/// `min(token_exp - now, window)`, and clamps the dispatch-loop re-auth
/// deadline to the same value. `None` leaves the session ending at the
/// token's own `exp`.
pub fn start_oauthbearer_broker_with_cap(
    log_dir: &std::path::Path,
    validator: krabka_security::OAuthBearerValidator,
    max_reauth: Option<krabka_units::Time>,
) -> impl std::future::Future<Output = krabka_broker::BrokerHandle> {
    let mut cfg =
        sasl_plaintext_mechanisms(log_dir.to_path_buf(), vec![SaslMechanism::OAuthBearer]);
    cfg.oauthbearer_validator = validator;
    cfg.connections_max_reauth = max_reauth;
    Box::pin(async move { Broker::start(cfg).await.expect("broker must start") })
}

/// Run a pre-auth `ApiVersions` and a `SaslHandshake`(OAUTHBEARER).
///
/// The helper asserts that the broker advertises OAUTHBEARER and that the
/// handshake succeeds.
pub async fn oauthbearer_handshake(
    stream: &mut TcpStream,
    corr: &mut i32,
) -> Result<(), io::Error> {
    let av_req = ApiVersionsRequest::default();
    let av_body = encode_named(&av_req, 0, "ApiVersions")?;
    let av = round_trip(
        stream,
        SocketExchangeSetup {
            correlation: WireCorrelationId(*corr),
            ..Default::default()
        },
        &av_body,
    )
    .await?;
    *corr += 1;
    decode_named::<ApiVersionsResponse>(&av, 0, "ApiVersions")?;

    let sh_req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let sh_body = encode_named(&sh_req, 1, "SaslHandshake")?;
    let sh = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(*corr),
            ..Default::default()
        },
        &sh_body,
    )
    .await?;
    *corr += 1;
    let sh_resp = decode_named::<SaslHandshakeResponse>(&sh, 1, "SaslHandshake")?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }
    if !sh_resp.mechanisms.iter().any(|m| m == "OAUTHBEARER") {
        return Err(io::Error::other("OAUTHBEARER not advertised"));
    }
    Ok(())
}

/// Send one `SaslAuthenticate v2` with `auth_bytes` and return the decoded
/// response.
pub async fn oauthbearer_authenticate(
    stream: &mut TcpStream,
    corr: &mut i32,
    auth_bytes: bytes::Bytes,
) -> Result<SaslAuthenticateResponse, io::Error> {
    let req = SaslAuthenticateRequest {
        auth_bytes,
        ..Default::default()
    };
    let body = encode_named(&req, 2, "SaslAuthenticate")?;
    let resp_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(*corr),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await?;
    *corr += 1;
    decode_named::<SaslAuthenticateResponse>(&resp_bytes, 2, "SaslAuthenticate")
}

/// Build an unsecured-JWS validator with zero clock skew and the default
/// `sub` principal claim.
///
/// Zero clock skew makes `exp` the exact session boundary. This validator is
/// the same as the one in the other OAuth tests, but it is pinned to zero
/// skew so that the assertion windows in the re-auth tests do not drift.
pub fn oauthbearer_zero_skew_validator() -> krabka_security::OAuthBearerValidator {
    krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
        allowable_clock_skew: krabka_units::secs(0),
        ..Default::default()
    })
}

/// Drive a `SASL_PLAINTEXT` OAUTHBEARER handshake to completion on a fresh
/// connection.
///
/// The function returns the still-open `TcpStream` and the
/// `session_lifetime_ms` field from the `SaslAuthenticateResponse`. Callers
/// can then assert on the timer and continue to use the connection. The
/// in-band re-auth scenarios do this.
///
/// `bearer_token` is the JWS string. For unsecured tests it is an `alg:none`
/// JWT with the wanted `sub` and `exp`. The function frames the RFC 7628
/// client-first message that wraps the token.
pub async fn drive_sasl_oauthbearer_session_open(
    addr: SocketAddr,
    bearer_token: &str,
) -> Result<(TcpStream, i64), io::Error> {
    let OauthbearerSession {
        mut stream,
        mut corr,
    } = oauthbearer_session_start(addr).await?;
    let auth =
        oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(bearer_token)).await?;
    if auth.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate error_code={} message={:?}",
            auth.error_code, auth.error_message
        )));
    }
    if !auth.auth_bytes.is_empty() {
        return Err(io::Error::other(
            "unexpected challenge — token was rejected",
        ));
    }
    Ok((stream, auth.session_lifetime_ms))
}

/// Drive a `SaslHandshake`(OAUTHBEARER) and `SaslAuthenticate` pair on an
/// already-authenticated stream, with a new bearer token.
///
/// This helper exercises KIP-368 in-band re-authentication. It returns
/// `Ok(())` when the broker accepts the new token. It returns `Err` when
/// either round reports a non-zero error code. The caller then asserts on
/// the rendered error text to tell 34 from 58.
pub async fn drive_inband_reauth(stream: &mut TcpStream, new_token: &str) -> Result<(), io::Error> {
    let handshake_request = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let handshake_body = encode_named(&handshake_request, 1, "SaslHandshake")?;
    let handshake_response_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(100),
            ..Default::default()
        },
        &handshake_body,
    )
    .await?;
    let handshake_response =
        decode_named::<SaslHandshakeResponse>(&handshake_response_bytes, 1, "SaslHandshake")?;
    if handshake_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslHandshake error_code={}",
            handshake_response.error_code
        )));
    }

    let authenticate_request = SaslAuthenticateRequest {
        auth_bytes: oauthbearer_initial(new_token),
        ..Default::default()
    };
    let authenticate_body = encode_named(&authenticate_request, 2, "SaslAuthenticate")?;
    let authenticate_response_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(101),
            header: RequestHeaderEncoding::Flexible,
        },
        &authenticate_body,
    )
    .await?;
    let authenticate_response = decode_named::<SaslAuthenticateResponse>(
        &authenticate_response_bytes,
        2,
        "SaslAuthenticate",
    )?;
    if authenticate_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslAuthenticate error_code={} message={:?}",
            authenticate_response.error_code, authenticate_response.error_message
        )));
    }
    Ok(())
}

/// Start the bearer-only broker while keeping its log directory alive.
pub async fn oauthbearer_fixture(
    validator: krabka_security::OAuthBearerValidator,
    cap: Option<krabka_units::Time>,
) -> (tempfile::TempDir, BrokerHandle, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let broker = start_oauthbearer_broker_with_cap(dir.path(), validator, cap).await;
    let addr = broker.listen_addr();
    (dir, broker, addr)
}

/// A connection that finished the pre-authentication rounds, and the next
/// correlation id to use on it.
pub struct OauthbearerSession {
    pub stream: TcpStream,
    pub corr: i32,
}

/// Open the pre-authentication rounds without consuming a bearer token.
pub async fn oauthbearer_session_start(addr: SocketAddr) -> Result<OauthbearerSession, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut corr = 1;
    oauthbearer_handshake(&mut stream, &mut corr).await?;
    Ok(OauthbearerSession { stream, corr })
}
