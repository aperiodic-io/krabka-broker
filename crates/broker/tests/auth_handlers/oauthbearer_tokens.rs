//! SASL/OAUTHBEARER token validation end-to-end.
//!
//! Two validators run against the live `SaslAuthenticate` path: the
//! unsecured `alg:none` JWS validator, and the signed validator whose key
//! set comes from an in-memory JWKS. Each is exercised on the accept path
//! and on the RFC 7628 two-round failure handshake.

use {
    crate::{
        harness::metadata_probe,
        oauthbearer::{
            now_unix_secs, oauthbearer_authenticate, oauthbearer_fixture, oauthbearer_initial,
            oauthbearer_session_start, start_oauthbearer_broker, unsecured_jws,
        },
    },
    assert2::assert,
    base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64},
    ring::{
        rand::SystemRandom,
        signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
    },
    std::io,
    tokio::net::TcpStream,
};

/// Happy path: a valid unsecured token authenticates in a single round.
///
/// A post-auth Metadata round-trip proves that the connection survived and
/// that the broker accepted the principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_happy_path() {
    let (_log_dir, handle, addr) =
        oauthbearer_fixture(krabka_security::OAuthBearerValidator::default(), None).await;

    let result: Result<(), io::Error> = async {
        let (mut stream, mut corr) = oauthbearer_session_start(addr).await?;

        let token = unsecured_jws("svc-account", now_unix_secs() + 3600);
        authenticate_success(
            &mut stream,
            &mut corr,
            &token,
            "unexpected challenge — token was rejected",
        )
        .await?;

        metadata_probe(&mut stream, corr).await
    }
    .await;

    handle.shutdown().await;
    result.expect("OAUTHBEARER session must succeed end-to-end");
}

/// Failure path: an expired token triggers the RFC 7628 two-round failure
/// handshake.
///
/// Round 1 returns the `invalid_token` JSON with `error_code = 0` and keeps
/// the connection open. Round 2, the client's `\x01` dummy, returns
/// `SASL_AUTHENTICATION_FAILED` (58).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_invalid_token_two_round_failure() {
    let log_dir = tempfile::tempdir().unwrap();
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: krabka_units::secs(0),
            ..Default::default()
        });
    let handle = start_oauthbearer_broker(log_dir.path(), validator).await;
    let addr = handle.listen_addr();

    let result: Result<(), io::Error> = async {
        let (mut stream, mut corr) = oauthbearer_session_start(addr).await?;

        // Expired token (exp an hour in the past, zero skew).
        let token = unsecured_jws("admin", now_unix_secs() - 3600);
        expect_invalid_token(&mut stream, &mut corr, &token).await?;
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("OAUTHBEARER failure handshake must complete");
}

/// Generate a fresh ES256 key and return `(key_pair, jwks_json)`.
///
/// The JWKS advertises the matching public key under `kid`.
fn es256_key(kid: &str) -> (ring::signature::EcdsaKeyPair, String) {
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let kp =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let point = kp.public_key().as_ref(); // 0x04 || x || y
    let jwks = format!(
        "{{\"keys\":[{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"{kid}\",\"x\":\"{}\",\"y\":\"{}\"}}]}}",
        B64.encode(&point[1..33]),
        B64.encode(&point[33..65]),
    );
    (kp, jwks)
}

/// Sign an ES256 JWS with `kp`, `kid` in the header and `claims` as the payload.
fn es256_token(kp: &ring::signature::EcdsaKeyPair, kid: &str, claims: &str) -> String {
    let header = B64.encode(format!("{{\"alg\":\"ES256\",\"kid\":\"{kid}\"}}").as_bytes());
    let payload = B64.encode(claims.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig = kp
        .sign(&ring::rand::SystemRandom::new(), signing_input.as_bytes())
        .unwrap();
    format!("{signing_input}.{}", B64.encode(sig.as_ref()))
}

/// A `Signed` validator whose key set comes from `jwks_json`.
///
/// The test needs no network fetch.
fn signed_validator(jwks_json: &str) -> krabka_security::OAuthBearerValidator {
    let handle = krabka_security::JwksHandle::new(
        krabka_security::Jwks::from_json(jwks_json, false).unwrap(),
    );
    krabka_security::OAuthBearerValidator::Signed(krabka_security::SignedJwsValidator::new(handle))
}

/// Happy path: a real signed ES256 token, verified against an in-memory JWKS,
/// authenticates in a single round.
///
/// This proves that the `Signed` validator is wired through the live
/// `SaslAuthenticate` path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_happy_path() {
    let (kp, jwks) = es256_key("k1");
    let (_log_dir, handle, addr) = oauthbearer_fixture(signed_validator(&jwks), None).await;

    let result: Result<(), io::Error> = async {
        let (mut stream, mut corr) = oauthbearer_session_start(addr).await?;

        let claims = format!(
            "{{\"sub\":\"svc-account\",\"exp\":{}}}",
            now_unix_secs() + 3600
        );
        let token = es256_token(&kp, "k1", &claims);
        authenticate_success(
            &mut stream,
            &mut corr,
            &token,
            "signed success round must be empty",
        )
        .await?;

        // Post-auth Metadata proves the connection survived authentication.
        metadata_probe(&mut stream, corr).await
    }
    .await;

    handle.shutdown().await;
    result.expect("signed OAUTHBEARER session must succeed end-to-end");
}

/// Failure path: a token signed by a *different* key than the JWKS advertises
/// triggers the RFC 7628 two-round failure handshake.
///
/// Round 1 carries the `invalid_token` JSON. Round 2, the `\x01` dummy,
/// returns 58.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_wrong_key_two_round_failure() {
    // JWKS advertises key A's public key; the token is signed by key B.
    let (_kp_a, jwks_a) = es256_key("k1");
    let (kp_b, _jwks_b) = es256_key("k1");
    let (_log_dir, handle, addr) = oauthbearer_fixture(signed_validator(&jwks_a), None).await;

    let result: Result<(), io::Error> = async {
        let (mut stream, mut corr) = oauthbearer_session_start(addr).await?;

        let claims = format!("{{\"sub\":\"admin\",\"exp\":{}}}", now_unix_secs() + 3600);
        let token = es256_token(&kp_b, "k1", &claims);
        expect_invalid_token(&mut stream, &mut corr, &token).await?;
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("signed OAUTHBEARER failure handshake must complete");
}

async fn expect_invalid_token(
    stream: &mut TcpStream,
    corr: &mut i32,
    token: &str,
) -> io::Result<()> {
    let round1 = oauthbearer_authenticate(stream, corr, oauthbearer_initial(token)).await?;
    assert!(round1.error_code == 0, "round 1 must not close yet");
    assert!(
        &round1.auth_bytes[..] == br#"{"status":"invalid_token"}"#,
        "round 1 must carry the RFC 7628 error JSON"
    );

    // The client's `\x01` dummy → SASL_AUTHENTICATION_FAILED (58).
    let round2 = oauthbearer_authenticate(stream, corr, bytes::Bytes::from_static(&[1u8])).await?;
    assert!(round2.error_code == 58, "round 2 must fail the connection");
    Ok(())
}

async fn authenticate_success(
    stream: &mut TcpStream,
    corr: &mut i32,
    token: &str,
    unexpected_challenge: &str,
) -> Result<(), io::Error> {
    let auth = oauthbearer_authenticate(stream, corr, oauthbearer_initial(token)).await?;
    if auth.error_code != 0 {
        return Err(io::Error::other(format!(
            "authenticate failed: code={} msg={:?}",
            auth.error_code, auth.error_message,
        )));
    }
    if !auth.auth_bytes.is_empty() {
        return Err(io::Error::other(unexpected_challenge.to_owned()));
    }
    Ok(())
}
