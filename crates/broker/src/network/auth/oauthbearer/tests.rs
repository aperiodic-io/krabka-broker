//! Round-trip tests for the OAUTHBEARER handler, including KIP-368 re-auth.

use {
    super::*,
    crate::network::auth::test_support::{
        assert_failed_authenticate_response, assert_success_authenticate_response,
        oauth_reauthenticating,
    },
    assert2::{assert, check},
    krabka_security::{Principal, SaslMechanism},
    krabka_units::{millis, secs},
};

fn unsecured_token(sub: &str, exp_s: i64) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    format!(
        "{}.{}.",
        B64.encode(b"{\"alg\":\"none\"}"),
        B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_s}}}").as_bytes())
    )
}

fn oauthbearer_client_response(token: &str) -> SaslAuthenticateRequest {
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(
            format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
        ),
        ..Default::default()
    }
}

fn signed_validator(cache_expiry: Option<Time>) -> krabka_security::OAuthBearerValidator {
    let mut validator =
        krabka_security::SignedJwsValidator::new(krabka_security::JwksHandle::default());
    validator.cache_expiry = cache_expiry;
    krabka_security::OAuthBearerValidator::Signed(validator)
}

#[test]
fn oauth_session_adapter_rejects_invalid_or_unrepresentable_lifetimes() {
    for decision in [
        oauth_session_decision(None, 100, None, false, true),
        oauth_session_decision(Some(100), 100, None, false, true),
        oauth_session_decision(Some(i64::MAX), -1, None, false, true),
        oauth_session_decision(Some(200), 100, Some(secs(0)), false, true),
        oauth_session_decision(Some(200), 100, None, true, false),
    ] {
        assert!(decision == krabka_verified::OAuthSessionDecision::Reject);
    }
}

#[test]
fn oauth_session_adapter_returns_the_exact_effective_expiry() {
    assert!(
        oauth_session_decision(Some(200), 100, Some(millis(40)), false, true)
            == krabka_verified::OAuthSessionDecision::Admit {
                session_lifetime_ms: 40,
                effective_expires_at_ms: 140,
            }
    );
}

async fn reauthenticate(
    validator: &krabka_security::OAuthBearerValidator,
    now_ms: i64,
    token: &str,
) -> (ConnectionAuth, SaslAuthenticateResponse) {
    let mut auth = oauth_reauthenticating("alice", now_ms + 1_000);
    let response = handle_authenticate_oauthbearer(
        &oauthbearer_client_response(token),
        &mut auth,
        validator,
        || now_ms,
        None,
    )
    .await;
    (auth, response)
}

#[tokio::test]
async fn signed_validator_fails_closed_for_stale_or_changing_jwks_cache() {
    let (now_ms, request, validator, generation, last_successful) = signed_cache_fixture(1);

    let changing = validate_bearer(
        &request.auth_bytes,
        &validator,
        Some((&generation, &last_successful)),
        || now_ms,
    )
    .await;
    assert!(changing == Err("JWKS cache is stale or changing"));

    generation.store(2, Ordering::Release);
    last_successful.store(0, Ordering::Release);
    let never_fetched = validate_bearer(
        &request.auth_bytes,
        &validator,
        Some((&generation, &last_successful)),
        || now_ms,
    )
    .await;
    assert!(never_fetched == Err("JWKS cache is stale or changing"));

    last_successful.store(now_ms - 1_001, Ordering::Release);
    let stale = validate_bearer(
        &request.auth_bytes,
        &validator,
        Some((&generation, &last_successful)),
        || now_ms,
    )
    .await;
    assert!(stale == Err("JWKS cache is stale or changing"));
}

#[tokio::test]
async fn signed_validator_uses_a_fresh_stable_jwks_cache() {
    let (now_ms, request, validator, generation, last_successful) = signed_cache_fixture(2);

    let result = validate_bearer(
        &request.auth_bytes,
        &validator,
        Some((&generation, &last_successful)),
        || now_ms,
    )
    .await;

    assert!(result == Err("token validation failed"));
}

#[tokio::test]
async fn oauthbearer_valid_token_authenticates() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let token = unsecured_token("svc-account", 1_000_000_900); // exp seconds → future of now
    let mut auth = oauth_pending();
    let resp = handle_authenticate_oauthbearer(
        &oauthbearer_client_response(&token),
        &mut auth,
        &validator,
        || now_ms,
        None,
    )
    .await;
    assert_success_authenticate_response(&resp, b"", 900_000);
    let p = auth.principal().expect("authenticated");
    assert!(p.name == "svc-account");
    assert!(p.auth_method == krabka_security::AuthMethod::SaslOAuthBearer);
    match auth {
        ConnectionAuth::Authenticated {
            expires_at_ms,
            authenticated_via_token,
            ..
        } => {
            assert!(expires_at_ms == Some(1_000_000_900_000));
            assert!(!authenticated_via_token);
        }
        _ => panic!("expected authenticated state"),
    }
}

#[tokio::test]
async fn oauthbearer_invalid_token_returns_error_json_then_fails_on_dummy() {
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: secs(0),
            ..Default::default()
        });
    let now_ms = 5_000_000_000_000;
    // exp far in the past → expired.
    let token = unsecured_token("admin", 1_000_000_000);
    let mut auth = oauth_pending();
    // Round 1: rejected → error JSON, error_code 0, connection stays open.
    let resp = handle_authenticate_oauthbearer(
        &oauthbearer_client_response(&token),
        &mut auth,
        &validator,
        || now_ms,
        None,
    )
    .await;
    assert_invalid_token_challenge(&resp);
    assert!(matches!(auth, ConnectionAuth::Negotiating {
        exchange: SaslExchange::OAuthBearerFailed,
        ..
    }));
    // Round 2: the client's `\x01` dummy → SASL_AUTHENTICATION_FAILED (58).
    let dummy = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(&[1u8]),
        ..Default::default()
    };
    let resp2 =
        handle_authenticate_oauthbearer(&dummy, &mut auth, &validator, || now_ms, None).await;
    assert_failed_authenticate_response(&resp2, Some(r#"{"status":"invalid_token"}"#));
    assert!(!auth.is_authenticated());
}

#[tokio::test]
async fn oauthbearer_malformed_response_returns_error_json() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let mut auth = oauth_pending();
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(b"not-a-valid-gs2-message"),
        ..Default::default()
    };
    let resp =
        handle_authenticate_oauthbearer(&req, &mut auth, &validator, || 1_000_000_000_000, None)
            .await;
    assert_invalid_token_challenge(&resp);
}

#[tokio::test]
async fn oauthbearer_authzid_mismatch_fails() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let token = unsecured_token("alice", 1_000_000_900);
    let mut auth = oauth_pending();
    // authzid "bob" != token principal "alice".
    let req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(
            format!("n,a=bob,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes(),
        ),
        ..Default::default()
    };
    let resp = handle_authenticate_oauthbearer(&req, &mut auth, &validator, || now_ms, None).await;
    assert_invalid_token_challenge(&resp);
    assert!(!auth.is_authenticated());
}

#[tokio::test]
async fn authenticate_during_reauth_same_principal_transitions_back_to_authenticated() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    // Token's exp is in seconds; the validator computes expires_at_ms = exp * 1000.
    let new_token_exp_seconds: i64 = 1_000_000_900;
    let new_token_exp_millis: i64 = new_token_exp_seconds * 1000;
    let token = unsecured_token("alice", new_token_exp_seconds);
    let (auth, resp) = reauthenticate(&validator, now_ms, &token).await;
    assert_success_authenticate_response(&resp, b"", new_token_exp_millis - now_ms);
    assert!(matches!(auth, ConnectionAuth::Authenticated {
        mechanism: SaslMechanism::OAuthBearer,
        expires_at_ms: Some(_),
        ..
    }));
    if let ConnectionAuth::Authenticated {
        principal,
        expires_at_ms,
        ..
    } = &auth
    {
        assert!(principal.name == "alice");
        assert!(*expires_at_ms == Some(new_token_exp_millis));
    } else {
        panic!("expected Authenticated");
    }
}

#[tokio::test]
async fn authenticate_during_reauth_different_principal_rejected_with_sasl_auth_failed() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    // Token belongs to "bob", but the prior session is "alice".
    let token = unsecured_token("bob", 1_000_000_900);
    let (auth, resp) = reauthenticate(&validator, now_ms, &token).await;
    // Kafka's `ensurePrincipalUnchanged` message.
    assert_failed_authenticate_response(
        &resp,
        Some("Cannot change principals during re-authentication from User.alice: User.bob"),
    );
    // Connection remained in Reauthenticating (dispatch will close).
    assert!(matches!(auth, ConnectionAuth::Reauthenticating { .. }));
}

/// A rejected re-authentication token gets the same RFC 7628 error challenge
/// as an initial one, and the client's `\x01` then fails the exchange with
/// that JSON as the message, leaving the previous session in place while the
/// dispatcher closes the connection.
#[tokio::test]
async fn a_rejected_reauth_token_gets_the_error_challenge_then_fails_with_its_json() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_000_000;
    let mut auth = oauth_reauthenticating("alice", now_ms + 1_000);
    let garbage = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(b"not a client response"),
        ..Default::default()
    };
    let challenge =
        handle_authenticate_oauthbearer(&garbage, &mut auth, &validator, || now_ms, None).await;
    assert_success_authenticate_response(&challenge, br#"{"status":"invalid_token"}"#, 0);

    let dummy = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from_static(&[1u8]),
        ..Default::default()
    };
    let failure =
        handle_authenticate_oauthbearer(&dummy, &mut auth, &validator, || now_ms, None).await;
    assert_failed_authenticate_response(&failure, Some(r#"{"status":"invalid_token"}"#));
    assert!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));
}

// KIP-368 ceiling: the server-side
// `connections.max.reauth.ms` cap clamps both the response field and
// the `Authenticated.expires_at_ms` stored on the connection.

#[tokio::test]
async fn handle_authenticate_oauthbearer_applies_max_session_lifetime_cap() {
    let validator =
        krabka_security::OAuthBearerValidator::Unsecured(krabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: secs(0),
            ..Default::default()
        });
    let now_ms = 1_000_000_i64;
    let exp_ms = now_ms + 60_000; // token good for 60s
    let token = unsecured_token("alice", exp_ms / 1000);
    let req = oauthbearer_client_response(&token);

    // (server cap in seconds, expected session lifetime in ms). The
    // stored expires_at_ms must reflect the clamped value too, not the
    // raw token exp.
    let cases = [
        (Some(secs(30)), 30_000_i64), // cap below the token's 60s exp → clamped
        (None, 60_000),               // unset cap → raw token exp
        (Some(secs(600)), 60_000),    // cap above exp → no effect
    ];
    for (cap, want_lifetime_ms) in cases {
        let mut auth = oauth_pending();
        let resp =
            handle_authenticate_oauthbearer(&req, &mut auth, &validator, || now_ms, cap).await;
        check!(resp.error_code == 0, "cap {cap:?}");
        check!(resp.session_lifetime_ms == want_lifetime_ms, "cap {cap:?}");
        match auth {
            ConnectionAuth::Authenticated { expires_at_ms, .. } => {
                assert!(
                    expires_at_ms == Some(now_ms + want_lifetime_ms),
                    "cap {cap:?}: expires_at_ms must reflect the clamped value"
                );
            }
            _ => panic!("cap {cap:?}: expected Authenticated"),
        }
    }
}

#[tokio::test]
async fn handle_authenticate_oauthbearer_rejects_a_zero_session_cap() {
    let validator = krabka_security::OAuthBearerValidator::default();
    let now_ms = 1_000_000_i64;
    let token = unsecured_token("alice", 2_000);
    let mut auth = oauth_pending();

    let response = handle_authenticate_oauthbearer(
        &oauthbearer_client_response(&token),
        &mut auth,
        &validator,
        || now_ms,
        Some(secs(0)),
    )
    .await;

    assert!(response.error_code == 0);
    assert!(response.session_lifetime_ms == 0);
    assert!(response.auth_bytes == bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#));
    assert!(matches!(auth, ConnectionAuth::Negotiating {
        exchange: SaslExchange::OAuthBearerFailed,
        ..
    }));
}

mod completion;

fn oauth_pending() -> ConnectionAuth {
    ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::OAuthBearer,
        exchange: SaslExchange::OAuthBearer,
        pending_token_expiry_ms: None,
    }
}

fn assert_invalid_token_challenge(response: &SaslAuthenticateResponse) {
    let expected = SaslAuthenticateResponse {
        error_code: 0,
        error_message: None,
        auth_bytes: bytes::Bytes::from_static(br#"{"status":"invalid_token"}"#),
        session_lifetime_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(response == &expected);
}

fn signed_cache_fixture(
    generation: u64,
) -> (
    i64,
    SaslAuthenticateRequest,
    krabka_security::OAuthBearerValidator,
    AtomicU64,
    AtomicI64,
) {
    let now_ms = 1_000_000;
    let request = oauthbearer_client_response(&unsecured_token("alice", 2_000));
    let validator = signed_validator(Some(secs(1)));
    (
        now_ms,
        request,
        validator,
        AtomicU64::new(generation),
        AtomicI64::new(now_ms),
    )
}
