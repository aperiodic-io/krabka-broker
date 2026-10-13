//! SASL/OAUTHBEARER (KIP-255, RFC 7628) and the KIP-368 re-auth arm.
//!
//! This module binds OAuth credential expiry to the KIP-368 session lifetime
//! and implements the two-message failure handshake RFC 7628 requires.

use {
    super::{
        response::{fail_authenticate, fail_authenticate_with, sasl_ok},
        state::{ConnectionAuth, SaslExchange, principal_change_message},
    },
    krabka_protocol::owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    },
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::atomic::{AtomicI64, AtomicU64, Ordering},
};

/// SASL/OAUTHBEARER `SaslAuthenticate` handler (KIP-255 / RFC 7628).
///
/// Round 1 (client initial response):
///   - `auth_bytes` is `n,,\x01auth=Bearer <token>\x01\x01`. The handler
///     parses the bearer token and validates it with `validator` against
///     the starting clock observation, then checks the completion snapshot.
///     On success, `auth` moves to `Authenticated` and the response
///     carries empty `auth_bytes` with `error_code = 0`. That is a
///     single-round success.
///   - On any parse or validation failure, the handler returns the RFC 7628
///     `{"status":"invalid_token"}` JSON in `auth_bytes` with
///     `error_code = 0`, keeps the connection open, and moves to
///     `OAuthBearerFailed`.
///
/// Round 2 runs only after a failure. The JVM client replies to the error JSON
/// with a single `\x01`. The handler returns `SASL_AUTHENTICATION_FAILED`
/// (58), and the dispatcher closes the connection.
#[cfg(test)]
pub async fn handle_authenticate_oauthbearer(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    clock_ms: impl Fn() -> i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    handle_authenticate_oauthbearer_inner(
        req,
        auth,
        validator,
        None,
        clock_ms,
        max_session_lifetime,
    )
    .await
}

pub async fn handle_authenticate_oauthbearer_with_jwks_cache(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    cache_generation: &AtomicU64,
    last_successful_fetch_ms: &AtomicI64,
    clock_ms: impl Fn() -> i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    handle_authenticate_oauthbearer_inner(
        req,
        auth,
        validator,
        Some(JwksCache {
            generation: cache_generation,
            last_successful_fetch_ms,
        }),
        clock_ms,
        max_session_lifetime,
    )
    .await
}

async fn handle_authenticate_oauthbearer_inner(
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    validator: &krabka_security::OAuthBearerValidator,
    jwks_cache: Option<JwksCache<'_>>,
    clock_ms: impl Fn() -> i64,
    max_session_lifetime: Option<Time>,
) -> SaslAuthenticateResponse {
    let (mechanism, previous_name) = match auth {
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearer,
            mechanism,
            ..
        } => (*mechanism, None),
        ConnectionAuth::Reauthenticating {
            previous,
            exchange: SaslExchange::OAuthBearer,
            ..
        } => (previous.mechanism, Some(previous.principal.name.clone())),
        // The client's final message completes the RFC 7628 failure handshake.
        ConnectionAuth::Negotiating {
            exchange: SaslExchange::OAuthBearerFailed,
            ..
        } => return oauthbearer_failure(),
        ConnectionAuth::Reauthenticating {
            exchange: SaslExchange::OAuthBearerFailed,
            ..
        } => {
            let previous =
                super::state::begin_reauth(auth).expect("matched Reauthenticating above");
            return super::state::finish_reauth(auth, previous, oauthbearer_failure());
        }
        _ => return fail_authenticate("not in oauthbearer negotiation"),
    };
    let ValidatedBearer {
        outcome,
        completed_ms: now_ms,
    } = match validate_bearer(&req.auth_bytes, validator, jwks_cache, &clock_ms).await {
        Ok(validated) => validated,
        Err(reason) => return reject_oauthbearer(auth, mechanism, reason),
    };
    let principal_matches = previous_name
        .as_ref()
        .is_none_or(|name| *name == outcome.principal.name);
    let krabka_verified::OAuthSessionDecision::Admit {
        session_lifetime_ms,
        effective_expires_at_ms,
    } = oauth_session_decision(
        outcome.expires_at_ms,
        now_ms,
        max_session_lifetime,
        previous_name.is_some(),
        principal_matches,
    )
    else {
        if let Some(previous) = previous_name {
            if !principal_matches {
                tracing::debug!(
                    previous = %previous,
                    attempted = %outcome.principal.name,
                    "OAUTHBEARER re-auth principal mismatch"
                );
                return fail_authenticate_with(principal_change_message(
                    &previous,
                    &outcome.principal.name,
                ));
            }
            return fail_authenticate("OAUTHBEARER re-auth rejected an invalid session lifetime");
        }
        return reject_oauthbearer(auth, mechanism, "invalid OAuth session lifetime");
    };
    // Anchor expiry to the clamped lifetime reported to the client, so the
    // dispatcher's deadline also respects the broker's session cap.
    *auth = ConnectionAuth::Authenticated {
        principal: outcome.principal,
        mechanism,
        expires_at_ms: Some(effective_expires_at_ms),
        authenticated_via_token: false,
    };
    sasl_ok(bytes::Bytes::new(), session_lifetime_ms)
}

fn oauth_session_decision(
    expires_at_ms: Option<i64>,
    now_ms: i64,
    max_session_lifetime: Option<Time>,
    reauthentication: bool,
    principal_matches: bool,
) -> krabka_verified::OAuthSessionDecision {
    krabka_verified::oauth_session_admission(krabka_verified::OAuthSessionFacts {
        expiry: if expires_at_ms.is_some() {
            krabka_verified::OAuthExpiryPresence::Present
        } else {
            krabka_verified::OAuthExpiryPresence::Missing
        },
        token_expires_at_ms: expires_at_ms.unwrap_or(0),
        now_ms,
        cap: if max_session_lifetime.is_some() {
            krabka_verified::OAuthSessionCap::Enabled
        } else {
            krabka_verified::OAuthSessionCap::Disabled
        },
        cap_ms: max_session_lifetime.map_or(0, krabka_units::convert::TimeExt::millis_i64),
        authentication: if reauthentication {
            krabka_verified::OAuthAuthenticationKind::Reauthentication
        } else {
            krabka_verified::OAuthAuthenticationKind::Initial
        },
        principal: if principal_matches {
            krabka_verified::OAuthPrincipalMatch::Matches
        } else {
            krabka_verified::OAuthPrincipalMatch::Differs
        },
    })
}

fn reject_oauthbearer(
    auth: &mut ConnectionAuth,
    mechanism: krabka_security::SaslMechanism,
    reason: &'static str,
) -> SaslAuthenticateResponse {
    if let ConnectionAuth::Reauthenticating { exchange, .. } = auth {
        tracing::debug!(reason, "OAUTHBEARER re-auth token rejected");
        *exchange = SaslExchange::OAuthBearerFailed;
    } else {
        tracing::debug!(reason, "OAUTHBEARER token rejected");
        *auth = ConnectionAuth::Negotiating {
            mechanism,
            exchange: SaslExchange::OAuthBearerFailed,
            pending_token_expiry_ms: None,
        };
    }
    sasl_ok(krabka_security::invalid_token_json().into_bytes(), 0)
}

/// `OAuthBearerSaslServer` throws `SaslAuthenticationException(errorMessage)`
/// on the client's `\x01` after an error challenge, so the failure text is the
/// same JSON the challenge carried.
fn oauthbearer_failure() -> SaslAuthenticateResponse {
    fail_authenticate_with(krabka_security::invalid_token_json())
}

/// The JWKS refresher's state that a signed validation is checked against.
#[derive(Clone, Copy)]
struct JwksCache<'a> {
    /// Bumped by each cache change.
    generation: &'a AtomicU64,
    /// When the cache last refreshed successfully.
    last_successful_fetch_ms: &'a AtomicI64,
}

/// A client response that validated: the outcome and the clock reading at
/// which validation completed.
#[derive(Debug, PartialEq)]
struct ValidatedBearer {
    outcome: krabka_security::AuthOutcome,
    completed_ms: i64,
}

/// Parses and validates an OAUTHBEARER client initial response. The authzid,
/// when present, must equal the token principal, as RFC 7628 and Kafka
/// require.
async fn validate_bearer(
    auth_bytes: &[u8],
    validator: &krabka_security::OAuthBearerValidator,
    jwks_cache: Option<JwksCache<'_>>,
    clock_ms: impl Fn() -> i64,
) -> Result<ValidatedBearer, &'static str> {
    let now_ms = clock_ms();
    let parsed = krabka_security::parse_client_initial_response(auth_bytes)
        .map_err(|_| "malformed OAUTHBEARER client response")?;
    let cache_guard = match (validator, jwks_cache) {
        (
            krabka_security::OAuthBearerValidator::Signed(signed),
            Some(JwksCache {
                generation,
                last_successful_fetch_ms: last_successful,
            }),
        ) => {
            let generation_before = generation.load(Ordering::Acquire);
            let last_successful_fetch_ms = last_successful.load(Ordering::Acquire);
            let generation_after = generation.load(Ordering::Acquire);
            let (expiry_enabled, expiry_ms) = signed
                .cache_expiry
                .map_or((false, 0), |expiry| (true, expiry.millis_i64()));
            let facts = krabka_verified::JwksCacheFacts {
                generation_before,
                generation_after,
                last_successful_fetch_ms,
                now_ms,
                expiry_enabled,
                expiry_ms,
            };
            if krabka_verified::jwks_cache_admission(facts)
                != krabka_verified::JwksCacheDecision::Admit
            {
                return Err("JWKS cache is stale or changing");
            }
            Some((generation, facts))
        }
        _ => None,
    };
    let outcome = validator
        .validate(&parsed.token, now_ms)
        .await
        .map_err(|_| "token validation failed")?;
    let completed_ms = clock_ms();
    let completed_cache = cache_guard.map(|(generation, mut facts)| {
        facts.generation_after = generation.load(Ordering::Acquire);
        facts
    });
    if !krabka_verified::oauth::oauth_validation_admission(now_ms, completed_ms, completed_cache) {
        return Err("validation snapshot is stale or clock moved backwards");
    }
    // Expiry during validation is a rejected token, so both authentication
    // arms must send the RFC 7628 challenge before the client's final reply.
    if outcome
        .expires_at_ms
        .is_some_and(|expiry| expiry <= completed_ms)
    {
        return Err("OAuth credential expired during validation");
    }
    if let Some(authzid) = parsed.authzid
        && authzid != outcome.principal.name
    {
        return Err("authzid does not match token principal");
    }
    Ok(ValidatedBearer {
        outcome,
        completed_ms,
    })
}

#[cfg(test)]
mod tests;
