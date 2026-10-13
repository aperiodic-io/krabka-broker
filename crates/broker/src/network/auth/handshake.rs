//! `SaslHandshake` (17): mechanism negotiation and the KIP-368 re-auth entry.
//!
//! This module owns the first request of every SASL connection. It answers
//! with the enabled mechanism list, moves a fresh connection to `Negotiating`,
//! and turns a handshake that arrives on an already-authenticated connection
//! into the `Reauthenticating` state that KIP-368 defines -- when Kafka would.

use {
    super::state::{AuthenticatedSnapshot, ConnectionAuth, SaslExchange},
    crate::codes::{ILLEGAL_SASL_STATE, UNSUPPORTED_SASL_MECHANISM},
    krabka_protocol::owned::{
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    krabka_security::SaslMechanism,
};

/// Kafka's `KafkaChannel.MIN_REAUTH_INTERVAL_ONE_SECOND_NANOS`, in ms: a
/// connection may not start a re-authentication more than once a second.
const MIN_REAUTH_INTERVAL_MS: i64 = 1_000;

/// What the dispatcher does with a `SaslHandshake`: the response to send, and
/// whether to close the connection once it is written.
#[derive(Debug)]
pub struct HandshakeOutcome {
    pub response: SaslHandshakeResponse,
    pub close_after: bool,
}

/// The per-connection clock KIP-368 re-authentication is rate-limited by.
pub struct ReauthClock<'a> {
    /// Unix epoch ms now.
    pub now_ms: i64,
    /// When this connection last started a re-authentication, if ever.
    pub last_start_ms: &'a mut Option<i64>,
}

/// Handles `SaslHandshake` (`api_key` 17), as Kafka's
/// `SaslServerAuthenticator.handleHandshakeRequest` and
/// `KafkaChannel.maybeBeginServerReauthentication` do.
///
/// Before authentication, an enabled mechanism moves `auth` to `Negotiating`
/// and answers `NONE` with the enabled list. Any other mechanism answers
/// [`UNSUPPORTED_SASL_MECHANISM`] (33) with the enabled list, and the
/// connection closes.
///
/// On an authenticated connection, a handshake starts a KIP-368
/// re-authentication only when the session has an expiry and the last one
/// started a second or more ago. Otherwise Kafka hands the frame to
/// `KafkaApis`, which answers [`ILLEGAL_SASL_STATE`] (34) with no mechanisms
/// and keeps the session and the connection. A re-authentication that names
/// the session's own mechanism moves to `Reauthenticating`; one that names
/// another enabled mechanism answers `NONE` and moves to `ReauthBadMechanism`,
/// which fails the connection on the next frame; an unsupported one answers
/// 33 and closes.
pub fn handle_handshake(
    req: &SaslHandshakeRequest,
    auth: &mut ConnectionAuth,
    enabled: &[SaslMechanism],
    clock: &mut ReauthClock<'_>,
) -> HandshakeOutcome {
    let enabled_names: Vec<String> = enabled.iter().map(|m| m.wire_name().to_string()).collect();
    let requested = SaslMechanism::from_wire(&req.mechanism).filter(|m| enabled.contains(m));
    let answer = |error_code, mechanisms, close_after| HandshakeOutcome {
        response: SaslHandshakeResponse {
            error_code,
            mechanisms,
            ..Default::default()
        },
        close_after,
    };

    match auth {
        ConnectionAuth::Authenticated {
            mechanism: current,
            expires_at_ms,
            ..
        } => {
            let recently_started = clock
                .last_start_ms
                .is_some_and(|last| clock.now_ms.saturating_sub(last) < MIN_REAUTH_INTERVAL_MS);
            if expires_at_ms.is_none() || recently_started {
                tracing::debug!(
                    requested = %req.mechanism,
                    "SaslHandshake on an authenticated connection that cannot re-authenticate now"
                );
                return answer(ILLEGAL_SASL_STATE, Vec::new(), false);
            }
            *clock.last_start_ms = Some(clock.now_ms);
            let current = *current;
            match requested {
                Some(m) if m == current => {
                    let ConnectionAuth::Authenticated {
                        principal,
                        mechanism,
                        expires_at_ms,
                        authenticated_via_token,
                    } = std::mem::replace(auth, ConnectionAuth::Anonymous)
                    else {
                        unreachable!("matched Authenticated above");
                    };
                    *auth = ConnectionAuth::Reauthenticating {
                        previous: AuthenticatedSnapshot {
                            principal,
                            mechanism,
                            expires_at_ms,
                            authenticated_via_token,
                        },
                        exchange: exchange_for_mechanism(m),
                        // A fresh exchange; a token-SCRAM round 1 populates
                        // it during re-auth exactly as during initial auth.
                        pending_token_expiry_ms: None,
                    };
                    answer(0, enabled_names, false)
                }
                Some(_) => {
                    tracing::debug!(
                        "SASL mechanism '{}' requested by client is not supported for \
                         re-authentication of mechanism '{}'",
                        req.mechanism,
                        current.wire_name()
                    );
                    *auth = ConnectionAuth::ReauthBadMechanism;
                    answer(0, enabled_names, false)
                }
                None => answer(UNSUPPORTED_SASL_MECHANISM, enabled_names, true),
            }
        }
        ConnectionAuth::Anonymous => {
            let Some(m) = requested else {
                tracing::debug!(requested = %req.mechanism, "SaslHandshake: unsupported mechanism");
                return answer(UNSUPPORTED_SASL_MECHANISM, enabled_names, true);
            };
            *auth = ConnectionAuth::Negotiating {
                mechanism: m,
                exchange: exchange_for_mechanism(m),
                // Fresh handshake; the token-fallback in
                // `handle_authenticate_scram` may populate this later during
                // SCRAM round 1.
                pending_token_expiry_ms: None,
            };
            answer(0, enabled_names, false)
        }
        // A handshake already chose a mechanism: Kafka accepts only
        // `SaslAuthenticate` now. The request gate refuses the frame before
        // it gets here; answer the same way if a caller does not gate.
        ConnectionAuth::Negotiating { .. }
        | ConnectionAuth::Reauthenticating { .. }
        | ConnectionAuth::ReauthBadMechanism => answer(ILLEGAL_SASL_STATE, Vec::new(), true),
    }
}

/// Builds the per-mechanism `SaslExchange` initial state.
///
/// It is separate from `handle_handshake` so that the initial-auth path and
/// the re-auth path construct the state in the same way.
fn exchange_for_mechanism(m: SaslMechanism) -> SaslExchange {
    match m {
        SaslMechanism::Plain => SaslExchange::Plain,
        // SCRAM exchange is built lazily on the first SaslAuthenticate
        // round, once the username is known. Until then we sit in
        // `ScramPending`. SHA-256 and SHA-512 share the same dispatch
        // state; the mechanism is preserved on the outer `Negotiating` /
        // `Reauthenticating` variant.
        SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => SaslExchange::ScramPending,
        // The token arrives in the first SaslAuthenticate; no pre-built
        // state needed.
        SaslMechanism::OAuthBearer => SaslExchange::OAuthBearer,
        // GSSAPI exchange is built lazily on the first SaslAuthenticate
        // round, once the client's AP-REQ arrives (we defer reading the
        // keytab until then). Until then we sit in `GssapiPending`.
        SaslMechanism::Gssapi => SaslExchange::GssapiPending,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::network::auth::test_support::authenticated as test_authenticated,
        assert2::assert,
    };

    const NOW_MS: i64 = 10_000_000;

    fn authenticated(expires_at_ms: Option<i64>) -> ConnectionAuth {
        test_authenticated(
            "alice",
            krabka_security::AuthMethod::SaslOAuthBearer,
            SaslMechanism::OAuthBearer,
            expires_at_ms,
            false,
        )
    }

    /// The state a handshake leaves, reduced to what the table compares.
    #[derive(Debug, PartialEq, Eq)]
    enum After {
        Anonymous,
        Negotiating(SaslMechanism),
        Reauthenticating(SaslMechanism),
        ReauthBadMechanism,
        Authenticated,
    }

    fn after(auth: &ConnectionAuth) -> After {
        match auth {
            ConnectionAuth::Anonymous => After::Anonymous,
            ConnectionAuth::Negotiating { mechanism, .. } => After::Negotiating(*mechanism),
            ConnectionAuth::Reauthenticating { previous, .. } => {
                After::Reauthenticating(previous.mechanism)
            }
            ConnectionAuth::ReauthBadMechanism => After::ReauthBadMechanism,
            ConnectionAuth::Authenticated { .. } => After::Authenticated,
        }
    }

    /// Rows of (case, starting state, last re-auth start, requested
    /// mechanism) and expected (error code, mechanism list returned, close,
    /// next state, last re-auth start afterwards), after Kafka's
    /// `SaslServerAuthenticator` and `KafkaChannel`.
    #[test]
    fn handshake_follows_kafkas_initial_and_reauthentication_rules() {
        type Case = (
            &'static str,
            fn() -> ConnectionAuth,
            Option<i64>,
            &'static str,
            (i16, bool, bool, After, Option<i64>),
        );
        let enabled = [SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512];
        let cases: [Case; 9] = [
            (
                "initial, enabled",
                || ConnectionAuth::Anonymous,
                None,
                "OAUTHBEARER",
                (
                    0,
                    true,
                    false,
                    After::Negotiating(SaslMechanism::OAuthBearer),
                    None,
                ),
            ),
            (
                "initial, disabled mechanism closes",
                || ConnectionAuth::Anonymous,
                None,
                "PLAIN",
                (33, true, true, After::Anonymous, None),
            ),
            (
                "initial, unknown mechanism closes",
                || ConnectionAuth::Anonymous,
                None,
                "NOPE",
                (33, true, true, After::Anonymous, None),
            ),
            (
                "session without expiry cannot re-authenticate",
                || authenticated(None),
                None,
                "OAUTHBEARER",
                (34, false, false, After::Authenticated, None),
            ),
            (
                "re-authentication less than a second after the last",
                || authenticated(Some(NOW_MS + 5_000)),
                Some(NOW_MS - 999),
                "OAUTHBEARER",
                (34, false, false, After::Authenticated, Some(NOW_MS - 999)),
            ),
            (
                "re-authentication, same mechanism",
                || authenticated(Some(NOW_MS + 5_000)),
                Some(NOW_MS - 1_000),
                "OAUTHBEARER",
                (
                    0,
                    true,
                    false,
                    After::Reauthenticating(SaslMechanism::OAuthBearer),
                    Some(NOW_MS),
                ),
            ),
            (
                "re-authentication switching to another enabled mechanism",
                || authenticated(Some(NOW_MS + 5_000)),
                None,
                "SCRAM-SHA-512",
                (0, true, false, After::ReauthBadMechanism, Some(NOW_MS)),
            ),
            (
                "re-authentication with an unsupported mechanism closes",
                || authenticated(Some(NOW_MS + 5_000)),
                None,
                "PLAIN",
                (33, true, true, After::Authenticated, Some(NOW_MS)),
            ),
            (
                "second handshake mid-exchange",
                || ConnectionAuth::Negotiating {
                    mechanism: SaslMechanism::OAuthBearer,
                    exchange: SaslExchange::OAuthBearer,
                    pending_token_expiry_ms: None,
                },
                None,
                "OAUTHBEARER",
                (
                    34,
                    false,
                    true,
                    After::Negotiating(SaslMechanism::OAuthBearer),
                    None,
                ),
            ),
        ];
        for (case, start, last_start, mechanism, expected) in cases {
            let (code, lists_mechanisms, close_after, next, last_after) = expected;
            let mut auth = start();
            let mut last_start_ms = last_start;
            let req = SaslHandshakeRequest {
                mechanism: mechanism.to_string(),
                ..Default::default()
            };
            let outcome = handle_handshake(&req, &mut auth, &enabled, &mut ReauthClock {
                now_ms: NOW_MS,
                last_start_ms: &mut last_start_ms,
            });
            let mechanisms = if lists_mechanisms {
                vec!["OAUTHBEARER".to_string(), "SCRAM-SHA-512".to_string()]
            } else {
                Vec::new()
            };
            assert!(
                outcome.response
                    == SaslHandshakeResponse {
                        error_code: code,
                        mechanisms,
                        ..Default::default()
                    },
                "{case}"
            );
            assert!(outcome.close_after == close_after, "{case}");
            assert!(after(&auth) == next, "{case}");
            assert!(last_start_ms == last_after, "{case}");
        }
    }
}
