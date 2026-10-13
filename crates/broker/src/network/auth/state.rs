//! Per-connection SASL session state and the pre-authentication allowlist.
//!
//! This module holds the data the state machine moves through: the
//! `ConnectionAuth` states themselves, the snapshot a KIP-368 in-band
//! re-authentication takes of the session it replaces, and the per-mechanism
//! `SaslExchange` that carries an exchange between rounds. The mechanism
//! handlers in the sibling modules read and write these types, so they live
//! apart from any one mechanism.

use {
    super::response::fail_authenticate_with,
    crate::handlers::ApiKeyCode,
    krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse,
    krabka_security::{AuthMethod, Principal, SaslMechanism, ScramServerExchange},
    krabka_verified::{
        authz::{
            RequestAuthState, request_auth_admission, sasl_session_expiry,
            session_expired_for_request,
        },
        delegation_token::{TokenApi, TokenApiAdmission, token_api_admission},
    },
};

// Several variants and the `principal` accessor are exercised by the PLAIN,
// SCRAM, and admin paths — keep the surface in one place.

/// Per-connection SASL state. Transitions:
/// `Anonymous` -> (`SaslHandshake`) -> `Negotiating` -> (`SaslAuthenticate` ok)
///   -> `Authenticated`.
///
/// For PLAINTEXT/SSL listeners, the dispatcher initialises the connection
/// directly to `Authenticated { principal: ANONYMOUS }`, so the pre-auth
/// gate does nothing.
#[derive(Debug)]
pub enum ConnectionAuth {
    /// PLAINTEXT / SSL listener, or pre-handshake on a SASL listener.
    Anonymous,
    /// The broker received `SaslHandshake` and waits for one or more
    /// `SaslAuthenticate` requests.
    Negotiating {
        mechanism: SaslMechanism,
        exchange: SaslExchange,
        /// KIP-48 side-channel: when the SCRAM round-1 lookup falls back to
        /// a delegation token, this field holds the token's
        /// `expiry_timestamp_ms`, so the round-2 success arm can:
        /// 1. Set `ConnectionAuth::Authenticated.expires_at_ms` (the
        ///    KIP-368 re-auth ceiling), and
        /// 2. Set `authenticated_via_token: true` (the KIP-48
        ///    token-to-token chain guard read by `CreateDelegationToken`).
        ///
        /// This field is `None` for every non-token-SCRAM negotiation:
        /// PLAIN, regular SCRAM, OAUTHBEARER, and token-SCRAM round 1 before
        /// the lookup fires. `Some(_)` is the unambiguous "token-authed
        /// session" marker.
        pending_token_expiry_ms: Option<i64>,
    },
    Authenticated {
        principal: Principal,
        /// SASL mechanism this connection authenticated with. KIP-368
        /// in-band re-auth reads it to reject a fresh `SaslHandshake` that
        /// switches mechanisms in the middle of a connection. For mTLS and
        /// anonymous connections, which use no SASL, this is
        /// `SaslMechanism::Plain` as an unused default. The in-band re-auth
        /// path cannot run there, because the listener does not accept
        /// `SaslHandshake` at all.
        mechanism: SaslMechanism,
        /// Session expiry as Unix epoch ms: the earlier of the credential's
        /// own lifetime and the listener's `connections.max.reauth.ms`
        /// ceiling, as [`session_expiry`] computes it. Only OAUTHBEARER (the
        /// token `exp`) and KIP-48 delegation tokens (the token expiry)
        /// supply a credential lifetime; PLAIN, SCRAM and GSSAPI carry one
        /// only when the listener caps re-authentication. `None` means no
        /// expiry, which is what an uncapped listener gives those three, and
        /// what mTLS and anonymous connections always get.
        ///
        /// No timer watches this instant. The dispatch loop compares each
        /// arriving request against it through [`ConnectionAuth::expired_for_request`],
        /// and closes the connection on the first one that is not part of a
        /// re-authentication.
        expires_at_ms: Option<i64>,
        /// KIP-48: whether this connection authenticated with a delegation
        /// token instead of a "real" principal credential. Token auth uses
        /// SCRAM-SHA-256 or SCRAM-SHA-512 with the `tokenauth=true` extension
        /// and the token's HMAC as the password equivalent.
        ///
        /// The delegation-token RPCs read this flag.
        /// Every delegation-token RPC rejects token-authed callers with
        /// `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`. This prevents token-to-token
        /// chains and leakage of sibling token HMACs through Describe. Only
        /// the token-auth path sets this to `true`; every other construction
        /// site defaults it to `false`.
        authenticated_via_token: bool,
    },
    /// In-band re-authentication in progress: a `SaslHandshake` from a
    /// previously `Authenticated` connection (KIP-368).
    ///
    /// This variant holds the previous session snapshot. The post-validate
    /// equality check compares against it for the same principal name and the
    /// same mechanism. A failed re-auth also uses it to name the still-current
    /// principal in the error message.
    Reauthenticating {
        previous: AuthenticatedSnapshot,
        exchange: SaslExchange,
        /// Same KIP-48 side-channel as
        /// [`ConnectionAuth::Negotiating::pending_token_expiry_ms`]: a
        /// multi-round re-auth exchange runs the mechanism's ordinary
        /// rounds, so a token-SCRAM round 1 has the same value to carry to
        /// round 2.
        pending_token_expiry_ms: Option<i64>,
    },
    /// A KIP-368 re-authentication handshake named an enabled mechanism other
    /// than the session's own. Kafka's `SaslServerAuthenticator` answers that
    /// handshake with `NONE`, moves to `REAUTH_BAD_MECHANISM`, and fails the
    /// connection on the next frame, whatever it is; the request gate refuses
    /// every request in this state.
    ReauthBadMechanism,
}

/// Snapshot of an `Authenticated` connection at the moment a re-auth
/// `SaslHandshake` arrives.
///
/// During re-auth, the `SaslAuthenticate` handler reads it to enforce
/// same-mechanism and same-principal-name semantics (KIP-368).
#[derive(Debug, Clone)]
pub struct AuthenticatedSnapshot {
    pub principal: Principal,
    pub mechanism: SaslMechanism,
    pub expires_at_ms: Option<i64>,
    /// KIP-48 token-auth flag of the session being replaced, so that a
    /// re-auth the broker refuses puts the connection back exactly as it
    /// was.
    pub authenticated_via_token: bool,
}

/// The KIP-368 session ceiling a mechanism reports and records: the earlier of
/// the credential's own lifetime and the listener's
/// `connections.max.reauth.ms`, as `(expires_at_ms, session_lifetime_ms)`.
///
/// `credential_expires_at_ms` is `None` for every credential that carries no
/// expiry of its own — PLAIN, SCRAM and GSSAPI — which leaves the cap as the
/// only ceiling. Only OAUTHBEARER's token `exp` and KIP-48's delegation-token
/// expiry supply one. An absent cap and a non-positive one both mean "no
/// broker ceiling", the way Kafka arms no re-auth deadline for a
/// `connections.max.reauth.ms` that is not positive.
pub(super) fn session_expiry(
    now_ms: i64,
    credential_expires_at_ms: Option<i64>,
    max_reauth: Option<krabka_units::Time>,
) -> (Option<i64>, i64) {
    sasl_session_expiry(
        now_ms,
        credential_expires_at_ms,
        max_reauth.map(krabka_units::convert::TimeExt::millis_i64),
    )
}

/// Enters a KIP-368 re-authentication: a `Reauthenticating` connection runs
/// the mechanism's ordinary exchange, so this hands the handler a
/// `Negotiating` state to drive and keeps the previous session's snapshot for
/// [`finish_reauth`] to check the result against.
///
/// Returns `None` for a connection that is not re-authenticating, which the
/// handlers read as "run the initial-auth path and return its response as-is".
pub(super) fn begin_reauth(auth: &mut ConnectionAuth) -> Option<AuthenticatedSnapshot> {
    if !matches!(auth, ConnectionAuth::Reauthenticating { .. }) {
        return None;
    }
    let ConnectionAuth::Reauthenticating {
        previous,
        exchange,
        pending_token_expiry_ms,
    } = std::mem::replace(auth, ConnectionAuth::Anonymous)
    else {
        unreachable!("matched Reauthenticating above");
    };
    *auth = ConnectionAuth::Negotiating {
        mechanism: previous.mechanism,
        exchange,
        pending_token_expiry_ms,
    };
    Some(previous)
}

/// Applies the KIP-368 rules to the state a re-auth exchange left behind.
///
/// A round that authenticated the same principal stands. A round that only
/// advanced the exchange (SCRAM round 1, a GSSAPI challenge) goes back to
/// `Reauthenticating`, so the request gate keeps refusing everything but
/// `SaslAuthenticate` until the exchange finishes. Anything else — a failed
/// exchange, or one that authenticated a different principal, which KIP-368
/// forbids mid-connection — restores the previous session and answers
/// `SASL_AUTHENTICATION_FAILED`, on which the dispatcher closes the
/// connection.
pub(super) fn finish_reauth(
    auth: &mut ConnectionAuth,
    previous: AuthenticatedSnapshot,
    resp: SaslAuthenticateResponse,
) -> SaslAuthenticateResponse {
    if resp.error_code == 0 {
        match auth {
            ConnectionAuth::Authenticated { principal, .. }
                if principal.name == previous.principal.name =>
            {
                return resp;
            }
            ConnectionAuth::Negotiating { .. } => {
                let ConnectionAuth::Negotiating {
                    exchange,
                    pending_token_expiry_ms,
                    ..
                } = std::mem::replace(auth, ConnectionAuth::Anonymous)
                else {
                    unreachable!("matched Negotiating above");
                };
                *auth = ConnectionAuth::Reauthenticating {
                    previous,
                    exchange,
                    pending_token_expiry_ms,
                };
                return resp;
            }
            _ => {}
        }
    }
    let switched_to = match auth {
        ConnectionAuth::Authenticated { principal, .. } => Some(principal.name.clone()),
        _ => None,
    };
    let previous_name = previous.principal.name.clone();
    *auth = ConnectionAuth::Authenticated {
        principal: previous.principal,
        mechanism: previous.mechanism,
        expires_at_ms: previous.expires_at_ms,
        authenticated_via_token: previous.authenticated_via_token,
    };
    if let Some(switched_to) = switched_to {
        tracing::debug!(
            previous = %previous_name,
            "re-authentication attempted a principal switch"
        );
        return fail_authenticate_with(principal_change_message(&previous_name, &switched_to));
    }
    resp
}

/// Kafka's `ReauthInfo.ensurePrincipalUnchanged` message, which renders each
/// principal as `<type>.<name>`. krabka's SASL principals are all `User`.
pub(super) fn principal_change_message(previous: &str, attempted: &str) -> String {
    format!(
        "Cannot change principals during re-authentication from User.{previous}: User.{attempted}"
    )
}

/// In-flight SASL exchange.
///
/// `Plain` carries no state, because PLAIN is a single round trip.
/// `ScramPending` is the post-handshake and pre-client-first state for SCRAM;
/// the broker needs the client's `username` to build a
/// `ScramServerExchange`, so it builds the real exchange lazily. `Scram`
/// wraps the live RFC 5802 server state machine once the first client message
/// arrives.
#[derive(Debug)]
pub enum SaslExchange {
    Plain,
    ScramPending,
    /// This variant is boxed because `ScramServerExchange` is larger than
    /// clippy's 200-byte `large_enum_variant` threshold. Its
    /// `principal_override: Option<Principal>` field serves the
    /// delegation-token SCRAM fallback. Boxing keeps this rare variant out of
    /// the size of the whole enum.
    Scram(Box<ScramServerExchange>),
    /// OAUTHBEARER, post-handshake and pre-token. The bearer token arrives
    /// in the first `SaslAuthenticate`, which on success is the only one.
    OAuthBearer,
    /// OAUTHBEARER token validation failed. The broker returned the RFC 7628
    /// error JSON with `error_code = 0` and kept the connection open. It now
    /// waits for the client's single-`\x01` final message before it fails the
    /// connection with `SASL_AUTHENTICATION_FAILED`.
    OAuthBearerFailed,
    /// GSSAPI post-handshake and pre-first-token. The broker builds the
    /// acceptor, and with it the live `GssapiServerExchange`, lazily on the
    /// first `SaslAuthenticate` round, once the client's AP-REQ arrives. This
    /// mirrors the SCRAM `ScramPending` pattern: the broker does not read the
    /// keytab until a client starts a GSSAPI exchange.
    GssapiPending,
    /// GSSAPI multi-round in flight: the live RFC 4752 server state machine,
    /// from GSS context establishment to security-layer negotiation. This
    /// variant is boxed to keep the `sspi`-backed acceptor out of the size of
    /// the whole enum. `wasm32-wasip1` has no Kerberos stack, so the variant
    /// does not exist there.
    #[cfg(not(target_family = "wasm"))]
    Gssapi(Box<super::gssapi::GssapiServerExchange>),
}

impl SaslExchange {
    /// Whether the exchange waits for another `SaslAuthenticate` round from
    /// the client: a SCRAM or GSSAPI exchange in flight, or the RFC 7628
    /// OAUTHBEARER failure that waits for the client's final message.
    #[must_use]
    pub fn awaits_client_round(&self) -> bool {
        match self {
            Self::Scram(_) | Self::OAuthBearerFailed => true,
            #[cfg(not(target_family = "wasm"))]
            Self::Gssapi(_) => true,
            Self::Plain | Self::ScramPending | Self::OAuthBearer | Self::GssapiPending => false,
        }
    }
}

impl ConnectionAuth {
    #[must_use]
    pub fn is_authenticated(&self) -> bool { matches!(self, Self::Authenticated { .. }) }

    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        if let Self::Authenticated { principal, .. } = self {
            Some(principal)
        } else {
            None
        }
    }

    /// Apply the shared KIP-48 API admission policy to this connection.
    #[must_use]
    pub(crate) fn token_api_admission(&self, api: TokenApi) -> TokenApiAdmission {
        let Self::Authenticated {
            principal,
            authenticated_via_token,
            ..
        } = self
        else {
            return TokenApiAdmission::Reject;
        };

        token_api_admission(
            principal.auth_method != AuthMethod::Anonymous,
            *authenticated_via_token,
            api,
        )
    }

    /// Whether the broker may serve `api_key` in the current auth state, as
    /// Kafka's `SaslServerAuthenticator` does.
    /// - `Anonymous` (no handshake yet): only `SaslHandshake=17` and
    ///   `ApiVersions=18`. A `SaslAuthenticate` here is an
    ///   `InvalidRequestException` in Kafka, which closes the connection.
    /// - `Negotiating` / `Reauthenticating` (a handshake chose a mechanism):
    ///   only `SaslAuthenticate=36`.
    /// - `ReauthBadMechanism`: nothing.
    /// - `Authenticated`: everything.
    ///
    /// The dispatch layer closes the connection on a refused request.
    #[must_use]
    pub fn allows_request(&self, api_key: ApiKeyCode) -> bool {
        let state = match self {
            Self::Anonymous => RequestAuthState::PreHandshake,
            Self::Negotiating { .. } | Self::Reauthenticating { .. } => {
                RequestAuthState::Exchanging
            }
            Self::ReauthBadMechanism => RequestAuthState::Failed,
            Self::Authenticated { .. } => RequestAuthState::Authenticated,
        };
        request_auth_admission(state, api_key)
    }

    /// KIP-368: whether this request must end the connection because the
    /// session's re-authentication window has already elapsed.
    ///
    /// The deadline is enforced here, when a request arrives, and nowhere
    /// else. Apache Kafka does the same: `Selector` consumes a `SaslHandshake`
    /// on an expired session as the start of a re-authentication, and only a
    /// request that reaches `Processor.processCompletedReceives` past the
    /// deadline closes the channel. No timer runs against an idle connection.
    ///
    /// That grace is what makes the window usable. A JVM client re-authenticates
    /// at 85–95% of the advertised `session_lifetime_ms`, but only on a
    /// connection it is about to write to, so a producer sending on its own
    /// cadence starts the exchange after the deadline as a matter of course. A
    /// broker that closed on the deadline itself would cut those clients off
    /// mid-re-authentication.
    ///
    /// `SaslHandshake` and `SaslAuthenticate` therefore pass whatever the
    /// clock says; every other API on an expired session does not.
    #[must_use]
    pub(crate) fn expired_for_request(&self, api_key: ApiKeyCode, now_ms: i64) -> bool {
        match self {
            Self::Authenticated { expires_at_ms, .. } => {
                session_expired_for_request(*expires_at_ms, api_key, now_ms)
            }
            _ => false,
        }
    }

    /// The mechanism a `SaslHandshake` named for the exchange now in flight,
    /// or `None` when no exchange is in flight.
    ///
    /// Two states carry one: a `Negotiating` connection holds the mechanism
    /// its handshake chose, and a `Reauthenticating` one holds the mechanism
    /// of the session it is replacing. `Anonymous` has run no handshake, and
    /// an `Authenticated` connection's exchange is over — its `mechanism` is
    /// an unused placeholder on the mTLS and anonymous paths, so reporting it
    /// would invent a mechanism the connection never used.
    ///
    /// It is what the `SaslAuthenticate` path and the pre-auth gate both
    /// label their `failed_authentication` with, so a rejection is counted
    /// under the mechanism the peer asked for whichever of the two saw it.
    #[must_use]
    pub fn negotiated_mechanism(&self) -> Option<SaslMechanism> {
        match self {
            Self::Negotiating { mechanism, .. } => Some(*mechanism),
            Self::Reauthenticating { previous, .. } => Some(previous.mechanism),
            Self::Anonymous | Self::ReauthBadMechanism | Self::Authenticated { .. } => None,
        }
    }
}

/// Run an ordinary SASL round and restore or renew the previous session when this is reauthentication.
pub(super) fn authenticate_with_reauth(
    auth: &mut ConnectionAuth,
    authenticate: impl FnOnce(&mut ConnectionAuth) -> SaslAuthenticateResponse,
) -> SaslAuthenticateResponse {
    let previous = begin_reauth(auth);
    let response = authenticate(auth);
    match previous {
        Some(previous) => finish_reauth(auth, previous, response),
        None => response,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::SASL_AUTHENTICATION_FAILED,
            network::auth::test_support::{authenticated, oauth_reauthenticating},
            test_support::{principal, sasl_principal},
        },
        assert2::{assert, check},
    };

    /// The request gate follows Kafka's `SaslServerAuthenticator`: before a
    /// handshake only `SaslHandshake` and `ApiVersions`; after one only
    /// `SaslAuthenticate`; after a re-authentication mechanism switch
    /// nothing.
    #[test]
    fn request_gate_follows_the_sasl_server_states() {
        fn negotiating() -> ConnectionAuth {
            ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::Plain,
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            }
        }
        fn reauthenticating() -> ConnectionAuth {
            ConnectionAuth::Reauthenticating {
                previous: AuthenticatedSnapshot {
                    principal: sasl_principal("alice"),
                    mechanism: SaslMechanism::Plain,
                    expires_at_ms: Some(1),
                    authenticated_via_token: false,
                },
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            }
        }
        // (state, [SaslHandshake, ApiVersions, SaslAuthenticate, Produce, Metadata])
        let cases: [(&str, ConnectionAuth, [bool; 5]); 4] = [
            ("anonymous", ConnectionAuth::Anonymous, [
                true, true, false, false, false,
            ]),
            ("negotiating", negotiating(), [
                false, false, true, false, false,
            ]),
            ("reauthenticating", reauthenticating(), [
                false, false, true, false, false,
            ]),
            (
                "reauth bad mechanism",
                ConnectionAuth::ReauthBadMechanism,
                [false; 5],
            ),
        ];
        for (name, auth, expected) in cases {
            let got = [17, 18, 36, 0, 3].map(|api_key| auth.allows_request(api_key));
            assert!(got == expected, "{name}");
        }
    }

    #[test]
    fn unauthenticated_states_have_no_principal() {
        let cases = [
            ("anonymous", ConnectionAuth::Anonymous),
            ("negotiating_plain", ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::Plain,
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            }),
            ("negotiating_scram_pending", ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::ScramSha512,
                exchange: SaslExchange::ScramPending,
                pending_token_expiry_ms: None,
            }),
        ];
        for (name, a) in cases {
            assert!(!a.is_authenticated(), "{name}");
            assert!(a.principal().is_none(), "{name}");
        }
    }

    #[test]
    fn token_api_admission_maps_connection_auth_states_exactly() {
        use krabka_security::AuthMethod;

        let apis = [
            TokenApi::Create,
            TokenApi::Renew,
            TokenApi::Expire,
            TokenApi::Describe,
        ];
        let authenticated =
            |auth_method, mechanism, authenticated_via_token| ConnectionAuth::Authenticated {
                principal: Principal {
                    name: "alice".into(),
                    auth_method,
                    groups: vec![],
                },
                mechanism,
                expires_at_ms: None,
                authenticated_via_token,
            };

        let rejected = [
            ("anonymous state", ConnectionAuth::Anonymous),
            ("negotiating state", ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::Plain,
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            }),
            ("reauthenticating state", ConnectionAuth::Reauthenticating {
                previous: AuthenticatedSnapshot {
                    principal: Principal {
                        name: "alice".into(),
                        auth_method: AuthMethod::SaslOAuthBearer,
                        groups: vec![],
                    },
                    mechanism: SaslMechanism::OAuthBearer,
                    expires_at_ms: Some(2_000_000),
                    authenticated_via_token: false,
                },
                exchange: SaslExchange::OAuthBearer,
                pending_token_expiry_ms: None,
            }),
            (
                "authenticated anonymous principal",
                authenticated(AuthMethod::Anonymous, SaslMechanism::Plain, false),
            ),
        ];
        for (state, auth) in rejected {
            for api in apis {
                check!(
                    auth.token_api_admission(api) == TokenApiAdmission::Reject,
                    "{state}: {api:?}"
                );
            }
        }

        for (method, mechanism) in [
            (AuthMethod::SaslPlain, SaslMechanism::Plain),
            (AuthMethod::SaslScramSha256, SaslMechanism::ScramSha256),
            (AuthMethod::SaslScramSha512, SaslMechanism::ScramSha512),
            (AuthMethod::SaslOAuthBearer, SaslMechanism::OAuthBearer),
            (AuthMethod::SaslGssapi, SaslMechanism::Gssapi),
            (AuthMethod::MTls, SaslMechanism::Plain),
        ] {
            let auth = authenticated(method, mechanism, false);
            for api in apis {
                check!(auth.token_api_admission(api) == TokenApiAdmission::Allow);
            }
        }

        // KafkaApis.allowTokenRequests refuses every delegation-token API,
        // Describe included, to a token-authenticated caller: a session
        // minted from one token must not be able to describe (and so leak
        // the HMACs of) any token at all, including its own.
        let token_auth = authenticated(
            AuthMethod::SaslScramSha256,
            SaslMechanism::ScramSha256,
            true,
        );
        for api in apis {
            check!(
                token_auth.token_api_admission(api) == TokenApiAdmission::Reject,
                "{api:?}"
            );
        }
    }

    #[test]
    fn authenticated_returns_principal() {
        let a = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslScramSha512,
            SaslMechanism::ScramSha512,
            None,
            false,
        );
        assert!(a.is_authenticated());
        let p = a.principal().expect("principal");
        assert!(p.name == "alice");
        assert!(p.auth_method == krabka_security::AuthMethod::SaslScramSha512);
    }

    // KIP-368: in-band re-auth tests.

    #[test]
    fn authenticated_state_carries_mechanism_and_expires_at_ms() {
        let auth = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslOAuthBearer,
            SaslMechanism::OAuthBearer,
            Some(2_000_000),
            false,
        );
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token: _,
            } => {
                check!(principal.name.as_str() == "alice");
                check!(mechanism == SaslMechanism::OAuthBearer);
                check!(expires_at_ms == Some(2_000_000));
            }
            _ => panic!("expected Authenticated"),
        }
    }

    #[test]
    fn allows_request_during_reauthenticating_only_sasl_authenticate() {
        let auth = oauth_reauthenticating("alice", 2_000_000);
        let cases = [
            (36, true),  // SaslAuthenticate
            (17, false), // SaslHandshake
            (18, false), // ApiVersions
            (3, false),  // Metadata
        ];
        for (api_key, allowed) in cases {
            assert!(auth.allows_request(api_key) == allowed, "api key {api_key}");
        }
    }

    #[test]
    fn allows_request_authenticated_allows_all() {
        let auth = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslScramSha512,
            SaslMechanism::ScramSha512,
            None,
            false,
        );
        for api_key in [0, 3, 17, 36] {
            assert!(auth.allows_request(api_key), "api key {api_key}");
        }
    }

    /// KIP-368: the deadline ends a connection only through a request, and
    /// never through one of the two frames a re-authentication is made of.
    /// That is what lets a client whose window has already closed start its
    /// exchange and be served, the way Apache Kafka serves one.
    #[test]
    fn an_expired_session_admits_a_re_authentication_and_nothing_else() {
        let now_ms = 1_000_i64;
        let expired = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslScramSha512,
            SaslMechanism::ScramSha512,
            Some(now_ms - 1),
            false,
        );
        let live = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslScramSha512,
            SaslMechanism::ScramSha512,
            Some(now_ms + 1),
            false,
        );
        let uncapped = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslScramSha512,
            SaslMechanism::ScramSha512,
            None,
            false,
        );
        let reauthenticating = ConnectionAuth::Reauthenticating {
            previous: AuthenticatedSnapshot {
                principal: Principal {
                    name: "alice".into(),
                    auth_method: AuthMethod::SaslScramSha512,
                    groups: vec![],
                },
                mechanism: SaslMechanism::ScramSha512,
                expires_at_ms: Some(now_ms - 1),
                authenticated_via_token: false,
            },
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: None,
        };

        let cases = [
            (
                "expired: SaslHandshake opens the re-auth",
                &expired,
                17,
                false,
            ),
            ("expired: SaslAuthenticate runs it", &expired, 36, false),
            ("expired: Produce closes", &expired, 0, true),
            ("expired: Metadata closes", &expired, 3, true),
            ("expired: ApiVersions closes", &expired, 18, true),
            ("live session serves Produce", &live, 0, false),
            ("uncapped session serves Produce", &uncapped, 0, false),
            (
                "an exchange in flight is never expired",
                &reauthenticating,
                0,
                false,
            ),
            (
                "anonymous has no deadline",
                &ConnectionAuth::Anonymous,
                0,
                false,
            ),
        ];
        for (case, auth, api_key, want) in cases {
            check!(auth.expired_for_request(api_key, now_ms) == want, "{case}");
        }
    }

    /// The mechanism a rejection is counted under is the one a handshake
    /// named, and nothing else: an `Authenticated` connection's placeholder
    /// mechanism never reaches the metric, because no exchange is in flight.
    #[test]
    fn negotiated_mechanism_names_only_an_exchange_in_flight() {
        let negotiating = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::Plain,
            exchange: SaslExchange::Plain,
            pending_token_expiry_ms: None,
        };
        let reauthenticating = oauth_reauthenticating("alice", 2_000_000);
        let authenticated = ConnectionAuth::Authenticated {
            principal: principal("ANONYMOUS"),
            mechanism: SaslMechanism::Plain,
            expires_at_ms: None,
            authenticated_via_token: false,
        };

        let cases = [
            ("anonymous", &ConnectionAuth::Anonymous, None),
            ("negotiating", &negotiating, Some(SaslMechanism::Plain)),
            (
                "reauthenticating uses previous",
                &reauthenticating,
                Some(SaslMechanism::OAuthBearer),
            ),
            ("authenticated placeholder", &authenticated, None),
        ];
        for (case, auth, want) in cases {
            check!(auth.negotiated_mechanism() == want, "{case}");
        }
    }

    fn alice() -> Principal { sasl_principal("alice") }

    /// The session a re-authentication is measured against: SCRAM-SHA-256
    /// under a delegation token, so that a restore which drops either the
    /// mechanism or the KIP-48 token flag is visible.
    fn assert_previous_session_restored(auth: ConnectionAuth) {
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                mechanism,
                expires_at_ms,
                authenticated_via_token,
            } => {
                check!(principal.name == "alice");
                check!(mechanism == SaslMechanism::ScramSha256);
                check!(expires_at_ms == Some(9_000));
                check!(authenticated_via_token);
            }
            other => panic!("expected the previous session restored, got {other:?}"),
        }
    }

    fn previous_session() -> AuthenticatedSnapshot {
        AuthenticatedSnapshot {
            principal: alice(),
            mechanism: SaslMechanism::ScramSha256,
            expires_at_ms: Some(9_000),
            authenticated_via_token: true,
        }
    }

    fn ok_response(session_lifetime_ms: i64) -> SaslAuthenticateResponse {
        SaslAuthenticateResponse {
            error_code: 0,
            error_message: None,
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms,
            ..Default::default()
        }
    }

    /// KIP-368 ceiling arithmetic. The window the broker advertises and the
    /// instant it enforces have to agree, the earlier of the two ceilings has
    /// to win in both directions, a `connections.max.reauth.ms` that is not
    /// positive has to arm nothing the way Kafka arms nothing, and a
    /// credential that has already expired has to report a zero window rather
    /// than a negative one that a client would read as a huge window.
    #[test]
    fn session_expiry_takes_the_earlier_ceiling_and_never_reports_a_negative_window() {
        let now = 1_000_000_i64;
        let cases = [
            ("no credential lifetime and no cap", None, None, (None, 0)),
            (
                "the cap is the only ceiling",
                None,
                Some(krabka_units::millis(30_000)),
                (Some(now + 30_000), 30_000),
            ),
            (
                "a zero cap arms no deadline",
                None,
                Some(krabka_units::millis(0)),
                (None, 0),
            ),
            (
                "a negative cap arms no deadline",
                None,
                Some(-krabka_units::millis(1)),
                (None, 0),
            ),
            (
                "the credential is the only ceiling",
                Some(now + 5_000),
                None,
                (Some(now + 5_000), 5_000),
            ),
            (
                "a credential shorter than the cap wins",
                Some(now + 5_000),
                Some(krabka_units::millis(30_000)),
                (Some(now + 5_000), 5_000),
            ),
            (
                "a cap shorter than the credential wins",
                Some(now + 60_000),
                Some(krabka_units::millis(30_000)),
                (Some(now + 30_000), 30_000),
            ),
            (
                "an already-expired credential reports no window",
                Some(now - 5_000),
                Some(krabka_units::millis(30_000)),
                (Some(now - 5_000), 0),
            ),
        ];
        for (case, credential, cap, want) in cases {
            check!(session_expiry(now, credential, cap) == want, "{case}");
        }

        check!(
            session_expiry(i64::MAX - 1, None, Some(krabka_units::millis(30_000)))
                == (Some(i64::MAX), 1)
        );

        // A saturated cap still respects an earlier credential deadline.
        check!(
            session_expiry(
                i64::MAX - 1,
                Some(i64::MAX - 1),
                Some(krabka_units::millis(30_000)),
            ) == (Some(i64::MAX - 1), 0)
        );
    }

    /// `begin_reauth` is the handlers' "is this a re-authentication?" test, so
    /// it must answer no — and leave the connection alone — for every state
    /// that is not one.
    #[test]
    fn begin_reauth_only_fires_for_a_reauthenticating_connection() {
        let cases = [
            ("anonymous", ConnectionAuth::Anonymous),
            ("negotiating", ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::Plain,
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            }),
            ("authenticated", ConnectionAuth::Authenticated {
                principal: alice(),
                mechanism: SaslMechanism::Plain,
                expires_at_ms: Some(9_000),
                authenticated_via_token: false,
            }),
        ];
        for (case, mut auth) in cases {
            let before = std::mem::discriminant(&auth);
            check!(begin_reauth(&mut auth).is_none(), "{case}");
            check!(std::mem::discriminant(&auth) == before, "{case}");
        }
    }

    /// A re-authentication runs the mechanism's ordinary exchange, so
    /// `begin_reauth` has to hand the handler a `Negotiating` state carrying
    /// the *previous* session's mechanism and the KIP-48 token side-channel.
    /// Dropping the mechanism would run the exchange under a default one, and
    /// dropping the side-channel would lose a token session's expiry ceiling
    /// between rounds.
    #[test]
    fn begin_reauth_hands_over_a_negotiating_state_with_the_previous_mechanism() {
        let mut auth = ConnectionAuth::Reauthenticating {
            previous: previous_session(),
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: Some(9_000),
        };

        let previous = begin_reauth(&mut auth).expect("a Reauthenticating connection yields one");
        check!(previous.principal.name == "alice");
        check!(previous.mechanism == SaslMechanism::ScramSha256);
        check!(previous.expires_at_ms == Some(9_000));
        check!(previous.authenticated_via_token);

        match auth {
            ConnectionAuth::Negotiating {
                mechanism,
                exchange,
                pending_token_expiry_ms,
            } => {
                check!(mechanism == SaslMechanism::ScramSha256);
                check!(matches!(exchange, SaslExchange::ScramPending));
                check!(pending_token_expiry_ms == Some(9_000));
            }
            other => panic!("expected Negotiating, got {other:?}"),
        }
    }

    /// A re-authentication that lands on the same principal stands, and the
    /// new session's own ceiling replaces the old one — that is what re-arms
    /// the KIP-368 window.
    #[test]
    fn finish_reauth_keeps_a_same_principal_session_and_its_new_window() {
        let mut auth = authenticated(
            "alice",
            krabka_security::AuthMethod::SaslPlain,
            SaslMechanism::Plain,
            Some(40_000),
            false,
        );
        let resp = finish_reauth(&mut auth, previous_session(), ok_response(30_000));

        check!(resp == ok_response(30_000));
        match auth {
            ConnectionAuth::Authenticated {
                principal,
                expires_at_ms,
                ..
            } => {
                check!(principal.name == "alice");
                check!(expires_at_ms == Some(40_000));
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
    }

    /// A multi-round mechanism's intermediate round answers `error_code == 0`
    /// with no principal yet. That round must go back to `Reauthenticating`,
    /// not be mistaken for a completed exchange: the request gate has to keep
    /// refusing the data plane until the last round lands.
    #[test]
    fn finish_reauth_returns_an_unfinished_exchange_to_reauthenticating() {
        let mut auth = ConnectionAuth::Negotiating {
            mechanism: SaslMechanism::ScramSha256,
            exchange: SaslExchange::ScramPending,
            pending_token_expiry_ms: Some(9_000),
        };
        let server_first = SaslAuthenticateResponse {
            auth_bytes: bytes::Bytes::from_static(b"r=nonce,s=salt,i=4096"),
            ..ok_response(0)
        };
        let resp = finish_reauth(&mut auth, previous_session(), server_first.clone());

        check!(resp == server_first);
        match &auth {
            ConnectionAuth::Reauthenticating {
                previous,
                pending_token_expiry_ms,
                ..
            } => {
                check!(previous.principal.name == "alice");
                check!(*pending_token_expiry_ms == Some(9_000));
            }
            other => panic!("expected Reauthenticating, got {other:?}"),
        }
        // The gate still refuses everything but the next round.
        check!(!auth.allows_request(3));
        check!(auth.allows_request(36));
    }

    /// KIP-368 forbids a principal switch mid-connection. The refusal has to
    /// both answer `SASL_AUTHENTICATION_FAILED` and put the connection back on
    /// the principal it already had, so a peer cannot promote itself by
    /// re-authenticating as someone else.
    #[test]
    fn finish_reauth_refuses_a_principal_switch_and_restores_the_old_session() {
        let mut auth = ConnectionAuth::Authenticated {
            principal: sasl_principal("mallory"),
            mechanism: SaslMechanism::Plain,
            expires_at_ms: Some(40_000),
            authenticated_via_token: false,
        };
        let resp = finish_reauth(&mut auth, previous_session(), ok_response(30_000));

        check!(
            resp == SaslAuthenticateResponse {
                error_code: SASL_AUTHENTICATION_FAILED,
                error_message: Some(
                    "Cannot change principals during re-authentication from User.alice: \
                     User.mallory"
                        .to_string()
                ),
                auth_bytes: bytes::Bytes::new(),
                session_lifetime_ms: 0,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }
        );
        assert_previous_session_restored(auth);
    }

    /// A re-authentication the mechanism rejected leaves the connection on its
    /// existing session — a bad password must not log the peer out — and the
    /// mechanism's own opaque failure reaches the wire unchanged, rather than
    /// being relabelled as a principal switch.
    #[test]
    fn finish_reauth_restores_the_old_session_after_a_failed_exchange() {
        let mut auth = ConnectionAuth::Anonymous;
        let failure = SaslAuthenticateResponse {
            error_code: SASL_AUTHENTICATION_FAILED,
            error_message: Some("authentication failed".to_string()),
            auth_bytes: bytes::Bytes::new(),
            session_lifetime_ms: 0,
            ..Default::default()
        };
        let resp = finish_reauth(&mut auth, previous_session(), failure.clone());

        check!(resp == failure);
        assert_previous_session_restored(auth);
    }
}
