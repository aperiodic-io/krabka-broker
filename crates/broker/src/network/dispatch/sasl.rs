//! Inline handling of the two SASL request frames. `SaslHandshake` (17) and
//! `SaslAuthenticate` (36) mutate the per-connection authentication state,
//! which the handler registry cannot reach, so the dispatch loop intercepts
//! them here before it consults the registry.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    api_key::ApiKey,
    owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use krabka_security::{AuthMethod, Principal, SaslMechanism};

use super::response::encode_response;
use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::ApiKeyCode,
    network::auth::{
        ConnectionAuth, ReauthClock, generic_failure_message, handle_authenticate_gssapi,
        handle_authenticate_oauthbearer_with_jwks_cache, handle_authenticate_plain,
        handle_authenticate_scram, handle_handshake,
    },
};

/// `SaslHandshake` wire `api_key`. The loop handles it inline, before the
/// handler table, because it mutates the per-connection auth state.
const SASL_HANDSHAKE_KEY: ApiKeyCode = ApiKey::SaslHandshake as i16;

/// `SaslAuthenticate` wire `api_key`. The loop handles it inline, before the
/// handler table, because it mutates the per-connection auth state.
const SASL_AUTHENTICATE_KEY: ApiKeyCode = ApiKey::SaslAuthenticate as i16;

/// `ApiVersions` wire `api_key`.
const API_VERSIONS_KEY: ApiKeyCode = ApiKey::ApiVersions as i16;

/// Outcome of a SASL frame interception: the bytes to write back to the peer,
/// and whether the dispatcher should close the connection after the send
/// completes. `SaslAuthenticate` failures and an illegal state both use the
/// close flag.
pub(super) struct SaslFrameOutcome {
    pub(super) response_bytes: Bytes,
    pub(super) close_after: bool,
}

/// The message Kafka's `KafkaApis.handleSaslAuthenticateRequest` puts on the
/// `ILLEGAL_SASL_STATE` answer to a `SaslAuthenticate` that reaches the
/// request handlers.
const AUTHENTICATE_AFTER_AUTHENTICATION: &str =
    "SaslAuthenticate request received after successful authentication";

/// The SASL frames of one connection: the listener kind and the mechanisms
/// that the listener enables.
pub(super) struct SaslListener<'a> {
    /// Whether the listener runs SASL (`SASL_PLAINTEXT` or `SASL_SSL`).
    pub(super) is_sasl: bool,
    /// The mechanisms that the listener enables.
    pub(super) mechanisms: &'a [SaslMechanism],
    /// The KIP-368 re-authentication window of the listener.
    pub(super) max_reauth: Option<krabka_units::Time>,
}

/// Per-connection SASL bookkeeping that outlives any one `ConnectionAuth`
/// state, as Kafka keeps it on the channel and its authenticator.
#[derive(Debug, Default)]
pub(super) struct SaslSession {
    /// A `SaslHandshake` v0 chose the mechanism: Kafka's
    /// `enableKafkaSaslAuthenticateHeaders` is off, and the exchange runs as
    /// raw size-prefixed SASL tokens with no Kafka request header.
    raw_tokens: bool,
    /// When this connection last started a KIP-368 re-authentication.
    last_reauth_start_ms: Option<i64>,
    /// A valid `ApiVersions` was answered before any handshake. Kafka's
    /// `SaslServerAuthenticator` then sits in `HANDSHAKE_REQUEST`, which takes
    /// only a `SaslHandshake`.
    api_versions_answered: bool,
}

impl SaslSession {
    /// Whether the next frame is a raw SASL token rather than a Kafka request.
    pub(super) fn expects_raw_token(&self, auth: &ConnectionAuth) -> bool {
        self.raw_tokens && auth.negotiated_mechanism().is_some()
    }

    /// Whether a request of `api_key` is a second `ApiVersions` before the
    /// handshake. The per-state gate admits `ApiVersions` there, but Kafka's
    /// `SaslServerAuthenticator.handleApiVersionsRequest` throws
    /// `IllegalStateException` for one outside `HANDSHAKE_OR_VERSIONS_REQUEST`,
    /// which closes the connection.
    pub(super) fn repeats_api_versions(&self, auth: &ConnectionAuth, api_key: ApiKeyCode) -> bool {
        self.api_versions_answered
            && api_key == API_VERSIONS_KEY
            && matches!(auth, ConnectionAuth::Anonymous)
    }

    /// Records that the request in `parsed` gets a full `ApiVersions` answer
    /// before the handshake. An `UNSUPPORTED_VERSION` or `INVALID_REQUEST`
    /// answer leaves Kafka's state as it was, so the client can ask again.
    pub(super) fn note_api_versions(
        &mut self,
        auth: &ConnectionAuth,
        parsed: &crate::network::request::ParsedRequest<'_>,
    ) {
        if parsed.api_key == API_VERSIONS_KEY
            && matches!(auth, ConnectionAuth::Anonymous)
            && crate::handlers::api_versions::is_valid_request_body(parsed.api_version, parsed.body)
        {
            self.api_versions_answered = true;
        }
    }
}

/// Waits out Kafka's `connection.failed.authentication.delay.ms`.
///
/// `Selector.maybeDelayCloseOnAuthenticationFailure` holds a failed
/// authentication's response, and the close that follows it, back by at least
/// that long, which slows credential guessing on one connection. The
/// connection's own task sleeps, so no other connection waits.
pub(super) async fn delay_failed_authentication(broker: &Broker) {
    let delay = broker.config.failed_authentication_delay();
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

/// Counts and audits one authentication that failed before any credential was
/// judged: a request the per-state gate refused, or a frame over
/// `sasl.server.max.receive.size`. The row names the mechanism a handshake
/// chose, or the `Unknown` sentinel when none did.
pub(super) fn record_refused_authentication(
    (metrics, audit_log): (&crate::metrics::BrokerMetrics, &krabka_audit::AuditLog),
    auth: &ConnectionAuth,
    peer: &SocketAddr,
    reason: &str,
) {
    let mech_label = auth
        .negotiated_mechanism()
        .map_or(crate::metrics::UNKNOWN_LABEL, SaslMechanism::wire_name);
    metrics.record_authentication(mech_label, false);
    emit_authentication(
        audit_log,
        peer,
        mech_label,
        auth.principal().map_or_else(
            || krabka_audit::AuditPrincipal {
                name: String::new(),
                auth_method: format!("{:?}", AuthMethod::Anonymous),
            },
            audit_principal,
        ),
        krabka_audit::AuditOutcome::Failure,
        Some(reason.to_string()),
    );
}

/// Accounts for a request the per-state auth gate refused, and returns the
/// response to write before the connection closes, if Kafka writes one.
///
/// A refusal is an authentication failure: it is counted and audited under
/// the mechanism a handshake named, or under the `Unknown` sentinel when none
/// did, so a peer that opens a SASL connection and immediately sends Produce
/// is not closed uncounted.
///
/// Once a handshake has run, Kafka fails the refusal with an
/// `AuthenticationException` (`IllegalSaslStateException`, or the
/// `SaslAuthenticationException` of a re-authentication that named another
/// mechanism), and its `Selector` delays the answer and the close. This waits
/// that delay out before it returns. Before a handshake the refusal is an
/// `InvalidRequestException` or an `IllegalStateException` (a second
/// `ApiVersions`), and Kafka closes at once.
pub(super) async fn refuse_gated_request(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &ConnectionAuth,
    peer: &SocketAddr,
    listener_name: &str,
) -> Option<Bytes> {
    tracing::info!(
        api_key = parsed.api_key,
        listener = %listener_name,
        "request blocked by per-state auth gate (ILLEGAL_SASL_STATE), closing connection"
    );
    record_refused_authentication(
        (&broker.metrics, &broker.audit_log),
        auth,
        peer,
        "request blocked by per-state auth gate",
    );
    if !matches!(auth, ConnectionAuth::Anonymous) {
        delay_failed_authentication(broker).await;
    }
    // Only mid-exchange does Kafka answer before closing.
    auth.negotiated_mechanism()
        .and_then(|_| unexpected_request_during_exchange(parsed))
}

/// The typed `ILLEGAL_SASL_STATE` answer Kafka's `SaslServerAuthenticator`
/// writes, before it closes the connection, for a `SaslHandshake` or
/// `ApiVersions` that arrives after a handshake chose a mechanism
/// (`Unexpected Kafka request of type ... during SASL authentication.`).
///
/// Kafka builds that answer with the request's own `getErrorResponse`, which
/// krabka has no generic equivalent of; any other `api_key` gets `None` and
/// the connection closes with no response.
fn unexpected_request_during_exchange(
    parsed: &crate::network::request::ParsedRequest<'_>,
) -> Option<Bytes> {
    use krabka_protocol::Encode;

    let mut body = BytesMut::new();
    let encoded = match parsed.api_key {
        SASL_HANDSHAKE_KEY => SaslHandshakeResponse {
            error_code: codes::ILLEGAL_SASL_STATE,
            ..Default::default()
        }
        .encode(&mut body, parsed.api_version),
        API_VERSIONS_KEY => krabka_protocol::owned::api_versions_response::ApiVersionsResponse {
            error_code: codes::ILLEGAL_SASL_STATE,
            ..Default::default()
        }
        .encode(&mut body, parsed.api_version),
        _ => return None,
    };
    encoded.ok()?;
    encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &body,
    )
    .ok()
}

/// Runs one raw SASL token of a `SaslHandshake` v0 exchange, as Kafka's
/// `SaslServerAuthenticator.handleSaslToken` does with
/// `enableKafkaSaslAuthenticateHeaders` off.
///
/// Returns the server token to write back as its own size-prefixed frame
/// (possibly empty, as Kafka writes it), or `None` when the exchange failed:
/// a v0 exchange has no response shape for an error, so Kafka closes the
/// connection without one.
pub(super) async fn handle_raw_sasl_token(
    broker: &Broker,
    token: &Bytes,
    auth: &mut ConnectionAuth,
    (listener, session): (&SaslListener<'_>, &mut SaslSession),
    peer: &SocketAddr,
) -> Option<Bytes> {
    let req = SaslAuthenticateRequest {
        auth_bytes: token.clone(),
        ..Default::default()
    };
    let resp = run_authenticate(broker, &req, auth, listener.max_reauth, peer).await;
    if resp.error_code != 0 {
        delay_failed_authentication(broker).await;
        return None;
    }
    if auth.is_authenticated() {
        session.raw_tokens = false;
    }
    Some(resp.auth_bytes)
}

/// Handles a `SaslHandshake` (17) or `SaslAuthenticate` (36) request inline.
///
/// For those two `api_key` values, the function returns a
/// [`SaslFrameOutcome`]. It returns `None` for every other `api_key`, and the
/// caller then falls through to the regular registry dispatch.
///
/// On a SASL listener, the frame runs the exchange and mutates `auth`. A
/// `PLAINTEXT` or `SSL` listener decided the principal when it accepted the
/// connection, so no SASL exchange can start there. Kafka sends the two frames
/// to `KafkaApis`, which answers `ILLEGAL_SASL_STATE` (34) and keeps the
/// connection and its principal. This function does the same, and it does
/// not touch `auth`.
///
/// An error here closes the connection. Such errors are protocol violations,
/// for example an undecodable header.
pub(super) async fn try_handle_sasl_frame(
    broker: &Broker,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut ConnectionAuth,
    listener: &SaslListener<'_>,
    session: &mut SaslSession,
    peer: &SocketAddr,
) -> Option<Result<SaslFrameOutcome, BrokerError>> {
    let api_key = parsed.api_key;
    if api_key != SASL_HANDSHAKE_KEY && api_key != SASL_AUTHENTICATE_KEY {
        return None;
    }
    if !listener.is_sasl {
        return Some(non_sasl_listener_response(parsed));
    }
    Some(
        async {
            use krabka_protocol::{Decode, Encode};

            let (resp_body, close_after) = match parsed.api_key {
                SASL_HANDSHAKE_KEY => {
                    let (body, close_after) =
                        handle_sasl_handshake(parsed, auth, listener.mechanisms, session)?;
                    // Kafka's `handleHandshakeRequest` turns
                    // `enableKafkaSaslAuthenticateHeaders` on only for v1+.
                    session.raw_tokens =
                        parsed.api_version == 0 && auth.negotiated_mechanism().is_some();
                    (body, close_after)
                }
                SASL_AUTHENTICATE_KEY => {
                    let mut cur: &[u8] = parsed.body;
                    let req = SaslAuthenticateRequest::decode(&mut cur, parsed.api_version)?;
                    // The request gate admits `SaslAuthenticate` only mid-exchange
                    // and once authenticated. On an authenticated connection Kafka
                    // hands it to `KafkaApis.handleSaslAuthenticateRequest`, which
                    // answers ILLEGAL_SASL_STATE and keeps the connection.
                    let (resp, close) = if auth.negotiated_mechanism().is_some() {
                        let resp =
                            run_authenticate(broker, &req, auth, listener.max_reauth, peer).await;
                        let close = resp.error_code != 0;
                        (resp, close)
                    } else if !auth.is_authenticated() {
                        // Before any handshake Kafka's `handleKafkaRequest` throws
                        // `InvalidRequestException`, which closes the connection with
                        // no response. The request gate refuses this frame first; an
                        // error here closes the same way if a caller does not gate.
                        return Err(BrokerError::Protocol(
                            krabka_protocol::ProtocolError::InvalidValue(
                                "SaslAuthenticate before SaslHandshake",
                            ),
                        ));
                    } else {
                        (
                            SaslAuthenticateResponse {
                                error_code: codes::ILLEGAL_SASL_STATE,
                                error_message: Some(AUTHENTICATE_AFTER_AUTHENTICATION.into()),
                                ..Default::default()
                            },
                            false,
                        )
                    };
                    let mut buf = BytesMut::with_capacity(resp.encoded_len(parsed.api_version));
                    resp.encode(&mut buf, parsed.api_version)?;
                    (buf.freeze(), close)
                }
                _ => unreachable!("filtered by caller to 17 / 36 only"),
            };

            let response_bytes = encode_response(
                parsed.api_key,
                parsed.correlation_id,
                parsed.body_flexible,
                &resp_body,
            )?;
            if close_after {
                // The answer to a failed exchange goes out, and the connection
                // closes, only once the failed-authentication delay has passed.
                delay_failed_authentication(broker).await;
            }
            Ok(SaslFrameOutcome {
                response_bytes,
                close_after,
            })
        }
        .await,
    )
}

/// Answers a SASL frame on a listener that runs no SASL, as Kafka's
/// `KafkaApis.handleSaslHandshakeRequest` and
/// `KafkaApis.handleSaslAuthenticateRequest` do.
///
/// `SaslHandshake` gets `ILLEGAL_SASL_STATE` and an empty mechanism list.
/// `SaslAuthenticate` gets `ILLEGAL_SASL_STATE` and Kafka's message. The
/// request body is not read, and the connection stays open.
fn non_sasl_listener_response(
    parsed: &crate::network::request::ParsedRequest<'_>,
) -> Result<SaslFrameOutcome, BrokerError> {
    use krabka_protocol::Encode;

    let mut body = BytesMut::new();
    if parsed.api_key == SASL_HANDSHAKE_KEY {
        let response = SaslHandshakeResponse {
            error_code: codes::ILLEGAL_SASL_STATE,
            ..Default::default()
        };
        body.reserve(response.encoded_len(parsed.api_version));
        response.encode(&mut body, parsed.api_version)?;
    } else {
        let response = SaslAuthenticateResponse {
            error_code: codes::ILLEGAL_SASL_STATE,
            error_message: Some(AUTHENTICATE_AFTER_AUTHENTICATION.into()),
            ..Default::default()
        };
        body.reserve(response.encoded_len(parsed.api_version));
        response.encode(&mut body, parsed.api_version)?;
    }
    let response_bytes = encode_response(
        parsed.api_key,
        parsed.correlation_id,
        parsed.body_flexible,
        &body,
    )?;
    Ok(SaslFrameOutcome {
        response_bytes,
        close_after: false,
    })
}

/// Runs one round of the exchange a handshake chose, on a `Negotiating` or
/// `Reauthenticating` connection, and accounts for it.
///
/// A mechanism failure that carries no client-facing text gets Kafka's
/// generic invalid-credentials sentence here, where the mechanism and the
/// phase are known.
async fn run_authenticate(
    broker: &Broker,
    req: &SaslAuthenticateRequest,
    auth: &mut ConnectionAuth,
    max_reauth: Option<krabka_units::Time>,
    peer: &SocketAddr,
) -> SaslAuthenticateResponse {
    let mech_opt = auth.negotiated_mechanism();
    let reauthentication = matches!(auth, ConnectionAuth::Reauthenticating { .. });
    let mut resp = match mech_opt {
        Some(SaslMechanism::Plain) => handle_authenticate_plain(
            req,
            auth,
            broker.config.plain_credentials.as_map(),
            max_reauth,
        ),
        Some(SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512) => handle_authenticate_scram(
            req,
            auth,
            &*broker.controller,
            broker.config.delegation_token_secret_key.as_ref(),
            max_reauth,
        ),
        Some(SaslMechanism::OAuthBearer) => {
            handle_authenticate_oauthbearer_with_jwks_cache(
                req,
                auth,
                &broker.config.oauthbearer_validator,
                &broker.config.oauthbearer_jwks_cache_generation,
                &broker.config.oauthbearer_jwks_last_successful_fetch_ms,
                crate::time_util::now_ms,
                max_reauth,
            )
            .await
        }
        Some(SaslMechanism::Gssapi) => {
            let cfg = broker
                .config
                .gssapi
                .as_ref()
                .expect("GSSAPI enabled without config");
            handle_authenticate_gssapi(req, auth, cfg, max_reauth)
        }
        // Callers only run an exchange a handshake started.
        None => SaslAuthenticateResponse {
            error_code: codes::ILLEGAL_SASL_STATE,
            error_message: None,
            ..Default::default()
        },
    };
    if let Some(mech) = mech_opt
        && resp.error_code == codes::SASL_AUTHENTICATION_FAILED
        && resp.error_message.is_none()
    {
        resp.error_message = Some(generic_failure_message(mech, reauthentication));
    }
    // Account this SaslAuthenticate frame in the per-mechanism
    // success/failure counters, under the mechanism the handshake chose.
    let mech_label = mech_opt.map_or(crate::metrics::UNKNOWN_LABEL, SaslMechanism::wire_name);
    let ok = resp.error_code == 0;
    broker.metrics.record_authentication(mech_label, ok);
    // One audit row per *completed* exchange, initial or KIP-368 re-auth
    // alike. A multi-round mechanism answers its intermediate rounds with
    // `error_code == 0` and no principal yet, so a zero code only completes
    // the exchange once the connection is authenticated. A non-zero code
    // always ends it.
    if !ok || auth.is_authenticated() {
        emit_authentication(
            &broker.audit_log,
            peer,
            mech_label,
            auth.principal()
                .map_or_else(|| claimed_principal(mech_opt, req), audit_principal),
            if ok {
                krabka_audit::AuditOutcome::Success
            } else {
                krabka_audit::AuditOutcome::Failure
            },
            resp.error_message.clone(),
        );
    }
    resp
}

/// Writes one `Authentication` audit row.
///
/// Every credential presentation the broker completes goes through here: a
/// `SaslAuthenticate` exchange that ended, the pre-auth gate that refused a
/// frame outright, and the mTLS binding a non-SASL listener does at accept
/// time. `source` is built the same way [`crate::handlers::admin_audit`]
/// builds it, so an auditor can join an authentication row to the
/// privileged-action rows the same session went on to write.
pub(crate) fn emit_authentication(
    audit_log: &krabka_audit::AuditLog,
    peer: &SocketAddr,
    mechanism: &str,
    principal: krabka_audit::AuditPrincipal,
    outcome: krabka_audit::AuditOutcome,
    reason: Option<String>,
) {
    audit_log.emit(krabka_audit::AuditEvent::Authentication {
        outcome,
        mechanism: mechanism.to_string(),
        principal,
        source: krabka_audit::AuditEndpoint {
            ip: peer.ip().to_string(),
            port: peer.port(),
        },
        reason,
        time_ms: crate::time_util::now_ms(),
    });
}

/// Renders a resolved [`Principal`] as an audit principal.
///
/// The name is the `User:<name>` Kafka form, which is what
/// `break_glass::handlers::principal_name` puts on a `PrivilegedAction` row.
/// An auditor joins the two by that string, so the two sites have to spell a
/// principal the same way.
pub(crate) fn audit_principal(principal: &Principal) -> krabka_audit::AuditPrincipal {
    krabka_audit::AuditPrincipal {
        name: principal.to_kafka().to_string(),
        auth_method: format!("{:?}", principal.auth_method),
    }
}

/// Names the identity a *failed* exchange claimed, where the connection state
/// resolved no principal.
///
/// PLAIN is the one mechanism that sends the user in the clear
/// (`\0<authzid>\0<authcid>\0<password>`), so its failure row can still say
/// who was refused. Every other mechanism keeps the identity inside its own
/// challenge/response and its failure row names no user.
// ponytail: PLAIN only. SCRAM's client-first `n=<user>` would need a GS2
// header parser here if a failed SCRAM row ever has to name the claimed user.
fn claimed_principal(
    mechanism: Option<SaslMechanism>,
    req: &SaslAuthenticateRequest,
) -> krabka_audit::AuditPrincipal {
    let name = match mechanism {
        Some(SaslMechanism::Plain) => req
            .auth_bytes
            .split(|&b| b == 0)
            .nth(1)
            .and_then(|user| std::str::from_utf8(user).ok())
            // The same `User:<name>` form [`audit_principal`] renders, so a
            // failed row and a successful one name one person identically.
            .map(|user| format!("User:{user}"))
            .unwrap_or_default(),
        _ => String::new(),
    };
    krabka_audit::AuditPrincipal {
        name,
        auth_method: format!(
            "{:?}",
            mechanism.map_or(AuthMethod::Anonymous, |m| { AuthMethod::from_sasl(m) })
        ),
    }
}

fn handle_sasl_handshake(
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &mut ConnectionAuth,
    sasl_mechanisms: &[SaslMechanism],
    session: &mut SaslSession,
) -> Result<(Bytes, bool), BrokerError> {
    use krabka_protocol::{Decode, Encode};

    let mut body = parsed.body;
    let request = SaslHandshakeRequest::decode(&mut body, parsed.api_version)?;
    let outcome = handle_handshake(
        &request,
        auth,
        sasl_mechanisms,
        &mut ReauthClock {
            now_ms: crate::time_util::now_ms(),
            last_start_ms: &mut session.last_reauth_start_ms,
        },
    );
    let response = outcome.response;
    let mut encoded = BytesMut::with_capacity(response.encoded_len(parsed.api_version));
    response.encode(&mut encoded, parsed.api_version)?;
    Ok((encoded.freeze(), outcome.close_after))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn request(
        auth_bytes: &[u8],
    ) -> krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
        krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
            auth_bytes: Bytes::copy_from_slice(auth_bytes),
            ..Default::default()
        }
    }

    /// A failed exchange resolved no principal, so the audit row can only name
    /// what the peer claimed. PLAIN is the one mechanism that says it in the
    /// clear, and the field the row must name is the *authcid* — the field
    /// after it is the password, which must never reach an audit sink. Every
    /// other mechanism keeps the identity inside its own challenge/response,
    /// so its row names nobody rather than guessing.
    #[test]
    fn a_refused_login_names_the_claimed_plain_user_and_never_the_password() {
        let cases = [
            (
                "plain names the authcid",
                Some(SaslMechanism::Plain),
                request(b"\0alice\0wonderland"),
                "User:alice",
                "SaslPlain",
            ),
            (
                "plain with an authzid still names the authcid",
                Some(SaslMechanism::Plain),
                request(b"admin\0alice\0wonderland"),
                "User:alice",
                "SaslPlain",
            ),
            (
                "a plain payload with no fields names nobody",
                Some(SaslMechanism::Plain),
                request(b"garbage"),
                "",
                "SaslPlain",
            ),
            (
                "a non-utf8 plain username names nobody",
                Some(SaslMechanism::Plain),
                request(&[0, 0xff, 0xfe, 0, b'p', b'w']),
                "",
                "SaslPlain",
            ),
            (
                "scram keeps its identity to itself",
                Some(SaslMechanism::ScramSha512),
                request(b"n,,n=alice,r=nonce"),
                "",
                "SaslScramSha512",
            ),
            (
                "no handshake ran at all",
                None,
                request(b"\0alice\0wonderland"),
                "",
                "Anonymous",
            ),
        ];
        for (case, mechanism, req, name, auth_method) in cases {
            check!(
                claimed_principal(mechanism, &req)
                    == krabka_audit::AuditPrincipal {
                        name: name.to_string(),
                        auth_method: auth_method.to_string(),
                    },
                "{case}"
            );
        }
    }

    /// An auditor joins an authentication row to the privileged-action rows
    /// the same session went on to write, by the principal string. That join
    /// only holds while this renders the `User:<name>` Kafka form that
    /// `break_glass::handlers::principal_name` puts on the other side.
    #[test]
    fn a_resolved_principal_is_audited_in_the_kafka_user_form() {
        let principal = krabka_security::Principal {
            name: "alice".to_string(),
            auth_method: krabka_security::AuthMethod::SaslScramSha512,
            groups: vec![],
        };
        assert!(
            audit_principal(&principal)
                == krabka_audit::AuditPrincipal {
                    name: "User:alice".to_string(),
                    auth_method: "SaslScramSha512".to_string(),
                }
        );
    }

    /// The refusal an operator has to be able to see after the fact: one
    /// `Authentication` row, `Failure`, naming the mechanism, the user the
    /// peer claimed, the peer's own address, and the reason.
    #[test]
    fn a_refused_plain_login_emits_one_failure_row_for_the_claimed_user() {
        let peer: SocketAddr = "192.0.2.9:9092".parse().expect("peer addr");
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        emit_authentication(
            log.as_ref(),
            &peer,
            SaslMechanism::Plain.wire_name(),
            claimed_principal(Some(SaslMechanism::Plain), &request(b"\0alice\0wonderland")),
            krabka_audit::AuditOutcome::Failure,
            Some("authentication failed".to_string()),
        );

        let event = rx.try_recv().expect("the failed authentication row");
        crate::network::test_support::assert_authentication_event(
            &event,
            krabka_audit::AuditOutcome::Failure,
            "PLAIN",
            ("User:alice", "SaslPlain"),
            ("192.0.2.9", 9092),
            Some("authentication failed".to_string()),
        );
        assert!(rx.try_recv().is_err(), "exactly one row per refusal");
    }

    fn sasl_listener(mechanisms: &[SaslMechanism]) -> SaslListener<'_> {
        SaslListener {
            is_sasl: true,
            mechanisms,
            max_reauth: None,
        }
    }

    #[derive(Clone, Copy)]
    struct ParsedApiVersion(i16);

    #[derive(Clone, Copy, Default)]
    enum BodyEncoding {
        #[default]
        Flexible,
        Legacy,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ParsedSetup<'a> {
        #[default(SASL_AUTHENTICATE_KEY)]
        api_key: ApiKeyCode,
        #[default(ParsedApiVersion(2))]
        api_version: ParsedApiVersion,
        body: &'a [u8],
        encoding: BodyEncoding,
    }

    fn parsed(setup: ParsedSetup<'_>) -> crate::network::request::ParsedRequest<'_> {
        crate::network::request::ParsedRequest {
            api_key: setup.api_key,
            api_version: setup.api_version.0,
            correlation_id: 7,
            body: setup.body,
            body_flexible: matches!(setup.encoding, BodyEncoding::Flexible),
            client_id: None,
        }
    }

    fn encode_body<T: krabka_protocol::Encode>(req: &T, version: i16) -> BytesMut {
        let mut buf = BytesMut::with_capacity(req.encoded_len(version));
        req.encode(&mut buf, version).expect("encode request body");
        buf
    }

    /// Strips the `ResponseHeader` an interception framed: `correlation_id`,
    /// plus the tagged-fields byte a flexible response carries.
    fn authenticate_response(
        bytes: &Bytes,
    ) -> krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
        use krabka_protocol::Decode as _;
        let mut cur = &bytes[5..];
        krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse::decode(
            &mut cur, 2,
        )
        .expect("decode the framed SaslAuthenticate response")
    }

    fn plain_payload(user: &str, password: &str) -> BytesMut {
        encode_body(
            &crate::network::auth::test_support::plain_request(user, password),
            2,
        )
    }

    async fn intercepted_authenticate(
        broker: &Broker,
        body: &[u8],
        auth: &mut ConnectionAuth,
        mechanisms: &[SaslMechanism],
        peer: &SocketAddr,
    ) -> SaslFrameOutcome {
        try_handle_sasl_frame(
            broker,
            &parsed(ParsedSetup {
                body,
                ..Default::default()
            }),
            auth,
            &sasl_listener(mechanisms),
            &mut SaslSession::default(),
            peer,
        )
        .await
        .expect("36 is intercepted")
        .expect("authenticate encodes")
    }

    async fn kept_authenticate(
        broker: &Broker,
        body: &[u8],
        auth: &mut ConnectionAuth,
        mechanisms: &[SaslMechanism],
        peer: &SocketAddr,
    ) -> SaslFrameOutcome {
        let outcome = intercepted_authenticate(broker, body, auth, mechanisms, peer).await;
        check!(!outcome.close_after);
        outcome
    }

    /// The interception boundary and the close decision, which are the two
    /// things the dispatch loop takes from this module.
    ///
    /// Only 17 and 36 may be taken out of the registry's hands — intercepting
    /// anything else would swallow the whole data plane. A refused credential
    /// and a `SaslAuthenticate` with no handshake behind it both have to set
    /// the close flag, because a peer that may keep guessing on the same
    /// connection is an open credential-stuffing window; a successful round
    /// must not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sasl_frames_are_intercepted_and_only_a_refusal_closes_the_connection() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = crate::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.plain_credentials
            .insert("alice".to_string(), "wonderland".to_string());
        let handle = crate::Broker::start(cfg).await.expect("broker start");
        let broker = std::sync::Arc::clone(handle.broker_for_test());
        let peer: SocketAddr = "192.0.2.9:9092".parse().expect("peer addr");
        let mechanisms = [SaslMechanism::Plain];

        let mut auth = crate::network::auth::ConnectionAuth::Anonymous;

        // A non-SASL api_key is left to the registry.
        let metadata = encode_body(
            &krabka_protocol::owned::metadata_request::MetadataRequest::default(),
            12,
        );
        check!(
            try_handle_sasl_frame(
                &broker,
                &parsed(ParsedSetup {
                    api_key: 3,
                    api_version: ParsedApiVersion(12),
                    body: &metadata,
                    ..Default::default()
                }),
                &mut auth,
                &sasl_listener(&mechanisms),
                &mut SaslSession::default(),
                &peer,
            )
            .await
            .is_none()
        );

        // SaslHandshake opens the exchange and keeps the connection.
        let handshake = encode_body(
            &krabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest {
                mechanism: "PLAIN".to_string(),
                ..Default::default()
            },
            1,
        );
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(ParsedSetup {
                api_key: SASL_HANDSHAKE_KEY,
                api_version: ParsedApiVersion(1),
                body: &handshake,
                encoding: BodyEncoding::Legacy,
            }),
            &mut auth,
            &sasl_listener(&mechanisms),
            &mut SaslSession::default(),
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        check!(!outcome.close_after);
        check!(auth.negotiated_mechanism() == Some(SaslMechanism::Plain));

        // The credential that matches authenticates and keeps the connection.
        let good = plain_payload("alice", "wonderland");
        let outcome = kept_authenticate(&broker, &good, &mut auth, &mechanisms, &peer).await;
        check!(authenticate_response(&outcome.response_bytes).error_code == 0);
        check!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));

        // A credential that does not match closes the connection.
        let mut guessing = crate::network::auth::ConnectionAuth::Anonymous;
        try_handle_sasl_frame(
            &broker,
            &parsed(ParsedSetup {
                api_key: SASL_HANDSHAKE_KEY,
                api_version: ParsedApiVersion(1),
                body: &handshake,
                encoding: BodyEncoding::Legacy,
            }),
            &mut guessing,
            &sasl_listener(&mechanisms),
            &mut SaslSession::default(),
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        let bad = plain_payload("alice", "hunter2");
        let outcome =
            intercepted_authenticate(&broker, &bad, &mut guessing, &mechanisms, &peer).await;
        check!(outcome.close_after, "a refused credential must close");
        check!(
            authenticate_response(&outcome.response_bytes)
                == krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::SASL_AUTHENTICATION_FAILED,
                    error_message: Some(
                        "Authentication failed: Invalid username or password".into()
                    ),
                    ..Default::default()
                }
        );
        check!(!guessing.is_authenticated());

        // A SaslAuthenticate with no handshake behind it closes the connection
        // with no response, rather than being fed to some default mechanism.
        let mut bare = crate::network::auth::ConnectionAuth::Anonymous;
        let outcome = try_handle_sasl_frame(
            &broker,
            &parsed(ParsedSetup {
                body: &good,
                ..Default::default()
            }),
            &mut bare,
            &sasl_listener(&mechanisms),
            &mut SaslSession::default(),
            &peer,
        )
        .await
        .expect("36 is intercepted");
        check!(outcome.is_err());
        check!(!bare.is_authenticated());

        // A SaslAuthenticate after authentication completed is Kafka's
        // `KafkaApis` answer: ILLEGAL_SASL_STATE, and the connection and its
        // principal stay.
        let outcome = kept_authenticate(&broker, &good, &mut auth, &mechanisms, &peer).await;
        check!(
            authenticate_response(&outcome.response_bytes)
                == krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::ILLEGAL_SASL_STATE,
                    error_message: Some(AUTHENTICATE_AFTER_AUTHENTICATION.into()),
                    ..Default::default()
                }
        );
        check!(auth.principal().map(|p| p.name.as_str()) == Some("alice"));

        // The frame is routed by the mechanism the handshake named, not by a
        // default one: a SCRAM client-first reaches the SCRAM handler, which
        // knows no such user and refuses.
        let mut scram = crate::network::auth::ConnectionAuth::Anonymous;
        let scram_handshake = encode_body(
            &krabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest {
                mechanism: "SCRAM-SHA-512".to_string(),
                ..Default::default()
            },
            1,
        );
        try_handle_sasl_frame(
            &broker,
            &parsed(ParsedSetup {
                api_key: SASL_HANDSHAKE_KEY,
                api_version: ParsedApiVersion(1),
                body: &scram_handshake,
                encoding: BodyEncoding::Legacy,
            }),
            &mut scram,
            &sasl_listener(&[SaslMechanism::ScramSha512]),
            &mut SaslSession::default(),
            &peer,
        )
        .await
        .expect("17 is intercepted")
        .expect("handshake encodes");
        check!(scram.negotiated_mechanism() == Some(SaslMechanism::ScramSha512));
        let client_first = encode_body(
            &krabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest {
                auth_bytes: Bytes::from_static(b"n,,n=nobody,r=clientnonce"),
                ..Default::default()
            },
            2,
        );
        let outcome = intercepted_authenticate(
            &broker,
            &client_first,
            &mut scram,
            &[SaslMechanism::ScramSha512],
            &peer,
        )
        .await;
        check!(outcome.close_after);
        // SCRAM raises no client-facing text, so Kafka's generic sentence.
        check!(
            authenticate_response(&outcome.response_bytes)
                == krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse {
                    error_code: codes::SASL_AUTHENTICATION_FAILED,
                    error_message: Some(
                        "Authentication failed during authentication due to invalid \
                         credentials with SASL mechanism SCRAM-SHA-512"
                            .into()
                    ),
                    ..Default::default()
                }
        );
        check!(!scram.is_authenticated());

        handle.shutdown().await;
    }
}
