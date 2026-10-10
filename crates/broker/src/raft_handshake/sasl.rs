//! Server-side SASL negotiation for the controller listener.
//!
//! The loop here drives the `network::auth` state machine one Kafka frame at
//! a time: it answers a pre-auth `ApiVersions`, runs `SaslHandshake` to pick
//! a mechanism, and then runs as many `SaslAuthenticate` rounds as the
//! mechanism needs. It returns the authenticated `Principal` to the caller,
//! which authorizes it before the raft engine takes the connection.

use std::net::SocketAddr;

use krabka_client_core::ClientDuplex;
use krabka_protocol::{
    Decode,
    owned::{
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_raft::{ControllerApiVersions, RaftHandshakeError};
use krabka_security::SaslMechanism;

use super::{
    API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, API_KEY_SASL_HANDSHAKE, BrokerRaftHandshake,
    frame::{read_kafka_request, write_response, write_response_body},
};
use crate::network::auth::{
    ConnectionAuth, ReauthClock, generic_failure_message, handle_authenticate_gssapi,
    handle_authenticate_oauthbearer_with_jwks_cache, handle_authenticate_plain,
    handle_authenticate_scram, handle_handshake,
};

/// Initial per-connection auth state for an unauthenticated SASL peer.
fn pre_auth_state() -> ConnectionAuth {
    ConnectionAuth::Anonymous
}

/// Regular controller credentials have no configured reauthentication cap.
/// OAuth and delegation-token credentials retain their own absolute deadline,
/// which the controller checks before each request. This listener currently
/// requires reconnecting to reauthenticate; its post-handshake SASL APIs return
/// `ILLEGAL_SASL_STATE`.
const CONTROLLER_MAX_REAUTH: Option<krabka_units::Time> = None;

/// Waits out Kafka's `connection.failed.authentication.delay.ms`, which holds
/// a failed authentication's answer and its close back. The controller
/// listener's `Selector` delays them as a broker listener's does.
async fn delay_failed_authentication(cfg: &BrokerRaftHandshake) {
    if !cfg.failed_authentication_delay.is_zero() {
        tokio::time::sleep(cfg.failed_authentication_delay).await;
    }
}

/// Writes one `Authentication` audit row for a completed controller SASL
/// exchange, so controller and inter-broker logins join the same audit trail
/// as the data plane's.
///
/// The principal is spelled in the `User:<name>` Kafka form, the same one
/// `network::dispatch::sasl` and the `PrivilegedAction` rows use, so an
/// auditor can join them.
fn emit_authentication(
    cfg: &BrokerRaftHandshake,
    peer: &SocketAddr,
    mechanism: SaslMechanism,
    principal: Option<&krabka_security::Principal>,
    outcome: krabka_audit::AuditOutcome,
    reason: Option<String>,
) {
    // The audit pipeline is built after the controller listener starts
    // accepting, so the cell is empty for the connections that arrive first.
    let Some(audit_log) = cfg.audit_log.get() else {
        return;
    };
    // A refused exchange resolved no principal, so its row names no user and
    // carries the mechanism's own auth method instead.
    let principal = principal.map_or_else(
        || krabka_audit::AuditPrincipal {
            name: String::new(),
            auth_method: format!("{:?}", krabka_security::AuthMethod::from_sasl(mechanism)),
        },
        crate::network::dispatch::sasl::audit_principal,
    );
    crate::network::dispatch::sasl::emit_authentication(
        audit_log,
        peer,
        mechanism.wire_name(),
        principal,
        outcome,
        reason,
    );
}

/// Drives the server-side SASL state machine until the connection
/// authenticates or the function writes an error response.
///
/// The loop invariant is that every iteration reads exactly one Kafka request
/// frame and writes exactly one response frame. The `auth` state machine,
/// `network::auth::ConnectionAuth`, carries continuation state across SCRAM
/// rounds.
///
/// The function returns the authenticated [`Principal`] and whether a
/// delegation token supplied the credential once
/// `auth.is_authenticated()` holds, so that `upgrade` can authorize it. It
/// returns `Err(...)` if the peer sent an unexpected frame or the auth
/// failed.
///
/// A pre-authentication `ApiVersions` gets the listener's own answer from
/// `api_versions`, as Kafka's `SaslServerAuthenticator` answers from the
/// `apiVersionSupplier` of the listener. That answer is the full table and the
/// features, or an `UNSUPPORTED_VERSION` or `INVALID_REQUEST` refusal. A
/// refusal keeps the connection open, so the peer can ask again.
pub(super) async fn run_inbound_sasl(
    stream: &mut dyn ClientDuplex,
    cfg: &BrokerRaftHandshake,
    peer: &SocketAddr,
    api_versions: &dyn ControllerApiVersions,
) -> Result<(krabka_security::Principal, bool, Option<i64>), RaftHandshakeError> {
    let mut auth = pre_auth_state();
    // A valid `ApiVersions` has been answered before any handshake: Kafka's
    // `SaslServerAuthenticator` then takes only a `SaslHandshake`.
    let mut api_versions_answered = false;
    loop {
        let request = read_kafka_request(stream, cfg.sasl_max_receive_bytes).await;
        if matches!(request, Err(RaftHandshakeError::Sasl(_))) {
            delay_failed_authentication(cfg).await;
        }
        let (api_key, api_version, corr_id, body) = request?;
        let repeated_api_versions = api_versions_answered
            && api_key == API_KEY_API_VERSIONS
            && matches!(auth, ConnectionAuth::Anonymous);
        if repeated_api_versions || !auth.allows_request(api_key) {
            // Before a handshake Kafka fails the request with an
            // `InvalidRequestException`, or an `IllegalStateException` for a
            // second `ApiVersions`, and closes at once. After one it is an
            // `AuthenticationException`, and the close is delayed.
            if !matches!(auth, ConnectionAuth::Anonymous) {
                delay_failed_authentication(cfg).await;
            }
            return Err(RaftHandshakeError::Sasl(format!(
                "pre-auth request api_key={api_key} rejected"
            )));
        }
        match api_key {
            // A JVM client sends ApiVersions first. It gets the same answer
            // as after authentication.
            API_KEY_API_VERSIONS => {
                let response = api_versions.respond(api_version, &body)?;
                write_response_body(stream, api_key, api_version, corr_id, &response).await?;
                // The answer starts with its int16 error code, at every
                // version. Only a valid request gets none, and an
                // `UNSUPPORTED_VERSION` or `INVALID_REQUEST` answer leaves
                // Kafka's state as it was, so the peer may ask again.
                api_versions_answered = response.starts_with(&[0, 0]);
            }
            API_KEY_SASL_HANDSHAKE => {
                let mut cur = body.as_slice();
                let req = SaslHandshakeRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                // The loop returns once authenticated, so this handshake is
                // never a re-authentication and the clock is never read.
                let outcome = handle_handshake(
                    &req,
                    &mut auth,
                    &cfg.enabled_sasl_mechanisms,
                    &mut ReauthClock {
                        now_ms: 0,
                        last_start_ms: &mut None,
                    },
                );
                let resp = outcome.response;
                let error_code = resp.error_code;
                write_sasl_response(
                    stream,
                    cfg,
                    (api_key, api_version, corr_id),
                    error_code,
                    &resp,
                )
                .await?;
                if error_code != 0 {
                    return Err(RaftHandshakeError::Sasl(format!(
                        "handshake error_code={error_code}"
                    )));
                }
            }
            API_KEY_SASL_AUTHENTICATE => {
                let mut cur = body.as_slice();
                let req = SaslAuthenticateRequest::decode(&mut cur, api_version)
                    .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
                let mech = match &auth {
                    ConnectionAuth::Negotiating { mechanism, .. } => *mechanism,
                    _ => {
                        return Err(RaftHandshakeError::Sasl(
                            "authenticate before handshake".into(),
                        ));
                    }
                };
                let mut resp = match mech {
                    SaslMechanism::Plain => handle_authenticate_plain(
                        &req,
                        &mut auth,
                        &cfg.plain_credentials,
                        CONTROLLER_MAX_REAUTH,
                    ),
                    SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                        let controller = cfg.controller.get().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "controller handle not initialised for SCRAM lookup".into(),
                            )
                        })?;
                        handle_authenticate_scram(
                            &req,
                            &mut auth,
                            controller.as_ref(),
                            cfg.delegation_token_secret_key.as_ref(),
                            CONTROLLER_MAX_REAUTH,
                        )
                    }
                    SaslMechanism::OAuthBearer => {
                        handle_authenticate_oauthbearer_with_jwks_cache(
                            &req,
                            &mut auth,
                            &cfg.oauthbearer_validator,
                            &cfg.oauthbearer_jwks_cache_generation,
                            &cfg.oauthbearer_jwks_last_successful_fetch_ms,
                            crate::time_util::now_ms,
                            CONTROLLER_MAX_REAUTH,
                        )
                        .await
                    }
                    SaslMechanism::Gssapi => {
                        let config = cfg.gssapi.as_ref().ok_or_else(|| {
                            RaftHandshakeError::Sasl(
                                "GSSAPI enabled on controller listener without configuration"
                                    .into(),
                            )
                        })?;
                        handle_authenticate_gssapi(&req, &mut auth, config, CONTROLLER_MAX_REAUTH)
                    }
                };
                if resp.error_code == crate::codes::SASL_AUTHENTICATION_FAILED
                    && resp.error_message.is_none()
                {
                    resp.error_message = Some(generic_failure_message(mech, false));
                }
                let error_code = resp.error_code;
                write_sasl_response(
                    stream,
                    cfg,
                    (api_key, api_version, corr_id),
                    error_code,
                    &resp,
                )
                .await?;
                if error_code != 0 {
                    emit_authentication(
                        cfg,
                        peer,
                        mech,
                        auth.principal(),
                        krabka_audit::AuditOutcome::Failure,
                        resp.error_message.clone(),
                    );
                    return Err(RaftHandshakeError::Sasl(format!(
                        "authenticate error_code={error_code}"
                    )));
                }
                if let ConnectionAuth::Authenticated {
                    principal,
                    authenticated_via_token,
                    expires_at_ms,
                    ..
                } = &auth
                {
                    emit_authentication(
                        cfg,
                        peer,
                        mech,
                        Some(principal),
                        krabka_audit::AuditOutcome::Success,
                        None,
                    );
                    return Ok((principal.clone(), *authenticated_via_token, *expires_at_ms));
                }
                // Multi-round mechanisms and the RFC 7628 rejection exchange
                // loop for the next `SaslAuthenticate` frame.
                assert2::assert!(
                    matches!(
                        &auth,
                        ConnectionAuth::Negotiating { exchange, .. }
                            if exchange.awaits_client_round()
                    ),
                    "expected SASL continuation after non-authenticated success"
                );
            }
            other => {
                return Err(RaftHandshakeError::Protocol(format!(
                    "unexpected api_key={other} during handshake"
                )));
            }
        }
    }
}

#[cfg(test)]
mod oauth_cache_tests;

async fn write_sasl_response<R: krabka_protocol::Encode>(
    stream: &mut dyn ClientDuplex,
    cfg: &BrokerRaftHandshake,
    request: (i16, i16, i32),
    error_code: i16,
    response: &R,
) -> Result<(), RaftHandshakeError> {
    if error_code != 0 {
        delay_failed_authentication(cfg).await;
    }
    write_response(stream, request.0, request.1, request.2, response).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::raft_handshake::test_support::{
        HandshakeApiKey, HandshakeApiVersion, HandshakeCorrelationId, HandshakeHeader,
        RequestFrameSetup, api_versions_body, read_response_frame, request_frame,
        sasl_authenticate_body, sasl_handshake_body, sasl_test_config,
    };

    fn test_peer() -> SocketAddr {
        "192.0.2.11:9093".parse().expect("peer addr")
    }

    type SaslResult = Result<(krabka_security::Principal, bool, Option<i64>), RaftHandshakeError>;

    fn sasl_server(
        capacity: usize,
        configure: impl FnOnce() -> BrokerRaftHandshake + Send + 'static,
        api_versions: impl ControllerApiVersions + 'static,
    ) -> (tokio::io::DuplexStream, tokio::task::JoinHandle<SaslResult>) {
        let (client, mut server) = tokio::io::duplex(capacity);
        let task = tokio::spawn(async move {
            let cfg = configure();
            run_inbound_sasl(&mut server, &cfg, &test_peer(), &api_versions).await
        });
        (client, task)
    }

    async fn plain_handshake(client: &mut tokio::io::DuplexStream, correlation_id: i32) -> Vec<u8> {
        client
            .write_all(&request_frame(RequestFrameSetup {
                correlation: HandshakeCorrelationId(correlation_id),
                body: &sasl_handshake_body(),
                ..Default::default()
            }))
            .await
            .expect("write handshake");
        let handshake = read_response_frame(client).await;
        assert!(&handshake[4..6] == &0i16.to_be_bytes());
        handshake
    }

    async fn api_versions_round_trip(
        client: &mut tokio::io::DuplexStream,
        version: i16,
        correlation_id: i32,
        body: &[u8],
    ) -> Vec<u8> {
        client
            .write_all(&request_frame(RequestFrameSetup {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                api_version: HandshakeApiVersion(version),
                correlation: HandshakeCorrelationId(correlation_id),
                header: HandshakeHeader::Flexible,
                body,
                ..Default::default()
            }))
            .await
            .expect("write api versions");
        read_response_frame(client).await
    }

    fn audited_config() -> (BrokerRaftHandshake, krabka_audit::AuditReceiver) {
        let cfg = sasl_test_config();
        let (log, rx) = krabka_audit::AuditLog::new(8);
        cfg.audit_log.set(log).expect("audit cell unset");
        (cfg, rx)
    }

    /// Echoes the request version and body, so a test sees which request the
    /// handshake answered and that it wrote the answer unchanged.
    struct FixedApiVersions;

    impl ControllerApiVersions for FixedApiVersions {
        fn respond(
            &self,
            request_version: i16,
            request_body: &[u8],
        ) -> Result<bytes::Bytes, RaftHandshakeError> {
            let mut body = request_version.to_be_bytes().to_vec();
            body.extend_from_slice(request_body);
            Ok(bytes::Bytes::from(body))
        }
    }

    async fn rejected_plain_login(
        cfg: BrokerRaftHandshake,
    ) -> (SaslAuthenticateResponse, SaslResult) {
        let (response, outcome) = plain_login(cfg, "broker", "wrong").await;
        assert!(response.error_code != 0);
        assert!(outcome.is_err());
        (response, outcome)
    }

    /// Drives one PLAIN exchange (handshake then authenticate) against
    /// `run_inbound_sasl` and returns the decoded `SaslAuthenticate` response
    /// with the loop's outcome.
    async fn plain_login(
        cfg: BrokerRaftHandshake,
        user: &str,
        password: &str,
    ) -> (
        SaslAuthenticateResponse,
        Result<(krabka_security::Principal, bool, Option<i64>), RaftHandshakeError>,
    ) {
        let (mut client, task) = sasl_server(4096, move || cfg, FixedApiVersions);
        let _handshake = plain_handshake(&mut client, 1).await;

        client
            .write_all(&request_frame(RequestFrameSetup {
                api_key: HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
                api_version: HandshakeApiVersion(2),
                correlation: HandshakeCorrelationId(2),
                header: HandshakeHeader::Flexible,
                body: &sasl_authenticate_body(user, password),
                ..Default::default()
            }))
            .await
            .expect("write authenticate");
        let frame = read_response_frame(&mut client).await;
        // correlation_id (4 bytes) + the flexible header's tagged-fields byte.
        let mut body = &frame[5..];
        let resp =
            SaslAuthenticateResponse::decode(&mut body, 2).expect("decode authenticate response");
        (resp, task.await.expect("server task"))
    }

    /// The controller path hands the raw stream to the raft engine, so it must
    /// not promise a re-auth deadline nothing later enforces.
    #[tokio::test]
    async fn run_inbound_sasl_advertises_no_session_lifetime() {
        let (resp, outcome) = plain_login(sasl_test_config(), "broker", "secret").await;
        assert!(resp.error_code == 0);
        assert!(resp.session_lifetime_ms == 0);
        assert!(outcome.is_ok());
    }

    #[tokio::test]
    async fn run_inbound_sasl_audits_the_successful_controller_login() {
        let (cfg, mut rx) = audited_config();

        let (resp, outcome) = plain_login(cfg, "broker", "secret").await;
        assert!(resp.error_code == 0);
        assert!(outcome.is_ok());

        let event = rx.try_recv().expect("the controller authentication row");
        crate::network::test_support::assert_authentication_event(
            &event,
            krabka_audit::AuditOutcome::Success,
            "PLAIN",
            ("User:broker", "SaslPlain"),
            ("192.0.2.11", 9093),
            None,
        );
    }

    #[tokio::test]
    async fn run_inbound_sasl_audits_the_failed_controller_login() {
        let (cfg, mut rx) = audited_config();

        let (resp, _outcome) = rejected_plain_login(cfg).await;

        let event = rx.try_recv().expect("the controller authentication row");
        crate::network::test_support::assert_authentication_event(
            &event,
            krabka_audit::AuditOutcome::Failure,
            "PLAIN",
            ("", "SaslPlain"),
            ("192.0.2.11", 9093),
            resp.error_message.clone(),
        );
    }

    #[tokio::test]
    async fn run_inbound_sasl_allows_api_versions_before_plain_authentication() {
        let (mut client, server) = sasl_server(4096, sasl_test_config, FixedApiVersions);

        // A served version and a version the listener does not serve. The
        // listener's answer goes out verbatim behind a v0 response header, and
        // neither answer ends the exchange.
        for (corr_id, version, body) in [
            (1, 3, api_versions_body(HandshakeApiVersion(3))),
            (4, 6, vec![0xff]),
        ] {
            let frame = api_versions_round_trip(&mut client, version, corr_id, &body).await;
            let mut expected = corr_id.to_be_bytes().to_vec();
            expected.extend_from_slice(&FixedApiVersions.respond(version, &body).unwrap());
            assert!(frame == expected, "ApiVersions v{version}");
        }

        let handshake = plain_handshake(&mut client, 2).await;
        assert!(&handshake[0..4] == &2i32.to_be_bytes());

        client
            .write_all(&request_frame(RequestFrameSetup {
                api_key: HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
                api_version: HandshakeApiVersion(2),
                correlation: HandshakeCorrelationId(3),
                header: HandshakeHeader::Flexible,
                body: &sasl_authenticate_body("broker", "secret"),
                ..Default::default()
            }))
            .await
            .expect("write authenticate");
        let authenticate = read_response_frame(&mut client).await;
        // corr_id 3 BE + empty tagged-fields byte (flexible header) +
        // error_code 0.
        assert!(authenticate[0..7] == [0, 0, 0, 3, 0, 0, 0]);

        let (principal, via_token, expires_at_ms) =
            server.await.expect("server task").expect("authenticated");
        assert!(principal.name == "broker");
        assert!(principal.auth_method == krabka_security::AuthMethod::SaslPlain);
        assert!(!via_token);
        assert!(expires_at_ms.is_none());
    }

    #[tokio::test]
    async fn run_inbound_sasl_rejects_disallowed_request_before_authentication() {
        let (mut client, server) = sasl_server(128, sasl_test_config, FixedApiVersions);
        client
            .write_all(&request_frame(RequestFrameSetup {
                api_key: HandshakeApiKey(1),
                api_version: HandshakeApiVersion(0),
                ..Default::default()
            }))
            .await
            .expect("write forbidden request");

        let err = server
            .await
            .expect("server task")
            .expect_err("pre-auth request rejected");
        assert!(
            matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("pre-auth request api_key=1 rejected"))
        );
    }

    /// An `ApiVersions` answer with the error code a real listener gives:
    /// none up to v4, and `UNSUPPORTED_VERSION` above it or for an empty body.
    struct CodedApiVersions;

    impl ControllerApiVersions for CodedApiVersions {
        fn respond(
            &self,
            request_version: i16,
            request_body: &[u8],
        ) -> Result<bytes::Bytes, RaftHandshakeError> {
            let error_code: i16 = if request_version > 4 || request_body.is_empty() {
                35
            } else {
                0
            };
            Ok(bytes::Bytes::from(error_code.to_be_bytes().to_vec()))
        }
    }

    /// After one valid `ApiVersions` the exchange takes only a
    /// `SaslHandshake`: Kafka's `SaslServerAuthenticator` throws
    /// `IllegalStateException` for a second one. An answer that refused the
    /// request, `UNSUPPORTED_VERSION` here, is not the first one.
    #[tokio::test]
    async fn a_second_api_versions_before_the_handshake_ends_the_exchange() {
        let (mut client, server) = sasl_server(4096, sasl_test_config, CodedApiVersions);

        // (version, body, error code of the answer)
        let requests = [
            (6, vec![0xff], 35),
            (3, api_versions_body(HandshakeApiVersion(3)), 0),
        ];
        for (corr_id, (version, body, error_code)) in (1..).zip(requests) {
            let frame = api_versions_round_trip(&mut client, version, corr_id, &body).await;
            assert!(frame[4..6] == i16::to_be_bytes(error_code), "v{version}");
        }
        client
            .write_all(&request_frame(RequestFrameSetup {
                api_key: HandshakeApiKey(API_KEY_API_VERSIONS),
                api_version: HandshakeApiVersion(3),
                correlation: HandshakeCorrelationId(3),
                header: HandshakeHeader::Flexible,
                body: &api_versions_body(HandshakeApiVersion(3)),
                ..Default::default()
            }))
            .await
            .expect("write the second api versions");

        let err = server
            .await
            .expect("server task")
            .expect_err("the second ApiVersions ends the exchange");
        assert!(
            matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("api_key=18 rejected"))
        );
    }

    /// A frame over `sasl.server.max.receive.size` fails the authentication
    /// before the frame is read: the stream holds the size prefix and nothing
    /// after it.
    #[tokio::test]
    async fn a_frame_over_the_sasl_receive_limit_fails_the_exchange() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let server = tokio::spawn(async move {
            let mut cfg = sasl_test_config();
            cfg.sasl_max_receive_bytes = 64;
            run_inbound_sasl(&mut server, &cfg, &test_peer(), &FixedApiVersions).await
        });
        client
            .write_all(&65_u32.to_be_bytes())
            .await
            .expect("write the size prefix");

        let err = server
            .await
            .expect("server task")
            .expect_err("an oversize frame fails the exchange");
        assert!(
            matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("invalid receive size"))
        );
    }

    /// A failed login writes its answer only after
    /// `connection.failed.authentication.delay.ms`, as Kafka's `Selector` holds
    /// the answer and the close of a failed authentication.
    #[tokio::test(start_paused = true)]
    async fn a_failed_login_holds_its_answer_for_the_configured_delay() {
        let delay = Duration::from_millis(500);
        let mut cfg = sasl_test_config();
        cfg.failed_authentication_delay = delay;

        let started = tokio::time::Instant::now();
        let (_resp, _outcome) = rejected_plain_login(cfg).await;
        assert!(
            started.elapsed() >= delay,
            "the answer came after {:?}",
            started.elapsed()
        );

        let started = tokio::time::Instant::now();
        let (resp, _) = plain_login(sasl_test_config(), "broker", "secret").await;
        assert!(resp.error_code == 0);
        assert!(
            started.elapsed() < delay,
            "a login that succeeds is not held"
        );
    }
}
