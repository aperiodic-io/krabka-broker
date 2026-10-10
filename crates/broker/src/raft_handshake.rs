//! Inbound TLS + SASL handshake for the controller listener.
//!
//! This module is the mirror image of the outbound auth flow of
//! `network::client::InterBrokerClient`. It reuses the
//! `network::auth::handle_handshake` and `handle_authenticate_*` state
//! machines, so the controller listener and the data plane share one source
//! of truth.
//!
//! The frame helpers `read_kafka_request` and `write_response` are the
//! server-side inverse of `network::client::round_trip`. The header
//! flexibility rules match exactly:
//!   - `SaslHandshake (17)` v0+ uses a non-flexible response header, a bare
//!     `correlation_id`.
//!   - `SaslAuthenticate (36)` v2+ uses a flexible response header, a
//!     `correlation_id` and a 1-byte tagged-fields section.
//!   - The `ApiVersions (18)` response header is *always* v0 by Kafka spec.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64},
    },
};

use krabka_client_core::ClientDuplex;
use krabka_raft::{
    ControllerApiVersions, ControllerHandle, RaftConnection, RaftHandshakeError,
    RaftListenerHandshake,
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{net::TcpStream, sync::OnceCell};
use tokio_rustls::TlsAcceptor;

mod authorization;
mod frame;
mod sasl;
#[cfg(test)]
mod test_support;

use self::sasl::run_inbound_sasl;

/// Late-bound handle to the broker's [`ControllerHandle`].
///
/// The broker constructs the handshake *before* `krabka_raft::Controller::start`
/// returns, and moves it into `ControllerConfig::handshake`, so the controller
/// is only available later. This type therefore carries an
/// `Arc<OnceCell<…>>`, and `Broker::start` calls `OnceCell::set` on it once
/// the controller is built. The SCRAM credential lookup, one round for each
/// authenticate, is the only code path that touches the cell.
pub type ControllerHandleArc = Arc<OnceCell<Arc<ControllerHandle>>>;

/// Late-bound handle to the broker's audit log.
///
/// The audit pipeline is built from the metadata source, so it does not exist
/// until after the controller listener is already accepting connections. The
/// cell is therefore filled once `Broker::start` has the log, and the SASL
/// path emits nothing for the handful of connections that may land first.
pub type AuditLogArc = Arc<OnceCell<Arc<krabka_audit::AuditLog>>>;

/// API key constants. They match the wire-protocol IDs used elsewhere.
const API_KEY_SASL_HANDSHAKE: i16 = 17;
const API_KEY_SASL_AUTHENTICATE: i16 = 36;
const API_KEY_API_VERSIONS: i16 = 18;

/// `SaslAuthenticate (36)` switches to flexible (v2) request *and* response
/// headers at this `api_version`. This is the KIP-482 flexible-versions
/// cutover.
const SASL_AUTHENTICATE_FLEXIBLE_VERSION: i16 = 2;

/// Per-broker handshake adapter. `Broker::start` constructs it and passes it
/// into `ControllerConfig::handshake`.
pub struct BrokerRaftHandshake {
    pub tls_acceptor: Option<TlsAcceptor>,
    pub plain_credentials: HashMap<String, String>,
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
    pub gssapi: Option<crate::network::auth::GssapiConfig>,
    pub oauthbearer_validator: krabka_security::OAuthBearerValidator,
    /// Same publication metadata as the shared JWKS refresher and data listener.
    pub oauthbearer_jwks_cache_generation: Arc<AtomicU64>,
    pub oauthbearer_jwks_last_successful_fetch_ms: Arc<AtomicI64>,
    pub protocol: ListenerProtocol,
    pub controller: ControllerHandleArc,
    /// Kafka's `delegation.token.secret.key`: the key that recomputes a
    /// delegation token's SCRAM password. Without one no token authenticates.
    pub delegation_token_secret_key: Option<krabka_security::SecretBytes>,
    /// Audit sink for the controller listener's own credential presentations.
    pub audit_log: AuditLogArc,
    /// Kafka's `sasl.server.max.receive.size`: the largest frame the SASL
    /// exchange reads. It replaces `socket.request.max.bytes` until the peer
    /// finishes authenticating, and a larger frame fails the authentication.
    pub sasl_max_receive_bytes: usize,
    /// Kafka's `connection.failed.authentication.delay.ms`: how long a failed
    /// SASL exchange holds its answer and the close that follows.
    pub failed_authentication_delay: std::time::Duration,
    /// Authorizer that the controller listener asks for each request.
    ///
    /// Authentication proves *who* the peer is: the SASL principal, the mTLS
    /// principal, or `ANONYMOUS`. The listener then checks the cluster
    /// operation that each api needs, as Kafka's `ControllerApis` does. The
    /// default `AllowAllAuthorizer` allows every principal.
    /// `SimpleAclAuthorizer` allows super-users and ACL grants.
    pub authorizer: Arc<dyn crate::authorizer::Authorizer>,
    /// KIP-371 `ssl.principal.mapping.rules` from the top-level
    /// `[tls_config]`, which map the Subject DN of a peer certificate to the
    /// connection principal.
    pub principal_mapper: crate::SslPrincipalMapper,
}

#[async_trait::async_trait]
impl RaftListenerHandshake for BrokerRaftHandshake {
    async fn upgrade(
        &self,
        stream: TcpStream,
        api_versions: &dyn ControllerApiVersions,
    ) -> Result<RaftConnection, RaftHandshakeError> {
        // Capture the peer address before the stream is consumed by TLS
        // termination — it is the `host` of the authorization request. When
        // there is none (a socket closed mid-accept, or a platform with
        // preopened sockets), it is the unspecified address, which no ACL
        // host pattern matches.
        let peer = crate::platform::Sockets::TARGET.peer_address(&stream);

        // 1. TLS termination (if the listener protocol requires it). A
        //    client certificate names the principal of an `SSL` connection,
        //    as on a broker listener.
        let mut certificate_principal = None;
        let mut stream: Box<dyn ClientDuplex> = if self.protocol.requires_tls() {
            let acceptor = self.tls_acceptor.clone().ok_or_else(|| {
                RaftHandshakeError::Tls("tls_config required for TLS controller listener".into())
            })?;
            let tls = acceptor
                .accept(stream)
                .await
                .map_err(|e| RaftHandshakeError::Tls(e.to_string()))?;
            certificate_principal = peer_certificate_principal(&tls, &self.principal_mapper)?;
            Box::new(tls)
        } else {
            Box::new(stream)
        };

        // 2. SASL termination (if the listener protocol requires it). The
        //    SASL principal replaces the certificate principal, as Kafka's
        //    `SaslServerAuthenticator` does on `SASL_SSL`.
        let mut principal = certificate_principal;
        let mut authenticated_via_token = false;
        let mut expires_at_ms = None;
        if self.protocol.requires_sasl() {
            let (authenticated, via_token, deadline) =
                run_inbound_sasl(&mut *stream, self, &peer, api_versions).await?;
            principal = Some(authenticated);
            authenticated_via_token = via_token;
            expires_at_ms = deadline;
        }

        // 3. Authorization runs for each request, not here. A principal
        //    without `ClusterAction` keeps its connection and can still run
        //    the KIP-919 Admin apis that its own grants allow.
        let grants = Arc::new(authorization::ControllerPeerGrants {
            authorizer: Arc::clone(&self.authorizer),
            controller: Arc::clone(&self.controller),
            principal: principal.clone().unwrap_or_else(anonymous),
            peer,
        });
        Ok(RaftConnection {
            stream,
            principal,
            authenticated_via_token,
            expires_at_ms,
            grants,
        })
    }
}

/// The principal of a connection without a certificate or SASL: Kafka's
/// `KafkaPrincipal.ANONYMOUS`.
fn anonymous() -> krabka_security::Principal {
    krabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

/// The mTLS principal of a controller-listener connection, or `None` when the
/// peer presented no certificate.
fn peer_certificate_principal<S>(
    stream: &tokio_rustls::server::TlsStream<S>,
    mapper: &crate::SslPrincipalMapper,
) -> Result<Option<krabka_security::Principal>, RaftHandshakeError> {
    let (_, server_connection) = stream.get_ref();
    server_connection
        .peer_certificates()
        .and_then(<[_]>::first)
        .and_then(|certificate| crate::network::auth::subject_dn_rfc2253(certificate.as_ref()))
        .map(|distinguished_name| certificate_principal(mapper, &distinguished_name))
        .transpose()
}

/// The principal of a certificate with `distinguished_name` as its Subject
/// DN, mapped with the configured `ssl.principal.mapping.rules`, as a broker
/// listener maps it. A DN that no rule matches fails the handshake, as
/// Kafka's `SslPrincipalMapper` does.
fn certificate_principal(
    mapper: &crate::SslPrincipalMapper,
    distinguished_name: &str,
) -> Result<krabka_security::Principal, RaftHandshakeError> {
    let name = mapper.apply(distinguished_name).ok_or_else(|| {
        RaftHandshakeError::Tls(format!(
            "no ssl.principal.mapping.rules rule matched {distinguished_name}"
        ))
    })?;
    Ok(krabka_security::Principal {
        name,
        auth_method: krabka_security::AuthMethod::MTls,
        groups: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    //! Narrow unit coverage.
    //!
    //! The richer behavioural tests live in `tests/raft_sasl.rs`, which starts
    //! a real two-broker raft cluster. Those cover the PLAIN happy path, the
    //! two SCRAM rounds, bad-credential rejection, and TLS termination. These
    //! tests check only the trait connections and the Plaintext
    //! short-circuit predicate, so that this layer catches a regression that
    //! flips `requires_*`.

    use assert2::assert;

    use super::*;

    #[test]
    fn plaintext_passthrough_short_circuits() {
        let cfg = test_support::handshake_config(test_support::HandshakeSetup {
            protocol: ListenerProtocol::Plaintext,
            mechanisms: vec![],
            credentials: HashMap::new(),
        });
        // `upgrade(TcpStream)` requires a real TCP socket, so we
        // exercise the short-circuit predicates directly here. The full
        // upgrade-path is exercised end-to-end in integration tests.
        assert!(!cfg.protocol.requires_tls());
        assert!(!cfg.protocol.requires_sasl());
    }

    /// The controller listener maps a certificate DN with the configured
    /// rules. A DN that no rule matches fails the handshake.
    #[test]
    fn a_certificate_principal_follows_the_configured_mapping_rules() {
        let dn = "CN=node-1,OU=brokers,O=krabka";
        let cases = [
            ("DEFAULT", vec!["DEFAULT"], Some(dn)),
            (
                "a rule that matches",
                vec!["RULE:^CN=(.*?),OU=brokers,.*$/$1/L"],
                Some("node-1"),
            ),
            (
                "a rule that does not match",
                vec!["RULE:^CN=(.*?),OU=clients,.*$/$1/"],
                None,
            ),
        ];
        for (name, rules, expected) in cases {
            let mapper = crate::SslPrincipalMapper::parse(&rules).expect("rules parse");
            let actual = certificate_principal(&mapper, dn).ok();
            let expected = expected.map(|principal| krabka_security::Principal {
                name: principal.to_owned(),
                auth_method: krabka_security::AuthMethod::MTls,
                groups: Vec::new(),
            });
            assert!(actual == expected, "{name}");
        }
    }
}
