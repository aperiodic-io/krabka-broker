//! Per-connection session state. It derives the initial `ConnectionAuth` for
//! a listener, borrows the connection's principal, holds the read off for the
//! KIP-219 mute window a throttled request earned, and arms the one deadline
//! that races the next frame read: the `connections.max.idle.ms` idle window.
//!
//! The idle deadline is a property of the listener, applies whatever the
//! connection's auth state, and is re-armed from `Instant::now()` on every
//! prefix and body read. Waiting for the broker byte budget pauses the idle window.
//! Response writes have the same no-progress window, enforced by `WriteIdle`
//! and the sendfile drainer; a client that stops reading cannot retain queue
//! permits forever. Successful writes re-arm it, and handler time is excluded.
//!
//! The KIP-368 SASL session deadline is deliberately not armed here. Kafka
//! runs no timer against it either: an expired session closes on the next
//! request that is not part of a re-authentication, which the dispatch loop
//! enforces through `ConnectionAuth::expired_for_request`.
//!
//! The idle window is armed after the mute rather than before it, so a pause
//! the broker itself imposed to shed load never counts as the client falling
//! silent.
//!
//! On a TLS listener the handshake that runs before this loop is held to the
//! same window by `accept::handshake_within_idle_window`, so a peer that opens
//! the socket and never negotiates is reclaimed too.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use super::{
    RequestByteBudget,
    receive::{ReceiveError, RequestFrame, receive_request},
};
use crate::{
    config::BrokerConfig,
    metrics::BrokerMetrics,
    network::{
        auth::ConnectionAuth,
        codec::{KafkaCodec, is_invalid_receive},
    },
};

/// Process-lifetime ANONYMOUS principal.
///
/// `RequestContext` only borrows a `&Principal`, so the defensive fallback can
/// return a `&'static Principal` instead of allocating a fresh `String` and
/// `Vec` for each Produce or Fetch. That fallback covers SASL pre-auth, where
/// `auth.principal()` is `None`. `Principal` carries a `Vec<String>`, so it
/// cannot be a `const`; `LazyLock` builds it once on first use.
static ANONYMOUS_PRINCIPAL: std::sync::LazyLock<krabka_security::Principal> =
    std::sync::LazyLock::new(|| krabka_security::Principal {
        name: "ANONYMOUS".to_string(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    });

/// Borrows the connection's authenticated principal.
///
/// When the connection has no principal yet, the defensive SASL pre-auth case,
/// the function falls back to the shared process-lifetime ANONYMOUS singleton.
/// This avoids a `Principal` clone for each request.
pub(super) fn principal_or_anonymous(auth: &ConnectionAuth) -> &krabka_security::Principal {
    auth.principal().unwrap_or(&ANONYMOUS_PRINCIPAL)
}

/// Resolves the connection's starting authentication state.
///
/// A SASL listener starts anonymous and authenticates over the wire. A
/// non-SASL listener is already decided at accept time: the mTLS principal the
/// cert chain produced, or `ANONYMOUS`. The mTLS case is a completed
/// credential presentation with no SASL frame behind it, so it is the one
/// place that writes its own `Authentication` audit row.
pub(super) fn initial_connection_auth(
    is_sasl_listener: bool,
    mtls_principal: Option<krabka_security::Principal>,
    audit_log: &krabka_audit::AuditLog,
    peer: &std::net::SocketAddr,
) -> ConnectionAuth {
    if is_sasl_listener {
        return ConnectionAuth::Anonymous;
    }
    let principal = match mtls_principal {
        Some(principal) => {
            super::sasl::emit_authentication(
                audit_log,
                peer,
                "SSL",
                super::sasl::audit_principal(&principal),
                krabka_audit::AuditOutcome::Success,
                None,
            );
            principal
        }
        None => krabka_security::Principal {
            name: "ANONYMOUS".to_string(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        },
    };
    ConnectionAuth::Authenticated {
        principal,
        mechanism: krabka_security::SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

/// The largest request frame a connection may send.
///
/// Kafka reads every frame of the authentication exchange under
/// `sasl.server.max.receive.size` (`SaslServerAuthenticator` holds its own
/// `NetworkReceive`), and every frame after it under
/// `socket.request.max.bytes`. A re-authentication starts on the larger limit,
/// since its `SaslHandshake` reaches the channel while it is still
/// authenticated.
#[derive(Clone, Copy)]
pub(super) struct RequestLimits {
    /// `socket.request.max.bytes`: the limit once authenticated.
    request_max: usize,
    /// `sasl.server.max.receive.size`: the limit until then.
    pre_auth_max: usize,
}

impl RequestLimits {
    pub(super) fn of(config: &BrokerConfig) -> Self {
        use krabka_units::convert::ByteSizeExt as _;

        Self {
            request_max: config.socket_request_max.bytes_usize(),
            pre_auth_max: config.sasl_server_max_receive.bytes_usize(),
        }
    }

    /// The limit for the next frame of a connection in state `auth`.
    pub(super) fn for_auth(self, auth: &ConnectionAuth) -> usize {
        if auth.is_authenticated() {
            self.request_max
        } else {
            self.pre_auth_max
        }
    }

    /// The limit of an authenticated connection.
    pub(super) fn request_max(self) -> usize {
        self.request_max
    }
}

/// What the connection is held to while it waits for its next frame: the
/// listener's idle window, the peer it belongs to, and the metrics handle the
/// close is counted on.
///
/// The idle window is `None` when the listener expires no connection, which is
/// what a non-positive `connections.max.idle.ms` asks for.
///
/// A peer that has not finished authenticating and sends a frame over
/// `sasl.server.max.receive.size` fails its authentication, which is counted
/// and audited, and closes only after `failed_authentication_delay`.
pub(super) struct FrameWaitPolicy {
    pub(super) idle: Option<std::time::Duration>,
    pub(super) peer: std::net::SocketAddr,
    pub(super) metrics: BrokerMetrics,
    pub(super) audit_log: std::sync::Arc<krabka_audit::AuditLog>,
    pub(super) failed_authentication_delay: std::time::Duration,
}

/// Reads the next request frame, after honouring any KIP-219 mute window.
///
/// `mute_until` is the deadline the previous response's throttle earned. Kafka
/// enforces a quota by muting the connection: the response is already on the
/// wire, and the broker simply stops reading requests until the window closes.
/// Holding the read back here — rather than delaying the write — is what keeps
/// a throttled client from timing out its in-flight request and retrying into
/// the quota that is shedding load.
///
/// The idle window is armed once the mute has drained and again after the
/// byte-budget wait, so broker backpressure does not spend the client's idle budget. The KIP-368 session
/// deadline is not armed here at all: Kafka closes an expired session on the
/// request that arrives past it, which the dispatch loop does with
/// `ConnectionAuth::expired_for_request`.
pub(super) async fn next_connection_frame<S>(
    framed: &mut Framed<S, KafkaCodec>,
    auth: &ConnectionAuth,
    mute_until: Option<tokio::time::Instant>,
    policy: &FrameWaitPolicy,
    (maximum, budget): (usize, Option<&RequestByteBudget>),
) -> Option<RequestFrame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::metrics::ConnectionCloseReason;

    if let Some(mute_until) = mute_until {
        tokio::time::sleep_until(mute_until).await;
    }
    let frame_result = receive_request(framed.get_mut(), maximum, budget, policy.idle).await;
    match frame_result {
        Ok(Some(frame)) => Some(frame),
        Err(ReceiveError::Idle) => {
            tracing::debug!(
                principal = principal_or_anonymous(auth).name.as_str(),
                peer = %policy.peer,
                idle_ms = policy.idle.unwrap_or_default().as_millis(),
                "connection idle past connections.max.idle.ms, closing"
            );
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::Idle);
            None
        }
        Err(ReceiveError::Io(error)) => {
            tracing::warn!(%error, peer = %policy.peer, "frame decode error, closing");
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::DecodeError);
            if is_invalid_receive(&error) && !auth.is_authenticated() {
                // Kafka's `SaslServerAuthenticator.authenticate` turns the
                // `InvalidReceiveException` of a receive read during the
                // exchange into a `SaslAuthenticationException`, so the
                // failure is counted like any other and its close is delayed.
                super::sasl::record_refused_authentication(
                    (&policy.metrics, &policy.audit_log),
                    auth,
                    &policy.peer,
                    "invalid receive size",
                );
                if !policy.failed_authentication_delay.is_zero() {
                    tokio::time::sleep(policy.failed_authentication_delay).await;
                }
            }
            None
        }
        Ok(None) => {
            policy
                .metrics
                .record_connection_close(ConnectionCloseReason::PeerClosed);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::network::test_support::assert_authentication_event;

    /// The mTLS binding is the one authentication with no SASL frame behind
    /// it, so `initial_connection_auth` is the only place that can record it.
    /// The anonymous and SASL starts presented no credential and must record
    /// nothing.
    #[test]
    fn initial_connection_auth_audits_the_mtls_binding_only() {
        let peer: std::net::SocketAddr = "192.0.2.7:9093".parse().expect("peer addr");
        let cert_dn = krabka_security::Principal {
            name: "CN=test-client,OU=integration,O=krabka".to_string(),
            auth_method: krabka_security::AuthMethod::MTls,
            groups: vec![],
        };

        for (what, is_sasl, mtls) in [
            ("a SASL listener", true, Some(cert_dn.clone())),
            ("an SSL listener with no client cert", false, None),
        ] {
            let (log, mut rx) = krabka_audit::AuditLog::new(8);
            let _auth = initial_connection_auth(is_sasl, mtls, log.as_ref(), &peer);
            assert!(rx.try_recv().is_err(), "{what} presented no credential");
        }

        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let auth = initial_connection_auth(false, Some(cert_dn), log.as_ref(), &peer);
        assert!(
            auth.principal().map(|p| p.name.as_str())
                == Some("CN=test-client,OU=integration,O=krabka")
        );

        let event = rx.try_recv().expect("the mTLS authentication row");
        assert_authentication_event(
            &event,
            krabka_audit::AuditOutcome::Success,
            "SSL",
            ("User:CN=test-client,OU=integration,O=krabka", "MTls"),
            ("192.0.2.7", 9093),
            None,
        );
    }
}
