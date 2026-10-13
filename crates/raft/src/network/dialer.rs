//! The outbound dial seam the controller hands to the peer sender, and the
//! PLAINTEXT dialer it falls back to.
//!
//! Terminating TLS and SASL on a controller-to-controller dial needs the
//! broker's `InterBrokerClient`, which this crate cannot depend on, so the dial
//! is a trait the broker injects an implementation of. `PlaintextDialer` is
//! what the controller uses when nothing is injected.

use {
    crate::kraft::types::NodeId,
    async_trait::async_trait,
    krabka_client_core::{ClientError, Connection, ConnectionOptions},
    std::net::SocketAddr,
};

/// Outbound dialer the controller hands to the peer sender.
///
/// `krabka-raft` cannot depend on `krabka-broker`, because that would be a
/// cycle. The broker therefore supplies an impl that wraps its
/// `InterBrokerClient`, with TLS and SASL, and injects it through
/// [`ControllerConfig::dialer`](crate::ControllerConfig). When no dialer is
/// injected, the controller falls back to a plain `Connection::connect(addr)`,
/// which is the PLAINTEXT path.
#[async_trait]
pub trait OutboundDialer: Send + Sync {
    /// Opens a `Connection` to the raft peer `target`, which is reachable on
    /// `addr`. The returned connection has already negotiated `ApiVersions`,
    /// and `raw_request` can use it immediately.
    async fn dial(
        &self,
        target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError>;
}

/// Default no-op dialer: it opens a raw `TcpStream` with `Connection::connect`.
///
/// The controller uses this dialer when the broker has injected no
/// `InterBrokerClient`-backed dialer. This is the legacy PLAINTEXT path.
pub struct PlaintextDialer;

#[async_trait]
impl OutboundDialer for PlaintextDialer {
    #[tracing::instrument(level = "debug", skip_all, fields(target = _target.0, addr), err)]
    async fn dial(
        &self,
        _target: NodeId,
        addr: &str,
        options: ConnectionOptions,
    ) -> Result<Connection, ClientError> {
        // Re-resolve `addr` (a `<host>:<port>`) on every dial. A `StatefulSet`
        // peer that restarts keeps its stable DNS name but gets a fresh pod IP;
        // resolving here (rather than once at startup) reaches the new IP.
        // A literal `ip:port` resolves to itself with no lookup, which is
        // what keeps this usable on WASI, where a host name is an error.
        let sock: SocketAddr = krabka_client_core::transport::resolve(addr)
            .await
            .map_err(ClientError::Io)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("raft peer address {addr:?} resolved to no addresses"),
                ))
            })?;
        Connection::connect(sock, options).await
    }
}
