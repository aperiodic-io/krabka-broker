//! Pluggable inbound handshake for the controller listener.
//!
//! This hook lets the broker terminate TLS and SASL on every accepted
//! controller-listener connection before the raft frames start to flow. The
//! trait abstraction keeps `krabka-raft` free of any dependency on
//! `krabka-broker` and `krabka-security`.

use {
    bytes::Bytes, krabka_client_core::ClientDuplex, std::sync::Arc, thiserror::Error,
    tokio::net::TcpStream,
};

/// Authenticated controller-listener connection plus request-level grants.
pub struct RaftConnection {
    /// The raft connection handler is generic over `AsyncRead + AsyncWrite +
    /// Unpin + Send + 'static`, so a `Box<dyn ClientDuplex>` plugs in directly.
    pub stream: Box<dyn ClientDuplex>,
    /// Authenticated Kafka principal. `None` represents a PLAINTEXT or
    /// one-way TLS connection with the normal `ANONYMOUS` identity.
    pub principal: Option<krabka_security::Principal>,
    /// Whether SCRAM authenticated with a delegation token rather than a
    /// regular credential.
    pub authenticated_via_token: bool,
    /// Absolute credential deadline, enforced before controller request dispatch.
    /// `None` means this credential has no finite session deadline.
    pub expires_at_ms: Option<i64>,
    /// The cluster grants of the connection principal. The listener asks it
    /// once for each request, as Kafka's `ControllerApis` does.
    pub grants: Arc<dyn ClusterGrants>,
}

/// An operation on the `Cluster("kafka-cluster")` resource that a
/// controller-listener api needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterOperation {
    /// `CLUSTER_ACTION`: the raft, registration and krabka-private metadata
    /// apis.
    ClusterAction,
    /// `ALTER`: `AddRaftVoter`, `RemoveRaftVoter` and `DescribeCluster`.
    Alter,
    /// `DESCRIBE`: `DescribeQuorum`.
    Describe,
}

/// Decides whether the principal of one controller-listener connection holds
/// an operation on the cluster resource.
///
/// The listener calls [`ClusterGrants::allows`] for every request, so an ACL
/// change applies to the next request of an open connection.
pub trait ClusterGrants: Send + Sync {
    fn allows(&self, operation: ClusterOperation) -> bool;

    /// The KIP-430 bitfield of the operations the principal holds on the
    /// cluster resource: bit `code` is set for each allowed operation of
    /// Kafka's `AclEntry.supportedOperations(CLUSTER)`, where `code` is the
    /// operation's `AclOperation` wire code. `DescribeCluster` answers it
    /// when the request sets `include_cluster_authorized_operations`.
    fn cluster_authorized_operations(&self) -> i32;
}

/// The KIP-430 bitfield of every operation Kafka's
/// `AclEntry.supportedOperations(CLUSTER)` names: `CREATE` (5), `ALTER` (7),
/// `DESCRIBE` (8), `CLUSTER_ACTION` (9), `DESCRIBE_CONFIGS` (10),
/// `ALTER_CONFIGS` (11) and `IDEMPOTENT_WRITE` (12).
const ALL_CLUSTER_OPERATIONS: i32 = 1 << 5 | 1 << 7 | 1 << 8 | 1 << 9 | 1 << 10 | 1 << 11 | 1 << 12;

/// The grants of a listener that installs no handshake: every operation is
/// allowed. The broker always installs a handshake, so only a controller
/// without a broker, such as a raft-only test, uses it.
#[derive(Debug)]
pub struct AllowAllGrants;

impl ClusterGrants for AllowAllGrants {
    fn allows(&self, _operation: ClusterOperation) -> bool { true }

    fn cluster_authorized_operations(&self) -> i32 { ALL_CLUSTER_OPERATIONS }
}

#[krabka_macros::transport_errors]
#[derive(Debug, Error)]
pub enum RaftHandshakeError {
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Per-connection handshake hook.
///
/// Implementors consume the raw `TcpStream` and return one of two things. On
/// success they return an authenticated `Box<dyn ClientDuplex>` that carries
/// the raft frames. On failure they return a `RaftHandshakeError`, and the
/// listener then drops the connection at debug level.
#[async_trait::async_trait]
pub trait RaftListenerHandshake: Send + Sync {
    /// Upgrades one accepted connection.
    ///
    /// A SASL listener answers the `ApiVersions` requests that arrive before
    /// authentication with `api_versions`, which gives the same answer as the
    /// listener gives after authentication. Kafka's `SocketServer` hands the
    /// same `apiVersionSupplier` to `SaslServerAuthenticator`.
    async fn upgrade(
        &self,
        stream: TcpStream,
        api_versions: &dyn ControllerApiVersions,
    ) -> Result<RaftConnection, RaftHandshakeError>;
}

/// The controller listener's `ApiVersions` answer, for a handshake that
/// answers `ApiVersions` before the listener gets the connection.
pub trait ControllerApiVersions: Send + Sync {
    /// Answers one `ApiVersions` request with the response body. The body goes
    /// out behind a v0 response header, at the version the body was encoded
    /// at: the request version, or v0 for a version the listener does not
    /// serve.
    ///
    /// # Errors
    /// Returns [`RaftHandshakeError::Protocol`] when the body of a served
    /// version does not decode. Kafka closes such a connection.
    fn respond(
        &self,
        request_version: i16,
        request_body: &[u8],
    ) -> Result<Bytes, RaftHandshakeError>;
}
