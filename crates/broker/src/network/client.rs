//! Outbound inter-broker client. It establishes TCP, and it optionally wraps
//! the connection in TLS and runs the SASL client handshake. It returns a
//! generic `AsyncRead` + `AsyncWrite` stream that the caller uses for normal
//! RPCs.
//!
//! The replicator's Fetch path, the raft transport's outbound dial, and the
//! controller-heartbeat loop all use this client.

#[cfg(not(target_family = "wasm"))]
use krabka_units::convert::ByteSizeExt as _;
use {
    crate::{
        config::InterBrokerCredentials,
        host_port::{HostPort, parse_host_port},
    },
    krabka_client_core::ClientDuplex,
    krabka_security::ListenerProtocol,
    krabka_units::{ByteSize, mebibytes},
    std::sync::Arc,
    thiserror::Error,
    tokio::net::TcpStream,
    tokio_rustls::TlsConnector,
};

/// Socket buffer applied to an outbound inter-broker connection when the
/// caller does not supply the broker's configured value. It matches the
/// `socket_send_buffer` / `socket_receive_buffer` defaults the accept path
/// uses, so an untuned dial behaves like an untuned accept.
const DEFAULT_SOCKET_BUFFER: ByteSize = mebibytes(1);

/// Tune an outbound inter-broker socket before TLS and SASL run on it.
///
/// - `TCP_NODELAY`: disable Nagle. Every RPC the broker originates —
///   replica Fetch, the `KRaft` quorum exchanges, envelope forwarding to the
///   controller, and the lag poller's high-watermark probes — is a small
///   request that would otherwise wait for the peer's delayed ACK, adding up
///   to ~40 ms to a replication round trip. Apache Kafka disables Nagle on
///   every channel its `Selector` opens, connect and accept alike.
/// - `SO_SNDBUF`/`SO_RCVBUF`: the same configured buffers the accept path
///   applies, so a replication stream has in-flight headroom in both
///   directions.
///
/// All failures are non-fatal and logged at debug level, exactly as on the
/// accept side: an untuned connection still works, just less efficiently.
#[cfg(not(target_family = "wasm"))]
fn tune_outbound_socket(stream: &TcpStream, send_buffer: ByteSize, receive_buffer: ByteSize) {
    tune_socket(stream, send_buffer, receive_buffer, [
        |e| tracing::debug!(error = %e, "TCP_NODELAY set failed on outbound socket"),
        |e| tracing::debug!(error = %e, "SO_SNDBUF set failed on outbound socket"),
        |e| tracing::debug!(error = %e, "SO_RCVBUF set failed on outbound socket"),
    ]);
}

/// Apply options in their original order; callers retain their diagnostic call sites.
pub(crate) fn tune_socket(
    stream: &TcpStream,
    send_buffer: ByteSize,
    receive_buffer: ByteSize,
    on_error: [fn(std::io::Error); 3],
) {
    if let Err(error) = stream.set_nodelay(true) {
        on_error[0](error);
    }
    #[cfg(not(target_family = "wasm"))]
    {
        let socket = socket2::SockRef::from(stream);
        if let Err(error) = socket.set_send_buffer_size(send_buffer.bytes_usize()) {
            on_error[1](error);
        }
        if let Err(error) = socket.set_recv_buffer_size(receive_buffer.bytes_usize()) {
            on_error[2](error);
        }
    }
    // WASI preview 1 has no socket-buffer options and keeps the host defaults.
    #[cfg(target_family = "wasm")]
    let _ = (send_buffer, receive_buffer);
}

/// Map the broker's [`InterBrokerCredentials`] onto the client-core
/// [`krabka_client_core::SaslCredentials`] understood by the shared
/// [`krabka_client_core::outbound_sasl`] handshake. The two enums carry
/// the same variants, so this is a field-for-field copy. The RLMM bootstrap
/// shares it, so the dialer and the metadata client agree on the
/// mapping.
pub(crate) fn to_client_creds(c: &InterBrokerCredentials) -> krabka_client_core::SaslCredentials {
    match c {
        InterBrokerCredentials::Plain { username, password } => {
            krabka_client_core::SaslCredentials::Plain {
                username: username.clone(),
                password: password.clone(),
            }
        }
        InterBrokerCredentials::Scram {
            mechanism,
            username,
            password,
        } => krabka_client_core::SaslCredentials::Scram {
            mechanism: *mechanism,
            username: username.clone(),
            password: password.clone(),
            // A broker authenticates with its own password, never a token.
            delegation_token: false,
        },
        InterBrokerCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => krabka_client_core::SaslCredentials::Gssapi {
            keytab_path: keytab_path.clone(),
            client_principal: client_principal.clone(),
            service_name: service_name.clone(),
            kdc_url: kdc_url.clone(),
        },
        InterBrokerCredentials::OAuthBearer { token_path } => {
            krabka_client_core::SaslCredentials::OAuthBearer {
                token: krabka_client_core::OAuthBearerTokenSource::File(token_path.clone()),
                extensions: std::collections::BTreeMap::new(),
            }
        }
    }
}

#[krabka_macros::transport_errors]
#[derive(Debug, Error)]
pub enum InterBrokerError {
    #[error("config: {0}")]
    Config(String),
    #[error("codec: {0}")]
    Codec(String),
}

/// Constructs outbound connections to other brokers, and runs TLS and SASL
/// as the listener protocol demands. It is cheap to clone and share, because
/// it holds only a `TlsConnector`, which is an `Arc` internally, and
/// credentials.
pub struct InterBrokerClient {
    tls_connector: Option<TlsConnector>,
    creds: Option<InterBrokerCredentials>,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
    socket_send_buffer: ByteSize,
    socket_receive_buffer: ByteSize,
}

impl InterBrokerClient {
    /// Sets the dispatch queue and frame limits of this broker's outbound
    /// client resource policy on `options`.
    pub(crate) fn apply_resource_policy(
        &self,
        options: &mut krabka_client_core::ConnectionOptions,
    ) {
        options.dispatch_queue_capacity = self.dispatch_queue_capacity;
        options.frame_max = self.frame_max;
    }

    #[must_use]
    pub fn new(tls_connector: Option<TlsConnector>, creds: Option<InterBrokerCredentials>) -> Self {
        Self::new_with_policy(
            tls_connector,
            creds,
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
            DEFAULT_SOCKET_BUFFER,
            DEFAULT_SOCKET_BUFFER,
        )
    }

    /// Construct with the broker process's outbound client resource policy.
    /// `socket_send_buffer` and `socket_receive_buffer` are the same
    /// configured sizes the accept path applies to sockets it accepts.
    #[must_use]
    pub fn new_with_policy(
        tls_connector: Option<TlsConnector>,
        creds: Option<InterBrokerCredentials>,
        dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
        frame_max: krabka_client_core::ClientFrameMax,
        socket_send_buffer: ByteSize,
        socket_receive_buffer: ByteSize,
    ) -> Self {
        Self {
            tls_connector,
            creds,
            dispatch_queue_capacity,
            frame_max,
            socket_send_buffer,
            socket_receive_buffer,
        }
    }

    /// Open the TCP connection every outbound inter-broker RPC rides, tuned
    /// with this client's socket policy before any TLS or SASL bytes flow.
    /// Tuning has to happen here: the handshakes are themselves small
    /// round-trip-bound exchanges that Nagle would stall, and once rustls owns
    /// the stream the raw socket is no longer reachable.
    #[cfg(not(target_family = "wasm"))]
    async fn dial_tuned(&self, host: &str, port: u16) -> Result<TcpStream, std::io::Error> {
        let tcp = TcpStream::connect((unbracket_host(host), port)).await?;
        tune_outbound_socket(&tcp, self.socket_send_buffer, self.socket_receive_buffer);
        Ok(tcp)
    }

    /// Open the TCP connection every outbound inter-broker RPC rides.
    ///
    /// WASI preview 1 has no `connect`, so the socket comes from the
    /// connector that the embedder installs with
    /// [`krabka_client_core::transport::install_connector`]. The socket policy
    /// travels with the request, but a connector socket takes none of it.
    #[cfg(target_family = "wasm")]
    async fn dial_tuned(&self, host: &str, port: u16) -> Result<TcpStream, std::io::Error> {
        krabka_client_core::transport::dial(
            unbracket_host(host),
            port,
            krabka_client_core::transport::SocketOptions {
                send_buffer: Some(self.socket_send_buffer),
                receive_buffer: Some(self.socket_receive_buffer),
                nodelay: true,
            },
        )
        .await
    }

    /// Dial `host:port`, do the protocol-appropriate TLS and SASL
    /// handshakes, and return an authenticated duplex stream. Callers
    /// drive normal Kafka RPCs, such as Fetch, Vote, and `AppendEntries`,
    /// through the returned stream as if it were a fresh `TcpStream`.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        options: &krabka_client_core::ConnectionOptions,
    ) -> Result<Box<dyn ClientDuplex>, InterBrokerError> {
        let tcp = self.dial_tuned(host, port).await?;
        let mut stream: Box<dyn ClientDuplex> = if listener_protocol.requires_tls() {
            let connector = self.tls_connector.clone().ok_or_else(|| {
                InterBrokerError::Config("TLS listener without TlsConnector".into())
            })?;
            let sni =
                tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                    .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
            let tls = connector
                .connect(sni, tcp)
                .await
                .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            krabka_client_core::outbound_sasl(
                &mut *stream,
                &to_client_creds(&creds),
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|e| InterBrokerError::Sasl(e.to_string()))?;
        }
        Ok(stream)
    }

    /// Dial `host:port`, run TLS and SASL as needed, and return a
    /// [`krabka_client_core::Connection`] over the resulting stream. The
    /// connection is fully usable for normal typed Kafka requests, such as
    /// `Fetch`, `OffsetForLeaderEpoch`, `BrokerHeartbeat`, and raft RPCs
    /// through `raw_request`.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect_as_connection(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        mut options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, InterBrokerError> {
        self.apply_resource_policy(&mut options);
        let stream = self
            .connect(host, port, listener_protocol, server_name, &options)
            .await?;
        krabka_client_core::Connection::from_stream(stream, options)
            .await
            .map_err(|e| InterBrokerError::Config(format!("Connection::from_stream: {e}")))
    }
}

fn unbracket_host(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

// ────────────────────────────────────────────────────────────────────────
// OutboundDialer adapter for krabka_raft::KrabkaRaftNetworkFactory.
// ────────────────────────────────────────────────────────────────────────

/// Adapter that lets `krabka_raft` reach the broker's
/// [`InterBrokerClient`] without taking a build dependency on the
/// broker crate. It wraps an `Arc<InterBrokerClient>` and the protocol and
/// SNI configuration once, and the raft network factory clones it cheaply.
pub struct InterBrokerDialer {
    client: Arc<InterBrokerClient>,
    listener_protocol: ListenerProtocol,
    server_name: String,
}

impl InterBrokerDialer {
    #[must_use]
    pub fn new(
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        server_name: String,
    ) -> Self {
        Self {
            client,
            listener_protocol,
            server_name,
        }
    }
}

#[async_trait::async_trait]
impl krabka_raft::OutboundDialer for InterBrokerDialer {
    async fn dial(
        &self,
        _target: krabka_raft::NodeId,
        addr: &str,
        options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
        // The raft transport hands us an address in `host:port` form
        // (the openraft `Node.addr` string). For SocketAddr-style
        // addresses we honour the configured `server_name` for SNI
        // separately from the literal host string.
        let HostPort { host, port } = parse_host_port(addr).ok_or_else(|| {
            krabka_client_core::ClientError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid raft peer address {addr:?}"),
            ))
        })?;
        self.client
            .connect_as_connection(
                &host,
                port,
                self.listener_protocol,
                &self.server_name,
                options,
            )
            .await
            .map_err(|e| match e {
                InterBrokerError::Io(io) => krabka_client_core::ClientError::Io(io),
                other => krabka_client_core::ClientError::Io(std::io::Error::other(format!(
                    "InterBrokerClient dial: {other}"
                ))),
            })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            DEFAULT_SOCKET_BUFFER, InterBrokerClient, to_client_creds, tune_outbound_socket,
            unbracket_host,
        },
        crate::{
            config::InterBrokerCredentials,
            network::test_support::{check_socket_tuning, tcp_pair},
        },
        krabka_units::kibibytes,
        std::path::PathBuf,
    };

    #[tokio::test]
    async fn outbound_socket_tuning_sets_nodelay_and_large_buffers() {
        let (server, client) = tcp_pair().await;
        check_socket_tuning(&client, |socket| {
            tune_outbound_socket(socket, kibibytes(64), kibibytes(128));
        });
        drop(server);
    }

    #[test]
    fn tuple_dial_host_strips_ipv6_brackets() {
        assert2::assert!(unbracket_host("[2001:db8::7]") == "2001:db8::7");
        assert2::assert!(unbracket_host("broker.example") == "broker.example");
    }

    #[tokio::test]
    async fn dialed_socket_is_tuned_before_tls_and_sasl() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener addr");
        let accept = tokio::spawn(async move { listener.accept().await });

        let client = InterBrokerClient::new_with_policy(
            None,
            None,
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
            kibibytes(64),
            kibibytes(256),
        );
        let dialed = client
            .dial_tuned(&addr.ip().to_string(), addr.port())
            .await
            .expect("dial loopback peer");
        let (server, _) = accept.await.expect("accept task").expect("accept dial");

        // Read the options back off the connected socket the dialer produced,
        // the way the accept path's tuning test does on its side.
        assert2::assert!(dialed.nodelay().expect("read TCP_NODELAY"));
        let sock = socket2::SockRef::from(&dialed);
        let send = sock.send_buffer_size().expect("read send buffer");
        let recv = sock.recv_buffer_size().expect("read recv buffer");
        // Kernels clamp and may double requested sizes, so assert the two
        // distinct configured buffers stayed distinct and ordered.
        assert2::assert!(recv > send);
        drop(server);
    }

    #[test]
    fn process_policy_overrides_call_site_defaults() {
        let client = InterBrokerClient::new_with_policy(
            None,
            None,
            krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap(),
            krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
            kibibytes(64),
            kibibytes(128),
        );
        let mut options = krabka_client_core::ConnectionOptions::default();
        client.apply_resource_policy(&mut options);
        assert2::assert!(options.dispatch_queue_capacity.get() == 7);
        assert2::assert!(options.frame_max.size() == krabka_units::kibibytes(32));
    }

    #[test]
    fn default_construction_uses_socket_tuning_defaults() {
        let client = InterBrokerClient::new(None, None);
        assert2::assert!(client.socket_send_buffer == DEFAULT_SOCKET_BUFFER);
        assert2::assert!(client.socket_receive_buffer == DEFAULT_SOCKET_BUFFER);
    }

    #[test]
    fn oauthbearer_credentials_preserve_rotation_path() {
        let token_path = PathBuf::from("/run/secrets/krabka/inter-broker-token");
        let credentials = to_client_creds(&InterBrokerCredentials::OAuthBearer {
            token_path: token_path.clone(),
        });
        let krabka_client_core::SaslCredentials::OAuthBearer {
            token: krabka_client_core::OAuthBearerTokenSource::File(actual_path),
            extensions,
        } = credentials
        else {
            panic!("expected file-backed OAUTHBEARER client credentials");
        };
        assert2::assert!(actual_path == token_path);
        assert2::assert!(extensions.is_empty());
    }
}
