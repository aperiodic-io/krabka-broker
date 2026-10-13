//! What the build target offers the broker for sockets.
//!
//! On a native target the operating system binds listeners, connects
//! outbound sockets, and reports the addresses of both ends. WASI preview 1,
//! the `wasm32-wasip1` target, has none of these calls. There an embedder
//! adopts preopened listening sockets and passes them to
//! [`crate::Broker::start_with_listeners`], and outbound connections come
//! from the connector that it installs with
//! [`krabka_client_core::transport::install_connector`].
//!
//! [`Sockets`] names the two models. The broker reads [`Sockets::TARGET`];
//! the model is a value rather than a `cfg` so that tests on a native target
//! can drive the preopened model too.

use {
    std::{io, net::SocketAddr},
    tokio::net::{TcpListener, TcpStream},
};

/// How a platform gives the broker its sockets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sockets {
    /// The operating system binds, connects, and reports socket addresses.
    Native,
    /// The embedder supplies every listener, already bound, and no socket
    /// can report its address. This is WASI preview 1.
    Preopened,
}

impl Sockets {
    /// The model of the build target.
    pub(crate) const TARGET: Self = if cfg!(target_os = "wasi") {
        Self::Preopened
    } else {
        Self::Native
    };

    /// The address that `listener` is bound to.
    ///
    /// A preopened socket cannot report its address, so there the address
    /// that the configuration gives for the listener, `configured`, stands
    /// in. The embedder must bind each listener on the address that the
    /// configuration names for it.
    ///
    /// # Errors
    ///
    /// Returns the error of `getsockname` on a native target.
    pub(crate) fn listener_address(
        self,
        listener: &TcpListener,
        configured: SocketAddr,
    ) -> io::Result<SocketAddr> {
        match self {
            Self::Native => listener.local_addr(),
            Self::Preopened => Ok(configured),
        }
    }

    /// The address of the peer at the other end of `stream`.
    ///
    /// A preopened platform cannot report it, and a native socket that
    /// closed during the accept has none. Both give the unspecified address
    /// `0.0.0.0:0`, which no ACL host pattern matches.
    pub(crate) fn peer_address(self, stream: &TcpStream) -> SocketAddr {
        const UNSPECIFIED: SocketAddr =
            SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);
        match self {
            Self::Native => stream.peer_addr().unwrap_or_else(|error| {
                tracing::debug!(%error, "peer_addr() failed, using 0.0.0.0:0");
                UNSPECIFIED
            }),
            Self::Preopened => UNSPECIFIED,
        }
    }
}

/// Bind a listening socket on `addr`.
///
/// # Errors
///
/// Returns the error of the bind.
#[cfg(not(target_os = "wasi"))]
pub(crate) async fn bind_listener(addr: SocketAddr) -> io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// WASI preview 1 has no `bind`: every listener has to come from the
/// embedder, so a listener that the broker would bind itself is an error.
#[cfg(target_os = "wasi")]
pub(crate) fn bind_listener(addr: SocketAddr) -> std::future::Ready<io::Result<TcpListener>> {
    std::future::ready(Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "cannot bind a listener on {addr}: this platform has no bind; pass a preopened \
             listener for it to Broker::start_with_listeners"
        ),
    )))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[tokio::test]
    async fn a_preopened_listener_reports_its_configured_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let configured: SocketAddr = "10.0.0.1:9092".parse().expect("literal");

        let preopened = Sockets::Preopened
            .listener_address(&listener, configured)
            .expect("configured address");
        let native = Sockets::Native
            .listener_address(&listener, configured)
            .expect("local address");

        assert!((preopened, native) == (configured, listener.local_addr().expect("local address")));
    }

    #[tokio::test]
    async fn a_preopened_peer_is_the_unspecified_address() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let client = TcpStream::connect(listener.local_addr().expect("local address"))
            .await
            .expect("connect");
        let (server, peer) = listener.accept().await.expect("accept");

        assert!(
            (
                Sockets::Preopened.peer_address(&server),
                Sockets::Native.peer_address(&server),
            ) == ("0.0.0.0:0".parse().expect("literal"), peer)
        );
        drop(client);
    }
}
