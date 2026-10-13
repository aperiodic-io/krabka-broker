//! The [`SendfileSink`] trait and its implementation for each stream kind a
//! connection can carry: a plaintext `TcpStream`, a userspace rustls
//! `TlsStream`, and a Linux kTLS `KtlsStream`.
//!
//! The trait answers one question for the fetch path, whether this stream can
//! take a records region through the kernel, and hands out the socket to
//! `sendfile(2)` when it can.

/// A byte sink that can also drain a segment-file-backed records region with
/// the most efficient mechanism available to it.
///
/// On a SENDFILE-alias platform (Linux + Apple + FreeBSD/DragonFly) a
/// plaintext `TcpStream` exposes its underlying socket for the
/// readiness-driven `sendfile` loop. Every other stream returns `None`,
/// including TLS, which encrypts in userspace. The drainer then falls back to
/// a buffered `pread` + `write_all` that produces identical wire bytes.
/// Windows has no safe `sendfile` or `TransmitFile`, so the
/// `tcp_for_sendfile` method is compiled out and sendfile is never used.
pub trait SendfileSink {
    /// Whether the underlying transport already enforces a no-progress write
    /// deadline. A buffered protocol delegates this to its byte transport.
    fn write_idle_is_enforced(&self) -> bool { false }
    crate::sendfile_cfg! {
        /// Maximum time a sendfile response write may make no progress.
        /// Direct streams have no deadline; the connection loop supplies its
        /// listener policy. Buffered writes enforce their deadline through
        /// `AsyncWrite` instead.
        fn write_idle_window(&self) -> Option<std::time::Duration> {
            None
        }
    }

    /// `true` when this stream can serve a records region with the kernel
    /// `sendfile(2)`, that is, a plaintext `TcpStream` on a SENDFILE-alias
    /// platform. Always `false` on TLS and on Windows. The fetch handler uses
    /// this to decide whether to emit `RecordsPayload::FileRegions` at all.
    fn is_sendfile_capable(&self) -> bool;

    crate::sendfile_cfg! {
        /// Borrow the underlying `TcpStream` for readiness-driven `sendfile`,
        /// when this stream *is* a plaintext `TcpStream`. `None` for TLS.
        /// Present only on SENDFILE-alias platforms (Linux + Apple +
        /// FreeBSD/DragonFly). Windows has no compatible safe `sendfile`.
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream>;
    }
}

impl SendfileSink for tokio::net::TcpStream {
    crate::sendfile_cfg! {
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
            Some(self)
        }
    }

    fn is_sendfile_capable(&self) -> bool {
        // True on every SENDFILE-alias platform; false on Windows.
        cfg!(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))
    }
}

impl<S: SendfileSink> SendfileSink for tokio_rustls::server::TlsStream<S> {
    crate::sendfile_cfg! {
        fn write_idle_window(&self) -> Option<std::time::Duration> {
            self.get_ref().0.write_idle_window()
        }
    }

    crate::sendfile_cfg! {
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
            None
        }
    }

    fn write_idle_is_enforced(&self) -> bool { self.get_ref().0.write_idle_is_enforced() }

    // Userspace TLS: rustls encrypts in userspace, so file bytes must pass
    // through the rustls buffer — there is no kernel file→socket path. This is
    // the non-kTLS fallback (the broker's `ktls_enabled` startup probe returned
    // false, or the kernel lacks the `tls` module): the connection is served
    // over a plain `TlsStream` and the fetch path falls back to pread+write_all.
    fn is_sendfile_capable(&self) -> bool { false }
}

// Linux kTLS (Increment F): a `KtlsStream` is just a TCP socket whose TLS
// record encryption is offloaded to the kernel. `sendfile(2)` onto the inner
// `TcpStream` makes the kernel encrypt the page-cache pages into TLS records on
// the way out — restoring zero-copy fetch on encrypted connections. The drainer
// (`write_fetch_plan`/`sendfile_region`) treats it identically to a plaintext
// `TcpStream`; only the encrypt locus moves kernel-side, so the TLS record
// stream a client decrypts is byte-identical to the userspace rustls path.
#[cfg(target_os = "linux")]
impl SendfileSink for ktls::KtlsStream<tokio::net::TcpStream> {
    fn is_sendfile_capable(&self) -> bool { true }

    fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> { Some(self.get_ref()) }
}
