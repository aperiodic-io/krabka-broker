//! Real rustls records over a bounded, paced byte transport. Paused time makes
//! ciphertext progress and a complete flush spanning many idle windows exact.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use assert2::assert;
use rustls::pki_types::{PrivateKeyDer, ServerName};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _, DuplexStream, ReadBuf},
    time::{Instant, Sleep},
};

use super::WriteIdle;
use crate::test_support::localhost_ecdsa_pair;

/// Limit ciphertext reads to 64 bytes every 10ms after the handshake. Delaying
/// plaintext reads would test rustls buffering rather than socket progress.
struct PacedReader {
    inner: DuplexStream,
    enabled: Arc<AtomicBool>,
    next_read: Option<Pin<Box<Sleep>>>,
}

impl AsyncRead for PacedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.enabled.load(Ordering::Relaxed) {
            return Pin::new(&mut this.inner).poll_read(cx, buf);
        }
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let timer = this
            .next_read
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(Duration::from_millis(10))));
        if timer.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        let mut bytes = [0; 64];
        let count = bytes.len().min(buf.remaining());
        let mut limited = ReadBuf::new(&mut bytes[..count]);
        match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
            Poll::Ready(Ok(())) => {
                buf.put_slice(limited.filled());
                this.next_read = None;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl AsyncWrite for PacedReader {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

type ServerTls = tokio_rustls::server::TlsStream<WriteIdle<DuplexStream>>;
type ClientTls = tokio_rustls::client::TlsStream<PacedReader>;

async fn tls_pair(window: Duration) -> (ServerTls, ClientTls, Arc<AtomicBool>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (certificate, key) = localhost_ecdsa_pair();
    let cert = certificate.der().clone();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.clone()],
            PrivateKeyDer::try_from(key.serialize_der()).expect("key"),
        )
        .expect("server config");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trust certificate");
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
    // Enough room for both peers to finish the handshake, including TLS 1.3
    // session tickets after the client stops polling its connector.
    let (server, client) = tokio::io::duplex(8 * 1024);
    let paced = Arc::new(AtomicBool::new(false));
    let reader = PacedReader {
        inner: client,
        enabled: paced.clone(),
        next_read: None,
    };
    let (server, client) = tokio::join!(
        acceptor.accept(WriteIdle::new(server, Some(window))),
        connector.connect(
            ServerName::try_from("localhost").expect("server name"),
            reader
        ),
    );
    (
        server.expect("server handshake"),
        client.expect("client handshake"),
        paced,
    )
}

#[tokio::test(start_paused = true)]
async fn tls_ciphertext_progress_rearms_idle_during_a_long_flush() {
    let window = Duration::from_millis(100);
    let (server, mut client, paced) = tls_pair(window).await;
    paced.store(true, Ordering::Relaxed);
    let start = Instant::now();
    let writer = tokio::spawn(async move {
        // The same response wrapper as dispatch must recognize the timer
        // beneath TLS rather than timing the complete encrypted flush.
        let mut server = WriteIdle::for_response(server, Some(window));
        server.write_all(&vec![7; 64 * 1024]).await?;
        server.flush().await
    });
    let mut received = vec![0; 64 * 1024];
    client
        .read_exact(&mut received)
        .await
        .expect("continuing TLS progress");
    writer
        .await
        .expect("writer task")
        .expect("TLS flush completes");
    assert!(received == vec![7; 64 * 1024]);
    assert!(start.elapsed() > window);
}

#[tokio::test(start_paused = true)]
async fn a_tls_reader_that_stops_ciphertext_progress_expires() {
    let window = Duration::from_millis(100);
    let (server, _nonreading_client, _paced) = tls_pair(window).await;
    let mut server = WriteIdle::for_response(server, Some(window));
    let result = async {
        server.write_all(&vec![7; 64 * 1024]).await?;
        server.flush().await
    }
    .await;
    assert!(result.expect_err("stalled TLS socket expires").kind() == io::ErrorKind::TimedOut);
}
