//! Drains an ordered [`WriteOp`] plan to a connection stream.
//!
//! Inline ops go out with `write_all`. A file-backed op goes through the
//! kernel `sendfile(2)` when the stream allows it, and through a buffered
//! `pread` and `write_all` fallback that produces identical wire bytes when it
//! does not.
//!
//! The drain is also where the broker learns which of those three paths a
//! fetch actually took. Every other signal a regression would move — the
//! response bytes, the request counter, the latency histogram — is identical
//! whether the kernel moved the records or the process copied them, so the
//! [`FetchDrainPath`] label is recorded here, from the arm that ran, and not
//! from the decision the fetch handler made earlier.

use {
    super::{WriteOp, sink::SendfileSink},
    crate::{
        error::BrokerError,
        metrics::{BrokerMetrics, FetchDrainPath},
    },
    tokio::io::{AsyncWrite, AsyncWriteExt},
};

crate::sendfile_cfg! {
    use bytes::BytesMut;

    use super::sendfile::sendfile_region;
}

/// Inline ops shorter than this are copied into the coalescing buffer; longer
/// ones are written from their own `Bytes`, where the copy would cost more
/// than the syscall it saves.
const COALESCE_BELOW: usize = 16 * 1024;

/// Writes and clears the coalesced inline bytes, if there are any.
async fn write_coalesced<S>(stream: &mut S, coalesced: &mut Vec<u8>) -> Result<(), BrokerError>
where
    S: AsyncWrite + Unpin,
{
    if !coalesced.is_empty() {
        stream.write_all(coalesced).await.map_err(BrokerError::Io)?;
        coalesced.clear();
    }
    Ok(())
}

/// Drain a fetch write-plan to `stream`, write each op in order, then flush.
///
/// The caller MUST have flushed any pending `Framed` codec output first, so
/// the bytes do not interleave with the codec's write buffer. Inline ops use
/// `write_all`. File ops use `sendfile` when the stream is a Linux plaintext
/// `TcpStream`, and a buffered `pread` + `write_all` fallback otherwise.
///
/// On success it bumps `fetch_response_drain_total` once, on the path the
/// plan's records regions took: a plan of inline ops is `vectored`, and a plan
/// with a file-backed op takes the label of the arm that drained it. A drain
/// that fails part-way counts nothing, because the response never reached the
/// client.
///
/// # Errors
///
/// Returns [`BrokerError::Io`] when `ops` is empty, which is not a frame a
/// Kafka client can parse, and when a write, a `sendfile`, or the fallback's
/// positioned read fails. Every one of those leaves a partly written frame on
/// the wire, so the caller closes the connection rather than sending another
/// response after it.
pub async fn write_fetch_plan<S>(
    stream: &mut S,
    ops: Vec<WriteOp>,
    metrics: &BrokerMetrics,
) -> Result<(), BrokerError>
where
    S: AsyncWrite + SendfileSink + Unpin,
{
    let mut ops = ops.into_iter();
    let first = ops.next().ok_or_else(|| {
        BrokerError::Io(std::io::Error::other(
            "fetch handler produced an empty write plan",
        ))
    })?;
    // The path the ops actually took, not the one the handler asked for. An
    // inline op claims the vectored path only if no file op has spoken yet; a
    // file op always overwrites, because every file region of one response
    // goes to the same stream and so takes the same arm.
    let mut path: Option<FetchDrainPath> = None;
    // Small inline ops -- the frame length, the response header, the envelope
    // metadata between records regions, and small records runs -- are copied
    // into one buffer and go out together. Written one by one, each is its own
    // syscall and, on a `TCP_NODELAY` socket, its own segment, so a small
    // fetch response cost four sends and as many wakeups at the reader.
    let mut coalesced = Vec::new();
    for op in std::iter::once(first).chain(ops) {
        krabka_macros::sendfile_match! { match op {
            WriteOp::Inline(b) => {
                if b.len() < COALESCE_BELOW {
                    coalesced.extend_from_slice(&b);
                } else {
                    write_coalesced(stream, &mut coalesced).await?;
                    stream.write_all(&b).await.map_err(BrokerError::Io)?;
                }
                path.get_or_insert(FetchDrainPath::Vectored);
            }
            WriteOp::File(region) => {
                write_coalesced(stream, &mut coalesced).await?;
                path = Some(drain_file_region(stream, &region).await?);
            }
        }}
    }
    write_coalesced(stream, &mut coalesced).await?;
    stream.flush().await.map_err(BrokerError::Io)?;
    // The empty plan is rejected above, so the plan named a path; `vectored`
    // is the honest fallback for a plan that somehow drained no op at all.
    metrics.record_fetch_response_drain(path.unwrap_or(FetchDrainPath::Vectored));
    Ok(())
}

crate::sendfile_cfg! {
    /// Positioned, full read of a `FileRegion` into `dst`, in a loop over
    /// short reads. `dst` must be exactly `region.len` bytes. This is the TLS
    /// and non-sendfile fallback for `WriteOp::File`. `read_at` (`FileExt`) is
    /// portable across every SENDFILE-alias unix.
    fn read_region_exact(
        region: &krabka_protocol::records::FileRegion,
        dst: &mut [u8],
    ) -> Result<(), BrokerError> {
        use std::os::unix::fs::FileExt;
        assert2::assert!((dst.len()) == (region.len));
        let mut filled = 0usize;
        let mut offset = region.offset;
        while filled < dst.len() {
            match region.file.read_at(&mut dst[filled..], offset) {
                Ok(0) => {
                    return Err(BrokerError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "FileRegion read hit EOF before len bytes",
                    )));
                }
                Ok(n) => {
                    filled += n;
                    offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(BrokerError::Io(e)),
            }
        }
        Ok(())
    }

    /// Drain one `FileRegion` to the socket, and report which arm did it.
    ///
    /// This method uses the kernel `sendfile(2)` when the stream is a
    /// plaintext `TcpStream` on a SENDFILE-alias platform. On TLS it falls
    /// back to a buffered `pread` + `write_all` that produces identical wire
    /// bytes. The returned [`FetchDrainPath`] is what the caller records, so
    /// the counter cannot claim a zero-copy drain that the copy arm served.
    async fn drain_file_region<S>(
        stream: &mut S,
        region: &krabka_protocol::records::FileRegion,
    ) -> Result<FetchDrainPath, BrokerError>
    where
        S: AsyncWrite + SendfileSink + Unpin,
    {
        if stream.tcp_for_sendfile().is_some() {
            // Re-borrow immutably for the readiness loop. `writable()`/`try_io()`
            // take `&self`, so this never conflicts with the (released) `&mut`.
            let tcp = stream
                .tcp_for_sendfile()
                .expect("checked Some on the line above");
            sendfile_region(tcp, region, stream.write_idle_window()).await?;
            Ok(FetchDrainPath::Sendfile)
        } else {
            // TLS fallback: pread the region into a buffer and write it.
            let mut buf = BytesMut::zeroed(region.len);
            read_region_exact(region, &mut buf)?;
            stream.write_all(&buf).await.map_err(BrokerError::Io)?;
            Ok(FetchDrainPath::Pread)
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::metrics::FetchDrainPathLabel};

    /// The count this drain's counter carries for `path`.
    fn drained(metrics: &BrokerMetrics, path: FetchDrainPath) -> u64 {
        metrics
            .fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .get()
    }

    /// The three drain counts, in `FetchDrainPath::ALL` order, so a test can
    /// compare the whole split at once instead of one path at a time.
    fn drain_counts(metrics: &BrokerMetrics) -> [u64; 3] {
        FetchDrainPath::ALL.map(|path| drained(metrics, path))
    }

    #[tokio::test]
    async fn writer_rejects_an_empty_fetch_plan() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connect = tokio::spawn(tokio::net::TcpStream::connect(
            listener.local_addr().unwrap(),
        ));
        let (mut server, _) = listener.accept().await.unwrap();
        let client = connect.await.unwrap().unwrap();
        let metrics = BrokerMetrics::new();

        let error = write_fetch_plan(&mut server, Vec::new(), &metrics)
            .await
            .expect_err("empty plans cannot form a Kafka response frame");

        assert2::assert!(error.to_string().contains("empty write plan"));
        // A response that never reached the client counts on no path.
        assert2::assert!((drain_counts(&metrics)) == ([0, 0, 0]));
        drop(client);
    }

    /// A plan of inline ops is the portable Increment-C path, and it is the
    /// only path a non-SENDFILE target can take, so it is asserted on every
    /// platform.
    #[tokio::test]
    async fn inline_only_plan_counts_as_the_vectored_path() {
        use {bytes::Bytes, tokio::io::AsyncReadExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).await.unwrap();
            got
        });
        let (mut server, _) = listener.accept().await.unwrap();
        let metrics = BrokerMetrics::new();

        let ops = vec![
            WriteOp::Inline(Bytes::from_static(b"header")),
            WriteOp::Inline(Bytes::from_static(b"records")),
        ];
        write_fetch_plan(&mut server, ops, &metrics).await.unwrap();
        drop(server);

        assert2::assert!((client.await.unwrap()) == (b"headerrecords".to_vec()));
        // One drained response, on the vectored path and no other.
        assert2::assert!((drain_counts(&metrics)) == ([0, 1, 0]));
    }

    // ─── Increment D/E (cross-platform sendfile) tests ────────────────────
    // Compiled on every SENDFILE-alias platform (Linux + Apple + FreeBSD/
    // DragonFly). The loopback-TCP roundtrip exercises the real readiness +
    // partial-write loop. The kTLS roundtrip below stays Linux-only (kTLS is a
    // Linux-only dependency).
    crate::sendfile_cfg! {
        mod sendfile_tests {
            use bytes::Bytes;

            use super::*;
            use crate::network::fetch_writer::{resolve_records_sendfile, test_support::file_payload};

            fn sequence_records(count: u32) -> Bytes {
                Bytes::from((0..count).flat_map(u32::to_le_bytes).collect::<Vec<_>>())
            }

            async fn receive_records(
                len: usize,
            ) -> (tokio::net::TcpStream, tokio::task::JoinHandle<Vec<u8>>) {
                use tokio::io::AsyncReadExt;
                let (server, mut client) = crate::network::test_support::tcp_pair().await;
                let reader = tokio::spawn(async move {
                    let mut got = vec![0u8; len];
                    client.read_exact(&mut got).await.unwrap();
                    got
                });
                (server, reader)
            }

            fn file_operations(payload: &krabka_protocol::records::RecordsPayload) -> Vec<WriteOp> {
                let ops = resolve_records_sendfile(payload).unwrap();
                assert2::assert!(ops.iter().any(|o| matches!(o, WriteOp::File(_))));
                ops
            }

            async fn drain_file_and_close<S: AsyncWrite + SendfileSink + Unpin>(
                mut server: S,
                payload: &krabka_protocol::records::RecordsPayload,
            ) -> BrokerMetrics {
                let ops = file_operations(payload);
                let metrics = BrokerMetrics::new();
                write_fetch_plan(&mut server, ops, &metrics).await.unwrap();
                drop(server);
                metrics
            }

            /// End-to-end `sendfile` over a real loopback TCP socket: the bytes
            /// the client reads must equal the file region. The test drives the
            /// real readiness and partial-write loop in `write_fetch_plan`.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn sendfile_roundtrip_over_tcp_is_byte_exact() {
                // Larger than the deliberately small send buffer to exercise partial writes.
                let records = sequence_records(4000);
                let (_tf, payload) = file_payload(&records);
                let (server, client) = receive_records(records.len()).await;
                // Shrink the send buffer to force partial sendfile writes.
                {
                    use socket2::SockRef;
                    let sr = SockRef::from(&server);
                    let _ = sr.set_send_buffer_size(8 * 1024);
                }
                let metrics = drain_file_and_close(server, &payload).await; // EOF for the client's read_exact tail
                assert2::assert!(
                    (client.await.unwrap()) == (&records[..]),
                    "sendfile'd bytes must match file"
                );

                // Byte equality alone cannot tell the kernel drain apart from the
                // copy that produces the same bytes. The counter can, and it is
                // the only thing here that would notice the plaintext fetch path
                // falling back.
                assert2::assert!((drain_counts(&metrics)) == ([1, 0, 0]));
            }

            /// The drain's own fallback arm: a stream that calls itself
            /// sendfile-capable but hands out no socket has its file region
            /// `pread` into a buffer. The bytes stay identical, so the counter is
            /// the only thing that separates this from the zero-copy drain — which
            /// is exactly why it is a separate label.
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn pread_fallback_is_byte_exact_and_counts_as_its_own_path() {
                // Larger than the deliberately small send buffer to exercise partial writes.
                let records = sequence_records(2000);
                let (_tf, payload) = file_payload(&records);
                let (server, client) = receive_records(records.len()).await;
                let server = NoSendfileStream(server);
                let metrics = drain_file_and_close(server, &payload).await; // EOF for the client's read_exact tail

                assert2::assert!((client.await.unwrap()) == (records.to_vec()));
                assert2::assert!((drain_counts(&metrics)) == ([0, 0, 1]));
            }

            /// A TCP stream that reports itself sendfile-capable but refuses to
            /// lend its socket, which is the shape of a stream that encrypts in
            /// userspace. It drives the drain's `pread` arm without standing up a
            /// TLS session, whose handshake is not what that arm is about.
            struct NoSendfileStream(tokio::net::TcpStream);

            impl SendfileSink for NoSendfileStream {
                fn is_sendfile_capable(&self) -> bool {
                    true
                }
                fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
                    None
                }
            }

            impl tokio::io::AsyncWrite for NoSendfileStream {
                fn poll_write(
                    mut self: std::pin::Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                    buf: &[u8],
                ) -> std::task::Poll<std::io::Result<usize>> {
                    std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
                }
                krabka_macros::async_write_delegate!(tuple);
            }

            /// Increment F end-to-end: `sendfile(2)` a `FileRegions` payload onto a
            /// `ktls::KtlsStream` (kernel-offloaded TLS) and assert the bytes a
            /// rustls TLS *client* decrypts are byte-identical to the file region.
            /// This proves that the kTLS path is wire-compatible: the kernel
            /// encrypts the same plaintext the userspace rustls path would have,
            /// so the client sees the same plaintext after decryption.
            ///
            /// The test skips, and does not fail, when the host kernel has no
            /// kTLS support, that is, when the `tls` module is not loaded or
            /// `CONFIG_TLS` is absent. The startup probe gates this exact
            /// condition in production, so a skip here mirrors a run of the
            /// fallback path.
            ///
            /// Linux-only: `ktls` is a Linux-only dependency, so this test is not
            /// compiled on the Apple/BSD members of the SENDFILE alias.
            #[cfg(target_os = "linux")]
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn ktls_sendfile_over_tls_is_byte_exact() {
                use tokio::{
                    io::{AsyncReadExt, AsyncWriteExt},
                    net::{TcpListener, TcpStream},
                };

                // The request the client sends before reading the response —
                // mirrors the real broker flow (client sends ApiVersions/Fetch, the
                // broker reads it via `Framed`, then writes the fetch response). It
                // also exercises the kTLS RX path on the server side.
                const REQ: &[u8] = b"fetch-request";

                let _ = rustls::crypto::ring::default_provider().install_default();

                // Records large enough to span several TLS records + partial writes.
                let mut records = Vec::new();
                for i in 0..8000u32 {
                    records.extend_from_slice(&i.to_le_bytes());
                }
                let records = Bytes::from(records);
                let (_tf, payload) = file_payload(&records);

                // Throwaway self-signed cert (localhost SAN).
                let (cert, key) = crate::test_support::localhost_ecdsa_pair();
                let key_der = rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap();

                let (acceptor, connector) = crate::network::ktls_probe::tls13_pair(&cert, key_der).unwrap();

                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();

                let expected = records.clone();
                let client = tokio::spawn(async move {
                    let tcp = TcpStream::connect(addr).await.unwrap();
                    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                    let mut tls = connector.connect(name, tcp).await.unwrap();
                    tls.write_all(REQ).await.unwrap();
                    tls.flush().await.unwrap();
                    let mut got = vec![0u8; expected.len()];
                    tls.read_exact(&mut got).await.unwrap();
                    got
                });

                let (tcp, _) = listener.accept().await.unwrap();
                {
                    use socket2::SockRef;
                    let sr = SockRef::from(&tcp);
                    let _ = sr.set_send_buffer_size(16 * 1024);
                }
                let tls = acceptor.accept(ktls::CorkStream::new(tcp)).await.unwrap();
                let mut ktls_stream = match ktls::config_ktls_server(tls).await {
                    Ok(s) => s,
                    Err(e) => {
                        // Kernel lacks kTLS support — the production startup probe
                        // would have returned false and the fallback path runs.
                        eprintln!(
                            "skipping ktls_sendfile_over_tls_is_byte_exact: kTLS unsupported on this host: {e}"
                        );
                        client.abort();
                        return;
                    }
                };

                // Read the client's request through the KtlsStream first (kTLS RX
                // + the ktls crate's drained-bytes replay). The real broker always
                // reads the Fetch request before writing the response.
                let mut req = vec![0u8; REQ.len()];
                ktls_stream.read_exact(&mut req).await.unwrap();
                assert2::assert!(
                    (req) == (REQ),
                    "kTLS RX must deliver the request bytes intact"
                );

                // The KtlsStream must report itself sendfile-capable, and the
                // resolver must emit a File op (true zero-copy over TLS).
                assert2::assert!(SendfileSink::is_sendfile_capable(&ktls_stream));
                let ops = file_operations(&payload);

                // sendfile the file region onto the kTLS socket — the kernel
                // encrypts it into TLS records on the way out.
                let metrics = BrokerMetrics::new();
                write_fetch_plan(&mut ktls_stream, ops, &metrics)
                    .await
                    .unwrap();
                ktls_stream.flush().await.unwrap();
                drop(ktls_stream); // sends close_notify; EOF for the client tail

                let got = client.await.unwrap();
                assert2::assert!(
                    (got) == (&records[..]),
                    "client-decrypted kTLS bytes must equal the file region (wire byte-exact)"
                );
                // kTLS is the one encrypted path that still reaches the kernel
                // drain; a fallback to userspace rustls would land on `pread`.
                assert2::assert!((drain_counts(&metrics)) == ([1, 0, 0]));
            }
        }
    }
}
