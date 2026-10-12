//! The kernel `sendfile(2)` transfer: one readiness-driven loop over partial
//! writes, and the per-OS syscall attempt below it.
//!
//! The loop is identical on every SENDFILE-alias platform. Only the single
//! syscall differs, because the Linux `rustix` binding reports a would-block
//! without a byte count while the BSD-family `nix` binding reports a count
//! that is valid even on `EAGAIN`.

crate::sendfile_cfg! {
    use crate::{error::BrokerError, time_util::sleep_until_opt};
}

crate::sendfile_cfg! {
    /// `sendfile(2)` a `FileRegion` to a plaintext `TcpStream`, in a loop over
    /// partial writes and `EAGAIN`.
    ///
    /// Every SENDFILE-alias platform **shares** the readiness loop. The socket
    /// is non-blocking under tokio, so on a full socket buffer the syscall
    /// reports `EAGAIN`/`WouldBlock`. The loop then awaits `tcp.writable()`
    /// and retries. `TcpStream::try_io` clears the readiness flag correctly on
    /// `WouldBlock`, so this needs no `spawn_blocking` and no second `AsyncFd`
    /// over the fd.
    ///
    /// This method tracks its own `sent_total` cursor and computes the
    /// absolute file offset for each attempt as `region.offset + sent_total`.
    /// It never touches the file's own cursor, and concurrent reads of the
    /// same `Arc<File>` stay unaffected. Only the single per-OS syscall
    /// attempt ([`sendfile_once`]) is cfg-selected. Everything around it is
    /// identical on Linux and on Apple/BSD.
    pub(super) async fn sendfile_region(
        tcp: &tokio::net::TcpStream,
        region: &krabka_protocol::records::FileRegion,
        idle: Option<std::time::Duration>,
    ) -> Result<(), BrokerError> {
        use std::io::ErrorKind;
        use std::os::fd::{AsFd, BorrowedFd};

        let in_fd: BorrowedFd<'_> = region.file.as_fd();
        // `TcpStream: AsFd` — borrow the socket fd safely (no `unsafe`/`borrow_raw`).
        let out_fd: BorrowedFd<'_> = tcp.as_fd();

        let mut sent_total: usize = 0;
        let mut deadline = idle.map(|window| tokio::time::Instant::now() + window);

        while sent_total < region.len {
            // Wait for the socket to be writable, then attempt one sendfile. If
            // the kernel reports it would block (with no forward progress),
            // `try_io` returns WouldBlock and we loop back to `writable()`.
            tokio::select! {
                biased;
                () = sleep_until_opt(deadline) => {
                    return Err(BrokerError::Io(std::io::Error::new(
                        ErrorKind::TimedOut, "sendfile made no progress within connections.max.idle.ms"
                    )));
                }
                result = tcp.writable() => result.map_err(BrokerError::Io)?,
            }
            let offset = region.offset + sent_total as u64;
            let remaining = region.len - sent_total;
            let res = tcp.try_io(tokio::io::Interest::WRITABLE, || {
                sendfile_once(out_fd, in_fd, offset, remaining)
            });
            match res {
                Ok(0) => {
                    // The syscall reported success with zero bytes while bytes
                    // still remain: the source file is shorter than expected
                    // (truncated mid-send). The `Arc<File>` should prevent this;
                    // treat as an I/O error so the connection closes rather than
                    // emitting a short frame.
                    return Err(BrokerError::Io(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "sendfile returned 0 before region fully sent",
                    )));
                }
                Ok(n) => {
                    sent_total += n;
                    deadline = idle.map(|window| tokio::time::Instant::now() + window);
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    // Not writable yet (true would-block: zero bytes moved); loop
                    // and re-await readiness.
                }
                Err(e) => return Err(BrokerError::Io(e)),
            }
        }
        Ok(())
    }
}

crate::sendfile_cfg! {
    /// One non-blocking `sendfile(2)` attempt. It returns the bytes transferred on
    /// this call. A true would-block, with zero forward progress, surfaces as
    /// `ErrorKind::WouldBlock` so the shared readiness loop re-arms. Any positive
    /// transfer returns `Ok(n)`, even when the kernel also signalled `EAGAIN`.
    ///
    /// **Linux** (`rustix`): `sendfile(out, in, Some(&mut offset), count)` returns
    /// the count and mutates `offset` in place. On `EAGAIN` it returns `Err`. The
    /// kernel does not report a partial count in `errno`, so `Err(EAGAIN)` always
    /// means zero bytes on this call. This function maps it straight to
    /// `WouldBlock`.
    ///
    /// **Apple / FreeBSD / `DragonFly`** (`nix`): the BSD-family `sendfile` returns
    /// `(nix::Result<()>, off_t)`, where the `off_t` is the bytes transferred on
    /// this call. That count is **valid even on `Err(EAGAIN)`**. This is the
    /// correctness landmine: on these platforms `EAGAIN` with `n > 0` is *forward
    /// progress*, not would-block. So this function does the following:
    ///
    /// * `Ok(())` → return `Ok(n)`. The send was full or partial, and the loop
    ///   advances by `n`.
    /// * `Err(EAGAIN)` with `n>0` → return `Ok(n)`. This counts the progress, and
    ///   the loop advances and re-arms readiness for the rest.
    /// * `Err(EAGAIN)` with `n==0` → return `Err(WouldBlock)`, a real would-block.
    /// * any other `Err` → propagate as a hard I/O error.
    ///
    /// `count` is always `Some(region_remaining)` and never `None` or 0, which
    /// would mean "to EOF", so this function never overshoots into the next batch.
    /// The header and trailer `hdtr` slices are `None`, because the frame metadata
    /// is a separate `WriteOp::Inline`, exactly as on Linux.
    ///
    /// NOTE: this arm is compile-reasoned only. The Windows/WSL toolchains used
    /// here do not build or run it. It needs a macOS or FreeBSD CI runner to
    /// verify the syscall semantics and the byte-exact wire output.
    fn sendfile_once(out_fd: std::os::fd::BorrowedFd<'_>, in_fd: std::os::fd::BorrowedFd<'_>, offset: u64, count: usize) -> std::io::Result<usize> {
        #[cfg(target_os = "linux")]
        {
            let mut off = offset;
            rustix::fs::sendfile(out_fd, in_fd, Some(&mut off), count).map_err(std::io::Error::from)
        }
        #[cfg(not(target_os = "linux"))]
        {
            use nix::errno::Errno;

            // `off_t` is the kernel's signed file-offset type; the byte ranges we send
            // are bounded by the segment size and always fit. Saturate defensively
            // rather than wrap if an offset ever exceeded `off_t::MAX`.
            let off = nix::libc::off_t::try_from(offset).unwrap_or(nix::libc::off_t::MAX);

            // The `count` parameter's element type differs by platform: macOS/iOS take
            // `Option<off_t>`, FreeBSD/DragonFly take `Option<usize>`. The
            // `count_arg` shim normalizes our `usize remaining` to the right type. We
            // never pass `None` (which would mean "send to EOF" and could overshoot the
            // current batch into the next one in the same `.log` file).
            let (result, sent) = bsd_sendfile(in_fd, out_fd, off, count);

            let n = usize::try_from(sent).unwrap_or(0);
            match result {
                // Fully/partially transferred without error.
                Ok(()) => Ok(n),
                // EAGAIN/EWOULDBLOCK: on BSD-family this can accompany real forward
                // progress (n > 0). Count the progress; only a zero-progress EAGAIN is a
                // true would-block that the readiness loop must wait on.
                Err(Errno::EAGAIN) => {
                    if n > 0 {
                        Ok(n)
                    } else {
                        Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
                    }
        }
        // EINTR with progress is also forward progress; without progress, retry
        // is harmless — surface as WouldBlock so the loop re-arms and re-issues.
        Err(Errno::EINTR) if n > 0 => Ok(n),
        Err(Errno::EINTR) => Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
        Err(e) => Err(std::io::Error::from_raw_os_error(e as i32)),
    }
        }
    }
}

/// Platform shim over the per-OS BSD-family `nix::sys::sendfile::sendfile`
/// signatures. macOS uses `Option<off_t>` for `count` and no flags. FreeBSD
/// also takes `SfFlags` and a readahead hint. `DragonFly` takes neither, but
/// uses `Option<usize>`. Returns `(nix::Result<()>, off_t bytes_sent)`.
///
/// Compile-reasoned only, because there is no macOS or BSD toolchain here. It
/// needs CI verification.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly"
))]
fn bsd_sendfile(
    in_fd: std::os::fd::BorrowedFd<'_>,
    out_sock: std::os::fd::BorrowedFd<'_>,
    offset: nix::libc::off_t,
    count: usize,
) -> (nix::Result<()>, nix::libc::off_t) {
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos"
    ))]
    {
        // Apple count is Option<off_t>, without flags or headers/trailers.
        let count = Some(nix::libc::off_t::try_from(count).unwrap_or(nix::libc::off_t::MAX));
        nix::sys::sendfile::sendfile(in_fd, out_sock, offset, count, None, None)
    }
    #[cfg(target_os = "freebsd")]
    {
        nix::sys::sendfile::sendfile(
            in_fd,
            out_sock,
            offset,
            Some(count),
            None,
            None,
            nix::sys::sendfile::SfFlags::empty(),
            0,
        )
    }
    #[cfg(target_os = "dragonfly")]
    {
        nix::sys::sendfile::sendfile(in_fd, out_sock, offset, Some(count), None, None)
    }
}
