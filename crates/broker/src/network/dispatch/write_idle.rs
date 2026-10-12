//! Close a response writer that stops making progress, returning its queue permits.

use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};

use crate::network::fetch_writer::SendfileSink;

/// The listener's idle window applies to pending writes as well as reads.
/// Each successful write starts a new window; handler and quota waits do not.
pub(crate) struct WriteIdle<S> {
    inner: S,
    window: Option<Duration>,
    pending: Option<Pin<Box<Sleep>>>,
}

impl<S> WriteIdle<S> {
    pub(crate) fn new(inner: S, window: Option<Duration>) -> Self {
        Self {
            inner,
            window,
            pending: None,
        }
    }

    fn stalled<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        let Some(window) = self.window else {
            return Poll::Pending;
        };
        let timer = self
            .pending
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(window)));
        if timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response write made no progress within connections.max.idle.ms",
            )))
        } else {
            Poll::Pending
        }
    }

    fn poll_result<T>(
        &mut self,
        cx: &mut Context<'_>,
        result: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match result {
            Poll::Ready(result) => {
                self.pending = None;
                Poll::Ready(result)
            }
            Poll::Pending => self.stalled(cx),
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for WriteIdle<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WriteIdle<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.poll_result(cx, result)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.poll_result(cx, result)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        this.poll_result(cx, result)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.poll_result(cx, result)
    }
}

impl<S: SendfileSink> SendfileSink for WriteIdle<S> {
    fn write_idle_is_enforced(&self) -> bool {
        self.window.is_some() || self.inner.write_idle_is_enforced()
    }
    fn is_sendfile_capable(&self) -> bool {
        self.inner.is_sendfile_capable()
    }

    crate::sendfile_cfg! {
        fn write_idle_window(&self) -> Option<Duration> {
            self.window.or_else(|| self.inner.write_idle_window())
        }

        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> {
            self.inner.tcp_for_sendfile()
        }
    }
}

impl<S: SendfileSink> WriteIdle<S> {
    /// Apply a listener's policy only if no lower transport layer already
    /// measures progress. TLS must measure ciphertext writes below rustls.
    pub(super) fn for_response(inner: S, window: Option<Duration>) -> Self {
        let window = if inner.write_idle_is_enforced() {
            None
        } else {
            window
        };
        Self::new(inner, window)
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tls_tests;
