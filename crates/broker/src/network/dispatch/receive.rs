//! Reserve request bytes after the size prefix and before reading the body.

use {
    super::RequestByteBudget,
    crate::time_util::sleep_until_opt,
    bytes::{Bytes, BytesMut},
    std::{io, time::Duration},
    tokio::{
        io::{AsyncRead, AsyncReadExt as _},
        sync::OwnedSemaphorePermit,
    },
};

pub(super) struct RequestFrame {
    pub(super) bytes: Bytes,
    pub(super) byte_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub(super) enum ReceiveError {
    Idle,
    Io(io::Error),
}

impl From<io::Error> for ReceiveError {
    fn from(error: io::Error) -> Self { Self::Io(error) }
}

impl RequestByteBudget {
    async fn acquire(&self, frame_bytes: usize) -> io::Result<OwnedSemaphorePermit> {
        let wanted = frame_bytes.max(1);
        if wanted > self.total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds queued.max.request.bytes",
            ));
        }
        let wanted = u32::try_from(wanted).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "request size exceeds the int32 frame limit",
            )
        })?;
        self.permits
            .clone()
            .acquire_many_owned(wanted)
            .await
            .map_err(|_| io::Error::other("request-byte budget is closed"))
    }
}

/// An idle peer holds no capacity, and a waiting size prefix holds no body.
/// Broker byte-budget waits do not consume the client's idle window.
pub(super) async fn receive_request<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_request_bytes: usize,
    budget: Option<&RequestByteBudget>,
    idle: Option<Duration>,
) -> Result<Option<RequestFrame>, ReceiveError> {
    let Some(size) = within_idle(idle, read_size(stream, max_request_bytes)).await? else {
        return Ok(None);
    };
    let byte_permit = match budget {
        Some(budget) => Some(budget.acquire(size).await?),
        None => None,
    };
    // Allocate only after the advertised body has reserved its whole budget.
    let bytes = within_idle(idle, read_body(stream, size)).await?;
    Ok(Some(RequestFrame { bytes, byte_permit }))
}

async fn within_idle<T>(
    idle: Option<Duration>,
    read: impl Future<Output = io::Result<T>>,
) -> Result<T, ReceiveError> {
    let deadline = idle.map(|window| tokio::time::Instant::now() + window);
    tokio::select! {
        biased;
        () = sleep_until_opt(deadline) => Err(ReceiveError::Idle),
        result = read => result.map_err(ReceiveError::Io),
    }
}

async fn read_size<S: AsyncRead + Unpin>(
    stream: &mut S,
    maximum: usize,
) -> io::Result<Option<usize>> {
    let mut prefix = [0_u8; 4];
    let read = stream.read(&mut prefix).await?;
    if read == 0 {
        return Ok(None);
    }
    stream.read_exact(&mut prefix[read..]).await?;
    let size = usize::try_from(i32::from_be_bytes(prefix))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative Kafka frame size"))?;
    if size > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds configured maximum size",
        ));
    }
    Ok(Some(size))
}

async fn read_body<S: AsyncRead + Unpin>(stream: &mut S, size: usize) -> io::Result<Bytes> {
    // Read into the buffer's spare capacity rather than zero-filling it
    // first: every request body is about to be overwritten in full. The
    // reader is bounded to this frame's remaining bytes, so no read reaches
    // into the next frame however much spare capacity the buffer has.
    let mut body = BytesMut::with_capacity(size);
    while body.len() < size {
        let remaining = u64::try_from(size - body.len()).unwrap_or(u64::MAX);
        if (&mut *stream).take(remaining).read_buf(&mut body).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    }
    Ok(body.freeze())
}

#[cfg(test)]
mod tests;
