use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use assert2::assert;
use bytes::Bytes;
use tokio::io::{AsyncRead, ReadBuf};

use super::{ReceiveError, RequestByteBudget, receive_request};

struct CountingRead {
    remaining: Bytes,
    consumed: Arc<AtomicUsize>,
}

impl AsyncRead for CountingRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let count = buffer.remaining().min(self.remaining.len());
        buffer.put_slice(&self.remaining.split_to(count));
        self.consumed.fetch_add(count, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

fn stream(body: &[u8]) -> (CountingRead, Arc<AtomicUsize>) {
    let mut bytes = i32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    bytes.extend_from_slice(body);
    let consumed = Arc::new(AtomicUsize::new(0));
    (
        CountingRead {
            remaining: Bytes::from(bytes),
            consumed: consumed.clone(),
        },
        consumed,
    )
}

#[tokio::test]
async fn waiting_connections_read_only_the_prefix_before_capacity_is_available() {
    let budget = RequestByteBudget::of(8);
    let held = budget.acquire(8).await.unwrap();
    let (mut first, first_read) = stream(b"12345678");
    let (mut second, second_read) = stream(b"abcdefgh");
    let mut first_request = Box::pin(receive_request(&mut first, 8, Some(&budget), None));
    let mut second_request = Box::pin(receive_request(&mut second, 8, Some(&budget), None));
    assert!(futures_util::poll!(&mut first_request).is_pending());
    assert!(futures_util::poll!(&mut second_request).is_pending());
    assert!(first_read.load(Ordering::SeqCst) == 4);
    assert!(second_read.load(Ordering::SeqCst) == 4);
    // Disconnecting a budget waiter must remove its place in the semaphore
    // queue without reading a body or blocking either surviving connection.
    let (mut cancelled, cancelled_read) = stream(b"cancel!!");
    let mut cancelled_request = Box::pin(receive_request(&mut cancelled, 8, Some(&budget), None));
    assert!(futures_util::poll!(&mut cancelled_request).is_pending());
    drop(cancelled_request);
    assert!(cancelled_read.load(Ordering::SeqCst) == 4);
    drop(held);
    let first_frame = first_request.await.unwrap().unwrap();
    assert!(first_frame.bytes == Bytes::from_static(b"12345678"));
    assert!(budget.available() == 0);
    assert!(futures_util::poll!(&mut second_request).is_pending());
    assert!(second_read.load(Ordering::SeqCst) == 4);
    drop(first_frame);
    let second_frame = second_request.await.unwrap().unwrap();
    assert!(second_frame.bytes == Bytes::from_static(b"abcdefgh"));
    drop(second_frame);
    assert!(budget.available() == 8);
}

#[tokio::test]
async fn invalid_sizes_are_rejected_before_reading_the_body() {
    for (size, maximum, budget_bytes) in [(9_i32, 8, 16), (9, 16, 8), (-1, 16, 16)] {
        let consumed = Arc::new(AtomicUsize::new(0));
        let mut remaining = size.to_be_bytes().to_vec();
        remaining.extend_from_slice(b"unread body");
        let mut reader = CountingRead {
            remaining: Bytes::from(remaining),
            consumed: consumed.clone(),
        };
        let budget = RequestByteBudget::of(budget_bytes);
        assert!(
            matches!(receive_request(&mut reader, maximum, Some(&budget), None).await, Err(ReceiveError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
        assert!(consumed.load(Ordering::SeqCst) == 4);
        assert!(budget.available() == budget_bytes);
    }
}

#[tokio::test]
async fn a_truncated_body_returns_its_reserved_capacity() {
    let (mut reader, _) = stream(b"abcd");
    reader.remaining.truncate(6);
    let budget = RequestByteBudget::of(8);
    assert!(
        matches!(receive_request(&mut reader, 8, Some(&budget), None).await, Err(ReceiveError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof)
    );
    assert!(budget.available() == 8);
}

#[tokio::test]
async fn idle_connections_reserve_no_request_bytes() {
    let (_client, mut server) = tokio::io::duplex(16);
    let budget = RequestByteBudget::of(8);
    let mut request = Box::pin(receive_request(&mut server, 8, Some(&budget), None));
    assert!(futures_util::poll!(&mut request).is_pending());
    assert!(budget.available() == 8);
}

#[tokio::test(start_paused = true)]
async fn broker_budget_waits_do_not_expire_the_client_idle_window() {
    let budget = RequestByteBudget::of(8);
    let held = budget.acquire(8).await.unwrap();
    let (mut reader, consumed) = stream(b"abcdefgh");
    let mut request = Box::pin(receive_request(
        &mut reader,
        8,
        Some(&budget),
        Some(Duration::from_millis(10)),
    ));
    assert!(futures_util::poll!(&mut request).is_pending());
    assert!(consumed.load(Ordering::SeqCst) == 4);
    tokio::time::advance(Duration::from_secs(1)).await;
    drop(held);
    assert!(request.await.unwrap().unwrap().bytes == Bytes::from_static(b"abcdefgh"));
}

#[tokio::test(start_paused = true)]
async fn an_idle_body_releases_its_byte_permit() {
    use tokio::io::AsyncWriteExt as _;

    let (mut client, mut server) = tokio::io::duplex(16);
    client.write_all(&8_i32.to_be_bytes()).await.unwrap();
    let budget = RequestByteBudget::of(8);
    let mut request = Box::pin(receive_request(
        &mut server,
        8,
        Some(&budget),
        Some(Duration::from_millis(10)),
    ));
    assert!(futures_util::poll!(&mut request).is_pending());
    assert!(budget.available() == 0);
    tokio::time::advance(Duration::from_millis(11)).await;
    assert!(matches!(request.await, Err(ReceiveError::Idle)));
    assert!(budget.available() == 8);
}

#[tokio::test]
async fn pipelined_frames_are_read_one_body_at_a_time() {
    let (mut reader, _) = stream(b"first");
    let mut remaining = reader.remaining.to_vec();
    remaining.extend_from_slice(&6_i32.to_be_bytes());
    remaining.extend_from_slice(b"second");
    reader.remaining = Bytes::from(remaining);
    for expected in [b"first".as_slice(), b"second".as_slice()] {
        let frame = receive_request(&mut reader, 8, None, None)
            .await
            .unwrap()
            .unwrap();
        assert!(frame.bytes.as_ref() == expected);
    }
    assert!(
        receive_request(&mut reader, 8, None, None)
            .await
            .unwrap()
            .is_none()
    );
}
