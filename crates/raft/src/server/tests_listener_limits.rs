//! The controller listener enforces the limits Kafka's `SocketServer` puts on
//! every listener it builds for `ControllerServer`: `socket.request.max.bytes`,
//! `connections.max.idle.ms`, and `max.connections` with
//! `max.connections.per.ip`.

use std::{sync::Arc, time::Duration};

use assert2::check;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

use super::{ConnectionContext, Unstable, handle_conn, run, test_support::single_voter_engine};
use crate::{AllowAllGrants, ListenerLimits, RaftError};

mod session_expiry;

/// An `ApiVersions` v0 request frame: a v1 header, `client_id` "c", no body.
fn api_versions_request(correlation_id: i32) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&18i16.to_be_bytes());
    frame.extend_from_slice(&0i16.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(&1i16.to_be_bytes());
    frame.push(b'c');
    let mut out = i32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(&frame);
    out
}

fn context(limits: ListenerLimits) -> ConnectionContext {
    ConnectionContext {
        peer: "127.0.0.1:9093".parse().unwrap(),
        principal: None,
        authenticated_via_token: false,
        expires_at_ms: None,
        grants: Arc::new(AllowAllGrants),
        unstable: Unstable::default(),
        limits,
    }
}

fn duplex_connection(
    engine: crate::kraft::KraftController,
    limits: ListenerLimits,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<(), RaftError>>,
) {
    let (client, server) = tokio::io::duplex(1 << 16);
    let connection = tokio::spawn(handle_conn(
        server,
        engine,
        CancellationToken::new(),
        None,
        None,
        context(limits),
        crate::kraft::KraftController::wall_clock_ms,
    ));
    (client, connection)
}

/// Reads one response frame and returns its correlation id.
async fn read_correlation_id<R: AsyncReadExt + Unpin>(stream: &mut R) -> i32 {
    let frame = super::test_support::read_frame(stream).await;
    i32::from_be_bytes(frame[..4].try_into().unwrap())
}

/// A size prefix over `socket.request.max.bytes` ends the connection with no
/// answer. The stream carries only the four size bytes, so a listener that
/// tried to read the frame first would wait on it instead of failing.
#[tokio::test]
async fn an_oversize_request_frame_closes_the_connection() {
    let (engine, _dir) = single_voter_engine();
    let limits = ListenerLimits {
        max_request_size: krabka_units::prelude::bytes(64),
        ..ListenerLimits::default()
    };
    let (mut client, connection) = duplex_connection(engine, limits);

    // A frame at the limit is served.
    client.write_all(&api_versions_request(7)).await.unwrap();
    check!(read_correlation_id(&mut client).await == 7);

    client.write_all(&65_i32.to_be_bytes()).await.unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .expect("the connection ends")
        .expect("the connection task does not panic");
    check!(matches!(
        outcome,
        Err(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue(_)
        ))
    ));
    let mut rest = Vec::new();
    check!(client.read_to_end(&mut rest).await.unwrap() == 0);
}

/// `connections.max.idle.ms` closes a connection that sends no request for
/// the window, and each request restarts the window.
#[tokio::test(start_paused = true)]
async fn an_idle_connection_closes_after_the_idle_window() {
    let (engine, _dir) = single_voter_engine();
    let window = Duration::from_secs(30);
    let limits = ListenerLimits {
        max_idle: Some(window),
        ..ListenerLimits::default()
    };
    let (mut client, mut connection) = duplex_connection(engine, limits);

    // Two requests 20 s apart keep a 30 s window open for 40 s.
    for correlation_id in [1, 2] {
        tokio::time::sleep(Duration::from_secs(20)).await;
        client
            .write_all(&api_versions_request(correlation_id))
            .await
            .unwrap();
        check!(read_correlation_id(&mut client).await == correlation_id);
        check!(!connection.is_finished());
    }

    tokio::time::sleep(window + Duration::from_secs(1)).await;
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut connection)
        .await
        .expect("the listener closes the idle connection")
        .expect("the connection task does not panic");
    check!(outcome.is_ok());
}

/// A window of `None` expires nothing.
#[tokio::test(start_paused = true)]
async fn a_listener_without_an_idle_window_keeps_a_silent_connection() {
    let (engine, _dir) = single_voter_engine();
    let limits = ListenerLimits {
        max_idle: None,
        ..ListenerLimits::default()
    };
    let (_client, connection) = duplex_connection(engine, limits);

    // Longer than the ten-minute Kafka default, which `None` replaces.
    tokio::time::sleep(Duration::from_mins(11)).await;
    check!(!connection.is_finished());
}

/// `max.connections` and `max.connections.per.ip` apply to the accept loop: a
/// connection over the ceiling is closed at once, and its slot returns when
/// an earlier connection ends.
#[tokio::test]
async fn the_accept_loop_closes_connections_over_the_ceiling() {
    let cases = [
        ("max.connections", 1, usize::MAX),
        ("max.connections.per.ip", usize::MAX, 1),
    ];
    for (name, max_connections, max_connections_per_ip) in cases {
        let (engine, _dir) = single_voter_engine();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let limits = ListenerLimits {
            max_connections,
            max_connections_per_ip,
            ..ListenerLimits::default()
        };
        let listener_task = tokio::spawn(run(
            listener,
            engine,
            shutdown.clone(),
            None,
            None,
            None,
            (Unstable::default(), limits),
        ));

        let mut first = TcpStream::connect(addr).await.unwrap();
        first.write_all(&api_versions_request(1)).await.unwrap();
        check!(read_correlation_id(&mut first).await == 1, "{name}");

        let mut second = TcpStream::connect(addr).await.unwrap();
        let mut refused = Vec::new();
        let closed = tokio::time::timeout(Duration::from_secs(5), second.read_to_end(&mut refused))
            .await
            .expect("the refused connection is closed");
        check!(closed.map_or(true, |read| read == 0), "{name}");
        check!(refused.is_empty(), "{name}");

        drop(first);
        let mut served = false;
        for attempt in 0..100 {
            let mut third = TcpStream::connect(addr).await.unwrap();
            third.write_all(&api_versions_request(3)).await.unwrap();
            let mut len = [0u8; 4];
            if third.read_exact(&mut len).await.is_ok() {
                served = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10 * (attempt % 5 + 1))).await;
        }
        check!(served, "{name}: the released slot serves a new connection");

        shutdown.cancel();
        listener_task.await.unwrap();
    }
}
