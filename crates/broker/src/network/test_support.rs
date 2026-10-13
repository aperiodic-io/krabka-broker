//! Wire and socket fixtures shared by network unit tests.

use {
    crate::config::BrokerConfig,
    bytes::{BufMut as _, BytesMut},
    tokio::net::{TcpListener, TcpStream},
};

// Build request headers independently of the parser under test. Arbitrary
// tagged bytes let malformed-header cases exercise the same path.
krabka_macros::request_frame_fixture!(request_frame);

pub(crate) fn raw_request_frame(
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) -> BytesMut {
    let mut frame = BytesMut::new();
    frame.put_i16(api_key);
    frame.put_i16(version);
    frame.put_i32(correlation_id);
    frame.put_slice(body);
    frame
}

pub(crate) async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener addr");
    let client_task = tokio::spawn(TcpStream::connect(addr));
    let (server, _) = listener.accept().await.expect("accept loopback client");
    let client = client_task
        .await
        .expect("connect task")
        .expect("connect loopback client");
    (server, client)
}

/// Kernels clamp and may double requested sizes; compare the configured
/// buffers to their deliberately smaller baselines rather than host-specific
/// values. Both accepted and outbound tuning must also enable `TCP_NODELAY`.
pub(crate) fn check_socket_tuning(socket: &TcpStream, tune: impl FnOnce(&TcpStream)) {
    let sock = socket2::SockRef::from(socket);
    socket.set_nodelay(false).expect("clear TCP_NODELAY");
    sock.set_send_buffer_size(4096).expect("shrink send buffer");
    sock.set_recv_buffer_size(8192).expect("shrink recv buffer");
    let send_before = sock.send_buffer_size().expect("read baseline send buffer");
    let recv_before = sock.recv_buffer_size().expect("read baseline recv buffer");
    tune(socket);
    assert2::assert!(socket.nodelay().expect("read TCP_NODELAY"));
    let send_after = sock.send_buffer_size().expect("read send buffer");
    let recv_after = sock.recv_buffer_size().expect("read recv buffer");
    assert2::assert!(send_after > send_before);
    assert2::assert!(recv_after > recv_before);
    assert2::assert!(recv_after > send_after);
}

/// Check every authentication field; only the event's clock value is supplied by the event.
pub(crate) fn assert_authentication_event(
    event: &krabka_audit::AuditEvent,
    outcome: krabka_audit::AuditOutcome,
    mechanism: &str,
    principal: (&str, &str),
    source: (&str, u16),
    reason: Option<String>,
) {
    let krabka_audit::AuditEvent::Authentication { time_ms, .. } = event else {
        panic!("expected an Authentication event, got {event:?}");
    };
    assert2::assert!(
        event
            == &krabka_audit::AuditEvent::Authentication {
                outcome,
                mechanism: mechanism.to_owned(),
                principal: krabka_audit::AuditPrincipal {
                    name: principal.0.to_owned(),
                    auth_method: principal.1.to_owned()
                },
                source: krabka_audit::AuditEndpoint {
                    ip: source.0.to_owned(),
                    port: source.1
                },
                reason,
                time_ms: *time_ms,
            }
    );
}

/// The unchanged default config and its directory guard, before caller-specific edits.
pub(crate) fn broker_config() -> (tempfile::TempDir, BrokerConfig) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    (dir, cfg)
}
