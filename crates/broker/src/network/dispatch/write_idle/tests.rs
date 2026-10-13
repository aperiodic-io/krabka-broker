//! Portable write stalls, continuing progress, and request-budget release.

use {
    super::WriteIdle,
    crate::{
        SslPrincipalMapper,
        config::ListenerSpec,
        network::{
            dispatch::{serve_connection_stream, test_support::plaintext_loop},
            fetch_writer::SendfileSink,
            test_support::request_frame,
        },
        test_support::start_broker_with,
    },
    assert2::assert,
    futures_util::{SinkExt as _, StreamExt as _},
    krabka_security::ListenerProtocol,
    std::time::Duration,
    tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream},
};

impl SendfileSink for DuplexStream {
    crate::sendfile_cfg! {
        fn tcp_for_sendfile(&self) -> Option<&tokio::net::TcpStream> { None }
    }

    fn is_sendfile_capable(&self) -> bool { false }
}

#[tokio::test(start_paused = true)]
async fn stalled_write_expires() {
    let (stream, _reader) = tokio::io::duplex(1);
    let mut writer = WriteIdle::new(stream, Some(Duration::from_millis(100)));
    let error = writer
        .write_all(&[1, 2])
        .await
        .expect_err("full sink must expire");
    assert!(error.kind() == std::io::ErrorKind::TimedOut);
}

#[tokio::test(start_paused = true)]
async fn successful_writes_rearm_the_window_instead_of_limiting_total_duration() {
    let (stream, mut reader) = tokio::io::duplex(1);
    let task = tokio::spawn(async move {
        WriteIdle::new(stream, Some(Duration::from_millis(100)))
            .write_all(&[1, 2, 3, 4, 5])
            .await
    });
    let mut received = Vec::new();
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(60)).await;
        received.push(reader.read_u8().await.expect("ongoing write"));
    }
    task.await
        .expect("writer task")
        .expect("progress prevents idle expiry");
    assert!(received == vec![1, 2, 3, 4, 5]);
}

#[tokio::test(start_paused = true)]
async fn disabled_idle_window_leaves_a_stalled_write_pending() {
    let (stream, _reader) = tokio::io::duplex(1);
    let mut writer = WriteIdle::new(stream, None);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), writer.write_all(&[1, 2]))
            .await
            .is_err()
    );
}

crate::sendfile_cfg! {
    #[tokio::test]
    async fn stalled_sendfile_obeys_the_same_idle_window() {
        use std::{io::Write as _, sync::Arc};
        use krabka_protocol::records::FileRegion;
        use crate::{
            error::BrokerError,
            metrics::BrokerMetrics,
            network::{fetch_writer::{WriteOp, write_fetch_plan}, test_support::tcp_pair},
        };

        let (server, _nonreading_client) = tcp_pair().await;
        socket2::SockRef::from(&server).set_send_buffer_size(4096).expect("small send buffer");
        let mut file = tempfile::NamedTempFile::new().expect("source file");
        let bytes = vec![7; 8 * 1024 * 1024];
        file.write_all(&bytes).expect("file contents");
        let region = FileRegion { file: Arc::new(file.reopen().expect("file handle")), offset: 0, len: bytes.len() };
        let mut stream = WriteIdle::new(server, Some(Duration::from_millis(50)));
        let error = tokio::time::timeout(Duration::from_secs(2), write_fetch_plan(
            &mut stream, vec![WriteOp::File(region)], &BrokerMetrics::new()
        )).await.expect("idle timeout bounds sendfile").expect_err("nonreading peer stalls");
        let BrokerError::Io(error) = error else { panic!("expected I/O timeout") };
        assert!(error.kind() == std::io::ErrorKind::TimedOut);
    }
}

#[tokio::test]
async fn stalled_connection_returns_broker_queue_permits_and_other_requests_proceed() {
    let (handle, _directory) = start_broker_with(|config| {
        config.connections_max_idle = Some(krabka_units::millis(100));
        config.queued_max_requests = 1;
        config.queued_max_request_bytes = Some(krabka_units::bytes(65_536));
    })
    .await;
    let broker = handle.broker_arc_for_test();
    let (stream, mut client) = tokio::io::duplex(1);
    let spec = ListenerSpec {
        name: "PLAINTEXT".into(),
        bind_addr: "127.0.0.1:9092".parse().expect("address"),
        advertised: "127.0.0.1:9092".into(),
        protocol: ListenerProtocol::Plaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    };
    let task = tokio::spawn(serve_connection_stream(
        broker.clone(),
        stream,
        spec,
        "127.0.0.1:1234".parse().expect("peer"),
        None,
    ));
    let request = request_frame().call().freeze();
    client
        .write_i32(i32::try_from(request.len()).expect("small request"))
        .await
        .expect("prefix");
    client.write_all(&request).await.expect("request");
    tokio::time::timeout(Duration::from_secs(2), async {
        while broker.queued_requests_sem.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("response holds capacity while client does not read");
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("write timeout closes connection")
        .expect("serve task");
    assert!(broker.queued_requests_sem.available_permits() == 1);
    assert!(
        broker
            .queued_request_bytes
            .as_ref()
            .expect("byte budget")
            .available()
            == 65_536
    );
    assert!(broker.metrics.queued_requests.get() == 0);

    let (healthy_task, mut healthy) = plaintext_loop(broker).await;
    healthy.send(request).await.expect("healthy request");
    tokio::time::timeout(Duration::from_secs(2), healthy.next())
        .await
        .expect("healthy response arrives")
        .expect("response")
        .expect("valid frame");
    drop(healthy);
    healthy_task.await.expect("healthy connection closes");
    handle.shutdown().await;
}
