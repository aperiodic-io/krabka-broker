//! `ApiVersions` on a `SASL_PLAINTEXT` controller listener, before and after
//! authentication.
//!
//! Kafka's `SocketServer` gives `SaslServerAuthenticator` the same
//! `apiVersionSupplier` that `ControllerApis` answers from. So the answer before
//! authentication is the full controller-listener table with the features, and
//! an unsupported version gets `UNSUPPORTED_VERSION` without closing the
//! connection.

mod support;

use std::net::SocketAddr;

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::support::discovery::api_versions_request_for;

// A request frame; flexible selects v2, and the prefix keeps the original u32 limit.
krabka_macros::vector_request_fixture!(frame, u32);

/// Sends one request and returns the response frame after the correlation id.
async fn round_trip(stream: &mut TcpStream, request: Vec<u8>, correlation_id: i32) -> Vec<u8> {
    stream.write_all(&request).await.expect("write request");
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("response length");
    let mut response = vec![0u8; u32::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut response)
        .await
        .expect("response frame");
    assert!(response[..4] == correlation_id.to_be_bytes());
    response.split_off(4)
}

fn encode<M: Encode>(message: &M, version: i16) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.to_vec()
}

async fn api_versions(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
) -> ApiVersionsResponse {
    let body = encode(&api_versions_request_for("krabka-test", "1.0"), version);
    let response = round_trip(
        stream,
        frame(18, version, correlation_id, version >= 3, &body),
        correlation_id,
    )
    .await;
    ApiVersionsResponse::decode(&mut &response[..], version).expect("decode ApiVersions")
}

async fn controller_listener_fixture() -> (TempDir, tokio::net::TcpListener, SocketAddr) {
    let dir = TempDir::new().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (dir, listener, addr)
}

async fn start_sasl_controller() -> (krabka_broker::BrokerHandle, SocketAddr, TempDir) {
    let (dir, controller, controller_addr) = controller_listener_fixture().await;
    let data_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    cfg.listeners = vec![crate::support::listeners::listener(
        "SASL_PLAINTEXT",
        data_addr,
        ListenerProtocol::SaslPlaintext,
    )];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), "secret".to_string());
    cfg.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: "broker".to_string(),
        password: "secret".to_string(),
    });
    let broker = Broker::start_with_controller_listener(cfg, Some(controller))
        .await
        .expect("start broker");
    (broker, controller_addr, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sasl_controller_listener_answers_api_versions_before_authentication() {
    let (broker, controller_addr, _dir) = start_sasl_controller().await;

    // Kafka 4.3.1 serves `ApiVersions` v0-v4; v5 is trunk's, so it is
    // refused below like an unknown version, while
    // `unstable.api.versions.enable` is off.
    for version in [0, 3, 4] {
        let mut stream = TcpStream::connect(controller_addr).await.expect("connect");

        // An unsupported version gets Kafka's v0 refusal, and the connection
        // stays open for the next request.
        let refusal = round_trip(&mut stream, frame(18, 5, 1, true, &[0xff]), 1).await;
        check!(
            ApiVersionsResponse::decode(&mut &refusal[..], 0).expect("decode refusal")
                == ApiVersionsResponse {
                    error_code: 35,
                    api_keys: vec![ApiVersion {
                        api_key: 18,
                        min_version: 0,
                        max_version: 4,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            "v{version}"
        );

        let before = api_versions(&mut stream, version, 2).await;

        let handshake = round_trip(
            &mut stream,
            frame(
                17,
                1,
                3,
                false,
                &encode(
                    &SaslHandshakeRequest {
                        mechanism: "PLAIN".into(),
                        ..Default::default()
                    },
                    1,
                ),
            ),
            3,
        )
        .await;
        assert!(handshake[..2] == 0i16.to_be_bytes());
        let authenticate = round_trip(
            &mut stream,
            frame(
                36,
                2,
                4,
                true,
                &encode(
                    &SaslAuthenticateRequest {
                        auth_bytes: bytes::Bytes::from_static(b"\0broker\0secret"),
                        ..Default::default()
                    },
                    2,
                ),
            ),
            4,
        )
        .await;
        // The flexible response header's tagged-fields byte, then error 0.
        assert!(authenticate[..3] == [0, 0, 0]);

        let after = api_versions(&mut stream, version, 5).await;
        // `finalized_features_epoch` is the controller's metadata offset, as
        // Kafka's `KRaftMetadataCache.features` reports it, so a record the
        // controller commits between the two requests may move it on. The
        // rest of the answer is unchanged by authentication.
        check!(
            after.finalized_features_epoch >= before.finalized_features_epoch,
            "v{version}"
        );
        check!(
            ApiVersionsResponse {
                finalized_features_epoch: before.finalized_features_epoch,
                ..after
            } == before,
            "v{version}"
        );
        check!(before.error_code == 0, "v{version}");
        check!(
            before.api_keys.iter().any(|key| key.api_key == 52),
            "v{version}: Vote is advertised"
        );
        if version >= 3 {
            check!(!before.supported_features.is_empty(), "v{version}");
        }
    }

    broker.shutdown().await;
}

/// A broker whose controller listener is PLAINTEXT, on `customize`d settings.
async fn start_plaintext_controller(
    customize: impl FnOnce(&mut BrokerConfig),
) -> (krabka_broker::BrokerHandle, SocketAddr, TempDir) {
    let (dir, controller, controller_addr) = controller_listener_fixture().await;
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    customize(&mut cfg);
    let broker = Broker::start_with_controller_listener(cfg, Some(controller))
        .await
        .expect("start broker");
    (broker, controller_addr, dir)
}

/// Waits for the listener to end `stream`: the next read is end of input or
/// an error, never a byte.
async fn assert_closed(stream: &mut TcpStream, what: &str) {
    let mut byte = [0u8; 1];
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(15), stream.read(&mut byte))
        .await
        .unwrap_or_else(|_| panic!("{what}: the listener held the connection open"));
    check!(
        outcome.as_ref().map_or(true, |read| *read == 0),
        "{what}: {outcome:?}"
    );
}

/// The controller listener is one of `SocketServer`'s listeners, so it holds
/// a peer to `socket.request.max.bytes`: a request within the limit is served,
/// and a size prefix over it closes the connection before the frame is read,
/// where it used to allocate for whatever the four bytes said.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_listener_refuses_a_request_over_socket_request_max() {
    let (broker, controller_addr, _dir) = start_plaintext_controller(|cfg| {
        cfg.socket_request_max = krabka_units::kibibytes(4);
    })
    .await;

    let mut stream = TcpStream::connect(controller_addr).await.expect("connect");
    let served = api_versions(&mut stream, 0, 1).await;
    check!(served.error_code == 0);

    // 1 MiB declared, no body behind it.
    stream
        .write_all(&(1024 * 1024_u32).to_be_bytes())
        .await
        .expect("write the size prefix");
    assert_closed(&mut stream, "an oversize request").await;

    broker.shutdown().await;
}

/// `connections.max.idle.ms` covers the controller listener, under the
/// listener's own name: a connection that sends nothing is closed once the
/// window passes, and one that keeps sending requests is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_listener_closes_an_idle_connection() {
    let window = std::time::Duration::from_millis(800);
    let (broker, controller_addr, _dir) = start_plaintext_controller(|cfg| {
        cfg.connections_max_idle_overrides
            .insert("CONTROLLER".to_string(), krabka_units::millis(800));
    })
    .await;

    let mut idle = TcpStream::connect(controller_addr).await.expect("connect");
    let mut busy = TcpStream::connect(controller_addr).await.expect("connect");
    let started = std::time::Instant::now();
    let mut correlation_id = 1;
    while started.elapsed() < window * 2 {
        let answered = api_versions(&mut busy, 0, correlation_id).await;
        check!(answered.error_code == 0);
        correlation_id += 1;
        tokio::time::sleep(window / 4).await;
    }
    assert_closed(&mut idle, "an idle connection").await;

    broker.shutdown().await;
}
