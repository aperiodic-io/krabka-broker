//! What a `SASL_PLAINTEXT` listener holds a connection to before it finishes
//! authenticating, and what a failed authentication costs it.
//!
//! - `sasl.server.max.receive.size` replaces `socket.request.max.bytes` as the
//!   frame limit for the whole exchange (`SaslServerAuthenticator`'s own
//!   `NetworkReceive`), and the larger limit returns once the peer is
//!   authenticated. An oversize frame fails the authentication.
//! - `connection.failed.authentication.delay.ms` holds a failed
//!   authentication's answer, and the close after it (`Selector`'s
//!   `delayedClosingChannels`).
//! - After one valid `ApiVersions`, `SaslServerAuthenticator` takes only a
//!   `SaslHandshake`: a second `ApiVersions` closes the connection, and an
//!   `UNSUPPORTED_VERSION` or `INVALID_REQUEST` answer does not count as the
//!   first.
//! - `SaslServerAuthenticator` answers that first `ApiVersions` without the
//!   KIP-1242 routing check, which only `KafkaApis` runs.

use {
    crate::{
        harness::{admin_plain_password, round_trip, wrong_scram_password},
        support::{
            client::http_get,
            discovery::{api_versions_request_for, topic_metadata_request},
            sasl::sasl_temp_config,
            topics::metadata_topic,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup, request_frame as support_request_frame,
            },
        },
    },
    assert2::{assert, check},
    bytes::{BufMut, BytesMut},
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request, api_versions_response, metadata_request, metadata_response,
            sasl_authenticate_request, sasl_authenticate_response, sasl_handshake_request,
            sasl_handshake_response,
        },
    },
    krabka_security::SaslMechanism,
    std::{io, net::SocketAddr, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

/// `REBOOTSTRAP_REQUIRED`, KIP-1242.
const REBOOTSTRAP_REQUIRED: i16 = 129;

/// A `SASL_PLAINTEXT` broker serving PLAIN for `admin`.
async fn start_broker(customize: impl FnOnce(&mut BrokerConfig)) -> BrokerHandle {
    let (log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    customize(&mut cfg);
    let handle = Broker::start(cfg).await.expect("broker must start");
    // The broker keeps its log directory for its whole life.
    std::mem::forget(log_dir);
    handle
}

fn encode<M: Encode>(message: &M, version: i16) -> Vec<u8> {
    let mut body = BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.to_vec()
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RequestFrameSetup<'a> {
    #[default(krabka_ids::ApiKey(18))]
    api_key: krabka_ids::ApiKey,
    #[default(krabka_ids::ApiVersion(3))]
    version: krabka_ids::ApiVersion,
    #[default(crate::support::wire::CorrelationId(2))]
    correlation: WireCorrelationId,
    #[default(crate::support::wire::HeaderEncoding::Flexible)]
    header: RequestHeaderEncoding,
    body: &'a [u8],
}

/// A sized request frame with the pre-authentication client's header.
#[bon::builder]
fn request_frame(
    #[builder(into, default = krabka_ids :: ApiKey (18))] api_key: krabka_ids::ApiKey,
    #[builder(into, default = krabka_ids :: ApiVersion (3))] version: krabka_ids::ApiVersion,
    #[builder(default = crate :: support :: wire :: CorrelationId (2))]
    correlation: WireCorrelationId,
    #[builder(default = crate :: support :: wire :: HeaderEncoding :: Flexible)]
    header: RequestHeaderEncoding,
    #[builder(default)] body: &'_ [u8],
) -> Vec<u8> {
    let setup = RequestFrameSetup {
        api_key,
        version,
        correlation,
        header,
        body,
    };
    let frame = support_request_frame()
        .api_key(setup.api_key)
        .version(setup.version)
        .correlation(setup.correlation)
        .header(setup.header)
        .client_id("c")
        .body(setup.body)
        .call();
    let mut out = BytesMut::new();
    out.put_i32(i32::try_from(frame.len()).expect("frame length fits i32"));
    out.put_slice(&frame);
    out.to_vec()
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let length = stream.read_u32().await?;
    let mut frame = vec![0u8; length as usize];
    stream.read_exact(&mut frame).await?;
    Ok(frame)
}

/// Waits for the broker to close `stream` without writing another frame.
async fn assert_closed_without_a_frame(stream: &mut TcpStream, what: &str) {
    let outcome = tokio::time::timeout(Duration::from_secs(10), read_frame(stream))
        .await
        .unwrap_or_else(|_| panic!("{what}: the broker held the connection open"));
    assert!(
        outcome.is_err(),
        "{what}: the broker answered with {outcome:?}"
    );
}

async fn api_versions(
    stream: &mut TcpStream,
    version: i16,
    request: &api_versions_request::ApiVersionsRequest,
) -> api_versions_response::ApiVersionsResponse {
    let body = round_trip(
        stream,
        SocketExchangeSetup {
            version: WireApiVersion(version),
            header: RequestHeaderEncoding::from_wire(version >= 3),
            ..Default::default()
        },
        &encode(request, version),
    )
    .await
    .expect("ApiVersions answer");
    api_versions_response::ApiVersionsResponse::decode(&mut &body[..], version)
        .expect("decode ApiVersions")
}

fn named_client(name: &str) -> api_versions_request::ApiVersionsRequest {
    api_versions_request_for(name, "1.0")
}

/// PLAIN login as `admin` on `stream`, after whichever requests already ran.
async fn plain_login(stream: &mut TcpStream, password: &str) -> Result<(), io::Error> {
    let handshake = encode(
        &sasl_handshake_request::SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        },
        1,
    );
    let answer = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(2),
            ..Default::default()
        },
        &handshake,
    )
    .await?;
    let answer = sasl_handshake_response::SaslHandshakeResponse::decode(&mut &answer[..], 1)
        .expect("decode handshake");
    if answer.error_code != 0 {
        return Err(io::Error::other(format!("handshake {}", answer.error_code)));
    }
    let token = sasl_authenticate_request::SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(format!("\0admin\0{password}").into_bytes()),
        ..Default::default()
    };
    let answer = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: WireCorrelationId(3),
            header: RequestHeaderEncoding::Flexible,
        },
        &encode(&token, 2),
    )
    .await?;
    let answer = sasl_authenticate_response::SaslAuthenticateResponse::decode(&mut &answer[..], 2)
        .expect("decode authenticate");
    if answer.error_code != 0 {
        return Err(io::Error::other(format!(
            "authenticate {}",
            answer.error_code
        )));
    }
    Ok(())
}

async fn scrape_metrics(addr: SocketAddr) -> String { http_get(addr, "/metrics", false).await }

/// A size prefix over `sasl.server.max.receive.size` fails the authentication
/// before the frame is read. The peer gets no answer, since Kafka builds none
/// for it, and the broker counts a failed authentication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_frame_over_the_sasl_receive_limit_fails_the_authentication() {
    let handle = start_broker(|cfg| {
        cfg.sasl_server_max_receive = krabka_units::kibibytes(1);
        cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    })
    .await;
    let metrics_addr = handle.metrics_addr().expect("metrics server bound");

    // A SaslAuthenticate that declares 2 KiB, over the 1 KiB limit. The
    // stream carries the size prefix and 8 body bytes, so a broker that
    // tried to read the frame first would wait on it instead.
    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    let mut frame = 2048_u32.to_be_bytes().to_vec();
    frame.extend_from_slice(&[0; 8]);
    stream.write_all(&frame).await.unwrap();
    assert_closed_without_a_frame(&mut stream, "an oversize pre-auth frame").await;

    let body = scrape_metrics(metrics_addr).await;
    handle.shutdown().await;
    let needle = "krabka_broker_failed_authentication_total{mechanism=\"Unknown\"} 1";
    assert!(body.contains(needle), "missing {needle} in:\n{body}");
}

/// The frame limit is `sasl.server.max.receive.size` for the whole exchange
/// and `socket.request.max.bytes` afterwards: the same 2 KiB request that fails
/// an authentication in flight is served once the peer has authenticated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_larger_request_limit_returns_once_the_peer_authenticated() {
    let handle = start_broker(|cfg| {
        cfg.sasl_server_max_receive = krabka_units::kibibytes(1);
    })
    .await;
    let addr = handle.listen_addr();

    // A 2 KiB Metadata request, as a list of topic names.
    let topics = (0..64)
        .map(|index| {
            metadata_topic(
                Some(format!("a-topic-name-of-thirty-two-bytes-{index:02}")),
                krabka_protocol::primitives::uuid::Uuid::default(),
            )
        })
        .collect::<Vec<_>>();
    let metadata = encode(
        &metadata_request::MetadataRequest {
            allow_auto_topic_creation: false,
            ..topic_metadata_request(Some(topics))
        },
        12,
    );
    assert!(
        metadata.len() > 2048,
        "the request is {} bytes",
        metadata.len()
    );

    // Mid-exchange the frame is over the limit: a handshake has chosen a
    // mechanism, and the next frame is read under the 1 KiB limit.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let handshake = encode(
        &sasl_handshake_request::SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        },
        1,
    );
    round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            ..Default::default()
        },
        &handshake,
    )
    .await
    .expect("handshake");
    stream
        .write_all(&request_frame().api_key(36).body(&vec![0u8; 2048]).call())
        .await
        .unwrap();
    assert_closed_without_a_frame(&mut stream, "an oversize frame mid-exchange").await;

    // Once authenticated, the same size is an ordinary request.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    plain_login(&mut stream, &admin_plain_password())
        .await
        .expect("login");
    let body = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(3),
            version: WireApiVersion(12),
            correlation: WireCorrelationId(4),
            header: RequestHeaderEncoding::Flexible,
        },
        &metadata,
    )
    .await
    .expect("Metadata after authentication");
    let response =
        metadata_response::MetadataResponse::decode(&mut &body[..], 12).expect("decode Metadata");
    check!(response.topics.len() == 64);
    handle.shutdown().await;
}

/// A failed authentication answers no sooner than
/// `connection.failed.authentication.delay.ms`, and a zero delay answers at
/// once. The answer still carries the failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_authentication_holds_its_answer_for_the_configured_delay() {
    let delay = Duration::from_millis(600);
    let handle = start_broker(|cfg| {
        cfg.connection_failed_authentication_delay = krabka_units::millis(600);
    })
    .await;

    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    let started = std::time::Instant::now();
    let outcome = plain_login(&mut stream, &wrong_scram_password()).await;
    let elapsed = started.elapsed();
    check!(outcome.is_err(), "{outcome:?}");
    check!(
        elapsed >= delay,
        "the failure came after {elapsed:?}, before the {delay:?} delay"
    );
    assert_closed_without_a_frame(&mut stream, "after the failed authentication").await;
    handle.shutdown().await;

    let handle = start_broker(|cfg| {
        cfg.connection_failed_authentication_delay = krabka_units::millis(0);
    })
    .await;
    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    let outcome = plain_login(&mut stream, &wrong_scram_password()).await;
    check!(outcome.is_err(), "{outcome:?}");
    handle.shutdown().await;
}

/// After one valid `ApiVersions` only a `SaslHandshake` is taken. A second
/// `ApiVersions` closes the connection with no answer. An `ApiVersions` that
/// was answered `UNSUPPORTED_VERSION` or `INVALID_REQUEST` left the state
/// where it was, so the client may ask again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_api_versions_before_the_handshake_closes_the_connection() {
    let handle = start_broker(|_| {}).await;
    let addr = handle.listen_addr();

    // A refused ApiVersions leaves the state alone, whichever way it is
    // refused, and the retries, a valid one, and a handshake all follow.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let unsupported = round_trip(
        &mut stream,
        SocketExchangeSetup {
            version: WireApiVersion(i16::MAX),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &[0xff],
    )
    .await
    .expect("UNSUPPORTED_VERSION answer");
    check!(
        api_versions_response::ApiVersionsResponse::decode(&mut &unsupported[..], 0)
            .expect("decode")
            .error_code
            == 35
    );
    let invalid = api_versions(&mut stream, 3, &named_client("not valid!")).await;
    check!(invalid.error_code == 42);
    let valid = api_versions(&mut stream, 3, &named_client("krabka-test")).await;
    check!(valid.error_code == 0);
    plain_login(&mut stream, &admin_plain_password())
        .await
        .expect("the handshake follows one valid ApiVersions");

    // A second valid one is what Kafka refuses.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let first = api_versions(&mut stream, 3, &named_client("krabka-test")).await;
    check!(first.error_code == 0);
    stream
        .write_all(
            &request_frame()
                .body(&encode(&named_client("krabka-test"), 3))
                .call(),
        )
        .await
        .unwrap();
    assert_closed_without_a_frame(&mut stream, "a second ApiVersions").await;
    handle.shutdown().await;
}

/// `SaslServerAuthenticator` answers the first `ApiVersions` from its own
/// supplier, whatever cluster or node the client meant to reach. Once the
/// connection is authenticated `KafkaApis` answers, and a stale identity asks
/// the client to rebootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_auth_api_versions_carries_no_rebootstrap_check() {
    let handle = start_broker(|cfg| {
        cfg.features.unstable_api_versions =
            krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    })
    .await;
    let stale = api_versions_request::ApiVersionsRequest {
        cluster_id: Some("some-other-cluster".into()),
        node_id: 41,
        ..api_versions_request_for("krabka-test", "1.0")
    };

    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    let before = api_versions(&mut stream, 5, &stale).await;
    check!(before.error_code == 0, "before authentication: {before:?}");
    check!(!before.api_keys.is_empty());

    // A cluster id without a node id is invalid whichever code answers.
    let mut lone_cluster = stale.clone();
    lone_cluster.node_id = -1;
    let mut fresh = TcpStream::connect(handle.listen_addr()).await.unwrap();
    let invalid = api_versions(&mut fresh, 5, &lone_cluster).await;
    check!(invalid.error_code == 42, "{invalid:?}");

    plain_login(&mut stream, &admin_plain_password())
        .await
        .expect("login after the ApiVersions");
    let after = api_versions(&mut stream, 5, &stale).await;
    check!(
        after.error_code == REBOOTSTRAP_REQUIRED,
        "after authentication: {after:?}"
    );
    handle.shutdown().await;
}
