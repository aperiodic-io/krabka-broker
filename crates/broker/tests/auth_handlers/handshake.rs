//! The SASL gate on a `SASL_PLAINTEXT` listener.
//!
//! `ApiVersions` is on the pre-auth allowlist and answers before any SASL
//! exchange, `Metadata` is not and the broker closes the connection instead,
//! a `SaslHandshake` for a mechanism the listener does not enable comes back
//! with the enabled list and then closes, a v0 handshake runs the exchange as
//! raw tokens, and anything but `SaslAuthenticate` after a handshake closes.

use {
    crate::{
        harness::{BrokerSocket, alice_password, alice_plain_socket, round_trip},
        kafka_wire::exchange,
        support::{
            sasl::sasl_plaintext_mechanisms,
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup, request_frame, write_frame,
            },
        },
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
            metadata_request::MetadataRequest, sasl_handshake_request::SaslHandshakeRequest,
            sasl_handshake_response::SaslHandshakeResponse,
        },
    },
    krabka_security::SaslMechanism,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

/// `ApiVersions`, `api_key` 18, is on the pre-auth allowlist and must succeed
/// without any SASL exchange.
///
/// The response should decode without an error. The supported-api list should
/// include `api_keys` 17, which is `SaslHandshake`, and 36, which is
/// `SaslAuthenticate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_reachable_pre_auth_on_sasl_listener() {
    let BrokerSocket {
        log_dir: _log_dir,
        handle,
        addr: _addr,
        mut stream,
    } = alice_plain_socket("alice-secret".to_string()).await;

    let av_resp: ApiVersionsResponse = exchange()
        .stream(&mut stream)
        .request(&ApiVersionsRequest::default())
        .client_id("krabka-sasl-test")
        .call()
        .await
        .expect("ApiVersions round-trip");

    check!(
        av_resp.error_code == 0,
        "ApiVersions error_code must be 0 on SASL listener pre-auth"
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 17),
        "ApiVersionsResponse must list SaslHandshake (17): {:?}",
        av_resp.api_keys
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 36),
        "ApiVersionsResponse must list SaslAuthenticate (36): {:?}",
        av_resp.api_keys
    );

    handle.shutdown().await;
}

/// A pre-auth `Metadata` request on a `SASL_PLAINTEXT` listener must not
/// succeed.
///
/// `Metadata`, `api_key` 3, is not on the pre-auth allowlist. T12 closes the
/// TCP connection and does not encode a typed error response. So the read
/// after the `Metadata` request must return an I/O error, either
/// `UnexpectedEof` or a connection reset, and not a well-formed response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_rejected_pre_auth_on_sasl_listener() {
    let BrokerSocket {
        log_dir: _log_dir,
        handle,
        addr: _addr,
        mut stream,
    } = alice_plain_socket("alice-secret".to_string()).await;

    // Send Metadata (api_key=3, v12, flexible) WITHOUT any auth.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();

    // Build the frame manually: header + body, then length-prefix.
    let frame = request_frame()
        .api_key(3)
        .version(12)
        .header(RequestHeaderEncoding::Flexible)
        .client_id("krabka-t19-test")
        .body(&md_body)
        .maybe_capacity(Some(
            <krabka_units::ByteSize as krabka_units::convert::ByteSizeExt>::from_bytes(
                u64::try_from(32 + md_body.len()).expect("frame capacity fits u64"),
            ),
        ))
        .call();

    write_frame(&mut stream, &frame, None).await.unwrap();

    // The broker closes the connection instead of responding — any read
    // attempt must return an error (UnexpectedEof / connection reset).
    let read_result = stream.read_u32().await;
    assert!(
        read_result.is_err(),
        "expected TCP close after pre-auth Metadata, but read succeeded: {read_result:?}"
    );

    handle.shutdown().await;
}

/// A `SaslHandshake` with an unsupported mechanism, GSSAPI, must return
/// `error_code = 33`, which is `UNSUPPORTED_SASL_MECHANISM`, with the enabled
/// list, and then close the connection, as Kafka's `SaslServerAuthenticator`
/// does. A new connection may then negotiate the supported mechanism, PLAIN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_mechanism_answers_33_then_closes() {
    let BrokerSocket {
        log_dir: _log_dir,
        handle,
        addr,
        mut stream,
    } = alice_plain_socket("alice-secret".to_string()).await;

    // ── 1. SaslHandshake with "GSSAPI" (not in enabled list).
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .unwrap();
    let sh_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            ..Default::default()
        },
        &sh_body,
    )
    .await
    .expect("SaslHandshake(GSSAPI) must get a response (not a TCP close)");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp =
        SaslHandshakeResponse::decode(&mut cur, 1).expect("SaslHandshakeResponse must decode");
    assert!(
        sh_resp.error_code == 33, // UNSUPPORTED_SASL_MECHANISM
        "GSSAPI handshake must return error_code=33, got {:?}",
        sh_resp.error_code
    );
    assert!(
        sh_resp.mechanisms.iter().any(|m| m == "PLAIN"),
        "error response must include the enabled mechanisms list: {:?}",
        sh_resp.mechanisms
    );

    // ── 2. Kafka's `handleHandshakeRequest` throws after that response, so
    // the connection is closed: a retry on it gets no answer.
    let mut plain_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut plain_body, 1)
    .unwrap();
    assert!(
        round_trip(
            &mut stream,
            SocketExchangeSetup {
                api_key: WireApiKey(17),
                version: WireApiVersion(1),
                correlation: WireCorrelationId(2),
                ..Default::default()
            },
            &plain_body
        )
        .await
        .is_err(),
        "the connection must be closed after UNSUPPORTED_SASL_MECHANISM"
    );

    // ── 3. A new connection may negotiate PLAIN.
    let mut retry = TcpStream::connect(addr).await.unwrap();
    let plain_resp_bytes = round_trip(
        &mut retry,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: WireCorrelationId(3),
            ..Default::default()
        },
        &plain_body,
    )
    .await
    .expect("SaslHandshake(PLAIN) on a new connection");
    let mut plain_cur: &[u8] = &plain_resp_bytes;
    let plain_resp = SaslHandshakeResponse::decode(&mut plain_cur, 1)
        .expect("SaslHandshakeResponse must decode");
    assert!(plain_resp.error_code == 0);

    handle.shutdown().await;
}

async fn start_plain_sasl_broker(log_dir: &std::path::Path) -> krabka_broker::BrokerHandle {
    let mut cfg = sasl_plaintext_mechanisms(log_dir.to_path_buf(), vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());
    krabka_broker::Broker::start(cfg)
        .await
        .expect("broker must start")
}

async fn plain_handshake(stream: &mut TcpStream, version: i16) -> SaslHandshakeResponse {
    let mut body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut body, version)
    .unwrap();
    let bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(version),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("SaslHandshake round-trip");
    let mut cur: &[u8] = &bytes;
    SaslHandshakeResponse::decode(&mut cur, version).expect("SaslHandshakeResponse must decode")
}

async fn write_raw_frame(stream: &mut TcpStream, payload: &[u8]) {
    stream
        .write_u32(u32::try_from(payload.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(payload).await.unwrap();
    stream.flush().await.unwrap();
}

/// `SaslHandshake` v0: Kafka's `enableKafkaSaslAuthenticateHeaders` stays
/// off, so the client sends its PLAIN token as a raw size-prefixed frame with
/// no Kafka request header, and the broker answers with a raw (here empty)
/// size-prefixed token. The connection is then authenticated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_handshake_runs_the_exchange_as_raw_tokens() {
    let PlainHandshake {
        log_dir: _log_dir,
        handle,
        mut stream,
    } = plain_handshake_fixture(0).await;

    let mut token = vec![0_u8];
    token.extend_from_slice(b"alice");
    token.push(0);
    token.extend_from_slice(alice_password().as_bytes());
    write_raw_frame(&mut stream, &token).await;
    let len = stream.read_u32().await.expect("raw server token");
    check!(len == 0, "PLAIN's server token is empty");

    // Authenticated: a data-plane request is served.
    let mut md_body = BytesMut::new();
    MetadataRequest::default().encode(&mut md_body, 12).unwrap();
    check!(
        round_trip(
            &mut stream,
            SocketExchangeSetup {
                api_key: WireApiKey(3),
                version: WireApiVersion(12),
                correlation: WireCorrelationId(2),
                header: RequestHeaderEncoding::Flexible
            },
            &md_body
        )
        .await
        .is_ok()
    );

    handle.shutdown().await;
}

/// A v0 raw token with a bad credential has no response shape to carry an
/// error, so Kafka closes the connection without one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_raw_token_with_bad_credentials_closes_without_response() {
    let PlainHandshake {
        log_dir: _log_dir,
        handle,
        mut stream,
    } = plain_handshake_fixture(0).await;
    write_raw_frame(&mut stream, b"\0alice\0not-the-password").await;
    check!(stream.read_u32().await.is_err());

    handle.shutdown().await;
}

/// After a handshake chose a mechanism, Kafka accepts only
/// `SaslAuthenticate`: an `ApiVersions` gets its own error response with
/// `ILLEGAL_SASL_STATE` (34), and then the connection closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_after_the_handshake_answers_34_then_closes() {
    let PlainHandshake {
        log_dir: _log_dir,
        handle,
        mut stream,
    } = plain_handshake_fixture(1).await;

    let mut av_body = BytesMut::new();
    ApiVersionsRequest::default()
        .encode(&mut av_body, 3)
        .unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            version: WireApiVersion(3),
            correlation: WireCorrelationId(2),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &av_body,
    )
    .await
    .expect("ApiVersions gets an error response");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ApiVersionsResponse::decode(&mut cur, 3).expect("ApiVersionsResponse must decode");
    assert!(
        resp == ApiVersionsResponse {
            error_code: 34,
            ..Default::default()
        }
    );
    check!(
        stream.read_u32().await.is_err(),
        "then the connection closes"
    );

    handle.shutdown().await;
}

/// A PLAIN SASL broker, the log directory that backs it, and an open socket.
struct PlainHandshake {
    log_dir: tempfile::TempDir,
    handle: krabka_broker::BrokerHandle,
    stream: TcpStream,
}

async fn plain_handshake_fixture(version: i16) -> PlainHandshake {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_plain_sasl_broker(log_dir.path()).await;
    let mut stream = TcpStream::connect(handle.listen_addr()).await.unwrap();
    check!(plain_handshake(&mut stream, version).await.error_code == 0);
    PlainHandshake {
        log_dir,
        handle,
        stream,
    }
}
