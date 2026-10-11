//! Raw-socket Kafka request framing and the SASL/PLAIN handshake for broker
//! integration tests that drive the wire by hand.
//!
//! [`round_trip`] writes one length-prefixed request and strips its response
//! header. The setup selects request encoding and either Kafka response
//! header rule or an explicit response encoding. [`sasl_plain_authenticate`] and
//! [`sasl_plain_authenticate_on`] run `ApiVersions`, `SaslHandshake` and
//! `SaslAuthenticate` over the same framing.
//!
//! The client id is always a parameter, because the client-quota suites key
//! their quotas on it.
//!
//! The module sits in `tests/kafka_wire/mod.rs` rather than in `support`, so a
//! suite that frames requests by hand can declare `mod kafka_wire;` without
//! compiling every cluster-boot helper. Cargo does not build a `mod.rs` under
//! `tests/` as its own test binary, and `crate_tests` in `//bazel:defs.bzl`
//! treats it as a helper source of every integration test.

// Each suite declares `mod kafka_wire;` and calls part of it, which is the
// same reason `support` carries this allow.
#![allow(dead_code)]

pub mod quotas;

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, Bytes, BytesMut};
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        elect_leaders_request::ElectLeadersRequest, elect_leaders_response::ElectLeadersResponse,
        produce_request::ProduceRequest, produce_response::ProduceResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
pub use topics::creatable_topic as topic;

use crate::support::{
    admin::{ElectionSetup, election_partition_errors, election_request},
    produce::{ProduceTimeoutMillis, SinglePartitionProduceSetup, batch_request},
    topics::{self, AssignedTopicSetup, ConfiguredTopicSetup, assigned_topic},
    wire::{self as frames, ApiRequestSetup, CorrelationId},
};

/// The `ApiVersions` API key, whose response header is v0 at every version.
const API_VERSIONS_KEY: i16 = 18;

#[derive(Clone, Copy, Debug, Default)]
pub enum ResponseHeaderSelection {
    /// Follow the request, retaining `ApiVersions`' always-legacy response header.
    #[default]
    ByProtocol,
    Exact(frames::HeaderEncoding),
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ExchangeSetup<'a> {
    #[default(krabka_ids::ApiKey(18))]
    pub api_key: krabka_ids::ApiKey,
    pub version: krabka_ids::ApiVersion,
    #[default(frames::CorrelationId(1))]
    pub correlation: frames::CorrelationId,
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    pub header: frames::HeaderEncoding,
    pub response_header: ResponseHeaderSelection,
}

/// Send one framed request and strip the chosen response header.
///
/// # Errors
/// Returns failed reads/writes or malformed response header errors.
pub async fn round_trip<S>(
    stream: &mut S,
    setup: ExchangeSetup<'_>,
    body: &[u8],
) -> io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ExchangeSetup {
        api_key,
        version: api_version,
        correlation: corr_id,
        client_id,
        header,
        response_header,
    } = setup;
    let response_header = match response_header {
        ResponseHeaderSelection::ByProtocol if api_key.0 == API_VERSIONS_KEY => {
            frames::HeaderEncoding::Legacy
        }
        ResponseHeaderSelection::ByProtocol => header,
        ResponseHeaderSelection::Exact(header) => header,
    };
    let frame = frames::request_frame(frames::WireFrameSetup {
        api_key,
        version: api_version,
        correlation: corr_id,
        header,
        client_id,
        body,
        capacity: Some(
            <krabka_units::ByteSize as krabka_units::convert::ByteSizeExt>::from_bytes(
                u64::try_from(16 + client_id.len() + body.len()).expect("frame capacity fits u64"),
            ),
        ),
        length_context: Some("client_id fits in i16"),
    });

    frames::write_frame(stream, &frame, Some("frame fits in u32")).await?;

    let resp = frames::read_frame(stream).await?;

    let mut cur = &resp[..];
    if cur.len() < 4 {
        return Err(io::Error::other("response missing correlation id"));
    }
    let _resp_corr_id = cur.get_i32();
    if response_header == frames::HeaderEncoding::Flexible {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let tagged = cur.get_u8();
        if tagged != 0 {
            return Err(io::Error::other(format!(
                "response header tagged-fields byte is {tagged}, expected 0"
            )));
        }
    }
    Ok(cur.to_vec())
}

#[derive(Clone, krabka_macros::FieldDefaults)]
pub struct PlainAuthenticationSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default("alice")]
    pub user: &'a str,
    #[default(crate::support::sasl::alice_password().into_bytes())]
    pub password: Vec<u8>,
}

impl<'a> PlainAuthenticationSetup<'a> {
    /// The admin fixture's super-user credentials, bound to a suite's client ID.
    pub fn admin(client_id: &'a str) -> Self {
        Self {
            client_id,
            user: "admin",
            password: b"admin-secret".to_vec(),
        }
    }
}

/// Connects to `addr` and authenticates the connection with SASL/PLAIN.
///
/// # Errors
///
/// Returns the error of a failed connect, of a failed round trip, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate(
    addr: SocketAddr,
    setup: &PlainAuthenticationSetup<'_>,
) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    sasl_plain_authenticate_on(&mut stream, setup).await?;
    Ok(stream)
}

#[derive(krabka_macros::FieldDefaults)]
pub struct PlainRequestSetup<'a> {
    pub authentication: PlainAuthenticationSetup<'a>,
    pub api: frames::ApiRequestSetup,
    #[default("request")]
    pub operation: &'a str,
}

/// Send a flexible request on a new PLAIN connection, retaining named codec errors.
///
/// # Errors
/// Returns authentication, framing, or codec errors from the exchange.
pub async fn request_as_plain<Q, R>(
    addr: SocketAddr,
    request: &Q,
    setup: &PlainRequestSetup<'_>,
) -> io::Result<R>
where
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let PlainRequestSetup {
        authentication,
        api,
        operation,
    } = setup;
    let version = api.version.0;
    let mut stream = sasl_plain_authenticate(addr, authentication).await?;
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|e| io::Error::other(format!("{operation} encode: {e}")))?;
    let response = round_trip(
        &mut stream,
        ExchangeSetup {
            api_key: api.api_key,
            version: WireApiVersion(version),
            correlation: frames::CorrelationId(4),
            client_id: authentication.client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await?;
    R::decode(&mut &response[..], version)
        .map_err(|e| io::Error::other(format!("{operation} decode: {e}")))
}

/// Drive a Produce v11 request over a fresh PLAIN-authenticated connection.
pub async fn drive_produce_as_plain(
    addr: SocketAddr,
    req: ProduceRequest,
    authentication: &PlainAuthenticationSetup<'_>,
) -> Result<ProduceResponse, io::Error> {
    request_as_plain(
        addr,
        &req,
        &PlainRequestSetup {
            authentication: authentication.clone(),
            api: ApiRequestSetup {
                api_key: krabka_ids::ApiKey(0),
                version: krabka_ids::ApiVersion(11),
            },
            operation: "Produce",
        },
    )
    .await
}

/// Create one topic as the admin fixture, checking the entire response's cardinality.
///
/// # Panics
/// Panics if authentication or creation fails, or the response has no unique topic row.
pub async fn create_topic_as_super_user(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    let name = setup.topic.name.clone();
    let request = topics::create_topic_request(topic(setup.topic));
    let response: krabka_protocol::owned::create_topics_response::CreateTopicsResponse =
        request_as_plain(
            addr,
            &request,
            &PlainRequestSetup {
                authentication: PlainAuthenticationSetup::admin(setup.client_id),
                api: ApiRequestSetup {
                    api_key: krabka_ids::ApiKey(19),
                    version: krabka_ids::ApiVersion(7),
                },
                operation: "CreateTopics",
            },
        )
        .await
        .expect("CreateTopics as super-user must round-trip");
    assert!(response.topics.len() == 1, "one topic in response");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        response.topics[0].error_message
    );
}

/// Authenticates an open connection with SASL/PLAIN: `ApiVersions` v0, then
/// `SaslHandshake` v1 for `PLAIN`, then `SaslAuthenticate` v2 carrying
/// `\0user\0password`, on correlation ids 1, 2 and 3.
///
/// # Errors
///
/// Returns the error of a failed round trip, encode or decode, or of a
/// non-zero `SaslHandshake` or `SaslAuthenticate` error code.
pub async fn sasl_plain_authenticate_on<S>(
    stream: &mut S,
    setup: &PlainAuthenticationSetup<'_>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let PlainAuthenticationSetup {
        client_id,
        user,
        password,
    } = setup;
    let av_body = encode_named(&ApiVersionsRequest::default(), 0, "ApiVersions")?;
    let av_resp = round_trip(
        stream,
        ExchangeSetup {
            api_key: WireApiKey(API_VERSIONS_KEY),
            client_id,
            ..Default::default()
        },
        &av_body,
    )
    .await?;
    decode_named::<ApiVersionsResponse>(&av_resp, 0, "ApiVersions")?;

    let sh_body = encode_named(
        &SaslHandshakeRequest {
            mechanism: "PLAIN".to_string(),
            ..Default::default()
        },
        1,
        "SaslHandshake",
    )?;
    let sh_resp = round_trip(
        stream,
        ExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: frames::CorrelationId(2),
            client_id,
            ..Default::default()
        },
        &sh_body,
    )
    .await?;
    let sh_resp = decode_named::<SaslHandshakeResponse>(&sh_resp, 1, "SaslHandshake")?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let payload = plain_payload(user, password);
    let auth_body = encode_named(
        &SaslAuthenticateRequest {
            auth_bytes: payload,
            ..Default::default()
        },
        2,
        "SaslAuthenticate",
    )?;
    let auth_resp = round_trip(
        stream,
        ExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: frames::CorrelationId(3),
            client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
        &auth_body,
    )
    .await?;
    let auth_resp = decode_named::<SaslAuthenticateResponse>(&auth_resp, 2, "SaslAuthenticate")?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }
    Ok(())
}

/// Encode a typed body with the caller's original operation-labelled error.
///
/// # Errors
/// Returns the codec failure with its operation and encoding phase.
pub fn encode_named<Q: Encode>(request: &Q, version: i16, operation: &str) -> io::Result<Bytes> {
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .map_err(|error| io::Error::other(format!("{operation} encode: {error}")))?;
    Ok(body.freeze())
}

/// Decode a typed body with its original operation-labelled error.
///
/// # Errors
/// Returns the codec failure with its operation and decoding phase.
pub fn decode_named<R: for<'de> Decode<'de>>(
    mut body: &[u8],
    version: i16,
    operation: &str,
) -> io::Result<R> {
    R::decode(&mut body, version)
        .map_err(|error| io::Error::other(format!("{operation} decode: {error}")))
}

impl<'a> ExchangeSetup<'a> {
    pub fn for_socket(setup: frames::SocketExchangeSetup, client_id: &'a str) -> Self {
        Self {
            api_key: setup.api_key,
            version: setup.version,
            correlation: setup.correlation,
            client_id,
            header: setup.header,
            ..Default::default()
        }
    }
}

/// Legacy request and response headers with the suite's exact client identity.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct LegacyExchangeSetup<'a> {
    pub api_key: krabka_ids::ApiKey,
    pub version: krabka_ids::ApiVersion,
    #[default(frames::CorrelationId(1))]
    pub correlation: frames::CorrelationId,
    #[default("legacy-fetch-test")]
    pub client_id: &'a str,
}

/// Exchange a legacy frame.
///
/// # Panics
/// Panics if socket I/O or response header decoding fails.
pub async fn legacy_round_trip(
    stream: &mut TcpStream,
    setup: LegacyExchangeSetup<'_>,
    body: &[u8],
) -> Vec<u8> {
    round_trip(
        stream,
        ExchangeSetup {
            api_key: setup.api_key,
            version: setup.version,
            correlation: setup.correlation,
            client_id: setup.client_id,
            ..Default::default()
        },
        body,
    )
    .await
    .expect("non-flexible round trip")
}

/// Encode, exchange and decode a typed request using the caller's exact wire version.
pub async fn exchange<S, Q, R>(
    stream: &mut S,
    request: &Q,
    setup: ExchangeSetup<'_>,
) -> io::Result<R>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let mut body = BytesMut::new();
    request
        .encode(&mut body, setup.version.0)
        .map_err(io::Error::other)?;
    let bytes = round_trip(stream, setup, &body).await?;
    R::decode(&mut &bytes[..], setup.version.0).map_err(io::Error::other)
}

/// Send one typed raw request on a fresh connection, retaining its encoded body length.
///
/// # Panics
/// Panics on encoding, connection, framing or response-decoding failure, as the raw fixtures do.
pub async fn request_once<Q, R>(
    addr: SocketAddr,
    request: &Q,
    api: (i16, i16),
    client_id: &str,
    header: (i32, bool),
) -> (usize, R)
where
    Q: Encode,
    R: for<'de> Decode<'de>,
{
    let (api_key, version) = api;
    let (correlation_id, flexible) = header;
    let mut body = BytesMut::new();
    request.encode(&mut body, version).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let response = round_trip(
        &mut stream,
        ExchangeSetup {
            api_key: WireApiKey(api_key),
            version: WireApiVersion(version),
            correlation: frames::CorrelationId(correlation_id),
            client_id,
            header: frames::HeaderEncoding::from_wire(flexible),
            ..Default::default()
        },
        &body,
    )
    .await
    .unwrap();
    let response = R::decode(&mut &response[..], version).unwrap();
    (body.len(), response)
}

/// PLAIN's empty authorization id followed by the user and password.
pub use crate::support::sasl::plain_payload;

/// Create exactly one topic on an open connection, checking the whole response row's status.
pub async fn create_topic_on(
    stream: &mut TcpStream,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let name = topic.name.clone();
    let request = topics::create_topic_request(topic);
    let response: krabka_protocol::owned::create_topics_response::CreateTopicsResponse = exchange(
        stream,
        &request,
        ExchangeSetup {
            api_key: WireApiKey(19),
            version: WireApiVersion(7),
            client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
    )
    .await
    .expect("CreateTopics round-trip");
    assert2::assert!(response.topics.len() == 1);
    assert2::assert!(
        response.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        response.topics[0].error_message
    );
}

/// Create a topic on a newly authenticated PLAIN session.
pub async fn create_topic_sasl(
    addr: SocketAddr,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
    setup: &PlainAuthenticationSetup<'_>,
) {
    let mut stream = sasl_plain_authenticate(addr, setup)
        .await
        .expect("SASL authenticate for CreateTopics");
    create_topic_on(&mut stream, setup.client_id, topic).await;
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ScramAuthenticationSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default("alice")]
    pub user: &'a str,
    #[default(crate::support::sasl::alice_password())]
    pub password: String,
    #[default(krabka_security::SaslMechanism::ScramSha512)]
    pub mechanism: krabka_security::SaslMechanism,
}

pub async fn sasl_scram_authenticate_on(
    stream: &mut TcpStream,
    setup: &ScramAuthenticationSetup<'_>,
) -> io::Result<()> {
    let (client_id, user, password, mechanism) = (
        setup.client_id,
        setup.user,
        setup.password.as_str(),
        setup.mechanism,
    );
    // ── 1. ApiVersions (v0, non-flexible). Same as PLAIN: pre-auth allowlist.
    let _av_resp: ApiVersionsResponse = exchange(
        stream,
        &ApiVersionsRequest::default(),
        ExchangeSetup {
            client_id,
            ..Default::default()
        },
    )
    .await?;

    // ── 2. SaslHandshake v1 (non-flexible).
    sasl_handshake_on(
        stream,
        SaslHandshakeSetup {
            client_id,
            mechanism: mechanism.wire_name(),
            ..Default::default()
        },
    )
    .await?;

    // ── 3. SCRAM client-first → server-first.
    let (client_first, client) = krabka_macros::scram_client_first!(user, password, mechanism)?;
    let scram_first_response = sasl_authenticate_on(
        stream,
        Bytes::from(client_first),
        SaslAuthenticateSetup {
            client_id,
            ..Default::default()
        },
    )
    .await?;
    if scram_first_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 1 failed: error_code={} error_message={:?}",
            scram_first_response.error_code, scram_first_response.error_message
        )));
    }
    let server_first = scram_first_response.auth_bytes.to_vec();

    // ── 4. SCRAM client-final → server-final.
    let (client_final, client) = client
        .step(&server_first)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    let scram_final_response = sasl_authenticate_on(
        stream,
        Bytes::from(client_final),
        SaslAuthenticateSetup {
            client_id,
            correlation: CorrelationId(4),
        },
    )
    .await?;
    if scram_final_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 2 failed: error_code={} error_message={:?}",
            scram_final_response.error_code, scram_final_response.error_message
        )));
    }
    // Client verifies server signature — proves the broker holds the
    // expected `server_key` rather than just any matching `stored_key`.
    client
        .verify_server_final(&scram_final_response.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    Ok(())
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SaslHandshakeSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default(frames::CorrelationId(2))]
    pub correlation: frames::CorrelationId,
    #[default("PLAIN")]
    pub mechanism: &'a str,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SaslAuthenticateSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default(frames::CorrelationId(3))]
    pub correlation: frames::CorrelationId,
}

/// A successful v1 handshake on an existing SASL session.
pub async fn sasl_handshake_on<S>(stream: &mut S, setup: SaslHandshakeSetup<'_>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let response: SaslHandshakeResponse = exchange(
        stream,
        &SaslHandshakeRequest {
            mechanism: setup.mechanism.to_string(),
            ..Default::default()
        },
        ExchangeSetup {
            api_key: WireApiKey(17),
            version: WireApiVersion(1),
            correlation: setup.correlation,
            client_id: setup.client_id,
            ..Default::default()
        },
    )
    .await?;
    if response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            response.error_code
        )));
    }
    Ok(())
}

/// One SASL Authenticate v2 message; callers decide whether an error is expected.
pub async fn sasl_authenticate_on<S>(
    stream: &mut S,
    auth_bytes: Bytes,
    setup: SaslAuthenticateSetup<'_>,
) -> io::Result<SaslAuthenticateResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    exchange(
        stream,
        &SaslAuthenticateRequest {
            auth_bytes,
            ..Default::default()
        },
        ExchangeSetup {
            api_key: WireApiKey(36),
            version: WireApiVersion(2),
            correlation: setup.correlation,
            client_id: setup.client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
    )
    .await
}

pub async fn sasl_plain_reauthenticate_on(
    stream: &mut TcpStream,
    corr: &mut frames::CorrelationId,
    setup: &PlainAuthenticationSetup<'_>,
) -> Result<SaslAuthenticateResponse, io::Error> {
    corr.0 += 1;
    sasl_handshake_on(
        stream,
        SaslHandshakeSetup {
            client_id: setup.client_id,
            correlation: *corr,
            ..Default::default()
        },
    )
    .await?;

    let payload = plain_payload(setup.user, &setup.password);
    corr.0 += 1;
    sasl_authenticate_on(
        stream,
        payload,
        SaslAuthenticateSetup {
            client_id: setup.client_id,
            correlation: *corr,
        },
    )
    .await
}

#[derive(Clone, Copy)]
pub struct ProduceValueBytes(pub usize);
impl Default for ProduceValueBytes {
    fn default() -> Self {
        Self(1024)
    }
}

#[derive(Clone, Copy)]
pub struct ProduceRecordCount(pub usize);
impl Default for ProduceRecordCount {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Clone, krabka_macros::FieldDefaults)]
pub struct ZeroFilledRecordsSetup {
    #[default("quota-topic".to_string())]
    pub topic: String,
    pub value_bytes: ProduceValueBytes,
    pub count: ProduceRecordCount,
}

#[derive(krabka_macros::FieldDefaults)]
pub struct SaslProduceSetup<'a> {
    pub authentication: PlainAuthenticationSetup<'a>,
    #[default("krabka-broker-test")]
    pub produce_client_id: &'a str,
    pub records: ZeroFilledRecordsSetup,
}

/// A Produce v11-compatible batch of zero-filled records for quota tests.
pub fn produce_records(
    setup: ZeroFilledRecordsSetup,
) -> krabka_protocol::owned::produce_request::ProduceRequest {
    let value = vec![0u8; setup.value_bytes.0];
    let records: Vec<Record> = (0..setup.count.0)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    batch_request(
        RecordBatch {
            last_offset_delta: i32::try_from(setup.count.0 - 1).unwrap(),
            records,
            ..Default::default()
        },
        SinglePartitionProduceSetup {
            topic: setup.topic,
            timeout: ProduceTimeoutMillis(30_000),
            ..Default::default()
        },
    )
}

/// Build an all-ISR Produce request with one record.
pub fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    batch_request(
        RecordBatch {
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::copy_from_slice(value)),
                ..Default::default()
            }],
            ..Default::default()
        },
        SinglePartitionProduceSetup {
            topic: topic.to_string(),
            partition: krabka_ids::PartitionIndex(partition),
            ..SinglePartitionProduceSetup::replicated()
        },
    )
}

/// Send the fixture election request and retain its complete response.
///
/// # Panics
/// Panics if encoding, socket I/O or response decoding fails.
pub async fn elect_leaders_response(
    stream: &mut TcpStream,
    request: &ElectLeadersRequest,
    client_id: &str,
) -> ElectLeadersResponse {
    exchange(
        stream,
        request,
        ExchangeSetup {
            api_key: WireApiKey(43),
            version: WireApiVersion(2),
            client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
    )
    .await
    .expect("ElectLeaders round-trip")
}

#[derive(krabka_macros::FieldDefaults)]
pub struct ElectionExchangeSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    pub election: ElectionSetup,
}

pub async fn elect_leaders(
    stream: &mut TcpStream,
    setup: ElectionExchangeSetup<'_>,
) -> Vec<(i32, i16)> {
    let topic = setup.election.topic.clone();
    let req = election_request(setup.election);
    let resp = elect_leaders_response(stream, &req, setup.client_id).await;

    assert!(
        resp.error_code == 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    election_partition_errors(resp, &topic)
}

pub async fn create_topic_plaintext(
    addr: SocketAddr,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    create_topic_on(&mut stream, client_id, topic).await;
}

/// Automatic placement and client identity, with a one-partition orders topic by default.
#[derive(krabka_macros::FieldDefaults)]
pub struct AutomaticTopicSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    pub topic: ConfiguredTopicSetup,
}

/// SASL/PLAIN topic creation defaults to the admin fixture's credentials.
#[derive(krabka_macros::FieldDefaults)]
pub struct SaslTopicSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    #[default("admin")]
    pub user: &'a str,
    #[default(b"admin-secret")]
    pub password: &'a [u8],
    pub topic: ConfiguredTopicSetup,
}

pub async fn create_configured_topic_sasl(addr: SocketAddr, setup: SaslTopicSetup<'_>) {
    create_topic_sasl(
        addr,
        topic(setup.topic),
        &PlainAuthenticationSetup {
            client_id: setup.client_id,
            user: setup.user,
            password: (setup.password).to_vec(),
        },
    )
    .await;
}

/// Create an automatically placed topic as the native admin fixture principal.
pub async fn create_topic_as_admin(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_configured_topic_sasl(
        addr,
        SaslTopicSetup {
            client_id: setup.client_id,
            topic: setup.topic,
            ..Default::default()
        },
    )
    .await;
}

/// Create a topic on a fresh plaintext connection with the original connect diagnostic.
///
/// # Panics
/// Panics if the connection fails or the topic creation is rejected.
pub async fn create_topic_on_fresh_connection(
    addr: SocketAddr,
    client_id: &str,
    topic: krabka_protocol::owned::create_topics_request::CreatableTopic,
) {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    create_topic_on(&mut stream, client_id, topic).await;
}

/// Create an automatically placed topic over plaintext.
pub async fn create_automatic_topic_plaintext(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_topic_on_fresh_connection(addr, setup.client_id, topic(setup.topic)).await;
}

#[derive(krabka_macros::FieldDefaults)]
pub struct AssignedTopicExchangeSetup<'a> {
    #[default("krabka-broker-test")]
    pub client_id: &'a str,
    pub topic: AssignedTopicSetup,
}

/// Create one partition with an explicit ordered assignment over plaintext.
pub async fn create_assigned_topic_plaintext(
    addr: SocketAddr,
    setup: AssignedTopicExchangeSetup<'_>,
) {
    create_topic_on_fresh_connection(addr, setup.client_id, assigned_topic(setup.topic)).await;
}

/// Elect named partitions over a new plaintext connection and retain the top-level oracle.
///
/// # Panics
/// Panics if the connection, election round-trip or top-level election check fails.
pub async fn elect_leaders_plaintext(
    addr: SocketAddr,
    setup: ElectionExchangeSetup<'_>,
) -> Vec<(i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    elect_leaders(&mut stream, setup).await
}

/// Authenticate with one client id and send Produce v11 with the requested wire id.
///
/// # Panics
/// Panics if record construction, SASL authentication, encoding or the wire exchange fails.
pub async fn produce_sasl_with_ids(
    addr: SocketAddr,
    setup: SaslProduceSetup<'_>,
) -> krabka_protocol::owned::produce_response::ProduceResponse {
    const VERSION: i16 = 11;
    let req = produce_records(setup.records);
    let mut stream = sasl_plain_authenticate(addr, &setup.authentication)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip(
        &mut stream,
        ExchangeSetup {
            api_key: WireApiKey(0),
            version: WireApiVersion(VERSION),
            client_id: setup.produce_client_id,
            header: frames::HeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    krabka_protocol::owned::produce_response::ProduceResponse::decode(&mut cur, VERSION)
        .expect("decode ProduceResponse")
}

/// Automatic plaintext creation with the original JBOD fixtures' connect unwrap policy.
pub async fn create_configured_topic_plaintext(addr: SocketAddr, setup: AutomaticTopicSetup<'_>) {
    create_topic_plaintext(addr, setup.client_id, topic(setup.topic)).await;
}
