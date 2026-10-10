//! Fixtures shared by the controller-listener handshake unit tests: frame
//! builders for a Kafka request, readers for the response frame, a
//! `BrokerRaftHandshake` configured for in-process PLAIN, and the encoded
//! request bodies that the SASL rounds send.

use std::{collections::HashMap, sync::Arc};

use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_raft::RaftHandshakeError;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::OnceCell,
    time::{Duration, timeout},
};

use super::{BrokerRaftHandshake, frame::read_kafka_request};

#[derive(Clone, Copy, Default)]
pub(super) struct HandshakeApiKey(pub i16);
#[derive(Clone, Copy, Default)]
pub(super) struct HandshakeApiVersion(pub i16);
#[derive(Clone, Copy, Default)]
pub(super) struct HandshakeCorrelationId(pub i32);

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum HandshakeHeader {
    #[default]
    Legacy,
    Flexible,
}

impl HandshakeHeader {
    pub(super) fn is_flexible(self) -> bool {
        self == Self::Flexible
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct RequestFrameSetup<'a> {
    #[default(HandshakeApiKey(super::API_KEY_SASL_HANDSHAKE))]
    pub api_key: HandshakeApiKey,
    #[default(HandshakeApiVersion(1))]
    pub api_version: HandshakeApiVersion,
    #[default(HandshakeCorrelationId(1))]
    pub correlation: HandshakeCorrelationId,
    #[default(Some(b"c"))]
    pub client_id: Option<&'a [u8]>,
    pub header: HandshakeHeader,
    #[default(b"")]
    pub body: &'a [u8],
}

pub(super) fn request_frame(setup: RequestFrameSetup<'_>) -> Vec<u8> {
    let RequestFrameSetup {
        api_key,
        api_version,
        correlation,
        client_id,
        header,
        body,
    } = setup;
    let mut frame = Vec::new();
    frame.extend_from_slice(&api_key.0.to_be_bytes());
    frame.extend_from_slice(&api_version.0.to_be_bytes());
    frame.extend_from_slice(&correlation.0.to_be_bytes());
    match client_id {
        Some(id) => {
            let len = i16::try_from(id.len()).expect("client id fits i16");
            frame.extend_from_slice(&len.to_be_bytes());
            frame.extend_from_slice(id);
        }
        None => frame.extend_from_slice(&(-1i16).to_be_bytes()),
    }
    if header == HandshakeHeader::Flexible {
        frame.push(0);
    }
    frame.extend_from_slice(body);

    let mut out = Vec::new();
    let len = u32::try_from(frame.len()).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&frame);
    out
}

pub(super) async fn read_request_from_frame(
    frame: Vec<u8>,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let (mut client, mut server) = tokio::io::duplex(4096);
    client.write_all(&frame).await.expect("write request frame");
    read_kafka_request(&mut server, 4096).await
}

pub(super) async fn read_response_frame<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> Vec<u8> {
    timeout(Duration::from_secs(1), async {
        let mut size_buf = [0u8; 4];
        stream
            .read_exact(&mut size_buf)
            .await
            .expect("response size");
        let size = u32::from_be_bytes(size_buf) as usize;
        let mut frame = vec![0u8; size];
        stream.read_exact(&mut frame).await.expect("response frame");
        frame
    })
    .await
    .expect("timely response")
}

pub(super) fn sasl_test_config() -> BrokerRaftHandshake {
    handshake_config(HandshakeSetup::default())
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct HandshakeSetup {
    #[default(ListenerProtocol::SaslPlaintext)]
    pub protocol: ListenerProtocol,
    #[default(vec![SaslMechanism::Plain])]
    pub mechanisms: Vec<SaslMechanism>,
    #[default(HashMap::from([("broker".to_owned(), "secret".to_owned())]))]
    pub credentials: HashMap<String, String>,
}

pub(super) fn handshake_config(setup: HandshakeSetup) -> BrokerRaftHandshake {
    BrokerRaftHandshake {
        tls_acceptor: None,
        plain_credentials: setup.credentials,
        enabled_sasl_mechanisms: setup.mechanisms,
        gssapi: None,
        oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
        oauthbearer_jwks_cache_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        oauthbearer_jwks_last_successful_fetch_ms: Arc::new(std::sync::atomic::AtomicI64::new(0)),
        protocol: setup.protocol,
        controller: Arc::new(OnceCell::new()),
        delegation_token_secret_key: None,
        audit_log: Arc::new(OnceCell::new()),
        sasl_max_receive_bytes: 4096,
        failed_authentication_delay: std::time::Duration::ZERO,
        authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
        principal_mapper: crate::SslPrincipalMapper::default(),
    }
}

pub(super) fn sasl_handshake_body() -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut body, 1)
    .expect("encode sasl handshake");
    body.to_vec()
}

pub(super) fn api_versions_body(version: HandshakeApiVersion) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    ApiVersionsRequest {
        client_software_name: "raft-peer".to_string(),
        client_software_version: "1.0".to_string(),
        ..Default::default()
    }
    .encode(&mut body, version.0)
    .expect("encode api versions");
    body.to_vec()
}

pub(super) fn sasl_authenticate_body(user: &str, password: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password.as_bytes());

    let mut body = bytes::BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .expect("encode sasl authenticate");
    body.to_vec()
}
