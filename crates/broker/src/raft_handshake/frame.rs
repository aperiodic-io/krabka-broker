//! Kafka request and response framing for the controller-listener handshake,
//! the server-side inverse of `network::client::round_trip`.
//!
//! The module reads one length-prefixed request frame at a time, strips the
//! `RequestHeader`, and writes the matching length-prefixed response frame
//! with the `ResponseHeader` that the peer expects. The header-flexibility
//! predicates that decide between the v1 and v2 request headers and the v0
//! and v1 response headers live here as well, because they are what makes
//! the two directions agree.

use krabka_client_core::ClientDuplex;
use krabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request, request_header::RequestHeader},
};
use krabka_raft::RaftHandshakeError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{API_KEY_API_VERSIONS, API_KEY_SASL_AUTHENTICATE, SASL_AUTHENTICATE_FLEXIBLE_VERSION};

/// Reads one length-prefixed Kafka request frame, removes the
/// `RequestHeader` (v1 or v2), and returns `(api_key, api_version,
/// correlation_id, body_bytes)`.
///
/// The header parsing matches the outbound encoder in
/// `network::client::round_trip`:
/// - v1, non-flexible: `api_key i16 | api_version i16 | corr_id i32 |
///   client_id i16-length-prefixed bytes`.
/// - v2, flexible, which `SaslAuthenticate v2+` and `ApiVersions v3+` use:
///   the v1 layout plus a tagged-fields section.
///
/// A size prefix over `max_frame_bytes` fails before the frame is read.
/// Kafka's `SaslServerAuthenticator` turns the `InvalidReceiveException` of
/// that read into a `SaslAuthenticationException`, so the failure is a SASL
/// one.
pub(super) async fn read_kafka_request(
    stream: &mut dyn ClientDuplex,
    max_frame_bytes: usize,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = u32::from_be_bytes(size_buf) as usize;
    crate::network::codec::validate_frame_length(size, max_frame_bytes).map_err(|_| {
        RaftHandshakeError::Sasl("Failing SASL authentication due to invalid receive size".into())
    })?;
    let mut frame = vec![0u8; size];
    stream.read_exact(&mut frame).await?;
    let [api_key_hi, api_key_lo, api_version_hi, api_version_lo, ..] = frame.as_slice() else {
        return Err(RaftHandshakeError::Protocol("short request header".into()));
    };
    let api_key = i16::from_be_bytes([*api_key_hi, *api_key_lo]);
    let api_version = i16::from_be_bytes([*api_version_hi, *api_version_lo]);
    let header_version = if is_request_header_flexible(api_key, api_version) {
        2
    } else {
        1
    };
    let mut body = frame.as_slice();
    let header = RequestHeader::decode(&mut body, header_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    Ok((
        header.request_api_key,
        header.request_api_version,
        header.correlation_id,
        body.to_vec(),
    ))
}

/// Encodes `resp`, prepends the `ResponseHeader` (v0 or v1 by the rules
/// below), and writes the length-prefixed frame.
pub(super) async fn write_response<R: Encode>(
    stream: &mut dyn ClientDuplex,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    resp: &R,
) -> Result<(), RaftHandshakeError> {
    let mut body = Vec::with_capacity(resp.encoded_len(api_version));
    resp.encode(&mut body, api_version)
        .map_err(|e| RaftHandshakeError::Protocol(e.to_string()))?;
    write_response_body(stream, api_key, api_version, corr_id, &body).await
}

/// Prepends the `ResponseHeader` (v0 or v1 by the rules below) to an encoded
/// response body, and writes the length-prefixed frame.
pub(super) async fn write_response_body(
    stream: &mut dyn ClientDuplex,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    body: &[u8],
) -> Result<(), RaftHandshakeError> {
    let flexible = is_response_header_flexible(api_key, api_version);
    let header_len = 4 + usize::from(flexible);
    let total = header_len + body.len();
    let total_u32 = u32::try_from(total)
        .map_err(|_| RaftHandshakeError::Protocol("response frame exceeds u32".into()))?;

    let mut out = Vec::with_capacity(4 + total);
    out.extend_from_slice(&total_u32.to_be_bytes());
    out.extend_from_slice(&corr_id.to_be_bytes());
    if flexible {
        out.push(0); // empty tagged-fields
    }
    out.extend_from_slice(body);
    stream.write_all(&out).await?;
    Ok(())
}

/// Request-header flexibility rules.
///
/// Mirrors the generated protocol schema. `SaslAuthenticate` becomes flexible
/// at v2 and `ApiVersions` at v3. `SaslHandshake` v0-v1 stays non-flexible.
fn is_request_header_flexible(api_key: i16, api_version: i16) -> bool {
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        API_KEY_API_VERSIONS => api_version >= api_versions_request::FLEXIBLE_MIN,
        _ => false,
    }
}

/// Response-header flexibility rules.
///
/// - `SaslHandshake (17)`: non-flexible at every version this module accepts.
/// - `SaslAuthenticate (36)`: flexible from v2.
/// - `ApiVersions (18)`: *always* a v0 response header by Kafka spec,
///   whatever the body flexibility. The Kafka clients special-case it.
fn is_response_header_flexible(api_key: i16, api_version: i16) -> bool {
    // SaslHandshake (17) and ApiVersions (18) keep the v0 response header
    // at every version we accept; only SaslAuthenticate (36) flips to a
    // flexible response header starting at v2.
    match api_key {
        API_KEY_SASL_AUTHENTICATE => api_version >= SASL_AUTHENTICATE_FLEXIBLE_VERSION,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::{BufMut, Bytes};

    use super::*;
    use crate::raft_handshake::{
        API_KEY_SASL_HANDSHAKE,
        test_support::{
            HandshakeApiKey, HandshakeApiVersion, HandshakeCorrelationId, HandshakeHeader,
            RequestFrameSetup, read_request_from_frame, read_response_frame, request_frame,
        },
    };

    struct FixedResp(&'static [u8]);

    impl Encode for FixedResp {
        fn encode<B: BufMut>(
            &self,
            buf: &mut B,
            _version: i16,
        ) -> Result<(), krabka_protocol::ProtocolError> {
            buf.put_slice(self.0);
            Ok(())
        }

        fn encoded_len(&self, _version: i16) -> usize {
            self.0.len()
        }
    }

    const SASL_HEADER_CASES: [(HandshakeApiKey, HandshakeApiVersion, HandshakeHeader); 4] = [
        (
            HandshakeApiKey(API_KEY_SASL_HANDSHAKE),
            HandshakeApiVersion(0),
            HandshakeHeader::Legacy,
        ),
        (
            HandshakeApiKey(API_KEY_SASL_HANDSHAKE),
            HandshakeApiVersion(1),
            HandshakeHeader::Legacy,
        ),
        (
            HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
            HandshakeApiVersion(1),
            HandshakeHeader::Legacy,
        ),
        (
            HandshakeApiKey(API_KEY_SASL_AUTHENTICATE),
            HandshakeApiVersion(2),
            HandshakeHeader::Flexible,
        ),
    ];

    #[test]
    fn header_flexibility_table_matches_outbound_encoder() {
        // SASL request and response headers share the version rules above.
        for (api_key, version, want) in SASL_HEADER_CASES.into_iter().chain([
            (
                HandshakeApiKey(API_KEY_API_VERSIONS),
                HandshakeApiVersion(2),
                HandshakeHeader::Legacy,
            ),
            (
                HandshakeApiKey(API_KEY_API_VERSIONS),
                HandshakeApiVersion(3),
                HandshakeHeader::Flexible,
            ),
        ]) {
            assert!(
                is_request_header_flexible(api_key.0, version.0) == want.is_flexible(),
                "request api_key {} v{}",
                api_key.0,
                version.0
            );
        }
        // ApiVersions responses always use header v0, including request v3.
        for (api_key, version, want) in SASL_HEADER_CASES.into_iter().chain([
            (
                HandshakeApiKey(API_KEY_API_VERSIONS),
                HandshakeApiVersion(0),
                HandshakeHeader::Legacy,
            ),
            (
                HandshakeApiKey(API_KEY_API_VERSIONS),
                HandshakeApiVersion(3),
                HandshakeHeader::Legacy,
            ),
        ]) {
            assert!(
                is_response_header_flexible(api_key.0, version.0) == want.is_flexible(),
                "response api_key {} v{}",
                api_key.0,
                version.0
            );
        }
    }

    #[tokio::test]
    async fn read_kafka_request_decodes_nonflex_and_flexible_headers() {
        let nonflex = request_frame(RequestFrameSetup {
            correlation: HandshakeCorrelationId(42),
            client_id: None,
            body: b"plain-body",
            ..Default::default()
        });
        let decoded = read_request_from_frame(nonflex)
            .await
            .expect("nonflex request");
        assert!(decoded == (17, 1, 42, b"plain-body".to_vec()));

        let flex = request_frame(RequestFrameSetup {
            api_key: HandshakeApiKey(36),
            api_version: HandshakeApiVersion(2),
            correlation: HandshakeCorrelationId(43),
            header: HandshakeHeader::Flexible,
            body: b"auth-body",
            ..Default::default()
        });
        let decoded = read_request_from_frame(flex).await.expect("flex request");
        assert!(decoded == (36, 2, 43, b"auth-body".to_vec()));

        let mut inner = bytes::BytesMut::new();
        RequestHeader {
            request_api_key: API_KEY_API_VERSIONS,
            request_api_version: 3,
            correlation_id: 44,
            client_id: Some("c".to_string()),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![
                krabka_protocol::UnknownTaggedField {
                    tag: 300,
                    bytes: Bytes::from_static(b"tag-payload"),
                },
            ]),
        }
        .encode(&mut inner, 2)
        .expect("encode flexible request header with tag");
        inner.extend_from_slice(b"api-body");
        let mut tagged = Vec::new();
        tagged.extend_from_slice(
            &u32::try_from(inner.len())
                .expect("frame fits u32")
                .to_be_bytes(),
        );
        tagged.extend_from_slice(&inner);
        let decoded = read_request_from_frame(tagged)
            .await
            .expect("tagged flexible request");
        assert!(decoded == (API_KEY_API_VERSIONS, 3, 44, b"api-body".to_vec()));
    }

    #[tokio::test]
    async fn read_kafka_request_rejects_short_and_truncated_headers() {
        for payload_len in 0..4 {
            let mut frame = Vec::new();
            frame.extend_from_slice(&u32::try_from(payload_len).unwrap().to_be_bytes());
            frame.resize(4 + payload_len, 0);
            let got = read_request_from_frame(frame).await;
            assert!(
                matches!(got, Err(RaftHandshakeError::Protocol(_))),
                "want protocol error for {payload_len}-byte header, got {got:?}"
            );
        }

        let mut short = Vec::new();
        short.extend_from_slice(&9u32.to_be_bytes());
        short.extend_from_slice(&[0; 9]);

        let mut truncated_client = Vec::new();
        truncated_client.extend_from_slice(&12u32.to_be_bytes());
        truncated_client.extend_from_slice(&17i16.to_be_bytes());
        truncated_client.extend_from_slice(&1i16.to_be_bytes());
        truncated_client.extend_from_slice(&7i32.to_be_bytes());
        truncated_client.extend_from_slice(&3i16.to_be_bytes());
        truncated_client.extend_from_slice(b"xy");

        let missing_tag = request_frame(RequestFrameSetup {
            api_key: HandshakeApiKey(36),
            api_version: HandshakeApiVersion(2),
            correlation: HandshakeCorrelationId(44),
            ..Default::default()
        });

        for frame in [short, truncated_client, missing_tag] {
            let got = read_request_from_frame(frame).await;
            assert!(
                matches!(got, Err(RaftHandshakeError::Protocol(_))),
                "want protocol error, got {got:?}"
            );
        }
    }

    /// A size prefix over the limit fails as a SASL failure, before any of the
    /// frame is read: the stream holds the four size bytes and nothing else, so
    /// a reader that tried to fill the frame would fail with an I/O error. The
    /// limit is inclusive, and a prefix that reads as a negative Kafka int is
    /// over it.
    #[tokio::test]
    async fn read_kafka_request_refuses_a_frame_over_the_sasl_receive_limit() {
        for (size, refused) in [
            (4096_u32, false),
            (4097, true),
            (u32::MAX, true),
            (i32::MIN.cast_unsigned(), true),
        ] {
            let (mut client, mut server) = tokio::io::duplex(64);
            client
                .write_all(&size.to_be_bytes())
                .await
                .expect("write the size prefix");
            // Closing the writer ends the stream, so a reader that goes on to
            // fill an admitted frame fails at once instead of waiting.
            drop(client);
            let got = read_kafka_request(&mut server, 4096).await;
            assert!(
                matches!(got, Err(RaftHandshakeError::Sasl(_))) == refused,
                "a {size}-byte frame: {got:?}"
            );
        }
    }

    #[tokio::test]
    async fn read_kafka_request_accepts_exact_client_id_end_boundary() {
        let frame = request_frame(RequestFrameSetup {
            correlation: HandshakeCorrelationId(45),
            client_id: Some(b"client"),
            ..Default::default()
        });
        let decoded = read_request_from_frame(frame).await.expect("exact header");
        assert!(decoded == (17, 1, 45, Vec::new()));
    }

    #[tokio::test]
    async fn read_kafka_request_accepts_exact_header_prefix_frame() {
        // A null client id and empty body make the frame exactly 10 bytes,
        // the minimum legal v1 request header.
        let frame = request_frame(RequestFrameSetup {
            correlation: HandshakeCorrelationId(46),
            client_id: None,
            ..Default::default()
        });
        let decoded = read_request_from_frame(frame).await.expect("exact prefix");
        assert!(decoded == (17, 1, 46, Vec::new()));
    }

    #[tokio::test]
    async fn write_response_uses_expected_header_versions() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_AUTHENTICATE,
                2,
                77,
                &FixedResp(&[0xaa, 0xbb]),
            )
            .await
            .expect("write flexible");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        // corr_id 77 BE + empty tagged-fields byte (flexible header) + body.
        let expected: Vec<u8> = [77i32.to_be_bytes().as_slice(), &[0x00], &[0xaa, 0xbb]].concat();
        assert!(frame == expected);

        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            write_response(
                &mut server,
                API_KEY_SASL_HANDSHAKE,
                1,
                78,
                &FixedResp(&[0xcc]),
            )
            .await
            .expect("write nonflex");
        });
        let frame = read_response_frame(&mut client).await;
        writer.await.expect("writer");
        assert!(&frame[0..4] == &78i32.to_be_bytes());
        assert!(&frame[4..] == &[0xcc]);
    }
}
