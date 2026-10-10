//! Borrowed Kafka request-header parsing for the broker dispatch loop.

use bytes::Buf;
use krabka_protocol::primitives::string_bytes_borrowed::get_nullable_string_borrowed;

use crate::{
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ParsedRequest<'a> {
    pub api_key: ApiKeyCode,
    pub api_version: ApiVersion,
    pub correlation_id: CorrelationId,
    pub body: &'a [u8],
    pub body_flexible: bool,
    pub client_id: Option<&'a str>,
}

pub(crate) fn parse_request<F>(
    frame: &[u8],
    flexible_for: F,
) -> Result<ParsedRequest<'_>, BrokerError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    if frame.len() < 8 {
        return Err(protocol_invalid("request frame < 8 bytes"));
    }

    let mut cur = frame;
    let api_key = cur.get_i16();
    let api_version = cur.get_i16();
    let correlation_id = cur.get_i32();
    let body_flexible = flexible_for(api_key, api_version);

    let client_id = get_nullable_string_borrowed(&mut cur)?;

    if body_flexible {
        krabka_protocol::tagged_fields::read_tagged_fields(&mut cur, |_tag, _payload| Ok(false))?;
    }

    Ok(ParsedRequest {
        api_key,
        api_version,
        correlation_id,
        body: cur,
        body_flexible,
        client_id,
    })
}

pub(crate) fn peek_api_key(frame: &[u8]) -> Result<ApiKeyCode, BrokerError> {
    if frame.len() < 2 {
        return Err(protocol_invalid("request frame < 2 bytes"));
    }
    Ok(i16::from_be_bytes([frame[0], frame[1]]))
}

fn protocol_invalid(message: &'static str) -> BrokerError {
    BrokerError::Protocol(krabka_protocol::ProtocolError::InvalidValue(message))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::{BufMut, BytesMut};

    use super::*;
    use crate::network::test_support::{RequestFrameSetup, request_frame};

    fn legacy_metadata_frame() -> BytesMut {
        request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(3),
            api_version: krabka_ids::ApiVersion(8),
            correlation_id: crate::network::test_support::FrameCorrelationId(42),
            client_id: Some(b"client-a"),
            body: b"body",
            ..Default::default()
        })
    }

    fn assert_flexible_client_body(parsed: &ParsedRequest<'_>) {
        check!(parsed.client_id == Some("client-a"));
        check!(parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_non_flexible_header() {
        let frame = legacy_metadata_frame();

        let parsed = parse_request(&frame, |_, _| false).expect("parse request");

        check!(parsed.api_key == 3);
        check!(parsed.api_version == 8);
        check!(parsed.correlation_id == 42);
        check!(parsed.client_id == Some("client-a"));
        check!(!parsed.body_flexible);
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_flexible_header_consumes_tagged_fields_byte() {
        let frame = request_frame(RequestFrameSetup {
            api_version: krabka_ids::ApiVersion(3),
            correlation_id: crate::network::test_support::FrameCorrelationId(7),
            client_id: Some(b"client-a"),
            tagged: Some(&[0]),
            body: b"body",
            ..Default::default()
        });

        let parsed =
            parse_request(&frame, |key, version| key == 18 && version >= 3).expect("parse request");

        check!(parsed.api_key == 18);
        check!(parsed.api_version == 3);
        check!(parsed.correlation_id == 7);
        assert_flexible_client_body(&parsed);
    }

    #[test]
    fn parse_request_flexible_header_skips_non_empty_tagged_fields() {
        let frame = request_frame(RequestFrameSetup {
            api_version: krabka_ids::ApiVersion(3),
            correlation_id: crate::network::test_support::FrameCorrelationId(7),
            client_id: Some(b"client-a"),
            tagged: Some(&[1, 1, 3, b't', b'a', b'g']),
            body: b"body",
            ..Default::default()
        });

        let parsed =
            parse_request(&frame, |key, version| key == 18 && version >= 3).expect("parse request");

        assert_flexible_client_body(&parsed);
    }

    #[test]
    fn parse_request_preserves_empty_client_id() {
        let frame = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(3),
            api_version: krabka_ids::ApiVersion(8),
            correlation_id: crate::network::test_support::FrameCorrelationId(42),
            client_id: Some(b""),
            body: b"body",
            ..Default::default()
        });

        let parsed = parse_request(&frame, |_, _| false).expect("parse request");

        check!(parsed.client_id == Some(""));
        check!(parsed.body == b"body".as_slice());
    }

    #[test]
    fn parse_request_rejects_invalid_utf8_client_id() {
        let frame = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(3),
            api_version: krabka_ids::ApiVersion(8),
            correlation_id: crate::network::test_support::FrameCorrelationId(42),
            client_id: Some(&[0xff, 0xfe]),
            body: b"body",
            ..Default::default()
        });

        let err = parse_request(&frame, |_, _| false).expect_err("invalid utf8 client id");

        assert!(matches!(
            err,
            BrokerError::Protocol(krabka_protocol::ProtocolError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn parse_request_rejects_truncated_headers() {
        let mut missing_client_id_len = BytesMut::new();
        missing_client_id_len.put_i16(3);
        missing_client_id_len.put_i16(8);
        missing_client_id_len.put_i32(42);

        let truncated_client_id = request_frame(RequestFrameSetup {
            api_key: krabka_ids::ApiKey(3),
            api_version: krabka_ids::ApiVersion(8),
            correlation_id: crate::network::test_support::FrameCorrelationId(42),
            client_id: Some(b"client"),
            ..Default::default()
        });
        let flexible_without_tag = request_frame(RequestFrameSetup {
            api_version: krabka_ids::ApiVersion(3),
            correlation_id: crate::network::test_support::FrameCorrelationId(42),
            client_id: Some(b"client"),
            ..Default::default()
        });

        let cases = [
            ("missing fixed header", vec![0_u8; 7]),
            ("missing client id length", missing_client_id_len.to_vec()),
            (
                "truncated client id",
                truncated_client_id[..truncated_client_id.len() - 1].to_vec(),
            ),
            (
                "flexible missing tagged byte",
                flexible_without_tag.to_vec(),
            ),
        ];

        for (case, frame) in cases {
            assert!(
                parse_request(&frame, |key, version| key == 18 && version >= 3).is_err(),
                "{case}"
            );
        }
    }

    #[test]
    fn peek_api_key_matches_existing_dispatch_behavior() {
        let present = legacy_metadata_frame();

        assert!(peek_api_key(&present).expect("api key") == 3);
    }
}
