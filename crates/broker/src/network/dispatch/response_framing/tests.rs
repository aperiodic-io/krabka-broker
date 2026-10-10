//! Tests for the response-framing benchmark seam.
//!
//! `benches/perf_deferrals.rs` prices a chained-`Buf` prototype against the
//! path the dispatch loop actually takes, and that comparison only means
//! anything while the two put the *same* bytes on the wire. The prototype is
//! assembled out of [`response_header_len`] and [`response_header_v1`]; the
//! path it is priced against is [`encode_response`] followed by the [`codec`]
//! the connection loop frames its stream with. Four functions, one wire
//! format, and nothing in a `cargo test` run had been holding them together.
//!
//! So these tests pin all four against the Kafka response header restated
//! here, and against the bytes a real `Framed` sink writes. A header shape
//! that drifts now fails a test, instead of quietly re-weighing a PERF note
//! whose "keep" the benchmark settled.

use assert2::assert;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::SinkExt as _;
use krabka_protocol::api_key::ApiKey;
use tokio::io::AsyncReadExt as _;
use tokio_util::codec::{Encoder as _, Framed};

use super::{codec, encode_response, response_header_len, response_header_v1};
use crate::{
    handlers::{ApiKeyCode, CorrelationId},
    network::codec::KafkaCodec,
};

/// Kafka's default `socket.request.max.bytes`, the request limit the dispatch
/// loop builds its codec with.
const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// The correlation id every case echoes. Distinctive in all four bytes, so a
/// truncated or byte-swapped write cannot pass.
const CORRELATION_ID: CorrelationId = 0x0102_0304;

#[derive(Clone, Copy)]
enum ResponseBodyEncoding {
    Legacy,
    Flexible,
}

impl ResponseBodyEncoding {
    const fn is_flexible(self) -> bool {
        matches!(self, Self::Flexible)
    }
}

/// One framing case: the api key, the flexibility of the body the handler
/// produced, and how many bytes that body is.
struct Case {
    name: &'static str,
    api_key: ApiKey,
    encoding: ResponseBodyEncoding,
    body_len: PatternedPayloadLength,
}

/// Both header shapes, on both sides of the `ApiVersions` exception, plus the
/// empty body that has nothing but a header to carry.
const CASES: [Case; 5] = [
    Case {
        name: "a flexible body takes the v1 header",
        api_key: ApiKey::Metadata,
        encoding: ResponseBodyEncoding::Flexible,
        body_len: PatternedPayloadLength(1024),
    },
    Case {
        name: "a non-flexible body takes the v0 header",
        api_key: ApiKey::Metadata,
        encoding: ResponseBodyEncoding::Legacy,
        body_len: PatternedPayloadLength(1024),
    },
    Case {
        name: "ApiVersions keeps the v0 header even when its body is flexible",
        api_key: ApiKey::ApiVersions,
        encoding: ResponseBodyEncoding::Flexible,
        body_len: PatternedPayloadLength(37),
    },
    Case {
        name: "a non-flexible ApiVersions body takes the same v0 header",
        api_key: ApiKey::ApiVersions,
        encoding: ResponseBodyEncoding::Legacy,
        body_len: PatternedPayloadLength(37),
    },
    Case {
        name: "an empty flexible body still carries its tagged-fields byte",
        api_key: ApiKey::Metadata,
        encoding: ResponseBodyEncoding::Flexible,
        body_len: PatternedPayloadLength(0),
    },
];

// A response body of `len` bytes, in a non-uniform pattern so nothing
// downstream can shortcut it and a truncation cannot land on a repeat.
krabka_macros::patterned_bytes_fixture!(body);

/// The response header Kafka puts in front of a body, restated here rather
/// than read back out of the broker: the correlation id, followed by an empty
/// tagged-fields byte when the body is flexible. `ApiVersions` is the standing
/// exception and stays on the v0 header at every version, because a client has
/// to parse that response before it knows which versions the broker speaks.
fn expected_header(api_key: ApiKey, encoding: ResponseBodyEncoding) -> Bytes {
    let mut header = BytesMut::new();
    header.put_i32(CORRELATION_ID);
    if encoding.is_flexible() && api_key != ApiKey::ApiVersions {
        header.put_u8(0);
    }
    header.freeze()
}

/// The bytes a sink framed with `codec` actually writes for `response`.
///
/// A real `Framed`, driven with `send`, which is the pair of `start_send` and
/// `poll_flush` that `serve_connection_stream` drives per response. Dropping
/// it closes the write half so the read side sees EOF.
async fn wire_bytes(response: Bytes, codec: KafkaCodec) -> Vec<u8> {
    let (client, mut server) = tokio::io::duplex(1024 * 1024);
    let mut framed = Framed::new(client, codec);
    framed
        .send(response)
        .await
        .expect("a duplex write succeeds");
    drop(framed);
    let mut wire = Vec::new();
    server
        .read_to_end(&mut wire)
        .await
        .expect("read the framed response back");
    wire
}

krabka_macros::frame_prefix_fixture!(wire_frame_prefix);

/// The chained-`Buf` prototype's wire image, assembled out of the two header
/// helpers the way `benches/perf_deferrals.rs` assembles it: the codec's
/// 4-byte frame length and the response header in one leading segment, then
/// the handler's body.
fn chained_prototype_wire(
    api_key: ApiKey,
    encoding: ResponseBodyEncoding,
    body: &Bytes,
) -> Vec<u8> {
    let api_key = api_key as ApiKeyCode;
    let body_flexible = encoding.is_flexible();
    let header_len = response_header_len(api_key, body_flexible);
    let mut wire = wire_frame_prefix(ResponsePrefixSetup {
        header_len: ResponseByteCount(header_len),
        correlation_id: ResponseCorrelationId(CORRELATION_ID),
        header: ResponseHeaderEncoding::from_wire(response_header_v1(api_key, body_flexible)),
        body_len: ResponseByteCount(body.len()),
        capacity: ResponseByteCount(4 + header_len + body.len()),
        context: "a test body fits in a frame",
    });
    wire.put_slice(body);
    wire.to_vec()
}

/// The seam's `encode_response` is the dispatch loop's own, and what it
/// produces is the header this test spells out followed by the untouched body.
/// Both header helpers describe exactly that header.
#[test]
fn the_seam_encodes_the_response_header_the_dispatch_loop_encodes() {
    for case in CASES {
        let payload = body(case.body_len);
        let header = expected_header(case.api_key, case.encoding);

        assert!(
            response_header_len(case.api_key as ApiKeyCode, case.encoding.is_flexible())
                == header.len(),
            "{}",
            case.name
        );
        assert!(
            response_header_v1(case.api_key as ApiKeyCode, case.encoding.is_flexible())
                == (header.len() == 5),
            "{}",
            case.name
        );

        let framed = encode_case(&case, &payload);

        let mut expected = BytesMut::from(&header[..]);
        expected.put_slice(&payload);
        assert!(framed.to_vec() == expected.to_vec(), "{}", case.name);

        // The seam exists only because the production function is
        // crate-internal. A reimplementation that drifted from it shows up
        // here rather than in a benchmark number nobody re-derives.
        let production = super::super::response::encode_response(
            case.api_key as ApiKeyCode,
            CORRELATION_ID,
            case.encoding.is_flexible(),
            &payload,
        )
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));
        assert!(framed == production, "{}", case.name);
    }
}

/// The seam's codec writes what the connection loop's codec writes, and both
/// write the length-prefixed frame this test spells out.
#[tokio::test]
async fn the_seam_codec_writes_the_frame_the_connection_loop_writes() {
    for case in CASES {
        let (_payload, framed) = encoded_case(&case);

        let mut expected = BytesMut::new();
        expected.put_u32(u32::try_from(framed.len()).expect("a test frame fits in a u32"));
        expected.put_slice(&framed);
        let expected = expected.to_vec();

        let seam = wire_bytes(framed.clone(), codec(MAX_FRAME_BYTES)).await;
        let production = wire_bytes(framed, crate::network::codec::codec(MAX_FRAME_BYTES)).await;

        assert!(seam == expected, "{}", case.name);
        assert!(production == expected, "{}", case.name);
    }
}

/// The prototype the bench prices against this path is byte-identical to it.
///
/// This is the invariant the whole "saved ns" column rests on: were the header
/// helpers to disagree with `encode_response`, the bench would be timing two
/// different amounts of work and reporting a saving that does not exist. The
/// bench asserts it too, but nothing in CI runs the bench.
#[tokio::test]
async fn the_chained_prototype_the_bench_prices_is_wire_identical() {
    for case in CASES {
        let (payload, framed) = encoded_case(&case);

        let copy_path = wire_bytes(framed, codec(MAX_FRAME_BYTES)).await;
        let prototype = chained_prototype_wire(case.api_key, case.encoding, &payload);

        assert!(copy_path == prototype, "{}", case.name);
    }
}

/// `socket.request.max.bytes` bounds a request and never a response, so a
/// codec built for small requests still frames a response larger than that
/// limit, as the production codec does.
#[test]
fn the_seam_codec_frames_a_response_over_the_request_limit() {
    let response = Bytes::from(vec![0_u8; 9]);
    let mut expected = BytesMut::new();
    expected.put_u32(9);
    expected.put_slice(&response);

    for (which, mut codec) in [
        ("seam", codec(8)),
        ("production", crate::network::codec::codec(8)),
    ] {
        let mut wire = BytesMut::new();
        codec
            .encode(response.clone(), &mut wire)
            .unwrap_or_else(|error| panic!("{which}: {error}"));
        assert!(wire == expected, "{which}");
    }
}

fn encode_case(case: &Case, payload: &Bytes) -> Bytes {
    encode_response(
        case.api_key as ApiKeyCode,
        CORRELATION_ID,
        case.encoding.is_flexible(),
        payload,
    )
    .unwrap_or_else(|error| panic!("{}: {error}", case.name))
}

fn encoded_case(case: &Case) -> (Bytes, Bytes) {
    let payload = body(case.body_len);
    let framed = encode_case(case, &payload);
    (payload, framed)
}
