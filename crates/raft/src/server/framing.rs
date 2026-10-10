//! Kafka request and response framing for the controller listener: the
//! length-prefixed frame codec, the request-header decode, and the
//! flexible-version negotiation that decides whether a frame carries tagged
//! fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_ids::{ApiKey, ApiVersion};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::api_versions::table;
use crate::{
    error::RaftError,
    wire::{API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE},
};

/// Kafka request-header `correlation_id`, echoed back in the response header.
pub(super) type CorrelationId = i32;

pub(super) fn is_eof(e: &RaftError) -> bool {
    matches!(e,
        RaftError::Storage(krabka_log::LogError::Io(io))
            if io.kind() == std::io::ErrorKind::UnexpectedEof
    )
}

fn io_err(e: std::io::Error) -> RaftError {
    RaftError::Storage(krabka_log::LogError::Io(e))
}

fn truncated(needed: usize) -> RaftError {
    RaftError::Protocol(krabka_protocol::ProtocolError::UnexpectedEof { needed })
}

fn require_remaining(available: usize, required: usize) -> Result<(), RaftError> {
    match required.checked_sub(available) {
        Some(0) | None => Ok(()),
        Some(needed) => Err(truncated(needed)),
    }
}

/// Whether a request frame for `api_key` at `version` carries tagged fields.
///
/// The minimum comes from the advertised API table, so an API the listener
/// serves reads its own generated `FLEXIBLE_MIN` and nothing else. Note that
/// this is asked of every version a peer may send, not only the advertised
/// range: a request outside that range still has to have its header parsed
/// before the handler can refuse it.
fn request_is_flexible(
    api_key: i16,
    version: i16,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
) -> bool {
    let flexible_min = match api_key {
        // Krabka-private RPCs are always framed flexible, at every version.
        API_KEY_SUBMIT_CHANGE | API_KEY_METADATA_FETCH | API_KEY_DELEGATION_TOKEN_MUTATION => {
            Some(i16::MIN)
        }
        _ => table::flexible_min(api_key).or_else(|| {
            admin_router.and_then(|router| {
                router
                    .api_versions()
                    .iter()
                    .find(|api| api.api_key == api_key)
                    .map(|api| api.flexible_min)
            })
        }),
    };
    flexible_min.is_some_and(|minimum| version >= minimum)
}

/// Reads one request frame and parses its header.
///
/// A size prefix that is negative, or above `max_request_bytes`
/// (`socket.request.max.bytes`), fails before the frame is read, as Kafka's
/// `NetworkReceive.readFrom` throws `InvalidReceiveException` on it. The caller
/// closes the connection, and no allocation is sized from the peer's number.
pub(super) async fn read_one_request<S>(
    stream: &mut S,
    admin_router: Option<&dyn crate::ControllerAdminRouter>,
    max_request_bytes: usize,
) -> Result<
    (
        ApiKey,
        ApiVersion,
        CorrelationId,
        Option<String>,
        Bytes,
        bool,
    ),
    RaftError,
>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    const REQUEST_HEADER_FIXED_LEN: usize = 8;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(io_err)?;
    let len = usize::try_from(i32::from_be_bytes(len_buf))
        .ok()
        .filter(|len| *len <= max_request_bytes)
        .ok_or(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue(
                "request frame size is negative or over socket.request.max.bytes",
            ),
        ))?;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(io_err)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(NULLABLE_STRING), tagged_fields(varint=0).
    // The two adjacent header `int16`s are wrapped into distinct newtypes here so
    // the transpose-prone pair can't be swapped by callers.
    let mut cur: &[u8] = &frame;
    require_remaining(cur.remaining(), REQUEST_HEADER_FIXED_LEN)?;
    let api_key_n = ApiKey(cur.get_i16());
    let api_version = ApiVersion(cur.get_i16());
    let correlation_id = cur.get_i32();

    // Decode client_id: NULLABLE_STRING (i16 length + bytes; -1 = null).
    require_remaining(cur.remaining(), 2)?;
    let cs_len = cur.get_i16();
    let client_id = match cs_len {
        -1 => None,
        0.. => {
            let n = usize::try_from(cs_len).expect("non-negative i16 fits usize");
            require_remaining(cur.remaining(), n)?;
            let (raw, rest) = cur.split_at(n);
            cur = rest;
            Some(
                std::str::from_utf8(raw)
                    .map_err(krabka_protocol::ProtocolError::InvalidUtf8)?
                    .to_owned(),
            )
        }
        _ => {
            return Err(RaftError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue("client id length below -1"),
            ));
        }
    };
    let response_flexible = request_is_flexible(api_key_n.get(), api_version.get(), admin_router);
    if response_flexible {
        krabka_protocol::tagged_fields::read_tagged_fields(&mut cur, |_tag, _payload| Ok(false))?;
    }

    Ok((
        api_key_n,
        api_version,
        correlation_id,
        client_id,
        Bytes::copy_from_slice(cur),
        response_flexible,
    ))
}

pub(super) async fn write_response<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, true).await
}

/// Write a response without the leading tagged-fields byte. Used only by the
/// `ApiVersions` v0 path, which decodes a `ResponseHeader v0`.
pub(super) async fn write_response_no_tagged_fields<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_response_frame(stream, correlation_id, body, false).await
}

pub(super) async fn write_response_frame<S>(
    stream: &mut S,
    correlation_id: CorrelationId,
    body: Bytes,
    include_tagged_fields: bool,
) -> Result<(), RaftError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(4 + usize::from(include_tagged_fields) + body.len());
    frame.put_i32(correlation_id);
    if include_tagged_fields {
        frame.put_u8(0); // empty tagged_fields (ResponseHeader v1)
    }
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(io_err)?;
    stream.write_all(&frame).await.map_err(io_err)?;
    stream.flush().await.map_err(io_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// `socket.request.max.bytes` for the tests that do not probe it.
    const MAX_REQUEST_BYTES: usize = 100 * 1024 * 1024;

    /// A size prefix over `socket.request.max.bytes`, or negative, is refused
    /// before the frame is read, and one at the limit is read. The stream
    /// carries the prefix alone, so a reader that tried to fill the frame
    /// would report end of input instead of the size error.
    #[tokio::test]
    async fn read_one_request_refuses_a_size_prefix_over_the_limit() {
        let limit = 64_usize;
        let cases = [
            ("at the limit", 64_i32, false),
            ("one over the limit", 65, true),
            ("a gigabyte over a 64-byte limit", 1 << 30, true),
            ("i32::MAX", i32::MAX, true),
            ("negative", -1, true),
            ("i32::MIN", i32::MIN, true),
        ];
        for (case, size, refused) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            client.write_all(&size.to_be_bytes()).await.unwrap();
            drop(client);

            let err = super::read_one_request(&mut server, None, limit)
                .await
                .expect_err("the stream holds no frame");

            check!(
                matches!(
                    err,
                    super::RaftError::Protocol(krabka_protocol::ProtocolError::InvalidValue(_))
                ) == refused,
                "{case}: {err}"
            );
            check!(super::is_eof(&err) == !refused, "{case}: {err}");
        }
    }

    /// A request is flexible from its API's own `FLEXIBLE_MIN` upward, and an
    /// API nobody declares is not flexible at any version.
    ///
    /// Each arm names its own constant, so one arm reaching for another API's
    /// minimum is invisible unless the versions either side of the boundary
    /// are asked for. Getting it wrong means reading tagged fields off a wire
    /// that has none, or skipping the ones that are there.
    #[test]
    fn a_request_is_flexible_from_its_own_minimum_upward() {
        use krabka_protocol::owned::{
            add_raft_voter_request, api_versions_request, describe_quorum_request, fetch_request,
            vote_request,
        };

        // (api key, that API's flexible minimum)
        let apis = [
            (
                api_versions_request::API_KEY,
                api_versions_request::FLEXIBLE_MIN,
            ),
            (fetch_request::API_KEY, fetch_request::FLEXIBLE_MIN),
            (vote_request::API_KEY, vote_request::FLEXIBLE_MIN),
            (
                describe_quorum_request::API_KEY,
                describe_quorum_request::FLEXIBLE_MIN,
            ),
            (
                add_raft_voter_request::API_KEY,
                add_raft_voter_request::FLEXIBLE_MIN,
            ),
        ];
        for (api_key, flexible_min) in apis {
            check!(
                request_is_flexible(api_key, flexible_min, None),
                "api {api_key} at its own minimum {flexible_min}"
            );
            check!(
                request_is_flexible(api_key, flexible_min + 1, None),
                "api {api_key} above its minimum"
            );
            if let Some(below) = flexible_min.checked_sub(1) {
                check!(
                    !request_is_flexible(api_key, below, None),
                    "api {api_key} below its minimum"
                );
            }
        }

        // An API this server does not route, with no admin extension to claim it.
        check!(!request_is_flexible(i16::MAX, 0, None));
    }

    /// A KIP-919 Admin router that declares one API surface and routes nothing.
    /// The framing path only ever asks it which versions it serves.
    struct StubAdminRouter(Vec<crate::ControllerApiVersion>);

    impl crate::ControllerAdminRouter for StubAdminRouter {
        fn api_versions(&self) -> &[crate::ControllerApiVersion] {
            &self.0
        }

        fn route(
            &self,
            _request: crate::ControllerAdminRequest,
        ) -> crate::ControllerAdminRouteFuture<'_> {
            Box::pin(std::future::ready(Ok(None)))
        }
    }

    fn length_prefixed(frame: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(frame.len() + 4);
        out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
        out.extend_from_slice(frame);
        out
    }

    type DecodedFrame = (
        ApiKey,
        ApiVersion,
        CorrelationId,
        Option<String>,
        Bytes,
        bool,
    );

    async fn decode_frame(
        frame: Vec<u8>,
        router: Option<&dyn crate::ControllerAdminRouter>,
    ) -> DecodedFrame {
        let (mut client, mut server) = tokio::io::duplex(128);
        let writer = tokio::spawn(async move {
            client.write_all(&frame).await.unwrap();
        });
        let request = super::read_one_request(&mut server, router, MAX_REQUEST_BYTES)
            .await
            .expect("decode");
        writer.await.unwrap();
        request
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct FrameCorrelationId(i32);

    /// Signed wire lengths include the null sentinel and malformed negative cases.
    #[derive(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct ClientIdLength(i16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ControllerFrameSetup<'a> {
        #[default(ApiKey(52))]
        api_key: ApiKey,
        #[default(ApiVersion(2))]
        api_version: ApiVersion,
        #[default(FrameCorrelationId(123))]
        correlation_id: FrameCorrelationId,
        #[default("raft-client")]
        client_id: &'a str,
        body: &'a [u8],
    }

    fn request_frame(setup: ControllerFrameSetup<'_>) -> Vec<u8> {
        let ControllerFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id,
            body,
        } = setup;
        let mut tagged_body = vec![0];
        tagged_body.extend_from_slice(body);
        raw_request_frame(RawControllerFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id_len: ClientIdLength(i16::try_from(client_id.len()).unwrap()),
            client_id_bytes: client_id.as_bytes(),
            tagged_or_body: &tagged_body,
        })
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct RawControllerFrameSetup<'a> {
        #[default(ApiKey(52))]
        api_key: ApiKey,
        #[default(ApiVersion(2))]
        api_version: ApiVersion,
        #[default(FrameCorrelationId(123))]
        correlation_id: FrameCorrelationId,
        #[default(ClientIdLength(-1))]
        client_id_len: ClientIdLength,
        client_id_bytes: &'a [u8],
        #[default(&[0])]
        tagged_or_body: &'a [u8],
    }

    fn raw_request_frame(setup: RawControllerFrameSetup<'_>) -> Vec<u8> {
        let RawControllerFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id_len,
            client_id_bytes,
            tagged_or_body,
        } = setup;
        let mut frame = bytes::BytesMut::new();
        frame.put_i16(api_key.get());
        frame.put_i16(api_version.get());
        frame.put_i32(correlation_id.0);
        frame.put_i16(client_id_len.0);
        frame.put_slice(client_id_bytes);
        frame.put_slice(tagged_or_body);
        length_prefixed(&frame)
    }

    #[test]
    fn is_eof_only_matches_unexpected_eof_io_errors() {
        let io_error = |kind| {
            super::RaftError::Storage(krabka_log::LogError::Io(std::io::Error::new(kind, "io")))
        };
        let cases = [
            (
                "unexpected EOF",
                io_error(std::io::ErrorKind::UnexpectedEof),
                true,
            ),
            (
                "broken pipe",
                io_error(std::io::ErrorKind::BrokenPipe),
                false,
            ),
            (
                "protocol error",
                super::RaftError::Protocol(krabka_protocol::ProtocolError::InvalidValue("not io")),
                false,
            ),
        ];
        for (_case, err, want) in cases {
            assert2::assert!(super::is_eof(&err) == want);
        }
    }

    #[tokio::test]
    async fn read_one_request_decodes_header_variants() {
        let cases = [
            (
                "flexible header with client id and body",
                request_frame(ControllerFrameSetup {
                    body: b"payload",
                    ..Default::default()
                }),
                b"payload".as_slice(),
            ),
            (
                "null client id with no body",
                raw_request_frame(RawControllerFrameSetup::default()),
                b"".as_slice(),
            ),
        ];
        for (case, frame, want_body) in cases {
            let (api_key, api_version, correlation_id, client_id, body, flexible) =
                decode_frame(frame, None).await;

            check!(
                (
                    api_key,
                    api_version,
                    correlation_id,
                    client_id.as_deref(),
                    body.as_ref(),
                    flexible,
                ) == (
                    ApiKey(52),
                    ApiVersion(2),
                    123,
                    if case.starts_with("null") {
                        None
                    } else {
                        Some("raft-client")
                    },
                    want_body,
                    true,
                ),
                "case: {case}"
            );
        }
    }

    #[tokio::test]
    async fn read_one_request_reports_header_shortfalls() {
        let partial_fixed = {
            let mut f = bytes::BytesMut::new();
            f.put_i16(52);
            f.put_i16(2);
            f.put_i32(123);
            f
        };
        let mut partial_client_id_len = partial_fixed.clone();
        partial_client_id_len.put_u8(0x80);
        let cases = [
            // Frame ends inside the 8-byte fixed header.
            ("short fixed header", length_prefixed(&[0, 52, 0, 2]), 4),
            // Fixed header complete, client-id length missing entirely.
            (
                "missing client id length",
                length_prefixed(&partial_fixed),
                2,
            ),
            // Only one byte of the 2-byte client-id length present.
            (
                "partial client id length",
                length_prefixed(&partial_client_id_len),
                1,
            ),
            // Client-id length declares 4 bytes; only 1 present.
            (
                "client id bytes shortfall",
                raw_request_frame(RawControllerFrameSetup {
                    client_id_len: ClientIdLength(4),
                    client_id_bytes: b"x",
                    tagged_or_body: &[],
                    ..Default::default()
                }),
                3,
            ),
        ];
        for (_case, frame, needed) in cases {
            let (mut client, mut server) = tokio::io::duplex(128);
            let writer = tokio::spawn(async move {
                client.write_all(&frame).await.unwrap();
            });

            let err = super::read_one_request(&mut server, None, MAX_REQUEST_BYTES)
                .await
                .expect_err("short frame");

            assert2::assert!(matches!(
                err,
                super::RaftError::Protocol(
                    krabka_protocol::ProtocolError::UnexpectedEof { needed: n }
                ) if n == needed
            ));
            writer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn read_one_request_keeps_nonflexible_body_prefix() {
        let (mut client, mut server) = tokio::io::duplex(128);
        let frame = raw_request_frame(RawControllerFrameSetup {
            api_key: ApiKey(53),
            api_version: ApiVersion(0),
            client_id_len: ClientIdLength(0),
            tagged_or_body: &[1, b'p', b'a', b'y'],
            ..Default::default()
        });
        let writer = tokio::spawn(async move {
            client.write_all(&frame).await.unwrap();
        });

        let (_, _, _, _, body, flexible) =
            super::read_one_request(&mut server, None, MAX_REQUEST_BYTES)
                .await
                .expect("decode");

        assert2::assert!(body.as_ref() == &[1, b'p', b'a', b'y']);
        assert2::assert!(!flexible);
        writer.await.unwrap();
    }

    /// `CreateTopics` is a KIP-919 Admin API: the listener's own table says
    /// nothing about it, so the flexibility minimum has to come from the
    /// attached router's table instead. Reading it from the wrong table means
    /// consuming a tagged-fields byte a v4 frame never carried, which eats the
    /// first byte of the body, or skipping the one a v5 frame did carry.
    ///
    /// The router is a fallback, not an override: it also declares `Vote`, an
    /// API the listener owns, at a minimum no version could reach. The
    /// listener's own table has to win that one, or a broker-side table could
    /// silently change how a KIP-595 peer frame is split.
    #[tokio::test]
    async fn an_admin_router_api_is_framed_from_the_routers_own_minimum() {
        use krabka_protocol::owned::{create_topics_request, vote_request};

        let router = StubAdminRouter(vec![
            crate::ControllerApiVersion {
                api_key: create_topics_request::API_KEY,
                min_version: create_topics_request::MIN_VERSION,
                max_version: create_topics_request::MAX_VERSION,
                released_max: Some(create_topics_request::MAX_VERSION),
                flexible_min: create_topics_request::FLEXIBLE_MIN,
            },
            crate::ControllerApiVersion {
                api_key: vote_request::API_KEY,
                min_version: vote_request::MIN_VERSION,
                max_version: vote_request::MAX_VERSION,
                released_max: Some(vote_request::MAX_VERSION),
                flexible_min: i16::MAX,
            },
        ]);
        let key = ApiKey(create_topics_request::API_KEY);
        let flexible_min = create_topics_request::FLEXIBLE_MIN;
        // An api key no Kafka message uses, so neither table claims it.
        let unclaimed = ApiKey(i16::MAX);
        let client_id = "admin-tool";
        let id_len = i16::try_from(client_id.len()).expect("client id length");

        let cases = [
            (
                "at the router's flexible minimum, the tagged-fields byte is consumed",
                key,
                ApiVersion(flexible_min),
                request_frame(ControllerFrameSetup {
                    api_key: key,
                    api_version: ApiVersion(flexible_min),
                    correlation_id: FrameCorrelationId(7),
                    client_id,
                    body: b"body",
                }),
                true,
            ),
            (
                "below it, the frame carries no tagged fields",
                key,
                ApiVersion(flexible_min - 1),
                raw_request_frame(RawControllerFrameSetup {
                    api_key: key,
                    api_version: ApiVersion(flexible_min - 1),
                    correlation_id: FrameCorrelationId(7),
                    client_id_len: ClientIdLength(id_len),
                    client_id_bytes: client_id.as_bytes(),
                    tagged_or_body: b"body",
                }),
                false,
            ),
            (
                "the listener's own table outranks the router's entry for it",
                ApiKey(vote_request::API_KEY),
                ApiVersion(vote_request::FLEXIBLE_MIN),
                request_frame(ControllerFrameSetup {
                    api_key: ApiKey(vote_request::API_KEY),
                    api_version: ApiVersion(vote_request::FLEXIBLE_MIN),
                    correlation_id: FrameCorrelationId(7),
                    client_id,
                    body: b"body",
                }),
                true,
            ),
            (
                "an api neither the listener nor the router declares is never flexible",
                unclaimed,
                ApiVersion(flexible_min),
                raw_request_frame(RawControllerFrameSetup {
                    api_key: unclaimed,
                    api_version: ApiVersion(flexible_min),
                    correlation_id: FrameCorrelationId(7),
                    client_id_len: ClientIdLength(id_len),
                    client_id_bytes: client_id.as_bytes(),
                    tagged_or_body: b"body",
                }),
                false,
            ),
        ];

        for (case, want_key, want_version, frame, want_flexible) in cases {
            let (api_key, api_version, correlation_id, decoded_client_id, body, flexible) =
                decode_frame(frame, Some(&router)).await;

            check!(
                (
                    api_key,
                    api_version,
                    correlation_id,
                    decoded_client_id.as_deref(),
                    body.as_ref(),
                    flexible,
                ) == (
                    want_key,
                    want_version,
                    7,
                    Some(client_id),
                    b"body".as_slice(),
                    want_flexible,
                ),
                "case: {case}"
            );
        }
    }
}
