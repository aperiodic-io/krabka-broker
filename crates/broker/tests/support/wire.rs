//! Length-prefixed Kafka frame I/O, leaving headers and decoding to each caller.

use std::io;

use krabka_units::ByteSize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Write and flush a frame with the caller's original length diagnostic.
///
/// # Errors
/// Returns the underlying socket write or flush error.
///
/// # Panics
/// Panics if the frame length does not fit Kafka's u32 length prefix.
pub async fn write_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    frame: &[u8],
    length_context: Option<&str>,
) -> io::Result<()> {
    let length = u32::try_from(frame.len());
    let length = match length_context {
        Some(context) => length.expect(context),
        None => length.unwrap(),
    };
    stream.write_u32(length).await?;
    stream.write_all(frame).await?;
    stream.flush().await
}

/// Read a complete frame, retaining its response header for caller validation.
///
/// # Errors
/// Returns the underlying socket read error.
pub async fn read_frame<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Vec<u8>> {
    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;
    Ok(resp)
}

/// A request sent at exactly version `V`, whatever the broker also supports.
#[derive(Clone, Debug)]
pub struct At<R, const V: i16>(pub R);

impl<R: krabka_protocol::Encode, const V: i16> krabka_protocol::Encode for At<R, V> {
    fn encode<B: bytes::BufMut>(
        &self,
        buf: &mut B,
        version: i16,
    ) -> Result<(), krabka_protocol::ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl<R: krabka_protocol::ProtocolRequest, const V: i16> krabka_protocol::ProtocolRequest
    for At<R, V>
{
    const API_KEY: i16 = R::API_KEY;
    const MIN_VERSION: i16 = V;
    const MAX_VERSION: i16 = V;
    const LATEST_STABLE_VERSION: i16 = V;
    const FLEXIBLE_MIN: i16 = R::FLEXIBLE_MIN;
    type Response = R::Response;
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct SocketExchangeSetup {
    #[default(krabka_ids::ApiKey(18))]
    pub api_key: krabka_ids::ApiKey,
    pub version: krabka_ids::ApiVersion,
    #[default(CorrelationId(1))]
    pub correlation: CorrelationId,
    pub header: HeaderEncoding,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ApiRequestSetup {
    #[default(krabka_ids::ApiKey(18))]
    pub api_key: krabka_ids::ApiKey,
    pub version: krabka_ids::ApiVersion,
}

/// Bind the shared Kafka exchange to a suite's original client identifier.
#[macro_export]
macro_rules! socket_round_trip_fixture {
    ($(#[$attrs:meta])* $vis:vis $name:ident, $client_id:expr) => {
        $(#[$attrs])*
        $vis async fn $name<S: ::tokio::io::AsyncRead + ::tokio::io::AsyncWrite + Unpin>(
            stream: &mut S,
            setup: $crate::support::wire::SocketExchangeSetup,
            body: &[u8],
        ) -> ::std::io::Result<Vec<u8>> {
            $crate::kafka_wire::round_trip(stream, $crate::kafka_wire::ExchangeSetup::for_socket(setup, $client_id), body).await
        }
    };
}

/// A flexible exchange whose suite pins the same correlation and client IDs.
#[macro_export]
macro_rules! flexible_round_trip_fixture {
    ($(#[$attrs:meta])* $vis:vis $name:ident, $client_id:expr, $correlation:expr) => {
        $(#[$attrs])*
        $vis async fn $name(
            stream: &mut ::tokio::net::TcpStream,
            setup: $crate::support::wire::ApiRequestSetup,
            body: &[u8],
        ) -> ::std::io::Result<Vec<u8>> {
            $crate::kafka_wire::round_trip(stream, $crate::kafka_wire::ExchangeSetup::for_socket($crate::support::wire::SocketExchangeSetup { api_key: setup.api_key, version: setup.version, correlation: $crate::support::wire::CorrelationId($correlation), header: $crate::support::wire::HeaderEncoding::Flexible }, $client_id), body).await
        }
    };
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub struct CorrelationId(pub i32);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HeaderEncoding {
    #[default]
    Legacy,
    Flexible,
}

impl HeaderEncoding {
    /// Adapt a protocol version's flexible-header flag at the codec boundary.
    pub fn from_wire(flexible: bool) -> Self {
        if flexible {
            Self::Flexible
        } else {
            Self::Legacy
        }
    }
}

/// The manually framed clients reserve a fixed header allowance plus the body.
pub fn request_body_capacity(body: &[u8]) -> ByteSize {
    use krabka_units::convert::ByteSizeExt;
    ByteSize::from_bytes(u64::try_from(16 + body.len()).expect("frame capacity fits u64"))
}

/// Encode a request header followed by its body, retaining the original allocation policy.
///
/// # Panics
/// Panics with the caller's diagnostic if its client ID does not fit the header's i16 length.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct WireFrameSetup<'a> {
    #[default(krabka_ids::ApiKey(18))]
    pub api_key: krabka_ids::ApiKey,
    pub version: krabka_ids::ApiVersion,
    #[default(CorrelationId(1))]
    pub correlation: CorrelationId,
    pub header: HeaderEncoding,
    #[default("test-client")]
    pub client_id: &'a str,
    pub body: &'a [u8],
    pub capacity: Option<ByteSize>,
    pub length_context: Option<&'a str>,
}

pub fn request_frame(setup: WireFrameSetup<'_>) -> bytes::BytesMut {
    use bytes::BufMut;
    use krabka_units::convert::ByteSizeExt;
    let WireFrameSetup {
        api_key,
        version,
        correlation,
        header,
        client_id,
        body,
        capacity,
        length_context,
    } = setup;
    let mut frame = capacity.map_or_else(bytes::BytesMut::new, |capacity| {
        bytes::BytesMut::with_capacity(capacity.bytes_usize())
    });
    frame.put_i16(api_key.0);
    frame.put_i16(version.0);
    frame.put_i32(correlation.0);
    let length = i16::try_from(client_id.len());
    frame.put_i16(match length_context {
        Some(context) => length.expect(context),
        None => length.unwrap(),
    });
    frame.put_slice(client_id.as_bytes());
    if header == HeaderEncoding::Flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);
    frame
}

/// Decode a manually framed response with the caller's header and error policies.
///
/// # Errors
/// Returns the protocol decode error with the original diagnostic prefix.
///
/// # Panics
/// Panics if the frame lacks the correlation ID or its requested tagged byte.
pub fn decode_response_frame<R>(
    frame: &[u8],
    version: i16,
    flexible_header: bool,
    context: &str,
) -> io::Result<R>
where
    R: for<'de> krabka_protocol::Decode<'de>,
{
    use bytes::Buf;
    let mut body = frame;
    let _correlation = body.get_i32();
    if flexible_header {
        let _tagged = body.get_u8();
    }
    R::decode(&mut body, version).map_err(|error| io::Error::other(format!("{context}: {error}")))
}

/// Bind a typed request and response to their wire API and codec diagnostic.
#[macro_export]
macro_rules! plain_request_driver_fixture {
    ($vis:vis $name:ident {
        request: $request:ty,
        response: $response:ty,
        api: $api:expr,
        operation: $operation:literal $(,)?
    }) => {
        $vis async fn $name(
            addr: ::std::net::SocketAddr,
            request: $request,
            authentication: &$crate::kafka_wire::PlainAuthenticationSetup<'_>,
        ) -> ::std::io::Result<$response> {
            $crate::kafka_wire::request_as_plain(
                addr,
                &request,
                &$crate::kafka_wire::PlainRequestSetup {
                    authentication: authentication.clone(),
                    api: $api,
                    operation: $operation,
                },
            ).await
        }
    };
}
