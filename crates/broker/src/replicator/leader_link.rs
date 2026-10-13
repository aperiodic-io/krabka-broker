//! The follower's own connection to its leader.
//!
//! [`krabka_client_core::Connection`] multiplexes. A writer task takes each
//! request from a channel, and a reader task matches each response to its
//! caller through a oneshot and copies the frame out of its read buffer to hand
//! it over. A follower's fetch loop has one request in flight at a time and
//! nothing to multiplex. On its path those tasks cost two task hand-offs per
//! round trip and a second copy of every replicated byte, which at saturation
//! was about 4% of a broker's CPU.
//!
//! A [`LeaderLink`] writes the request and reads the response on the fetch
//! loop's own task, into the buffer it then returns, so a fetch response's
//! records are slices of the bytes the socket read.

use {
    bytes::{Buf, BufMut, Bytes, BytesMut},
    krabka_client_core::{ApiVersionTable, ClientError},
    krabka_protocol::{
        Decode, Encode, ProtocolError, ProtocolRequest,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
        tagged_fields::read_tagged_fields,
    },
    krabka_units::{Time, convert::TimeExt},
    tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
};

/// Kafka's `UNSUPPORTED_VERSION` error code.
const UNSUPPORTED_VERSION: i16 = 35;

/// The KIP-511 software name a follower's `ApiVersions` sends.
const CLIENT_SOFTWARE_NAME: &str = "krabka-broker";

/// What a [`LeaderLink`] needs besides its stream.
pub(super) struct LinkSettings {
    /// The request header's `client_id`.
    pub(super) client_id: String,
    /// The largest response frame the link reads, as `ClientFrameMax`
    /// bounds a [`krabka_client_core::Connection`].
    pub(super) frame_max: usize,
    /// How long one request may wait for its response.
    pub(super) request_timeout: Time,
}

/// See the module documentation.
pub(super) struct LeaderLink<S> {
    stream: S,
    versions: ApiVersionTable,
    settings: LinkSettings,
    next_correlation: i32,
}

impl<S: AsyncRead + AsyncWrite + Unpin> LeaderLink<S> {
    /// Negotiates API versions over `stream`, which TLS and SASL have already
    /// secured if the listener asks for them.
    ///
    /// # Errors
    ///
    /// Returns the transport, codec or server error that ended the
    /// `ApiVersions` exchange.
    pub(super) async fn open(stream: S, settings: LinkSettings) -> Result<Self, ClientError> {
        let mut link = Self {
            stream,
            versions: ApiVersionTable::default(),
            settings,
            next_correlation: 0,
        };
        link.versions = link.api_versions().await?;
        Ok(link)
    }

    /// Sends `request` at the highest version both sides speak and decodes
    /// the response from the frame bytes, so a [`Decode`] that takes
    /// `Bytes` slices out of its buffer copies nothing.
    ///
    /// # Errors
    ///
    /// Returns a version, transport, timeout or codec error. After any error
    /// the stream is in an unknown state and the link must be dropped.
    pub(super) async fn send<R: ProtocolRequest>(
        &mut self,
        request: R,
    ) -> Result<R::Response, ClientError> {
        let version = self.versions.negotiate::<R>()?;
        let flexible = version >= R::FLEXIBLE_MIN;
        let mut body = self
            .exchange(R::API_KEY, version, flexible, |frame| {
                request.encode(frame, version)
            })
            .await?;
        // ResponseHeader v1, which every flexible response but ApiVersions
        // uses, carries tagged fields after the correlation id.
        if flexible {
            read_tagged_fields(&mut body, |_, _| Ok(false))?;
        }
        Ok(R::Response::decode(&mut body, version)?)
    }

    /// Kafka's `ApiVersions` negotiation: the highest version first, and on
    /// `UNSUPPORTED_VERSION` the highest version the leader lists.
    async fn api_versions(&mut self) -> Result<ApiVersionTable, ClientError> {
        let request = ApiVersionsRequest {
            client_software_name: CLIENT_SOFTWARE_NAME.to_owned(),
            client_software_version: env!("CARGO_PKG_VERSION").to_owned(),
            ..ApiVersionsRequest::default()
        };
        let mut version = ApiVersionsRequest::MAX_VERSION;
        loop {
            let flexible = version >= ApiVersionsRequest::FLEXIBLE_MIN;
            let body = self
                .exchange(ApiVersionsRequest::API_KEY, version, flexible, |frame| {
                    request.encode(frame, version)
                })
                .await?;
            // ApiVersions answers with ResponseHeader v0 at every version.
            let response = decode_api_versions(&body, version)?;
            if response.error_code == UNSUPPORTED_VERSION && version > 0 {
                version = response
                    .api_keys
                    .iter()
                    .find(|key| key.api_key == ApiVersionsRequest::API_KEY)
                    .map_or(0, |key| key.max_version)
                    .min(version - 1)
                    .max(0);
                continue;
            }
            if response.error_code != 0 {
                return Err(ClientError::Server {
                    error_code: response.error_code,
                });
            }
            return Ok(ApiVersionTable::from_response(&response));
        }
    }

    /// Writes one request and returns its response frame after the
    /// correlation id, within the request timeout.
    async fn exchange(
        &mut self,
        api_key: i16,
        version: i16,
        flexible_header: bool,
        body: impl FnOnce(&mut BytesMut) -> Result<(), ProtocolError>,
    ) -> Result<Bytes, ClientError> {
        let correlation = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);
        let frame = request_frame(
            RequestHeader {
                api_key,
                version,
                correlation,
                client_id: &self.settings.client_id,
                flexible: flexible_header,
            },
            body,
        )?;
        let timeout = self.settings.request_timeout;
        tokio::time::timeout(timeout.to_std(), self.round_trip(&frame, correlation))
            .await
            .map_err(|_| ClientError::Timeout(timeout))?
    }

    async fn round_trip(&mut self, frame: &[u8], correlation: i32) -> Result<Bytes, ClientError> {
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        let mut length = [0_u8; 4];
        self.stream
            .read_exact(&mut length)
            .await
            .map_err(disconnected_at_eof)?;
        let length = usize::try_from(i32::from_be_bytes(length))
            .ok()
            .filter(|&length| (4..=self.settings.frame_max).contains(&length))
            .ok_or_else(|| {
                invalid(format!(
                    "response frame of {} bytes is outside 4..={}",
                    i32::from_be_bytes(length),
                    self.settings.frame_max
                ))
            })?;
        let mut response = BytesMut::with_capacity(length);
        while response.len() < length {
            let wanted = u64::try_from(length - response.len()).unwrap_or(u64::MAX);
            if (&mut self.stream)
                .take(wanted)
                .read_buf(&mut response)
                .await?
                == 0
            {
                return Err(ClientError::Disconnected);
            }
        }
        let mut response = response.freeze();
        let answered = response.get_i32();
        if answered != correlation {
            return Err(invalid(format!(
                "response carries correlation id {answered}, expected {correlation}"
            )));
        }
        Ok(response)
    }
}

/// The request header fields a frame starts with.
#[derive(Clone, Copy)]
struct RequestHeader<'a> {
    api_key: i16,
    version: i16,
    correlation: i32,
    client_id: &'a str,
    /// Whether the header is v2, with a tagged-fields byte, as a flexible
    /// request's is.
    flexible: bool,
}

/// A size-prefixed request frame: the header, then what `body` encodes.
fn request_frame(
    header: RequestHeader<'_>,
    body: impl FnOnce(&mut BytesMut) -> Result<(), ProtocolError>,
) -> Result<BytesMut, ClientError> {
    let mut frame = BytesMut::with_capacity(64);
    frame.put_i32(0);
    frame.put_i16(header.api_key);
    frame.put_i16(header.version);
    frame.put_i32(header.correlation);
    let client_id_length = i16::try_from(header.client_id.len())
        .map_err(|_| ClientError::InvalidConfig("client_id is longer than i16::MAX".into()))?;
    frame.put_i16(client_id_length);
    frame.put_slice(header.client_id.as_bytes());
    if header.flexible {
        frame.put_u8(0);
    }
    body(&mut frame)?;
    let length = i32::try_from(frame.len() - 4)
        .map_err(|_| ClientError::InvalidArgument("request frame exceeds i32::MAX".into()))?;
    frame[..4].copy_from_slice(&length.to_be_bytes());
    Ok(frame)
}

/// Decodes an `ApiVersions` response sent for a request at `version`, falling
/// back to version 0 as Kafka's `ApiVersionsResponse.parse` does: a leader that
/// does not speak `version` answers at version 0.
fn decode_api_versions(body: &[u8], version: i16) -> Result<ApiVersionsResponse, ClientError> {
    let decode = |version| -> Result<ApiVersionsResponse, ClientError> {
        let mut cursor = body;
        let response = ApiVersionsResponse::decode(&mut cursor, version)?;
        if cursor.is_empty() {
            Ok(response)
        } else {
            Err(invalid(format!(
                "ApiVersions response v{version} has {} bytes after its end",
                cursor.len()
            )))
        }
    };
    match decode(version) {
        Err(_) if version != 0 => decode(0),
        decoded => decoded,
    }
}

fn disconnected_at_eof(error: std::io::Error) -> ClientError {
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        ClientError::Disconnected
    } else {
        ClientError::Io(error)
    }
}

fn invalid(message: String) -> ClientError {
    ClientError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

#[cfg(test)]
mod tests;
