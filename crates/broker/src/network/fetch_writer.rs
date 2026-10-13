//! Zero-copy fetch response writer (Increments C + D).
//!
//! The generic dispatch loop writes every response with
//! `Framed<S, KafkaCodec>::send`, which copies the whole body into
//! the codec's write buffer. `encode_response` already copied that body once
//! to prepend the correlation header. For a 100 KB+ fetch that is hundreds of
//! KB of avoidable `memcpy` per request.
//!
//! This module replaces that path **for Fetch responses only** with an ordered
//! [`WriteOp`] plan:
//!
//! * **Increment C (portable, TLS-safe):** the writer writes the response
//!   header and the envelope metadata inline from userspace, and hands each
//!   partition's records region to the socket as its own segment with a
//!   vectored `write_all`. It does not copy the records bytes through the
//!   codec.
//! * **Increment D (Linux plaintext only):** for large records runs on a
//!   plaintext `TcpStream`, the records region becomes a [`WriteOp::File`]
//!   backed by the segment `.log` fd. The kernel `sendfile(2)` zero-copy path
//!   drains it from the page cache to the NIC and never through userspace. On
//!   TLS, on non-Linux, and for small runs, the writer falls back to the
//!   vectored/`pread` path of Increment C. The wire bytes are identical.
//!
//! ## Framing
//!
//! Kafka frames every response with a 4-byte big-endian length prefix. The
//! length is **not** part of any records or file bytes, so the writer computes
//! it up front from the exact body length (`correlation header + Σ op
//! lengths`) and writes it from userspace before it drains the ops. The writer
//! knows the exact body length without materializing the body: the records and
//! file ops carry their own length, and the inline ops are already-built
//! `Bytes`.
//!
//! ## Layout
//!
//! The plan for the response body comes from the `body_plan` child, which
//! encodes the metadata and leaves each records field as its own op. The
//! `resolve` child turns one such records op into the segments the writer
//! emits, `sink` decides what a given stream can take, and `drain` writes the
//! finished plan out with `sendfile` for the file-backed ops.

use {
    self::body_plan::{FetchWriteOp, fetch_response_write_plan},
    crate::{
        error::BrokerError,
        network::{codec, response_header_len, response_header_v1},
    },
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::{
        api_key::ApiKey, owned::fetch_response::FetchResponse, records::RecordsPayload,
    },
};

mod body_plan;
mod drain;
mod resolve;
mod sendfile;
mod sink;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::body_plan::records_to_serve;
pub use self::{drain::write_fetch_plan, resolve::resolve_records_inline, sink::SendfileSink};

crate::sendfile_cfg! {
    pub use self::resolve::resolve_records_sendfile;
}

/// One ordered segment of the fetch response wire frame.
#[derive(Debug)]
pub enum WriteOp {
    /// Userspace bytes: the length prefix and correlation header, partition
    /// metadata, records length prefixes, and tagged-field trailers. On the
    /// vectored path of Increment C it also holds the resolved records bytes.
    Inline(Bytes),
    /// A records region backed by a segment `.log` file (Increments D + E).
    /// `sendfile(2)` drains it on a plaintext `TcpStream` (Linux + Apple +
    /// FreeBSD/DragonFly). Every other stream uses a buffered `pread` +
    /// `write_all` fallback.
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    File(krabka_protocol::records::FileRegion),
}

impl WriteOp {
    /// Byte length this op contributes to the frame body. The frame-length
    /// accounting in tests uses it.
    ///
    /// It is not called `len`: an op is not a container, and a `len` on a
    /// publicly reachable type owes callers an `is_empty` that would mean
    /// nothing here.
    #[must_use]
    #[cfg(test)]
    pub fn body_len(&self) -> usize {
        match self {
            Self::Inline(b) => b.len(),
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Self::File(r) => r.len,
        }
    }
}

/// Build the ordered [`WriteOp`] plan for a v4+ fetch response. The first
/// inline op holds the leading frame-length prefix and the correlation header.
///
/// Byte-exactness: the concatenation of every op's bytes equals exactly what
/// `encode_response(api_key=1, correlation_id, body_flexible,
/// &encode_fetch_response(resp))` would produce, because:
///   * the frame length == 4-byte big-endian `u32` of `(header_len + body_len)`,
///   * the header == `correlation_id`, plus an empty tagged byte iff `body_flexible`,
///   * the body ops come straight from [`FetchResponse::write_plan`], whose
///     concatenation is byte-identical to `FetchResponse::encode`. The
///     protocol-crate golden tests prove that.
///
/// `resolve_records` decides how the writer emits each records segment. For
/// the portable C path see [`resolve_records_inline`]. Increment D supplies a
/// resolver that emits `WriteOp::File` on Linux plaintext.
pub fn build_fetch_plan<F>(
    resp: &FetchResponse,
    version: i16,
    correlation_id: i32,
    body_flexible: bool,
    mut resolve_records: F,
) -> Result<Vec<WriteOp>, BrokerError>
where
    F: FnMut(&RecordsPayload) -> Result<Vec<WriteOp>, BrokerError>,
{
    assert2::assert!(
        version >= 4,
        "build_fetch_plan requires the canonical v4+ codec"
    );
    // The response header is v1 (a trailing empty tagged-fields byte) iff the
    // body is flexible. The `encode_response` exception for ApiVersions
    // (api_key 18) never applies here — this is always Fetch (api_key 1).
    let header_v1 = response_header_v1(ApiKey::Fetch as i16, body_flexible);
    let header_len = response_header_len(ApiKey::Fetch as i16, body_flexible);

    let proto_plan = fetch_response_write_plan(resp, version)?;
    let body_len: usize = proto_plan.iter().map(FetchWriteOp::len).sum();
    let frame_body_len = header_len + body_len;
    let size_prefix = codec::response_frame_length(frame_body_len)?;

    let mut ops: Vec<WriteOp> = Vec::with_capacity(proto_plan.len() + 1);

    // First inline op: 4-byte frame length + correlation header.
    let mut head = BytesMut::with_capacity(4 + header_len);
    head.put_u32(size_prefix);
    head.put_i32(correlation_id);
    if header_v1 {
        head.put_u8(0); // empty response-header tagged fields
    }
    ops.push(WriteOp::Inline(head.freeze()));

    for op in proto_plan {
        match op {
            FetchWriteOp::Inline(b) => ops.push(WriteOp::Inline(b)),
            FetchWriteOp::Records(payload) => {
                ops.extend(resolve_records(&payload)?);
            }
        }
    }
    Ok(ops)
}
