//! The records resolvers that turn one `RecordsPayload` into the [`WriteOp`]
//! segments the drainer writes.
//!
//! [`resolve_records_inline`] is the portable path: it keeps every payload
//! kind in userspace bytes, and shares the `Bytes` view where the payload is
//! already verbatim wire bytes. [`resolve_records_sendfile`] instead keeps a
//! `FileRegions` payload file-backed, one op per contributing segment, for the
//! kernel `sendfile(2)` drain.

use {
    super::WriteOp, crate::error::BrokerError, bytes::BytesMut,
    krabka_protocol::records::RecordsPayload,
};

/// Portable (Increment C) records resolver: emit the records payload as a
/// single inline segment. For `RecordsPayload::Raw` this function hands the
/// verbatim `.log` `Bytes` to the socket directly. That is a refcounted view
/// and copies no records bytes. For parsed and legacy payloads it encodes them
/// into a fresh buffer, which is the rare non-passthrough path. For a
/// `FileRegions` payload, the TLS and non-Linux fallback, it `pread`s the
/// regions into one buffer.
///
/// # Errors
///
/// Returns [`BrokerError::Io`] only for the payload kinds this function
/// encodes rather than shares: a parsed batch whose re-encode fails, and a
/// file-backed payload whose positioned read fails, which is a segment file
/// that was truncated or unlinked under the reader. A `Raw` or `Legacy`
/// payload is a refcount bump and cannot fail.
pub fn resolve_records_inline(payload: &RecordsPayload) -> Result<Vec<WriteOp>, BrokerError> {
    let bytes = krabka_macros::records_payload_match! { match payload {
        // `Raw`/`Legacy` are already verbatim wire bytes — share the `Bytes`.
        RecordsPayload::Raw(b) | RecordsPayload::Legacy(b) => b.clone(),
        // Parsed batches must be encoded; rare on the fetch path.
        RecordsPayload::V2(_) => encode_payload(payload)?,
        RecordsPayload::FileRegions(_) => {
            // TLS / non-sendfile fallback for a FileRegions payload: pread into a
            // buffer (byte-identical to the sendfile'd region).
            encode_payload(payload)?
        }
    } };
    Ok(vec![WriteOp::Inline(bytes)])
}

fn encode_payload(payload: &RecordsPayload) -> Result<bytes::Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(payload.payload_len());
    payload
        .encode_to(&mut buf)
        .map_err(|error| BrokerError::Io(std::io::Error::other(error.to_string())))?;
    Ok(buf.freeze())
}

/// Plaintext-sendfile (Increments D + E) records resolver: emit each
/// `FileRegion` of a `FileRegions` payload as its own [`WriteOp::File`],
/// one per contributing segment, for the kernel `sendfile` drain. Every
/// other payload kind defers to [`resolve_records_inline`], as does a
/// `FileRegions` payload that arrives here on a non-sendfile path. This
/// function compiles on the SENDFILE alias (Linux + Apple +
/// FreeBSD/DragonFly).
///
/// # Errors
///
/// A `FileRegions` payload cannot fail here: the regions are cloned, not
/// read. Every other payload kind carries [`resolve_records_inline`]'s
/// error conditions, because it is what serves them.
#[krabka_macros::sendfile_cfg]
pub fn resolve_records_sendfile(payload: &RecordsPayload) -> Result<Vec<WriteOp>, BrokerError> {
    match payload {
        RecordsPayload::FileRegions(regions) => {
            Ok(regions.iter().cloned().map(WriteOp::File).collect())
        }
        _ => resolve_records_inline(payload),
    }
}

// ─── Increment D/E (cross-platform sendfile) tests ────────────────────
// Compiled on every SENDFILE-alias platform (Linux + Apple + FreeBSD/
// DragonFly). The plan-shape + pread-fallback tests are pure userspace and
// portable.
#[cfg(test)]
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
mod tests {
    use {
        super::*,
        crate::network::fetch_writer::{
            build_fetch_plan,
            test_support::{file_payload, raw_batch},
        },
        krabka_protocol::owned::fetch_response::{
            FetchResponse, FetchableTopicResponse, PartitionData,
        },
    };

    /// The Increment-D wire invariant: a `FileRegions` payload through the
    /// sendfile resolver produces the SAME framed wire bytes as the
    /// equivalent `Raw` payload through the inline resolver. Only the op
    /// kinds differ, File against Inline. The records bytes the broker
    /// emits are identical for both the sendfile path and the copy path.
    #[test]
    fn sendfile_plan_wire_bytes_equal_raw_plan() {
        for version in [4i16, 11, 12, 18] {
            // Records bytes large enough to be realistic.
            let records = {
                let mut b = BytesMut::new();
                b.extend_from_slice(&raw_batch(0));
                b.extend_from_slice(&raw_batch(1));
                b.freeze()
            };
            let (_tf, file_payload) = file_payload(&records);

            let raw_resp = FetchResponse {
                session_id: 1,
                responses: vec![FetchableTopicResponse {
                    topic: if version <= 12 {
                        "t".into()
                    } else {
                        String::new()
                    },
                    partitions: vec![PartitionData {
                        partition_index: 0,
                        high_watermark: 2,
                        last_stable_offset: 2,
                        log_start_offset: 0,
                        records: Some(RecordsPayload::Raw(records.clone())),
                        ..PartitionData::default()
                    }],
                    ..FetchableTopicResponse::default()
                }],
                ..FetchResponse::default()
            };
            let mut file_resp = raw_resp.clone();
            file_resp.responses[0].partitions[0].records = Some(file_payload);

            let raw_ops =
                build_fetch_plan(&raw_resp, version, 9, version >= 12, resolve_records_inline)
                    .unwrap();
            let file_ops = build_fetch_plan(
                &file_resp,
                version,
                9,
                version >= 12,
                resolve_records_sendfile,
            )
            .unwrap();

            // The file plan must actually contain a File op (zero-copy).
            assert2::assert!(
                file_ops.iter().any(|o| matches!(o, WriteOp::File(_))),
                "sendfile resolver must emit a File op at v{version}"
            );

            // Resolve both plans to bytes (pread the file ops) and compare.
            let raw_bytes = resolve_ops_to_bytes(&raw_ops);
            let file_bytes = resolve_ops_to_bytes(&file_ops);
            assert2::assert!(
                (raw_bytes) == (file_bytes),
                "sendfile plan wire bytes must equal raw plan at v{version}"
            );
        }
    }

    krabka_macros::file_region_fixture!(region_bytes);

    /// Resolve a plan to bytes and read File ops out of their backing
    /// file. This mirrors what the sendfile drain transmits and what the
    /// TLS pread fallback copies.
    fn resolve_ops_to_bytes(ops: &[WriteOp]) -> Vec<u8> {
        let mut out = Vec::new();
        for op in ops {
            match op {
                WriteOp::Inline(b) => out.extend_from_slice(b),
                WriteOp::File(region) => out.extend_from_slice(&region_bytes(region)),
            }
        }
        out
    }

    /// The TLS / non-sendfile fallback: `resolve_records_inline` on a
    /// `FileRegions` payload preads the regions into one inline op whose
    /// bytes equal the file contents.
    #[test]
    fn inline_fallback_preads_file_regions() {
        let records = {
            let mut b = BytesMut::new();
            b.extend_from_slice(&raw_batch(3));
            b.extend_from_slice(&raw_batch(4));
            b.freeze()
        };
        let (_tf, payload) = file_payload(&records);
        let ops = resolve_records_inline(&payload).unwrap();
        assert2::assert!((ops.len()) == (1));
        let WriteOp::Inline(ref b) = ops[0] else {
            panic!("fallback must produce an inline op");
        };
        assert2::assert!((&b[..]) == (&records[..]));
    }
}
