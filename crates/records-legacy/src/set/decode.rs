//! Decoding a v0/v1 `MessageSet`: walking the `(offset, size, message)`
//! frames, and unwrapping the single compression layer the format allows.
//!
//! The recursion into a compressed wrapper is what makes this the larger half
//! of the codec. Unwrapping is where nested compression is rejected, where the
//! decompression policy is applied, and where the KIP-32 relative inner
//! offsets of a v1 wrapper are rewritten back to absolute ones.

use {
    super::ParsedRecord,
    crate::{
        error::LegacyRecordsError,
        message::{Magic, Message, compression_from_attrs},
    },
    bytes::Buf,
    krabka_compression::{CompressionType, RecordDecompressionPolicy},
    krabka_ids::Offset,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
};

/// The length of a wire slice as a byte count.
///
/// This function saturates and does not wrap. A `usize` above `u64::MAX`
/// cannot occur on any target Krabka builds for.
fn size_of_slice(slice: &[u8]) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(slice.len()).unwrap_or(u64::MAX))
}

/// Decode a flat, uncompressed `MessageSet` from `buf`.
///
/// This function consumes exactly `set_size_bytes` bytes from the buffer.
/// It unwraps a compressed wrapper message at the top level one time. It
/// rejects nested compression, that is, a compressed wrapper inside a
/// compressed wrapper.
/// # Errors
/// Returns `Truncated` if `buf` holds fewer than `set_size_bytes` bytes.
/// Returns `Truncated` if an entry header or an entry body stops early.
/// Returns `NegativeLength` if an entry carries a negative `message_size`.
/// Returns `NestedCompression` if a compressed wrapper holds another compressed wrapper.
/// Returns the error from `Message::decode_from` for a malformed or corrupt message frame.
/// Returns a compression error if the wrapper value does not decompress within the default policy.
pub fn decode_message_set<B: Buf>(
    buf: &mut B,
    set_size_bytes: usize,
) -> Result<Vec<ParsedRecord>, LegacyRecordsError> {
    decode_message_set_with_policy(buf, set_size_bytes, RecordDecompressionPolicy::default())
}

/// Decode a legacy `MessageSet` with explicit decompression limits.
///
/// # Errors
///
/// Returns the legacy records error for malformed input, for truncated
/// input, for corrupt input, for nested compression, or for input above
/// the limits in `policy`.
pub fn decode_message_set_with_policy<B: Buf>(
    buf: &mut B,
    set_size_bytes: usize,
    policy: RecordDecompressionPolicy,
) -> Result<Vec<ParsedRecord>, LegacyRecordsError> {
    if buf.remaining() < set_size_bytes {
        return Err(LegacyRecordsError::Truncated {
            needed: set_size_bytes - buf.remaining(),
        });
    }
    let mut region = vec![0u8; set_size_bytes];
    buf.copy_to_slice(&mut region);
    let mut out = Vec::new();
    decode_into(
        &region, &mut out, /* allow_compression = */ true, policy,
    )?;
    Ok(out)
}

fn decode_into(
    bytes: &[u8],
    out: &mut Vec<ParsedRecord>,
    allow_compression: bool,
    policy: RecordDecompressionPolicy,
) -> Result<(), LegacyRecordsError> {
    let mut cur = bytes;
    while !cur.is_empty() {
        if cur.len() < 12 {
            return Err(LegacyRecordsError::Truncated {
                needed: 12 - cur.len(),
            });
        }
        let offset = cur.get_i64();
        let size = cur.get_i32();
        if size < 0 {
            return Err(LegacyRecordsError::NegativeLength {
                label: "message_size",
                len: size,
            });
        }
        let size = usize::try_from(size).expect("nonnegative i32 fits usize");
        if cur.remaining() < size {
            return Err(LegacyRecordsError::Truncated {
                needed: size - cur.remaining(),
            });
        }
        let msg = Message::decode_from(&mut cur, size)?;
        let codec = compression_from_attrs(msg.attributes)?;
        if codec == CompressionType::None {
            out.push(ParsedRecord {
                offset: Offset(offset),
                timestamp: msg.timestamp,
                key: msg.key,
                value: msg.value,
            });
        } else {
            if !allow_compression {
                return Err(LegacyRecordsError::NestedCompression);
            }
            let inner_compressed = msg.value.ok_or_else(|| {
                LegacyRecordsError::Malformed("compressed wrapper has null value".into())
            })?;
            // Bound decompressed output to guard against a decompression bomb
            // in a legacy compressed wrapper.
            let max_output = policy.output_limit(size_of_slice(&inner_compressed));
            let inner_bytes = krabka_compression::decompress(codec, &inner_compressed, max_output)?;

            // Parse the inner set (no nested compression allowed).
            let start_len = out.len();
            decode_into(&inner_bytes, out, false, policy)?;

            // KIP-32 anchors the last relative inner offset at the wrapper
            // offset. Retained records can have gaps after compaction.
            // An empty wrapper has no offsets to reconstruct.
            if matches!(msg.magic, Magic::V1)
                && let Some(last) = out[start_len..].last()
            {
                let base_abs = i128::from(offset) - i128::from(last.offset.0);
                for rec in &mut out[start_len..] {
                    let absolute = base_abs + i128::from(rec.offset.0);
                    rec.offset = Offset(i64::try_from(absolute).map_err(|_| {
                        LegacyRecordsError::Malformed("absolute inner offset exceeds i64".into())
                    })?);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            message::attrs_with_compression,
            set::{
                encode_compressed_message_set, encode_flat_message_set,
                test_support::{compressed_bytes, decode_bytes, sample_records_v1},
            },
        },
        bytes::{BufMut, Bytes, BytesMut},
        krabka_compression::CompressionError,
        krabka_units::prelude::{bytes, kibibytes},
    };

    fn record_at(offset: i64) -> ParsedRecord {
        ParsedRecord {
            offset: Offset(offset),
            timestamp: Some(10),
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }
    }

    fn v1_wrapper(inner_offsets: &[i64], wrapper_offset: i64) -> BytesMut {
        let inner: Vec<_> = inner_offsets.iter().copied().map(record_at).collect();
        let mut flat = BytesMut::new();
        encode_flat_message_set(inner, Magic::V1, &mut flat);
        let wrapper = Message {
            magic: Magic::V1,
            attributes: attrs_with_compression(0, CompressionType::Gzip),
            timestamp: Some(10),
            key: None,
            value: Some(krabka_compression::compress(CompressionType::Gzip, &flat).unwrap()),
        };
        let mut wire = BytesMut::new();
        wire.put_i64(wrapper_offset);
        wire.put_i32(i32::try_from(wrapper.encoded_len()).unwrap());
        wrapper.encode_into(&mut wire);
        wire
    }

    #[yare::parameterized(
        first_sparse_gap = { [0, 2], 102, [100, 102] },
        second_sparse_gap = { [3, 5], 102, [100, 102] },
        extreme_sparse_gap = { [i64::MIN, i64::MIN + 2], i64::MAX, [i64::MAX - 2, i64::MAX] },
    )]
    fn sparse_v1_wrapper_anchors_last_inner_offset_without_closing_gaps(
        inner: [i64; 2],
        outer: i64,
        absolute: [i64; 2],
    ) {
        // Construct the inner MessageSet independently of the compressed-set
        // encoder: Kafka retains relative gaps when compaction removes a row.
        let wire = v1_wrapper(&inner, outer);
        let decoded = decode_bytes(&wire);
        let expected: Vec<_> = absolute.into_iter().map(record_at).collect();
        assert2::assert!(decoded == expected);
    }

    #[test]
    fn v1_wrapper_rejects_unrepresentable_offsets() {
        let wire = v1_wrapper(&[0, 1], i64::MIN);
        assert2::assert!(matches!(
            decode_message_set(&mut &wire[..], wire.len()),
            Err(LegacyRecordsError::Malformed(_))
        ));
    }

    #[yare::parameterized(
        minimum = { i64::MIN },
        maximum = { i64::MAX },
    )]
    fn empty_v1_wrapper_accepts_extreme_outer_offsets(outer: i64) {
        let wire = v1_wrapper(&[], outer);
        assert2::assert!(decode_bytes(&wire).is_empty());
    }

    #[test]
    fn slice_length_lifts_to_a_byte_count() {
        assert2::check!(size_of_slice(&[]) == ByteSize::ZERO);
        assert2::check!(size_of_slice(&[0u8; 4096]) == kibibytes(4));
    }

    krabka_macros::legacy_policy_fixture!(policy_wire, crate);

    #[test]
    fn decompression_policy_limits_legacy_decode() {
        let wire = policy_wire();

        decode_message_set(&mut &wire[..], wire.len()).unwrap();

        let policy =
            RecordDecompressionPolicy::new(krabka_units::fraction(1.0), bytes(1), bytes(32))
                .unwrap();
        assert2::assert!(matches!(
            decode_message_set_with_policy(&mut &wire[..], wire.len(), policy),
            Err(LegacyRecordsError::Compression(
                CompressionError::TooLarge { limit: 32 }
            ))
        ));
    }

    #[test]
    fn rejects_nested_compression() {
        // Construct a wrapper containing a wrapper.
        let inner_recs = sample_records_v1();
        let mut inner_outer = BytesMut::new();
        encode_compressed_message_set(
            &inner_recs,
            Magic::V1,
            CompressionType::Gzip,
            &mut inner_outer,
        )
        .unwrap();

        // Now wrap that bytestream as the value of another compressed wrapper.
        let outer_compressed =
            krabka_compression::compress(CompressionType::Gzip, &inner_outer).unwrap();
        let outer_msg = Message {
            magic: Magic::V1,
            attributes: attrs_with_compression(0, CompressionType::Gzip),
            timestamp: Some(0),
            key: None,
            value: Some(outer_compressed),
        };
        let mut wire = BytesMut::new();
        let outer_len = outer_msg.encoded_len();
        wire.put_i64(0);
        wire.put_i32(i32::try_from(outer_len).unwrap());
        outer_msg.encode_into(&mut wire);

        let mut cur: &[u8] = &wire[..];
        let err = decode_message_set(&mut cur, wire.len()).unwrap_err();
        assert2::assert!(matches!(err, LegacyRecordsError::NestedCompression));
    }

    // --- mutation-coverage tests --------------------------------------------
    //
    // Round-trips above don't exercise the malformed/boundary paths or pin
    // exact framing. These do: precise `needed` counts, error-variant
    // boundaries, the decompression-cap floor, and the v1 inner-offset rewrite.
    //

    #[test]
    fn decode_message_set_short_buffer_reports_needed() {
        // 4 bytes available, caller claims a 12-byte set: needed = 12 - 4 = 8.
        let data = [0u8; 4];
        let mut cur: &[u8] = &data;
        let err = decode_message_set(&mut cur, 12).unwrap_err();
        assert2::assert!(matches!(err, LegacyRecordsError::Truncated { needed: 8 }));
    }

    #[test]
    fn entry_header_truncated_reports_needed() {
        // 8 bytes where a 12-byte (offset+size) entry header is required:
        // needed = 12 - 8 = 4.
        let data = [0u8; 8];
        let mut cur: &[u8] = &data;
        let err = decode_message_set(&mut cur, 8).unwrap_err();
        assert2::assert!(matches!(err, LegacyRecordsError::Truncated { needed: 4 }));
    }

    fn entry_error(size: i32, trailing: &[u8]) -> LegacyRecordsError {
        let mut data = BytesMut::new();
        data.put_i64(0);
        data.put_i32(size);
        data.put_slice(trailing);
        decode_message_set(&mut &data[..], data.len()).unwrap_err()
    }

    #[test]
    fn entry_zero_message_size_is_malformed() {
        // offset(8) + size(0): clears the `< 12` and `size < 0` guards, then
        // Message::decode_from rejects the 0-byte frame as Malformed (< 6).
        // Distinguishes the `<` boundaries from `<=`/`==`.
        let err = entry_error(0, &[]);
        assert2::assert!(matches!(err, LegacyRecordsError::Malformed(_)));
    }

    #[test]
    fn entry_negative_message_size_rejected() {
        let err = entry_error(-1, &[0u8; 4]); // keep region >= 12 bytes
        assert2::assert!(matches!(err, LegacyRecordsError::NegativeLength {
            label: "message_size",
            len: -1
        }));
    }

    #[test]
    fn entry_message_body_truncated_reports_needed() {
        // Entry claims a 10-byte message but only 2 bytes follow:
        // needed = 10 - 2 = 8.
        let err = entry_error(10, &[0u8; 2]);
        assert2::assert!(matches!(err, LegacyRecordsError::Truncated { needed: 8 }));
    }

    #[test]
    fn compressed_wrapper_allows_large_decompressed_output() {
        // The 16 MiB decompression-cap floor must let a ~2 MiB wrapper through
        // even though the compressed size is tiny. Shrinking the floor (a `*`
        // flip in `16 * 1024 * 1024`) would reject this round-trip as TooLarge.
        let big = vec![0x7Eu8; 2 * 1024 * 1024];
        let recs = vec![ParsedRecord {
            offset: Offset(0),
            timestamp: Some(5),
            key: None,
            value: Some(Bytes::from(big.clone())),
        }];
        let buf = compressed_bytes(&recs, Magic::V1, CompressionType::Gzip);
        let decoded = decode_bytes(&buf);
        assert2::assert!(decoded == recs);
    }

    #[test]
    fn v1_inner_offset_rewrite_leaves_preceding_flat_records_unchanged() {
        // A set with a flat record FOLLOWED by a compressed v1 wrapper: when the
        // wrapper is decoded, `out` already holds the flat record (start_len >
        // 0). Offset reconstruction applies only to the wrapper's inner set.
        let mut buf = BytesMut::new();
        let flat = ParsedRecord {
            offset: Offset(50),
            timestamp: Some(1),
            key: None,
            value: Some(Bytes::from_static(b"flat")),
        };
        encode_flat_message_set(vec![flat.clone()], Magic::V1, &mut buf);
        let inner = vec![
            ParsedRecord {
                offset: Offset(100),
                timestamp: Some(2),
                key: None,
                value: Some(Bytes::from_static(b"x")),
            },
            ParsedRecord {
                offset: Offset(101),
                timestamp: Some(3),
                key: None,
                value: Some(Bytes::from_static(b"y")),
            },
        ];
        encode_compressed_message_set(&inner, Magic::V1, CompressionType::Gzip, &mut buf).unwrap();

        let decoded = decode_bytes(&buf);
        let expected = std::iter::once(flat).chain(inner).collect::<Vec<_>>();
        assert2::assert!(decoded == expected);
    }
}
