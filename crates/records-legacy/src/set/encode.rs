//! Encoding a v0/v1 `MessageSet`, flat or wrapped in a single compressed
//! outer message.
//!
//! The two entry points are separate because the wire layouts are: a flat set
//! writes one outer message per record, while a compressed set writes exactly
//! one outer message whose value is a complete inner `MessageSet`. The
//! wrapper's own offset and timestamp, and whether the inner offsets are
//! absolute or relative, are the KIP-32 conventions that differ between v0
//! and v1.

use bytes::{BufMut, BytesMut};
use krabka_compression::CompressionType;

use super::ParsedRecord;
use crate::{
    error::LegacyRecordsError,
    message::{Magic, Message, attrs_with_compression},
};

fn message_for_record(record: ParsedRecord, magic: Magic) -> Message {
    Message {
        magic,
        attributes: 0,
        timestamp: match magic {
            Magic::V0 => None,
            Magic::V1 => Some(record.timestamp.unwrap_or(-1)),
        },
        key: record.key,
        value: record.value,
    }
}

/// Encode a flat `MessageSet` of magic `magic` into `buf`.
///
/// This function writes one outer message per record. Use it to emit an
/// uncompressed batch.
pub fn encode_flat_message_set<B: BufMut, I: IntoIterator<Item = ParsedRecord>>(
    records: I,
    magic: Magic,
    buf: &mut B,
) {
    for r in records {
        let offset = r.offset;
        let msg = message_for_record(r, magic);
        let msg_len = msg.encoded_len();
        buf.put_i64(offset.0);
        // Safe: legacy messages are well-bounded; capping at i32::MAX is
        // sufficient for any realistic batch.
        buf.put_i32(i32::try_from(msg_len).unwrap_or(i32::MAX));
        msg.encode_into(buf);
    }
}

/// Encode a `MessageSet` wrapped in a single compressed outer message.
///
/// The inner set is uncompressed and contains one message per record. It
/// follows the KIP-32 conventions: v1 inner offsets are relative,
/// `0..N-1`.
/// # Errors
/// Returns `Malformed` if `codec` is `CompressionType::Zstd`, which v0/v1 cannot represent.
/// Returns a compression error if the codec cannot compress the inner set.
pub fn encode_compressed_message_set<B: BufMut>(
    records: &[ParsedRecord],
    magic: Magic,
    codec: CompressionType,
    buf: &mut B,
) -> Result<(), LegacyRecordsError> {
    assert2::assert!(
        codec != CompressionType::None,
        "use encode_flat_message_set for uncompressed"
    );
    if matches!(codec, CompressionType::Zstd) {
        return Err(LegacyRecordsError::Malformed(
            "zstd compression not representable in v0/v1".into(),
        ));
    }
    if records.is_empty() {
        // Nothing to wrap. Encode as a zero-message wrapper would be
        // ambiguous; emit nothing instead.
        return Ok(());
    }

    // Build inner uncompressed MessageSet.
    let mut inner = BytesMut::new();
    let count = i64::try_from(records.len()).unwrap_or(i64::MAX);
    for (i, r) in records.iter().enumerate() {
        let inner_offset = match magic {
            Magic::V0 => r.offset.0,
            // v1: relative 0..count-1
            Magic::V1 => i64::try_from(i).unwrap_or(i64::MAX),
        };
        let msg = message_for_record(r.clone(), magic);
        let msg_len = msg.encoded_len();
        inner.put_i64(inner_offset);
        inner.put_i32(i32::try_from(msg_len).unwrap_or(i32::MAX));
        msg.encode_into(&mut inner);
    }

    // Compress.
    let compressed = krabka_compression::compress(codec, &inner)?;

    // Wrapper message.
    let wrapper_attributes = attrs_with_compression(0, codec);
    let wrapper_timestamp = match magic {
        Magic::V0 => None,
        Magic::V1 => Some(
            records
                .iter()
                .filter_map(|r| r.timestamp)
                .max()
                .unwrap_or(-1),
        ),
    };
    let wrapper = Message {
        magic,
        attributes: wrapper_attributes,
        timestamp: wrapper_timestamp,
        key: None,
        value: Some(compressed),
    };
    let wrapper_len = wrapper.encoded_len();

    // Wrapper offset: v0 = 0 (per Kafka convention pre-KIP-32),
    // v1 = absolute offset of last inner record.
    let wrapper_offset = match magic {
        Magic::V0 => 0,
        Magic::V1 => records[records.len() - 1].offset.0,
    };
    buf.put_i64(wrapper_offset);
    buf.put_i32(i32::try_from(wrapper_len).unwrap_or(i32::MAX));
    wrapper.encode_into(buf);
    let _ = count;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use krabka_ids::Offset;

    use super::*;
    use crate::set::{
        decode_message_set,
        test_support::{compressed_bytes, decode_bytes, sample_records_v0, sample_records_v1},
    };

    #[test]
    fn message_set_roundtrips() {
        for (_name, magic, codec) in [
            ("flat v0", Magic::V0, None),
            ("flat v1", Magic::V1, None),
            ("gzip v0", Magic::V0, Some(CompressionType::Gzip)),
            ("gzip v1", Magic::V1, Some(CompressionType::Gzip)),
            ("snappy v1", Magic::V1, Some(CompressionType::Snappy)),
        ] {
            let records = match magic {
                Magic::V0 => sample_records_v0(),
                Magic::V1 => sample_records_v1(),
            };
            let mut buffer = BytesMut::new();
            if let Some(codec) = codec {
                encode_compressed_message_set(&records, magic, codec, &mut buffer).unwrap();
            } else {
                encode_flat_message_set(records.clone(), magic, &mut buffer);
            }

            let decoded = decode_message_set(&mut &buffer[..], buffer.len()).unwrap();
            assert2::assert!(decoded == records);
        }
    }

    #[test]
    fn flat_v1_missing_timestamp_encodes_minus_one() {
        let recs = vec![ParsedRecord {
            offset: Offset(7),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let mut buf = BytesMut::new();
        encode_flat_message_set(recs, Magic::V1, &mut buf);
        let decoded = decode_bytes(&buf);
        assert2::assert!(
            decoded
                == vec![ParsedRecord {
                    offset: Offset(7),
                    timestamp: Some(-1),
                    key: None,
                    value: Some(Bytes::from_static(b"v")),
                }]
        );
    }

    #[test]
    fn compressed_v1_missing_timestamps_default_to_minus_one() {
        // Records with no timestamps: inner messages encode ts = -1, and the
        // wrapper's own timestamp (max over records, none present) is -1.
        let recs = vec![ParsedRecord {
            offset: Offset(9),
            timestamp: None,
            key: None,
            value: Some(Bytes::from_static(b"v")),
        }];
        let buf = compressed_bytes(&recs, Magic::V1, CompressionType::Gzip);

        // Inspect the raw wrapper message's own timestamp before unwrapping.
        let mut cur: &[u8] = &buf[..];
        let _wrapper_offset = cur.get_i64();
        let wrapper_size = usize::try_from(cur.get_i32()).unwrap();
        let wrapper = Message::decode_from(&mut cur, wrapper_size).unwrap();
        // The inner record's timestamp survives the unwrap as -1.
        let decoded = decode_bytes(&buf);
        assert2::assert!(wrapper.timestamp == Some(-1));
        assert2::assert!(decoded[0].timestamp == Some(-1));
    }
}
