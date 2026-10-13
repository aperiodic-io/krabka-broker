//! The sample record sets both halves of the codec's tests round-trip.
//!
//! `message_set_roundtrips` encodes them and `rejects_nested_compression`
//! decodes a wrapper built from them, so the fixtures sit beside the two
//! modules rather than inside either one.

use {super::ParsedRecord, crate::Magic, bytes::Bytes, krabka_ids::Offset};

pub(super) fn sample_records_v1() -> Vec<ParsedRecord> {
    sample_records_v1_at(100, [1_700_000_000, 1_700_000_010, 1_700_000_020])
}

pub(crate) fn sample_records_v1_at(base: i64, timestamps: [i64; 3]) -> Vec<ParsedRecord> {
    [
        (0, Some(b"a"), b"1"),
        (1, Some(b"b"), b"2"),
        (2, None, b"3"),
    ]
    .into_iter()
    .zip(timestamps)
    .map(|((delta, key, value), timestamp)| ParsedRecord {
        offset: Offset(base + delta),
        timestamp: Some(timestamp),
        key: key.map(|key| Bytes::from_static(key)),
        value: Some(Bytes::from_static(value)),
    })
    .collect()
}

pub(crate) fn decode_bytes(wire: &[u8]) -> Vec<ParsedRecord> {
    let mut cursor = wire;
    super::decode_message_set(&mut cursor, wire.len()).unwrap()
}

pub(super) fn compressed_bytes(
    records: &[ParsedRecord],
    magic: Magic,
    codec: krabka_compression::CompressionType,
) -> bytes::BytesMut {
    let mut wire = bytes::BytesMut::new();
    super::encode_compressed_message_set(records, magic, codec, &mut wire).unwrap();
    wire
}

pub(super) fn sample_records_v0() -> Vec<ParsedRecord> {
    sample_records_v1()
        .into_iter()
        .map(|r| ParsedRecord {
            timestamp: None,
            ..r
        })
        .collect()
}
