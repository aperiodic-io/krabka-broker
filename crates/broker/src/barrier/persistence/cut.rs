//! The cut record: the published marker offsets of one epoch, and the
//! partitions the injection failed to reach.
//!
//! This is the record that `krabka-streams-java` and `krabka-streams-go` decode
//! by hand, so the golden vector that pins the bytes lives beside the codec.

use krabka_ids::PartitionIndex;
use krabka_log::Offset;

use super::primitives as codec;

/// Whether an injection reached every partition of its frozen target set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CutStatus {
    /// Every target partition carries the marker.
    Complete,
    /// At least one target partition carries no marker, and it never will.
    /// The `missing` list of the cut names those partitions.
    Partial,
}

impl CutStatus {
    /// The `i8` that this status writes into a cut value.
    pub(crate) const fn code(self) -> i8 {
        match self {
            Self::Complete => 0,
            Self::Partial => 1,
        }
    }
}

impl TryFrom<i8> for CutStatus {
    type Error = codec::ProtocolError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Complete),
            1 => Ok(Self::Partial),
            _ => Err(codec::ProtocolError::InvalidValue(
                "unknown barrier cut status",
            )),
        }
    }
}

/// One partition of a cut, and the offset its marker took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionOffset {
    pub(crate) partition: PartitionIndex,
    pub(crate) offset: Offset,
}

/// The marker offsets of one topic in a cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicOffsets {
    pub(crate) topic: String,
    pub(crate) partitions: Vec<PartitionOffset>,
}

/// A partition that an injection did not reach.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MissingPartition {
    pub(crate) topic: String,
    pub(crate) partition: PartitionIndex,
}

/// The published offsets of one epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CutValue {
    pub(crate) triggered_at: i64,
    pub(crate) completed_at: i64,
    pub(crate) status: CutStatus,
    pub(crate) topics: Vec<TopicOffsets>,
    /// The partitions that carry no marker for this epoch. It is empty when
    /// the status is [`CutStatus::Complete`].
    pub(crate) missing: Vec<MissingPartition>,
}

/// Encode a cut.
///
/// # Errors
/// Returns [`codec::BrokerError::Protocol`] when a topic name is longer than 32767
/// bytes, which the `i16` length cannot carry.
pub(crate) fn encode_cut(value: &CutValue) -> Result<Vec<u8>, codec::BrokerError> {
    let mut out = Vec::new();
    codec::put_i16(&mut out, codec::RECORD_VERSION);
    codec::put_i64(&mut out, value.triggered_at);
    codec::put_i64(&mut out, value.completed_at);
    codec::put_i8(&mut out, value.status.code());
    codec::put_array_len(&mut out, value.topics.len(), false);
    for topic in &value.topics {
        codec::put_string(&mut out, &topic.topic)?;
        codec::put_array_len(&mut out, topic.partitions.len(), false);
        for entry in &topic.partitions {
            codec::put_i32(&mut out, entry.partition.get());
            codec::put_i64(&mut out, entry.offset.0);
        }
    }
    codec::put_array_len(&mut out, value.missing.len(), false);
    for entry in &value.missing {
        codec::put_string(&mut out, &entry.topic)?;
        codec::put_i32(&mut out, entry.partition.get());
    }
    Ok(out)
}

/// Decode a cut.
///
/// # Errors
/// Returns a [`codec::ProtocolError`] when the value is truncated, carries a version
/// other than [`codec::RECORD_VERSION`], names an unknown status, holds a negative
/// array length, holds a non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_cut(bytes: &[u8]) -> Result<CutValue, codec::ProtocolError> {
    let mut cur = bytes;
    codec::expect_version(&mut cur)?;
    let triggered_at = codec::get_i64(&mut cur)?;
    let completed_at = codec::get_i64(&mut cur)?;
    let status = CutStatus::try_from(codec::get_i8(&mut cur)?)?;
    let topics = codec::decode_vec(&mut cur, |c| {
        let topic = codec::get_string_owned(c)?;
        let partitions = codec::decode_vec(c, |p| {
            let partition = PartitionIndex(codec::get_i32(p)?);
            let offset = Offset(codec::get_i64(p)?);
            Ok(PartitionOffset { partition, offset })
        })?;
        Ok(TopicOffsets { topic, partitions })
    })?;
    let missing = codec::decode_vec(&mut cur, |c| {
        let topic = codec::get_string_owned(c)?;
        let partition = PartitionIndex(codec::get_i32(c)?);
        Ok(MissingPartition { topic, partition })
    })?;
    codec::expect_end(cur)?;
    Ok(CutValue {
        triggered_at,
        completed_at,
        status,
        topics,
        missing,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        barrier::persistence::{RecordKey, decode_key, encode_key, test_support::sample_cut},
        coordinator::unified::persistence::MAX_STRING_BYTES,
    };

    #[test]
    fn a_cut_round_trips() {
        let value = sample_cut();
        assert!(decode_cut(&encode_cut(&value).expect("encodes")).ok() == Some(value));
    }

    #[test]
    fn a_complete_cut_names_no_missing_partition() {
        let value = CutValue {
            status: CutStatus::Complete,
            missing: Vec::new(),
            ..sample_cut()
        };
        let decoded = decode_cut(&encode_cut(&value).expect("encodes")).expect("decodes");
        assert!(decoded == value);
        assert!(decoded.missing.is_empty());
    }

    #[test]
    fn a_cut_with_no_topic_round_trips() {
        let value = CutValue {
            topics: Vec::new(),
            missing: Vec::new(),
            status: CutStatus::Complete,
            ..sample_cut()
        };
        assert!(decode_cut(&encode_cut(&value).expect("encodes")).ok() == Some(value));
    }

    #[test]
    fn a_cut_rejects_an_unknown_status() {
        let mut bytes = encode_cut(&sample_cut()).expect("encodes");
        // version i16, triggered_at i64, completed_at i64, then the status i8.
        bytes[18] = 9;
        assert!(decode_cut(&bytes).is_err());
    }

    #[test]
    fn every_decoder_rejects_a_truncated_record() {
        let cut = encode_cut(&sample_cut()).expect("encodes");
        for len in 0..cut.len() {
            assert!(decode_cut(&cut[..len]).is_err(), "length {len}");
        }
    }

    /// The cut format is frozen across this crate, `krabka-streams-rs`,
    /// `krabka-streams-java` and `krabka-streams-go`. These bytes are encoded
    /// straight from the layout this module's own documentation states,
    /// independently of all four implementations, so an encoder or a decoder
    /// that drifts fails here. The same vector is asserted in the other three.
    ///
    /// Key: version 0, kind 2, group `orders-cut`, epoch 7.
    /// Value: version 0, triggered 1724500000000, completed 1724500000042,
    /// status complete, topic `orders` with partition 0 at offset 1024 and
    /// partition 1 at offset 2048, and no missing partitions.
    const GOLDEN_CUT_KEY: &[u8] = &[
        0x00, 0x00, 0x00, 0x02, 0x00, 0x0a, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, 0x2d, 0x63, 0x75,
        0x74, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
    ];

    const GOLDEN_CUT_VALUE: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84, 0x35, 0xbd, 0x00, 0x00, 0x00, 0x01, 0x91, 0x84,
        0x35, 0xbd, 0x2a, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x06, 0x6f, 0x72, 0x64, 0x65, 0x72,
        0x73, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00,
        0x00, 0x00, 0x00,
    ];

    fn golden_cut() -> CutValue {
        CutValue {
            triggered_at: 1_724_500_000_000,
            completed_at: 1_724_500_000_042,
            status: CutStatus::Complete,
            topics: vec![super::super::test_support::orders_offsets()],
            missing: Vec::new(),
        }
    }

    #[test]
    fn the_golden_cut_key_decodes_and_re_encodes_byte_for_byte() {
        let expected = RecordKey::cut("orders-cut", 7);
        assert!(decode_key(GOLDEN_CUT_KEY).ok() == Some(expected.clone()));
        assert!(encode_key(&expected).ok().as_deref() == Some(GOLDEN_CUT_KEY));
    }

    #[test]
    fn the_golden_cut_value_decodes_and_re_encodes_byte_for_byte() {
        let expected = golden_cut();
        assert!(decode_cut(GOLDEN_CUT_VALUE).ok() == Some(expected.clone()));
        assert!(encode_cut(&expected).ok().as_deref() == Some(GOLDEN_CUT_VALUE));
    }

    /// A topic name of a cut is a string with an `i16` length, both where the
    /// cut offsets it and where it lists it as missing. The encoder writes one
    /// of 32767 bytes and refuses a longer one with an error.
    #[test]
    fn a_topic_name_of_32767_bytes_encodes_and_one_of_32768_is_refused() {
        for (length, encodes) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let topic = "t".repeat(length);
            let offset_row = CutValue {
                topics: vec![TopicOffsets {
                    topic: topic.clone(),
                    partitions: Vec::new(),
                }],
                missing: Vec::new(),
                ..sample_cut()
            };
            let missing_row = CutValue {
                topics: Vec::new(),
                missing: vec![MissingPartition {
                    topic,
                    partition: PartitionIndex(2),
                }],
                ..sample_cut()
            };
            for (place, value) in [("offsets", offset_row), ("missing", missing_row)] {
                let encoded = encode_cut(&value);
                assert!(encoded.is_ok() == encodes, "{place}, {length} bytes");
                if let Ok(bytes) = encoded {
                    assert!(
                        decode_cut(&bytes).ok() == Some(value),
                        "{place}, {length} bytes"
                    );
                }
            }
        }
    }

    #[test]
    fn a_status_survives_its_wire_code() {
        for status in [CutStatus::Complete, CutStatus::Partial] {
            assert!(CutStatus::try_from(status.code()).ok() == Some(status));
        }
    }
}
