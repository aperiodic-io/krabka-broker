//! The injection-start record: the target set an injection freezes before the
//! coordinator appends its first marker.
//!
//! Recovery reads this record to learn which partitions an interrupted
//! injection was meant to reach, so its shape is independent of the cut that
//! the injection eventually publishes.

use super::primitives as codec;

/// One topic in a frozen target set, and how many partitions it had.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicTarget {
    pub(crate) topic: String,
    pub(crate) partition_count: i32,
}

/// The frozen target set of one injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InjectionStartValue {
    /// Fences a coordinator that lost and regained leadership.
    pub(crate) coordinator_epoch: i32,
    pub(crate) triggered_at: i64,
    pub(crate) targets: Vec<TopicTarget>,
}

/// Encode a frozen target set.
///
/// # Errors
/// Returns [`codec::BrokerError::Protocol`] when a topic name is longer than 32767
/// bytes, which the `i16` length cannot carry.
pub(crate) fn encode_injection_start(
    value: &InjectionStartValue,
) -> Result<Vec<u8>, codec::BrokerError> {
    let mut out = Vec::new();
    codec::put_i16(&mut out, codec::RECORD_VERSION);
    codec::put_i32(&mut out, value.coordinator_epoch);
    codec::put_i64(&mut out, value.triggered_at);
    codec::put_array_len(&mut out, value.targets.len(), false);
    for target in &value.targets {
        codec::put_string(&mut out, &target.topic)?;
        codec::put_i32(&mut out, target.partition_count);
    }
    Ok(out)
}

/// Decode a frozen target set.
///
/// # Errors
/// Returns a [`codec::ProtocolError`] when the value is truncated, carries a version
/// other than [`codec::RECORD_VERSION`], holds a negative array length, holds a
/// non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_injection_start(
    bytes: &[u8],
) -> Result<InjectionStartValue, codec::ProtocolError> {
    let mut cur = bytes;
    codec::expect_version(&mut cur)?;
    let coordinator_epoch = codec::get_i32(&mut cur)?;
    let triggered_at = codec::get_i64(&mut cur)?;
    let targets = codec::decode_vec(&mut cur, |c| {
        let topic = codec::get_string_owned(c)?;
        let partition_count = codec::get_i32(c)?;
        Ok(TopicTarget {
            topic,
            partition_count,
        })
    })?;
    codec::expect_end(cur)?;
    Ok(InjectionStartValue {
        coordinator_epoch,
        triggered_at,
        targets,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        barrier::persistence::test_support::sample_injection_start,
        coordinator::unified::persistence::MAX_STRING_BYTES,
    };

    #[test]
    fn an_injection_start_round_trips() {
        let value = sample_injection_start();
        let bytes = encode_injection_start(&value).expect("encodes");
        assert!(decode_injection_start(&bytes).ok() == Some(value));
    }

    /// A target topic is a string with an `i16` length. The encoder writes one
    /// of 32767 bytes and refuses a longer one with an error.
    #[test]
    fn a_topic_name_of_32767_bytes_encodes_and_one_of_32768_is_refused() {
        for (length, encodes) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let value = InjectionStartValue {
                targets: vec![TopicTarget {
                    topic: "t".repeat(length),
                    partition_count: 2,
                }],
                ..sample_injection_start()
            };
            let encoded = encode_injection_start(&value);
            assert!(encoded.is_ok() == encodes, "{length} bytes");
            if let Ok(bytes) = encoded {
                assert!(
                    decode_injection_start(&bytes).ok() == Some(value),
                    "{length} bytes"
                );
            }
        }
    }
}
