//! The group-definition record: the topics a barrier group cuts across, its
//! injection interval, and its cut retention.
//!
//! A group record is the only record kind whose value carries an interval, so
//! it is the only one that touches the millisecond conversion for [`Time`].

use {
    super::primitives as codec,
    krabka_units::{
        Time,
        convert::wire::{opt_time_from_millis_i64, opt_time_to_millis_i64},
    },
};

#[cfg(test)]
fn assert_topic_name_encoding<E>(
    length: usize,
    should_encode: bool,
    encoded: Result<Vec<u8>, E>,
    round_trips: impl FnOnce(&[u8]) -> bool,
) {
    assert2::assert!(encoded.is_ok() == should_encode, "{length} bytes");
    if let Ok(bytes) = encoded {
        assert2::assert!(round_trips(&bytes), "{length} bytes");
    }
}

/// A barrier group definition.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`Time`] is backed by a
/// float.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupValue {
    /// The topics the group cuts across.
    pub(crate) topics: Vec<String>,
    /// How often the coordinator injects without a trigger request. `None`
    /// turns periodic injection off.
    pub(crate) interval: Option<Time>,
    /// How many cuts the coordinator keeps before it trims the older ones.
    pub(crate) retained_cuts: i32,
    /// The highest epoch this group has allocated.
    pub(crate) last_epoch: i64,
}

/// Encode a group definition.
///
/// # Errors
/// Returns [`codec::BrokerError::Protocol`] when a topic name is longer than 32767
/// bytes, which the `i16` length cannot carry.
pub(crate) fn encode_group(value: &GroupValue) -> Result<Vec<u8>, codec::BrokerError> {
    let mut out = Vec::new();
    codec::put_i16(&mut out, codec::RECORD_VERSION);
    codec::put_array_len(&mut out, value.topics.len(), false);
    for topic in &value.topics {
        codec::put_string(&mut out, topic)?;
    }
    codec::put_i64(&mut out, opt_time_to_millis_i64(value.interval));
    codec::put_i32(&mut out, value.retained_cuts);
    codec::put_i64(&mut out, value.last_epoch);
    Ok(out)
}

/// Decode a group definition.
///
/// # Errors
/// Returns a [`codec::ProtocolError`] when the value is truncated, carries a version
/// other than [`codec::RECORD_VERSION`], holds a negative array length, holds a
/// non-UTF-8 topic name, or has trailing bytes.
pub(crate) fn decode_group(bytes: &[u8]) -> Result<GroupValue, codec::ProtocolError> {
    let mut cur = bytes;
    codec::expect_version(&mut cur)?;
    let topics = codec::decode_vec(&mut cur, |c| codec::get_string_owned(c))?;
    let interval = opt_time_from_millis_i64(codec::get_i64(&mut cur)?);
    let retained_cuts = codec::get_i32(&mut cur)?;
    let last_epoch = codec::get_i64(&mut cur)?;
    codec::expect_end(cur)?;
    Ok(GroupValue {
        topics,
        interval,
        retained_cuts,
        last_epoch,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            barrier::persistence::{
                injection_start::{
                    InjectionStartValue, TopicTarget, decode_injection_start,
                    encode_injection_start,
                },
                test_support::{sample_group, sample_injection_start},
            },
            coordinator::unified::persistence::MAX_STRING_BYTES,
        },
        assert2::assert,
        krabka_units::convert::TimeExt,
    };

    #[test]
    fn a_group_value_round_trips() {
        let value = sample_group();
        assert!(decode_group(&encode_group(&value).expect("encodes")).ok() == Some(value));
    }

    #[test]
    fn an_absent_interval_round_trips_as_none() {
        let value = GroupValue {
            interval: None,
            ..sample_group()
        };
        let decoded = decode_group(&encode_group(&value).expect("encodes")).expect("decodes");
        assert!(decoded == value);
        assert!(decoded.interval.is_none());
    }

    #[test]
    fn an_interval_keeps_its_millisecond_value() {
        let value = sample_group();
        let decoded = decode_group(&encode_group(&value).expect("encodes")).expect("decodes");
        assert!(decoded.interval.map(TimeExt::millis_i64) == Some(60_000));
    }

    #[derive(Clone, Copy)]
    enum TopicNameRecord {
        Group,
        InjectionStart,
    }

    /// Topic names use an `i16` byte length in each barrier record.
    #[yare::parameterized(
        group_at_limit = { TopicNameRecord::Group, MAX_STRING_BYTES, true },
        group_over_limit = { TopicNameRecord::Group, MAX_STRING_BYTES + 1, false },
        injection_start_at_limit = { TopicNameRecord::InjectionStart, MAX_STRING_BYTES, true },
        injection_start_over_limit = {
            TopicNameRecord::InjectionStart,
            MAX_STRING_BYTES + 1,
            false,
        },
    )]
    fn topic_name_length_respects_the_i16_encoding_limit(
        record: TopicNameRecord,
        length: usize,
        encodes: bool,
    ) {
        match record {
            TopicNameRecord::Group => {
                let value = GroupValue {
                    topics: vec!["orders".to_owned(), "t".repeat(length)],
                    ..sample_group()
                };
                assert_topic_name_encoding(length, encodes, encode_group(&value), |bytes| {
                    decode_group(bytes).ok() == Some(value)
                });
            }
            TopicNameRecord::InjectionStart => {
                let value = InjectionStartValue {
                    targets: vec![TopicTarget {
                        topic: "t".repeat(length),
                        partition_count: 2,
                    }],
                    ..sample_injection_start()
                };
                assert_topic_name_encoding(
                    length,
                    encodes,
                    encode_injection_start(&value),
                    |bytes| decode_injection_start(bytes).ok() == Some(value),
                );
            }
        }
    }

    #[test]
    fn an_empty_topic_list_round_trips() {
        let value = GroupValue {
            topics: Vec::new(),
            ..sample_group()
        };
        assert!(decode_group(&encode_group(&value).expect("encodes")).ok() == Some(value));
    }
}
