//! The resolved-regular-expression record at key version 16.
//!
//! [`RegularExpressionValue`] is what a KIP-848 consumer group knows about one
//! `SubscribedTopicRegex` its members use: the topics the pattern resolved to,
//! the metadata version they were resolved at, and when. Kafka's
//! `GroupMetadataManager` writes it whenever it resolves a regex, and replays
//! it, so that a coordinator failover restores the topics of every regex
//! subscription without a heartbeat that carries the pattern.
//!
//! # Layout
//!
//! From `ConsumerGroupRegularExpressionValue.json` at Apache Kafka tag
//! `4.3.1`, which declares `"flexibleVersions": "0+"`. In field order:
//! `Topics` (`[]string`), `Version` (int64) and `Timestamp` (int64). The key,
//! `ConsumerGroupRegularExpressionKey`, is the group id and the regular
//! expression, both non-flexible strings; see
//! [`NextGenKey::RegularExpression`](super::NextGenKey::RegularExpression).

use {
    crate::coordinator::unified::persistence::{
        flex::{get_string_array, put_string_array, value_codec},
        get_i64,
    },
    bytes::BufMut,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegularExpressionValue {
    /// The topics that the regular expression resolved to, which the resolving
    /// principal may `Describe`.
    pub topics: Vec<String>,
    /// The version of the metadata image the topics were resolved from.
    pub version: i64,
    /// The wall-clock time of the resolution, in milliseconds since the epoch.
    pub timestamp_ms: i64,
}

value_codec! {
    RegularExpressionValue("ConsumerGroupRegularExpressionValue"),
    encode(&self) -> buf {
        put_string_array(buf, &self.topics);
        buf.put_i64(self.version);
        buf.put_i64(self.timestamp_ms);
    }
    decode(buf) {
        let topics = get_string_array(buf)?;
        let version = get_i64(buf)?;
        let timestamp_ms = get_i64(buf)?;
        Ok(Self {
            topics,
            version,
            timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::coordinator::unified::test_support::wire_bytes, assert2::assert};

    fn value() -> RegularExpressionValue {
        RegularExpressionValue {
            topics: vec!["a".into(), "bc".into()],
            version: 7,
            timestamp_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn bytes_match_kafka_schema() {
        let want = wire_bytes(&[
            "0000",             // value version 0
            "030261036263",     // Topics, compact array of compact strings
            "0000000000000007", // Version
            "0000018bcfe56800", // Timestamp
            "00",               // no tagged fields
        ]);
        assert!(&value().encode()[..] == &want[..]);
    }

    #[test]
    fn roundtrip() {
        let v = value();
        assert!(RegularExpressionValue::decode(&v.encode()).unwrap() == v);
        let empty = RegularExpressionValue {
            topics: vec![],
            ..v
        };
        assert!(RegularExpressionValue::decode(&empty.encode()).unwrap() == empty);
    }

    #[test]
    fn a_truncated_record_is_refused() {
        let full = value().encode();
        assert!(RegularExpressionValue::decode(&full[..full.len() - 1]).is_err());
    }
}
