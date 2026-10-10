//! The two share-group records whose value is a single epoch counter.
//!
//! The group metadata record at key version 11 carries the group epoch, and
//! the target-assignment metadata record at key version 12 carries the
//! assignment epoch.
//!
//! # Layout
//!
//! From `ShareGroupMetadataValue.json` and
//! `ShareGroupTargetAssignmentMetadataValue.json` at Apache Kafka tag `4.3.1`.
//! Both declare `"flexibleVersions": "0+"`.
//!
//! - `ShareGroupMetadataValue`: `Epoch` (int32) and `MetadataHash` (int64).
//!   The hash is a plain field rather than a tagged one, so it is always on the
//!   wire. The group keeps it as Kafka does (see
//!   [`topic_hash`](crate::coordinator::unified::topic_hash)).
//! - `ShareGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32), then
//!   the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from KIP-1263,
//!   which Kafka's generated writer omits when it is 0.
//!
//! Both records end with the message's tagged-field count.

use bytes::BufMut;

use crate::coordinator::unified::persistence::{
    flex::{epoch_value, value_codec},
    get_i32, get_i64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShareGroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: the hash of the subscribed topics' metadata
    /// when the epoch was written.
    pub metadata_hash: i64,
}

value_codec! {
    ShareGroupMetadataValue("ShareGroupMetadataValue"),
    encode(self) -> buf {
        buf.put_i32(self.epoch);
        buf.put_i64(self.metadata_hash);
    }
    decode(buf) {
        let epoch = get_i32(buf)?;
        let metadata_hash = get_i64(buf)?;
        Ok(Self {
            epoch,
            metadata_hash,
        })
    }
}

epoch_value!(
    ShareGroupTargetAssignmentMetadataValue("ShareGroupTargetAssignmentMetadataValue") {
        assignment_epoch,
        /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
        /// which the assignment calculation finished, or 0 when it is unknown.
        assignment_timestamp_ms,
    }
);

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_GROUP_METADATA, KEY_SHARE_TARGET_ASSIGNMENT_METADATA, ShareGroupKey,
    };

    /// i16 version | i32 `Epoch` | i64 `MetadataHash` | uvarint tagged count, as
    /// Kafka 4.3.1's generated writer lays `ShareGroupMetadataValue` out. The
    /// hash is a plain field, so 0 is on the wire too.
    #[test]
    fn group_metadata_bytes_match_kafka_schema() {
        // (case, value, Kafka's bytes)
        let rows: [(&str, ShareGroupMetadataValue, &[u8]); 2] = [
            (
                "no subscribed topic",
                ShareGroupMetadataValue {
                    epoch: 7,
                    metadata_hash: 0,
                },
                b"\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            ),
            (
                "Kafka's group hash of the golden topics foo and bar",
                ShareGroupMetadataValue {
                    epoch: 2,
                    metadata_hash: -556_879_919_459_959_918,
                },
                b"\x00\x00\x00\x00\x00\x02\xf8\x45\x90\xa9\xea\x0a\x7b\x92\x00",
            ),
        ];
        for (case, value, bytes) in rows {
            check!(&value.encode()[..] == bytes, "{case}");
            check!(
                ShareGroupMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn group_metadata_round_trip() {
        let key = ShareGroupKey::GroupMetadata {
            group_id: "g1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_GROUP_METADATA),
        );

        let v = ShareGroupMetadataValue {
            epoch: 7,
            metadata_hash: 0,
        };
        assert!(ShareGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    /// i16 version | i32 `AssignmentEpoch` | tagged-field trailer, which holds
    /// `AssignmentTimestamp` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        let rows = crate::coordinator::unified::test_support::assignment_metadata_golden_cases(
            crate::coordinator::unified::test_support::AssignmentMetadataGoldenSetup::default(),
        );
        for row in rows {
            let case = row.description;
            let bytes = row.bytes;
            let value = ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: row.epoch.0,
                assignment_timestamp_ms: row.timestamp.0,
            };
            check!(&value.encode()[..] == bytes, "{case}");
            check!(
                ShareGroupTargetAssignmentMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let key = ShareGroupKey::TargetAssignmentMetadata {
            group_id: "g1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_TARGET_ASSIGNMENT_METADATA),
        );

        let v = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
            assignment_timestamp_ms: 7,
        };
        assert!(ShareGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = ShareGroupMetadataValue {
            epoch: 1,
            metadata_hash: 0,
        }
        .encode();
        assert!(ShareGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
        let t = ShareGroupTargetAssignmentMetadataValue {
            assignment_epoch: 1,
            assignment_timestamp_ms: 0,
        }
        .encode();
        assert!(ShareGroupTargetAssignmentMetadataValue::decode(&t[..t.len() - 1]).is_err());
    }
}
