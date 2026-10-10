//! The two next-gen records whose value is a single epoch counter.
//!
//! The group metadata record at key version 3 carries the group epoch, and the
//! target-assignment metadata record at key version 6 carries the assignment
//! epoch.
//!
//! # Layout
//!
//! Both come from the Apache Kafka schemas at tag `4.3.1`, and both declare
//! `"flexibleVersions": "0+"`:
//!
//! - `ConsumerGroupMetadataValue`: `Epoch` (int32), then the tagged
//!   `MetadataHash` (int64, tag 0, default 0), which KIP-1101 added for
//!   Kafka's own rebalance trigger. The group keeps it as Kafka does (see
//!   [`topic_hash`](crate::coordinator::unified::topic_hash)).
//! - `ConsumerGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0), which
//!   KIP-1263 added.
//!
//! Kafka's generated writer leaves a tagged field out when it holds its
//! default, and its reader restores the default for an absent one. A record
//! whose tagged fields all hold their defaults therefore ends in the empty
//! tagged-field trailer, the single `0` byte whose absence is what makes
//! Kafka's reader underflow.

use crate::coordinator::unified::persistence::flex::epoch_value;

epoch_value!(GroupMetadataValue("ConsumerGroupMetadataValue") {
    epoch,
    /// Kafka's `MetadataHash`: the hash of the subscribed topics' metadata
    /// when the epoch was written.
    metadata_hash,
});
epoch_value!(TargetAssignmentMetadataValue("ConsumerGroupTargetAssignmentMetadataValue") {
    assignment_epoch,
    /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
    /// which the assignment calculation finished, or 0 when it is unknown.
    assignment_timestamp_ms,
});

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `ConsumerGroupMetadataValue` as Kafka 4.3.1's generated writer lays it
    /// out: i16 version 0, i32 `Epoch`, then the tagged-field trailer, which
    /// holds `MetadataHash` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn group_metadata_value_bytes_match_kafka_schema() {
        // (case, value, Kafka's bytes)
        let rows: [(&str, GroupMetadataValue, &[u8]); 3] = [
            (
                "no subscribed topic: the hash is the default and absent",
                GroupMetadataValue {
                    epoch: 7,
                    metadata_hash: 0,
                },
                b"\x00\x00\x00\x00\x00\x07\x00",
            ),
            (
                "a small hash",
                GroupMetadataValue {
                    epoch: 7,
                    metadata_hash: 0x2a,
                },
                b"\x00\x00\x00\x00\x00\x07\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a",
            ),
            (
                "Kafka's group hash of the golden topics foo and bar",
                GroupMetadataValue {
                    epoch: 2,
                    metadata_hash: -556_879_919_459_959_918,
                },
                b"\x00\x00\x00\x00\x00\x02\x01\x00\x08\xf8\x45\x90\xa9\xea\x0a\x7b\x92",
            ),
        ];
        for (case, value, bytes) in rows {
            check!(&value.encode()[..] == bytes, "{case}");
            check!(
                GroupMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn group_metadata_value_rejects_a_missing_tagged_trailer() {
        // This is the byte whose absence made Kafka's generated reader throw
        // BufferUnderflowException over the whole topic.
        let full = GroupMetadataValue {
            epoch: 7,
            metadata_hash: 0,
        }
        .encode();
        let truncated = &full[..full.len() - 1];
        assert!(GroupMetadataValue::decode(truncated).is_err());
    }

    /// `ConsumerGroupTargetAssignmentMetadataValue` as Kafka 4.3.1's
    /// generated writer lays it out: i16 version 0, i32 `AssignmentEpoch`,
    /// then the tagged-field trailer, which holds `AssignmentTimestamp` as tag
    /// 0 with an eight-byte payload unless it is the default 0.
    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        let rows = crate::coordinator::unified::test_support::assignment_metadata_golden_cases(
            crate::coordinator::unified::test_support::AssignmentMetadataGoldenSetup {
                unknown_time_description: "a converted classic group: the time is unknown",
            },
        );
        for row in rows {
            let case = row.description;
            let bytes = row.bytes;
            let value = TargetAssignmentMetadataValue {
                assignment_epoch: row.epoch.0,
                assignment_timestamp_ms: row.timestamp.0,
            };
            check!(&value.encode()[..] == bytes, "{case}");
            check!(
                TargetAssignmentMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }
}
