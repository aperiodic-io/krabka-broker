//! The record keys of the KIP-848 next-gen consumer-group records, and their
//! codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 3 for
//! `ConsumerGroupMetadataKey`, 4 for the deprecated
//! `ConsumerGroupPartitionMetadataKey`, 5 for `ConsumerGroupMemberMetadataKey`, 6 for
//! `ConsumerGroupTargetAssignmentMetadataKey`, 7 for
//! `ConsumerGroupTargetAssignmentMemberKey`, 8 for
//! `ConsumerGroupCurrentMemberAssignmentKey` and 16 for
//! `ConsumerGroupRegularExpressionKey`. The group id follows, and then the
//! member id for the per-member records and the regular expression for the
//! resolved-regular-expression record. [`NextGenKey`] is the parsed form that
//! the `__consumer_offsets` replay path dispatches on.
//!
//! Every `coordinator-key` schema declares `"flexibleVersions": "none"`, so a
//! key string keeps the legacy `i16` length prefix and a key carries no
//! tagged-field trailer. Only the values are flexible; see
//! [`persistence::flex`](crate::coordinator::unified::persistence::flex).
//!
//! The broker writes the key-4 record only as a tombstone, as Kafka 4.3.1
//! does; see [`partition_metadata`](super::partition_metadata).

use crate::coordinator::unified::persistence::group_record_keys;

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_PARTITION_METADATA: i16 = 4;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;
pub const KEY_REGULAR_EXPRESSION: i16 = 16;

group_record_keys! {
    pub enum NextGenKey {
        GroupMetadata => KEY_GROUP_METADATA,
        /// The deprecated `ConsumerGroupPartitionMetadataKey`.
        PartitionMetadata => KEY_PARTITION_METADATA,
        MemberMetadata(member_id) => KEY_MEMBER_METADATA,
        TargetAssignmentMetadata => KEY_TARGET_ASSIGNMENT_METADATA,
        TargetAssignmentMember(member_id) => KEY_TARGET_ASSIGNMENT_MEMBER,
        CurrentMemberAssignment(member_id) => KEY_CURRENT_MEMBER_ASSIGNMENT,
        RegularExpression(regex) => KEY_REGULAR_EXPRESSION,
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    fn parse_key;

    /// Encodes a [`NextGenKey`] with its leading `i16` key version.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes, which a non-flexible key string cannot carry.
    fn encode_key;

    invalid "unknown next-gen key version";
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// `ConsumerGroupPartitionMetadataKey`: an `i16` key version of 4, then
    /// the group id as a non-flexible string.
    #[test]
    fn partition_metadata_key_bytes_match_kafka_schema() {
        let key = NextGenKey::PartitionMetadata {
            group_id: "grp".into(),
        };
        let golden: &[u8] = b"\x00\x04\x00\x03grp";
        assert!(&encode_key(&key).unwrap()[..] == golden);
        assert!(parse_key(KEY_PARTITION_METADATA, &golden[2..]).unwrap() == key);
    }

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }

    /// `ConsumerGroupRegularExpressionKey`: an `i16` key version of 16, then
    /// the group id and the regular expression as non-flexible strings.
    #[test]
    fn regular_expression_key_bytes_match_kafka_schema() {
        let key = NextGenKey::RegularExpression {
            group_id: "g".into(),
            regex: "a.*".into(),
        };
        assert!(&encode_key(&key).unwrap()[..] == b"\x00\x10\x00\x01g\x00\x03a.*");
        let encoded = encode_key(&key).unwrap();
        let mut r = &encoded[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == key);
    }

    #[test]
    fn key_roundtrip_member_metadata() {
        let k = NextGenKey::MemberMetadata {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        let kb = encode_key(&k).unwrap();
        let mut r = &kb[..];
        let v = bytes::Buf::get_i16(&mut r);
        assert!(parse_key(v, r).unwrap() == k);
    }
}
