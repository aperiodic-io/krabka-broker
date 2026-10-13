//! The record keys of the KIP-932 share-group records, and their codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 10 for
//! `ShareGroupMemberMetadataKey`, 11 for `ShareGroupMetadataKey`, 12 for
//! `ShareGroupTargetAssignmentMetadataKey`, 13 for
//! `ShareGroupTargetAssignmentMemberKey`, 14 for
//! `ShareGroupCurrentMemberAssignmentKey` and 15 for
//! `ShareGroupStatePartitionMetadataKey`. The group id follows, and then the
//! member id for the per-member records. [`ShareGroupKey`] is the parsed form
//! that the `__consumer_offsets` replay path dispatches on.
//!
//! These numbers are not free choices. Kafka's `GroupCoordinatorRecordSerde`
//! looks the leading `i16` up in its own `CoordinatorRecordType` table, so a
//! record written under a number Kafka assigns to a different type is decoded
//! as that other type, and its tools die in the middle of the topic rather than
//! skip it.
//!
//! Every `coordinator-key` schema declares `"flexibleVersions": "none"`, so a
//! key string keeps the legacy `i16` length prefix and a key carries no
//! tagged-field trailer. Only the values are flexible.

use {
    crate::{
        coordinator::unified::persistence::{encode_string_key, get_string},
        error::BrokerError,
    },
    bytes::Bytes,
    krabka_protocol::ProtocolError,
};

pub const KEY_SHARE_MEMBER_METADATA: i16 = 10;
pub const KEY_SHARE_GROUP_METADATA: i16 = 11;
pub const KEY_SHARE_TARGET_ASSIGNMENT_METADATA: i16 = 12;
pub const KEY_SHARE_TARGET_ASSIGNMENT_MEMBER: i16 = 13;
pub const KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT: i16 = 14;
pub const KEY_SHARE_GROUP_STATE_PARTITION_METADATA: i16 = 15;

#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::GroupRecordKey)]
#[group_key(invalid = "unknown share-group key version")]
pub enum ShareGroupKey {
    #[group_key(version = KEY_SHARE_GROUP_METADATA)]
    GroupMetadata { group_id: String },
    #[group_key(version = KEY_SHARE_MEMBER_METADATA)]
    MemberMetadata { group_id: String, member_id: String },
    #[group_key(version = KEY_SHARE_TARGET_ASSIGNMENT_METADATA)]
    TargetAssignmentMetadata { group_id: String },
    #[group_key(version = KEY_SHARE_TARGET_ASSIGNMENT_MEMBER)]
    TargetAssignmentMember { group_id: String, member_id: String },
    #[group_key(version = KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT)]
    CurrentMemberAssignment { group_id: String, member_id: String },
    #[group_key(version = KEY_SHARE_GROUP_STATE_PARTITION_METADATA)]
    StatePartitionMetadata { group_id: String },
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_share_key(version: i16, buf: &[u8]) -> Result<ShareGroupKey, BrokerError> {
    ShareGroupKey::parse(version, buf)
}

/// Encodes a [`ShareGroupKey`] with its leading `i16` key version.
///
/// # Errors
///
/// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes, which a non-flexible key string cannot carry.
pub fn encode_share_key(key: &ShareGroupKey) -> Result<Bytes, BrokerError> { key.encode() }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_share_key(99, &[]).is_err());
    }

    #[test]
    fn key_versions_match_kafka_api_keys() {
        // apiKey of each schema at Apache Kafka tag 4.3.1.
        assert!(KEY_SHARE_MEMBER_METADATA == 10);
        assert!(KEY_SHARE_GROUP_METADATA == 11);
        assert!(KEY_SHARE_TARGET_ASSIGNMENT_METADATA == 12);
        assert!(KEY_SHARE_TARGET_ASSIGNMENT_MEMBER == 13);
        assert!(KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT == 14);
        assert!(KEY_SHARE_GROUP_STATE_PARTITION_METADATA == 15);
    }

    #[test]
    fn group_metadata_key_bytes_match_kafka_schema() {
        // i16 apiKey 11, then a non-flexible i16-length group id.
        let bytes = encode_share_key(&ShareGroupKey::GroupMetadata {
            group_id: "g1".into(),
        })
        .unwrap();
        assert!(&bytes[..] == b"\x00\x0b\x00\x02g1");
    }

    #[test]
    fn member_metadata_key_bytes_match_kafka_schema() {
        let bytes = encode_share_key(&ShareGroupKey::MemberMetadata {
            group_id: "g1".into(),
            member_id: "m1".into(),
        })
        .unwrap();
        assert!(&bytes[..] == b"\x00\x0a\x00\x02g1\x00\x02m1");
    }
}
