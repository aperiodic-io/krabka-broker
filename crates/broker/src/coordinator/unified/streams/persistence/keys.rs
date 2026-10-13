//! The record keys of the KIP-1071 streams-group records, and their codec.
//!
//! Every key starts with an `i16` key-version discriminator, the `apiKey` of
//! the matching Apache Kafka schema at tag `4.3.1`: 17 for
//! `StreamsGroupMetadataKey`, 19 for `StreamsGroupMemberMetadataKey`, 20 for
//! `StreamsGroupTargetAssignmentMetadataKey`, 21 for
//! `StreamsGroupTargetAssignmentMemberKey`, 22 for
//! `StreamsGroupCurrentMemberAssignmentKey` and 23 for
//! `StreamsGroupTopologyKey`. The group id follows, and then the member id for
//! the per-member records. [`StreamsGroupKey`] is the parsed form that the
//! `__consumer_offsets` replay path dispatches on.
//!
//! These numbers are not free choices. Kafka's `GroupCoordinatorRecordSerde`
//! looks the leading `i16` up in its own `CoordinatorRecordType` table, so a
//! record written under a number Kafka assigns to a different type is decoded
//! as that other type, and its tools die in the middle of the topic rather than
//! skip it.
//!
//! Kafka 4.3.1 assigns no record type 18: KIP-1101 replaced the streams
//! partition-metadata record with the `MetadataHash` of
//! `StreamsGroupMetadataValue`. The broker neither writes nor reads it, so a
//! record of type 18 replays as an unknown type and is skipped, as Kafka's
//! loader skips it.
//!
//! Every `coordinator-key` schema declares `"flexibleVersions": "none"`, so a
//! key string keeps the legacy `i16` length prefix and a key carries no
//! tagged-field trailer. Only the values are flexible.

use crate::coordinator::unified::persistence::{group_record_keys, string_key_encoders};

pub const KEY_STREAMS_GROUP_METADATA: i16 = 17;
pub const KEY_STREAMS_MEMBER_METADATA: i16 = 19;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_METADATA: i16 = 20;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER: i16 = 21;
pub const KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT: i16 = 22;
pub const KEY_STREAMS_TOPOLOGY: i16 = 23;

group_record_keys! {
    pub enum StreamsGroupKey {
        GroupMetadata => KEY_STREAMS_GROUP_METADATA,
        MemberMetadata(member_id) => KEY_STREAMS_MEMBER_METADATA,
        Topology => KEY_STREAMS_TOPOLOGY,
        TargetAssignmentMetadata => KEY_STREAMS_TARGET_ASSIGNMENT_METADATA,
        TargetAssignmentMember(member_id) => KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER,
        CurrentMemberAssignment(member_id) => KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT,
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    fn parse_streams_key;

    /// Encodes a [`StreamsGroupKey`] for dispatch, with a leading `i16` key
    /// version.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    fn encode_streams_key;

    invalid "unknown streams-group key version";
}

string_key_encoders! {
    /// Encodes the group-metadata key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_group_metadata_key(group_id) = KEY_STREAMS_GROUP_METADATA;

    /// Encodes the member-metadata key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_member_metadata_key(group_id, member_id) = KEY_STREAMS_MEMBER_METADATA;

    /// Encodes the topology key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_topology_key(group_id) = KEY_STREAMS_TOPOLOGY;

    /// Encodes the target-assignment-metadata key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_target_assignment_metadata_key(group_id) = KEY_STREAMS_TARGET_ASSIGNMENT_METADATA;

    /// Encodes the target-assignment-member key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_target_assignment_member_key(group_id, member_id) = KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER;

    /// Encodes the current-member-assignment key.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::BrokerError::Protocol`] when a string of the key is longer than
    /// 32767 bytes.
    pub fn encode_current_member_assignment_key(group_id, member_id) = KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT;
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_streams_key(99, &[]).is_err());
        assert!(parse_streams_key(18, b"\x00\x02g1").is_err());
    }

    #[test]
    fn key_versions_match_kafka_api_keys() {
        // apiKey of each schema at Apache Kafka tag 4.3.1, which defines no
        // streams record type 18.
        assert!(KEY_STREAMS_GROUP_METADATA == 17);
        assert!(KEY_STREAMS_MEMBER_METADATA == 19);
        assert!(KEY_STREAMS_TARGET_ASSIGNMENT_METADATA == 20);
        assert!(KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER == 21);
        assert!(KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT == 22);
        assert!(KEY_STREAMS_TOPOLOGY == 23);
    }

    #[test]
    fn group_metadata_key_bytes_match_kafka_schema() {
        // i16 apiKey 17, then a non-flexible i16-length group id.
        assert!(&encode_group_metadata_key("g1").unwrap()[..] == b"\x00\x11\x00\x02g1");
    }

    #[test]
    fn member_metadata_key_bytes_match_kafka_schema() {
        assert!(
            &encode_member_metadata_key("g1", "m1").unwrap()[..] == b"\x00\x13\x00\x02g1\x00\x02m1"
        );
    }
}
