//! KIP-932 share-group record types persisted in `__consumer_offsets`.
//!
//! The wire encoding follows the Apache Kafka schemas at tag `4.3.1`, under
//! `group-coordinator/src/main/resources/common/message/`. A key is
//! non-flexible and starts with the schema's `apiKey` as an `i16`: 10 for the
//! member metadata, 11 for the group metadata, 12 and 13 for the target
//! assignment, 14 for the current member assignment and 15 for the share-state
//! partition metadata. A value starts with an `i16` schema version, 0 for all
//! six records, and is flexible: compact strings, compact arrays and a
//! tagged-field trailer on the message and on every nested struct. The leaf
//! helpers are the shared `persistence::flex` ones.
//!
//! Share-group records drop the consumer-only fields: `instance_id`,
//! `server_assignor`, `subscribed_topic_regex`, `rebalance_timeout_ms`, and the
//! revocation and pending-assignment machinery.
//!
//! This file is the module root. The key discriminator and its codec live in
//! `keys`, the two single-epoch records in `epochs`, the member metadata record
//! in `member`, the target and current assignment records with their shared
//! topic-partition codec in `assignment`, and the share-state partition
//! metadata record in `partition_metadata`.

mod assignment;
mod epochs;
mod keys;
mod member;
mod partition_metadata;

pub use self::{
    assignment::{ShareGroupCurrentMemberAssignmentValue, ShareGroupTargetAssignmentMemberValue},
    epochs::{ShareGroupMetadataValue, ShareGroupTargetAssignmentMetadataValue},
    keys::{
        KEY_SHARE_CURRENT_MEMBER_ASSIGNMENT, KEY_SHARE_GROUP_METADATA,
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA, KEY_SHARE_MEMBER_METADATA,
        KEY_SHARE_TARGET_ASSIGNMENT_MEMBER, KEY_SHARE_TARGET_ASSIGNMENT_METADATA, ShareGroupKey,
        encode_share_key, parse_share_key,
    },
    member::ShareGroupMemberMetadataValue,
    partition_metadata::{
        DeletingTopic, ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo,
        UNKNOWN_TOPIC_NAME,
    },
};

#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) struct ShareKeyVersion(pub i16);

/// Check the independent schema discriminator and the decoded key together.
#[cfg(test)]
pub(super) fn check_key_round_trip(key: &ShareGroupKey, expected: ShareKeyVersion) {
    let bytes = encode_share_key(key).unwrap();
    let (version, body) = crate::coordinator::unified::test_support::peek_version(&bytes);
    assert2::assert!(version == expected.0);
    assert2::assert!(parse_share_key(version, body).unwrap() == *key);
}
