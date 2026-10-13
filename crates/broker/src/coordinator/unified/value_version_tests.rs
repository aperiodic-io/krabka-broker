//! Every `__consumer_offsets` value the group coordinator reads declares
//! `"validVersions": "0"` in Kafka 4.3.1, and Kafka's
//! `CoordinatorRecordSerde.deserialize` throws `UnknownRecordVersionException`
//! for any other value version, which fails the load. Each decoder here
//! accepts its version-0 value and refuses the same bytes at version 1.

use {
    super::{
        persistence_next_gen as ng,
        share::persistence as sp,
        streams::persistence as st,
        test_support::{next_current, next_member, share_member, streams_member},
    },
    crate::error::BrokerError,
    assert2::check,
    bytes::Bytes,
};

type Decoder = fn(&[u8]) -> Result<(), BrokerError>;

fn at_version(value: &Bytes, version: i16) -> Vec<u8> {
    let mut bytes = value.to_vec();
    bytes[..2].copy_from_slice(&version.to_be_bytes());
    bytes
}

#[test]
fn every_coordinator_value_refuses_an_unknown_version() {
    let rows: Vec<(&str, Bytes, Decoder)> = vec![
        (
            "ConsumerGroupMetadataValue",
            ng::GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }
            .encode(),
            |b| ng::GroupMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupPartitionMetadataValue",
            ng::PartitionMetadataValue::default().encode(),
            |b| ng::PartitionMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupMemberMetadataValue",
            next_member("c").encode(),
            |b| ng::MemberMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupTargetAssignmentMetadataValue",
            ng::TargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            |b| ng::TargetAssignmentMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupTargetAssignmentMemberValue",
            ng::TargetAssignmentMemberValue::default().encode(),
            |b| ng::TargetAssignmentMemberValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupCurrentMemberAssignmentValue",
            next_current(1).encode(),
            |b| ng::CurrentMemberAssignmentValue::decode(b).map(|_| ()),
        ),
        (
            "ConsumerGroupRegularExpressionValue",
            ng::RegularExpressionValue {
                topics: vec![],
                version: 1,
                timestamp_ms: 1,
            }
            .encode(),
            |b| ng::RegularExpressionValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupMemberMetadataValue",
            share_member("c").encode(),
            |b| sp::ShareGroupMemberMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupMetadataValue",
            sp::ShareGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }
            .encode(),
            |b| sp::ShareGroupMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupTargetAssignmentMetadataValue",
            sp::ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            |b| sp::ShareGroupTargetAssignmentMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupTargetAssignmentMemberValue",
            sp::ShareGroupTargetAssignmentMemberValue::default().encode(),
            |b| sp::ShareGroupTargetAssignmentMemberValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupCurrentMemberAssignmentValue",
            sp::ShareGroupCurrentMemberAssignmentValue::default().encode(),
            |b| sp::ShareGroupCurrentMemberAssignmentValue::decode(b).map(|_| ()),
        ),
        (
            "ShareGroupStatePartitionMetadataValue",
            sp::ShareGroupStatePartitionMetadataValue::default().encode(),
            |b| sp::ShareGroupStatePartitionMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupMetadataValue",
            st::StreamsGroupMetadataValue {
                epoch: 1,
                metadata_hash: 2,
                validated_topology_epoch: -1,
                last_assignment_configs: None,
                description: st::DescriptionEpochs::default(),
            }
            .encode(),
            |b| st::StreamsGroupMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupMemberMetadataValue",
            streams_member("c").encode(),
            |b| st::StreamsGroupMemberMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupTargetAssignmentMetadataValue",
            st::StreamsGroupTargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            |b| st::StreamsGroupTargetAssignmentMetadataValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupTargetAssignmentMemberValue",
            st::StreamsGroupTargetAssignmentMemberValue::default().encode(),
            |b| st::StreamsGroupTargetAssignmentMemberValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupCurrentMemberAssignmentValue",
            st::StreamsGroupCurrentMemberAssignmentValue::default().encode(),
            |b| st::StreamsGroupCurrentMemberAssignmentValue::decode(b).map(|_| ()),
        ),
        (
            "StreamsGroupTopologyValue",
            st::StreamsGroupTopologyValue::default().encode(),
            |b| st::StreamsGroupTopologyValue::decode(b).map(|_| ()),
        ),
    ];
    for (name, value, decode) in rows {
        check!(
            decode(&at_version(&value, 0)).is_ok(),
            "{name} at version 0"
        );
        check!(
            decode(&at_version(&value, 1)).is_err(),
            "{name} at version 1"
        );
        check!(
            decode(&at_version(&value, -1)).is_err(),
            "{name} at version -1"
        );
    }
}
