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

/// A coordinator value type: its name, an encoded value, and its decoder.
struct VersionRow {
    name: &'static str,
    value: Bytes,
    decode: Decoder,
}

fn at_version(value: &Bytes, version: i16) -> Vec<u8> {
    let mut bytes = value.to_vec();
    bytes[..2].copy_from_slice(&version.to_be_bytes());
    bytes
}

#[test]
fn every_coordinator_value_refuses_an_unknown_version() {
    let rows: Vec<VersionRow> = vec![
        VersionRow {
            name: "ConsumerGroupMetadataValue",
            value: ng::GroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }
            .encode(),
            decode: |b| ng::GroupMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupPartitionMetadataValue",
            value: ng::PartitionMetadataValue::default().encode(),
            decode: |b| ng::PartitionMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupMemberMetadataValue",
            value: next_member("c").encode(),
            decode: |b| ng::MemberMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupTargetAssignmentMetadataValue",
            value: ng::TargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            decode: |b| ng::TargetAssignmentMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupTargetAssignmentMemberValue",
            value: ng::TargetAssignmentMemberValue::default().encode(),
            decode: |b| ng::TargetAssignmentMemberValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupCurrentMemberAssignmentValue",
            value: next_current(1).encode(),
            decode: |b| ng::CurrentMemberAssignmentValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ConsumerGroupRegularExpressionValue",
            value: ng::RegularExpressionValue {
                topics: vec![],
                version: 1,
                timestamp_ms: 1,
            }
            .encode(),
            decode: |b| ng::RegularExpressionValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupMemberMetadataValue",
            value: share_member("c").encode(),
            decode: |b| sp::ShareGroupMemberMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupMetadataValue",
            value: sp::ShareGroupMetadataValue {
                epoch: 1,
                metadata_hash: 0,
            }
            .encode(),
            decode: |b| sp::ShareGroupMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupTargetAssignmentMetadataValue",
            value: sp::ShareGroupTargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            decode: |b| sp::ShareGroupTargetAssignmentMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupTargetAssignmentMemberValue",
            value: sp::ShareGroupTargetAssignmentMemberValue::default().encode(),
            decode: |b| sp::ShareGroupTargetAssignmentMemberValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupCurrentMemberAssignmentValue",
            value: sp::ShareGroupCurrentMemberAssignmentValue::default().encode(),
            decode: |b| sp::ShareGroupCurrentMemberAssignmentValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "ShareGroupStatePartitionMetadataValue",
            value: sp::ShareGroupStatePartitionMetadataValue::default().encode(),
            decode: |b| sp::ShareGroupStatePartitionMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupMetadataValue",
            value: st::StreamsGroupMetadataValue {
                epoch: 1,
                metadata_hash: 2,
                validated_topology_epoch: -1,
                last_assignment_configs: None,
                description: st::DescriptionEpochs::default(),
            }
            .encode(),
            decode: |b| st::StreamsGroupMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupMemberMetadataValue",
            value: streams_member("c").encode(),
            decode: |b| st::StreamsGroupMemberMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupTargetAssignmentMetadataValue",
            value: st::StreamsGroupTargetAssignmentMetadataValue {
                assignment_epoch: 1,
                assignment_timestamp_ms: 0,
            }
            .encode(),
            decode: |b| st::StreamsGroupTargetAssignmentMetadataValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupTargetAssignmentMemberValue",
            value: st::StreamsGroupTargetAssignmentMemberValue::default().encode(),
            decode: |b| st::StreamsGroupTargetAssignmentMemberValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupCurrentMemberAssignmentValue",
            value: st::StreamsGroupCurrentMemberAssignmentValue::default().encode(),
            decode: |b| st::StreamsGroupCurrentMemberAssignmentValue::decode(b).map(|_| ()),
        },
        VersionRow {
            name: "StreamsGroupTopologyValue",
            value: st::StreamsGroupTopologyValue::default().encode(),
            decode: |b| st::StreamsGroupTopologyValue::decode(b).map(|_| ()),
        },
    ];
    for VersionRow {
        name,
        value,
        decode,
    } in rows
    {
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
