//! The two streams records whose value is a single epoch counter.
//!
//! The group metadata record at key version 17 carries the group epoch, and
//! the target-assignment metadata record at key version 20 carries the
//! assignment epoch.
//!
//! # Layout
//!
//! From `StreamsGroupMetadataValue.json` and
//! `StreamsGroupTargetAssignmentMetadataValue.json` at Apache Kafka tag
//! `4.3.1`. Both declare `"flexibleVersions": "0+"`.
//!
//! - `StreamsGroupMetadataValue`: `Epoch` (int32) and `MetadataHash` (int64),
//!   both plain fields, then the tagged `ValidatedTopologyEpoch` (int32, tag 0,
//!   default -1) and `LastAssignmentConfigs` (tag 1, a nullable array of
//!   `LastAssignmentConfig { Key: string, Value: string }`, default null). As
//!   Kafka's generated writer does, it writes each tagged field only when it is
//!   not its default. Kafka trunk adds KIP-1331's
//!   `StoredDescriptionTopologyEpoch` (int32, tag 2, default -1) and
//!   `FailedDescriptionTopologyEpoch` (int32, tag 3, default -1), which the
//!   value keeps as [`DescriptionEpochs`]. Kafka 4.3.1 does not have them. A
//!   read always keeps them, as 4.3.1 keeps the unknown tagged fields it
//!   reads, and the writer leaves them at -1, and so out of the record,
//!   unless the topology description plugin is configured: see
//!   `ActorState::trunk_records`.
//! - `StreamsGroupTargetAssignmentMetadataValue`: `AssignmentEpoch` (int32),
//!   then the tagged `AssignmentTimestamp` (int64, tag 0, default 0) from
//!   KIP-1263, which Kafka's generated writer omits when it is 0.

use bytes::{BufMut, Bytes, BytesMut};

pub use crate::coordinator::unified::streams::description::DescriptionEpochs;
use crate::{
    coordinator::unified::persistence::{
        flex::{
            epoch_value, get_value_version, put_compact_array_len, put_compact_string,
            put_empty_tagged_fields, put_tagged_fields, read_tagged,
        },
        get_i32, get_i64,
    },
    error::BrokerError,
};

/// The tag of `ValidatedTopologyEpoch`.
const TAG_VALIDATED_TOPOLOGY_EPOCH: u32 = 0;
/// The tag of `LastAssignmentConfigs`.
const TAG_LAST_ASSIGNMENT_CONFIGS: u32 = 1;
/// The tag of Kafka trunk's `StoredDescriptionTopologyEpoch` (KIP-1331).
const TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 2;
/// The tag of Kafka trunk's `FailedDescriptionTopologyEpoch` (KIP-1331).
const TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH: u32 = 3;

/// The schema default of `ValidatedTopologyEpoch`: no validated topology.
pub const NO_VALIDATED_TOPOLOGY_EPOCH: i32 = -1;

/// One `LastAssignmentConfig` of `StreamsGroupMetadataValue`: a
/// configuration parameter of the last assignment, as a key and a value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastAssignmentConfig {
    pub key: String,
    pub value: String,
}

/// Key v17 value: the streams group epoch, the metadata hash that the group
/// last configured its topology against, the topology epoch that the group
/// validated, and the assignment configuration of that epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMetadataValue {
    pub epoch: i32,
    /// Kafka's `MetadataHash`: see
    /// [`metadata_hash`](crate::coordinator::unified::streams::topology::metadata_hash).
    pub metadata_hash: i64,
    /// Kafka's `ValidatedTopologyEpoch`: the epoch of the topology whose
    /// topics the metadata held in a valid configuration, or
    /// [`NO_VALIDATED_TOPOLOGY_EPOCH`].
    pub validated_topology_epoch: i32,
    /// Kafka's `LastAssignmentConfigs`, in the order of the record, or `None`
    /// for the null default.
    pub last_assignment_configs: Option<Vec<LastAssignmentConfig>>,
    /// Kafka trunk's KIP-1331 tags 2 and 3, each written only when it is not
    /// [`DescriptionEpochs::NONE`].
    pub description: DescriptionEpochs,
}

impl StreamsGroupMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.put_i64(self.metadata_hash);
        let mut tags = Vec::new();
        if self.validated_topology_epoch != NO_VALIDATED_TOPOLOGY_EPOCH {
            tags.push((
                TAG_VALIDATED_TOPOLOGY_EPOCH,
                Bytes::copy_from_slice(&self.validated_topology_epoch.to_be_bytes()),
            ));
        }
        if let Some(configs) = &self.last_assignment_configs {
            let mut payload = BytesMut::new();
            put_compact_array_len(&mut payload, configs.len());
            for config in configs {
                put_compact_string(&mut payload, &config.key);
                put_compact_string(&mut payload, &config.value);
                put_empty_tagged_fields(&mut payload);
            }
            tags.push((TAG_LAST_ASSIGNMENT_CONFIGS, payload.freeze()));
        }
        for (tag, epoch) in [
            (
                TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.stored,
            ),
            (
                TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH,
                self.description.failed,
            ),
        ] {
            if epoch != DescriptionEpochs::NONE {
                tags.push((tag, Bytes::copy_from_slice(&epoch.to_be_bytes())));
            }
        }
        put_tagged_fields(&mut buf, tags);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when `buf` ends before a field, when the value version
    /// is not 0, or when a tagged field does not decode.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(&mut buf, 0, "unknown StreamsGroupMetadataValue version")?;
        let epoch = get_i32(&mut buf)?;
        let metadata_hash = get_i64(&mut buf)?;
        let mut validated_topology_epoch = NO_VALIDATED_TOPOLOGY_EPOCH;
        let mut last_assignment_configs = None;
        let mut description = DescriptionEpochs::default();
        read_tagged(&mut buf, |tag, payload| match tag {
            TAG_VALIDATED_TOPOLOGY_EPOCH => {
                validated_topology_epoch = krabka_protocol::primitives::fixed::get_i32(payload)?;
                Ok(true)
            }
            TAG_LAST_ASSIGNMENT_CONFIGS => {
                last_assignment_configs = get_last_assignment_configs(payload)?;
                Ok(true)
            }
            TAG_STORED_DESCRIPTION_TOPOLOGY_EPOCH => {
                description.stored = krabka_protocol::primitives::fixed::get_i32(payload)?;
                Ok(true)
            }
            TAG_FAILED_DESCRIPTION_TOPOLOGY_EPOCH => {
                description.failed = krabka_protocol::primitives::fixed::get_i32(payload)?;
                Ok(true)
            }
            _ => Ok(false),
        })?;
        Ok(Self {
            epoch,
            metadata_hash,
            validated_topology_epoch,
            last_assignment_configs,
            description,
        })
    }
}

/// Reads the nullable compact array of `LastAssignmentConfig` structs.
fn get_last_assignment_configs(
    buf: &mut &[u8],
) -> Result<Option<Vec<LastAssignmentConfig>>, krabka_protocol::ProtocolError> {
    use krabka_protocol::{
        primitives::{array, string_bytes},
        tagged_fields::read_tagged_fields,
    };
    let Some(count) = array::get_nullable_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut configs = Vec::with_capacity(count.min(buf.len()));
    for _ in 0..count {
        let key = string_bytes::get_compact_string_owned(buf)?;
        let value = string_bytes::get_compact_string_owned(buf)?;
        read_tagged_fields(buf, |_, _| Ok(false))?;
        configs.push(LastAssignmentConfig { key, value });
    }
    Ok(Some(configs))
}

epoch_value!(
    /// Key v20 value: the target-assignment epoch and the KIP-1263
    /// `AssignmentTimestamp`.
    StreamsGroupTargetAssignmentMetadataValue("StreamsGroupTargetAssignmentMetadataValue") {
        assignment_epoch,
        /// Kafka's `AssignmentTimestamp`: the wall-clock time in milliseconds at
        /// which the assignment calculation finished, or 0 when it is unknown.
        assignment_timestamp_ms,
    }
);

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        streams::persistence::{
            KEY_STREAMS_GROUP_METADATA, KEY_STREAMS_TARGET_ASSIGNMENT_METADATA, StreamsGroupKey,
            encode_group_metadata_key, encode_target_assignment_metadata_key, parse_streams_key,
        },
        test_support::peek_version,
    };

    /// Kafka 4.3.1's generated writer puts a tagged field only when it is not
    /// its default, in tag order. Each row is the whole value: version, epoch,
    /// hash, then the trailer.
    #[test]
    fn group_metadata_bytes_match_kafka_schema() {
        const HEAD: &[u8] = b"\x00\x00\x00\x00\x00\x07\x01\x02\x03\x04\x05\x06\x07\x08";
        let value = |validated, configs: Option<&[(&str, &str)]>| StreamsGroupMetadataValue {
            description: DescriptionEpochs::default(),
            epoch: 7,
            metadata_hash: 0x0102_0304_0506_0708,
            validated_topology_epoch: validated,
            last_assignment_configs: configs.map(|configs| {
                configs
                    .iter()
                    .map(|(key, value)| LastAssignmentConfig {
                        key: (*key).into(),
                        value: (*value).into(),
                    })
                    .collect()
            }),
        };
        let rows: [(&str, StreamsGroupMetadataValue, &[u8]); 4] = [
            (
                "both tagged fields at their defaults",
                value(-1, None),
                b"\x00",
            ),
            (
                "Kafka's bump record: a validated epoch and num.standby.replicas",
                value(0, Some(&[("num.standby.replicas", "0")])),
                b"\x02\x00\x04\x00\x00\x00\x00\x01\x19\x02\x15num.standby.replicas\x020\x00",
            ),
            (
                "an empty configuration list is not null",
                value(-1, Some(&[])),
                b"\x01\x01\x01\x01",
            ),
            (
                "two configurations",
                value(3, Some(&[("a", "1"), ("b", "22")])),
                b"\x02\x00\x04\x00\x00\x00\x03\x01\x0c\x03\x02a\x021\x00\x02b\x0322\x00",
            ),
        ];
        check_metadata_trailers(HEAD, rows);
    }

    /// Kafka trunk's KIP-1331 tags 2 and 3 follow the 4.3.1 tags, each only
    /// when it is not -1. A value that holds neither, which is what the
    /// broker writes unless the topology description plugin is configured,
    /// is byte for byte the 4.3.1 record; a record with them decodes whole
    /// whatever the broker writes.
    #[test]
    fn group_metadata_trunk_tags_match_kafka_trunk() {
        const HEAD: &[u8] = b"\x00\x00\x00\x00\x00\x07\x01\x02\x03\x04\x05\x06\x07\x08";
        let value = |stored, failed| StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: 0x0102_0304_0506_0708,
            validated_topology_epoch: 4,
            last_assignment_configs: None,
            description: DescriptionEpochs { stored, failed },
        };
        let rows: [(&str, StreamsGroupMetadataValue, &[u8]); 4] = [
            (
                "4.3.1: no description epoch",
                value(-1, -1),
                b"\x01\x00\x04\x00\x00\x00\x04",
            ),
            (
                "trunk: a stored description",
                value(4, -1),
                b"\x02\x00\x04\x00\x00\x00\x04\x02\x04\x00\x00\x00\x04",
            ),
            (
                "trunk: an uncertain store and a failed epoch",
                value(-2, 3),
                b"\x03\x00\x04\x00\x00\x00\x04\x02\x04\xff\xff\xff\xfe\x03\x04\x00\x00\x00\x03",
            ),
            (
                "trunk: a failed epoch alone",
                value(-1, 0),
                b"\x02\x00\x04\x00\x00\x00\x04\x03\x04\x00\x00\x00\x00",
            ),
        ];
        check_metadata_trailers(HEAD, rows);
    }

    fn check_metadata_trailers(
        head: &[u8],
        rows: impl IntoIterator<Item = (&'static str, StreamsGroupMetadataValue, &'static [u8])>,
    ) {
        for (row, value, trailer) in rows {
            let bytes = value.encode();
            assert!(bytes[..] == [head, trailer].concat()[..], "{row}");
            assert!(
                StreamsGroupMetadataValue::decode(&bytes).unwrap() == value,
                "{row}"
            );
        }
    }

    #[test]
    fn group_metadata_round_trip() {
        let kb = encode_group_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_GROUP_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::GroupMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupMetadataValue {
            epoch: 7,
            metadata_hash: -9,
            validated_topology_epoch: 2,
            last_assignment_configs: Some(vec![LastAssignmentConfig {
                key: "num.standby.replicas".into(),
                value: "1".into(),
            }]),
            description: DescriptionEpochs {
                stored: 2,
                failed: -1,
            },
        };
        assert!(StreamsGroupMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    /// i16 version | i32 `AssignmentEpoch` | tagged-field trailer, which holds
    /// `AssignmentTimestamp` as tag 0 with an eight-byte payload unless it is
    /// the default 0.
    #[test]
    fn target_assignment_metadata_bytes_match_kafka_schema() {
        // (case, value, Kafka's bytes)
        let rows: [(&str, StreamsGroupTargetAssignmentMetadataValue, &[u8]); 2] = [
            (
                "the time is unknown",
                StreamsGroupTargetAssignmentMetadataValue {
                    assignment_epoch: 12,
                    assignment_timestamp_ms: 0,
                },
                b"\x00\x00\x00\x00\x00\x0c\x00",
            ),
            (
                "an assignment that finished at 2026-10-07T00:00:00Z",
                StreamsGroupTargetAssignmentMetadataValue {
                    assignment_epoch: 12,
                    assignment_timestamp_ms: 1_791_331_200_000,
                },
                b"\x00\x00\x00\x00\x00\x0c\x01\x00\x08\x00\x00\x01\xa1\x13\xa8\xec\x00",
            ),
        ];
        for (case, value, bytes) in rows {
            assert!(&value.encode()[..] == bytes, "{case}");
            assert!(
                StreamsGroupTargetAssignmentMetadataValue::decode(bytes).unwrap() == value,
                "{case}"
            );
        }
    }

    #[test]
    fn target_assignment_metadata_round_trip() {
        let kb = encode_target_assignment_metadata_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: 12,
            assignment_timestamp_ms: 7,
        };
        assert!(StreamsGroupTargetAssignmentMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn epoch_records_reject_a_missing_tagged_trailer() {
        let g = StreamsGroupMetadataValue {
            epoch: 1,
            metadata_hash: 0,
            validated_topology_epoch: NO_VALIDATED_TOPOLOGY_EPOCH,
            last_assignment_configs: None,
            description: DescriptionEpochs::default(),
        }
        .encode();
        assert!(StreamsGroupMetadataValue::decode(&g[..g.len() - 1]).is_err());
    }
}
