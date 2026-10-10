//! The KIP-932 share-group state partition metadata record at key version 15.
//!
//! # Layout
//!
//! From `ShareGroupStatePartitionMetadataValue.json` at Apache Kafka tag
//! `4.3.1`, which declares `"flexibleVersions": "0+"`. Three plain arrays, in
//! this order:
//!
//! - `InitializingTopics` (`[]TopicPartitionsInfo{TopicId uuid, TopicName
//!   string, Partitions []int32}`),
//! - `InitializedTopics` (the same struct),
//! - `DeletingTopics` (`[]TopicInfo{TopicId uuid, TopicName string}`).
//!
//! # Topic names
//!
//! `__consumer_offsets` is read by tooling that has no access to the broker's
//! topic-id index, so every entry carries the topic's real name, exactly as
//! `GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord`
//! writes `InitMapValue.name()`. The name reaches this record from the
//! metadata image, and it survives a restart because the decoder keeps the
//! name each entry was written with.
//!
//! A name the broker cannot resolve is written as [`UNKNOWN_TOPIC_NAME`],
//! which is what `GroupMetadataManager.attachTopicName` and
//! `GroupMetadataManager.attachInitValue` write for a topic id the metadata
//! image no longer holds.
//!
//! The group writes a partition to `InitializingTopics` before it asks the
//! persister to initialize it, and moves it to `InitializedTopics` once the
//! persister answers, as Kafka's `GroupMetadataManager.addInitializingTopicsRecords`
//! and `initializeShareGroupState` do. It writes a topic to `DeletingTopics`
//! before it asks the persister to delete the topic's share state, and takes
//! it out once the persister answers, as Kafka's
//! `sharePartitionsEligibleForOffsetDeletion`,
//! `shareGroupBuildPartitionDeleteRequest` and
//! `completeDeleteShareGroupOffsets` do.

use crate::{
    coordinator::unified::persistence::flex::{
        get_compact_array, get_compact_string, get_i32_array, get_uuid, put_compact_array,
        put_compact_string, put_empty_tagged_fields, put_i32_array, put_uuid, skip_tagged_fields,
        value_codec,
    },
    error::BrokerError,
};

/// The `TopicName` written for a topic id the metadata image cannot resolve,
/// byte for byte what Kafka's `GroupMetadataManager` writes in the same spot.
pub const UNKNOWN_TOPIC_NAME: &str = "<UNKNOWN>";

/// One `TopicPartitionsInfo` of the `InitializingTopics` or
/// `InitializedTopics` array.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicPartitionsInfo {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

/// One `TopicInfo` of the `DeletingTopics` array.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletingTopic {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
}

/// KIP-932 `ShareGroupStatePartitionMetadata`, key v15.
///
/// The record tracks which `(topic_id, partition)` share-states a group has
/// initialized. It also holds the topics whose share-state the broker is
/// deleting. The record lets the group coordinator skip the re-initialization
/// of partitions across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupStatePartitionMetadataValue {
    pub initializing: Vec<TopicPartitionsInfo>,
    pub initialized: Vec<TopicPartitionsInfo>,
    pub deleting: Vec<DeletingTopic>,
}

value_codec! {
    ShareGroupStatePartitionMetadataValue("ShareGroupStatePartitionMetadataValue"),
    encode(&self) -> buf {
        for topics in [&self.initializing, &self.initialized] {
            put_compact_array(buf, topics.iter(), |buf, topic| {
                put_uuid(buf, *topic.topic_id.as_bytes());
                put_compact_string(buf, &topic.topic_name);
                put_i32_array(buf, &topic.partitions);
                put_empty_tagged_fields(buf);
            });
        }
        put_compact_array(buf, self.deleting.iter(), |buf, topic| {
            put_uuid(buf, *topic.topic_id.as_bytes());
            put_compact_string(buf, &topic.topic_name);
            put_empty_tagged_fields(buf);
        });
    }
    decode(buf) {
        let initializing = get_topic_partitions_infos(buf)?;
        let initialized = get_topic_partitions_infos(buf)?;
        let deleting = get_compact_array(buf, |buf| {
            let topic_id = uuid::Uuid::from_bytes(get_uuid(buf)?);
            let topic_name = get_compact_string(buf)?;
            skip_tagged_fields(buf)?;
            Ok(DeletingTopic {
                topic_id,
                topic_name,
            })
        })?;
        Ok(Self {
            initializing,
            initialized,
            deleting,
        })
    }
}

/// Decodes one `[]TopicPartitionsInfo` array.
fn get_topic_partitions_infos(buf: &mut &[u8]) -> Result<Vec<TopicPartitionsInfo>, BrokerError> {
    get_compact_array(buf, |buf| {
        let topic_id = uuid::Uuid::from_bytes(get_uuid(buf)?);
        let topic_name = get_compact_string(buf)?;
        let partitions = get_i32_array(buf)?;
        skip_tagged_fields(buf)?;
        Ok(TopicPartitionsInfo {
            topic_id,
            topic_name,
            partitions,
        })
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        share::persistence::{KEY_SHARE_GROUP_STATE_PARTITION_METADATA, ShareGroupKey},
        test_support::wire_bytes,
    };

    fn initialized(id: u8, name: &str, partitions: Vec<i32>) -> TopicPartitionsInfo {
        TopicPartitionsInfo {
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            topic_name: name.to_owned(),
            partitions,
        }
    }

    fn deleting(id: u8, name: &str) -> DeletingTopic {
        DeletingTopic {
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            topic_name: name.to_owned(),
        }
    }

    #[test]
    fn state_partition_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupStatePartitionMetadataValue {
            initializing: vec![],
            initialized: vec![initialized(1, "orders", vec![0])],
            deleting: vec![deleting(9, "carts")],
        };
        let want = wire_bytes(&[
            "0000",
            "01", // empty InitializingTopics
            "02", // one InitializedTopics entry
            "01010101010101010101010101010101",
            "07", // TopicName "orders"
            "6f7264657273",
            "02", // one partition
            "00000000",
            "00", // TopicPartitionsInfo tagged fields
            "02", // one DeletingTopics entry
            "09090909090909090909090909090909",
            "06", // TopicName "carts"
            "6361727473",
            "00", // TopicInfo tagged fields
            "00", // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn state_partition_metadata_round_trip() {
        let key = ShareGroupKey::StatePartitionMetadata {
            group_id: "g1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_GROUP_STATE_PARTITION_METADATA),
        );

        let cases = [
            ShareGroupStatePartitionMetadataValue::default(),
            ShareGroupStatePartitionMetadataValue {
                initializing: vec![initialized(5, "returns", vec![3])],
                initialized: vec![
                    initialized(1, "orders", vec![0, 1, 2]),
                    initialized(2, "shipments", vec![]),
                ],
                deleting: vec![deleting(9, "carts")],
            },
            // A name the writer could not resolve stays exactly what Kafka
            // would have written, and a name with multi-byte characters keeps
            // its byte length through the compact-string codec.
            ShareGroupStatePartitionMetadataValue {
                initializing: vec![],
                initialized: vec![initialized(3, UNKNOWN_TOPIC_NAME, vec![7])],
                deleting: vec![deleting(4, "temperaturmålinger")],
            },
        ];
        for v in cases {
            assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
        }
    }

    #[test]
    fn state_partition_metadata_decodes_an_initializing_list() {
        // An InitializingTopics entry decodes, and it does not shift the
        // fields after it.
        let mut bytes: Vec<u8> = vec![0x00, 0x00];
        bytes.push(0x02); // one InitializingTopics entry
        bytes.extend_from_slice(&[7u8; 16]);
        bytes.extend_from_slice(b"\x02t"); // TopicName "t"
        bytes.push(0x02);
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.push(0x00);
        bytes.push(0x01); // empty InitializedTopics
        bytes.push(0x01); // empty DeletingTopics
        bytes.push(0x00); // message tagged fields
        assert!(
            ShareGroupStatePartitionMetadataValue::decode(&bytes).unwrap()
                == ShareGroupStatePartitionMetadataValue {
                    initializing: vec![initialized(7, "t", vec![4])],
                    ..Default::default()
                }
        );
    }

    #[test]
    fn state_partition_metadata_rejects_a_missing_tagged_trailer() {
        let full = ShareGroupStatePartitionMetadataValue::default().encode();
        assert!(ShareGroupStatePartitionMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
