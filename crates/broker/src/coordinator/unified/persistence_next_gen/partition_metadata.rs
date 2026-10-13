//! The KIP-848 subscription metadata record at key version 4, which Kafka 4.1
//! deprecated.
//!
//! Before 4.1 a consumer group kept the metadata of its subscribed topics in
//! a `ConsumerGroupPartitionMetadata` record. KIP-1101 replaced it with the
//! `MetadataHash` of `ConsumerGroupMetadataValue`, but Kafka 4.3.1 still knows
//! the record type: its loader decodes the record, and its replay creates the
//! consumer group and remembers that the log holds such a record, so that the
//! next heartbeat that refreshes the group's metadata, or the group's
//! deletion, tombstones it. Kafka 4.3.1 never writes a value for it.
//!
//! # Layout
//!
//! From `ConsumerGroupPartitionMetadataKey.json` and
//! `ConsumerGroupPartitionMetadataValue.json` at Apache Kafka tag `4.3.1`. The
//! key is the group id. The value declares `"validVersions": "0"` and
//! `"flexibleVersions": "0+"`: a compact array of `TopicMetadata` (`TopicId`
//! uuid, `TopicName` string, `NumPartitions` int32, and a compact array of
//! `PartitionMetadata`, each a `Partition` int32 and a compact array of
//! `Racks` strings), every struct and the message ending with a tagged-field
//! count.

use {
    crate::{
        coordinator::unified::persistence::{
            flex::{
                get_compact_array_len, get_compact_string, get_string_array, get_uuid,
                get_value_version, put_compact_array_len, put_compact_string,
                put_empty_tagged_fields, put_string_array, put_uuid, skip_tagged_fields,
            },
            get_i32,
        },
        error::BrokerError,
    },
    bytes::{BufMut, Bytes, BytesMut},
};

/// Kafka's `ConsumerGroupPartitionMetadataValue.PartitionMetadata`, the racks
/// of one partition. Kafka stopped filling it in 4.0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRacks {
    pub partition: i32,
    pub racks: Vec<String>,
}

/// Kafka's `ConsumerGroupPartitionMetadataValue.TopicMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribedTopicMetadata {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
    pub num_partitions: i32,
    pub partition_metadata: Vec<PartitionRacks>,
}

/// Key v4 value: the subscription metadata a pre-4.1 coordinator kept.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionMetadataValue {
    pub topics: Vec<SubscribedTopicMetadata>,
}

impl PartitionMetadataValue {
    /// Encodes the value as Kafka's generated writer does. The broker never
    /// writes one; tests use it to build what an older Kafka wrote.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_array_len(&mut buf, self.topics.len());
        for topic in &self.topics {
            put_uuid(&mut buf, *topic.topic_id.as_bytes());
            put_compact_string(&mut buf, &topic.topic_name);
            buf.put_i32(topic.num_partitions);
            put_compact_array_len(&mut buf, topic.partition_metadata.len());
            for partition in &topic.partition_metadata {
                buf.put_i32(partition.partition);
                put_string_array(&mut buf, &partition.racks);
                put_empty_tagged_fields(&mut buf);
            }
            put_empty_tagged_fields(&mut buf);
        }
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when the value version is not 0, or when the value
    /// ends before a field.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        get_value_version(
            &mut buf,
            0,
            "unknown ConsumerGroupPartitionMetadataValue version",
        )?;
        let topic_count = get_compact_array_len(&mut buf)?;
        let mut topics = Vec::with_capacity(topic_count.min(buf.len()));
        for _ in 0..topic_count {
            let topic_id = uuid::Uuid::from_bytes(get_uuid(&mut buf)?);
            let topic_name = get_compact_string(&mut buf)?;
            let num_partitions = get_i32(&mut buf)?;
            let partition_count = get_compact_array_len(&mut buf)?;
            let mut partition_metadata = Vec::with_capacity(partition_count.min(buf.len()));
            for _ in 0..partition_count {
                let partition = get_i32(&mut buf)?;
                let racks = get_string_array(&mut buf)?;
                skip_tagged_fields(&mut buf)?;
                partition_metadata.push(PartitionRacks { partition, racks });
            }
            skip_tagged_fields(&mut buf)?;
            topics.push(SubscribedTopicMetadata {
                topic_id,
                topic_name,
                num_partitions,
                partition_metadata,
            });
        }
        skip_tagged_fields(&mut buf)?;
        Ok(Self { topics })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    /// Golden bytes of the value Kafka's generated
    /// `ConsumerGroupPartitionMetadataValue` writer produces at version 0 for
    /// one topic with one partition on rack `r1`.
    const GOLDEN: &[u8] = b"\x00\x00\
        \x02\
        \x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\
        \x04foo\
        \x00\x00\x00\x03\
        \x02\x00\x00\x00\x01\x02\x03r1\x00\
        \x00\
        \x00";

    fn golden_value() -> PartitionMetadataValue {
        PartitionMetadataValue {
            topics: vec![SubscribedTopicMetadata {
                topic_id: uuid::Uuid::from_bytes([
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                ]),
                topic_name: "foo".into(),
                num_partitions: 3,
                partition_metadata: vec![PartitionRacks {
                    partition: 1,
                    racks: vec!["r1".into()],
                }],
            }],
        }
    }

    #[test]
    fn value_bytes_match_kafka_schema() {
        assert!(PartitionMetadataValue::decode(GOLDEN).unwrap() == golden_value());
        assert!(&golden_value().encode()[..] == GOLDEN);
    }

    #[test]
    fn value_decode_refuses_what_kafka_refuses() {
        struct Row<'a> {
            name: &'static str,
            bytes: &'a [u8],
        }

        let mut version_1 = GOLDEN.to_vec();
        version_1[1] = 1;
        let rows: [Row<'_>; 3] = [
            Row {
                name: "value version 1",
                bytes: &version_1,
            },
            Row {
                name: "truncated value",
                bytes: &GOLDEN[..GOLDEN.len() - 1],
            },
            Row {
                name: "empty value",
                bytes: b"",
            },
        ];
        for Row { name, bytes } in rows {
            check!(PartitionMetadataValue::decode(bytes).is_err(), "{name}");
        }
    }
}
