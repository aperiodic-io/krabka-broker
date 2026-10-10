//! The streams topology record at key version 23.
//!
//! The value holds the topology epoch and one [`StoredSubtopology`] per
//! subtopology. A subtopology names its source topics, both exact and regex,
//! the repartition sinks it produces, and the changelog and repartition-source
//! topics the coordinator must materialize, each described by a
//! [`StoredTopicInfo`]. A [`StoredCopartitionGroup`] records which of those
//! topics must be copartitioned, by index into the subtopology's own lists.
//!
//! # Layout
//!
//! From `StreamsGroupTopologyValue.json` at Apache Kafka tag `4.3.1`, which
//! declares `"flexibleVersions": "0+"`. `Epoch` (int32), then `Subtopologies`
//! (`[]Subtopology`), whose fields are in this order: `SubtopologyId` (string),
//! `SourceTopics` (`[]string`), `SourceTopicRegex` (`[]string`),
//! `StateChangelogTopics` (`[]TopicInfo`), `RepartitionSinkTopics`
//! (`[]string`), `RepartitionSourceTopics` (`[]TopicInfo`) and
//! `CopartitionGroups` (`[]CopartitionGroup`).
//!
//! `TopicInfo` is `{Name string, Partitions int32, ReplicationFactor int16,
//! TopicConfigs []TopicConfig{key string, value string}}`, and
//! `CopartitionGroup` is three `[]int16` of indices. Every string and array is
//! compact, and every struct as well as the message ends with a tagged-field
//! count.

use bytes::{BufMut, BytesMut};

use super::codec::{
    decode_i16_list, decode_key_value_list, encode_i16_list, encode_key_value_list,
};
use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array, get_compact_string, get_string_array, put_compact_array,
            put_compact_string, put_empty_tagged_fields, put_string_array, skip_tagged_fields,
            value_codec,
        },
        get_i16, get_i32,
    },
    error::BrokerError,
};

/// An internal, changelog, or repartition topic that a subtopology refers to.
/// It carries the partition count, the replication factor, and the per-topic
/// config overrides that the coordinator should materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredTopicInfo {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,
    pub topic_configs: Vec<(String, String)>,
}

impl StoredTopicInfo {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_compact_string(buf, &self.name);
        buf.put_i32(self.partitions);
        buf.put_i16(self.replication_factor);
        encode_key_value_list(buf, &self.topic_configs);
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let name = get_compact_string(buf)?;
        let partitions = get_i32(buf)?;
        let replication_factor = get_i16(buf)?;
        let topic_configs = decode_key_value_list(buf)?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
        })
    }
}

/// A copartition group. It holds the indices, into the enclosing subtopology's
/// topic lists, of the topics that must be copartitioned with one another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCopartitionGroup {
    pub source_topics: Vec<i16>,
    pub source_topic_regex: Vec<i16>,
    pub repartition_source_topics: Vec<i16>,
}

impl StoredCopartitionGroup {
    fn encode_into(&self, buf: &mut BytesMut) {
        encode_i16_list(buf, &self.source_topics);
        encode_i16_list(buf, &self.source_topic_regex);
        encode_i16_list(buf, &self.repartition_source_topics);
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let group = Self {
            source_topics: decode_i16_list(buf)?,
            source_topic_regex: decode_i16_list(buf)?,
            repartition_source_topics: decode_i16_list(buf)?,
        };
        skip_tagged_fields(buf)?;
        Ok(group)
    }
}

/// One subtopology of a streams topology. It holds the source topics, both
/// exact and regex, the repartition sinks it produces and the repartition
/// sources it consumes, its changelog topics, and any copartition
/// constraints.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredSubtopology {
    pub subtopology_id: String,
    pub source_topics: Vec<String>,
    pub source_topic_regex: Vec<String>,
    pub repartition_sink_topics: Vec<String>,
    pub state_changelog_topics: Vec<StoredTopicInfo>,
    pub repartition_source_topics: Vec<StoredTopicInfo>,
    pub copartition_groups: Vec<StoredCopartitionGroup>,
}

impl StoredSubtopology {
    fn encode_into(&self, buf: &mut BytesMut) {
        put_compact_string(buf, &self.subtopology_id);
        put_string_array(buf, &self.source_topics);
        put_string_array(buf, &self.source_topic_regex);
        put_compact_array(buf, self.state_changelog_topics.iter(), |buf, t| {
            t.encode_into(buf);
        });
        put_string_array(buf, &self.repartition_sink_topics);
        put_compact_array(buf, self.repartition_source_topics.iter(), |buf, t| {
            t.encode_into(buf);
        });
        put_compact_array(buf, self.copartition_groups.iter(), |buf, cg| {
            cg.encode_into(buf);
        });
        put_empty_tagged_fields(buf);
    }
    fn decode_from(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let subtopology_id = get_compact_string(buf)?;
        let source_topics = get_string_array(buf)?;
        let source_topic_regex = get_string_array(buf)?;
        let state_changelog_topics = get_compact_array(buf, StoredTopicInfo::decode_from)?;
        let repartition_sink_topics = get_string_array(buf)?;
        let repartition_source_topics = get_compact_array(buf, StoredTopicInfo::decode_from)?;
        let copartition_groups = get_compact_array(buf, StoredCopartitionGroup::decode_from)?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            subtopology_id,
            source_topics,
            source_topic_regex,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
            copartition_groups,
        })
    }
}

/// Key v23 value: the group's resolved topology, that is the epoch and the
/// subtopologies.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTopologyValue {
    pub epoch: i32,
    pub subtopologies: Vec<StoredSubtopology>,
}

value_codec! {
    StreamsGroupTopologyValue("StreamsGroupTopologyValue"),
    encode(&self) -> buf {
        buf.put_i32(self.epoch);
        put_compact_array(buf, self.subtopologies.iter(), |buf, s| {
            s.encode_into(buf);
        });
    }
    decode(buf) {
        let epoch = get_i32(buf)?;
        let subtopologies = get_compact_array(buf, StoredSubtopology::decode_from)?;
        Ok(Self {
            epoch,
            subtopologies,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        streams::{
            persistence::{
                KEY_STREAMS_TOPOLOGY, StreamsGroupKey, encode_topology_key, parse_streams_key,
            },
            topology::test_support::{example_subtopology, sub},
        },
        test_support::{peek_version, wire_bytes},
    };

    #[test]
    fn topology_round_trip() {
        let kb = encode_topology_key("g1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TOPOLOGY);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::Topology {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupTopologyValue {
            epoch: 2,
            subtopologies: vec![example_subtopology(), sub("1")],
        };
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn topology_bytes_match_kafka_schema() {
        let v = StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies: vec![StoredSubtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["s".into()],
                source_topic_regex: vec![],
                repartition_sink_topics: vec![],
                state_changelog_topics: vec![StoredTopicInfo {
                    name: "c".into(),
                    partitions: 0,
                    replication_factor: 3,
                    topic_configs: vec![],
                }],
                repartition_source_topics: vec![],
                copartition_groups: vec![],
            }],
        };
        let want = wire_bytes(&[
            "0000", "00000001", // Epoch
            "02",       // one Subtopology
            "0230",     // SubtopologyId
            "020273",   // SourceTopics
            "01",       // empty SourceTopicRegex
            "02",       // one StateChangelogTopics entry
            "0263",     // Name
            "00000000", // Partitions
            "0003",     // ReplicationFactor
            "01",       // empty TopicConfigs
            "00",       // TopicInfo tagged fields
            "01",       // empty RepartitionSinkTopics
            "01",       // empty RepartitionSourceTopics
            "01",       // empty CopartitionGroups
            "00",       // Subtopology tagged fields
            "00",       // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn topology_rejects_a_missing_tagged_trailer() {
        let full = StreamsGroupTopologyValue::default().encode();
        assert!(StreamsGroupTopologyValue::decode(&full[..full.len() - 1]).is_err());
    }

    #[test]
    fn topology_empty_round_trip() {
        let v = StreamsGroupTopologyValue::default();
        assert!(StreamsGroupTopologyValue::decode(&v.encode()).unwrap() == v);
    }
}
