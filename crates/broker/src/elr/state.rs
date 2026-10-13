//! The published ELR value: its grammar, and the projection
//! `DescribeTopicPartitions` reads out of it.
//!
//! ## What the last-known ELR holds
//!
//! Kafka 4.3.1 keeps the single replica that led the partition when it lost
//! its leader in `lastKnownElr`, and clears it as soon as the partition has a
//! leader again (`PartitionChangeBuilder.maybeUpdateLastKnownLeader`). So a
//! one-member last-known ELR is also how a partition says it has no leader.
//! krabka's partition record cannot say it itself, because
//! `PartitionRecord::leader` always names a replica: a partition without a
//! leader keeps its last leader in that field, and [`is_leaderless`] reads the
//! one-member last-known ELR that names it as the marker. `Metadata` and
//! `DescribeTopicPartitions` project such a partition as `leader = -1`.
//!
//! ## Nullable versus empty
//!
//! Both response fields are nullable in the schema and both default to null,
//! but a real broker never sends null: `KRaftMetadataCache` builds them with
//! `Replicas.toList`, which returns an empty list for an empty replica array.
//! The distinction is visible in the tool. `TopicCommand.PartitionDescription`
//! prints `Elr: N/A` and `LastKnownElr: N/A` for a null and prints the joined
//! ids -- empty for an empty list -- otherwise, so a null would make
//! `kafka-topics --describe` read as "this broker does not know" against a
//! cluster where the true answer is "none". `apache/kafka:4.3.1` renders
//! `Elr: ` and `LastKnownElr: ` for a healthy topic. So does krabka:
//! [`TopicElr::partition`] always returns lists, and the handler always wraps
//! them in `Some`.

use {
    crate::features::{ELR_VERSION, feature_enabled},
    krabka_metadata::{MetadataImage, NodeId, PartitionRecord},
    std::collections::BTreeMap,
};

const LEGACY_ELR_CONFIG: &str = "krabka.elr";

/// One partition's ELR state, in the wire types the response uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PartitionElr {
    /// `DescribeTopicPartitionsResponsePartition.eligible_leader_replicas`.
    pub(crate) eligible_leader_replicas: Vec<i32>,
    /// `DescribeTopicPartitionsResponsePartition.last_known_elr`.
    pub(crate) last_known_elr: Vec<i32>,
}

impl PartitionElr {
    /// `true` when the partition carries no ELR state at all. Such a
    /// partition is left out of the published value entirely.
    pub(crate) fn is_empty(&self) -> bool {
        self.eligible_leader_replicas.is_empty() && self.last_known_elr.is_empty()
    }

    /// Whether this state says the partition has no leader, its last leader
    /// being `recorded_leader`: the last-known ELR holds exactly that one
    /// replica. Kafka's controller writes it when the partition loses its
    /// leader and clears it when a leader is elected, so it is never set while
    /// one exists.
    pub(crate) fn is_leaderless(&self, recorded_leader: NodeId) -> bool {
        matches!(
            self.last_known_elr.as_slice(),
            [only] if u64::try_from(*only) == Ok(recorded_leader.0)
        )
    }
}

/// Every partition of one topic that carries ELR state.
///
/// A topic with no state parses to an empty map, and every partition then
/// projects as [`PartitionElr::default`]: two empty lists, never null.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TopicElr(BTreeMap<i32, PartitionElr>);

impl TopicElr {
    /// Read the ELR state `image` holds for `topic`.
    ///
    /// One call per topic, not per partition: the handler walks a topic's
    /// partitions in a single pass and each lookup here is a config-map hit
    /// plus a parse of the whole value.
    pub(crate) fn of_topic(image: &MetadataImage, topic: &str) -> Self {
        let mut state = if feature_enabled(image, ELR_VERSION, 1) {
            image
                .topic_config(topic)
                .and_then(|config| config.get(LEGACY_ELR_CONFIG))
                .map_or_else(Self::default, |value| Self::parse(value))
        } else {
            Self::default()
        };
        for partition in image.partitions_of(topic) {
            let (eligible, last_known) = image.partition_elr(topic, partition.partition);
            let dedicated = PartitionElr {
                eligible_leader_replicas: wire_node_ids(eligible.iter().copied()),
                last_known_elr: wire_node_ids(last_known.iter().copied()),
            };
            if !dedicated.is_empty() {
                state.0.insert(partition.partition, dedicated);
            }
        }
        state
    }

    /// Parse the [`ELIGIBLE_LEADER_REPLICAS`] config value.
    ///
    /// The grammar is
    ///
    /// ```text
    /// value := entry (";" entry)*
    /// entry := partition ":" ids ":" ids
    /// ids   := (id ("," id)*)?
    /// ```
    ///
    /// where `partition` is the partition index and each `ids` is the
    /// eligible-leader set and then the last-known set, as node ids. So
    /// `0:2,3:;4::5` says partition 0 has ELR `{2,3}` and no last-known ELR,
    /// and partition 4 has no ELR and a last-known ELR of `{5}`. Partitions
    /// with neither are left out of the value entirely.
    ///
    /// Parsing is total: a malformed entry is dropped rather than failing the
    /// request, because the alternative is a `DescribeTopicPartitions` that
    /// errors for a whole topic over a config the client cannot even see. A
    /// dropped entry reads as "no ELR", the same answer the topic gives before
    /// the controller has ever published the key.
    pub(crate) fn parse(value: &str) -> Self {
        Self(
            value
                .split(';')
                .filter(|entry| !entry.is_empty())
                .filter_map(parse_entry)
                .map(|ParsedEntry { partition, elr }| (partition, elr))
                .collect(),
        )
    }

    /// The ELR state of one partition. Absent partitions project as two empty
    /// lists, which is the "no ELR" answer Kafka gives.
    pub(crate) fn partition(&self, partition: i32) -> PartitionElr {
        self.0.get(&partition).cloned().unwrap_or_default()
    }

    pub(crate) fn records(&self, topic: &str) -> Vec<krabka_metadata::MetadataRecord> {
        self.0
            .iter()
            .map(|(partition, elr)| {
                krabka_metadata::MetadataRecord::V1PartitionElr(
                    krabka_metadata::PartitionElrRecord {
                        topic: topic.to_owned(),
                        partition: *partition,
                        eligible_leader_replicas: metadata_node_ids(&elr.eligible_leader_replicas),
                        last_known_elr: metadata_node_ids(&elr.last_known_elr),
                    },
                )
            })
            .collect()
    }
}

/// Whether `image` records `partition` as having no leader.
///
/// See [`PartitionElr::is_leaderless`]. The projections that answer for a
/// partition -- `Metadata` and `DescribeTopicPartitions` -- read it here.
pub(crate) fn is_leaderless(image: &MetadataImage, partition: &PartitionRecord) -> bool {
    matches!(
        image.partition_elr(&partition.topic, partition.partition).1,
        [only] if *only == partition.leader
    )
}

#[cfg(test)]
pub(crate) fn test_records(topic: &str, value: &str) -> Vec<krabka_metadata::MetadataRecord> {
    TopicElr::parse(value).records(topic)
}

pub(crate) fn legacy_elr(image: &MetadataImage, topic: &str) -> Option<TopicElr> {
    image
        .topic_config(topic)?
        .get(LEGACY_ELR_CONFIG)
        .map(|value| TopicElr::parse(value))
}

pub(crate) fn without_legacy_elr(
    image: &MetadataImage,
    topic: &str,
    replacement: Option<&std::collections::BTreeMap<String, String>>,
) -> Option<krabka_metadata::MetadataRecord> {
    let mut overrides = replacement
        .cloned()
        .or_else(|| image.topic_config(topic).cloned())?;
    overrides.remove(LEGACY_ELR_CONFIG)?;
    Some(krabka_metadata::MetadataRecord::V1TopicConfig(
        krabka_metadata::TopicConfigRecord {
            topic: topic.to_owned(),
            overrides,
        },
    ))
}

pub(super) fn metadata_node_ids(ids: &[i32]) -> Vec<NodeId> {
    ids.iter()
        .filter_map(|id| u64::try_from(*id).ok().map(NodeId))
        .collect()
}

/// Narrow metadata node ids to the wire type, dropping any that do not fit.
///
/// A node id wider than `i32` cannot be named in a
/// `DescribeTopicPartitionsResponsePartition`, so it could never be reported
/// even if it were published. Dropping it here keeps the published value
/// readable rather than storing an id the projection would have to invent an
/// answer for.
pub(crate) fn wire_node_ids(nodes: impl IntoIterator<Item = NodeId>) -> Vec<i32> {
    nodes
        .into_iter()
        .filter_map(|node| i32::try_from(node.0).ok())
        .collect()
}

/// Parse one `partition:ids:ids` entry. `None` drops the entry.
fn parse_entry(entry: &str) -> Option<ParsedEntry> {
    let mut fields = entry.split(':');
    let partition: i32 = fields.next()?.parse().ok()?;
    let eligible_leader_replicas = parse_ids(fields.next()?)?;
    let last_known_elr = parse_ids(fields.next()?)?;
    // A fourth field means the writer used a grammar this reader does not
    // know, so the entry is not safe to project.
    if fields.next().is_some() {
        return None;
    }
    Some(ParsedEntry {
        partition,
        elr: PartitionElr {
            eligible_leader_replicas,
            last_known_elr,
        },
    })
}

/// The ELR state of one partition, as an entry names it.
struct ParsedEntry {
    partition: i32,
    elr: PartitionElr,
}

/// Parse a possibly-empty comma-separated node-id list.
fn parse_ids(ids: &str) -> Option<Vec<i32>> {
    if ids.is_empty() {
        return Some(Vec::new());
    }
    ids.split(',').map(|id| id.parse().ok()).collect()
}

#[cfg(test)]
mod tests;
