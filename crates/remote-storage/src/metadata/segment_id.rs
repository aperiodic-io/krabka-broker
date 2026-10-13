//! Identity types for a remote log segment.
//!
//! [`TopicIdPartition`] names a partition by its stable topic UUID, and
//! [`RemoteLogSegmentId`] pairs that partition with the random per-segment
//! UUID that makes one segment globally unique. Both are pure identity, which
//! is why they sit apart from the metadata record that carries them.

use {
    std::hash::{Hash, Hasher},
    uuid::Uuid,
};

/// A partition addressed by its stable topic UUID, with the topic name for
/// diagnostics.
///
/// Equality and hash use `topic_id` and `partition` only. The topic name is
/// informational, and a topic's id is its identity. This matches Kafka's
/// `TopicIdPartition`.
#[derive(Debug, Clone)]
pub struct TopicIdPartition {
    /// Stable topic UUID, as assigned at topic creation.
    pub topic_id: Uuid,
    /// Topic name (informational; not part of identity).
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

impl TopicIdPartition {
    /// Constructs a [`TopicIdPartition`].
    #[must_use]
    pub fn new(topic_id: Uuid, topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic_id,
            topic: topic.into(),
            partition,
        }
    }
}

impl PartialEq for TopicIdPartition {
    fn eq(&self, other: &Self) -> bool {
        self.topic_id == other.topic_id && self.partition == other.partition
    }
}

impl Eq for TopicIdPartition {}

impl Hash for TopicIdPartition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_id.hash(state);
        self.partition.hash(state);
    }
}

/// Globally-unique identifier for one remote log segment: the owning
/// partition plus a random per-segment UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteLogSegmentId {
    /// The partition this segment belongs to.
    pub topic_id_partition: TopicIdPartition,
    /// Random per-segment UUID.
    pub id: Uuid,
}

impl RemoteLogSegmentId {
    /// Constructs a [`RemoteLogSegmentId`] from an explicit UUID.
    #[must_use]
    pub fn new(topic_id_partition: TopicIdPartition, id: Uuid) -> Self {
        Self {
            topic_id_partition,
            id,
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, std::collections::HashSet};

    #[test]
    fn topic_id_partition_identity_ignores_name() {
        let a = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 3);
        let b = TopicIdPartition::new(Uuid::from_u128(7), "renamed", 3);
        assert!(a == b);
        let set: HashSet<_> = maplit::hashset! {a, b};
        assert!(set.len() == 1, "same id+partition must collapse in a set");
    }

    #[test]
    fn topic_id_partition_distinct_partitions_differ() {
        let a = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 0);
        let b = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 1);
        assert!(a != b);
    }
}
