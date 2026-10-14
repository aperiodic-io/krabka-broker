//! Kafka-compatible `__consumer_offsets` partition selection.

use {
    super::bootstrap::{OFFSETS_NUM_PARTITIONS, OFFSETS_TOPIC},
    krabka_metadata::MetadataImage,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupRoutingError {
    Unavailable,
    NotCoordinator,
}

/// Select a group-id's `__consumer_offsets` partition with Kafka's
/// `Utils.abs(groupId.hashCode()) % partitionCount` rule.
///
/// The verified kernel does the Java UTF-16 `String.hashCode`, the
/// `Utils.abs(Integer.MIN_VALUE) == 0` corner and the modulo; the
/// transaction and SHARE coordinators route through the same kernel.
///
/// # Panics
///
/// Panics when `partition_count` is not positive. The offsets topic always
/// has at least one partition, and the bootstrap default is positive.
#[must_use]
pub(crate) fn partition_for_group_with_count(group_id: &str, partition_count: i32) -> i32 {
    let utf16: Vec<u16> = group_id.encode_utf16().collect();
    krabka_verified::broker::java_string_hash_partition(&utf16, partition_count)
        .expect("offsets topic partition count must be positive")
}

/// Select from the live offsets-topic partition count, falling back to the
/// bootstrap default while metadata is not available yet.
#[must_use]
pub(crate) fn partition_for_group(image: &MetadataImage, group_id: &str) -> i32 {
    let count = image.topic_partition_count(OFFSETS_TOPIC);
    partition_for_group_with_count(
        group_id,
        if count > 0 {
            count
        } else {
            OFFSETS_NUM_PARTITIONS
        },
    )
}

/// Resolve the group partition and verify that `node_id` is its current
/// leader. Group RPC handlers use this before they create or access actors.
pub(crate) fn local_partition_for_group(
    image: &MetadataImage,
    node_id: krabka_raft::NodeId,
    group_id: &str,
) -> Result<i32, GroupRoutingError> {
    let partition = partition_for_group(image, group_id);
    let record = image
        .partition(OFFSETS_TOPIC, partition)
        .ok_or(GroupRoutingError::Unavailable)?;
    if record.leader != node_id {
        return Err(GroupRoutingError::NotCoordinator);
    }
    Ok(partition)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Goldens from the JVM: `Utils.abs(groupId.hashCode()) % partitionCount`.
    #[yare::parameterized(
        empty = { "", 50, 0 },
        abc_seven_partitions = { "abc", 7, 6 },
        abc_single_partition = { "abc", 1, 0 },
        negative_hash = { "consumer-group", 50, 38 },
        supplementary_character = { "🦀", 50, 2 },
        minimum_hash = { "polygenelubricants", 50, 0 },
    )]
    fn group_partition_matches_jvm_goldens(
        group_id: &'static str,
        partition_count: i32,
        expected: i32,
    ) {
        check!(partition_for_group_with_count(group_id, partition_count) == expected);
    }

    #[test]
    fn partition_for_group_falls_back_when_offsets_topic_is_unloaded() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        check!(partition_for_group(&image, "consumer-group") == 38);
    }

    #[test]
    fn partition_for_group_uses_live_partition_count_when_present() {
        let image = ten_partition_image();
        check!(partition_for_group(&image, "consumer-group") == 8);
    }

    #[test]
    fn local_partition_for_group_routes_or_reports_errors() {
        use uuid::Uuid;

        let empty = MetadataImage::new(Uuid::nil());
        check!(
            local_partition_for_group(&empty, krabka_raft::NodeId(1), "consumer-group")
                == Err(GroupRoutingError::Unavailable)
        );

        let image = ten_partition_image();

        check!(
            local_partition_for_group(&image, krabka_raft::NodeId(1), "consumer-group") == Ok(8)
        );
        check!(
            local_partition_for_group(&image, krabka_raft::NodeId(2), "consumer-group")
                == Err(GroupRoutingError::NotCoordinator)
        );
    }
    fn ten_partition_image() -> MetadataImage {
        use {
            krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord},
            uuid::Uuid,
        };
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: OFFSETS_TOPIC.into(),
            topic_id: Uuid::from_u128(1),
            partitions: 10,
            replication_factor: 1,
        }));
        for p in 0..10 {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: OFFSETS_TOPIC.into(),
                partition: p,
                leader: krabka_raft::NodeId(1),
                ..PartitionRecord::default()
            }));
        }
        image
    }
}
