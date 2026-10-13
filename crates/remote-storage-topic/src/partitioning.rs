//! Deterministic `TopicIdPartition` → metadata-topic-partition mapping.
//!
//! Every metadata event for one user partition must land in the same
//! `__remote_log_metadata` partition, so its lifecycle ordering survives the
//! round trip through Kafka. The hash inputs are `topic_id` and `partition`,
//! which match the identity of `TopicIdPartition`. A topic rename therefore
//! does not move the bucket.

use {krabka_remote_storage::TopicIdPartition, krabka_verified::remote_metadata_partition};

/// Pick the metadata-topic partition for `tp`, given the
/// `partition_count` of `__remote_log_metadata`.
///
/// # Panics
///
/// Panics when `partition_count <= 0`.
#[must_use]
pub fn metadata_partition_for(tp: &TopicIdPartition, partition_count: i32) -> i32 {
    remote_metadata_partition(tp.topic_id.as_u128(), tp.partition, partition_count)
        .expect("partition_count must be positive")
}

/// Deduped, sorted set of `__remote_log_metadata` partitions that carry
/// metadata for the given user-topic-partitions, for the metadata topic's
/// `partition_count`. This is the set a broker must consume to serve remote
/// reads for the partitions it leads or follows.
///
/// # Panics
///
/// Panics when `partition_count <= 0`. [`metadata_partition_for`] raises the
/// panic.
#[must_use]
pub fn metadata_partitions_for<'a, I>(tps: I, partition_count: i32) -> Vec<i32>
where
    I: IntoIterator<Item = &'a TopicIdPartition>,
{
    assert2::assert!(partition_count > 0, "partition_count must be positive");
    let mut set: Vec<i32> = tps
        .into_iter()
        .map(|tp| metadata_partition_for(tp, partition_count))
        .collect();
    set.sort_unstable();
    set.dedup();
    set
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, uuid::Uuid};

    fn tp(name: &str, partition: i32) -> TopicIdPartition {
        TopicIdPartition::new(
            Uuid::from_u128(0x1234_5678_9ABC_DEF0_0011_2233_4455_6677),
            name,
            partition,
        )
    }

    #[test]
    fn is_in_range() {
        for p in 0..10 {
            let bucket = metadata_partition_for(&tp("orders", p), 50);
            assert!((0..50).contains(&bucket), "bucket {bucket} out of [0,50)");
        }
    }

    #[test]
    fn matches_jvm_remote_metadata_partitioner_goldens() {
        for (partition, expected) in [(0, 6), (1, 36), (3, 2), (7, 26)] {
            assert!(metadata_partition_for(&tp("orders", partition), 50) == expected);
        }
    }

    #[test]
    fn is_deterministic_across_calls() {
        let a = metadata_partition_for(&tp("orders", 7), 50);
        let b = metadata_partition_for(&tp("orders", 7), 50);
        assert!(a == b);
    }

    #[test]
    fn ignores_topic_name() {
        // Identity is (topic_id, partition); name is informational.
        let a = metadata_partition_for(&tp("orders", 3), 50);
        let b = metadata_partition_for(&tp("renamed", 3), 50);
        assert!(a == b, "renaming a topic must not re-bucket its metadata");
    }

    #[test]
    fn different_partitions_distribute() {
        let mut seen = std::collections::HashSet::new();
        for p in 0..200 {
            seen.insert(metadata_partition_for(&tp("orders", p), 50));
        }
        // Sanity: a hash that always returned 0 would land only one bucket.
        assert!(
            seen.len() > 5,
            "hash should spread across buckets, got {}",
            seen.len()
        );
    }

    #[test]
    fn single_partition_count_collapses_to_zero() {
        for p in 0..20 {
            assert!(metadata_partition_for(&tp("t", p), 1) == 0);
        }
    }

    #[test]
    #[should_panic(expected = "partition_count must be positive")]
    fn rejects_zero_partition_count() { let _ = metadata_partition_for(&tp("t", 0), 0); }

    #[test]
    fn metadata_partitions_for_dedupes_and_sorts() {
        // Two user-partitions that hash to the same metadata partition must
        // collapse to one entry; the result is sorted ascending.
        let a = tp("orders", 0);
        let b = tp("orders", 1);
        let pa = metadata_partition_for(&a, 50);
        let pb = metadata_partition_for(&b, 50);
        let got = metadata_partitions_for([a.clone(), b.clone(), a.clone()].iter(), 50);
        let mut expected: Vec<i32> = vec![pa, pb];
        expected.sort_unstable();
        expected.dedup();
        assert!(got == expected);
        assert!(got.windows(2).all(|w| w[0] < w[1]), "sorted, deduped");
    }

    #[test]
    fn metadata_partitions_for_empty_is_empty() {
        let none: [TopicIdPartition; 0] = [];
        assert!(metadata_partitions_for(none.iter(), 50).is_empty());
    }

    #[test]
    fn metadata_partitions_for_empty_rejects_nonpositive_count() {
        let none: [TopicIdPartition; 0] = [];
        for count in [0, -1] {
            let result = std::panic::catch_unwind(|| metadata_partitions_for(none.iter(), count));
            assert!(result.is_err(), "count {count} must panic before iteration");
        }
    }
}
