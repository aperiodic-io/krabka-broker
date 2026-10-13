//! `murmur2(group) % num_partitions`.
//!
//! Every record of one barrier group lands on one `__barrier_state`
//! partition, so the group's epochs hold a total order. The hash follows
//! Apache Kafka's `Utils.abs(murmur2(...)) % numPartitions` convention.

use crate::kafka_hash::murmur2_partition;

/// Map a barrier group name to a partition index in `__barrier_state`.
///
/// The function applies the JVM `Utils.abs(int)` semantics, which return 0 for
/// `Integer.MIN_VALUE` to avoid arithmetic overflow.
#[must_use]
pub(crate) fn partition_for_group(group: &str, num_partitions: i32) -> i32 {
    murmur2_partition(group.as_bytes(), num_partitions)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    // The vectors come from the canonical JVM
    // `Utils.abs(Utils.murmur2(s.getBytes(UTF_8))) % 50` implementation.
    #[test]
    fn matches_the_jvm_murmur2_vectors() {
        struct Case {
            group: &'static str,
            expected: i32,
        }
        let cases: &[Case] = &[
            Case {
                group: "my-tid",
                expected: 43,
            },
            Case {
                group: "producer-1",
                expected: 45,
            },
            Case {
                group: "tx-orders-prod",
                expected: 26,
            },
        ];
        for Case { group, expected } in cases {
            assert!(
                partition_for_group(group, 50) == *expected,
                "group `{group}` should hash to partition {expected}"
            );
        }
    }

    #[test]
    fn the_same_group_always_lands_on_the_same_partition() {
        for group in ["", "orders-cut", "a-group-with-symbols-!@#$%"] {
            let first = partition_for_group(group, 50);
            assert!(partition_for_group(group, 50) == first);
        }
    }

    #[test]
    fn every_group_lands_inside_the_partition_range() {
        let long = "x".repeat(200);
        let groups = ["", "a", "orders-cut", "payments-cut", &long];
        for group in groups {
            for num in [1, 3, 50, 256] {
                let index = partition_for_group(group, num);
                assert!(
                    (0..num).contains(&index),
                    "group={group:?} num={num} produced {index}"
                );
            }
        }
    }
}
