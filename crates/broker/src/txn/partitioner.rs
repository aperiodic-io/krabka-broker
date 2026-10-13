//! `String.hashCode(transactional_id) % num_partitions`.
//!
//! Apache Kafka selects a transaction coordinator with Java's UTF-16
//! `String.hashCode` and `Utils.abs`. This adapter delegates those operations
//! and the partition modulo to the verified kernel.

/// Checked form of [`partition_for_tid`].
///
/// Returns `None` when `num_partitions` is not positive.
#[must_use]
pub fn try_partition_for_tid(transactional_id: &str, num_partitions: i32) -> Option<i32> {
    let utf16: Vec<u16> = transactional_id.encode_utf16().collect();
    krabka_verified::broker::java_string_hash_partition(&utf16, num_partitions)
}

/// Map a `transactional_id` to a partition index in `__transaction_state`.
///
/// # Panics
///
/// Panics when `num_partitions` is not positive; broker configuration enforces
/// that invariant.
#[must_use]
pub fn partition_for_tid(transactional_id: &str, num_partitions: i32) -> i32 {
    try_partition_for_tid(transactional_id, num_partitions)
        .expect("transaction state partition count must be positive")
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    // Reference vectors generated from the JVM implementation:
    //   Utils.abs(tid.hashCode()) % 50
    #[test]
    fn matches_jvm_for_canonical_tids() {
        let cases: &[(&str, i32)] = &[("my-tid", 20), ("producer-1", 30), ("tx-orders-prod", 16)];
        for (tid, expected) in cases {
            assert!(
                partition_for_tid(tid, 50) == *expected,
                "tid `{tid}` should hash to partition {expected}"
            );
        }
    }

    #[test]
    fn always_in_bounds() {
        for s in [
            "",
            "a",
            "really-long-transactional-id-with-many-bytes-and-symbols-!@#$%",
        ] {
            for n in [1, 50, 256] {
                let p = partition_for_tid(s, n);
                assert!((0..n).contains(&p));
            }
        }
    }

    #[test]
    fn hashes_non_bmp_text_as_utf16() {
        assert!(partition_for_tid("😀", 50) == 49);
        assert!(partition_for_tid("a😀b", 50) == 44);
    }

    #[test]
    fn signed_minimum_hash_maps_to_zero() {
        // This Java String has hashCode() == Integer.MIN_VALUE.
        assert!(partition_for_tid("polygenelubricants", 50) == 0);
    }

    #[test]
    fn rejects_invalid_partition_counts_without_panicking() {
        assert!(try_partition_for_tid("tid", 0) == None);
        assert!(try_partition_for_tid("tid", -1) == None);
    }

    #[test]
    fn exact_retry_is_deterministic() {
        let first = partition_for_tid("retry-tid", 50);
        assert!(partition_for_tid("retry-tid", 50) == first);
    }
}
