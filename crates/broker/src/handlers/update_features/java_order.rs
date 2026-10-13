//! The iteration order of a `java.util.HashMap<String, _>`.
//!
//! Kafka's `QuorumController.updateFeatures` puts the request rows into a
//! `HashMap` and `FeatureControlManager.updateFeatures` stops at the first
//! row that fails in that map's iteration order. Which row's error the
//! response names therefore depends on Java's bucket layout, and this module
//! reproduces it.

/// `HashMap`'s initial table size.
const INITIAL_CAPACITY: usize = 16;

/// Java's `String.hashCode` over UTF-16 code units.
fn java_string_hash(key: &str) -> i32 {
    key.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// `HashMap.hash`: the hash code with its high half folded into the low half.
fn spread(key: &str) -> u32 {
    let hash = u32::from_ne_bytes(java_string_hash(key).to_ne_bytes());
    hash ^ (hash >> 16)
}

/// The distinct `keys`, in the order a `new HashMap<>()` filled by `put` in
/// `keys` order iterates them.
///
/// The table starts at 16 buckets and doubles whenever the size passes three
/// quarters of it. Iteration walks the buckets in index order, and a bucket
/// keeps its keys in first-insertion order: a resize splits each bucket
/// without reordering it, and putting an existing key again leaves it where
/// it is. A bucket only turns into a tree at 8 keys in a table of 64 or more,
/// far past any `UpdateFeatures` request, so that case is not modelled.
pub(super) fn hash_map_order<'a>(keys: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut distinct: Vec<&str> = Vec::new();
    for key in keys {
        if !distinct.contains(&key) {
            distinct.push(key);
        }
    }
    let mut capacity = INITIAL_CAPACITY;
    while distinct.len() > capacity / 4 * 3 {
        capacity *= 2;
    }
    let mask = u32::try_from(capacity - 1).unwrap_or(u32::MAX);
    // A stable sort keeps first-insertion order inside each bucket.
    distinct.sort_by_key(|key| spread(key) & mask);
    distinct
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn java_string_hash_matches_the_jvm() {
        // "hello".hashCode() and "polygenelubricants".hashCode() on a JVM.
        for (key, want) in [
            ("", 0),
            ("hello", 99_162_322),
            ("polygenelubricants", i32::MIN),
        ] {
            assert!(java_string_hash(key) == want, "{key}");
        }
    }

    /// The order observed on a Kafka 4.3.1 broker: an eight-feature
    /// `kafka-features upgrade` in which every row fails names the error of
    /// `group.version`, and a two-row request that lists `share.version` first
    /// still names `group.version`.
    #[test]
    fn feature_names_iterate_in_kafka_order() {
        let cases: [(&[&str], &[&str]); 3] = [
            (
                &[
                    "group.version",
                    "share.version",
                    "transaction.version",
                    "streams.version",
                    "eligible.leader.replicas.version",
                    "kraft.version",
                    "metadata.version",
                    "no.such.feature",
                ],
                &[
                    "group.version",
                    "no.such.feature",
                    "kraft.version",
                    "share.version",
                    "metadata.version",
                    "streams.version",
                    "transaction.version",
                    "eligible.leader.replicas.version",
                ],
            ),
            (&["share.version", "group.version"], &[
                "group.version",
                "share.version",
            ]),
            (
                // Both land in bucket 7, which keeps first-insertion order.
                &["metadata.version", "share.version", "metadata.version"],
                &["metadata.version", "share.version"],
            ),
        ];
        for (keys, want) in cases {
            assert!(hash_map_order(keys.iter().copied()) == want, "{keys:?}");
        }
    }

    #[test]
    fn the_table_grows_past_three_quarters_full() {
        // Thirteen keys resize the table to 32 buckets, which moves "k12"
        // (bucket 17 of 32, bucket 1 of 16) behind every key in buckets 2-16.
        let keys: Vec<String> = (0..13).map(|index| format!("k{index}")).collect();
        let order = hash_map_order(keys.iter().map(String::as_str));
        let buckets: Vec<u32> = order.iter().map(|key| spread(key) & 31).collect();
        let mut sorted = buckets.clone();
        sorted.sort_unstable();
        assert!(order.len() == 13);
        assert!(buckets == sorted);
    }
}
