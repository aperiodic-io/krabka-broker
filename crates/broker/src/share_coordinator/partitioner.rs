//! `String.hashCode("{group}:{topicId}:{partition}") % num_partitions`.
//!
//! This is Apache Kafka's share-coordinator key form. The module hashes it
//! with Java's UTF-16 `String.hashCode` and Kafka's `Utils.abs` convention. A
//! share key therefore resolves to the same `__share_group_state` partition on
//! Krabka as on Apache Kafka.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

/// Map a share-coordinator key `(group_id, topic_id, partition)` to a
/// partition index in `__share_group_state`.
///
/// The function builds Kafka's key string with the JVM's URL-safe, unpadded
/// topic-id encoding and delegates Java UTF-16 hashing, `Utils.abs`, and the
/// partition modulo to the verified kernel.
///
/// # Panics
///
/// Panics when `num` is not positive; broker configuration enforces that invariant.
#[must_use]
pub fn partition_for_share_key(
    group_id: &str,
    topic_id: &uuid::Uuid,
    partition: i32,
    num: i32,
) -> i32 {
    let topic_id = URL_SAFE_NO_PAD.encode(topic_id.as_bytes());
    let key = format!("{group_id}:{topic_id}:{partition}");
    let utf16: Vec<u16> = key.encode_utf16().collect();
    krabka_verified::broker::java_string_hash_partition(&utf16, num)
        .expect("share coordinator partition count must be positive")
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, uuid::Uuid};

    #[test]
    fn deterministic_for_same_key() {
        let id = Uuid::from_bytes([5; 16]);
        let a = partition_for_share_key("g", &id, 0, 50);
        let b = partition_for_share_key("g", &id, 0, 50);
        assert!(a == b);
    }

    #[test]
    fn matches_jvm_base64_key_and_java_hash_golden() {
        let id = Uuid::from_bytes([5; 16]);
        assert!(partition_for_share_key("g", &id, 0, 50) == 2);
        assert!(partition_for_share_key("🦀", &id, 7, 17) == 8);
    }

    #[test]
    fn distinct_keys_differ_somewhere() {
        let id = Uuid::from_bytes([5; 16]);
        // Not all distinct keys must differ, but the partition must depend on
        // every component for at least some inputs.
        let p0 = partition_for_share_key("g", &id, 0, 50);
        let p1 = partition_for_share_key("g", &id, 1, 50);
        let pg = partition_for_share_key("h", &id, 0, 50);
        assert!(p0 != p1 || p0 != pg);
    }

    #[test]
    fn always_in_bounds() {
        let ids = [
            Uuid::nil(),
            Uuid::from_bytes([255; 16]),
            Uuid::from_bytes([1; 16]),
        ];
        for id in ids {
            for g in ["", "group", "a-very-long-share-group-id-with-symbols-!@#"] {
                for p in [0, 7, 49, i32::MAX] {
                    for num in [1, 3, 50, 256] {
                        let idx = partition_for_share_key(g, &id, p, num);
                        assert!(
                            (0..num).contains(&idx),
                            "g={g:?} id={id} p={p} num={num} produced {idx}"
                        );
                    }
                }
            }
        }
    }
}
