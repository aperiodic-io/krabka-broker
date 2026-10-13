//! KIP-405 remote-log metadata topic partitioning.

use creusot_std::prelude::*;

const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;

// Java `int` arithmetic is arithmetic modulo 2^32, so every Java `int` below
// is carried as its two's-complement bit image in a `u32`, with wrapping
// `u32` operations standing in for Java's `int` ones.

/// Java `Long.hashCode`, `(int) (value ^ (value >>> 32))`, as its `u32` image.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn java_long_hash_model(bits: u64) -> u32 {
    pearlite! { ((bits ^ (bits >> 32u32)) & 0xffff_ffffu64) as u32 }
}

#[cfg_attr(creusot, ensures(result == java_long_hash_model(bits)))]
fn java_long_hash(bits: u64) -> u32 { ((bits ^ (bits >> 32)) & 0xffff_ffff) as u32 }

/// The `u32` image of a Java `int`: the value itself, or the value plus 2^32
/// (`u32::MAX - (-1 - value)`) when it is negative. Both casts stay in range.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn java_int_image_model(value: i32) -> u32 {
    pearlite! {
        if value@ >= 0 { value as u32 } else { u32::MAX - (-1i32 - value) as u32 }
    }
}

#[cfg_attr(creusot, ensures(result == java_int_image_model(value)))]
fn java_int_image(value: i32) -> u32 {
    if value < 0 {
        // `2^32 + value`, as `u32::MAX - (-1 - value)`.
        u32::MAX - value.abs_diff(-1)
    } else {
        value.abs_diff(0)
    }
}

/// Java `Objects.hash(uuid.getLeastSignificantBits(),
/// uuid.getMostSignificantBits(), partition)`, as its `u32` image.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn java_objects_hash_model(topic_id: u128, user_partition: i32) -> u32 {
    pearlite! {
        let most_significant_bits = (topic_id >> 64u32) as u64;
        let least_significant_bits = (topic_id & 0xffff_ffff_ffff_ffffu128) as u64;
        let first = 1u32 * 31u32 + java_long_hash_model(least_significant_bits);
        let second = first * 31u32 + java_long_hash_model(most_significant_bits);
        second * 31u32 + java_int_image_model(user_partition)
    }
}

#[cfg_attr(
    creusot,
    ensures(result == java_objects_hash_model(topic_id, user_partition))
)]
fn java_objects_hash(topic_id: u128, user_partition: i32) -> u32 {
    let most_significant_bits = (topic_id >> 64) as u64;
    let least_significant_bits = (topic_id & 0xffff_ffff_ffff_ffff) as u64;

    let mut hash = 1_u32;
    hash = hash
        .wrapping_mul(31)
        .wrapping_add(java_long_hash(least_significant_bits));
    hash = hash
        .wrapping_mul(31)
        .wrapping_add(java_long_hash(most_significant_bits));
    hash.wrapping_mul(31)
        .wrapping_add(java_int_image(user_partition))
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn reverse_bytes_model(value: u32) -> u32 {
    pearlite! {
        (value >> 24u32)
            | ((value >> 8u32) & 0x0000_ff00u32)
            | ((value << 8u32) & 0x00ff_0000u32)
            | (value << 24u32)
    }
}

#[cfg_attr(creusot, ensures(result == reverse_bytes_model(value)))]
fn reverse_bytes(value: u32) -> u32 {
    (value >> 24) | ((value >> 8) & 0x0000_ff00) | ((value << 8) & 0x00ff_0000) | (value << 24)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_murmur2_int_model(value: u32) -> u32 {
    pearlite! {
        let chunk0 = reverse_bytes_model(value);
        let chunk1 = chunk0 * 0x5bd1_e995u32;
        let chunk2 = (chunk1 ^ (chunk1 >> 24u32)) * 0x5bd1_e995u32;
        let hash0 = (0x9747_b28cu32 ^ 4u32) * 0x5bd1_e995u32;
        let hash1 = hash0 ^ chunk2;
        let hash2 = (hash1 ^ (hash1 >> 13u32)) * 0x5bd1_e995u32;
        hash2 ^ (hash2 >> 15u32)
    }
}

/// Kafka Murmur2 over the four big-endian bytes of a Java `int`, given as its
/// `u32` image.
#[cfg_attr(creusot, ensures(result == kafka_murmur2_int_model(value)))]
fn kafka_murmur2_int(value: u32) -> u32 {
    let mut hash = MURMUR2_SEED ^ 4;
    let mut chunk = reverse_bytes(value);
    chunk = chunk.wrapping_mul(MURMUR2_M);
    chunk ^= chunk >> 24;
    chunk = chunk.wrapping_mul(MURMUR2_M);
    hash = hash.wrapping_mul(MURMUR2_M);
    hash ^= chunk;
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(MURMUR2_M);
    hash ^ (hash >> 15)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn kafka_to_positive_model(hash: u32) -> Int {
    pearlite! { hash@ % 2_147_483_648 }
}

/// Kafka `Utils.toPositive`, `number & 0x7fffffff`, which on the `u32` image
/// is the remainder modulo 2^31.
#[cfg_attr(creusot, ensures(result@ == kafka_to_positive_model(hash)))]
#[ensures(0 <= result@)]
#[ensures(result@ <= i32::MAX@)]
fn kafka_to_positive(hash: u32) -> i32 { (u64::from(hash) % 0x8000_0000) as i32 }

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn remote_metadata_partition_model(
    topic_id: u128,
    user_partition: i32,
    metadata_partition_count: i32,
) -> Int {
    pearlite! {
        if metadata_partition_count <= 0i32 {
            0
        } else {
            kafka_to_positive_model(kafka_murmur2_int_model(
                java_objects_hash_model(topic_id, user_partition)
            )) % metadata_partition_count@
        }
    }
}

/// Map a user topic-id partition to its KIP-405 metadata topic partition.
///
/// This is byte-for-byte compatible with Kafka's
/// `RemoteLogMetadataTopicPartitioner`: Java `Objects.hash` receives the
/// UUID's least-significant bits, most-significant bits, and user partition;
/// its big-endian `int` bytes are passed through Kafka Murmur2 and
/// `Utils.toPositive` before the metadata partition modulo.
#[cfg_attr(
    creusot,
    ensures(metadata_partition_count@ > 0 ==>
        exists<partition: i32> result == Some(partition)
            && partition@ == remote_metadata_partition_model(
                topic_id,
                user_partition,
                metadata_partition_count
            ))
)]
#[ensures((result == None) == (metadata_partition_count@ <= 0))]
#[ensures(forall<partition: i32> result == Some(partition) ==>
    0 <= partition@ && partition@ < metadata_partition_count@)]
#[must_use]
pub fn remote_metadata_partition(
    topic_id: u128,
    user_partition: i32,
    metadata_partition_count: i32,
) -> Option<i32> {
    if metadata_partition_count <= 0 {
        return None;
    }

    let objects_hash = java_objects_hash(topic_id, user_partition);
    let murmur2 = kafka_murmur2_int(objects_hash);
    Some(kafka_to_positive(murmur2) % metadata_partition_count)
}

/// Select the inclusive metadata-log cursor immediately after a snapshot's
/// committed offset.
///
/// `-1` is the only no-event sentinel and maps to a full replay from zero.
/// Every real committed offset advances by exactly one. Values below the
/// sentinel and a committed `i64::MAX` fail closed instead of producing a
/// negative or wrapped cursor.
#[cfg_attr(
    creusot,
    ensures(match result {
        Some(cursor) => -1 <= committed@ && committed@ < i64::MAX@ && cursor@ == committed@ + 1,
        None => committed@ < -1 || committed@ == i64::MAX@,
    })
)]
#[ensures(forall<cursor: i64> result == Some(cursor) ==> 0 <= cursor@)]
#[must_use]
pub const fn remote_metadata_resume_cursor(committed: i64) -> Option<i64> {
    if committed < -1 {
        return None;
    }
    committed.checked_add(1)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn matches_jvm_remote_metadata_partitioner_goldens() {
        let topic_id = 0x1234_5678_9abc_def0_0011_2233_4455_6677;
        for (partition, expected) in [(0, 6), (1, 36), (3, 2), (7, 26)] {
            check!(remote_metadata_partition(topic_id, partition, 50) == Some(expected));
        }
    }

    #[test]
    fn matches_java_signed_uuid_and_partition_extremes() {
        for (topic_id, partition, expected) in [
            (0_u128, i32::MIN, 96),
            (u128::MAX, i32::MAX, 5),
            (0xffff_ffff_ffff_ffff_8000_0000_0000_0000, i32::MIN, 45),
        ] {
            check!(remote_metadata_partition(topic_id, partition, 97) == Some(expected));
        }
    }

    #[test]
    fn rejects_nonpositive_metadata_partition_counts() {
        check!(remote_metadata_partition(0, 0, 0) == None);
        check!(remote_metadata_partition(0, 0, -1) == None);
    }

    #[test]
    fn resume_cursor_maps_none_to_zero_and_advances_exactly() {
        check!(remote_metadata_resume_cursor(-1) == Some(0));
        check!(remote_metadata_resume_cursor(0) == Some(1));
        check!(remote_metadata_resume_cursor(41) == Some(42));
        check!(remote_metadata_resume_cursor(i64::MAX - 1) == Some(i64::MAX));
    }

    #[test]
    fn resume_cursor_rejects_malformed_and_overflowing_offsets() {
        check!(remote_metadata_resume_cursor(-2) == None);
        check!(remote_metadata_resume_cursor(i64::MIN) == None);
        check!(remote_metadata_resume_cursor(i64::MAX) == None);
    }
}
