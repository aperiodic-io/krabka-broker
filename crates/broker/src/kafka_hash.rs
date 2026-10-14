//! Kafka-compatible hashing helpers.

const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_M: u32 = 0x5bd1_e995;
const MURMUR2_R: u32 = 24;

#[must_use]
pub(crate) fn murmur2(data: &[u8]) -> u32 {
    let length = data.len();
    let length_bytes = length.to_le_bytes();
    let length32 = u32::from_le_bytes(length_bytes[..4].try_into().expect("usize is at least u32"));
    let mut h: u32 = MURMUR2_SEED ^ length32;
    let (chunks, rem) = data.as_chunks::<4>();

    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(MURMUR2_M);
        k ^= k >> MURMUR2_R;
        k = k.wrapping_mul(MURMUR2_M);
        h = h.wrapping_mul(MURMUR2_M);
        h ^= k;
    }

    match rem {
        [a] => {
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b] => {
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [a, b, c] => {
            h ^= u32::from(*c) << 16;
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(MURMUR2_M);
        }
        [] => {}
        _ => unreachable!("chunks_exact(4) leaves at most three remainder bytes"),
    }

    h ^= h >> 13;
    h = h.wrapping_mul(MURMUR2_M);
    h ^= h >> 15;
    h
}

/// Apply Kafka's `Utils.abs(murmur2(data)) % numPartitions` convention.
#[must_use]
pub(crate) fn murmur2_partition(data: &[u8], num_partitions: i32) -> i32 {
    let hash = murmur2(data).cast_signed();
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive % num_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        empty = { b"", 0x106e_08d9 },
        one_byte = { b"a", 0xa2d0_b27c },
        two_bytes = { b"ab", 0x12d8_262a },
        three_bytes = { b"abc", 0x1c94_221b },
        four_bytes = { b"abcd", 0xb11a_b5f4 },
        five_bytes = { b"abcde", 0x1b89_7edd },
    )]
    fn murmur2_matches_kafka_vectors_for_remainder_lengths(input: &[u8], expected: u32) {
        assert2::assert!(murmur2(input) == expected);
    }

    #[yare::parameterized(
        transaction_id = { b"my-tid", 50, 43 },
        producer_id = { b"producer-1", 50, 45 },
        orders_transaction = { b"tx-orders-prod", 50, 26 },
        five_byte_key = { b"abcde", 7, 4 },
    )]
    fn murmur2_partition_matches_kafka_utils_abs_mod(input: &[u8], partitions: i32, expected: i32) {
        assert2::assert!(murmur2_partition(input, partitions) == expected);
    }
}
