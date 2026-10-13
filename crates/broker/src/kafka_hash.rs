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
    use {super::*, assert2::assert};

    #[test]
    fn murmur2_matches_kafka_vectors_for_remainder_lengths() {
        struct Case {
            input: &'static [u8],
            expected: u32,
        }
        let cases: &[Case] = &[
            Case {
                input: b"",
                expected: 0x106e_08d9,
            },
            Case {
                input: b"a",
                expected: 0xa2d0_b27c,
            },
            Case {
                input: b"ab",
                expected: 0x12d8_262a,
            },
            Case {
                input: b"abc",
                expected: 0x1c94_221b,
            },
            Case {
                input: b"abcd",
                expected: 0xb11a_b5f4,
            },
            Case {
                input: b"abcde",
                expected: 0x1b89_7edd,
            },
        ];

        for Case { input, expected } in cases {
            assert!(murmur2(input) == *expected);
        }
    }

    #[test]
    fn murmur2_partition_matches_kafka_utils_abs_mod() {
        struct Case {
            input: &'static [u8],
            partitions: i32,
            expected: i32,
        }
        let cases: &[Case] = &[
            Case {
                input: b"my-tid",
                partitions: 50,
                expected: 43,
            },
            Case {
                input: b"producer-1",
                partitions: 50,
                expected: 45,
            },
            Case {
                input: b"tx-orders-prod",
                partitions: 50,
                expected: 26,
            },
            Case {
                input: b"abcde",
                partitions: 7,
                expected: 4,
            },
        ];

        for Case {
            input,
            partitions,
            expected,
        } in cases
        {
            assert!(murmur2_partition(input, *partitions) == *expected);
        }
    }
}
