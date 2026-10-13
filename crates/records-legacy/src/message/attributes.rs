//! The legacy `attributes` byte: its bit layout, and the mapping between its
//! low three bits and a [`CompressionType`].
//!
//! This is the one part of the v0/v1 header that is not a plain fixed-width
//! field, so its two conversions live together here: the codes stop at 3, and
//! a codec that v0/v1 cannot name has to fail rather than truncate.

use {crate::error::LegacyRecordsError, krabka_compression::CompressionType};

/// Bit layout of the legacy `attributes` byte.
pub mod attrs {
    pub const COMPRESSION_MASK: i8 = 0x07;
    /// v1-only: 0 = `CreateTime`, 1 = `LogAppendTime`.
    pub const TIMESTAMP_TYPE_BIT: i8 = 1 << 3;
}

/// Map a v0/v1 compression code to a [`CompressionType`].
///
/// The compression code is the low 3 bits of `attributes`.
/// `0` => `None`, `1` => `Gzip`, `2` => `Snappy`, `3` => `Lz4`. v0/v1
/// never carried Zstd on the wire. KIP-110 was v2-only.
/// # Errors
/// Returns `LegacyRecordsError::Malformed` if the compression code is not 0, 1, 2, or 3.
pub fn compression_from_attrs(byte: i8) -> Result<CompressionType, LegacyRecordsError> {
    match byte & attrs::COMPRESSION_MASK {
        0 => Ok(CompressionType::None),
        1 => Ok(CompressionType::Gzip),
        2 => Ok(CompressionType::Snappy),
        3 => Ok(CompressionType::Lz4),
        other => Err(LegacyRecordsError::Malformed(format!(
            "legacy compression code {other} not supported (v0/v1 carries 0..=3)"
        ))),
    }
}

#[must_use]
/// # Panics
/// Panics if `codec` is `CompressionType::Zstd`, or any other codec that v0/v1 cannot carry.
pub fn attrs_with_compression(byte: i8, codec: CompressionType) -> i8 {
    let code: i8 = match codec {
        CompressionType::None => 0,
        CompressionType::Gzip => 1,
        CompressionType::Snappy => 2,
        CompressionType::Lz4 => 3,
        CompressionType::Zstd => panic!("legacy v0/v1 cannot carry zstd"),
        _ => panic!("unrecognised compression codec {codec:?} for v0/v1"),
    };
    (byte & !attrs::COMPRESSION_MASK) | code
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attrs_codec_roundtrip() {
        for (_name, compression) in [
            ("none", CompressionType::None),
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
        ] {
            let bits = attrs_with_compression(0, compression);
            assert2::assert!(compression_from_attrs(bits).unwrap() == compression);
        }
    }

    // --- mutation-coverage tests --------------------------------------------
    //
    // The round-trip tests above pass through any arithmetic flip that cancels
    // between encode and decode, and never exercise the malformed/boundary
    // paths. The tests below pin exact bit layouts, the timestamp sentinel, and
    // the precise `needed` byte counts / error variants on truncated input.
    //
    // A few mutants here are genuinely equivalent and intentionally left alone:
    //   - `attrs_with_compression` line `(byte & !MASK) | code`: `| -> ^` is a
    //     no-op because the two operands are bit-disjoint.
    //   - `encoded_len() - 4` in `encode_into` only sizes a `Vec::with_capacity`
    //     hint; the output bytes are identical regardless.

    #[test]
    fn attribute_bit_constants() {
        // `1 << 3`; a `>>` flip would zero the timestamp-type bit.
        assert2::assert!(attrs::TIMESTAMP_TYPE_BIT == 0b0000_1000);
        assert2::assert!(attrs::COMPRESSION_MASK == 0b0000_0111);
    }

    #[test]
    fn attrs_with_compression_exact_codes() {
        for (_name, compression, want) in [
            ("none", CompressionType::None, 0),
            ("gzip", CompressionType::Gzip, 1),
            ("snappy", CompressionType::Snappy, 2),
            ("lz4", CompressionType::Lz4, 3),
        ] {
            assert2::assert!(attrs_with_compression(0, compression) == want);
        }
    }

    #[test]
    fn attrs_with_compression_replaces_low_bits_keeps_high() {
        // Overwriting an existing codec replaces (not ORs) the low 3 bits.
        let ts = attrs::TIMESTAMP_TYPE_BIT;
        for (_name, initial, compression, expected) in [
            ("replace lz4 with gzip", 3, CompressionType::Gzip, 1),
            (
                "preserve timestamp with gzip",
                ts,
                CompressionType::Gzip,
                ts | 1,
            ),
            (
                "preserve timestamp with none",
                ts,
                CompressionType::None,
                ts,
            ),
        ] {
            assert2::assert!(attrs_with_compression(initial, compression) == expected);
        }
    }

    #[test]
    #[should_panic(expected = "cannot carry zstd")]
    fn attrs_with_compression_panics_on_zstd() {
        let _ = attrs_with_compression(0, CompressionType::Zstd);
    }
}
