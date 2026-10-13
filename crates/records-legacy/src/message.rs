//! v0/v1 `Message` wire format.
//!
//! Each message sits inside a [`MessageSet`](crate::set) entry, after the
//! per-entry `(offset:i64, message_size:i32)` framing. The wire layout is:
//!
//! ```text
//! v0: crc:u32 | magic:i8=0 | attrs:i8 |                 | key | value
//! v1: crc:u32 | magic:i8=1 | attrs:i8 | timestamp:i64   | key | value
//! ```
//!
//! `key` and `value` are nullable bytes: an i32 length, where -1 means
//! null. The codec computes the CRC-32 (IEEE polynomial) over the bytes
//! from `magic` through the end of `value`. That is, over everything
//! inside the message except the CRC field itself.
//!
//! The magic byte and the message fields stay here. `attributes` holds the
//! bit layout of the `attributes` byte and its compression mapping, `encode`
//! sizes and writes a message, and `decode` parses one frame back.

use {crate::error::LegacyRecordsError, bytes::Bytes, krabka_compression::CompressionType};

mod attributes;
mod decode;
mod encode;

#[cfg(test)]
mod test_support;

pub use self::attributes::{attrs, attrs_with_compression, compression_from_attrs};

/// Magic byte, that is, the legacy message-format version: 0 or 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Magic {
    V0,
    V1,
}

impl Magic {
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::V0 => 0,
            Self::V1 => 1,
        }
    }

    /// # Errors
    /// Returns `LegacyRecordsError::UnsupportedMagic` if `b` is not 0 or 1.
    pub fn from_i8(b: i8) -> Result<Self, LegacyRecordsError> {
        match b {
            0 => Ok(Self::V0),
            1 => Ok(Self::V1),
            other => Err(LegacyRecordsError::UnsupportedMagic { found: other }),
        }
    }
}

/// Owned legacy message, decoded after the frame parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub magic: Magic,
    pub attributes: i8,
    /// `Some` when `magic` is `V1`. `None` when `magic` is `V0`.
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Message {
    #[must_use]
    pub fn compression(&self) -> CompressionType {
        compression_from_attrs(self.attributes).unwrap_or(CompressionType::None)
    }
}
