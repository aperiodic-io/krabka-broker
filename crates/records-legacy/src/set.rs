//! v0/v1 `MessageSet`: a sequence of `(offset, size, message)` frames.
//!
//! The frames lie back-to-back. The set has no overall length prefix.
//!
//! ```text
//! [ offset:i64 | size:i32 | <Message bytes for `size` bytes> ]*
//! ```
//!
//! v0/v1 encodes compression as a *wrapper* message whose value is itself
//! a compressed inner `MessageSet`. This module handles that layout.
//! `encode_compressed_message_set` optionally wraps a flat `MessageSet` in
//! a single compressed outer message. `decode_message_set` unwraps a
//! single layer for the caller.

use {bytes::Bytes, krabka_ids::Offset};

mod decode;
mod encode;

#[cfg(test)]
pub(crate) mod test_support;

pub use self::{
    decode::{decode_message_set, decode_message_set_with_policy},
    encode::{encode_compressed_message_set, encode_flat_message_set},
};

/// A single decoded `MessageSet` entry.
///
/// The entry holds the offset-tagged payload of one logical record, after
/// the codec unwraps compression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRecord {
    pub offset: Offset,
    /// Always `Some` when the source magic is v1. `None` when it is v0.
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}
