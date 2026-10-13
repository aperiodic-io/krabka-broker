//! Byte-exact codec for the `__barrier_state` topic records.
//!
//! The topic carries three record kinds, and the key names which one a record
//! is. A group record holds a barrier group's definition. An injection-start
//! record freezes the target set of one injection before the coordinator
//! appends any marker. A cut record holds the published offsets of one epoch.
//!
//! This module is a codec only. The barrier coordinator owns the runtime
//! wiring.
//!
//! The layout is deliberately plain, because `krabka-streams-java` and
//! `krabka-streams-go` decode cut records by hand. Every integer is
//! big-endian. A string is an `i16` byte length and then UTF-8 bytes, so an
//! encoder refuses one of more than 32767 bytes with an error instead of
//! writing a length it cannot hold. An `i32` count precedes every array. There
//! are no compact lengths and no tagged fields.
//!
//! Wire, in field order:
//!
//! ```text
//! key:
//!   version i16 = 0
//!   kind    i16                0 group, 1 injection start, 2 cut
//!   group   string
//!   epoch   i64                -1 for a group record
//!
//! group value:
//!   version       i16 = 0
//!   topics        i32 [ topic string ]
//!   interval_ms   i64          -1 turns off periodic injection
//!   retained_cuts i32
//!   last_epoch    i64
//!
//! injection-start value:
//!   version           i16 = 0
//!   coordinator_epoch i32
//!   triggered_at      i64
//!   targets           i32 [ topic string | partition_count i32 ]
//!
//! cut value:
//!   version      i16 = 0
//!   triggered_at i64
//!   completed_at i64
//!   status       i8            0 complete, 1 partial
//!   topics       i32 [ topic string | partitions i32 [ partition i32 | offset i64 ] ]
//!   missing      i32 [ topic string | partition i32 ]
//! ```
//!
//! A group record with a null value is a tombstone, and it deletes the group.
//!
//! One record kind per module, plus the reads all three share: `key` holds the
//! key that every record carries, `group`, `injection_start` and `cut` hold one
//! value kind each, and `primitives` holds the version check, the
//! trailing-byte check, and the counted array.

mod cut;
mod group;
mod injection_start;
mod key;
mod primitives;
#[cfg(test)]
mod test_support;

pub(crate) use self::{
    cut::{
        CutStatus, CutValue, MissingPartition, PartitionOffset, TopicOffsets, decode_cut,
        encode_cut,
    },
    group::{GroupValue, decode_group, encode_group},
    injection_start::{
        InjectionStartValue, TopicTarget, decode_injection_start, encode_injection_start,
    },
    key::{RecordKey, RecordKind, decode_key, encode_key},
};

/// The record version that every `__barrier_state` record carries.
///
/// Part of the 1.x on-disk contract. Version 0 is the only version so far,
/// and the decoder rejects any other value. A new version keeps a decoder for
/// every earlier 1.x version. See `docs/persisted_formats.md`.
pub(crate) const RECORD_VERSION: i16 = 0;

/// The epoch that a group record writes, because a group definition belongs to
/// no single epoch.
pub(crate) const NO_EPOCH: i64 = -1;

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn no_epoch_constant_is_negative_one() {
        assert!(NO_EPOCH == -1);
    }
}
