//! Domain newtypes for the audit crate.
//!
//! These types wrap the same-typed primitives that recur across the hash-chain,
//! the spool, and the verifier. A transposed call site, for example
//! `set_depth(bytes, count)`, is then a compile error and not a silent
//! corruption. See the [newtype guidance] in the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

use {
    derive_more::{Add, AddAssign, Display, From, Into},
    krabka_macros::PrimitiveCmp,
};

/// Per-broker hash-chain sequence number in each record's `seq` header.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, PrimitiveCmp,
)]
pub struct Seq(pub u64);

/// Epoch-millisecond timestamp for the checkpoint `time` and the OCSF `time`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, PrimitiveCmp,
)]
pub struct EpochMs(pub i64);

/// Count of chained data records.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, PrimitiveCmp,
)]
pub struct RecordCount(pub u64);

/// Count of signed checkpoints.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, PrimitiveCmp,
)]
pub struct CheckpointCount(pub u64);

/// Number of bytes currently held in the spool.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    From,
    Into,
    Add,
    AddAssign,
    PrimitiveCmp,
)]
pub struct SpoolBytes(pub u64);

/// Configured upper bound on spool size in bytes.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, PrimitiveCmp,
)]
pub struct MaxSpoolBytes(pub u64);
