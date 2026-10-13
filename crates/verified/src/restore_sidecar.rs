//! Restore-side validation of archived sparse indexes and state sidecars.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, invariant};

/// One decoded aborted-transaction index entry (`AbortedTxn`).
#[krabka_macros::model_type]
pub struct RestoreAbortedTxn {
    pub producer_id: i64,
    /// The transaction's first offset. It may precede the segment base when
    /// the transaction began before a segment roll.
    pub start_offset: i64,
    /// The abort marker's offset.
    pub last_offset: i64,
}

/// The inclusive offset extent of the segment that owns an index.
#[krabka_macros::model_type]
pub struct RestoreSegmentExtent {
    pub base_offset: i64,
    pub last_offset: i64,
}

mod restore_producer_ids_strict;
pub use restore_producer_ids_strict::{
    restore_index_frontier, restore_leader_epoch_entry_valid, restore_offset_index_entry_valid,
    restore_producer_ids_strict, restore_time_index_entry_valid, restore_txn_index_entry_valid,
};

#[cfg(test)]
mod tests;
