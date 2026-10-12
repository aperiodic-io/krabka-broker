//! Log compaction primitives. These are almost-pure helpers that work on
//! [`Segment`] handles and the on-disk file layout. [`crate::Log::compact`]
//! uses them.
//!
//! The algorithm makes a single pass over the **sealed** segment list, from
//! oldest to newest. It builds a key-to-latest-offset map, then rewrites the
//! surviving records into a single new segment at the lowest input base
//! offset. It never touches the active segment. Both walks track the
//! transactions they meet with a [`CleanedTransactionMetadata`], so a batch of
//! an aborted transaction never wins the map and never survives the rewrite.
//!
//! The algorithm drops records with `key.is_none()`, as Kafka's `LogCleaner`
//! does. A tombstone is a record with `key.is_some()` and `value.is_none()`.
//! The algorithm treats a tombstone like any other value and keeps it as the
//! most-recent entry for its key. `delete.retention.ms` ages tombstones out.

mod batch_reader;
mod decision;
mod offset_map;
mod rewrite;
mod swap;
mod txn_metadata;

#[cfg(test)]
mod test_support;

pub(crate) use self::decision::{
    BatchMeta, RecordMeta, RetainDecision, TxnDataState, retain_decision, should_index_key,
};
#[cfg(test)]
pub(crate) use self::decision::{data_retention_metadata, marker_retention_metadata};
pub use self::{
    offset_map::build_offset_map,
    rewrite::{
        CleaningRound, ProducerLastRecord, RewriteOutput, RewriteRetention, rewrite_segments,
    },
    swap::atomic_swap,
    txn_metadata::CleanedTransactionMetadata,
};

// Exhaustive stateright enumeration of the KIP-534 retention contract over the
// pure decision cores above (reachable via `super::`).
#[cfg(test)]
#[path = "compact_model.rs"]
mod compact_model;

#[cfg(test)]
mod retention_fuzz;

pub(crate) use swap::atomic_swap_retiring;
