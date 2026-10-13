//! Producer-snapshot selection, validation, replay, and deletion decisions.
//!
//! The rules are Kafka's, from three places:
//!
//! - `LogLoader.load` picks the log start the reload runs against, and first
//!   calls `ProducerStateManager.removeStraySnapshots` with the base offset
//!   of every local segment.
//! - `ProducerStateManager.truncateAndReload(logStartOffset, logEndOffset, _)`
//!   deletes every snapshot outside `(logStartOffset, logEndOffset]`, then
//!   `loadFromSnapshot` loads the newest snapshot that is left.
//! - `UnifiedLog.rebuildProducerState` replays each local segment from
//!   `max(segment.baseOffset, mapEndOffset, logStartOffset)`.

#[cfg(creusot)]
use crate::producer::ProducerDecision;
use creusot_std::prelude::*;

open_logic! {
/// A physically retained data row is valid and belongs to the requested producer.
pub(crate) fn retained_producer_row(end: Int, row: ProducerSnapshotEntryFacts, pid: i64) -> bool {
    pearlite! {
        snapshot_entry_valid_model(end, row) && row.last_offset@ >= 0
            && row.producer_id == pid
    }
}
}

open_logic! {
/// A retained snapshot row matches both endpoints of the requested sequence.
pub fn snapshot_sequence_matches(row: ProducerSnapshotEntryFacts, base: Int, delta: Int) -> bool {
    pearlite! {
        base == crate::producer::sequence_modulo_2_31(row.last_sequence@ - row.offset_delta@)
            && row.last_sequence@ == crate::producer::sequence_modulo_2_31(base + delta)
    }
}
}

open_logic! {
/// The original retained batch span and its exclusive acknowledgement frontier.
pub(crate) fn recovered_batch_coordinates(
    row: ProducerSnapshotEntryFacts,
    base: Int,
    frontier: Int,
    end: Int,
) -> bool {
    pearlite! { base == row.last_offset@ - row.offset_delta@
    && frontier == row.last_offset@ + 1
    && 0 <= base && base < frontier && frontier <= end }
}
}

open_logic! {
/// Classification after no retained batch matches the requested sequence.
pub(crate) fn nonduplicate_snapshot_decision(
    incoming_epoch: Int,
    epoch: Int,
    sequence: Int,
    last_sequence: Int,
) -> ProducerDecision {
    pearlite! { if incoming_epoch < epoch { ProducerDecision::Fenced }
    else if incoming_epoch > epoch {
        if sequence == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder }
    } else if sequence == crate::producer::sequence_modulo_2_31(last_sequence + 1) { ProducerDecision::Append }
    else { ProducerDecision::OutOfOrder } }
}
}

model_types! {
    @proof (derive(std::clone::Clone, Copy, DeepModel));
    /// The offsets one producer-state reload runs against.
    ///
    /// These are three `i64` values with three different meanings, so they
    /// travel as one struct and a transposed call site does not compile.
    pub struct ProducerReloadRange {
        /// The `logStartOffset` Kafka hands `truncateAndReload`, as
        /// [`producer_snapshot_reload_log_start`] picks it.
        pub log_start: i64,
        /// The first offset a local segment can serve: the greater of the
        /// oldest local segment's base and the log start.
        pub local_start: i64,
        /// The log end offset the state is rebuilt up to.
        pub log_end: i64,
    }
}

model_types! {
    @copy_only
    /// One decoded producer-state snapshot entry, field for field as Kafka's
    /// `ProducerStateManager` snapshot schema (version 1) lays it out, minus the
    /// timestamp, which no validity rule reads.
    ///
    /// Seven integers of four widths with seven meanings travel as one struct, so
    /// a transposed call site does not compile.
    pub struct ProducerSnapshotEntryFacts {
        /// `ProducerId`.
        pub producer_id: i64,
        /// `ProducerEpoch`.
        pub producer_epoch: i16,
        /// `LastSequence`, or `-1` for a producer whose only record is a marker.
        pub last_sequence: i32,
        /// `LastOffset`, or `-1` with the same meaning.
        pub last_offset: i64,
        /// `OffsetDelta`: the last batch's offset count less one.
        pub offset_delta: i32,
        /// `CoordinatorEpoch`, or `-1` before any marker.
        pub coordinator_epoch: i32,
        /// `CurrentTxnFirstOffset`, or `-1` with no open transaction.
        pub current_txn_first_offset: i64,
    }
}

mod snapshot_transaction_valid;
#[cfg(creusot)]
pub use snapshot_transaction_valid::{
    is_segment_base, kafka_reload_keeps, kafka_replay_start, kafka_stray_removed,
    snapshot_last_record_valid, snapshot_transaction_valid,
};
pub use snapshot_transaction_valid::{
    producer_snapshot_latest_index, producer_snapshot_reload_keeps,
    producer_snapshot_reload_log_start, producer_snapshot_stray,
};

mod replay_start;
#[cfg(creusot)]
pub use replay_start::snapshot_entry_valid_model;
pub use replay_start::{producer_snapshot_entry_valid, producer_snapshot_replay_start};

open_logic! {
/// Producer data rows preserve their strictly increasing physical completion offsets.
pub(crate) fn producer_offsets_ordered(rows: Seq<ProducerSnapshotEntryFacts>) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < j && j < rows.len() ==> rows[i].last_offset@ < rows[j].last_offset@ }
}
}

#[cfg(test)]
mod tests;
