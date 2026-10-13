//! Batch-offset continuity, record placement, and rewrite admission for
//! offline restore verification.

use creusot_std::prelude::*;

/// Batch fate after folding the exact per-record selection results.
#[krabka_macros::model_type]
pub enum RestoreFilterDecision {
    Keep,
    Empty,
    Filter,
}

/// Which operator exclusion predicates matched one record.
///
/// The host evaluates each pattern against the record; the kernel owns only
/// how the matches combine.
#[krabka_macros::model_type]
pub struct RestoreExclusions {
    /// `--exclude-producer-id` names the batch's producer.
    pub producer: bool,
    /// An `--exclude-offset` range covers the record's absolute offset.
    pub offset: bool,
    /// Regex matches over the record's own bytes.
    pub content: RestoreContentExclusions,
}

/// Which content patterns matched one record's raw key and header bytes.
#[krabka_macros::model_type]
pub struct RestoreContentExclusions {
    /// An `--exclude-key` pattern matches the record key.
    pub key: bool,
    /// An `--exclude-header` pattern matches one record header.
    pub header: bool,
}

/// One remote segment's lifecycle state in the RLMM snapshot, or its absence.
#[krabka_macros::model_type]
pub enum RestoreSnapshotState {
    /// The snapshot does not list the segment.
    Missing,
    /// `COPY_SEGMENT_STARTED` or `COPY_SEGMENT_FINISHED`.
    Live,
    /// `DELETE_SEGMENT_STARTED`.
    DeleteStarted,
    /// `DELETE_SEGMENT_FINISHED`.
    DeleteFinished,
}

/// What restore does with one segment after comparing scan and snapshot.
#[krabka_macros::model_type]
pub enum RestoreReconcileDecision {
    /// The scanned bytes are restored.
    Keep,
    /// The segment is absent or being deleted; nothing is restored for it.
    Exclude,
    /// The archive scan and the snapshot contradict each other.
    Disagree,
}

/// Kafka's batch timestamp type: attributes bit 3
/// (`DefaultRecordBatch.TIMESTAMP_TYPE_MASK`).
#[krabka_macros::model_type]
pub enum RestoreTimestampType {
    CreateTime,
    LogAppendTime,
}

/// The batch-header fields that place a record in offset and time.
#[krabka_macros::model_type]
pub struct RestoreBatchFrame {
    pub base_offset: i64,
    pub last_offset_delta: i32,
    pub timestamp_type: RestoreTimestampType,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
}

/// One record's encoded offset and timestamp deltas.
#[krabka_macros::model_type]
pub struct RestoreRecordDeltas {
    pub offset_delta: i32,
    pub timestamp_delta: i64,
}

/// Producer identity and batch kind of one rewritten batch.
#[krabka_macros::model_type]
pub struct RestoreProducer {
    /// Attributes bit 5.
    pub control: bool,
    /// Attributes bit 4.
    pub transactional: bool,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// Offset layout of one rewritten batch.
#[krabka_macros::model_type]
pub struct RestoreLayout {
    pub base_offset: i64,
    pub last_offset_delta: i32,
    pub records_count: i32,
}

mod record_coordinates;
#[cfg(creusot)]
pub use record_coordinates::{kafka_record_timestamp, restore_excluded, restore_record_placeable};
pub use record_coordinates::{
    restore_archive_reconcile, restore_batch_filter_decision, restore_batch_past_offset_bound,
    restore_batch_step, restore_record_coordinates, restore_record_selected,
};

mod rewritten_record;
#[cfg(creusot)]
pub use rewritten_record::{
    legal_header, legal_producer, non_idempotent_producer, restore_rewrite_record_legal,
};
pub use rewritten_record::{restore_rewritten_batch_header, restore_rewritten_record};

#[cfg(test)]
mod tests;
