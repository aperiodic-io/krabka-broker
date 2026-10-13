//! KIP-534 log-compaction decision core.
//!
//! This core comes out of `krabka-log`, so that Creusot can verify it. The host
//! crate re-exports these functions, and the stateright model in
//! `krabka-log/src/compact_model.rs` drives these exact functions.

use creusot_std::prelude::*;

/// Per-record facts the retain decision needs.
#[krabka_macros::model_type]
pub struct RecordMeta {
    /// Whether the record has a key.
    pub has_key: bool,
    /// Whether the record has a non-null value.
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[krabka_macros::model_type]
pub struct BatchMeta {
    /// Whether the batch is a transactional control batch.
    pub is_control: bool,
    /// Producer id for transactional batches. It is negative for
    /// non-transactional batches.
    pub producer_id: i64,
    /// The batch's existing delete horizon, which is `base_timestamp` when bit
    /// 6 is set. It is `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Whether the cleaner read data of a control marker's own transaction in this
/// pass. This is Kafka's per-transaction rule (`CleanedTransactionMetadata`),
/// not a per-producer one.
#[krabka_macros::model_type]
pub enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// The pass read at least one batch of this marker's transaction, so the
    /// marker stays.
    DataSurvives,
    /// The pass read no batch of this marker's transaction, so the marker may
    /// age out through the delete horizon.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[krabka_macros::model_type]
pub enum RetainDecision {
    /// Keep the record as-is.
    Keep,
    /// Keep the record, but stamp its batch with this delete horizon:
    /// `base_timestamp = horizon`, with bit 6 set.
    SetHorizon(i64),
    /// Drop the record.
    Delete,
}

/// Whether a compaction batch-stream decode is complete, made progress, or
/// encountered corrupt input.
#[krabka_macros::model_type]
pub enum CompactionDecodeStep {
    /// The input is exhausted. This is the only successful completion state.
    Done,
    /// One batch decoded and consumed at least one byte.
    Continue,
    /// Nonempty input did not decode with strict progress.
    Corrupt,
}

mod retain_decision;
#[cfg(creusot)]
pub use retain_decision::compute_horizon_model;
pub use retain_decision::{compaction_decode_step, compute_horizon, retain_decision};

#[cfg(test)]
mod tests;
