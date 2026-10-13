//! Local-log segment-chain and torn-tail recovery decisions.

#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, Int, invariant, logic};

#[krabka_macros::model_type]
pub struct LocalRecoveryStep {
    pub valid_end: u64,
    pub last_offset: i64,
    pub next_offset: i64,
}

/// What `Log::open` observes at one base offset that carries a compaction
/// `.swap` file, from a single directory listing.
///
/// Compaction writes its survivor segment under `.cleaned` names, fsyncs it,
/// renames the sidecars and then the log from `.cleaned` to `.swap`, and only
/// after that rename is durable deletes the segments it replaces. So a
/// `.log.swap` with no `.log.cleaned` beside it is a complete, durable segment,
/// and it may already be the only copy of the records it holds.
#[krabka_macros::model_type]
pub struct LocalRecoverySwapFacts {
    /// `<base>.log.cleaned` exists: the rewrite, or its rename to `.swap`,
    /// never finished.
    pub log_cleaned_exists: bool,
    /// `<base>.log.swap` exists.
    pub log_swap_exists: bool,
    /// `<base>.log` exists.
    pub final_log_exists: bool,
}

/// Crash-recovery action for one base offset that carries `.swap` files.
#[krabka_macros::model_type]
pub enum LocalRecoverySwapAction {
    /// The swap never committed: delete its `.swap` files. No segment it
    /// would replace has been touched.
    AbortSwap,
    /// The swap committed: delete every segment it replaces, then rename the
    /// swap into place. This is Kafka's `LogLoader.load` second and third
    /// passes.
    CompleteSwap,
    /// The log rename already finished; promote the remaining sidecars.
    PromoteSidecars,
    /// Sidecar swaps with no log in any form.
    Reject,
}

mod index_frontier;
#[cfg(creusot)]
pub use index_frontier::swap_committed;
pub use index_frontier::{
    local_recovery_batch_step, local_recovery_index_frontier, local_recovery_sealed_last,
    local_recovery_segment_chain, local_recovery_swap_action, local_recovery_swap_replaces,
};

#[cfg(test)]
mod tests;
