//! Storage-transition admission decisions.

use creusot_std::prelude::*;

/// Segment-prefix and active-segment selection for a tail truncation.
#[krabka_macros::model_type]
pub struct LocalTruncationPlan {
    pub retained_sealed: usize,
    pub keep_active: bool,
}

/// KIP-405 `RemoteLogSegmentState`, shared by every remote-segment kernel so
/// the host maps its state onto one type.
#[cfg_attr(creusot, derive(std::clone::Clone, Copy, DeepModel))]
#[cfg_attr(
    not(creusot),
    derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)
)]
pub enum RemoteSegmentLifecycle {
    CopyStarted,
    CopyFinished,
    DeleteStarted,
    DeleteFinished,
}

/// KIP-405 `RemotePartitionDeleteState`.
#[krabka_macros::model_type]
pub enum RemotePartitionDeleteLifecycle {
    Marked,
    Started,
    Finished,
}

/// Mutation of the primary remote-metadata cache and its derived epoch index.
#[krabka_macros::model_type]
pub enum RemoteCacheAction {
    /// Reject a stale, conflicting, or resurrection attempt.
    Reject,
    /// Preserve state for an exact retry or an already-absent tombstone.
    Noop,
    /// Store the new readable state and rebuild the derived epoch index.
    StoreFinished,
    /// Store a non-readable state and rebuild the index without it.
    StoreHidden,
    /// Remove primary state and rebuild the index without it.
    Remove,
}

mod future_log_swap_admission;
pub use future_log_swap_admission::{
    future_log_swap_admission, local_append_coordinates, local_truncation_plan,
    truncation_batch_retained, truncation_frontier, truncation_relative_offset,
};

mod remote_partition_delete_transition;
pub use remote_partition_delete_transition::{
    remote_cache_action, remote_partition_delete_transition, remote_segment_transition,
};

#[cfg(test)]
mod tests;
