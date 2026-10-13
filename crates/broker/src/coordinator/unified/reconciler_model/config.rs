//! The bounded model configuration: the member-id pool, the partition count and
//! the epoch cap, together with the static metadata image and the coordinator
//! config that the driven code reads.
//!
//! The shape of a run lives here rather than in the model state, so that the
//! state stays the part the checker enumerates and the bounds stay the part a
//! test picks.

use crate::coordinator::unified::actor::reconciliation_model_support::{ModelMetadata, metadata};

/// Bounded config. It lives here, not in the state.
pub(super) struct ReconModel {
    /// Member-id pool. A member can join, leave, and rejoin.
    pub(super) pool: Vec<&'static str>,
    pub(super) partitions: i32,
    pub(super) max_epoch: i32,
}

impl ReconModel {
    pub(super) fn basic() -> Self {
        Self {
            pool: vec!["a", "b"],
            partitions: 2,
            max_epoch: 9,
        }
    }

    pub(super) fn wide() -> Self {
        Self {
            pool: vec!["a", "b", "c"],
            partitions: 2,
            max_epoch: 7,
        }
    }

    pub(super) fn metadata(&self) -> ModelMetadata { metadata(self.partitions) }
}
