//! The bounded model configuration: the member-id pool, the partition count,
//! and the epoch cap, plus the metadata and coordinator config the driven code
//! needs.
//!
//! The shape of a run lives here rather than in the model state, so that the
//! state stays the part the checker enumerates and the bounds stay the part a
//! test picks.

use crate::coordinator::unified::actor::reconciliation_model_support::{ModelMetadata, metadata};

pub(super) struct CgcModel {
    pub(super) pool: Vec<&'static str>,
    pub(super) partitions: i32,
    pub(super) max_epoch: i32,
}

impl CgcModel {
    pub(super) fn basic() -> Self {
        Self {
            pool: vec!["a", "b"],
            partitions: 2,
            max_epoch: 7,
        }
    }

    pub(super) fn wide() -> Self {
        Self {
            pool: vec!["a", "b", "c"],
            partitions: 2,
            max_epoch: 6,
        }
    }

    pub(super) fn metadata(&self) -> ModelMetadata { metadata(self.partitions) }
}
