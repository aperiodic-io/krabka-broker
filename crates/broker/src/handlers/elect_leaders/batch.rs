//! The records one `ElectLeaders` request accumulates before it appends them,
//! and the audit trail that append owes once its outcome is known.
//!
//! A request elects many partitions and spends at most one break-glass approval
//! per proposal, so the consumed proposals and the leader changes gather here
//! and reach the metadata log as a single raft append.

use {
    crate::break_glass::handlers::batch::GatedBatch,
    krabka_metadata::BreakGlassAction,
    std::ops::{Deref, DerefMut},
};

/// What one `ElectLeaders` request accumulates across its partitions.
///
/// The [`GatedBatch`] it derefs to is the single raft append that carries
/// every consumed proposal beside every leader change the request makes, and
/// the unclean elections whose `Applied` events wait on that append.
pub(super) struct ElectionBatch(GatedBatch);

impl Default for ElectionBatch {
    fn default() -> Self {
        Self(GatedBatch::new(
            BreakGlassAction::UncleanElectLeaders,
            "unclean leader election admitted",
            "unclean leader election committed",
        ))
    }
}

impl Deref for ElectionBatch {
    type Target = GatedBatch;

    fn deref(&self) -> &GatedBatch { &self.0 }
}

impl DerefMut for ElectionBatch {
    fn deref_mut(&mut self) -> &mut GatedBatch { &mut self.0 }
}
