//! The growable in-memory log a simulated node replicates.
//!
//! [`SimLog`] is a focused copy of the integration harness log. It implements
//! the [`LogView`] seam the state machine reads through, and it stores a leader
//! epoch per record, so the diverging-epoch lookup is real and not a stub.

use crate::{
    event::LogEnd,
    simulation_support::{append_epochs, truncate_epochs},
    types::{Epoch, LogView},
};

/// A growable in-memory replicated log.
///
/// Each appended record stores the leader epoch that produced it, so
/// `end_offset_for_epoch` is a real lookup and not a stub.
#[derive(Debug, Clone, Default)]
pub(super) struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<Epoch>,
}

krabka_macros::epoch_log_view!(SimLog, crate);

impl SimLog {
    pub(super) fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        append_epochs(&mut self.epochs, epoch, count);
    }

    pub(super) fn truncate_to(&mut self, offset: i64) { truncate_epochs(&mut self.epochs, offset); }

    pub(super) fn replicate_from(&mut self, leader: &Self) {
        let leader_epochs = &leader.epochs;
        if self.epochs.len() < leader_epochs.len() {
            self.epochs.clone_from(leader_epochs);
        }
    }

    pub(super) fn record_count(&self) -> usize { self.epochs.len() }

    pub(super) fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}
