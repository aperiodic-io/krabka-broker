//! The in-memory fake per-node log (the in-memory consensus-core work stream). It is separate from the trait it
//! satisfies so that the real-log binary compiles the trait without also
//! carrying the fake's implementation in the same file.

use {
    super::node_log::SimNodeLog,
    krabka_kraft_core::simulation_support::{append_epochs, replicate_epochs, truncate_epochs},
    krabka_raft::kraft::types::Epoch,
};

/// A growable in-memory replicated log.
///
/// Each appended record stores the leader epoch that produced it, so
/// `end_offset_for_epoch` is a real lookup and not a stub. It returns the offset
/// of the first record whose epoch is strictly greater than the queried epoch,
/// which is where that epoch's run ends.
#[derive(Debug, Clone, Default)]
pub struct SimLog {
    /// `epochs[i]` is the leader epoch of the record at offset `i`.
    epochs: Vec<Epoch>,
}

krabka_macros::epoch_log_view!(SimLog, ::krabka_raft::kraft::types);

impl SimNodeLog for SimLog {
    fn append_in_epoch(&mut self, epoch: Epoch, count: usize) {
        append_epochs(&mut self.epochs, epoch, count);
    }

    fn truncate_to(&mut self, offset: i64) { truncate_epochs(&mut self.epochs, offset); }

    fn replicate_from(&mut self, leader: &Self) {
        // Divergent prefixes are truncated before this method runs.
        replicate_epochs(&mut self.epochs, &leader.epochs);
    }

    fn record_count(&self) -> usize { self.epochs.len() }
}
