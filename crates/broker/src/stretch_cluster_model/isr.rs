//! In-sync-replica bookkeeping: the replica ordering that the controller
//! writes, and the catch-up that a site recovery folds in.
//!
//! The rejoin rule is the one place where the model states a behaviour that
//! `isr_maintenance` performs in production, so it is worth its own file.

use {
    super::{
        config::StretchModel,
        state::{StretchState, same_component},
    },
    krabka_raft::NodeId,
};

impl StretchModel {
    /// Put the in-sync replica set back in replica order, which is the order
    /// the controller writes and the order a clean election reads.
    pub fn normalize_isr(&self, isr: &mut [NodeId]) {
        isr.sort_by_key(|node| {
            self.replicas
                .iter()
                .position(|replica| replica == node)
                .unwrap_or(usize::MAX)
        });
    }

    /// Fold the `isr_maintenance` catch-up into a site recovery. A replica
    /// that runs again and that reaches the leader has nothing to catch up on
    /// in an idle partition, so the controller puts it back in the in-sync
    /// replica set.
    pub fn rejoin_isr(&self, state: &mut StretchState, site: u8) {
        if !same_component(state, site, self.site_of(state.leader)) {
            return;
        }
        let returning: Vec<NodeId> = self
            .replicas
            .iter()
            .copied()
            .filter(|&replica| self.site_of(replica) == site && !state.isr.contains(&replica))
            .collect();
        state.isr.extend(returning);
        self.normalize_isr(&mut state.isr);
    }
}
