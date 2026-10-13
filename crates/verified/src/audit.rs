//! Pure admission and sync-cadence arithmetic for the audit spool.

use creusot_std::prelude::*;

/// State transition for one attempted audit-spool append.
#[krabka_macros::model_type]
pub struct SpoolAppendDecision {
    pub accepted: bool,
    pub new_bytes: u64,
    pub sync: bool,
    pub next_unsynced: u64,
}

/// Admission result for a signed audit checkpoint at the current chain head.
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AuditCheckpointAdmission {
    Admit,
    RejectSignature,
    RejectHead,
    RejectSequence,
}

/// Admission result for a records-lost marker.
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq))]
pub enum AuditLossMarkerAdmission {
    /// The marker is accepted and `generation` becomes the last accepted one.
    Admit {
        generation: u64,
    },
    Reject,
}

/// Fail-open audit losses: the pending count `PendingLosses` holds, or the
/// batch a durable records-lost marker reports. `generation` is the
/// `loss_generation` a marker for these losses names.
#[krabka_macros::model_type(runtime(Clone, Copy, Debug, Default, PartialEq, Eq, Hash))]
pub struct AuditLosses {
    pub generation: u64,
    pub count: u64,
}

mod spool_append_decision;
pub use spool_append_decision::{
    audit_checkpoint_admission, audit_loss_marker_admission, settle_loss_batch,
    spool_append_decision,
};

#[cfg(test)]
mod tests;
