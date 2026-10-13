//! Controller recovery replay-bound decisions.

use creusot_std::prelude::*;

/// Whether controller recovery replays one metadata record.
#[krabka_macros::model_type]
pub enum ReplayRecordDecision {
    /// Apply the record, which sits at this absolute offset.
    Apply(i64),
    /// Skip the record.
    Skip,
}

/// Whether a replay loop advances its cursor, and to where.
#[krabka_macros::model_type]
pub enum ReplayCursorDecision {
    /// Continue replay from this exclusive next offset.
    Advance(i64),
    /// End replay.
    Stop,
}

/// The keyed barrier-state record that recovery is folding.
#[krabka_macros::model_type]
pub enum BarrierRecoveryRecordKind {
    Group,
    InjectionStart,
    Cut,
}

/// The only state mutation one ordered barrier record may perform.
#[krabka_macros::model_type]
pub enum BarrierRecoveryFoldAction {
    DefineGroup,
    RemoveGroup,
    SetPending,
    KeepPending,
    ClearPending,
    UpsertCut { retire_pending: bool },
    RemoveCut,
}

/// Whether recovery may close an interrupted barrier injection.
#[krabka_macros::model_type]
pub enum BarrierRecoveryFinalizeDecision {
    NoPending,
    MalformedPending,
    UnknownCoordinator,
    FencedCoordinator,
    FinalizePartial,
}

mod replay_cursor_decision;
#[cfg(creusot)]
pub use replay_cursor_decision::replay_record_admitted;
pub use replay_cursor_decision::{
    barrier_recovery_finalize_decision, barrier_recovery_fold_action, replay_cursor_decision,
    replay_record_decision,
};

mod should_capture_first_downgrade;
pub use should_capture_first_downgrade::{
    replay_batch_cursor_decision, should_capture_first_downgrade,
};

#[cfg(test)]
mod tests;
