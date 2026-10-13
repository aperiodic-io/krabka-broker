use {
    super::{
        BarrierRecoveryFinalizeDecision, BarrierRecoveryFoldAction, BarrierRecoveryRecordKind,
        ReplayCursorDecision, ReplayRecordDecision,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A record is replayed iff its absolute offset `batch_base + record_delta`
/// lies in the half-open replay window `[from, end)` and its batch is of the
/// kind this pass replays: data batches for the metadata image, control
/// batches for the voter and quorum state.
pub fn replay_record_admitted(
    batch_base: i64,
    record_delta: i32,
    from: i64,
    end: i64,
    control_batch: bool,
    replay_control: bool,
) -> bool {
    pearlite! {
        record_delta@ >= 0
            && control_batch == replay_control
            && from@ <= batch_base@ + record_delta@
            && batch_base@ + record_delta@ < end@
    }
}

/// Decide whether controller recovery replays one decoded metadata record,
/// and at which absolute offset.
///
/// A negative delta is malformed and skipped. Because an admitted offset is
/// below `end`, it is always representable, so no overflow case needs a rule
/// of its own.
#[ensures(match result {
    ReplayRecordDecision::Apply(offset) =>
        replay_record_admitted(batch_base, record_delta, from, end, control_batch, replay_control)
            && offset@ == batch_base@ + record_delta@,
    ReplayRecordDecision::Skip =>
        !replay_record_admitted(batch_base, record_delta, from, end, control_batch, replay_control),
})]
#[must_use]
pub fn replay_record_decision(
    batch_base: i64,
    record_delta: i32,
    from: i64,
    end: i64,
    control_batch: bool,
    replay_control: bool,
) -> ReplayRecordDecision {
    if record_delta < 0 || batch_base >= end || control_batch != replay_control {
        return ReplayRecordDecision::Skip;
    }
    let delta = i64::from(record_delta);
    if batch_base > i64::MAX - delta {
        return ReplayRecordDecision::Skip;
    }
    let offset = batch_base + delta;
    if offset < from || offset >= end {
        ReplayRecordDecision::Skip
    } else {
        ReplayRecordDecision::Apply(offset)
    }
}

/// Select the exact mutation for one decoded barrier-state record.
///
/// The host applies this result to the entry named by the decoded group key,
/// in log order. A consumed epoch suppresses a later retry of its
/// injection-start, and only a matching epoch may clear the pending injection.
#[ensures(match result {
    BarrierRecoveryFoldAction::DefineGroup => {
        kind == BarrierRecoveryRecordKind::Group && value_present
    }
    BarrierRecoveryFoldAction::RemoveGroup => {
        kind == BarrierRecoveryRecordKind::Group && !value_present
    }
    BarrierRecoveryFoldAction::SetPending => {
            kind == BarrierRecoveryRecordKind::InjectionStart
            && value_present
            && !epoch_already_consumed
    }
    BarrierRecoveryFoldAction::KeepPending => {
        kind == BarrierRecoveryRecordKind::InjectionStart
            && ((value_present && epoch_already_consumed)
                || (!value_present && pending_epoch != Some(record_epoch)))
    }
    BarrierRecoveryFoldAction::ClearPending => {
        kind == BarrierRecoveryRecordKind::InjectionStart
            && !value_present
            && pending_epoch == Some(record_epoch)
    }
    BarrierRecoveryFoldAction::UpsertCut { retire_pending } => {
        kind == BarrierRecoveryRecordKind::Cut
            && value_present
            && retire_pending == (pending_epoch == Some(record_epoch))
    }
    BarrierRecoveryFoldAction::RemoveCut => {
        kind == BarrierRecoveryRecordKind::Cut && !value_present
    }
})]
#[must_use]
pub fn barrier_recovery_fold_action(
    kind: BarrierRecoveryRecordKind,
    value_present: bool,
    record_epoch: i64,
    pending_epoch: Option<i64>,
    epoch_already_consumed: bool,
) -> BarrierRecoveryFoldAction {
    match kind {
        BarrierRecoveryRecordKind::Group => {
            if value_present {
                BarrierRecoveryFoldAction::DefineGroup
            } else {
                BarrierRecoveryFoldAction::RemoveGroup
            }
        }
        BarrierRecoveryRecordKind::InjectionStart => {
            if value_present {
                if epoch_already_consumed {
                    BarrierRecoveryFoldAction::KeepPending
                } else {
                    BarrierRecoveryFoldAction::SetPending
                }
            } else if pending_epoch == Some(record_epoch) {
                BarrierRecoveryFoldAction::ClearPending
            } else {
                BarrierRecoveryFoldAction::KeepPending
            }
        }
        BarrierRecoveryRecordKind::Cut => {
            if value_present {
                BarrierRecoveryFoldAction::UpsertCut {
                    retire_pending: pending_epoch == Some(record_epoch),
                }
            } else {
                BarrierRecoveryFoldAction::RemoveCut
            }
        }
    }
}

/// Decide whether an interrupted injection can be finalized conservatively.
///
/// A valid finalization has a current coordinator at or above the frozen
/// coordinator epoch and at least one valid target partition. The recovery
/// adapter supplies no observed marker offsets, so this decision can only
/// authorize a partial cut.
#[ensures((result == BarrierRecoveryFinalizeDecision::NoPending) == !has_pending)]
#[ensures((result == BarrierRecoveryFinalizeDecision::MalformedPending)
    == (has_pending && (frozen_coordinator_epoch@ < 0 || !targets_valid)))]
#[ensures((result == BarrierRecoveryFinalizeDecision::UnknownCoordinator)
    == (has_pending
        && frozen_coordinator_epoch@ >= 0
        && targets_valid
        && current_coordinator_epoch == None))]
#[ensures((result == BarrierRecoveryFinalizeDecision::FencedCoordinator)
    == (has_pending
        && frozen_coordinator_epoch@ >= 0
        && targets_valid
        && match current_coordinator_epoch {
            Some(current) => current@ < frozen_coordinator_epoch@,
            None => false,
        }))]
#[ensures((result == BarrierRecoveryFinalizeDecision::FinalizePartial)
    == (has_pending
        && frozen_coordinator_epoch@ >= 0
        && targets_valid
        && match current_coordinator_epoch {
            Some(current) => current@ >= frozen_coordinator_epoch@,
            None => false,
        }))]
#[must_use]
pub fn barrier_recovery_finalize_decision(
    has_pending: bool,
    current_coordinator_epoch: Option<i32>,
    frozen_coordinator_epoch: i32,
    targets_valid: bool,
) -> BarrierRecoveryFinalizeDecision {
    if !has_pending {
        return BarrierRecoveryFinalizeDecision::NoPending;
    }
    if frozen_coordinator_epoch < 0 || !targets_valid {
        return BarrierRecoveryFinalizeDecision::MalformedPending;
    }
    let Some(current) = current_coordinator_epoch else {
        return BarrierRecoveryFinalizeDecision::UnknownCoordinator;
    };
    if current < frozen_coordinator_epoch {
        BarrierRecoveryFinalizeDecision::FencedCoordinator
    } else {
        BarrierRecoveryFinalizeDecision::FinalizePartial
    }
}

/// Advance a replay cursor to the next batch offset the reader reported, and
/// stop when there is none or it would not move the cursor forward, so a
/// replay loop always terminates.
#[ensures(match result {
    ReplayCursorDecision::Advance(next_offset) => next == Some(next_offset)
        && next_offset@ > cursor@,
    ReplayCursorDecision::Stop => match next {
        None => true,
        Some(next_offset) => next_offset@ <= cursor@,
    },
})]
#[must_use]
pub fn replay_cursor_decision(cursor: i64, next: Option<i64>) -> ReplayCursorDecision {
    match next {
        Some(next_offset) if next_offset > cursor => ReplayCursorDecision::Advance(next_offset),
        Some(_) | None => ReplayCursorDecision::Stop,
    }
}
