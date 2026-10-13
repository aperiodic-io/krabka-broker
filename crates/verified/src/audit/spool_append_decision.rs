use {
    super::{AuditCheckpointAdmission, AuditLossMarkerAdmission, AuditLosses, SpoolAppendDecision},
    creusot_std::prelude::*,
};

/// Bind a verified checkpoint to the exact nonempty chain position and head.
#[ensures((result == AuditCheckpointAdmission::Admit) == (
    signature_valid && head_matches && expected_seq@ > 0
        && checkpoint_seq_high@ + 1 == expected_seq@))]
#[ensures((result == AuditCheckpointAdmission::RejectSignature) == !signature_valid)]
#[ensures((result == AuditCheckpointAdmission::RejectHead)
    == (signature_valid && !head_matches))]
#[ensures((result == AuditCheckpointAdmission::RejectSequence) == (
    signature_valid && head_matches
        && (expected_seq@ == 0 || checkpoint_seq_high@ + 1 != expected_seq@)))]
#[must_use]
pub fn audit_checkpoint_admission(
    signature_valid: bool,
    head_matches: bool,
    expected_seq: u64,
    checkpoint_seq_high: u64,
) -> AuditCheckpointAdmission {
    if !signature_valid {
        AuditCheckpointAdmission::RejectSignature
    } else if !head_matches {
        AuditCheckpointAdmission::RejectHead
    } else if expected_seq == 0 || checkpoint_seq_high != expected_seq - 1 {
        AuditCheckpointAdmission::RejectSequence
    } else {
        AuditCheckpointAdmission::Admit
    }
}

/// Admit exactly a marker whose body is the two-field
/// `{records_lost, loss_generation}` shape with a positive count and a
/// generation strictly newer than the last accepted one.
#[ensures(match result {
    AuditLossMarkerAdmission::Admit { generation: admitted } => header_matches
        && field_count@ == 2
        && count@ > 0
        && generation == Some(admitted)
        && admitted@ > previous_generation@,
    AuditLossMarkerAdmission::Reject => !(header_matches && field_count@ == 2 && count@ > 0
        && match generation {
            Some(generation) => generation@ > previous_generation@,
            None => false,
        }),
})]
#[must_use]
pub fn audit_loss_marker_admission(
    header_matches: bool,
    field_count: u64,
    count: u64,
    generation: Option<u64>,
    previous_generation: u64,
) -> AuditLossMarkerAdmission {
    match generation {
        Some(generation)
            if header_matches
                && field_count == 2
                && count > 0
                && generation > previous_generation =>
        {
            AuditLossMarkerAdmission::Admit { generation }
        }
        _ => AuditLossMarkerAdmission::Reject,
    }
}

/// Settle the pending losses `state` against a durable marker that reports
/// `batch`, at the writer's commit and at open-time reconciliation alike.
///
/// A marker settles only its own generation, and only the count it reports.
/// Losses that `AuditHandle::emit` added after the writer's snapshot stay
/// pending, and move to the next generation. The host only ever moves a
/// generation forward, so no marker already in the spool names that one: no
/// later reconciliation settles them a second time, and the next marker names
/// a generation newer than the last, as [`audit_loss_marker_admission`]
/// requires. A marker of another generation settles nothing.
///
/// A batch larger than the pending count settles it to zero. The host never
/// produces one -- a batch is a snapshot of the pending count, which only grows
/// within a generation until settled -- so that case is clamped, not
/// conserved. At `u64::MAX` the generation saturates rather than bumps; that
/// takes 2^64 settlements and is the host's to rule out.
#[ensures(state.generation != batch.generation
    ==> result.generation == state.generation && result.count == state.count)]
#[ensures(state.generation == batch.generation && batch.count@ <= state.count@
    ==> result.count@ + batch.count@ == state.count@)]
#[ensures(state.generation == batch.generation && batch.count@ > state.count@
    ==> result.count@ == 0)]
#[ensures(state.generation == batch.generation
    ==> (result.generation != state.generation)
        == (result.count@ > 0 && state.generation@ < u64::MAX@))]
#[ensures(result.generation != state.generation
    ==> result.generation@ == state.generation@ + 1)]
#[must_use]
pub fn settle_loss_batch(state: AuditLosses, batch: AuditLosses) -> AuditLosses {
    if state.generation != batch.generation {
        return state;
    }
    if batch.count >= state.count {
        return AuditLosses {
            generation: state.generation,
            count: 0,
        };
    }
    AuditLosses {
        generation: if state.generation < u64::MAX {
            state.generation + 1
        } else {
            state.generation
        },
        count: state.count - batch.count,
    }
}

/// Admit one frame and advance the successful-append sync cadence.
#[requires(sync_every@ > 0)]
#[requires(unsynced@ < sync_every@)]
#[ensures(result.accepted
    == (current_bytes@ <= max_bytes@ && frame_bytes@ <= max_bytes@ - current_bytes@))]
#[ensures(result.accepted ==> result.new_bytes@ == current_bytes@ + frame_bytes@)]
#[ensures(result.accepted ==> result.new_bytes@ <= max_bytes@)]
#[ensures(!result.accepted ==> result.new_bytes@ == current_bytes@)]
#[ensures(result.sync
    == (result.accepted && unsynced@ + 1 >= sync_every@))]
#[ensures(result.next_unsynced@ < sync_every@)]
#[ensures(result.next_unsynced@ == if result.accepted {
    if unsynced@ + 1 >= sync_every@ { 0 } else { unsynced@ + 1 }
} else {
    unsynced@
})]
#[must_use]
pub fn spool_append_decision(
    current_bytes: u64,
    frame_bytes: u64,
    max_bytes: u64,
    unsynced: u64,
    sync_every: u64,
) -> SpoolAppendDecision {
    if current_bytes > max_bytes || frame_bytes > max_bytes - current_bytes {
        return SpoolAppendDecision {
            accepted: false,
            new_bytes: current_bytes,
            sync: false,
            next_unsynced: unsynced,
        };
    }

    let next_unsynced = unsynced + 1;
    let sync = next_unsynced >= sync_every;
    SpoolAppendDecision {
        accepted: true,
        new_bytes: current_bytes + frame_bytes,
        sync,
        next_unsynced: if sync { 0 } else { next_unsynced },
    }
}
