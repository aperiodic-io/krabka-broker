use {
    super::{AuditLosses, settle_loss_batch},
    crate::audit::{AuditLossMarkerAdmission, audit_loss_marker_admission},
    creusot_std::prelude::*,
};

/// Replaying a durable loss marker cannot subtract pending losses twice.
/// Return both actual states so a consumer can use the conservation law.
/// Generation exhaustion is a real ceiling of the saturating host protocol.
#[requires(state.generation@ < u64::MAX@)]
#[ensures(result.0.count == result.1.count && result.0.generation == result.1.generation)]
#[ensures(result.0.count@ == if state.generation == marker.generation {
    if marker.count@ <= state.count@ { state.count@ - marker.count@ } else { 0 }
} else { state.count@ })]
#[ensures(result.0.generation@ == if state.generation == marker.generation
    && marker.count@ < state.count@ { state.generation@ + 1 } else { state.generation@ })]
pub(super) fn loss_settlement_is_idempotent(
    state: AuditLosses,
    marker: AuditLosses,
) -> (AuditLosses, AuditLosses) {
    let settled = settle_loss_batch(state, marker);
    let replayed = settle_loss_batch(settled, marker);
    (settled, replayed)
}

type AdmittedLossReplay = (
    AuditLosses,
    AuditLosses,
    AuditLossMarkerAdmission,
    AuditLossMarkerAdmission,
);

/// An admitted durable snapshot settles its count exactly once, keeps losses
/// added after that snapshot, rejects its duplicate, and permits a new marker
/// precisely when a remainder exists. Matching generations and the snapshot
/// count bound are host invariants; decoding and durability remain external.
#[requires(state.generation@ < u64::MAX@)]
#[requires(marker.generation == state.generation && marker.count@ <= state.count@)]
#[ensures(match result {
    None => !header_matches || field_count@ != 2 || marker.count@ == 0
        || marker.generation@ <= previous@,
    Some((settled, replayed, duplicate, next)) => header_matches && field_count@ == 2
        && marker.count@ > 0 && marker.generation@ > previous@
        && settled.count@ + marker.count@ == state.count@
        && settled.count == replayed.count && settled.generation == replayed.generation
        && settled.generation@ == if settled.count@ > 0 {
            marker.generation@ + 1
        } else { marker.generation@ }
        && duplicate == AuditLossMarkerAdmission::Reject
        && match next {
            AuditLossMarkerAdmission::Reject => settled.count@ == 0,
            AuditLossMarkerAdmission::Admit { generation } => settled.count@ > 0
                && generation == settled.generation && generation@ > marker.generation@,
        },
})]
pub(super) fn admitted_loss_marker_preserves_pending(
    state: AuditLosses,
    marker: AuditLosses,
    previous: u64,
    header_matches: bool,
    field_count: u64,
) -> Option<AdmittedLossReplay> {
    let admission = audit_loss_marker_admission(
        header_matches,
        field_count,
        marker.count,
        Some(marker.generation),
        previous,
    );
    let AuditLossMarkerAdmission::Admit { generation } = admission else {
        return None;
    };
    let (settled, replayed) = loss_settlement_is_idempotent(state, marker);
    let duplicate = audit_loss_marker_admission(
        header_matches,
        field_count,
        marker.count,
        Some(marker.generation),
        generation,
    );
    let next = audit_loss_marker_admission(
        true,
        2,
        replayed.count,
        Some(replayed.generation),
        generation,
    );
    Some((settled, replayed, duplicate, next))
}
