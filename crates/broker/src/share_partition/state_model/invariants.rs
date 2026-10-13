//! The safety claims the model proves: the predicates that must hold in every
//! reachable state, and the comparison that must hold across every single
//! transition.
//!
//! Both kinds of claim live together because they say one thing between them,
//! which is what a share partition may never do. The state-level predicates
//! become `Property::always` entries, and `assert_transition` runs inside
//! `next_state` on the parent and the child.

use {
    super::{
        observe::{offset_dc, offset_state},
        state::ShareAction,
    },
    crate::share_partition::state::{AcquisitionState, InFlightBatch, RecordState},
    assert2::assert,
    krabka_log::Offset,
};

// ---- state-level invariants (Property::always predicates) ------------------

/// Batches are sorted, gap-free, non-overlapping, and exactly cover
/// `[start_offset, end_offset)`. Also, `start_offset <= end_offset`.
pub(super) fn window_integrity(sm: &AcquisitionState) -> bool {
    if sm.start_offset > sm.end_offset {
        return false;
    }
    if sm.batches.is_empty() {
        return sm.start_offset == sm.end_offset;
    }
    if sm.batches[0].first_offset != sm.start_offset {
        return false;
    }
    for w in sm.batches.windows(2) {
        if w[0].first_offset > w[0].last_offset || w[0].last_offset + 1 != w[1].first_offset {
            return false;
        }
    }
    let last = sm.batches.last().expect("non-empty checked above");
    last.first_offset <= last.last_offset && last.last_offset + 1 == sm.end_offset
}

/// Every Acquired batch carries exactly one owner. With
/// `window_integrity`'s non-overlap, no offset is concurrently held by two
/// members. That is the main share-group guarantee.
pub(super) fn mutual_exclusion(sm: &AcquisitionState) -> bool {
    sm.batches
        .iter()
        .all(|b| b.state != RecordState::Acquired || b.acquired_by.is_some())
}

/// Lock bookkeeping matches the delivery state. Acquired ⇒ both owner and
/// deadline present. Every other state ⇒ neither present.
pub(super) fn lock_consistency(sm: &AcquisitionState) -> bool {
    sm.batches.iter().all(|b| match b.state {
        RecordState::Acquired => b.acquired_by.is_some() && b.lock_deadline.is_some(),
        _ => b.acquired_by.is_none() && b.lock_deadline.is_none(),
    })
}

/// The delivery complete count is Kafka's: the number of Acknowledged and
/// Archived records in the window, all of which sit at or above the SPSO
/// (`SharePartition.deliveryCompleteCount`). It is not a cumulative counter,
/// so it shrinks when the SPSO moves past terminal records, and
/// `end - start - count` is the lag that `DescribeShareGroupOffsets` reports.
pub(super) fn delivery_complete_count_is_terminal_in_window(sm: &AcquisitionState) -> bool {
    let terminal: i64 = sm
        .batches
        .iter()
        .filter(|b| b.first_offset >= sm.start_offset && b.is_terminal())
        .map(InFlightBatch::len)
        .sum();
    i64::from(sm.delivery_complete_count) == terminal
}

// ---- transition-level invariants (asserted in next_state) ------------------

/// Compare a parent machine to its child after one operation, and panic on any
/// monotonicity or durability violation. This stays OUT of the fingerprinted
/// state, so no path-history ghost can explode the space. That was the Phase-1
/// OOM lesson.
pub(super) fn assert_transition(
    parent: &AcquisitionState,
    child: &AcquisitionState,
    action: ShareAction,
) {
    // KFC-1: `promote_deferred` is the only route out of `Deferred`. A leader
    // reload does write the record back as `Available`, but the new leader
    // re-derives the deferral before the state is readable again, so the
    // deferred set is unchanged across that transition too.
    if action != ShareAction::PromoteDeferred {
        for raw in parent.start_offset.0..parent.end_offset.0 {
            let off = Offset(raw);
            if offset_state(parent, off) == Some(RecordState::Deferred) {
                assert!(
                    offset_state(child, off) == Some(RecordState::Deferred),
                    "deferred offset {off} left Deferred on {action:?}"
                );
            }
        }
    }
    assert!(
        child.start_offset >= parent.start_offset,
        "SPSO regressed: {} -> {}",
        parent.start_offset,
        child.start_offset
    );
    // Per-offset delivery_count never regresses for offsets live in both.
    for raw in child.start_offset.0..child.end_offset.0 {
        let off = Offset(raw);
        if let (Some(pc), Some(cc)) = (offset_dc(parent, off), offset_dc(child, off)) {
            assert!(
                cc >= pc,
                "delivery_count regressed at offset {off}: {pc} -> {cc}"
            );
        }
    }
    // An Acknowledged offset is terminal: in the child it is still Acknowledged
    // or has dropped below the (non-decreasing) SPSO — never resurrected.
    for raw in parent.start_offset.0..parent.end_offset.0 {
        let off = Offset(raw);
        if offset_state(parent, off) == Some(RecordState::Acknowledged) {
            match offset_state(child, off) {
                None => assert!(
                    off < child.start_offset,
                    "acknowledged offset {off} vanished while still in window"
                ),
                Some(s) => assert!(
                    s == RecordState::Acknowledged,
                    "acknowledged offset {off} reverted to {s:?}"
                ),
            }
        }
    }
}
