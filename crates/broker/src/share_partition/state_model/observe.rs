//! Read-only queries over the real `AcquisitionState`, which reach its private
//! batch list because this module is a descendant of the module that defines
//! it.
//!
//! Every other model module asks its questions about a machine through these
//! functions, so the one place that depends on the internal batch
//! representation is this file.

use {
    crate::share_partition::state::{AcquisitionState, RecordState},
    krabka_log::Offset,
};

/// Delivery state of `off`, if it currently lies in a batch.
pub(super) fn offset_state(sm: &AcquisitionState, off: Offset) -> Option<RecordState> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.state)
}

/// Delivery count of `off`, if it currently lies in a batch.
pub(super) fn offset_dc(sm: &AcquisitionState, off: Offset) -> Option<i16> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.delivery_count)
}

/// Every offset in the window that the schedule currently holds back.
pub(super) fn deferred_offsets(sm: &AcquisitionState) -> Vec<Offset> {
    sm.batches
        .iter()
        .filter(|b| b.state == RecordState::Deferred)
        .flat_map(|b| (b.first_offset.0..=b.last_offset.0).map(Offset))
        .collect()
}

/// Maximal contiguous offset runs currently Acquired by `member`. Adjacent
/// same-owner batches with different lock deadlines do not coalesce, so this
/// function stitches them back into one run. The whole run is then
/// ack-able and renew-able at once.
pub(super) fn acquired_runs(sm: &AcquisitionState, member: &str) -> Vec<(Offset, Offset)> {
    let mut runs: Vec<(Offset, Offset)> = Vec::new();
    let mut cur: Option<(Offset, Offset)> = None;
    for b in &sm.batches {
        let mine = b.state == RecordState::Acquired && b.acquired_by.as_deref() == Some(member);
        match (mine, cur) {
            (true, Some((f, l))) if b.first_offset == l + 1 => cur = Some((f, b.last_offset)),
            (true, Some((f, l))) => {
                runs.push((f, l));
                cur = Some((b.first_offset, b.last_offset));
            }
            (true, None) => cur = Some((b.first_offset, b.last_offset)),
            (false, Some((f, l))) => {
                runs.push((f, l));
                cur = None;
            }
            (false, None) => {}
        }
    }
    if let Some((f, l)) = cur {
        runs.push((f, l));
    }
    runs
}
