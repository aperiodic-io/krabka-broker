use creusot_std::prelude::*;

#[cfg(creusot)]
use super::epoch::{epoch_archive_valid, epoch_window_valid};
use super::{
    EpochEntry, FetchWatermarks, ProducerReloadRange, truncated_snapshot_selection_bounds_replay,
    validated_epochs_bound_truncated_fetch,
};
use crate::broker::FetchVisibility;
#[cfg(creusot)]
use crate::broker::{clamped_fetch_watermarks, committed_fetch_response};
#[cfg(creusot)]
use crate::leader_epoch::kafka_end_offset_for;

type EpochSnapshotReplay = (i32, FetchWatermarks, FetchVisibility, Option<usize>, i64);

/// Consume the validated epoch cut to select a retained snapshot and exact
/// replay cursor. Reject invalid inputs or a resolved cut below either floor:
/// that host boundary requires reset, not replay of pruned history. Snapshot
/// bytes, complete enumeration, actual batch-aligned truncation and I/O remain
/// external. Ok(None) preserves the distinct unplaceable-epoch outcome.
#[ensures(match result {
    Err(()) => !epoch_window_valid(segment_base@, w.log_start@, w.log_end@)
        || !epoch_archive_valid(entries@, segment_base@, w.log_end@)
        || local_start@ < 0 || local_start@ > w.log_end@
        || exists<f: Int, cut: Int> kafka_end_offset_for(entries@, requested@, w.log_end@, f, cut)
            && cut >= 0 && (cut < w.log_start@ || cut < local_start@),
    Ok(None) => epoch_window_valid(segment_base@, w.log_start@, w.log_end@)
        && epoch_archive_valid(entries@, segment_base@, w.log_end@)
        && 0 <= local_start@ && local_start@ <= w.log_end@
        && kafka_end_offset_for(entries@, requested@, w.log_end@, -1, -1),
    Ok(Some((found, bounded, visibility, selected, cursor))) => super::epoch::resolved_epoch_cut(entries@, segment_base@, w, requested@, found@, bounded.log_end@)
        && found@ <= requested@ && segment_base@ <= bounded.log_end@ && bounded.log_end@ <= w.log_end@
        && clamped_fetch_watermarks(w, bounded)
        && visibility.limit_offset@ == w.hw@.min(w.lso@).min(w.deliverable@).min(bounded.log_end@)
        && committed_fetch_response(visibility, bounded.hw, bounded.lso, bounded.deliverable, w.log_start@, bounded.log_end@)
        && 0 <= local_start@ && local_start@ <= cursor@ && w.log_start@ <= cursor@ && cursor@ <= bounded.log_end@
        && match selected {
            None => cursor@ == w.log_start@.max(local_start@)
                && forall<i: Int> 0 <= i && i < snapshots@.len()
                    ==> !(w.log_start@ < snapshots@[i]@ && snapshots@[i]@ <= bounded.log_end@),
            Some(index) => index@ < snapshots@.len()
                && w.log_start@ < snapshots@[index@]@ && snapshots@[index@]@ <= bounded.log_end@
                && cursor@ == local_start@.max(snapshots@[index@]@)
                && super::trim::latest_retained_snapshot(snapshots@, w.log_start@, bounded.log_end@, snapshots@[index@]@),
        },
})]
pub(super) fn resolved_epoch_bounds_retained_replay(
    entries: &[EpochEntry],
    requested: i32,
    segment_base: i64,
    w: FetchWatermarks,
    snapshots: &[i64],
    local_start: i64,
) -> Result<Option<EpochSnapshotReplay>, ()> {
    if local_start < 0 || local_start > w.log_end {
        return Err(());
    }
    let Some((found, bounded, visibility)) =
        validated_epochs_bound_truncated_fetch(entries, requested, segment_base, w)?
    else {
        return Ok(None);
    };
    if bounded.log_end < w.log_start || bounded.log_end < local_start {
        return Err(());
    }
    let range = ProducerReloadRange {
        log_start: w.log_start,
        local_start,
        log_end: w.log_end,
    };
    let (selected, cursor) =
        truncated_snapshot_selection_bounds_replay(snapshots, range, bounded.log_end).ok_or(())?;
    Ok(Some((found, bounded, visibility, selected, cursor)))
}
