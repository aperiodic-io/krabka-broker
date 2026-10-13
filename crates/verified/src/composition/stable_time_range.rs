#[cfg(creusot)]
use super::time_index::sparse_timestamp_window_valid;
#[cfg(creusot)]
use super::time_index::time_segment_valid;
use {
    super::{
        FetchWatermarks, SparseTimestampWindow, committed_fetch_excludes_unstable,
        constructed_time_range_preserves_first,
    },
    creusot_std::prelude::*,
};

/// Consume the derived transaction/HW/delivery prefix and a constructed interval
/// scan. Offset order makes its exclusive limit safe even when timestamps regress;
/// inherited LSO and timestamp cursors cannot replace the actual derived limit.
/// Complete decoded records and a valid minimum-equivalent transaction snapshot,
/// coherent publication, physical reads and client-side abort filtering remain
/// external. This proves prefix gating only.
#[requires(sparse_timestamp_window_valid(window.0@, window.1@, window.2@))]
#[requires(span.0@ >= 0 && w.log_start@ >= 0 && targets.0@ <= targets.1@)]
#[requires(super::time_index::absolute_record_prefix_bounded(window.0@, window.0@.len(), span.0@, i64::MAX@))]
#[ensures(match result {
    Err(()) => (exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@)
        || !time_segment_valid(span.0@, span.1@)
        || exists<i: Int> 0 <= i && i < window.0@.len() && span.0@ + window.0@[i]@ > span.1@,
    Ok((lso, limit, selected)) => time_segment_valid(span.0@, span.1@)
        && (super::time_index::absolute_record_prefix_bounded(window.0@, window.0@.len(), span.0@, span.1@))
        && crate::transaction::first_unstable_frontier(starts@, w.log_end@, lso@)
        && limit@ == lso@.min(w.hw@).min(w.deliverable@)
        && (crate::transaction::unstable_fetch_limit_maximal(starts@, w.log_end@, w.hw@, w.deliverable@, limit@))
        && match selected {
            None => timestamp_prefix_excluded(window.0@, window.1@, window.0@.len(), span.0@, w.log_start@, limit@, targets),
            Some(index) => index@ < window.0@.len() && w.log_start@ <= span.0@ + window.0@[index@]@
                && span.0@ + window.0@[index@]@ < limit@ && targets.0@ <= window.1@[index@]@ && window.1@[index@]@ <= targets.1@
                && (forall<i: Int> 0 <= i && i < starts@.len() ==> span.0@ + window.0@[index@]@ < starts@[i]@)
                && timestamp_prefix_excluded(window.0@, window.1@, index@, span.0@, w.log_start@, limit@, targets),
        },
})]
pub(super) fn stable_time_range_preserves_first(
    window: SparseTimestampWindow<'_>,
    starts: &[i64],
    span: (i64, i64), // segment base and inclusive end
    w: FetchWatermarks,
    targets: (i64, i64),
) -> Result<(i64, i64, Option<usize>), ()> {
    let (lso, visibility) = committed_fetch_excludes_unstable(starts, w).ok_or(())?;
    let (_lower, _upper, _scan, candidate) =
        constructed_time_range_preserves_first(window, (span.0, span.1, w.log_start), targets)?;
    let selected = match candidate {
        Some(index) if span.0 + i64::from(window.0[index]) < visibility.limit_offset => Some(index),
        _ => None,
    };
    Ok((lso, visibility.limit_offset, selected))
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// No inspected record is both visible in the offset window and inside the timestamp interval.
fn timestamp_prefix_excluded(
    offsets: Seq<u32>,
    timestamps: Seq<i64>,
    count: Int,
    base: Int,
    floor: Int,
    limit: Int,
    targets: (i64, i64),
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count ==> base + offsets[i]@ < floor || base + offsets[i]@ >= limit || super::timestamp::outside_timestamp_interval(timestamps[i]@, targets) }
}
