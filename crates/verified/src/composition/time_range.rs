#[cfg(creusot)]
use super::time_index::sparse_timestamp_window_valid;
#[cfg(creusot)]
use super::time_index::time_segment_valid;
use {
    super::{
        SparseTimestampWindow, constructed_time_index_preserves_first, first_timestamp_index,
        validated_time_cursors_are_monotone,
    },
    creusot_std::prelude::*,
};

type TimeRangeWitness = (i64, i64, i64, Option<usize>);

/// Construct truthful sparse maxima and consume validated cursors to find the
/// first retained timestamp in a closed interval. Scan the complete suffix:
/// an upper timestamp cursor cannot exclude later timestamp regressions.
/// Faithful decoding/enumeration, physical reads and coherent floors are external.
#[requires(sparse_timestamp_window_valid(window.0@, window.1@, window.2@))]
#[requires(bounds.0@ >= 0 && bounds.2@ >= 0 && targets.0@ <= targets.1@)]
#[requires(super::time_index::absolute_record_prefix_bounded(window.0@, window.0@.len(), bounds.0@, i64::MAX@))]
#[ensures(match result {
    Err(()) => !time_segment_valid(bounds.0@, bounds.1@)
        || exists<i: Int> 0 <= i && i < window.0@.len() && bounds.0@ + window.0@[i]@ > bounds.1@,
    Ok((lower_cursor, upper_cursor, scan, selected)) => time_segment_valid(bounds.0@, bounds.1@)
        && (super::time_index::absolute_record_prefix_bounded(window.0@, window.0@.len(), bounds.0@, bounds.1@))
        && bounds.0@ <= scan@ && scan@ <= lower_cursor@ && lower_cursor@ <= upper_cursor@ && upper_cursor@ <= bounds.1@
        && (forall<i: Int> 0 <= i && i < window.0@.len() && bounds.0@ + window.0@[i]@ < scan@ ==> window.1@[i]@ < targets.0@)
        && match selected {
            None => forall<i: Int> 0 <= i && i < window.0@.len()
                ==> bounds.0@ + window.0@[i]@ < bounds.2@ || super::timestamp::outside_timestamp_interval(window.1@[i]@, targets),
            Some(index) => index@ < window.0@.len() && bounds.0@ + window.0@[index@]@ >= bounds.2@
                && bounds.0@ + window.0@[index@]@ >= scan@ && targets.0@ <= window.1@[index@]@ && window.1@[index@]@ <= targets.1@
                && forall<i: Int> 0 <= i && i < index@
                    ==> bounds.0@ + window.0@[i]@ < bounds.2@ || super::timestamp::outside_timestamp_interval(window.1@[i]@, targets),
        },
})]
pub(super) fn constructed_time_range_preserves_first(
    window: SparseTimestampWindow<'_>,
    bounds: (i64, i64, i64), // base, inclusive segment end, logical floor
    targets: (i64, i64),
) -> Result<TimeRangeWitness, ()> {
    let (base, end, minimum) = bounds;
    let (lower, upper) = targets;
    let (offsets, times, rows) = window;
    let (entries, _single_target) =
        constructed_time_index_preserves_first(offsets, times, rows, lower);
    let (lower_cursor, upper_cursor, scan) =
        validated_time_cursors_are_monotone(&entries, base, end, lower, upper)?;
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(super::time_index::absolute_record_prefix_bounded(offsets@, i@, base@, end@))]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if base + i64::from(offsets[i]) > end {
            return Err(());
        }
        i += 1;
    }
    proof_assert!(forall<i: Int> 0 <= i && i < offsets@.len() && base@ + offsets@[i]@ < scan@ ==> times@[i]@ < lower@);
    let mut eligible: Vec<i64> = Vec::new();
    i = 0;
    #[invariant(i@ <= offsets@.len() && eligible@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> eligible@[j]@ ==
        if base@ + offsets@[j]@ >= minimum@ && lower@ <= times@[j]@ && times@[j]@ <= upper@ { 1 } else { 0 })]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        eligible.push(i64::from(
            base + i64::from(offsets[i]) >= scan
                && base + i64::from(offsets[i]) >= minimum
                && times[i] >= lower
                && times[i] <= upper,
        ));
        i += 1;
    }
    let selected = first_timestamp_index(&eligible, 1);
    Ok((lower_cursor, upper_cursor, scan, selected))
}
