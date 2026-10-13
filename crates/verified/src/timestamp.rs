//! Timestamp-scan selection and progress kernels.

use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{Int, Seq, invariant, logic};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The complete first-match guarantee shared by direct and indexed scans.
pub(crate) fn first_timestamp_match(
    timestamps: Seq<i64>,
    target: Int,
    matched: Option<usize>,
) -> bool {
    pearlite! {
        match matched {
            Some(index) => index@ < timestamps.len()
                && timestamps[index@]@ >= target
                && forall<i: Int> 0 <= i && i < index@ ==> timestamps[i]@ < target,
            None => forall<i: Int> 0 <= i && i < timestamps.len() ==> timestamps[i]@ < target,
        }
    }
}

/// Select the first timestamp at or after `target`.
#[ensures(first_timestamp_match(timestamps@, target@, result))]
#[must_use]
pub fn first_timestamp_index(timestamps: &[i64], target: i64) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(index@ <= timestamps@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==> timestamps@[i]@ < target@)]
    #[variant(timestamps@.len() - index@)]
    while index < timestamps.len() {
        if timestamps[index] >= target {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Select the earliest occurrence of the maximum timestamp.
#[ensures(match result {
    Some(index) => index@ < timestamps@.len()
        && forall<i: Int> 0 <= i && i < timestamps@.len() ==>
            timestamps@[i]@ <= timestamps@[index@]@
        && forall<i: Int> 0 <= i && i < index@ ==>
            timestamps@[i]@ < timestamps@[index@]@,
    None => timestamps@.len() == 0,
})]
#[must_use]
pub fn earliest_max_timestamp_index(timestamps: &[i64]) -> Option<usize> {
    if matches!(timestamps.len(), 0) {
        return None;
    }
    let mut best = 0usize;
    let mut index = 1usize;
    #[invariant(best@ < index@)]
    #[invariant(index@ <= timestamps@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@ ==>
        timestamps@[i]@ <= timestamps@[best@]@)]
    #[invariant(forall<i: Int> 0 <= i && i < best@ ==>
        timestamps@[i]@ < timestamps@[best@]@)]
    #[variant(timestamps@.len() - index@)]
    while index < timestamps.len() {
        if timestamps[index] > timestamps[best] {
            best = index;
        }
        index += 1;
    }
    Some(best)
}

/// Compute one record's absolute offset and timestamp without signed overflow.
#[ensures(match result {
    Some((offset, timestamp)) => offset@ == batch_base@ + offset_delta@
        && timestamp@ == batch_timestamp@ + timestamp_delta@,
    None => batch_base@ + offset_delta@ < i64::MIN@
        || batch_base@ + offset_delta@ > i64::MAX@
        || batch_timestamp@ + timestamp_delta@ < i64::MIN@
        || batch_timestamp@ + timestamp_delta@ > i64::MAX@,
})]
#[must_use]
pub fn timestamp_record_coordinates(
    batch_base: i64,
    offset_delta: i32,
    batch_timestamp: i64,
    timestamp_delta: i64,
) -> Option<(i64, i64)> {
    Some((
        batch_base.checked_add(i64::from(offset_delta))?,
        batch_timestamp.checked_add(timestamp_delta)?,
    ))
}

/// Advance strictly past a decoded batch, rejecting malformed or stale bounds.
#[ensures(match result {
    Some(next) => next@ > cursor@
        && next@ == batch_base@ + last_offset_delta@ + 1,
    None => last_offset_delta@ < 0
        || batch_base@ + last_offset_delta@ + 1 > i64::MAX@
        || batch_base@ + last_offset_delta@ < cursor@,
})]
#[must_use]
pub fn timestamp_scan_next(cursor: i64, batch_base: i64, last_offset_delta: i32) -> Option<i64> {
    if last_offset_delta < 0 {
        return None;
    }
    let last = batch_base.checked_add(i64::from(last_offset_delta))?;
    if last < cursor {
        return None;
    }
    last.checked_add(1)
}

/// Double a nonzero scan window with checked arithmetic.
#[ensures(match result {
    Some(next) => next@ == window@ * 2 && next@ > window@,
    None => window@ == 0 || window@ * 2 > u32::MAX@,
})]
#[must_use]
pub fn timestamp_scan_window(window: u32) -> Option<u32> {
    if window == 0 {
        return None;
    }
    window.checked_mul(2)
}

/// First matching decoded record at or above the logical retention floor.
/// A sparse byte read may start inside a batch that still contains discarded
/// records; those records cannot satisfy the lookup.
#[ensures(match result {
    Some(index) => index@ < records@.len()
        && records@[index@].0@ >= minimum@ && records@[index@].1@ >= target@
        && forall<i: Int> 0 <= i && i < index@
            ==> records@[i].0@ < minimum@ || records@[i].1@ < target@,
    None => forall<i: Int> 0 <= i && i < records@.len()
        ==> records@[i].0@ < minimum@ || records@[i].1@ < target@,
})]
#[must_use]
pub fn first_timestamp_record_index(
    records: &[(i64, i64)],
    minimum: i64,
    target: i64,
) -> Option<usize> {
    let mut index = 0usize;
    #[invariant(index@ <= records@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < index@
        ==> records@[i].0@ < minimum@ || records@[i].1@ < target@)]
    #[variant(records@.len() - index@)]
    while index < records.len() {
        if records[index].0 >= minimum && records[index].1 >= target {
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Decode record time according to the batch timestamp type. Append time
/// replaces producer fields entirely, including producer arithmetic overflow.
#[ensures(match result {
    Some(timestamp) => timestamp@ == match append_time {
        Some(stamp) => stamp@,
        None => batch_timestamp@ + timestamp_delta@,
    },
    None => append_time == None && (batch_timestamp@ + timestamp_delta@ < i64::MIN@
        || batch_timestamp@ + timestamp_delta@ > i64::MAX@),
})]
#[must_use]
pub fn timestamp_record_time(
    batch_timestamp: i64,
    timestamp_delta: i64,
    append_time: Option<i64>,
) -> Option<i64> {
    match append_time {
        Some(stamp) => Some(stamp),
        None => batch_timestamp.checked_add(timestamp_delta),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        earliest_max_timestamp_index, first_timestamp_index, timestamp_record_coordinates,
        timestamp_scan_next, timestamp_scan_window,
    };

    #[test]
    fn selection_progress_and_overflow_are_fail_closed() {
        let timestamps = [5, 9, 9, 3];
        assert2::assert!(first_timestamp_index(&timestamps, 9) == Some(1));
        assert2::assert!(first_timestamp_index(&timestamps, 10).is_none());
        assert2::assert!(earliest_max_timestamp_index(&timestamps) == Some(1));
        assert2::assert!(earliest_max_timestamp_index(&[]).is_none());
        assert2::assert!(timestamp_record_coordinates(10, 2, 100, 3) == Some((12, 103)));
        assert2::assert!(timestamp_record_coordinates(i64::MAX, 1, 0, 0).is_none());
        assert2::assert!(timestamp_record_coordinates(0, 0, i64::MAX, 1).is_none());
        assert2::assert!(timestamp_scan_next(10, 8, 2) == Some(11));
        assert2::assert!(timestamp_scan_next(10, 10, 0) == Some(11));
        assert2::assert!(timestamp_scan_next(10, 8, 1).is_none());
        assert2::assert!(timestamp_scan_next(i64::MAX, i64::MAX, 0).is_none());
        assert2::assert!(timestamp_scan_window(1) == Some(2));
        assert2::assert!(timestamp_scan_window(u32::MAX).is_none());
    }
}
