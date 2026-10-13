#[cfg(creusot)]
use super::timestamp::time_archive_valid;
use {
    super::{
        restore_index_frontier, time_index_lookup, validated_remote_and_local_time_starts_agree,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Coherent decoded records and ordered sparse rows for timestamp scans.
pub fn sparse_timestamp_window_valid(
    offsets: Seq<u32>,
    timestamps: Seq<i64>,
    rows: Seq<(usize, usize)>,
) -> bool {
    pearlite! {
        offsets.len() == timestamps.len()
            && (crate::sequence::strictly_increasing(offsets))
            && (forall<i: Int> 0 <= i && i < rows.len() ==> rows[i].0@ <= rows[i].1@ && rows[i].1@ < timestamps.len())
            && (forall<i: Int, j: Int> 0 <= i && i < j && j < rows.len() ==> rows[i].0@ < rows[j].0@ && rows[i].1@ <= rows[j].1@)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn time_segment_valid(base: Int, end: Int) -> bool {
    pearlite! { 0 <= base && base <= end && end - base <= u32::MAX@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn time_cursor_matches(
    entries: Seq<(i64, u32)>,
    target: Int,
    relative: Int,
    strict: bool,
) -> bool {
    pearlite! {
        (relative == 0 && forall<i: Int> 0 <= i && i < entries.len()
            ==> if strict { entries[i].0@ >= target } else { entries[i].0@ > target })
        || exists<i: Int> 0 <= i && i < entries.len() && relative == entries[i].1@
            && (if strict { entries[i].0@ < target } else { entries[i].0@ <= target })
            && forall<j: Int> i < j && j < entries.len()
                ==> if strict { entries[j].0@ >= target } else { entries[j].0@ > target }
    }
}

/// Return actual absolute inclusive cursors and the safe strict lower start.
/// Reject exactly invalid segment extents or canonical rows. Equal timestamps
/// select the final inclusive row, but a >= record scan needs a strict predecessor.
/// Cursor monotonicity cannot justify ending a scan at the upper cursor when
/// decoded record timestamps may regress. Decoding and truthful bounds are external.
#[requires(lower_target@ <= upper_target@)]
#[ensures(match result {
    Err(()) => !time_segment_valid(segment_base@, segment_end@)
        || !time_archive_valid(entries@, segment_end@ - segment_base@),
    Ok((lower, upper, scan)) => time_segment_valid(segment_base@, segment_end@)
        && time_archive_valid(entries@, segment_end@ - segment_base@)
        && segment_base@ <= scan@ && scan@ <= lower@ && lower@ <= upper@ && upper@ <= segment_end@
        && time_cursor_matches(entries@, lower_target@, lower@ - segment_base@, false)
        && time_cursor_matches(entries@, upper_target@, upper@ - segment_base@, false)
        && time_cursor_matches(entries@, lower_target@, scan@ - segment_base@, true),
})]
pub(super) fn validated_time_cursors_are_monotone(
    entries: &[(i64, u32)],
    segment_base: i64,
    segment_end: i64,
    lower_target: i64,
    upper_target: i64,
) -> Result<(i64, i64, i64), ()> {
    let max_relative = restore_index_frontier(segment_base, segment_end).ok_or(())?;
    let (_count, _remote, scan) =
        validated_remote_and_local_time_starts_agree(entries, max_relative, lower_target)?;
    let lower = time_index_lookup(entries, lower_target);
    let upper = time_index_lookup(entries, upper_target);
    Ok((
        segment_base + i64::from(lower),
        segment_base + i64::from(upper),
        segment_base + i64::from(scan),
    ))
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every inspected relative record offset remains within the absolute upper bound.
pub(super) fn absolute_record_prefix_bounded(
    offsets: Seq<u32>,
    count: Int,
    base: Int,
    end: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < count ==> base + offsets[i]@ <= end }
}
