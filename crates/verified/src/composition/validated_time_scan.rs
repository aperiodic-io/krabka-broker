use creusot_std::prelude::*;

#[cfg(creusot)]
use super::timestamp::{time_archive_valid, time_cursor_matches};
use super::{
    indexed_timestamp_scan_finds_first, remote_timestamp_scan_preserves_first,
    validated_remote_and_local_time_starts_agree,
};

type TimeScanWitness = (usize, u32, u32, Option<usize>);

open_logic! {
fn time_scan_input_valid(
    entries: Seq<(i64, u32)>,
    offsets: Seq<u32>,
    times: Seq<i64>,
    maximum: Int,
) -> bool {
    pearlite! {
        time_archive_valid(entries, maximum) && offsets.len() == times.len()
        && (forall<i: Int> 0 <= i && i < offsets.len() ==> offsets[i]@ <= maximum)
        && (crate::sequence::strictly_increasing(offsets))
        && (super::timestamp::sparse_rows_bound_records(entries, offsets, times))
    }
}
}

/// Check canonical archived rows against complete decoded records, then consume
/// the actual remote floor and local cursor to scan the retained suffix.
/// Invalid shape or untruthful prefix bounds are rejected exactly; accepted
/// inputs return the original globally first retained match, or complete absence.
/// Faithful decoding, complete enumeration and physical reads remain external.
#[ensures(match result {
    Err(()) => !time_scan_input_valid(entries@, offsets@, times@, max_relative@),
    Ok((count, remote, local, selected)) => time_scan_input_valid(entries@, offsets@, times@, max_relative@)
        && time_cursor_matches(entries@, (count, remote, local))
        && super::timestamp::timestamp_archive_prefix(entries@, target@, count@)
        && (forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ < remote@ ==> times@[i]@ < target@)
        && match selected {
            None => forall<i: Int> 0 <= i && i < offsets@.len() ==> offsets@[i]@ < minimum@ || times@[i]@ < target@,
            Some(index) => index@ < offsets@.len() && offsets@[index@]@ >= minimum@ && times@[index@]@ >= target@
                && remote@ <= offsets@[index@]@ && offsets@[index@]@ <= max_relative@
                && forall<i: Int> 0 <= i && i < index@ ==> offsets@[i]@ < minimum@ || times@[i]@ < target@,
        },
})]
pub(super) fn validated_retained_time_scan_agrees(
    entries: &[(i64, u32)],
    offsets: &[u32],
    times: &[i64],
    max_relative: i64,
    minimum: u32,
    target: i64,
) -> Result<TimeScanWitness, ()> {
    let (_count, _remote, local) =
        validated_remote_and_local_time_starts_agree(entries, max_relative, target)?;
    if offsets.len() != times.len() {
        return Err(());
    }
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> offsets@[j]@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@ ==> offsets@[j]@ < offsets@[k]@)]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        if i64::from(offsets[i]) > max_relative || (i > 0 && offsets[i - 1] >= offsets[i]) {
            return Err(());
        }
        i += 1;
    }
    i = 0;
    #[invariant(i@ <= entries@.len())]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k < offsets@.len()
        && offsets@[k]@ < entries@[j].1@ ==> times@[k]@ <= entries@[j].0@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let mut j = 0usize;
        #[invariant(j@ <= offsets@.len())]
        #[invariant(forall<k: Int> 0 <= k && k < j@ && offsets@[k]@ < entries@[i@].1@ ==> times@[k]@ <= entries@[i@].0@)]
        #[variant(offsets@.len() - j@)]
        while j < offsets.len() {
            if offsets[j] < entries[i].1 && times[j] > entries[i].0 {
                return Err(());
            }
            j += 1;
        }
        i += 1;
    }
    let mut start = 0usize;
    #[invariant(start@ <= offsets@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < start@ ==> offsets@[i]@ < minimum@)]
    #[variant(offsets@.len() - start@)]
    while start < offsets.len() && offsets[start] < minimum {
        start += 1;
    }
    proof_assert!(forall<i: Int> start@ <= i && i < offsets@.len() ==> offsets@[i]@ >= minimum@);
    let retained_offsets = &offsets[start..];
    let retained_times = &times[start..];
    proof_assert!(forall<i: Int> start@ <= i && i < times@.len()
        ==> times@[i] == retained_times@[i - start@]);
    let (remote_count, remote_floor, selected) =
        remote_timestamp_scan_preserves_first(entries, retained_offsets, retained_times, target);
    let _local_selected =
        indexed_timestamp_scan_finds_first(entries, retained_offsets, retained_times, target);
    proof_assert!(remote_count == _count && remote_floor == local && selected == _local_selected);
    match selected {
        None => {
            proof_assert!(forall<i: Int> start@ <= i && i < times@.len() ==> times@[i]@ < target@);
            Ok((remote_count, remote_floor, local, None))
        }
        Some(index) => {
            proof_assert!(forall<i: Int> start@ <= i && i < start@ + index@ ==> times@[i]@ < target@);
            Ok((remote_count, remote_floor, local, Some(start + index)))
        }
    }
}
