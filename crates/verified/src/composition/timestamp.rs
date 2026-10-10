use creusot_std::prelude::*;

use super::{
    earliest_max_timestamp_index, first_timestamp_index, remote_time_index_candidate_count,
    restore_time_index_entry_valid, time_index_scan_start,
};
#[cfg(creusot)]
use crate::timestamp::first_timestamp_match;

open_logic! {
/// Record offsets remain strictly increasing even when their timestamps do not.
pub(super) fn record_offsets_ordered(offsets: Seq<u32>) -> bool {
    pearlite! {
        crate::sequence::strictly_increasing(offsets)
    }
}
}

open_logic! {
/// Validated archive cursors agree on the preceding indexed offset.
pub(super) fn time_cursor_matches(entries: Seq<(i64, u32)>, cursor: (usize, u32, u32)) -> bool {
    pearlite! {
        cursor.1 == cursor.2 && cursor.0@ <= entries.len()
            && cursor.1 == if cursor.0@ == 0 { 0u32 } else { entries[cursor.0@ - 1].1 }
    }
}
}

/// Construct a sparse row from the actual prefix maximum. The indexed record
/// can be a batch base while `through` includes the batch's remaining records.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(indexed@ <= through@ && through@ < timestamps@.len())]
#[requires(record_offsets_ordered(offsets@))]
#[ensures(result.1 == offsets@[indexed@])]
#[ensures(exists<i: Int> 0 <= i && i <= through@ && timestamps@[i] == result.0)]
#[ensures(forall<i: Int> 0 <= i && i <= through@ ==> timestamps@[i]@ <= result.0@)]
#[ensures(forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ <= result.1@
    ==> timestamps@[i]@ <= result.0@)]
pub(super) fn running_maximum_index_entry(
    offsets: &[u32],
    timestamps: &[i64],
    indexed: usize,
    through: usize,
) -> (i64, u32) {
    // Expose the offset-order law before relating its selected row to the prefix.
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
        ==> offsets@[i]@ < offsets@[j]@);
    let prefix = &timestamps[..=through];
    match earliest_max_timestamp_index(prefix) {
        Some(index) => (prefix[index], offsets[indexed]),
        None => unreachable!(),
    }
}

open_logic! {
pub(super) fn sparse_maxima_bound_prefix(
    entries: Seq<(i64, u32)>,
    offsets: Seq<u32>,
    timestamps: Seq<i64>,
) -> bool {
    pearlite! {
        (forall<i: Int, j: Int> 0 <= i && i < j && j < entries.len() ==> entries[i].0@ <= entries[j].0@)
        && (sparse_rows_bound_records(entries, offsets, timestamps))
    }
}
}

/// A strict-predecessor sparse start followed by the existing record selector
/// finds the global first match, even with nonmonotone timestamps and offset
/// gaps. Each sparse timestamp must bound all records before its offset.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(record_offsets_ordered(offsets@))]
#[requires(sparse_maxima_bound_prefix(entries@, offsets@, timestamps@))]
#[ensures(first_timestamp_match(timestamps@, target@, result))]
pub(super) fn indexed_timestamp_scan_finds_first(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> Option<usize> {
    // Give the sparse scan and its prefix witness the separate quantified laws.
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
        ==> entries@[i].0@ <= entries@[j].0@);
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
        && 0 <= j && j < offsets@.len() && offsets@[j]@ < entries@[i].1@
        ==> timestamps@[j]@ <= entries@[i].0@);
    let relative = time_index_scan_start(entries, target);
    let mut start = 0usize;
    #[invariant(start@ <= offsets@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < start@ ==> timestamps@[i]@ < target@)]
    #[variant(offsets@.len() - start@)]
    while start < offsets.len() && offsets[start] < relative {
        proof_assert!(timestamps@[start@]@ < target@);
        start += 1;
    }
    let suffix = &timestamps[start..];
    // Translate suffix indexes back to the original record window.
    proof_assert!(forall<i: Int> start@ <= i && i < timestamps@.len()
        ==> suffix@[i - start@] == timestamps@[i]);
    let index = first_timestamp_index(suffix, target)?;
    Some(start + index)
}

/// The remote prefix selector and floor adapter preserve the global first
/// record match. Unlike a binary search, this accepts unsorted index timestamps
/// and offset padding. Index rows must bound earlier record timestamps; a
/// zero-offset padding row has no earlier record to bound. Return the actual
/// counted prefix, scan floor and globally first matching record index.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(record_offsets_ordered(offsets@))]
#[requires(sparse_rows_bound_records(entries@, offsets@, timestamps@))]
#[ensures(result.0@ <= entries@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.0@ ==> entries@[i].0@ < target@)]
#[ensures(forall<i: Int> 1 <= i && i < result.0@ ==> entries@[i - 1].1@ < entries@[i].1@)]
#[ensures(result.0@ < entries@.len() ==> entries@[result.0@].0@ >= target@
    || (result.0@ > 0 && entries@[result.0@].1@ <= entries@[result.0@ - 1].1@))]
#[ensures(result.1 == if result.0@ == 0 { 0u32 } else { entries@[result.0@ - 1].1 })]
#[ensures(forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ < result.1@
    ==> timestamps@[i]@ < target@)]
#[ensures(first_timestamp_match(timestamps@, target@, result.2))]
pub(super) fn remote_timestamp_scan_preserves_first(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> (usize, u32, Option<usize>) {
    let count = remote_time_index_candidate_count(entries, target);
    let (relative, selected) = if count == 0 {
        (0, &entries[..0])
    } else {
        (entries[count - 1].1, &entries[count - 1..count])
    };
    // A selected remote row is strictly below the target, so the one-row
    // strict search starts at exactly the floor the remote adapter returns.
    let _local_floor = time_index_scan_start(selected, target);
    proof_assert!(relative == _local_floor);
    let matched = indexed_timestamp_scan_finds_first(selected, offsets, timestamps, target);
    (count, relative, matched)
}

open_logic! {
pub fn time_archive_valid(entries: Seq<(i64, u32)>, max_relative: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < entries.len() ==> entries[i].1@ <= max_relative)
        && (time_rows_ordered(entries, entries.len()))
    }
}
}

krabka_macros::validate_archive_rows! {
/// Archive row validation makes the remote prefix floor and local binary
/// scan start agree. Raw trailing padding must be excluded before validation;
/// the separate remote scan theorem admits its zero-offset rows directly.
/// Reject exactly invalid rows; successful validation returns actual cursors.
#[ensures(|result| match result {
    Err(()) => !time_archive_valid(entries@, max_relative@),
    Ok((count, remote, local)) => time_archive_valid(entries@, max_relative@)
        && time_cursor_matches(entries@, (count, remote, local))
        && (entries@.len() == 0 || remote@ <= max_relative@)
        && timestamp_archive_prefix(entries@, target@, count@),
})]
pub(super) fn validated_remote_and_local_time_starts_agree(
    entries: &[(i64, u32)],
    max_relative: i64,
    target: i64,
) -> Result<(usize, u32, u32), ()>;
    entries, i, previous, timestamp, relative;
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].0@ <= entries@[k].0@ && entries@[j].1@ < entries@[k].1@)]
    ;
        restore_time_index_entry_valid(previous, timestamp, relative, max_relative);
    let count = remote_time_index_candidate_count(entries, target);
    let remote = if count == 0 { 0 } else { entries[count - 1].1 };
    let local = time_index_scan_start(entries, target);
    Ok((count, remote, local))
}

/// Build an arbitrary sparse index from real prefix maxima, then use it to
/// search. Return the actual rows with exact prefix maxima and the globally
/// first matching record. No supplied upper-bound boolean or assumed timestamp
/// ordering of records is needed. Decoding and complete enumeration are external.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(record_offsets_ordered(offsets@))]
#[requires(forall<i: Int> 0 <= i && i < rows@.len()
    ==> rows@[i].0@ <= rows@[i].1@ && rows@[i].1@ < timestamps@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < rows@.len()
    ==> rows@[i].0@ < rows@[j].0@ && rows@[i].1@ <= rows@[j].1@)]
#[ensures(result.0@.len() == rows@.len())]
#[ensures(forall<i: Int> 0 <= i && i < rows@.len()
    ==> result.0@[i].1 == offsets@[rows@[i].0@]
        && (exists<j: Int> 0 <= j && j <= rows@[i].1@ && result.0@[i].0 == timestamps@[j])
        && (forall<j: Int> 0 <= j && j <= rows@[i].1@ ==> timestamps@[j]@ <= result.0@[i].0@))]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result.0@.len()
    ==> result.0@[i].0@ <= result.0@[j].0@ && result.0@[i].1@ < result.0@[j].1@)]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < result.0@.len()
    && 0 <= j && j < offsets@.len() && offsets@[j]@ <= result.0@[i].1@
    ==> timestamps@[j]@ <= result.0@[i].0@)]
#[ensures(first_timestamp_match(timestamps@, target@, result.1))]
pub(super) fn constructed_time_index_preserves_first(
    offsets: &[u32],
    timestamps: &[i64],
    rows: &[(usize, usize)],
    target: i64,
) -> (Vec<(i64, u32)>, Option<usize>) {
    let mut entries: Vec<(i64, u32)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len() && entries@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].1 == offsets@[rows@[j].0@])]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        exists<k: Int> 0 <= k && k <= rows@[j].1@ && timestamps@[k] == entries@[j].0)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k <= rows@[j].1@
        ==> timestamps@[k]@ <= entries@[j].0@)]
    #[invariant(time_rows_ordered(entries@, i@))]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k < offsets@.len()
        && offsets@[k]@ <= entries@[j].1@ ==> timestamps@[k]@ <= entries@[j].0@)]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let (indexed, through) = rows[i];
        entries.push(running_maximum_index_entry(
            offsets, timestamps, indexed, through,
        ));
        i += 1;
    }
    let selected = indexed_timestamp_scan_finds_first(&entries, offsets, timestamps, target);
    (entries, selected)
}

open_logic! {
/// The prefix preserves timestamp order and distinct increasing record offsets.
pub fn time_rows_ordered(entries: Seq<(i64, u32)>, count: Int) -> bool {
    pearlite! { forall<left: Int, right: Int> 0 <= left && left < right && right < count
    ==> entries[left].0@ <= entries[right].0@ && entries[left].1@ < entries[right].1@ }
}
}

open_logic! {
/// The archive prefix contains timestamps below the target and stops at the first eligible row.
pub(super) fn timestamp_archive_prefix(entries: Seq<(i64, u32)>, target: Int, count: Int) -> bool {
    pearlite! { (forall<i: Int> 0 <= i && i < count ==> entries[i].0@ < target)
    && (count < entries.len() ==> entries[count].0@ >= target) }
}
}

open_logic! {
/// Every sparse maximum bounds timestamps of decoded records before its relative offset.
pub fn sparse_rows_bound_records(
    entries: Seq<(i64, u32)>,
    offsets: Seq<u32>,
    times: Seq<i64>,
) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < entries.len() && 0 <= j && j < offsets.len()
    && offsets[j]@ < entries[i].1@ ==> times[j]@ <= entries[i].0@ }
}
}

open_logic! {
/// A timestamp lies outside the requested closed interval.
pub fn outside_timestamp_interval(time: Int, targets: (i64, i64)) -> bool {
    pearlite! { time < targets.0@ || time > targets.1@ }
}
}
