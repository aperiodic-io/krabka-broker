#[cfg(creusot)]
use crate::timestamp::first_timestamp_match;
use {
    super::{
        TimeEntry, earliest_max_timestamp_index, first_timestamp_index,
        remote_time_index_candidate_count, restore_time_index_entry_valid, time_index_scan_start,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Record offsets remain strictly increasing even when their timestamps do not.
pub(super) fn record_offsets_ordered(offsets: Seq<u32>) -> bool {
    pearlite! {
        crate::sequence::strictly_increasing(offsets)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Validated archive cursors agree on the preceding indexed offset.
pub(super) fn time_cursor_matches(entries: Seq<TimeEntry>, cursor: (usize, u32, u32)) -> bool {
    pearlite! {
        cursor.1 == cursor.2 && cursor.0@ <= entries.len()
            && cursor.1 == if cursor.0@ == 0 { 0u32 } else { entries[cursor.0@ - 1].coordinate }
    }
}

/// Construct a sparse row from the actual prefix maximum. The indexed record
/// can be a batch base while `through` includes the batch's remaining records.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(indexed@ <= through@ && through@ < timestamps@.len())]
#[requires(record_offsets_ordered(offsets@))]
#[ensures(result.coordinate == offsets@[indexed@])]
#[ensures(exists<i: Int> 0 <= i && i <= through@ && timestamps@[i] == result.key)]
#[ensures(forall<i: Int> 0 <= i && i <= through@ ==> timestamps@[i]@ <= result.key@)]
#[ensures(forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ <= result.coordinate@
    ==> timestamps@[i]@ <= result.key@)]
pub(super) fn running_maximum_index_entry(
    offsets: &[u32],
    timestamps: &[i64],
    indexed: usize,
    through: usize,
) -> TimeEntry {
    // Expose the offset-order law before relating its selected row to the prefix.
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < offsets@.len()
        ==> offsets@[i]@ < offsets@[j]@);
    let prefix = &timestamps[..=through];
    match earliest_max_timestamp_index(prefix) {
        Some(index) => TimeEntry::new(prefix[index], offsets[indexed]),
        None => unreachable!(),
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn sparse_maxima_bound_prefix(
    entries: Seq<TimeEntry>,
    offsets: Seq<u32>,
    timestamps: Seq<i64>,
) -> bool {
    pearlite! {
        (forall<i: Int, j: Int> 0 <= i && i < j && j < entries.len() ==> entries[i].key@ <= entries[j].key@)
        && (sparse_rows_bound_records(entries, offsets, timestamps))
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
    entries: &[TimeEntry],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> Option<usize> {
    // Give the sparse scan and its prefix witness the separate quantified laws.
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
        ==> entries@[i].key@ <= entries@[j].key@);
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
        && 0 <= j && j < offsets@.len() && offsets@[j]@ < entries@[i].coordinate@
        ==> timestamps@[j]@ <= entries@[i].key@);
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
#[ensures(forall<i: Int> 0 <= i && i < result.0@ ==> entries@[i].key@ < target@)]
#[ensures(forall<i: Int> 1 <= i && i < result.0@ ==> entries@[i - 1].coordinate@ < entries@[i].coordinate@)]
#[ensures(result.0@ < entries@.len() ==> entries@[result.0@].key@ >= target@
    || (result.0@ > 0 && entries@[result.0@].coordinate@ <= entries@[result.0@ - 1].coordinate@))]
#[ensures(result.1 == if result.0@ == 0 { 0u32 } else { entries@[result.0@ - 1].coordinate })]
#[ensures(forall<i: Int> 0 <= i && i < offsets@.len() && offsets@[i]@ < result.1@
    ==> timestamps@[i]@ < target@)]
#[ensures(first_timestamp_match(timestamps@, target@, result.2))]
pub(super) fn remote_timestamp_scan_preserves_first(
    entries: &[TimeEntry],
    offsets: &[u32],
    timestamps: &[i64],
    target: i64,
) -> (usize, u32, Option<usize>) {
    let count = remote_time_index_candidate_count(entries, target);
    let (relative, selected) = if count == 0 {
        (0, &entries[..0])
    } else {
        (entries[count - 1].coordinate, &entries[count - 1..count])
    };
    // A selected remote row is strictly below the target, so the one-row
    // strict search starts at exactly the floor the remote adapter returns.
    let _local_floor = time_index_scan_start(selected, target);
    proof_assert!(relative == _local_floor);
    let matched = indexed_timestamp_scan_finds_first(selected, offsets, timestamps, target);
    (count, relative, matched)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn time_archive_valid(entries: Seq<TimeEntry>, max_relative: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < entries.len() ==> entries[i].coordinate@ <= max_relative)
        && (time_rows_ordered(entries, entries.len()))
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
    entries: &[TimeEntry],
    max_relative: i64,
    target: i64,
) -> Result<(usize, u32, u32), ()>;
    entries, i, previous, timestamp, relative;
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].coordinate@ <= max_relative@)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@
        ==> entries@[j].key@ <= entries@[k].key@ && entries@[j].coordinate@ < entries@[k].coordinate@)]
    ;
        restore_time_index_entry_valid(previous, timestamp, relative, max_relative);
    let count = remote_time_index_candidate_count(entries, target);
    let remote = if count == 0 { 0 } else { entries[count - 1].coordinate };
    let local = time_index_scan_start(entries, target);
    Ok((count, remote, local))
}

/// A constructed index and the first matching record selected from it.
#[krabka_macros::model_type(proof(DeepModel), runtime(Debug, PartialEq, Eq))]
pub struct ConstructedTimeIndex {
    pub entries: Vec<TimeEntry>,
    pub selected: Option<usize>,
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
#[ensures(result.entries@.len() == rows@.len())]
#[ensures(forall<i: Int> 0 <= i && i < rows@.len()
    ==> result.entries@[i].coordinate == offsets@[rows@[i].0@]
        && (exists<j: Int> 0 <= j && j <= rows@[i].1@ && result.entries@[i].key == timestamps@[j])
        && (forall<j: Int> 0 <= j && j <= rows@[i].1@ ==> timestamps@[j]@ <= result.entries@[i].key@))]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result.entries@.len()
    ==> result.entries@[i].key@ <= result.entries@[j].key@ && result.entries@[i].coordinate@ < result.entries@[j].coordinate@)]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < result.entries@.len()
    && 0 <= j && j < offsets@.len() && offsets@[j]@ <= result.entries@[i].coordinate@
    ==> timestamps@[j]@ <= result.entries@[i].key@)]
#[ensures(first_timestamp_match(timestamps@, target@, result.selected))]
pub(super) fn constructed_time_index_preserves_first(
    offsets: &[u32],
    timestamps: &[i64],
    rows: &[(usize, usize)],
    target: i64,
) -> ConstructedTimeIndex {
    let mut entries: Vec<TimeEntry> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len() && entries@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> entries@[j].coordinate == offsets@[rows@[j].0@])]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        exists<k: Int> 0 <= k && k <= rows@[j].1@ && timestamps@[k] == entries@[j].key)]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k <= rows@[j].1@
        ==> timestamps@[k]@ <= entries@[j].key@)]
    #[invariant(time_rows_ordered(entries@, i@))]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < i@ && 0 <= k && k < offsets@.len()
        && offsets@[k]@ <= entries@[j].coordinate@ ==> timestamps@[k]@ <= entries@[j].key@)]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let (indexed, through) = rows[i];
        entries.push(running_maximum_index_entry(
            offsets, timestamps, indexed, through,
        ));
        i += 1;
    }
    let selected = indexed_timestamp_scan_finds_first(&entries, offsets, timestamps, target);
    ConstructedTimeIndex { entries, selected }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The prefix preserves timestamp order and distinct increasing record offsets.
pub fn time_rows_ordered(entries: Seq<TimeEntry>, count: Int) -> bool {
    pearlite! { forall<left: Int, right: Int> 0 <= left && left < right && right < count
    ==> entries[left].key@ <= entries[right].key@ && entries[left].coordinate@ < entries[right].coordinate@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// The archive prefix contains timestamps below the target and stops at the first eligible row.
pub(super) fn timestamp_archive_prefix(entries: Seq<TimeEntry>, target: Int, count: Int) -> bool {
    pearlite! { (forall<i: Int> 0 <= i && i < count ==> entries[i].key@ < target)
    && (count < entries.len() ==> entries[count].key@ >= target) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every sparse maximum bounds timestamps of decoded records before its relative offset.
pub fn sparse_rows_bound_records(
    entries: Seq<TimeEntry>,
    offsets: Seq<u32>,
    times: Seq<i64>,
) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < entries.len() && 0 <= j && j < offsets.len()
    && offsets[j]@ < entries[i].coordinate@ ==> times[j]@ <= entries[i].key@ }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A timestamp lies outside the requested closed interval.
pub fn outside_timestamp_interval(time: Int, targets: (i64, i64)) -> bool {
    pearlite! { time < targets.0@ || time > targets.1@ }
}
