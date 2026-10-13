#[cfg(creusot)]
use super::restore_selection::selection_ordered;
use {
    super::{
        FetchWatermarks, RestoreAbortedTxn, RestoreSegmentExtent, aborted_transaction_interval,
        aborted_transaction_overlaps, fetch_visibility, restore_txn_index_entry_valid,
        truncation_frontier,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn restored_abort_index_valid(
    entries: Seq<RestoreAbortedTxn>,
    owner: RestoreSegmentExtent,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < entries.len()
    ==> entries[i].producer_id@ >= 0 && owner.base_offset@ >= 0
        && 0 <= entries[i].start_offset@ && entries[i].start_offset@ <= entries[i].last_offset@
        && owner.base_offset@ <= entries[i].last_offset@ && entries[i].last_offset@ <= owner.last_offset@
        && (i > 0 ==> entries[i - 1].last_offset@ < entries[i].last_offset@) }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub fn abort_intersects_fetch(entry: RestoreAbortedTxn, from: Int, end: Int) -> bool {
    pearlite! { from < end && entry.start_offset@ < end && entry.last_offset@ >= from }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every required source interval has its producer/start pair in the wire rows.
pub(super) fn wire_aborts_cover_source(
    source: Seq<RestoreAbortedTxn>,
    rows: Seq<(i64, i64)>,
    from: Int,
    end: Int,
) -> bool {
    pearlite! {
        forall<i: Int> 0 <= i && i < source.len()
            && abort_intersects_fetch(source[i], from, end)
            ==> exists<j: Int> 0 <= j && j < rows.len()
                && rows[j] == (source[i].producer_id, source[i].start_offset)
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A returned producer/start pair comes from an interval intersecting Fetch.
pub(super) fn wire_abort_from_source(
    source: Seq<RestoreAbortedTxn>,
    row: (i64, i64),
    from: Int,
    end: Int,
) -> bool {
    pearlite! {
        exists<j: Int> 0 <= j && j < source.len()
            && row == (source[j].producer_id, source[j].start_offset)
            && abort_intersects_fetch(source[j], from, end)
    }
}

/// Admit the complete index and return every selected original row index.
/// Narrowing consumer visibility loses no still-overlapping abort and never
/// introduces one. Transactions may start before their marker's segment.
#[ensures(match result {
    None => !restored_abort_index_valid(entries@, segment),
    Some(selected) => restored_abort_index_valid(entries@, segment)
        && selected@.len() <= entries@.len()
        && (forall<i: Int> 0 <= i && i < selected@.len() ==> selected@[i]@ < entries@.len())
        && (crate::sequence::strictly_increasing(selected@))
        && (forall<i: Int> 0 <= i && i < entries@.len()
            ==> (crate::sequence::contains_source_index(selected@, i))
                == abort_intersects_fetch(entries@[i], fetch_start@,
                    w.hw@.min(w.lso@).min(w.deliverable@).min(cut@)))
        && (forall<i: Int> 0 <= i && i < selected@.len() ==>
            abort_intersects_fetch(entries@[selected@[i]@], fetch_start@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@))
            && abort_intersects_fetch(entries@[selected@[i]@], fetch_start@, w.hw@.min(w.lso@).min(w.deliverable@))
            && entries@[selected@[i]@].start_offset@ < w.hw@
            && entries@[selected@[i]@].start_offset@ < w.lso@
            && entries@[selected@[i]@].start_offset@ < w.deliverable@),
})]
pub(super) fn restored_aborts_remain_bounded_when_fetch_shrinks(
    entries: &[RestoreAbortedTxn],
    segment: RestoreSegmentExtent,
    w: FetchWatermarks,
    fetch_start: i64,
    cut: i64,
) -> Option<Vec<usize>> {
    let visibility = fetch_visibility(false, true, w, fetch_start);
    let narrowed = truncation_frontier(visibility.limit_offset, cut);
    let mut selected: Vec<usize> = Vec::new();
    let mut i = 0usize;
    let mut previous_last = None;
    #[invariant(i@ <= entries@.len() && selected@.len() <= i@)]
    #[invariant(previous_last == if i@ == 0 { None } else { Some(entries@[i@ - 1].last_offset) })]
    #[invariant(restored_abort_index_valid(entries@.subsequence(0, i@), segment))]
    #[invariant(selection_ordered(selected@, i@))]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> (crate::sequence::contains_source_index(selected@, j))
            == abort_intersects_fetch(entries@[j], fetch_start@, narrowed@))]
    #[variant(entries@.len() - i@)]
    while i < entries.len() {
        let entry = entries[i];
        if !restore_txn_index_entry_valid(previous_last, entry, segment) {
            return None;
        }
        let (start, last) = aborted_transaction_interval(
            Some(entry.start_offset),
            entry.last_offset,
            entry.producer_id,
        )
        .expect("admitted interval");
        if aborted_transaction_overlaps(start, last, fetch_start, narrowed) {
            selected.push(i);
        }
        previous_last = Some(entry.last_offset);
        i += 1;
    }
    // Close the admitted prefix explicitly before exporting whole-index validity.
    proof_assert!(i@ == entries@.len());
    proof_assert!(entries@.subsequence(0, i@) == entries@);
    Some(selected)
}
