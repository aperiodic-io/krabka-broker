use {
    super::{
        ConstructedTimeIndex, SparseTimestampWindow, constructed_time_index_preserves_first,
        indexed_timestamp_scan_finds_first,
    },
    creusot_std::prelude::*,
};

/// Consume the actual constructed index to scan at/after the logical floor.
/// Preserve original record indices and exclude every earlier retained match,
/// even when a pruned record matches first or timestamps regress later.
/// Complete faithful decoding and coherent retained floors remain external.
#[krabka_macros::retained_timestamp_bounds(minimum)]
#[requires(forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ <= i64::MAX@)]
#[ensures(match result {
    None => forall<i: Int> 0 <= i && i < window.0@.len()
        ==> base@ + window.0@[i]@ < minimum@ || window.1@[i]@ < target@,
    Some(index) => index@ < window.0@.len() && window.1@[index@]@ >= target@
        && base@ + window.0@[index@]@ >= minimum@
        && forall<i: Int> 0 <= i && i < index@
            ==> base@ + window.0@[i]@ < minimum@ || window.1@[i]@ < target@,
})]
pub(super) fn constructed_index_retained_candidate(
    window: SparseTimestampWindow<'_>,
    base: i64,
    minimum: i64,
    target: i64,
) -> Option<usize> {
    let (offsets, timestamps, rows) = window;
    let ConstructedTimeIndex {
        entries,
        selected: _untrimmed,
    } = constructed_time_index_preserves_first(offsets, timestamps, rows, target);
    let mut start = 0usize;
    #[invariant(start@ <= offsets@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < start@ ==> base@ + offsets@[i]@ < minimum@)]
    #[variant(offsets@.len() - start@)]
    while start < offsets.len() && base + i64::from(offsets[start]) < minimum {
        start += 1;
    }
    proof_assert!(forall<i: Int> start@ <= i && i < offsets@.len()
        ==> minimum@ <= base@ + offsets@[i]@);
    let retained_offsets = &offsets[start..];
    let retained_timestamps = &timestamps[start..];
    // Expose the suffix's original indexes before proving its sparse prefix bound.
    proof_assert!(forall<i: Int> 0 <= i && i < retained_offsets@.len()
        ==> retained_offsets@[i] == offsets@[start@ + i]
            && retained_timestamps@[i] == timestamps@[start@ + i]);
    proof_assert!(forall<i: Int, j: Int> 0 <= i && i < entries@.len()
        && 0 <= j && j < retained_offsets@.len() && retained_offsets@[j]@ < entries@[i].coordinate@
        ==> retained_timestamps@[j]@ <= entries@[i].key@);
    let selected = indexed_timestamp_scan_finds_first(
        &entries,
        retained_offsets,
        retained_timestamps,
        target,
    )?;
    Some(start + selected)
}
