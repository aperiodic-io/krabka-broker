//! Inclusive interval integrity for remote aborted-transaction indexes.

use creusot_std::prelude::*;

/// Whether an aborted-transaction entry intersects a requested offset range.
#[krabka_macros::model_type]
pub enum RemoteTxnOverlapDecision {
    /// At least one interval is inverted.
    Invalid,
    /// Both intervals are valid and do not intersect.
    Disjoint,
    /// Both intervals are valid and intersect inclusively.
    Overlap,
}

/// Classify the inclusive entry interval `[entry_start, entry_last]` against
/// the inclusive query interval `[query_from, query_to]`.
#[ensures(result == RemoteTxnOverlapDecision::Invalid
    == (entry_start@ > entry_last@ || query_from@ > query_to@))]
#[ensures(result == RemoteTxnOverlapDecision::Overlap
    == (entry_start@ <= entry_last@
        && query_from@ <= query_to@
        && entry_start@ <= query_to@
        && entry_last@ >= query_from@))]
#[ensures(result == RemoteTxnOverlapDecision::Disjoint
    == (entry_start@ <= entry_last@
        && query_from@ <= query_to@
        && (entry_start@ > query_to@ || entry_last@ < query_from@)))]
#[must_use]
pub fn remote_txn_overlap_decision(
    entry_start: i64,
    entry_last: i64,
    query_from: i64,
    query_to: i64,
) -> RemoteTxnOverlapDecision {
    if entry_start > entry_last || query_from > query_to {
        RemoteTxnOverlapDecision::Invalid
    } else if entry_start <= query_to && entry_last >= query_from {
        RemoteTxnOverlapDecision::Overlap
    } else {
        RemoteTxnOverlapDecision::Disjoint
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[yare::parameterized(
        before_range = { 10, 14, 0, 9, RemoteTxnOverlapDecision::Disjoint },
        overlap_start = { 10, 14, 0, 10, RemoteTxnOverlapDecision::Overlap },
        exact_single = { 10, 10, 10, 10, RemoteTxnOverlapDecision::Overlap },
        overlap_end = { 10, 14, 14, 100, RemoteTxnOverlapDecision::Overlap },
        after_range = { 10, 14, 15, 100, RemoteTxnOverlapDecision::Disjoint },
        inverted_entry = { 14, 10, 0, 100, RemoteTxnOverlapDecision::Invalid },
        inverted_query = { 10, 14, 100, 0, RemoteTxnOverlapDecision::Invalid },
    )]
    fn overlap_is_inclusive_and_invalid_intervals_fail_closed(
        entry_start: i64,
        entry_last: i64,
        query_from: i64,
        query_to: i64,
        expected: RemoteTxnOverlapDecision,
    ) {
        check!(
            remote_txn_overlap_decision(entry_start, entry_last, query_from, query_to) == expected
        );
    }
}
