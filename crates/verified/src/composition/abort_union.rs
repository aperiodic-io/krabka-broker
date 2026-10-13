#[cfg(creusot)]
use super::restored_aborts::{
    abort_intersects_fetch, restored_abort_index_valid, wire_abort_from_source,
    wire_aborts_cover_source,
};
use {
    super::{
        FetchWatermarks, RestoreAbortedTxn, RestoreSegmentExtent, fetch_visibility,
        restored_aborts_remain_bounded_when_fetch_shrinks, truncation_frontier,
    },
    crate::{
        remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision},
        transaction::unique_aborted_transaction_rows,
    },
    creusot_std::prelude::*,
};

/// Archive admission, consumer visibility, remote inclusive overlap and local
/// half-open overlap produce every necessary wire abort row exactly once.
/// Marker owners may lie entirely beyond the fetched data window. These two
/// arrays are complete decoded source indexes; enumeration, current lineage,
/// I/O, and client-side record/marker filtering remain host obligations.
#[requires(from@ >= 0)]
#[ensures(match result {
    None => !(restored_abort_index_valid(remote@, owners.0)
        && restored_abort_index_valid(local@, owners.1)),
    Some(rows) => restored_abort_index_valid(remote@, owners.0)
        && restored_abort_index_valid(local@, owners.1)
        && (crate::sequence::distinct(rows@))
        && (forall<i: Int> 0 <= i && i < rows@.len() ==> rows@[i].0@ >= 0 && rows@[i].1@ >= 0
            && (wire_abort_from_source(remote@, rows@[i], from@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@))
                || wire_abort_from_source(local@, rows@[i], from@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@))))
        && wire_aborts_cover_source(remote@, rows@, from@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@))
        && wire_aborts_cover_source(local@, rows@, from@, w.hw@.min(w.lso@).min(w.deliverable@).min(cut@)),
})]
pub(super) fn restored_abort_sources_cover_committed_fetch(
    remote: &[RestoreAbortedTxn],
    local: &[RestoreAbortedTxn],
    owners: (RestoreSegmentExtent, RestoreSegmentExtent),
    w: FetchWatermarks,
    from: i64,
    cut: i64,
) -> Option<Vec<(i64, i64)>> {
    let remote_indices =
        restored_aborts_remain_bounded_when_fetch_shrinks(remote, owners.0, w, from, cut)?;
    let local_indices =
        restored_aborts_remain_bounded_when_fetch_shrinks(local, owners.1, w, from, cut)?;
    let limit = truncation_frontier(fetch_visibility(false, true, w, from).limit_offset, cut);
    let query_last = if limit > from { limit - 1 } else { -1 };
    let mut rows: Vec<(i64, i64)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= remote_indices@.len() && rows@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> rows@[j] == (remote@[remote_indices@[j]@].producer_id, remote@[remote_indices@[j]@].start_offset))]
    #[variant(remote_indices@.len() - i@)]
    while i < remote_indices.len() {
        let entry = remote[remote_indices[i]];
        if matches!(
            remote_txn_overlap_decision(entry.start_offset, entry.last_offset, from, query_last),
            RemoteTxnOverlapDecision::Overlap
        ) {
            rows.push((entry.producer_id, entry.start_offset));
        }
        i += 1;
    }
    let mut i = 0usize;
    #[invariant(i@ <= local_indices@.len() && rows@.len() == remote_indices@.len() + i@)]
    #[invariant(forall<j: Int> 0 <= j && j < remote_indices@.len()
        ==> rows@[j] == (remote@[remote_indices@[j]@].producer_id, remote@[remote_indices@[j]@].start_offset))]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> rows@[remote_indices@.len() + j] == (local@[local_indices@[j]@].producer_id, local@[local_indices@[j]@].start_offset))]
    #[variant(local_indices@.len() - i@)]
    while i < local_indices.len() {
        let entry = local[local_indices[i]];
        rows.push((entry.producer_id, entry.start_offset));
        i += 1;
    }
    proof_assert!(forall<j: Int> 0 <= j && j < rows@.len() ==> {
        if j < remote_indices@.len() {
            let k = remote_indices@[j]@;
            0 <= k && k < remote@.len()
                && rows@[j] == (remote@[k].producer_id, remote@[k].start_offset)
                && abort_intersects_fetch(remote@[k], from@, limit@)
        } else {
            let k = local_indices@[j - remote_indices@.len()]@;
            0 <= k && k < local@.len()
                && rows@[j] == (local@[k].producer_id, local@[k].start_offset)
                && abort_intersects_fetch(local@[k], from@, limit@)
        }
    });
    proof_assert!(forall<k: Int> 0 <= k && k < remote@.len()
        && abort_intersects_fetch(remote@[k], from@, limit@)
        ==> exists<j: Int> 0 <= j && j < remote_indices@.len()
            && rows@[j] == (remote@[k].producer_id, remote@[k].start_offset));
    proof_assert!(forall<k: Int> 0 <= k && k < local@.len()
        && abort_intersects_fetch(local@[k], from@, limit@)
        ==> exists<j: Int> remote_indices@.len() <= j && j < rows@.len()
            && rows@[j] == (local@[k].producer_id, local@[k].start_offset));
    Some(unique_aborted_transaction_rows(&rows))
}
