#[cfg(creusot)]
use super::restored_aborts::{
    restored_abort_index_valid, wire_abort_from_source, wire_aborts_cover_source,
};
#[cfg(creusot)]
use crate::transaction::pending_starts_bound_fetch;
use {
    super::{
        FetchWatermarks, RestoreAbortedTxn, RestoreSegmentExtent,
        committed_fetch_excludes_unstable, restored_abort_sources_cover_committed_fetch,
        truncation_frontier,
    },
    creusot_std::prelude::*,
};

type StableAbortFetch = (i64, i64, Vec<(i64, i64)>);

/// Complete unstable starts determine the read-committed prefix, whose remote
/// and local abort sources supply exactly the necessary unique wire rows.
/// Starts, indexes and watermarks must describe one coherent log lineage;
/// byte reads, requested-floor authorization and client filtering are external.
#[requires(from@ >= 0)]
#[ensures(match result {
    None => (exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@)
        || !restored_abort_index_valid(remote@, owners.0)
        || !restored_abort_index_valid(local@, owners.1),
    Some((lso, limit, rows)) => restored_abort_index_valid(remote@, owners.0)
        && restored_abort_index_valid(local@, owners.1)
        && lso@ <= w.log_end@
        && (pending_starts_bound_fetch(starts@, w.log_end@, lso@, limit@))
        && ((starts@.len() == 0 && lso@ == w.log_end@)
            || (exists<i: Int> 0 <= i && i < starts@.len() && lso@ == starts@[i]@))
        && limit@ == lso@.min(w.hw@).min(w.deliverable@).min(cut@)
        && (crate::sequence::distinct(rows@))
        && (forall<i: Int> 0 <= i && i < rows@.len() ==>
            wire_abort_from_source(remote@, rows@[i], from@, limit@)
            || wire_abort_from_source(local@, rows@[i], from@, limit@))
        && wire_aborts_cover_source(remote@, rows@, from@, limit@)
        && wire_aborts_cover_source(local@, rows@, from@, limit@),
})]
#[ensures(match result {
    None => true,
    Some((_, limit, _)) => forall<v: Int> v <= w.log_end@ && v <= w.hw@
        && v <= w.deliverable@ && v <= cut@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> v <= starts@[i]@)
        ==> v <= limit@,
})]
pub(super) fn stable_abort_sources_cover_fetch(
    starts: &[i64],
    remote: &[RestoreAbortedTxn],
    local: &[RestoreAbortedTxn],
    owners: (RestoreSegmentExtent, RestoreSegmentExtent),
    w: FetchWatermarks,
    from: i64,
    cut: i64,
) -> Option<StableAbortFetch> {
    let (lso, visibility) = committed_fetch_excludes_unstable(starts, w)?;
    let limit = truncation_frontier(visibility.limit_offset, cut);
    let rows = restored_abort_sources_cover_committed_fetch(
        remote,
        local,
        owners,
        FetchWatermarks { lso, ..w },
        from,
        cut,
    )?;
    Some((lso, limit, rows))
}
