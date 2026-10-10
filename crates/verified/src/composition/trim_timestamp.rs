use creusot_std::prelude::*;

#[cfg(creusot)]
use super::time_index::sparse_timestamp_window_valid;
#[cfg(creusot)]
use super::trim::{trim_cursor_bounded, trim_store_frontiers_valid, trim_well_formed};
#[cfg(creusot)]
use super::trim::{trim_has_no_snapshot, trim_rejection};
use super::{
    DeleteRecordsTrimDecision, DeleteRecordsTrimFacts, SparseTimestampWindow,
    admitted_trim_bounds_reload_and_retry, constructed_index_retained_candidate,
};

type TrimTimestampWitness = (i64, Option<usize>, i64, Option<usize>);

/// Read from the actual completed logical floor, independently of the newer
/// producer replay cursor. A retained match below that cursor remains readable.
/// Decoding, snapshot contents and durable completion remain host obligations.
#[requires(sparse_timestamp_window_valid(window.0@, window.1@, window.2@))]
#[requires(trim_store_frontiers_valid(facts, stores.0@, stores.1@))]
#[requires(base@ >= 0)]
#[requires(forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ < facts.log_end@)]
#[ensures(match result {
    Err(error) => trim_rejection(facts, error),
    Ok((floor, snapshot, cursor, selected)) => trim_well_formed(facts)
        && (facts.requested@ == -1 || facts.requested@ <= facts.high_watermark@)
        && trim_cursor_bounded(facts, (stores.0@, stores.1@), floor@, cursor@)
        && match snapshot {
            None => trim_has_no_snapshot(snapshots@, floor, facts.log_end, cursor),
            Some(index) => index@ < snapshots@.len() && floor@ < snapshots@[index@]@ && cursor == snapshots@[index@]
                && super::trim::latest_retained_snapshot(snapshots@, floor@, facts.log_end@, cursor@),
        }
        && match selected {
            None => forall<i: Int> 0 <= i && i < window.0@.len() ==> base@ + window.0@[i]@ < floor@ || window.1@[i]@ < target@,
            Some(index) => index@ < window.0@.len() && floor@ <= base@ + window.0@[index@]@ && base@ + window.0@[index@]@ < facts.log_end@
                && window.1@[index@]@ >= target@
                && forall<i: Int> 0 <= i && i < index@ ==> base@ + window.0@[i]@ < floor@ || window.1@[i]@ < target@,
        },
})]
pub(super) fn completed_trim_preserves_retained_timestamp(
    facts: DeleteRecordsTrimFacts,
    stores: (i64, i64),
    snapshots: &[i64],
    window: SparseTimestampWindow<'_>,
    base: i64,
    target: i64,
) -> Result<TrimTimestampWitness, DeleteRecordsTrimDecision> {
    let (wal, local) = stores;
    let (floor, snapshot, cursor) =
        admitted_trim_bounds_reload_and_retry(facts, wal, local, snapshots)?;
    let selected = constructed_index_retained_candidate(window, base, floor, target);
    Ok((floor, snapshot, cursor, selected))
}
