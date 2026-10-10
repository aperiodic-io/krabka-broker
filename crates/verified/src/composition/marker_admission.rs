use creusot_std::prelude::*;

use super::{
    FetchWatermarks, committed_fetch_excludes_unstable, control_marker_bounds_committed_fetch,
};
use crate::transaction::{
    TransactionMarkerMaterializationDecision as Decision, TransactionMarkerPartitionState,
    TransactionMarkerRequest, transaction_marker_equal_epoch_fenced,
    transaction_marker_materialization_decision,
};

open_logic! {
pub(super) fn pending_marker_admitted(
    version: i16,
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
) -> bool {
    pearlite! { request.producer_id@ >= 0
    && request.producer_epoch@ >= current.producer_epoch@
    && request.coordinator_epoch@ >= current.coordinator_epoch@
    && (version@ < 2 || request.producer_epoch != current.producer_epoch
        || request.producer_epoch@ == i16::MAX@) }
}
}

#[cfg(creusot)]
mod progress;
#[cfg(creusot)]
use progress::lemma_maximal_prefix_advances;

open_logic! {
fn marker_prefix_maximal(bounds: (i64, i64), marker_end: Int, starts: Seq<i64>, limit: Int) -> bool {
    pearlite! { forall<v: Int> v <= bounds.0@ && v <= bounds.1@
        && v <= marker_end + 1
        && (forall<i: Int> 0 <= i && i < starts.len() ==> v <= starts[i]@)
        ==> v <= limit }
}
}

type AdmittedMarkerFetch = (Decision, i64, i64, Option<(i64, i64)>);

/// Connect both marker-generation fences to log closure and consumer visibility.
/// Rejected markers leave the pending transaction and Fetch limit unchanged.
/// An admitted marker releases its start only after HW crosses the whole marker;
/// its resulting limit is maximal under the remaining starts and delivery cap.
/// The PID-keyed pending state and other starts must be complete and coherent.
/// Successful durable append, serialized application, marker encoding and offset
/// publication are host obligations; this does not prove actor or I/O completion.
#[requires(current.has_pending_transaction && current.producer_epoch@ >= 0
    && current.coordinator_epoch@ >= -1)]
#[requires(0 <= pending_start@ && pending_start@ <= span.0@ && 0 <= span.1@
    && span.0@ + span.1@ < i64::MAX@)]
#[requires(crate::transaction::pending_starts_before_marker(other_starts@, span.0@))]
#[ensures(result.1@ <= pending_start@ && result.1@ <= result.2@)]
#[ensures(result.2@ <= bounds.0@ && result.2@ <= bounds.1@
    && result.2@ <= span.0@ + span.1@ + 1)]
#[ensures(forall<i: Int> 0 <= i && i < other_starts@.len()
    ==> result.2@ <= other_starts@[i]@)]
#[ensures((result.0 == Decision::AppendAndPublishOffsets)
    == (pending_marker_admitted(version, request, current)
        && request.is_commit && request.is_offsets_partition))]
#[ensures((result.0 == Decision::AppendWithoutOffsetPublication)
    == (pending_marker_admitted(version, request, current)
        && !(request.is_commit && request.is_offsets_partition)))]
#[ensures(result.0 != Decision::Retry)]
#[ensures(!pending_marker_admitted(version, request, current)
    || bounds.0@ <= span.0@ + span.1@ ==> result.2 == result.1)]
#[ensures(match result.3 {
    Some((start, last)) => pending_marker_admitted(version, request, current)
        && !request.is_commit && start == pending_start && last@ == span.0@ + span.1@,
    None => !pending_marker_admitted(version, request, current) || request.is_commit,
})]
#[ensures(pending_marker_admitted(version, request, current)
    && bounds.0@ > span.0@ + span.1@ ==>
    marker_prefix_maximal(bounds, span.0@ + span.1@, other_starts@, result.2@))]
#[ensures(pending_marker_admitted(version, request, current)
    && bounds.0@ > span.0@ + span.1@ && bounds.1@ > pending_start@
    && (forall<i: Int> 0 <= i && i < other_starts@.len() ==> other_starts@[i]@ > pending_start@)
    ==> result.2@ > result.1@)]
pub(super) fn admitted_marker_bounds_committed_fetch(
    version: i16,
    request: TransactionMarkerRequest,
    current: TransactionMarkerPartitionState,
    pending_start: i64,
    span: (i64, i32), // prospective marker base and last-offset delta
    other_starts: &[i64],
    bounds: (i64, i64), // observed HW and delivery cap
) -> AdmittedMarkerFetch {
    let (_, other_visibility) = committed_fetch_excludes_unstable(
        other_starts,
        FetchWatermarks {
            log_start: 0,
            log_end: span.0,
            hw: bounds.0,
            lso: span.0,
            deliverable: bounds.1,
        },
    )
    .unwrap();
    let before = other_visibility.limit_offset.min(pending_start);
    proof_assert!(before@ <= pending_start@
        && before@ <= bounds.0@ && before@ <= bounds.1@
        && (forall<j: Int> 0 <= j && j < other_starts@.len()
            ==> before@ <= other_starts@[j]@));
    proof_assert!(forall<v: Int> v <= pending_start@ && v <= bounds.0@ && v <= bounds.1@
        && (forall<j: Int> 0 <= j && j < other_starts@.len() ==> v <= other_starts@[j]@)
        ==> v <= before@);
    let decision = if transaction_marker_equal_epoch_fenced(
        version,
        request.producer_epoch,
        current.producer_epoch,
        current.has_pending_transaction,
    ) {
        Decision::RejectProducerEpoch
    } else {
        transaction_marker_materialization_decision(request, current)
    };
    match decision {
        Decision::AppendAndPublishOffsets | Decision::AppendWithoutOffsetPublication => {}
        _ => return (decision, before, before, None),
    }
    // build_marker_batch emits exactly one of these COMMIT/ABORT keys.
    let key = [0, 0, 0, u8::from(request.is_commit)];
    let (after, aborted) = control_marker_bounds_committed_fetch(
        &key,
        true,
        (request.producer_id, request.producer_id, pending_start),
        span,
        other_starts,
        bounds,
    );
    proof_assert!(before@ <= after@);
    if bounds.0 <= span.0 + i64::from(span.1) {
        proof_assert!(after@ <= pending_start@);
        proof_assert!(after == before);
    }
    proof_assert!(bounds.0@ > span.0@ + span.1@ ==>
        marker_prefix_maximal(bounds, span.0@ + span.1@, other_starts@, after@));
    proof_assert!({
        let advances = bounds.0@ > span.0@ + span.1@ && bounds.1@ > pending_start@
            && (forall<j: Int> 0 <= j && j < other_starts@.len()
                ==> other_starts@[j]@ > pending_start@);
        advances ==> lemma_maximal_prefix_advances(pending_start@,
            (bounds.0@, bounds.1@, span.0@ + span.1@ + 1), other_starts@, after@)
    });
    (decision, before, after, aborted)
}
