use creusot_std::prelude::*;

use super::{
    ProducerDecision, ProducerSnapshotEntryFacts, producer_decision,
    replayed_window_preserves_first_retry_coordinates,
};
#[cfg(creusot)]
use crate::producer_snapshot::{
    nonduplicate_snapshot_decision, recovered_batch_coordinates, retained_producer_row,
    snapshot_sequence_matches,
};
use crate::raft::frontier_reaches;

open_logic! {
pub(super) fn retry_row_matches(
    rows: Seq<ProducerSnapshotEntryFacts>,
    request: (i16, i32, i32, bool),
    window: (Int, Int),
    publication: (Int, Int, bool),
) -> bool {
    pearlite! { publication.2 == (publication.0 >= publication.1)
        && request.0 == rows[window.1].producer_epoch
        && snapshot_sequence_matches(rows[window.1], request.1@, request.2@)
        && (forall<i: Int> window.0 <= i && i < window.1 ==>
            !snapshot_sequence_matches(rows[i], request.1@, request.2@)) }
}
}

type RebuiltRetry = (usize, ProducerDecision, Option<(usize, i64, i64, bool)>);

/// The latest contiguous epoch's last five data rows form the retry window.
/// Rows may come from a snapshot seed below the physical floor and a replay
/// tail. Their order, PID association and faithful projection remain host facts.
#[requires(0 <= end@ && 0 <= hwm@)]
#[requires(forall<i: Int> 0 <= i && i < rows@.len() ==>
    retained_producer_row(end@, rows@[i], rows@[0].producer_id))]
#[ensures(result.0@ <= rows@.len() && rows@.len() - result.0@ <= 5
    && (rows@.len() == 0 ==> result.0@ == 0)
    && (rows@.len() > 0 ==> result.0@ < rows@.len()
        && (forall<i: Int> result.0@ <= i && i < rows@.len() ==>
            rows@[i].producer_epoch == rows@[rows@.len() - 1].producer_epoch)
        && (result.0@ > 0 && rows@.len() - result.0@ < 5 ==>
            rows@[result.0@ - 1].producer_epoch != rows@[rows@.len() - 1].producer_epoch))
    && (match result.1 { ProducerDecision::Duplicate { .. } => true, _ => false }) ==
        (exists<i: Int> result.0@ <= i && i < rows@.len()
            && request.0 == rows@[i].producer_epoch
            && snapshot_sequence_matches(rows@[i], request.1@, request.2@))
    && match result.2 { None => (match result.1 { ProducerDecision::Duplicate { .. } => false, _ => true }),
        Some((index, base, frontier, ready)) => result.0@ <= index@ && index@ < rows@.len()
            && (match result.1 { ProducerDecision::Duplicate { retained: slot } =>
                slot@ == if index@ + 1 == rows@.len() { 4 } else { index@ - result.0@ }, _ => false })
            && recovered_batch_coordinates(rows@[index@], base@, frontier@, end@)
            && retry_row_matches(rows@, request, (result.0@, index@), (hwm@, frontier@, ready)),
    }
    && (rows@.len() == 0 ==> result.1 == if request.3 && end@ == 0 && request.1@ != 0 {
        ProducerDecision::OutOfOrder } else { ProducerDecision::Append })
    && (rows@.len() > 0 && result.2 == None ==> result.1 ==
        nonduplicate_snapshot_decision(request.0@, rows@[rows@.len() - 1].producer_epoch@, request.1@, rows@[rows@.len() - 1].last_sequence@)))]
pub(super) fn rebuilt_data_window_bounds_retry(
    end: i64,
    hwm: i64,
    rows: &[ProducerSnapshotEntryFacts],
    request: (i16, i32, i32, bool),
) -> RebuiltRetry {
    let count = rows.len();
    if count == 0 {
        return (
            0,
            producer_decision(
                None,
                &[],
                request.0,
                request.1,
                request.2,
                end == 0,
                request.3,
            ),
            None,
        );
    }
    let epoch = rows[count - 1].producer_epoch;
    let mut first = count;
    #[invariant(first@ <= count@ && count@ - first@ <= 5)]
    #[invariant(forall<j: Int> first@ <= j && j < count@ ==> rows@[j].producer_epoch == epoch)]
    #[variant(first)]
    while first > 0 && count - first < 5 && rows[first - 1].producer_epoch == epoch {
        first -= 1;
    }
    proof_assert!(first@ < count@);
    let mut kept: Vec<ProducerSnapshotEntryFacts> = Vec::new();
    let mut i = first;
    #[invariant(first@ <= i@ && i@ <= count@ && kept@.len() == i@ - first@)]
    #[invariant(forall<j: Int> 0 <= j && j < kept@.len() ==> kept@[j] == rows@[first@ + j])]
    #[variant(count@ - i@)]
    while i < count {
        kept.push(rows[i]);
        i += 1;
    }
    let (decision, coordinates) = replayed_window_preserves_first_retry_coordinates(
        end,
        &kept,
        (request.0, request.1, request.2),
    );
    proof_assert!(forall<j: Int> first@ <= j && j < count@ ==> rows@[j] == kept@[j - first@]);
    let Some((origin, base, frontier)) = coordinates else {
        return (first, decision, None);
    };
    (
        first,
        decision,
        Some((
            first + origin,
            base,
            frontier,
            frontier_reaches(hwm, frontier),
        )),
    )
}
