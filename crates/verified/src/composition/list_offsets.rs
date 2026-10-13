#[cfg(creusot)]
use super::timestamp::{record_offsets_ordered, sparse_maxima_bound_prefix};
use {
    super::{first_unstable_offset, indexed_timestamp_scan_finds_first},
    crate::list_offsets::{
        ListOffsetsBoundDecision, ListOffsetsBoundFacts, ListOffsetsSelectionDecision,
        ListOffsetsSelectionFacts, list_offsets_bound_decision, list_offsets_kind,
        list_offsets_selection_decision,
    },
    creusot_std::prelude::*,
};

/// Sparse timestamp lookup, transaction-derived isolation, and response
/// selection return the first visible match exactly when one exists.
/// Arrays are the complete decoded data window; sparse maxima bound earlier
/// timestamps. Frontiers and pending starts describe one coherent snapshot.
#[requires(offsets@.len() == timestamps@.len())]
#[requires(0 <= frontiers.0@ && frontiers.0@ <= frontiers.1@
    && 0 <= frontiers.2@ && frontiers.2@ <= frontiers.1@)]
#[requires(request.2@ >= 0 && candidate_epoch@ >= -1)]
#[requires(forall<i: Int> 0 <= i && i < offsets@.len()
    ==> frontiers.0@ + offsets@[i]@ < frontiers.1@)]
#[requires(record_offsets_ordered(offsets@))]
#[requires(forall<i: Int> 0 <= i && i < starts@.len()
    ==> 0 <= starts@[i]@ && starts@[i]@ <= frontiers.1@)]
#[requires(sparse_maxima_bound_prefix(entries@, offsets@, timestamps@))]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        leader_epoch == candidate_epoch
        && exists<i: Int> 0 <= i && i < offsets@.len()
            && offset@ == frontiers.0@ + offsets@[i]@
            && timestamp == timestamps@[i] && timestamp@ >= request.2@
            && (forall<j: Int> 0 <= j && j < i ==> timestamps@[j]@ < request.2@)
            && list_offset_candidate_visible(frontiers, request, starts@, offset@),
    ListOffsetsSelectionDecision::Unknown =>
        !(exists<i: Int> 0 <= i && i < offsets@.len() && timestamps@[i]@ >= request.2@
            && list_offset_candidate_visible(frontiers, request, starts@, frontiers.0@ + offsets@[i]@)),
})]
pub(super) fn timestamp_list_offsets_finds_first_visible(
    entries: &[(i64, u32)],
    offsets: &[u32],
    timestamps: &[i64],
    starts: &[i64],
    frontiers: (i64, i64, i64), // segment base, log end, HWM
    request: (i32, i8, i64),    // replica id, isolation level, timestamp
    candidate_epoch: i32,       // host lookup for the matched record
) -> ListOffsetsSelectionDecision {
    let lso = first_unstable_offset(starts, frontiers.1).expect("coherent pending starts");
    let bound = match list_offsets_bound_decision(ListOffsetsBoundFacts {
        replica_id: request.0,
        isolation_level: request.1,
        log_end: frontiers.1,
        high_watermark: frontiers.2,
        last_stable: lso,
    }) {
        ListOffsetsBoundDecision::Bound { offset } => offset,
        ListOffsetsBoundDecision::RejectMalformed => {
            return ListOffsetsSelectionDecision::RejectMalformed;
        }
    };
    let (offset, timestamp) =
        match indexed_timestamp_scan_finds_first(entries, offsets, timestamps, request.2) {
            Some(index) => (frontiers.0 + i64::from(offsets[index]), timestamps[index]),
            None => (-1, -1),
        };
    list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind: list_offsets_kind(request.2, 11),
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch,
        last_fetchable: bound,
    })
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A candidate lies below the replica bound and every applicable unstable start.
fn list_offset_candidate_visible(
    frontiers: (i64, i64, i64),
    request: (i32, i8, i64),
    starts: Seq<i64>,
    offset: Int,
) -> bool {
    pearlite! { offset < (if request.0@ == -1 { frontiers.2@ } else { frontiers.1@ })
    && (request.0@ == -1 && request.1@ == 1 ==>
        forall<j: Int> 0 <= j && j < starts.len() ==> offset < starts[j]@) }
}
