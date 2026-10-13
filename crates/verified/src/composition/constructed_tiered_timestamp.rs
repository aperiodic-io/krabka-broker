use {
    super::{SparseTimestampWindow, constructed_index_retained_candidate},
    crate::list_offsets::{
        ListOffsetsSelectionDecision, ListOffsetsSelectionFacts, earliest_timestamp_candidate,
        list_offsets_kind, list_offsets_selection_decision,
    },
    creusot_std::prelude::*,
};

/// Construct each tier's sparse index, select retained matches, merge the
/// earliest absolute coordinate and apply exclusive `ListOffsets` visibility.
/// Actual row maxima establish scan safety; no supplied bound boolean is used.
/// Complete decoding/enumeration, overlapping-record consistency, epoch lookup,
/// coherent publication and successful I/O remain host obligations.
#[requires(remote.0@.len() == remote.1@.len() && local.0@.len() == local.1@.len())]
#[requires(bases.0@ >= 0 && bases.1@ >= 0 && request.0@ >= 0 && request.1@ >= 0 && request.2@ >= 0 && epoch@ >= -1)]
#[requires(super::time_index::absolute_record_prefix_bounded(remote.0@, remote.0@.len(), bases.0@, i64::MAX@))]
#[requires(super::time_index::absolute_record_prefix_bounded(local.0@, local.0@.len(), bases.1@, i64::MAX@))]
#[requires(crate::sequence::strictly_increasing(remote.0@))]
#[requires(crate::sequence::strictly_increasing(local.0@))]
#[requires(forall<i: Int> 0 <= i && i < remote.2@.len() ==> remote.2@[i].0@ <= remote.2@[i].1@ && remote.2@[i].1@ < remote.1@.len())]
#[requires(forall<i: Int> 0 <= i && i < local.2@.len() ==> local.2@[i].0@ <= local.2@[i].1@ && local.2@[i].1@ < local.1@.len())]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < remote.2@.len() ==> remote.2@[i].0@ < remote.2@[j].0@ && remote.2@[i].1@ <= remote.2@[j].1@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < local.2@.len() ==> local.2@[i].0@ < local.2@[j].0@ && local.2@[i].1@ <= local.2@[j].1@)]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        request.1@ <= offset@ && offset@ < request.2@ && timestamp@ >= request.0@ && leader_epoch == epoch
        && ((exists<i: Int> 0 <= i && i < remote.0@.len() && offset@ == bases.0@ + remote.0@[i]@ && timestamp == remote.1@[i])
            || (exists<i: Int> 0 <= i && i < local.0@.len() && offset@ == bases.1@ + local.0@[i]@ && timestamp == local.1@[i]))
        && (forall<i: Int> 0 <= i && i < remote.0@.len() && bases.0@ + remote.0@[i]@ >= request.1@ && remote.1@[i]@ >= request.0@
            ==> offset@ <= bases.0@ + remote.0@[i]@)
        && (forall<i: Int> 0 <= i && i < local.0@.len() && bases.1@ + local.0@[i]@ >= request.1@ && local.1@[i]@ >= request.0@
            ==> offset@ <= bases.1@ + local.0@[i]@),
    ListOffsetsSelectionDecision::Unknown =>
        (forall<i: Int> 0 <= i && i < remote.0@.len() ==> bases.0@ + remote.0@[i]@ < request.1@ || remote.1@[i]@ < request.0@ || bases.0@ + remote.0@[i]@ >= request.2@)
        && (forall<i: Int> 0 <= i && i < local.0@.len() ==> bases.1@ + local.0@[i]@ < request.1@ || local.1@[i]@ < request.0@ || bases.1@ + local.0@[i]@ >= request.2@),
})]
pub(super) fn constructed_tiered_timestamp_preserves_first(
    remote: SparseTimestampWindow<'_>,
    local: SparseTimestampWindow<'_>,
    bases: (i64, i64),
    request: (i64, i64, i64), // target timestamp, logical floor, exclusive bound
    epoch: i32,
) -> ListOffsetsSelectionDecision {
    let remote = constructed_index_retained_candidate(remote, bases.0, request.1, request.0)
        .map(|i| (bases.0 + i64::from(remote.0[i]), remote.1[i]));
    let local = constructed_index_retained_candidate(local, bases.1, request.1, request.0)
        .map(|i| (bases.1 + i64::from(local.0[i]), local.1[i]));
    let (offset, timestamp) = earliest_timestamp_candidate(remote, local).unwrap_or((-1, -1));
    list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind: list_offsets_kind(request.0, 11),
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch: epoch,
        last_fetchable: request.2,
    })
}
