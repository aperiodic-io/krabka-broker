use {
    crate::{
        list_offsets::{
            ListOffsetsSelectionDecision, ListOffsetsSelectionFacts, earliest_timestamp_candidate,
            list_offsets_kind, list_offsets_selection_decision,
        },
        timestamp::first_timestamp_record_index,
    },
    creusot_std::prelude::*,
};

/// Floor-aware first matches in independently ordered, overlapping tiers
/// compose into the first retained, visible match in their union. Complete
/// decoding, consistent overlapping records, and coherent captured frontiers
/// remain host obligations; failed tier reads are outside this success trace.
#[requires(target@ >= 0 && minimum@ >= 0 && bound@ >= 0 && epoch@ >= -1)]
#[requires(forall<i: Int> 0 <= i && i < remote@.len() ==> remote@[i].0@ >= 0)]
#[requires(forall<i: Int> 0 <= i && i < local@.len() ==> local@[i].0@ >= 0)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < remote@.len()
    ==> remote@[i].0@ < remote@[j].0@)]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < local@.len()
    ==> local@[i].0@ < local@[j].0@)]
#[ensures(match result {
    ListOffsetsSelectionDecision::RejectMalformed => false,
    ListOffsetsSelectionDecision::Resolved { offset, timestamp, leader_epoch } =>
        minimum@ <= offset@ && offset@ < bound@ && timestamp@ >= target@ && leader_epoch == epoch
        && ((exists<i: Int> 0 <= i && i < remote@.len() && (offset, timestamp) == remote@[i])
            || (exists<i: Int> 0 <= i && i < local@.len() && (offset, timestamp) == local@[i]))
        && (forall<i: Int> 0 <= i && i < remote@.len()
            && remote@[i].0@ >= minimum@ && remote@[i].1@ >= target@ ==> offset@ <= remote@[i].0@)
        && (forall<i: Int> 0 <= i && i < local@.len()
            && local@[i].0@ >= minimum@ && local@[i].1@ >= target@ ==> offset@ <= local@[i].0@),
    ListOffsetsSelectionDecision::Unknown =>
        (forall<i: Int> 0 <= i && i < remote@.len()
            ==> remote@[i].0@ < minimum@ || remote@[i].1@ < target@ || remote@[i].0@ >= bound@)
        && (forall<i: Int> 0 <= i && i < local@.len()
            ==> local@[i].0@ < minimum@ || local@[i].1@ < target@ || local@[i].0@ >= bound@),
})]
pub(super) fn tiered_timestamp_lookup_preserves_first(
    remote: &[(i64, i64)],
    local: &[(i64, i64)],
    target: i64,
    minimum: i64,
    bound: i64,
    epoch: i32,
) -> ListOffsetsSelectionDecision {
    let remote = first_timestamp_record_index(remote, minimum, target).map(|i| remote[i]);
    let local = first_timestamp_record_index(local, minimum, target).map(|i| local[i]);
    let (offset, timestamp) = earliest_timestamp_candidate(remote, local).unwrap_or((-1, -1));
    list_offsets_selection_decision(ListOffsetsSelectionFacts {
        kind: list_offsets_kind(target, 11),
        candidate_offset: offset,
        candidate_timestamp: timestamp,
        candidate_epoch: epoch,
        last_fetchable: bound,
    })
}
