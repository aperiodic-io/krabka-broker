use {
    super::{
        RestoreBatchFrame, RestoreExclusions, RestoreFilterDecision, RestoreRecordDeltas,
        local_append_coordinates, restore_batch_filter_decision, restore_record_coordinates,
        restore_record_selected,
    },
    creusot_std::prelude::*,
};

open_logic! {
pub(super) fn selection_input_valid(
    frame: RestoreBatchFrame,
    records: Seq<(RestoreRecordDeltas, RestoreExclusions)>,
) -> bool {
    pearlite! { frame.base_offset@ >= 0 && frame.last_offset_delta@ >= 0
    && frame.base_offset@ + frame.last_offset_delta@ + 1 <= i64::MAX@
    && forall<i: Int> 0 <= i && i < records.len()
        ==> crate::restore::restore_record_placeable(frame, records[i].0) }
}
}

open_logic! {
pub(super) fn restore_source_selected(
    frame: RestoreBatchFrame,
    row: (RestoreRecordDeltas, RestoreExclusions),
    offset_bound: Option<i64>,
    timestamp_bound: Option<i64>,
) -> bool {
    pearlite! {
        (match offset_bound { Some(bound) => frame.base_offset@ + row.0.offset_delta@ <= bound@, None => true })
        && (match timestamp_bound { Some(bound) => crate::restore::kafka_record_timestamp(frame, row.0) < bound@, None => true })
        && !(row.1.producer || row.1.offset || row.1.content.key || row.1.content.header)
    }
}
}

open_logic! {
pub(super) fn selection_ordered(selected: Seq<usize>, count: Int) -> bool {
    pearlite! {
        (forall<j: Int> 0 <= j && j < selected.len() ==> selected[j]@ < count)
        && (crate::sequence::strictly_increasing(selected))
    }
}
}

/// Return the original indices of exactly every selected record, in source
/// order, alongside the complete archived span and whole-batch decision.
/// Invalid coordinates reject the complete input, including excluded rows.
#[ensures(match result {
    None => !selection_input_valid(frame, records@),
    Some((next, decision, selected)) => selection_input_valid(frame, records@)
        && next@ == frame.base_offset@ + frame.last_offset_delta@ + 1
        && selected@.len() <= records@.len()
        && (forall<i: Int> 0 <= i && i < selected@.len() ==> selected@[i]@ < records@.len())
        && (crate::sequence::strictly_increasing(selected@))
        && (forall<i: Int> 0 <= i && i < records@.len() ==>
            (crate::sequence::contains_source_index(selected@, i))
                == restore_source_selected(frame, records@[i], offset_bound, timestamp_bound))
        && (forall<i: Int> 0 <= i && i < selected@.len() ==>
            restore_source_selected(frame, records@[selected@[i]@], offset_bound, timestamp_bound)
            && frame.base_offset@ <= frame.base_offset@ + records@[selected@[i]@].0.offset_delta@
            && frame.base_offset@ + records@[selected@[i]@].0.offset_delta@ < next@)
        && decision == if selected@.len() == records@.len() { RestoreFilterDecision::Keep }
            else if selected@.len() == 0 { RestoreFilterDecision::Empty }
            else { RestoreFilterDecision::Filter },
})]
pub(super) fn restore_selection_respects_batch_extent(
    frame: RestoreBatchFrame,
    records: &[(RestoreRecordDeltas, RestoreExclusions)],
    offset_bound: Option<i64>,
    timestamp_bound: Option<i64>,
) -> Option<(i64, RestoreFilterDecision, Vec<usize>)> {
    let (_, next) = local_append_coordinates(
        frame.base_offset,
        frame.base_offset,
        frame.last_offset_delta,
    )?;
    let mut selected: Vec<usize> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= records@.len() && selected@.len() <= i@)]
    #[invariant(selection_input_valid(frame, records@.subsequence(0, i@)))]
    #[invariant(selection_ordered(selected@, i@))]
    #[invariant(forall<j: Int> 0 <= j && j < selected@.len() ==>
        restore_source_selected(frame, records@[selected@[j]@], offset_bound, timestamp_bound)
        && frame.base_offset@ <= frame.base_offset@ + records@[selected@[j]@].0.offset_delta@
        && frame.base_offset@ + records@[selected@[j]@].0.offset_delta@ < next@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        (crate::sequence::contains_source_index(selected@, j))
            == restore_source_selected(frame, records@[j], offset_bound, timestamp_bound))]
    #[variant(records@.len() - i@)]
    while i < records.len() {
        let (record, exclusions) = records[i];
        let (offset, timestamp) = restore_record_coordinates(frame, record)?;
        if restore_record_selected(offset, offset_bound, timestamp, timestamp_bound, exclusions) {
            selected.push(i);
        }
        i += 1;
    }
    let kept = selected.len();
    let decision = restore_batch_filter_decision(kept > 0, kept < records.len());
    Some((next, decision, selected))
}
