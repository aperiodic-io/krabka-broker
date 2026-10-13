#[cfg(creusot)]
use crate::producer_snapshot::{retained_producer_row, snapshot_sequence_matches};
use {
    super::{
        ProducerDecision, ProducerEntryFacts, ProducerSnapshotEntryFacts, producer_decision,
        rebuilt_data_window_bounds_retry,
    },
    crate::producer::producer_completion_window,
    creusot_std::prelude::*,
};

open_logic! {
pub fn completed_row(
    rows: Seq<ProducerSnapshotEntryFacts>,
    incoming: ProducerSnapshotEntryFacts,
    index: Int,
) -> ProducerSnapshotEntryFacts {
    pearlite! { if index == rows.len() { incoming } else { rows[index] } }
}
}

open_logic! {
pub fn matches_retry(
    rows: Seq<ProducerSnapshotEntryFacts>,
    incoming: ProducerSnapshotEntryFacts,
    index: Int,
    request: (i16, i32, i32),
) -> bool {
    pearlite! { let row = completed_row(rows, incoming, index);
    request.0 == row.producer_epoch
    && snapshot_sequence_matches(row, request.1@, request.2@) }
}
}

type CompletedRetry = (
    bool,
    Vec<usize>,
    ProducerDecision,
    Option<(usize, i64, i64, bool)>,
);

/// Physical completion order, rather than acknowledgement scheduling, defines
/// the five-batch retry window. Equal offsets identify the same batch and keep
/// old metadata. Marker-only current epochs reject older completions without
/// becoming an empty producer. Complete truthful completions and serialized
/// installation remain host obligations; truncation races and bytes are outside.
#[requires(0 <= end@ && 0 <= hwm@ && rows@.len() <= 5)]
#[requires(crate::producer_snapshot::snapshot_entry_valid_model(end@, incoming) && incoming.last_offset@ >= 0)]
#[requires(match current { None => rows@.len() == 0, Some(epoch) => epoch@ >= 0 })]
#[requires(forall<i: Int> 0 <= i && i < rows@.len() ==>
    retained_producer_row(end@, rows@[i], incoming.producer_id) && current == Some(rows@[i].producer_epoch))]
#[requires(crate::producer_snapshot::producer_offsets_ordered(rows@))]
#[ensures(result.0 == (crate::producer::completion_epoch_accepts(current, incoming.producer_epoch)))]
#[ensures(crate::producer::completion_window_bounded(result.1@, result.0))]
#[ensures(forall<j: Int> 0 <= j && j < result.1@.len() ==> result.1@[j]@ <= rows@.len()
    && completed_row(rows@, incoming, result.1@[j]@).producer_epoch ==
        (match current { Some(epoch) => if epoch@ > incoming.producer_epoch@ { epoch } else { incoming.producer_epoch }, None => incoming.producer_epoch }))]
#[ensures((forall<i: Int, j: Int> 0 <= i && i < j && j < result.1@.len() ==>
    completed_row(rows@, incoming, result.1@[i]@).last_offset@ < completed_row(rows@, incoming, result.1@[j]@).last_offset@)
    && (forall<i: Int> 0 <= i && i < rows@.len()
    && (match current { Some(epoch) => incoming.producer_epoch@ <= epoch@, None => false })
    && !crate::producer::completion_source_selected(result.1@, i) ==>
        result.1@.len() == 5 && (forall<j: Int> 0 <= j && j < result.1@.len() ==>
            rows@[i].last_offset@ < completed_row(rows@, incoming, result.1@[j]@).last_offset@))
    && (result.0 && !(exists<j: Int> 0 <= j && j < result.1@.len()
    && completed_row(rows@, incoming, result.1@[j]@).last_offset == incoming.last_offset) ==>
        result.1@.len() == 5 && (forall<j: Int> 0 <= j && j < result.1@.len() ==>
            incoming.last_offset@ < completed_row(rows@, incoming, result.1@[j]@).last_offset@)))]
#[ensures(!result.0 ==> result.1@.len() == rows@.len()
    && (forall<j: Int> 0 <= j && j < rows@.len() ==> result.1@[j]@ == j))]
#[ensures((match result.2 { ProducerDecision::Duplicate { .. } => true, _ => false }) ==
    (exists<j: Int> 0 <= j && j < result.1@.len() && matches_retry(rows@, incoming, result.1@[j]@, request)))]
#[ensures(match result.3 { None => (match result.2 { ProducerDecision::Duplicate { .. } => false, _ => true }),
    Some((source, base, frontier, ready)) => source@ <= rows@.len()
        && 0 <= base@ && base@ < frontier@ && frontier@ <= end@ && ready == (hwm@ >= frontier@)
        && base@ == completed_row(rows@, incoming, source@).last_offset@ - completed_row(rows@, incoming, source@).offset_delta@
        && frontier@ == completed_row(rows@, incoming, source@).last_offset@ + 1
        && matches_retry(rows@, incoming, source@, request)
        && (exists<j: Int> 0 <= j && j < result.1@.len() && result.1@[j] == source
            && (match result.2 { ProducerDecision::Duplicate { retained: slot } => slot@ == if j + 1 == result.1@.len() { 4 } else { j }, _ => false })
            && (forall<k: Int> 0 <= k && k < j ==> !matches_retry(rows@, incoming, result.1@[k]@, request))),
})]
#[ensures(((result.2 == ProducerDecision::Fenced) == (request.0@ <
    (match current { Some(epoch) => if epoch@ > incoming.producer_epoch@ { epoch@ } else { incoming.producer_epoch@ }, None => incoming.producer_epoch@ })))
    && (result.1@.len() == 0 ==> result.2 ==
    if request.0@ < (match current { Some(epoch) => epoch@, None => incoming.producer_epoch@ }) { ProducerDecision::Fenced }
    else if request.1@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder }))]
#[ensures(rows@.len() > 0 && current == Some(incoming.producer_epoch)
    && incoming.last_offset@ <= rows@[rows@.len() - 1].last_offset@ ==>
    result.1@.len() > 0 && result.1@[result.1@.len() - 1]@ == rows@.len() - 1)]
pub(super) fn completed_batches_preserve_first_retry(
    end: i64,
    hwm: i64,
    current: Option<i16>,
    rows: &[ProducerSnapshotEntryFacts],
    incoming: ProducerSnapshotEntryFacts,
    request: (i16, i32, i32),
) -> CompletedRetry {
    let mut ends: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len() && ends@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> ends@[j] == rows@[j].last_offset)]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        ends.push(rows[i].last_offset);
        i += 1;
    }
    let (accepted, selected) = producer_completion_window(
        current,
        incoming.producer_epoch,
        &ends,
        incoming.last_offset,
    );
    let count = selected.len();
    if count == 0 {
        let epoch = current.unwrap();
        let decision = producer_decision(
            Some(ProducerEntryFacts {
                epoch,
                last_sequence: -1,
            }),
            &[],
            request.0,
            request.1,
            request.2,
            end == 0,
            false,
        );
        return (accepted, selected, decision, None);
    }
    let mut kept: Vec<ProducerSnapshotEntryFacts> = Vec::new();
    let mut j = 0usize;
    #[invariant(j@ <= selected@.len() && kept@.len() == j@)]
    #[invariant(forall<k: Int> 0 <= k && k < j@ ==> kept@[k] == completed_row(rows@, incoming, selected@[k]@))]
    #[variant(selected@.len() - j@)]
    while j < selected.len() {
        let source = selected[j];
        kept.push(if source == rows.len() {
            incoming
        } else {
            rows[source]
        });
        j += 1;
    }
    proof_assert!(forall<k: Int> 0 <= k && k < kept@.len() ==> matches_retry(rows@, incoming, selected@[k]@, request)
        == (request.0 == kept@[k].producer_epoch
            && request.1@ == crate::producer::sequence_modulo_2_31(kept@[k].last_sequence@ - kept@[k].offset_delta@)
            && kept@[k].last_sequence@ == crate::producer::sequence_modulo_2_31(request.1@ + request.2@)));
    let (_, decision, witness) =
        rebuilt_data_window_bounds_retry(end, hwm, &kept, (request.0, request.1, request.2, false));
    let Some((index, base, frontier, ready)) = witness else {
        return (accepted, selected, decision, None);
    };
    let source = selected[index];
    (
        accepted,
        selected,
        decision,
        Some((source, base, frontier, ready)),
    )
}
