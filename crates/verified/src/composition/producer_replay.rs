#[cfg(creusot)]
use crate::producer_snapshot::{
    nonduplicate_snapshot_decision, recovered_batch_coordinates, retained_producer_row,
    snapshot_sequence_matches,
};
use {
    super::{
        ProducerDecision, ProducerEntryFacts, ProducerSnapshotEntryFacts, RetainedSequenceRange,
        decrement_sequence, increment_sequence, local_append_coordinates,
        produce_durability_frontier, producer_decision, producer_snapshot_entry_valid,
    },
    creusot_std::prelude::*,
};

/// A valid snapshot's retained data batch reconstructs its sequence and
/// physical span, answers retries with the original acknowledgement frontier,
/// fences older epochs, and admits the next sequence at the same epoch.
/// The host must load this entry for the request's PID, without a replayed
/// tail, into Kafka's five-slot ring (four empty earlier slots, then the last
/// batch). Duplicate classification is sequence based; it does not compare bytes.
#[ensures((result != None) ==
    (snapshot@ >= 0 && entry.producer_id@ >= 0 && entry.producer_epoch@ >= 0
        && entry.coordinator_epoch@ >= -1 && entry.last_sequence@ >= 0
        && entry.offset_delta@ >= 0 && entry.offset_delta@ <= entry.last_offset@
        && entry.last_offset@ < snapshot@
        && (entry.current_txn_first_offset@ == -1
            || (0 <= entry.current_txn_first_offset@ && entry.current_txn_first_offset@ <= entry.last_offset@
                && entry.current_txn_first_offset@ < snapshot@))))]
#[ensures(match result {
    None => true,
    Some((base, sequence, frontier, retry, successor)) =>
        0 <= base@ && base@ == entry.last_offset@ - entry.offset_delta@
        && 0 <= sequence@ && sequence@ <= i32::MAX@
        && sequence@ == crate::producer::sequence_modulo_2_31(entry.last_sequence@ - entry.offset_delta@)
        && frontier@ == entry.last_offset@ + 1 && base@ < frontier@ && frontier@ <= snapshot@
        && (request_epoch == entry.producer_epoch ==> retry == ProducerDecision::Duplicate { retained: 4usize })
        && (request_epoch@ < entry.producer_epoch@ ==> retry == ProducerDecision::Fenced)
        && (request_epoch@ > entry.producer_epoch@ ==> retry ==
            if sequence@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder })
        && successor == ProducerDecision::Append,
})]
pub(super) fn reloaded_snapshot_preserves_last_batch_retry(
    snapshot: i64,
    entry: ProducerSnapshotEntryFacts,
    request_epoch: i16,
) -> Option<(i64, i32, i64, ProducerDecision, ProducerDecision)> {
    if !producer_snapshot_entry_valid(snapshot, entry) || entry.last_offset < 0 {
        return None;
    }
    let base = entry.last_offset - i64::from(entry.offset_delta);
    let sequence = decrement_sequence(entry.last_sequence, entry.offset_delta);
    let retained = [
        None,
        None,
        None,
        None,
        Some(RetainedSequenceRange {
            base_sequence: sequence,
            last_sequence: entry.last_sequence,
        }),
    ];
    let tracked = Some(ProducerEntryFacts {
        epoch: entry.producer_epoch,
        last_sequence: entry.last_sequence,
    });
    let retry = producer_decision(
        tracked,
        &retained,
        request_epoch,
        sequence,
        entry.offset_delta,
        false,
        true,
    );
    let successor = producer_decision(
        tracked,
        &retained,
        entry.producer_epoch,
        increment_sequence(entry.last_sequence, 1),
        0,
        false,
        true,
    );
    let (last, next) = local_append_coordinates(base, base, entry.offset_delta)?;
    let frontier = produce_durability_frontier(base, entry.offset_delta)?;
    if last != entry.last_offset || next != frontier {
        return None;
    }
    Some((base, sequence, frontier, retry, successor))
}

/// A faithfully rebuilt data window uses four earlier slots and the last
/// batch at slot four. Compose snapshot-row admission/reconstruction with
/// retry classification and acknowledgements, preserving the first matching
/// batch's original physical span. Sequence wrap can alias retained batches;
/// a retry need not identify the newest one. Complete same-PID/epoch replay
/// and faithful row projection are host obligations, not proved here.
#[requires(0 < rows@.len() && rows@.len() <= 5)]
#[requires(forall<i: Int> 0 <= i && i < rows@.len()
    ==> retained_producer_row(end@, rows@[i], rows@[0].producer_id)
        && rows@[i].producer_epoch == rows@[0].producer_epoch)]
#[ensures((match result.0 { ProducerDecision::Duplicate { .. } => true, _ => false }) ==
    (request.0 == rows@[0].producer_epoch && exists<i: Int> 0 <= i && i < rows@.len()
        && snapshot_sequence_matches(rows@[i], request.1@, request.2@)))]
#[ensures(match result {
    (ProducerDecision::Duplicate { retained: slot }, Some((index, base, frontier))) =>
        index@ < rows@.len() && slot@ == if index@ + 1 == rows@.len() { 4 } else { index@ }
        && recovered_batch_coordinates(rows@[index@], base@, frontier@, end@)
        && snapshot_sequence_matches(rows@[index@], request.1@, request.2@)
        && forall<i: Int> 0 <= i && i < index@ ==>
            !(snapshot_sequence_matches(rows@[i], request.1@, request.2@)),
    (ProducerDecision::Duplicate { .. }, None) => false,
    (_, None) => true,
    (_, Some(_)) => false,
})]
#[ensures(match result.0 {
    ProducerDecision::Duplicate { .. } => true,
    _ => result.0 == nonduplicate_snapshot_decision(request.0@, rows@[0].producer_epoch@, request.1@, rows@[rows@.len() - 1].last_sequence@),
})]
pub(super) fn replayed_window_preserves_first_retry_coordinates(
    end: i64,
    rows: &[ProducerSnapshotEntryFacts],
    request: (i16, i32, i32),
) -> (ProducerDecision, Option<(usize, i64, i64)>) {
    let mut retained: [Option<RetainedSequenceRange>; 5] = [None; 5];
    let mut coordinates: [Option<(usize, i64, i64)>; 5] = [None; 5];
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==>
        match coordinates@[if k + 1 == rows@.len() { 4 } else { k }] {
            Some((origin, base, frontier)) => origin@ == k
                && base@ == rows@[k].last_offset@ - rows@[k].offset_delta@
                && frontier@ == rows@[k].last_offset@ + 1,
            None => false,
        })]
    #[invariant(forall<slot: Int> 0 <= slot && slot < 5 ==> match coordinates@[slot] {
        None => retained@[slot] == None,
        Some((index, base, frontier)) => index@ < i@
            && slot == if index@ + 1 == rows@.len() { 4 } else { index@ }
            && recovered_batch_coordinates(rows@[index@], base@, frontier@, end@)
            && match retained@[slot] {
                Some(range) => range.base_sequence@ == crate::producer::sequence_modulo_2_31(
                    rows@[index@].last_sequence@ - rows@[index@].offset_delta@)
                    && range.last_sequence == rows@[index@].last_sequence,
                None => false,
            },
    })]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let row = rows[i];
        let (base, sequence, frontier, _, _) =
            reloaded_snapshot_preserves_last_batch_retry(end, row, row.producer_epoch).unwrap();
        let slot = if i + 1 == rows.len() { 4 } else { i };
        retained[slot] = Some(RetainedSequenceRange {
            base_sequence: sequence,
            last_sequence: row.last_sequence,
        });
        coordinates[slot] = Some((i, base, frontier));
        i += 1;
    }
    let last = rows[rows.len() - 1];
    let decision = producer_decision(
        Some(ProducerEntryFacts {
            epoch: last.producer_epoch,
            last_sequence: last.last_sequence,
        }),
        &retained,
        request.0,
        request.1,
        request.2,
        false,
        true,
    );
    let duplicate = match decision {
        ProducerDecision::Duplicate { retained: slot } => coordinates[slot],
        _ => None,
    };
    (decision, duplicate)
}
