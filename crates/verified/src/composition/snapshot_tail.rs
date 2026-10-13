#[cfg(creusot)]
use super::control_truncation::physical_batch_sequence_valid;
#[cfg(creusot)]
use super::control_truncation::whole_batch_frontier;
#[cfg(creusot)]
use crate::producer_snapshot::{retained_producer_row, snapshot_sequence_matches};
use {
    super::{
        ProducerDecision, ProducerReloadRange, ProducerSnapshotEntryFacts,
        producer_snapshot_entry_valid, producer_snapshot_replay_start,
        rebuilt_data_window_bounds_retry, whole_batch_truncation_bounds_controls,
    },
    creusot_std::prelude::*,
};

type LoadedData = Option<(i64, Option<ProducerSnapshotEntryFacts>)>;
// Cursor, seed present, replayed source indices, window start, decision, witness.
type SeededRetry = (
    i64,
    bool,
    Vec<Option<usize>>,
    usize,
    ProducerDecision,
    Option<(Option<usize>, i64, i64, bool)>,
);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn replay_data_row(
    loaded: LoadedData,
    tail: Seq<ProducerSnapshotEntryFacts>,
    source: Option<usize>,
) -> Option<ProducerSnapshotEntryFacts> {
    pearlite! { match source {
        Some(index) => Some(tail[index@]),
        None => match loaded { Some((_, row)) => row, None => None },
    } }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn retry_matches_row(
    row: Option<ProducerSnapshotEntryFacts>,
    request: (i16, i32, i32, bool),
) -> bool {
    pearlite! { match row { None => false, Some(row) => request.0 == row.producer_epoch
    && snapshot_sequence_matches(row, request.1@, request.2@) } }
}

/// Reconstruct a data-only PID from an admitted snapshot seed and the exact
/// locally replayed physical prefix. The seed can lie below both retained
/// floors. Snapshot identity/coverage, complete decoded local rows, marker-free
/// PID history, serialized replay and actual I/O remain host obligations.
#[requires(physical_batch_sequence_valid(ends@, physical_start@, bounds.0@))]
#[requires(range.log_end@ == if ends@.len() == 0 { physical_start@ } else { ends@[ends@.len() - 1]@ })]
#[requires(0 <= range.log_start@ && range.log_start@ <= range.log_end@
    && physical_start@ <= range.local_start@ && range.local_start@ <= range.log_end@
    && 0 <= bounds.1@ && bounds.1@ <= range.log_end@)]
#[requires(forall<i: Int> 0 <= i && i < tail@.len() ==>
    retained_producer_row(range.log_end@, tail@[i], tail@[0].producer_id)
    && (exists<j: Int> 0 <= j && j < ends@.len() && ends@[j]@ == tail@[i].last_offset@ + 1)
    && match loaded { Some((_, Some(seed))) => seed.producer_id == tail@[i].producer_id, _ => true })]
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < tail@.len() ==> tail@[i].last_offset@ < tail@[j].last_offset@)]
#[ensures(whole_batch_frontier(ends@, physical_start, bounds.0@, result.0))]
#[ensures((result.1 != None) == (range.log_start@ <= result.0@ && range.local_start@ <= result.0@
    && match loaded { None => true, Some((offset, row)) => range.log_start@ < offset@ && offset@ <= result.0@
        && match row { None => true, Some(entry) => entry.last_offset@ >= 0
            && crate::producer_snapshot::snapshot_entry_valid_model(offset@, entry) } }))]
#[ensures(match result.1 { None => true, Some((cursor, seeded, origins, first, decision, witness)) =>
    cursor@ == range.local_start@.max(match loaded { None => range.log_start@, Some((offset, _)) => offset@ })
    && range.log_start@ <= cursor@ && cursor@ <= result.0@
    && seeded == (match loaded { Some((_, Some(_))) => true, _ => false })
    && first@ <= origins@.len() && origins@.len() - first@ <= 5
    && (seeded ==> origins@.len() > 0 && origins@[0] == None)
    && (forall<j: Int> 0 <= j && j < origins@.len() ==>
        (origins@[j] == None) == (seeded && j == 0)
        && match origins@[j] { None => true, Some(index) => index@ < tail@.len()
            && cursor@ <= tail@[index@].last_offset@ && tail@[index@].last_offset@ < result.0@ })
    && (forall<i: Int> 0 <= i && i < tail@.len() ==>
        (exists<j: Int> 0 <= j && j < origins@.len() && match origins@[j] { None => false, Some(index) => index@ == i })
        == (cursor@ <= tail@[i].last_offset@ && tail@[i].last_offset@ < result.0@))
    && (forall<i: Int, j: Int> 0 <= i && i < j && j < origins@.len() ==>
        match (origins@[i], origins@[j]) { (Some(a), Some(b)) => a@ < b@, _ => true })
    && (origins@.len() == 0 ==> first@ == 0)
    && (origins@.len() > 0 ==> first@ < origins@.len()
        && match replay_data_row(loaded, tail@, origins@[origins@.len() - 1]) { None => false, Some(last) =>
            (forall<j: Int> first@ <= j && j < origins@.len() ==>
                match replay_data_row(loaded, tail@, origins@[j]) { None => false, Some(row) => row.producer_epoch == last.producer_epoch })
            && (first@ > 0 && origins@.len() - first@ < 5 ==>
                match replay_data_row(loaded, tail@, origins@[first@ - 1]) { None => false, Some(row) => row.producer_epoch != last.producer_epoch }) })
    && (match decision { ProducerDecision::Duplicate { .. } => true, _ => false }) ==
        (exists<j: Int> first@ <= j && j < origins@.len() && retry_matches_row(replay_data_row(loaded, tail@, origins@[j]), request))
    && match witness { None => (match decision { ProducerDecision::Duplicate { .. } => false, _ => true }),
        Some((source, base, frontier, ready)) => 0 <= base@ && base@ < frontier@ && frontier@ <= result.0@
            && ready == (bounds.1@ >= frontier@)
            && (exists<j: Int> first@ <= j && j < origins@.len() && origins@[j] == source
                && (match decision { ProducerDecision::Duplicate { retained: slot } => slot@ == if j + 1 == origins@.len() { 4 } else { j - first@ }, _ => false })
                && (forall<k: Int> first@ <= k && k < j ==> !retry_matches_row(replay_data_row(loaded, tail@, origins@[k]), request)))
            && match replay_data_row(loaded, tail@, source) { None => false, Some(row) =>
                base@ == row.last_offset@ - row.offset_delta@ && frontier@ == row.last_offset@ + 1
                && retry_matches_row(Some(row), request) },
    }
    && (origins@.len() == 0 ==> decision == if request.3 && result.0@ == 0 && request.1@ != 0 {
        ProducerDecision::OutOfOrder } else { ProducerDecision::Append })
    && (origins@.len() > 0 && witness == None ==>
        match replay_data_row(loaded, tail@, origins@[origins@.len() - 1]) { None => false, Some(last) => decision ==
            if request.0@ < last.producer_epoch@ { ProducerDecision::Fenced }
            else if request.0@ > last.producer_epoch@ { if request.1@ == 0 { ProducerDecision::Append } else { ProducerDecision::OutOfOrder } }
            else if request.1@ == crate::producer::sequence_modulo_2_31(last.last_sequence@ + 1) { ProducerDecision::Append }
            else { ProducerDecision::OutOfOrder } })
})]
pub(super) fn loaded_snapshot_bounds_truncated_retry(
    ends: &[i64],
    physical_start: i64,
    range: ProducerReloadRange,
    bounds: (i64, i64),
    loaded: LoadedData,
    tail: &[ProducerSnapshotEntryFacts],
    request: (i16, i32, i32, bool),
) -> (i64, Option<SeededRetry>) {
    let (cut, previous_hwm) = bounds;
    let (_, end, hwm, _, _, _, _) =
        whole_batch_truncation_bounds_controls(ends, physical_start, cut, previous_hwm, &[], 0);
    let shortened = ProducerReloadRange {
        log_end: end,
        ..range
    };
    let snapshot = loaded.map(|(offset, _)| offset);
    let Some(cursor) = producer_snapshot_replay_start(shortened, snapshot) else {
        return (end, None);
    };
    let seed = match loaded {
        Some((offset, Some(entry))) => {
            if !producer_snapshot_entry_valid(offset, entry) || entry.last_offset < 0 {
                return (end, None);
            }
            Some(entry)
        }
        _ => None,
    };
    let mut rows: Vec<ProducerSnapshotEntryFacts> = Vec::new();
    let mut origins: Vec<Option<usize>> = Vec::new();
    if let Some(entry) = seed {
        proof_assert!(crate::producer_snapshot::snapshot_entry_valid_model(end@, entry));
        rows.push(entry);
        origins.push(None);
    }
    let mut i = 0usize;
    #[invariant(i@ <= tail@.len() && rows@.len() == origins@.len())]
    #[invariant(seed != None ==> rows@.len() > 0 && origins@[0] == None)]
    #[invariant(forall<j: Int> 0 <= j && j < origins@.len() ==>
        (origins@[j] == None) == (seed != None && j == 0)
        && match origins@[j] { None => Some(rows@[j]) == seed, Some(index) => index@ < i@
            && cursor@ <= tail@[index@].last_offset@ && tail@[index@].last_offset@ < end@
            && rows@[j] == tail@[index@] })]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==>
        (exists<j: Int> 0 <= j && j < origins@.len() && match origins@[j] { None => false, Some(index) => index@ == k })
        == (cursor@ <= tail@[k].last_offset@ && tail@[k].last_offset@ < end@))]
    #[invariant(forall<j: Int> 0 <= j && j < rows@.len() ==>
        crate::producer_snapshot::snapshot_entry_valid_model(end@, rows@[j]) && rows@[j].last_offset@ >= 0)]
    #[invariant(forall<a: Int, b: Int> 0 <= a && a < rows@.len() && 0 <= b && b < rows@.len() ==>
        rows@[a].producer_id == rows@[b].producer_id)]
    #[invariant(forall<a: Int, b: Int> 0 <= a && a < b && b < origins@.len() ==>
        match (origins@[a], origins@[b]) { (Some(x), Some(y)) => x@ < y@, _ => true })]
    #[variant(tail@.len() - i@)]
    while i < tail.len() {
        if cursor <= tail[i].last_offset && tail[i].last_offset < end {
            proof_assert!(crate::producer_snapshot::snapshot_entry_valid_model(end@, tail@[i@]));
            rows.push(tail[i]);
            origins.push(Some(i));
        }
        i += 1;
    }
    proof_assert!(forall<j: Int> 0 <= j && j < origins@.len() ==>
        Some(rows@[j]) == replay_data_row(loaded, tail@, origins@[j]));
    proof_assert!(forall<j: Int> 0 <= j && j < rows@.len() ==>
        retry_matches_row(replay_data_row(loaded, tail@, origins@[j]), request) ==
        (request.0 == rows@[j].producer_epoch
            && snapshot_sequence_matches(rows@[j], request.1@, request.2@)));
    let (first, decision, witness) = rebuilt_data_window_bounds_retry(end, hwm, &rows, request);
    let Some((index, base, frontier, ready)) = witness else {
        return (
            end,
            Some((cursor, seed.is_some(), origins, first, decision, None)),
        );
    };
    let source = origins[index];
    (
        end,
        Some((
            cursor,
            seed.is_some(),
            origins,
            first,
            decision,
            Some((source, base, frontier, ready)),
        )),
    )
}
