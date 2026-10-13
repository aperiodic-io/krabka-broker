#[cfg(creusot)]
use super::completed_producer::completed_row;
#[cfg(creusot)]
use crate::producer_snapshot::retained_producer_row;
use {
    super::{ProducerSnapshotEntryFacts, completed_batches_preserve_first_retry},
    crate::{
        produce::produce_durability_frontier, producer::decrement_sequence, raft::frontier_reaches,
    },
    creusot_std::prelude::*,
};

// (evicted, nominated frontier, nominated ready, retained (source, frontier, ready)).
type EvictionWaiters = (bool, i64, bool, Vec<(usize, i64, bool)>);

/// Evicting an eligible completed batch requires five physically newer batches.
/// Every retained batch's waiter therefore waits beyond the evicted frontier:
/// a ready retained waiter implies that the evicted batch is also HWM-covered.
/// Epoch replacement, callback completeness and physical persistence are outside
/// this implication. Sequence aliases can still name a different retained batch.
#[requires(0 <= end@ && 0 <= hwm@ && origin@ < rows@.len() && rows@.len() <= 5)]
#[requires(crate::producer_snapshot::snapshot_entry_valid_model(end@, incoming) && incoming.last_offset@ >= 0)]
#[requires(incoming.producer_epoch@ <= epoch@)]
#[requires(forall<i: Int> 0 <= i && i < rows@.len() ==>
    retained_producer_row(end@, rows@[i], incoming.producer_id) && rows@[i].producer_epoch == epoch)]
#[requires(crate::producer_snapshot::producer_offsets_ordered(rows@))]
#[ensures(result.1@ == rows@[origin@].last_offset@ + 1 && 0 < result.1@ && result.1@ <= end@)]
#[ensures(result.2 == (hwm@ >= result.1@))]
#[ensures(0 < result.3@.len() && result.3@.len() <= 5)]
#[ensures(result.0 == !(exists<j: Int> 0 <= j && j < result.3@.len() && result.3@[j].0 == origin))]
#[ensures(forall<j: Int> 0 <= j && j < result.3@.len() ==>
    result.3@[j].0@ <= rows@.len()
    && result.3@[j].1@ == completed_row(rows@, incoming, result.3@[j].0@).last_offset@ + 1
    && 0 < result.3@[j].1@ && result.3@[j].1@ <= end@
    && result.3@[j].2 == (hwm@ >= result.3@[j].1@))]
#[ensures(forall<i: Int, j: Int> 0 <= i && i < j && j < result.3@.len() ==>
    result.3@[i].1@ < result.3@[j].1@)]
#[ensures(result.0 ==> result.3@.len() == 5 && (forall<j: Int> 0 <= j && j < result.3@.len() ==>
    result.1@ < result.3@[j].1@ && (result.3@[j].2 ==> result.2)))]
#[ensures(result.0 && !result.2 ==> (forall<j: Int> 0 <= j && j < result.3@.len() ==> !result.3@[j].2))]
pub(super) fn completed_eviction_bounds_waiters(
    end: i64,
    hwm: i64,
    epoch: i16,
    rows: &[ProducerSnapshotEntryFacts],
    incoming: ProducerSnapshotEntryFacts,
    origin: usize,
) -> EvictionWaiters {
    let nominated = rows[origin];
    let frontier = produce_durability_frontier(
        nominated.last_offset - i64::from(nominated.offset_delta),
        nominated.offset_delta,
    )
    .unwrap();
    let ready = frontier_reaches(hwm, frontier);
    let (_, selected, _, _) = completed_batches_preserve_first_retry(
        end,
        hwm,
        Some(epoch),
        rows,
        incoming,
        (
            epoch,
            decrement_sequence(nominated.last_sequence, nominated.offset_delta),
            nominated.offset_delta,
        ),
    );
    let mut retained: Vec<(usize, i64, bool)> = Vec::new();
    let mut present = false;
    let mut j = 0usize;
    #[invariant(j@ <= selected@.len() && retained@.len() == j@)]
    #[invariant(present == (exists<k: Int> 0 <= k && k < j@ && selected@[k] == origin))]
    #[invariant(forall<k: Int> 0 <= k && k < j@ ==>
        retained@[k].0 == selected@[k]
        && retained@[k].1@ == completed_row(rows@, incoming, selected@[k]@).last_offset@ + 1
        && retained@[k].2 == (hwm@ >= retained@[k].1@))]
    #[variant(selected@.len() - j@)]
    while j < selected.len() {
        let source = selected[j];
        let row = if source == rows.len() {
            incoming
        } else {
            rows[source]
        };
        let target = produce_durability_frontier(
            row.last_offset - i64::from(row.offset_delta),
            row.offset_delta,
        )
        .unwrap();
        retained.push((source, target, frontier_reaches(hwm, target)));
        present = present || source == origin;
        j += 1;
    }
    (!present, frontier, ready, retained)
}
