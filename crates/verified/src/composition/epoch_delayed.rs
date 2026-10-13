#[cfg(creusot)]
use super::epoch_handoff::handoff_input_valid;
use {
    super::{
        ProducerDecision, ProducerSnapshotEntryFacts, completed_batches_preserve_first_retry,
        decrement_sequence, epoch_handoff_distinguishes_identity_and_data_retry,
        rebuilt_data_window_bounds_retry,
    },
    crate::transaction::InitProducerIdIdentityDecision,
    creusot_std::prelude::*,
};

type DelayedHandoff = (
    InitProducerIdIdentityDecision,
    Vec<(bool, ProducerDecision)>,
    ProducerDecision,
    (i64, i64, bool),
);

/// After a verified same-PID handoff, arbitrarily many old-epoch callbacks
/// cannot replace the new batch or change its retry coordinates/readiness.
/// The old identity remains an initialization retry; old data remains fenced.
/// Truthful same-PID projections, serialized installation and one HWM observation
/// are host facts. New-epoch arrivals, truncation, expiry and persistence are outside.
#[requires(handoff_input_valid(end@, hwm@, epoch@, rows@, first))]
#[requires(forall<i: Int> 0 <= i && i < delayed@.len() ==>
    crate::producer_snapshot::snapshot_entry_valid_model(end@, delayed@[i]) && delayed@[i].last_offset@ >= 0
    && delayed@[i].producer_id == first.producer_id && delayed@[i].producer_epoch@ <= epoch@)]
#[ensures(result.0 == InitProducerIdIdentityDecision::Retry)]
#[ensures(result.1@.len() == delayed@.len()
    && (forall<i: Int> 0 <= i && i < result.1@.len() ==> !result.1@[i].0 && result.1@[i].1 == ProducerDecision::Fenced))]
#[ensures(match result.2 { ProducerDecision::Duplicate { retained } => retained@ == 4, _ => false })]
#[ensures(result.3.0@ == first.last_offset@ - first.offset_delta@
    && result.3.1@ == first.last_offset@ + 1 && 0 <= result.3.0@ && result.3.0@ < result.3.1@
    && result.3.1@ <= end@ && result.3.2 == (hwm@ >= result.3.1@))]
pub(super) fn epoch_handoff_survives_delayed_completions(
    end: i64,
    hwm: i64,
    epoch: i16,
    rows: &[ProducerSnapshotEntryFacts],
    mut first: ProducerSnapshotEntryFacts,
    delayed: &[ProducerSnapshotEntryFacts],
    request: (i32, i32),
) -> DelayedHandoff {
    let (next_epoch, identity, selected, _) =
        epoch_handoff_distinguishes_identity_and_data_retry(end, hwm, epoch, rows, first, request);
    first.producer_epoch = next_epoch;
    let mut kept = Vec::new();
    let source = selected[0];
    kept.push(if source == rows.len() {
        first
    } else {
        rows[source]
    });
    let mut outcomes: Vec<(bool, ProducerDecision)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= delayed@.len() && outcomes@.len() == i@)]
    #[invariant(kept@.len() == 1 && kept@[0] == first)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> !outcomes@[j].0 && outcomes@[j].1 == ProducerDecision::Fenced)]
    #[variant(delayed@.len() - i@)]
    while i < delayed.len() {
        let incoming = delayed[i];
        let (accepted, selected, decision, _) = completed_batches_preserve_first_retry(
            end,
            hwm,
            Some(next_epoch),
            &kept,
            incoming,
            (epoch, request.0, request.1),
        );
        // Consume the selected origin rather than assuming callbacks leave state alone.
        let source = selected[0];
        let survivor = if source == kept.len() {
            incoming
        } else {
            kept[source]
        };
        kept[0] = survivor;
        outcomes.push((accepted, decision));
        i += 1;
    }
    let (_, decision, witness) = rebuilt_data_window_bounds_retry(
        end,
        hwm,
        &kept,
        (
            next_epoch,
            decrement_sequence(first.last_sequence, first.offset_delta),
            first.offset_delta,
            false,
        ),
    );
    let (_, base, frontier, ready) = witness.unwrap();
    (identity, outcomes, decision, (base, frontier, ready))
}
