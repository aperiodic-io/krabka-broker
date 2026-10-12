use creusot_std::prelude::*;

use super::{
    FetchWatermarks, delivery_watermark_advance, earliest_max_timestamp_index, fetch_visibility,
    restore_batch_step, scheduled_delivery_visible,
};
use crate::broker::FetchVisibility;
#[cfg(creusot)]
use crate::broker::committed_fetch_response;

open_logic! {
pub fn scheduled_activation_due(activation: Int, uncertainty: Int, now: Int) -> bool {
    pearlite! { uncertainty >= 0 && activation + uncertainty <= i64::MAX@
    && activation + uncertainty <= now }
}
}

open_logic! {
pub(super) fn scheduled_batches_valid(batches: Seq<(i64, i32)>, start: Int, end: Int) -> bool {
    pearlite! {
        (forall<i: Int> 0 <= i && i < batches.len() ==> batches[i].1@ >= 0
            && start <= batches[i].0@ && batches[i].0@ + batches[i].1@ + 1 <= end
            && (i > 0 ==> batches[i - 1].0@ + batches[i - 1].1@ + 1 <= batches[i].0@))
        && if batches.len() == 0 { start == end }
            else { batches[batches.len() - 1].0@ + batches[batches.len() - 1].1@ + 1 == end }
    }
}
}

open_logic! {
pub(super) fn scheduled_prefix_covers_pending(
    batches: Seq<(i64, i32)>, activations: Seq<i64>, clock: (Int, Int), frontier: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < batches.len()
        && !scheduled_activation_due(activations[i]@, clock.0, clock.1)
        ==> frontier <= batches[i].0@ }
}
}

/// The maximum over actual batch activation times is due iff every batch is
/// due. This justifies the whole-segment activation shortcut, including empty
/// segments, signed timestamp extremes, and deadline overflow.
#[ensures(result == (forall<i: Int> 0 <= i && i < activations@.len() ==>
    uncertainty@ >= 0 && activations@[i]@ + uncertainty@ <= i64::MAX@
        && activations@[i]@ + uncertainty@ <= now@))]
pub(super) fn segment_maximum_proves_delivery(
    activations: &[i64],
    uncertainty: i64,
    now: i64,
) -> bool {
    match earliest_max_timestamp_index(activations) {
        Some(index) => scheduled_delivery_visible(true, uncertainty, activations[index], now),
        None => true,
    }
}

type ScheduledPrefix = (i64, FetchVisibility, FetchVisibility);

/// Return the greatest delivery prefix and actual consumer/follower views.
/// Reject any malformed or incomplete batch walk; compaction gaps are allowed.
/// Recompute from the start rather than validating a cached cursor. Complete
/// decoded batches and activation stamps from one log are host obligations.
#[requires(scheduled_inputs_coherent(batches@, activations@, w.log_start@, w.log_end@))]
#[ensures(match result {
    None => !scheduled_batches_valid(batches@, w.log_start@, w.log_end@),
    Some((frontier, consumer, follower)) => scheduled_batches_valid(batches@, w.log_start@, w.log_end@)
        && w.log_start@ <= frontier@ && frontier@ <= w.log_end@
        && scheduled_prefix_covers_pending(batches@, activations@, (uncertainty@, now@), frontier@)
        && (frontier@ == w.log_end@ || exists<i: Int> 0 <= i && i < batches@.len()
            && frontier@ == batches@[i].0@
            && !scheduled_activation_due(activations@[i]@, uncertainty@, now@))
        && consumer.limit_offset@ == w.hw@.min(w.lso@).min(frontier@)
        && committed_fetch_response(consumer, w.hw, w.lso, frontier, w.log_start@, w.log_end@)
        && follower.limit_offset == w.log_end && follower.response_hw == w.hw
        && follower.response_lso@ == w.hw@.min(w.lso@) && follower.effective_lso == w.lso
        && !follower.read_committed_aborts && !follower.out_of_range
        && follower.empty == (w.log_start == w.log_end),
})]
pub(super) fn scheduled_prefix_bounds_fetch(
    batches: &[(i64, i32)],
    activations: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) -> Option<ScheduledPrefix> {
    let all_due = segment_maximum_proves_delivery(activations, uncertainty, now);
    let mut cursor = w.log_start;
    let mut candidate = w.log_end;
    let mut i = 0usize;
    #[invariant(i@ <= batches@.len())]
    #[invariant(w.log_start@ <= cursor@ && cursor@ <= w.log_end@)]
    #[invariant(cursor@ == if i@ == 0 { w.log_start@ }
        else { batches@[i@ - 1].0@ + batches@[i@ - 1].1@ + 1 })]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> batches@[j].1@ >= 0
        && w.log_start@ <= batches@[j].0@ && batches@[j].0@ + batches@[j].1@ + 1 <= w.log_end@
        && (j > 0 ==> batches@[j - 1].0@ + batches@[j - 1].1@ + 1 <= batches@[j].0@))]
    #[invariant(w.log_start@ <= candidate@ && candidate@ <= w.log_end@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        && !scheduled_activation_due(activations@[j]@, uncertainty@, now@)
        ==> candidate@ <= batches@[j].0@)]
    #[invariant(candidate@ == w.log_end@ || exists<j: Int> 0 <= j && j < i@
        && candidate@ == batches@[j].0@
        && !scheduled_activation_due(activations@[j]@, uncertainty@, now@))]
    #[variant(batches@.len() - i@)]
    while i < batches.len() {
        let (base, delta) = batches[i];
        let next = restore_batch_step(cursor, base, delta)?;
        if next > w.log_end {
            return None;
        }
        if !all_due
            && !scheduled_delivery_visible(true, uncertainty, activations[i], now)
            && base < candidate
        {
            candidate = base;
        }
        cursor = next;
        i += 1;
    }
    if cursor != w.log_end {
        return None;
    }
    let deliverable = delivery_watermark_advance(w.log_start, w.log_start, candidate, w.log_end);
    let bounded = FetchWatermarks { deliverable, ..w };
    let consumer = fetch_visibility(false, true, bounded, w.log_start);
    let follower = fetch_visibility(true, true, bounded, w.log_start);
    Some((deliverable, consumer, follower))
}

open_logic! {
/// Scheduled batches and activation times have equal length, and log bounds are coherent.
pub(super) fn scheduled_inputs_coherent(
    batches: Seq<(i64, i32)>,
    activations: Seq<i64>,
    start: Int,
    end: Int,
) -> bool {
    pearlite! { batches.len() == activations.len() && super::offset_range_valid(start, end) }
}
}
