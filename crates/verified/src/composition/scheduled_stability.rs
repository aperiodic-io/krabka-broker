use creusot_std::prelude::*;

#[cfg(creusot)]
use super::delivery::{
    scheduled_activation_due, scheduled_batches_valid, scheduled_inputs_coherent,
    scheduled_prefix_covers_pending,
};
use super::{
    FetchWatermarks, committed_fetch_excludes_unstable, fetch_visibility,
    scheduled_prefix_bounds_fetch,
};
use crate::broker::FetchVisibility;
#[cfg(creusot)]
use crate::transaction::pending_starts_bound_fetch;

type ScheduledStablePrefix = (i64, i64, FetchVisibility, FetchVisibility);

/// Derive both delivery and transaction frontiers from coherent complete
/// inputs, then expose the greatest consumer prefix allowed by both gates.
/// Replication remains ungated. Cached input LSO/delivery fields are ignored;
/// decoded bytes, cached-cursor invalidation and input completeness are external.
#[requires(scheduled_inputs_coherent(batches@, activations@, w.log_start@, w.log_end@))]
#[ensures(match result {
    None => !scheduled_batches_valid(batches@, w.log_start@, w.log_end@)
        || exists<i: Int> 0 <= i && i < starts@.len() && starts@[i]@ > w.log_end@,
    Some((delivery, lso, consumer, follower)) => scheduled_batches_valid(batches@, w.log_start@, w.log_end@)
        && w.log_start@ <= delivery@ && delivery@ <= w.log_end@ && lso@ <= w.log_end@
        && scheduled_prefix_covers_pending(batches@, activations@, (uncertainty@, now@), delivery@)
        && scheduled_prefix_covers_pending(batches@, activations@, (uncertainty@, now@), consumer.limit_offset@)
        && (delivery@ == w.log_end@ || exists<i: Int> 0 <= i && i < batches@.len()
            && delivery@ == batches@[i].0@
            && !scheduled_activation_due(activations@[i]@, uncertainty@, now@))
        && (pending_starts_bound_fetch(starts@, w.log_end@, lso@, consumer.limit_offset@))
        && ((starts@.len() == 0 && lso@ == w.log_end@)
            || exists<i: Int> 0 <= i && i < starts@.len() && lso@ == starts@[i]@)
        && consumer.limit_offset@ == w.hw@.min(lso@).min(delivery@)
        && crate::broker::committed_fetch_response(consumer, w.hw, lso, delivery, w.log_start@)
        && follower.limit_offset == w.log_end && follower.response_hw == w.hw
        && follower.response_lso@ == w.hw@.min(lso@) && follower.effective_lso == lso
        && !follower.read_committed_aborts && !follower.out_of_range
        && follower.empty == (w.log_start == w.log_end),
})]
#[ensures(match result {
    None => true,
    Some((_, _, consumer, _)) => forall<v: Int> v <= w.log_end@ && v <= w.hw@
        && (forall<i: Int> 0 <= i && i < starts@.len() ==> v <= starts@[i]@)
        && (forall<i: Int> 0 <= i && i < batches@.len()
            && !scheduled_activation_due(activations@[i]@, uncertainty@, now@)
            ==> v <= batches@[i].0@)
        ==> v <= consumer.limit_offset@,
})]
pub(super) fn scheduled_stable_prefix_bounds_fetch(
    batches: &[(i64, i32)],
    activations: &[i64],
    starts: &[i64],
    uncertainty: i64,
    now: i64,
    w: FetchWatermarks,
) -> Option<ScheduledStablePrefix> {
    let (delivery, _, _) =
        scheduled_prefix_bounds_fetch(batches, activations, uncertainty, now, w)?;
    let (lso, consumer) = committed_fetch_excludes_unstable(
        starts,
        FetchWatermarks {
            deliverable: delivery,
            ..w
        },
    )?;
    let follower = fetch_visibility(
        true,
        true,
        FetchWatermarks {
            lso,
            deliverable: delivery,
            ..w
        },
        w.log_start,
    );
    Some((delivery, lso, consumer, follower))
}
