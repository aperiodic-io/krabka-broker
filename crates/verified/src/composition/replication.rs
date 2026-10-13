use {
    super::{
        FetchWatermarks, ReplicaFetchFacts, ReplicaFetchMutation, advance_high_watermark,
        fetch_visibility, local_append_coordinates, replica_fetch_mutation, truncation_frontier,
    },
    creusot_std::prelude::*,
};

/// A fenced/error replica response leaves the log end and HWM unchanged.
/// Divergence can only shrink them; admitted append coordinates plus monotone
/// HWM advancement keep consumer Fetch inside the new local log.
#[requires(w.hw@ <= w.log_end@)]
#[ensures(result.1@ <= result.0@ && result.2@ <= result.1@)]
#[ensures(if crate::broker::replica_fetch_fenced(facts) || facts.error_code@ != 0 {
    result.0 == w.log_end && result.1 == w.hw
} else { true })]
#[ensures(facts.diverging_epoch@ >= 0
    ==> result.0@ <= w.log_end@ && result.1@ <= w.hw@)]
#[ensures(result.0@ > w.log_end@ ==> !crate::broker::replica_fetch_fenced(facts)
    && facts.error_code@ == 0 && facts.diverging_epoch@ < 0
    && result.0@ == supplied_base@ + delta@ + 1)]
pub(super) fn fenced_replication_bounds_fetch(
    facts: ReplicaFetchFacts,
    diverging_end_offset: i64,
    supplied_base: i64,
    delta: i32,
    reported_hw: i64,
    w: FetchWatermarks,
) -> (i64, i64, i64) {
    let (end, hw) = match replica_fetch_mutation(facts) {
        ReplicaFetchMutation::Truncate => {
            let end = truncation_frontier(w.log_end, diverging_end_offset);
            (end, truncation_frontier(w.hw, end))
        }
        ReplicaFetchMutation::Append => {
            match local_append_coordinates(w.log_end, supplied_base, delta) {
                Some((_, next)) => (next, advance_high_watermark(w.hw, reported_hw, next)),
                None => (w.log_end, w.hw),
            }
        }
        ReplicaFetchMutation::Reject | ReplicaFetchMutation::Retry => (w.log_end, w.hw),
    };
    let visibility = fetch_visibility(
        false,
        true,
        FetchWatermarks {
            log_end: end,
            hw,
            ..w
        },
        w.log_start,
    );
    (end, hw, visibility.limit_offset)
}
