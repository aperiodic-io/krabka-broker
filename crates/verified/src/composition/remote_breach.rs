#[cfg(creusot)]
use super::trim::trim_rejection;
#[cfg(creusot)]
use super::trim::{trim_frontier, trim_store_frontiers_valid, trim_well_formed};
use {
    super::{
        DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
        delete_records_trim_application, delete_records_trim_decision, trim::trim_steps_converge,
    },
    crate::retention::{RemoteRetentionSegment, remote_retention_prefix},
    creusot_std::prelude::*,
};

type RemoteBreachPlan = (
    i64,
    (i64, i64),
    Option<i64>,
    Vec<RemoteRetentionSegment>,
    usize,
);

/// Admit logical trim, consume completed store steps and successful checkpoint
/// publication, then derive remote log-start breaches from that checkpoint.
/// With time/size retention disabled, cleanup is exactly the oldest whole-row
/// prefix below the published floor. Store progress alone cannot authorize it.
/// Trace/publication observations mean durable completion; truthful metadata,
/// actual I/O and atomic publication remain host obligations.
#[requires(trim_store_frontiers_valid(facts, stores.0@, stores.1@))]
#[requires(match previous { None => true, Some(floor) => 0 <= floor@ && floor@ <= stores.0@ && floor@ <= stores.1@ })]
#[requires(super::finished_ranges_valid(finished@.len(), |i: Int| (finished@[i].0, finished@[i].1)))]
#[requires(forall<i: Int> 0 <= i && i < finished@.len() ==> !finished@[i].3)]
#[ensures(match result {
    Err(error) => trim_rejection(facts, error),
    Ok(plan) => {
        let canonical = trim_frontier(facts).max(stores.0@).max(stores.1@);
        trim_well_formed(facts) && (facts.requested@ == -1 || facts.requested@ <= facts.high_watermark@)
            && plan.0@ == canonical && 0 <= canonical && canonical <= facts.high_watermark@
            && canonical <= facts.log_end@ && (!facts.has_delivery_watermark || canonical <= facts.delivery_watermark@)
            && stores.0@ <= plan.1.0@ && plan.1.0@ <= canonical
            && stores.1@ <= plan.1.1@ && plan.1.1@ <= canonical
            && (plan.1.1@ > stores.1@ ==> plan.1.0@ == canonical)
            && ((forall<i: Int> 0 <= i && i < applied@.len() ==> !applied@[i]) ==> plan.1 == stores)
            && ((exists<i: Int, j: Int> 0 <= i && i < j && j < applied@.len() && applied@[i] && applied@[j])
                ==> plan.1.0@ == canonical && plan.1.1@ == canonical)
            && plan.2 == if published && plan.1.0@ == canonical && plan.1.1@ == canonical { Some(plan.0) } else { previous }
            && (forall<old: i64> previous == Some(old) ==> exists<next: i64> plan.2 == Some(next) && old@ <= next@)
            && plan.3@.len() == finished@.len() && plan.4@ <= finished@.len()
            && (forall<i: Int> 0 <= i && i < finished@.len()
                ==> plan.3@[i].size == finished@[i].2 && !plan.3@[i].time_expired
                    && plan.3@[i].log_start_breached == match plan.2 { None => false, Some(floor) => finished@[i].1@ < floor@ })
            && (!deletes_allowed ==> plan.4@ == 0)
            && match plan.2 {
                None => plan.4@ == 0,
                Some(floor) => 0 <= floor@ && floor@ <= canonical
                    && (forall<i: Int> 0 <= i && i < plan.4@ ==> finished@[i].1@ < floor@)
                    && (deletes_allowed && plan.4@ < finished@.len() ==> finished@[plan.4@].1@ >= floor@)
                    && (forall<n: Int> deletes_allowed && 0 <= n && n <= finished@.len()
                        && (forall<i: Int> 0 <= i && i < n ==> finished@[i].1@ < floor@) ==> n <= plan.4@),
            }
    },
})]
pub(super) fn published_trim_bounds_remote_breach_cleanup(
    facts: DeleteRecordsTrimFacts,
    stores: (i64, i64),
    previous: Option<i64>,
    applied: &[bool],
    published: bool,
    deletes_allowed: bool,
    finished: &[(i64, i64, u64, bool)],
) -> Result<RemoteBreachPlan, DeleteRecordsTrimDecision> {
    let accepted = match delete_records_trim_decision(facts) {
        DeleteRecordsTrimDecision::Apply { frontier }
        | DeleteRecordsTrimDecision::Noop { frontier } => frontier,
        error => return Err(error),
    };
    let canonical = accepted.max(stores.0).max(stores.1);
    let observed = trim_steps_converge(accepted, stores.0, stores.1, applied);
    let complete = matches!(
        delete_records_trim_application(accepted, observed.0, observed.1),
        DeleteRecordsTrimApplication::Complete { .. }
    );
    proof_assert!(complete == (observed.0 == canonical && observed.1 == canonical));
    let checkpoint = if published && complete {
        Some(canonical)
    } else {
        previous
    };
    let facts = super::remote_retention_facts(finished, checkpoint);
    let count = remote_retention_prefix(deletes_allowed, &facts, 0);
    proof_assert!(forall<j: Int> 0 <= j && j < count@ ==> facts@[j].log_start_breached);
    proof_assert!(deletes_allowed && count@ < finished@.len() ==> !facts@[count@].log_start_breached);
    Ok((canonical, observed, checkpoint, facts, count))
}
