use creusot_std::prelude::*;

use super::{
    DeleteRecordsTrimApplication, DeleteRecordsTrimDecision, DeleteRecordsTrimFacts,
    ProducerReloadRange, delete_records_trim_application, delete_records_trim_decision,
    diskless_trim_decision, producer_snapshot_latest_index, producer_snapshot_replay_start,
};

open_logic! {
/// The durable stores have coherent frontiers within the trim visibility bounds.
pub fn trim_store_frontiers_valid(facts: DeleteRecordsTrimFacts, wal: Int, local: Int) -> bool {
    pearlite! {
        0 <= wal && wal <= facts.high_watermark@ && wal <= facts.log_end@
            && 0 <= local && local <= facts.high_watermark@ && local <= facts.log_end@
            && (facts.has_delivery_watermark ==> wal <= facts.delivery_watermark@ && local <= facts.delivery_watermark@)
    }
}
}

open_logic! {
/// The completed floor bounds the replay cursor within visibility caps.
pub(super) fn trim_cursor_bounded(facts: DeleteRecordsTrimFacts, stores: (Int, Int), floor: Int, cursor: Int) -> bool {
    pearlite! {
        floor == trim_frontier(facts).max(stores.0).max(stores.1)
            && 0 <= floor && floor <= cursor && cursor <= facts.log_end@
            && floor <= facts.high_watermark@
            && (!facts.has_delivery_watermark || floor <= facts.delivery_watermark@)
    }
}
}

/// Fold completed durable steps, including arbitrary pauses/failed attempts.
/// A true trace entry means the selected store reached the planned frontier;
/// it does not mean an RPC acknowledged it. I/O/atomic checkpointing are external.
/// One completed step catches WAL up; two catch both stores up. The global
/// frontier is fixed throughout, and replay after completion cannot advance it.
#[requires(requested@ >= 0 && wal_start@ >= 0 && local_start@ >= 0)]
#[ensures(wal_start@ <= result.0@ && local_start@ <= result.1@)]
#[ensures(result.1@ > local_start@ ==> result.0@ == requested@.max(wal_start@).max(local_start@))]
#[ensures(result.0@ <= requested@.max(wal_start@).max(local_start@)
    && result.1@ <= requested@.max(wal_start@).max(local_start@))]
#[ensures((exists<i: Int> 0 <= i && i < applied@.len() && applied@[i])
    ==> result.0@ == requested@.max(wal_start@).max(local_start@))]
#[ensures((exists<i: Int, j: Int> 0 <= i && i < j && j < applied@.len()
    && applied@[i] && applied@[j])
    ==> result.0 == result.1
        && result.1@ == requested@.max(wal_start@).max(local_start@))]
#[ensures((forall<i: Int> 0 <= i && i < applied@.len() ==> !applied@[i])
    ==> result == (wal_start, local_start))]
pub(super) fn trim_steps_converge(
    requested: i64,
    wal_start: i64,
    local_start: i64,
    applied: &[bool],
) -> (i64, i64) {
    let _frontier = requested.max(wal_start).max(local_start);
    let mut wal = wal_start;
    let mut local = local_start;
    let mut i = 0usize;
    #[invariant(i@ <= applied@.len())]
    #[invariant(wal_start@ <= wal@ && wal@ <= _frontier@)]
    #[invariant(local_start@ <= local@ && local@ <= _frontier@)]
    #[invariant(requested@.max(wal@).max(local@) == _frontier@)]
    #[invariant(local@ > local_start@ ==> wal == _frontier)]
    #[invariant((exists<j: Int> 0 <= j && j < i@ && applied@[j]) ==> wal == _frontier)]
    #[invariant((exists<j: Int, k: Int> 0 <= j && j < k && k < i@
        && applied@[j] && applied@[k]) ==> local == _frontier)]
    #[invariant((forall<j: Int> 0 <= j && j < i@ ==> !applied@[j])
        ==> wal == wal_start && local == local_start)]
    #[variant(applied@.len() - i@)]
    while i < applied.len() {
        if applied[i] {
            match delete_records_trim_application(requested, wal, local) {
                DeleteRecordsTrimApplication::TrimWal { frontier } => wal = frontier,
                DeleteRecordsTrimApplication::TrimLocal { frontier } => local = frontier,
                DeleteRecordsTrimApplication::Complete { .. } => {}
                DeleteRecordsTrimApplication::RejectMalformed => unreachable!(),
            }
        }
        i += 1;
    }
    (wal, local)
}

open_logic! {
pub fn trim_well_formed(facts: DeleteRecordsTrimFacts) -> bool {
    pearlite! { facts.requested@ >= -1 && facts.current_start@ >= 0
    && facts.current_start@ <= facts.high_watermark@ && facts.high_watermark@ <= facts.log_end@
    && (!facts.has_delivery_watermark || facts.current_start@ <= facts.delivery_watermark@) }
}
}

open_logic! {
/// Exact admission reason carried by every fallible trim composition.
pub(super) fn trim_rejection(
    facts: DeleteRecordsTrimFacts,
    error: DeleteRecordsTrimDecision,
) -> bool {
    pearlite! {
        match error {
            DeleteRecordsTrimDecision::RejectMalformed => !trim_well_formed(facts),
            DeleteRecordsTrimDecision::RejectOutOfRange => trim_well_formed(facts)
                && facts.requested@ != -1 && facts.requested@ > facts.high_watermark@,
            _ => false,
        }
    }
}
}

open_logic! {
pub fn trim_frontier(facts: DeleteRecordsTrimFacts) -> Int {
    pearlite! {
        let resolved = if facts.requested@ == -1 { facts.high_watermark@ } else { facts.requested@ };
        let bounded = if facts.has_delivery_watermark { resolved.min(facts.delivery_watermark@) } else { resolved };
        bounded.max(facts.current_start@)
    }
}
}

/// Admit logical deletion, complete reconciliation and return its actual floor,
/// latest retained snapshot index and exact replay cursor. Rejections preserve
/// their precise admission reason. Store frontiers must already obey the caps;
/// durable host completion, snapshot contents and whole-batch replay are external.
#[requires(0 <= wal_start@ && wal_start@ <= facts.high_watermark@ && wal_start@ <= facts.log_end@)]
#[requires(0 <= local_start@ && local_start@ <= facts.high_watermark@ && local_start@ <= facts.log_end@)]
#[requires(facts.has_delivery_watermark ==> wal_start@ <= facts.delivery_watermark@ && local_start@ <= facts.delivery_watermark@)]
#[ensures(match result {
    Err(error) => trim_rejection(facts, error),
    Ok((floor, selected, cursor)) => trim_well_formed(facts)
        && (facts.requested@ == -1 || facts.requested@ <= facts.high_watermark@)
        && trim_cursor_bounded(facts, (wal_start@, local_start@), floor@, cursor@)
        && match selected {
            None => trim_has_no_snapshot(snapshots@, floor, facts.log_end, cursor),
            Some(index) => index@ < snapshots@.len() && floor@ < snapshots@[index@]@ && snapshots@[index@]@ <= facts.log_end@
                && cursor == snapshots@[index@]
                && latest_retained_snapshot(snapshots@, floor@, facts.log_end@, snapshots@[index@]@),
        },
})]
pub(super) fn admitted_trim_bounds_reload_and_retry(
    facts: DeleteRecordsTrimFacts,
    wal_start: i64,
    local_start: i64,
    snapshots: &[i64],
) -> Result<(i64, Option<usize>, i64), DeleteRecordsTrimDecision> {
    let target = match delete_records_trim_decision(facts) {
        DeleteRecordsTrimDecision::Apply { frontier }
        | DeleteRecordsTrimDecision::Noop { frontier } => frontier,
        error => return Err(error),
    };
    let (wal, local) = trim_steps_converge(target, wal_start, local_start, &[true, true]);
    let _retry = delete_records_trim_decision(DeleteRecordsTrimFacts {
        current_start: local,
        ..facts
    });
    let _application = delete_records_trim_application(target, wal, local);
    proof_assert!(
        _retry == DeleteRecordsTrimDecision::Noop { frontier: local }
            && _application == DeleteRecordsTrimApplication::Complete { frontier: local }
    );
    let range = ProducerReloadRange {
        log_start: local,
        local_start: local,
        log_end: facts.log_end,
    };
    let selected = producer_snapshot_latest_index(snapshots, range);
    let snapshot = selected.map(|index| snapshots[index]);
    let cursor = producer_snapshot_replay_start(range, snapshot)
        .expect("completed trim preserves reload bounds");
    Ok((local, selected, cursor))
}

/// Return the actual physical WAL/cache frontiers after the eviction trace.
/// A disabled plan returns unchanged frontiers, including an inherited invalid
/// cache start; it cannot claim that unrelated inherited state is newly safe.
/// Enabled plans preserve object/HWM caps, progress and exact completion.
/// Physical eviction does not advance the logical deletion floor; durable
/// checkpoint publication and committed object coverage remain host obligations.
#[requires(0 <= wal_start@ && wal_start@ <= indexed_frontier@)]
#[requires(wal_start@ + (if safety_lag@ < 0 { 0 } else { safety_lag@ }) <= high_watermark@)]
#[ensures(result.0@ >= wal_start@)]
#[ensures(result.1@ >= local_start@)]
#[ensures({
    let lag = safety_lag@.max(0);
    let enabled = local_start@ >= 0 && local_start@ < indexed_frontier@.min(high_watermark@ - lag);
    if !enabled { result == (wal_start, local_start) }
    else { let frontier = indexed_frontier@.min(high_watermark@ - lag).max(wal_start@);
        result.0@ <= frontier && result.1@ <= frontier
        && result.0@ <= indexed_frontier@ && result.1@ <= indexed_frontier@
        && result.0@ + lag <= high_watermark@ && result.1@ + lag <= high_watermark@
        && ((exists<i: Int> 0 <= i && i < applied@.len() && applied@[i]) ==> result.0@ == frontier)
        && ((exists<i: Int, j: Int> 0 <= i && i < j && j < applied@.len() && applied@[i] && applied@[j])
            ==> result.0@ == frontier && result.1@ == frontier)
        && ((forall<i: Int> 0 <= i && i < applied@.len() ==> !applied@[i]) ==> result == (wal_start, local_start))
    }
})]
pub(super) fn diskless_trim_reconciliation_preserves_coverage(
    indexed_frontier: i64,
    high_watermark: i64,
    safety_lag: i64,
    wal_start: i64,
    local_start: i64,
    applied: &[bool],
) -> (i64, i64) {
    let plan = diskless_trim_decision(indexed_frontier, high_watermark, safety_lag, local_start);
    if !plan.should_trim {
        return (wal_start, local_start);
    }
    trim_steps_converge(plan.target, wal_start, local_start, applied)
}

open_logic! {
/// The floor is the replay cursor exactly when the retained window has no snapshot.
pub(super) fn trim_has_no_snapshot(snapshots: Seq<i64>, floor: i64, end: i64, cursor: i64) -> bool {
    pearlite! { cursor == floor && forall<i: Int> 0 <= i && i < snapshots.len()
    ==> !(floor@ < snapshots[i]@ && snapshots[i]@ <= end@) }
}
}

open_logic! {
/// No snapshot in the retained window lies above the selected replay frontier.
pub(super) fn latest_retained_snapshot(
    snapshots: Seq<i64>,
    floor: Int,
    end: Int,
    selected: Int,
) -> bool {
    pearlite! { forall<i: Int> 0 <= i && i < snapshots.len() && floor < snapshots[i]@ && snapshots[i]@ <= end
    ==> snapshots[i]@ <= selected }
}
}
