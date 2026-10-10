use super::*;

/// Commit the actual supported control rows with the controller's half-open
/// history and waiter kernels. Equal old/new voter values cannot replace the
/// final row's offset. Progress is (append base, previous HWM, requested HWM,
/// actual log end).
/// No-append preflight consumes none of those coordinates.
#[requires(control_inputs_coherent(old@, state.1, reports@))]
#[ensures((result != None) == control_supported_request(old@, state, request, (node, target),
    (progress.0@ >= 0 && 0 <= progress.1@ && progress.1@ <= progress.3@
            && progress.0@ + control_record_count(state.1.kraft_version, request.kind) <= progress.3@
            && control_prefix_majorities(old@, reports@, request.kind, node,
                progress.0@ + control_record_count(state.1.kraft_version, request.kind)))))]
#[ensures(match result { None => true, Some((plan, next, rows, frontier)) =>
    admitted_plan(state.1, request.kind, plan)
    && next@.len() == plan.next_voter_count@
    && (membership_matches_change(old@, next@, request.kind, node))
    && rows@.len() == control_record_count(state.1.kraft_version, request.kind)
    && (frontier == None) == plan.preflight_only
    && match frontier { None => rows@.len() == 0,
        Some((end, hwm, prefix, ready, common)) =>
            end@ == progress.0@ + rows@.len() && progress.0@ < end@
            && 0 <= progress.1@ && progress.1@ <= progress.3@ && end@ <= progress.3@
            && progress.1@ <= hwm@ && hwm@ <= progress.3@
            && hwm@ == progress.1@.max(progress.2@.min(progress.3@))
            && prefix@ <= rows@.len()
            && ready == (prefix@ == rows@.len())
            && ready == (hwm@ >= end@)
            && ready == (progress.1@ >= end@ || progress.2@ >= end@)
            && (forall<i: Int> 0 <= i && i < rows@.len() ==>
                rows@[i].0@ == progress.0@ + i
                && rows@[i].1 == (plan.write_kraft_version && i == 0)
                && rows@[i].2 == (i < prefix@)
                && rows@[i].2 == (rows@[i].0@ < hwm@))
            && common_prefix_reported(old@, reports@, next@, common, end@),
    },
})]
pub(crate) fn reconfiguration_control_commit_waiter(
    old: &[u64],
    state: ReconfigurationState,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
    reports: &[(i64, i64)],
    progress: (i64, i64, i64, i64),
) -> CommittedControl {
    let (base, previous, requested, log_end) = progress;
    let (plan, next, deltas, support) =
        reconfiguration_control_prefix_support(old, state, request, node, target, base, reports)?;
    let Some((end, _, common)) = support else {
        return Some((plan, next, Vec::new(), None));
    };
    if previous < 0 || previous > log_end || end > log_end {
        return None;
    }
    let hwm = advance_high_watermark(previous, requested, log_end);
    let mut offsets: Vec<i64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= deltas@.len() && offsets@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> offsets@[j]@ == base@ + j)]
    #[variant(deltas@.len() - i@)]
    while i < deltas.len() {
        offsets.push(base + i64::from(deltas[i]));
        i += 1;
    }
    let prefix = control_history_frontier(&offsets, hwm);
    proof_assert!(offsets@.len() > 0 && offsets@[offsets@.len() - 1]@ == end@ - 1);
    proof_assert!(prefix@ == offsets@.len() ==> end@ <= hwm@);
    proof_assert!(prefix@ < offsets@.len() ==> hwm@ < end@);
    let ready = frontier_reaches(hwm, end);
    let mut rows: Vec<(i64, bool, bool)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= offsets@.len() && rows@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        rows@[j].0@ == base@ + j && rows@[j].1 == (plan.write_kraft_version && j == 0)
        && rows@[j].2 == (j < prefix@) && rows@[j].2 == (rows@[j].0@ < hwm@))]
    #[variant(offsets@.len() - i@)]
    while i < offsets.len() {
        let committed = in_half_open_window(offsets[i], 0, hwm);
        rows.push((offsets[i], plan.write_kraft_version && i == 0, committed));
        i += 1;
    }
    Some((plan, next, rows, Some((end, hwm, prefix, ready, common))))
}
