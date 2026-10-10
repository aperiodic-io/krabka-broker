use super::*;

/// Construct real control-record deltas from the admitted plan and derive
/// old/new support from each node's reported prefix, never a supplied grant
/// count. Preflight writes nothing and needs no append support. Reported
/// prefixes, common log identity, directory/epoch facts and actual control
/// encoding remain host obligations. `KRaft` Fetch positions do not prove fsync.
#[requires(control_inputs_coherent(old@, state.1, reports@))]
#[ensures((result != None) == control_supported_request(old@, state, request, (node, target),
    (base@ >= 0 && base@ + control_record_count(state.1.kraft_version, request.kind) <= i64::MAX@
            && control_prefix_majorities(old@, reports@, request.kind, node,
                base@ + control_record_count(state.1.kraft_version, request.kind)))))]
#[ensures(match result { None => true, Some((plan, next, deltas, support)) =>
    admitted_plan(state.1, request.kind, plan)
    && next@.len() == plan.next_voter_count@ && next@.len() > 0
    && (membership_matches_change(old@, next@, request.kind, node))
    && (crate::sequence::distinct(next@))
    && deltas@.len() == control_record_count(state.1.kraft_version, request.kind)
    && (forall<i: Int> 0 <= i && i < deltas@.len() ==> deltas@[i]@ == i)
    && (support == None) == (deltas@.len() == 0)
    && match support { None => plan.preflight_only,
        Some((end, counts, common)) => !plan.preflight_only && base@ >= 0
            && end@ == base@ + deltas@.len() && base@ < end@
            && counts.0@ <= old@.len() && counts.1@ <= next@.len()
            && counts.0@ == prefix_count(old@, reports@, request.kind, node, end@, false, reports@.len())
            && counts.1@ == prefix_count(old@, reports@, request.kind, node, end@, true, reports@.len())
            && counts.0@ >= old@.len() / 2 + 1 && counts.1@ >= next@.len() / 2 + 1
            && common_prefix_reported(old@, reports@, next@, common, end@)
                && forall<j: Int> 0 <= j && j < deltas@.len()
                    ==> base@ + deltas@[j]@ < end@,
    },
})]
pub(crate) fn reconfiguration_control_prefix_support(
    old: &[u64],
    state: ReconfigurationState,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) -> SupportedControl {
    let (plan, next) =
        constructed_voter_reconfiguration(old, state.0, state.1, request, node, target)?;
    if plan.preflight_only {
        return Some((plan, next, Vec::new(), None));
    }
    let mut records: Vec<()> = Vec::new();
    if plan.write_kraft_version {
        records.push(());
    }
    if plan.write_voters {
        records.push(());
    }
    let deltas = metadata_record_offset_deltas(records.len())?;
    let &last = deltas.last()?;
    let (_, end) = local_append_coordinates(base, base, last)?;
    let mut votes: Vec<(bool, bool)> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= reports@.len() && votes@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        votes@[j].0 == (reports@[j].0@ >= end@) && votes@[j].1 == (reports@[j].1@ >= end@))]
    #[variant(reports@.len() - i@)]
    while i < reports.len() {
        votes.push((reports[i].0 >= end, reports[i].1 >= end));
        i += 1;
    }
    let (plan, next, counts, quorums, common) =
        reconfigured_majorities_overlap(old, state.0, state.1, request, node, target, &votes)?;
    proof_assert!({
        prefix_grants_agree(old@, next@, votes@, reports@, request.kind, node, end@, false, votes@.len());
        prefix_grants_agree(old@, next@, votes@, reports@, request.kind, node, end@, true, votes@.len());
        true
    });
    if !quorums.0 || !quorums.1 {
        return None;
    }
    let common = common?;
    Some((plan, next, deltas, Some((end, counts, common))))
}
