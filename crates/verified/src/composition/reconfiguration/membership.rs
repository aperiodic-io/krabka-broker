#[cfg(creusot)]
use super::spec::{
    admitted_membership, has_node, prefix_node_extend, reconfiguration_admitted,
    single_change_shape,
};
use {
    crate::{
        reconfiguration::{
            CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter,
            VoterChangeKind, VoterChangeRequest, VoterReconfigurationDecision,
            VoterReconfigurationPlan, voter_reconfiguration_decision,
        },
        wal::wal_voter_set_valid,
    },
    creusot_std::prelude::*,
};

/// Apply an admitted one-voter plan to actual logical IDs. The old first ID
/// anchors the existing ID validator; it does not assert leadership or WAL
/// installation. The controller's `BTreeMap` gives this nonempty unique shape.
/// Current count/captured IDs must agree. Directory, catch-up and durability
/// facts remain host obligations; unknown-directory binding is not proof of
/// physical storage continuity.
#[requires(context.voter_count@ == old@.len())]
#[ensures((result != None) == (reconfiguration_admitted(old@, node, leadership, context, request, target)))]
#[ensures(match result { None => true, Some((plan, next)) =>
    admitted_membership(context, request.kind, plan, leadership, next@)
    && single_change_shape(old@, next@, request.kind, node)
    && match request.kind {
        VoterChangeKind::Add => next@.len() == old@.len() + 1,
        VoterChangeKind::Remove => next@.len() + 1 == old@.len(),
        VoterChangeKind::Update | VoterChangeKind::FinalizeKraftVersion => next@.len() == old@.len(),
    },
})]
pub(crate) fn constructed_voter_reconfiguration(
    old: &[u64],
    leadership: ReconfigurationLeadership,
    context: CurrentVoterSet,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
) -> Option<(VoterReconfigurationPlan, Vec<u64>)> {
    let &anchor = old.first()?;
    if !wal_voter_set_valid(old, anchor, old.len()) {
        return None;
    }
    let mut found = false;
    let mut i = 0usize;
    #[invariant(i@ <= old@.len())]
    #[invariant(found == has_node(old@, i@, node))]
    #[variant(old@.len() - i@)]
    while i < old.len() {
        proof_assert!({ prefix_node_extend(old@, i@); true });
        found = found || old[i] == node;
        i += 1;
    }
    if !matches!(request.kind, VoterChangeKind::FinalizeKraftVersion)
        && found == matches!(target.membership, TargetMembership::Absent)
    {
        return None;
    }
    let VoterReconfigurationDecision::Admit(plan) =
        voter_reconfiguration_decision(leadership, context, request, target)
    else {
        return None;
    };
    let remove = matches!(request.kind, VoterChangeKind::Remove);
    let add = matches!(request.kind, VoterChangeKind::Add);
    let mut next: Vec<u64> = Vec::new();
    i = 0;
    #[invariant(i@ <= old@.len())]
    #[invariant(next@.len() == i@ - if remove && has_node(old@, i@, node) { 1 } else { 0 })]
    #[invariant(forall<id: u64> has_node(next@, next@.len(), id)
        == (has_node(old@, i@, id) && (!remove || id != node)))]
    #[invariant(crate::sequence::distinct(next@))]
    #[variant(old@.len() - i@)]
    while i < old.len() {
        proof_assert!({ prefix_node_extend(old@, i@); true });
        if !remove || old[i] != node {
            proof_assert!(!has_node(old@, i@, old@[i@]));
            proof_assert!(!has_node(next@, next@.len(), old@[i@]));
            #[cfg(creusot)]
            let before = snapshot!(next@);
            next.push(old[i]);
            proof_assert!(forall<id: u64> has_node(next@, next@.len(), id)
                == (has_node(*before, before.len(), id) || old@[i@] == id));
        }
        i += 1;
    }
    if add {
        proof_assert!(!has_node(next@, next@.len(), node));
        #[cfg(creusot)]
        let before = snapshot!(next@);
        next.push(node);
        proof_assert!(forall<id: u64> has_node(next@, next@.len(), id)
            == (has_node(*before, before.len(), id) || node == id));
    }
    Some((plan, next))
}
