use {super::*, assert2::assert, std::collections::BTreeSet};

pub(super) fn grant_sets(
    old: &[u64],
    next: &[u64],
    node: u64,
    votes: &[(bool, bool)],
) -> (BTreeSet<u64>, BTreeSet<u64>) {
    let next_ids: BTreeSet<_> = next.iter().copied().collect();
    let old_grants = old
        .iter()
        .enumerate()
        .filter(|(i, _)| votes[*i].0)
        .map(|(_, id)| *id)
        .collect();
    let mut new_grants = old
        .iter()
        .enumerate()
        .filter(|(i, id)| votes[*i].1 && next_ids.contains(*id))
        .map(|(_, id)| *id)
        .collect::<BTreeSet<_>>();
    if !old.contains(&node) && next_ids.contains(&node) && votes[old.len()].1 {
        new_grants.insert(node);
    }
    (old_grants, new_grants)
}

pub(super) fn expected_membership(
    old: &[u64],
    leader: ReconfigurationLeadership,
    current: CurrentVoterSet,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
) -> Option<(VoterReconfigurationPlan, Vec<u64>)> {
    let ids: BTreeSet<_> = old.iter().copied().collect();
    let present = ids.contains(&node);
    let generic = !ids.is_empty()
        && ids.len() == old.len()
        && leader.is_leader
        && leader.no_pending_change
        && leader.epoch_committed
        && current.latest_controls_committed
        && current.kraft_version <= 1;
    let coherent = request.kind == VoterChangeKind::FinalizeKraftVersion
        || present == (target.membership != TargetMembership::Absent);
    let allowed = match request.kind {
        VoterChangeKind::Add => {
            current.kraft_version == 1 && !present && target.version_compatible && target.caught_up
        }
        VoterChangeKind::Remove => {
            current.kraft_version == 1
                && present
                && ids.len() > 1
                && target.membership == TargetMembership::PresentSameDirectory
        }
        VoterChangeKind::Update => {
            present
                && target.version_compatible
                && matches!(
                    target.membership,
                    TargetMembership::PresentSameDirectory
                        | TargetMembership::PresentUnknownDirectory
                )
        }
        VoterChangeKind::FinalizeKraftVersion => {
            current.kraft_version == 0
                && request.requested_kraft_version == 1
                && current.all_voters_support_requested
        }
    };
    if !(generic && coherent && allowed) {
        return None;
    }
    let mut next = old.to_vec();
    match request.kind {
        VoterChangeKind::Add => next.push(node),
        VoterChangeKind::Remove => next.retain(|id| *id != node),
        VoterChangeKind::Update | VoterChangeKind::FinalizeKraftVersion => {}
    }
    let finalize = request.kind == VoterChangeKind::FinalizeKraftVersion;
    let preflight = request.kind == VoterChangeKind::Update && current.kraft_version == 0;
    Some((
        VoterReconfigurationPlan {
            next_voter_count: next.len(),
            next_kraft_version: if finalize { 1 } else { current.kraft_version },
            write_voters: !preflight,
            write_kraft_version: finalize,
            preflight_only: preflight,
        },
        next,
    ))
}

pub(super) fn check_overlap(
    old: &[u64],
    leader: ReconfigurationLeadership,
    current: CurrentVoterSet,
    request: VoterChangeRequest,
    node: u64,
    target: TargetVoter,
    votes: &[(bool, bool)],
) {
    let membership = expected_membership(old, leader, current, request, node, target);
    assert!(
        constructed_voter_reconfiguration(old, leader, current, request, node, target)
            == membership
    );
    let expected = membership.map(|(plan, next)| {
        let (old_grants, new_grants) = grant_sets(old, &next, node, votes);
        let common = old
            .iter()
            .copied()
            .find(|id| old_grants.contains(id) && new_grants.contains(id));
        let counts = (old_grants.len(), new_grants.len());
        let quorums = (counts.0 > old.len() / 2, counts.1 > next.len() / 2);
        if quorums.0 && quorums.1 {
            assert!(common.is_some());
        }
        (plan, next, counts, quorums, common)
    });
    assert!(
        reconfigured_majorities_overlap(old, leader, current, request, node, target, votes)
            == expected
    );
}

pub(super) fn leading() -> ReconfigurationLeadership {
    ReconfigurationLeadership {
        is_leader: true,
        no_pending_change: true,
        epoch_committed: true,
    }
}
pub(super) fn current(n: usize, version: u16) -> CurrentVoterSet {
    CurrentVoterSet {
        voter_count: n,
        kraft_version: version,
        latest_controls_committed: true,
        all_voters_support_requested: true,
    }
}
pub(super) fn target(member: TargetMembership) -> TargetVoter {
    TargetVoter {
        membership: member,
        version_compatible: true,
        caught_up: true,
    }
}
