use {
    super::{
        oracle::{expected_membership, grant_sets},
        *,
    },
    crate::{
        composition::reconfiguration::SupportedControl, reconfiguration::ReconfigurationState,
    },
    assert2::assert,
};

pub(super) fn expected_control(
    old: &[u64],
    state: ReconfigurationState,
    request: VoterChangeRequest,
    node: u64,
    candidate: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) -> SupportedControl {
    expected_membership(old, state.0, state.1, request, node, candidate).and_then(|(plan, next)| {
        let count = usize::from(plan.write_kraft_version) + usize::from(plan.write_voters);
        let deltas: Vec<_> = (0..count).map(|i| i32::try_from(i).unwrap()).collect();
        if count == 0 {
            return Some((plan, next, deltas, None));
        }
        let end = i128::from(base) + i128::try_from(count).unwrap();
        if base < 0 || end > i128::from(i64::MAX) {
            return None;
        }
        let end = i64::try_from(end).unwrap();
        let votes: Vec<_> = reports.iter().map(|r| (r.0 >= end, r.1 >= end)).collect();
        let (old_grants, new_grants) = grant_sets(old, &next, node, &votes);
        let counts = (old_grants.len(), new_grants.len());
        if counts.0 <= old.len() / 2 || counts.1 <= next.len() / 2 {
            return None;
        }
        let common = old
            .iter()
            .copied()
            .find(|id| old_grants.contains(id) && new_grants.contains(id))
            .unwrap();
        Some((plan, next, deltas, Some((end, counts, common))))
    })
}

fn check_control(
    old: &[u64],
    state: ReconfigurationState,
    request: VoterChangeRequest,
    node: u64,
    candidate: TargetVoter,
    base: i64,
    reports: &[(i64, i64)],
) {
    assert!(
        reconfiguration_control_prefix_support(old, state, request, node, candidate, base, reports)
            == expected_control(old, state, request, node, candidate, base, reports)
    );
}

proptest! {
    #[test]
    fn control_support_matches_actual_prefix_sets(
        input in request_cases(), base in any::<i64>(),
        reports in prefix_report_cases(),
    ) {
        let (old, node, (leader, context, request, candidate)) = input;
        check_control(&old, (leader, context), request, node, candidate, base, &reports[..=old.len()]);
    }
}

#[test]
fn complete_control_batches_need_both_actual_prefix_majorities() {
    for n in 1..=5 {
        let old: Vec<_> = (0..n).rev().map(|i| u64::try_from(i).unwrap()).collect();
        for (kind, node, member, version, count) in [
            (
                VoterChangeKind::Add,
                u64::MAX,
                TargetMembership::Absent,
                1,
                1,
            ),
            (
                VoterChangeKind::Remove,
                0,
                TargetMembership::PresentSameDirectory,
                1,
                1,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentSameDirectory,
                1,
                1,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentUnknownDirectory,
                0,
                0,
            ),
            (
                VoterChangeKind::FinalizeKraftVersion,
                0,
                TargetMembership::Absent,
                0,
                2,
            ),
        ] {
            let end = 13 + count;
            for mask in 0..(1usize << (2 * (n + 1))) {
                let reports: Vec<_> = (0..=n)
                    .map(|i| {
                        (
                            if mask & (1 << (2 * i)) != 0 {
                                end
                            } else {
                                end - 1
                            },
                            if mask & (1 << (2 * i + 1)) != 0 {
                                end
                            } else {
                                end - 1
                            },
                        )
                    })
                    .collect();
                check_control(
                    &old,
                    (leading(), current(n, version)),
                    VoterChangeRequest {
                        kind,
                        requested_kraft_version: 1,
                    },
                    node,
                    target(member),
                    13,
                    &reports,
                );
            }
        }
    }
}

#[test]
fn preflight_has_no_append_and_successor_exhaustion_cannot_alias_a_record() {
    for (kind, node, member, version) in [
        (VoterChangeKind::Add, 3, TargetMembership::Absent, 1),
        (
            VoterChangeKind::Update,
            1,
            TargetMembership::PresentUnknownDirectory,
            0,
        ),
        (
            VoterChangeKind::FinalizeKraftVersion,
            1,
            TargetMembership::Absent,
            0,
        ),
    ] {
        for base in [-1, 0, i64::MAX - 2, i64::MAX - 1, i64::MAX] {
            for reports in [[(i64::MAX, i64::MAX); 4], [(-1, -1); 4]] {
                check_control(
                    &[1, 2, 0],
                    (leading(), current(3, version)),
                    VoterChangeRequest {
                        kind,
                        requested_kraft_version: 1,
                    },
                    node,
                    target(member),
                    base,
                    &reports,
                );
            }
        }
    }
}
