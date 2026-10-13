use {super::*, assert2::assert, std::collections::BTreeSet};

proptest! {
    #[test]
    fn changed_quorums_match_actual_set_intersections(
        input in request_cases(),
        ballots in prop::collection::vec((any::<bool>(), any::<bool>()), 11),
    ) {
        let (old, node, (leader, context, request, target)) = input;
        check_overlap(&old, leader, context, request, node, target, &ballots[..=old.len()]);
    }
}

#[test]
fn all_small_majority_pairs_have_a_real_shared_voter() {
    for n in 1..=5 {
        let old: Vec<_> = (0..n).rev().map(|id| u64::try_from(id).unwrap()).collect();
        for (kind, node, membership, version) in [
            (VoterChangeKind::Add, u64::MAX, TargetMembership::Absent, 1),
            (
                VoterChangeKind::Remove,
                0,
                TargetMembership::PresentSameDirectory,
                1,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentUnknownDirectory,
                0,
            ),
            (
                VoterChangeKind::Update,
                0,
                TargetMembership::PresentSameDirectory,
                1,
            ),
            (
                VoterChangeKind::FinalizeKraftVersion,
                0,
                TargetMembership::Absent,
                0,
            ),
        ] {
            for mask in 0..(1usize << (2 * (n + 1))) {
                let votes: Vec<_> = (0..=n)
                    .map(|i| (mask & (1 << (2 * i)) != 0, mask & (1 << (2 * i + 1)) != 0))
                    .collect();
                check_overlap(
                    &old,
                    leading(),
                    current(n, version),
                    VoterChangeRequest {
                        kind,
                        requested_kraft_version: 1,
                    },
                    node,
                    target(membership),
                    &votes,
                );
            }
        }
    }
}

#[test]
fn shape_and_single_change_bounds_are_real() {
    let request = VoterChangeRequest {
        kind: VoterChangeKind::Add,
        requested_kraft_version: 1,
    };
    for old in [&[][..], &[7, 7], &[7, 1, 4]] {
        for node in [1, 99] {
            for membership in [
                TargetMembership::Absent,
                TargetMembership::PresentSameDirectory,
            ] {
                let votes = std::vec![(true, true); old.len() + 1];
                check_overlap(
                    old,
                    leading(),
                    current(old.len(), 1),
                    request,
                    node,
                    target(membership),
                    &votes,
                );
            }
        }
    }
    // Two additions can separate non-adjacent majority sets. Single-step
    // intersection does not prove safety of overlapping uncommitted changes.
    let old: BTreeSet<_> = [0, 1, 2].into_iter().collect();
    let next: BTreeSet<_> = [0, 1, 2, 3, 4].into_iter().collect();
    let old_grants: BTreeSet<_> = [0, 1].into_iter().collect();
    let new_grants: BTreeSet<_> = [2, 3, 4].into_iter().collect();
    assert!(old_grants.len() > old.len() / 2 && new_grants.len() > next.len() / 2);
    assert!(old_grants.is_subset(&old) && new_grants.is_subset(&next));
    assert!(old_grants.is_disjoint(&new_grants));
}
