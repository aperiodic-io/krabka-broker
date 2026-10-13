use {
    super::{constructed_wal_placement_is_installable, wal_placement_survives_one_rack_loss},
    assert2::assert,
    proptest::prelude::*,
    std::collections::HashSet,
};

fn placement_oracle(candidates: &[(u64, u64)], local: u64, requested: usize) -> Vec<(u64, u64)> {
    if requested == 0 {
        return Vec::new();
    }
    let Some(&first) = candidates.iter().find(|(node, _)| *node == local) else {
        return Vec::new();
    };
    let mut selected = std::vec![first];
    let mut nodes = HashSet::from([first.0]);
    let mut racks = HashSet::from([first.1]);
    for &(node, rack) in candidates {
        if selected.len() == requested {
            break;
        }
        if !nodes.contains(&node) && !racks.contains(&rack) {
            selected.push((node, rack));
            nodes.insert(node);
            racks.insert(rack);
        }
    }
    selected
}

fn check_placement(candidates: &[(u64, u64)], local: u64, requested: usize, failed: u64) {
    let selected = placement_oracle(candidates, local, requested);
    let nodes: Vec<_> = selected.iter().map(|(node, _)| *node).collect();
    let admitted = requested != 0 && selected.len() == requested;
    assert!(
        constructed_wal_placement_is_installable(candidates, local, requested)
            == (selected.clone(), nodes, admitted)
    );
    if requested >= 3 {
        let survivors: Vec<_> = selected
            .iter()
            .filter(|(_, rack)| *rack != failed)
            .map(|(node, _)| *node)
            .collect();
        let quorum = admitted && survivors.len() > requested / 2;
        assert!(
            wal_placement_survives_one_rack_loss(candidates, local, requested, failed)
                == (selected.clone(), survivors.clone(), admitted, quorum)
        );
        assert!(survivors.iter().copied().collect::<HashSet<_>>().len() == survivors.len());
        assert!(!admitted || quorum);
    }
}

proptest! {
    #[test]
    fn exported_placement_and_survivors_match_independent_sets(
        candidates in proptest::collection::vec((0u64..8, 0u64..8), 0..32),
        local in 0u64..8,
        requested in 0usize..10,
        failed_rack in 0u64..8,
    ) {
        check_placement(&candidates, local, requested, failed_rack);
    }
}

#[test]
fn placement_witness_covers_local_loss_duplicates_and_incomplete_configs() {
    let candidates = [(1, 10), (1, 20), (2, 10), (3, 20), (4, 30), (4, 40)];
    for requested in [0, 1, 2, 3, 4, usize::MAX] {
        for local in [1, 99] {
            for failed in [10, 20, 30, 40, u64::MAX] {
                check_placement(&candidates, local, requested, failed);
                check_placement(&[], local, requested, failed);
            }
        }
    }
    let extremes = [(u64::MAX, u64::MAX), (0, 0), (1, 1)];
    check_placement(&extremes, u64::MAX, 3, u64::MAX);
    // Maximal greedy admission need not find a globally maximum placement
    // when conflicting metadata rows give one broker several rack labels.
    assert!(
        constructed_wal_placement_is_installable(&[(1, 10), (1, 20), (2, 10)], 1, 2)
            == (std::vec![(1, 10)], std::vec![1], false)
    );
    // Even if every selected node survives, an incomplete installation stays
    // rejected instead of claiming availability from a smaller voter majority.
    assert!(
        wal_placement_survives_one_rack_loss(&candidates, 1, 4, 99)
            == (
                std::vec![(1, 10), (3, 20), (4, 30)],
                std::vec![1, 3, 4],
                false,
                false
            )
    );
}
