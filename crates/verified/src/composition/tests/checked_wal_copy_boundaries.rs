use {super::*, assert2::assert};

fn check_two_durable_followers(reported: &[i64], watermarks: FetchWatermarks) {
    assert!(
        installed_wal_quorum_bounds_fetch(&[1, 2, 3], reported, 1, 3, 0, watermarks)
            == Some((10, 10, std::vec![(2, 10), (3, 10)]))
    );
}

#[test]
fn checked_wal_copy_boundaries() {
    let source = [(0, 1, std::vec![1, 2]), (2, 0, std::vec![3, 4, 5])];
    assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 3, 10, 15) == Some((3, 15)));
    assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 3, 10, 14).is_none());
    assert!(checked_wal_copy_replays_exactly(&source, &source, 0, 2, 10, 15).is_none());
    assert!(checked_wal_copy_replays_exactly(&source, &source[..1], 0, 3, 10, 15).is_none());
    let mut different = source.clone();
    different[1].2[2] ^= 1;
    assert!(checked_wal_copy_replays_exactly(&source, &different, 0, 3, 10, 15).is_none());
    assert!(
        checked_wal_copy_replays_exactly(&[], &[], i64::MAX, i64::MAX, u64::MAX, u64::MAX)
            == Some((i64::MAX, u64::MAX))
    );
    assert!(checked_wal_copy_replays_exactly(&[], &[], 0, 0, 2, 1).is_none());
    for source in [
        [(0, -1, std::vec![1])],
        [(-1, 0, std::vec![1])],
        [(i64::MAX, 0, std::vec![1])],
        [(0, 0, std::vec![])],
    ] {
        assert!(
            checked_wal_copy_replays_exactly(&source, &source, source[0].0, 1, 0, 16).is_none()
        );
    }
    let gap = [(0, 0, std::vec![1]), (2, 0, std::vec![2])];
    assert!(checked_wal_copy_replays_exactly(&gap, &gap, 0, 3, 0, 16).is_none());
    let last = [(i64::MAX - 1, 0, std::vec![1])];
    assert!(
        checked_wal_copy_replays_exactly(
            &last,
            &last,
            i64::MAX - 1,
            i64::MAX,
            u64::MAX - 1,
            u64::MAX
        ) == Some((i64::MAX, u64::MAX))
    );
    assert!(
        checked_wal_copy_replays_exactly(&last, &last, i64::MAX - 1, i64::MAX, u64::MAX, u64::MAX)
            .is_none()
    );
}

proptest! {
    #[test]
    fn installed_wal_fetch_support_matches_identity_and_sorted_offset_oracles(
        votes in proptest::collection::vec((0u64..16, any::<i64>()), 0..12),
        configured in 0usize..12,
        matching_count in any::<bool>(),
        end in 0i64..=i64::MAX,
        start in 0i64..=i64::MAX,
        current in 0i64..=i64::MAX,
        lso in any::<i64>(),
        deliverable in any::<i64>(),
    ) {
        let (voters, reported): (Vec<_>, Vec<_>) = votes.into_iter().unzip();
        let local = voters.first().copied().unwrap_or(0);
        let expected = if matching_count { voters.len() } else { configured };
        let distinct: std::collections::HashSet<_> = voters.iter().copied().collect();
        let current = current.min(end);
        let w = FetchWatermarks { log_start: start.min(end), log_end: end, hw: 0, lso, deliverable };
        let result = installed_wal_quorum_bounds_fetch(&voters, &reported, local, expected, current, w);
        assert!(result.is_some() == (expected > 0 && voters.len() == expected && distinct.len() == voters.len()));
        if let Some((hw, limit, supporters)) = result {
            let mut sorted: Vec<_> = reported.iter().map(|offset| (*offset).min(end)).collect();
            sorted.sort_unstable_by(|a, b| b.cmp(a));
            let expected_hw = current.max(w.log_start).max(sorted[voters.len() / 2]);
            assert!(hw == expected_hw && limit == hw.min(lso).min(deliverable));
            let expected_support: Vec<_> = voters.iter().copied().zip(reported.iter().copied())
                .map(|(node, offset)| (node, offset.min(end)))
                .filter(|(_, offset)| *offset >= limit).collect();
            assert!(supporters == expected_support);
            let support_nodes: std::collections::HashSet<_> = supporters.iter().map(|(node, _)| *node).collect();
            assert!(support_nodes.len() == supporters.len());
            if hw > current && limit > w.log_start { assert!(support_nodes.len() >= voters.len() / 2 + 1); }
        }
    }
}

#[test]
fn installed_wal_fetch_support_boundaries() {
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 10,
        hw: 0,
        lso: 10,
        deliverable: 10,
    };
    check_two_durable_followers(&[0, 10, 10], w);
    // The unsynced leader end is not a second vote for a single follower.
    assert!(
        installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, 10, 0], 1, 3, 0, w)
            == Some((0, 0, std::vec![(1, 0), (2, 10), (3, 0)]))
    );
    assert!(installed_wal_quorum_bounds_fetch(&[1, 2, 2], &[0, 10, 10], 1, 3, 0, w).is_none());
    assert!(installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[0, 10], 1, 3, 0, w).is_none());
    assert!(installed_wal_quorum_bounds_fetch(&[2, 1, 3], &[10, 10, 10], 1, 3, 0, w).is_none());
    assert!(installed_wal_quorum_bounds_fetch(&[], &[], 1, 3, 0, w).is_none());
    assert!(installed_wal_quorum_bounds_fetch(&[1, 2], &[10, 10], 1, 3, 0, w).is_none());
    // Raising the floor alone exposes no retained records and need not
    // obtain new support. An inherited HWM needs prior durability evidence.
    for (current, start) in [(0, 5), (5, 0)] {
        assert!(
            installed_wal_quorum_bounds_fetch(
                &[1, 2, 3],
                &[0, 0, 0],
                1,
                3,
                current,
                FetchWatermarks {
                    log_start: start,
                    ..w
                }
            ) == Some((5, 5, std::vec![]))
        );
    }
    let maximum = FetchWatermarks {
        log_end: i64::MAX,
        lso: i64::MAX,
        deliverable: i64::MAX,
        ..w
    };
    assert!(
        installed_wal_quorum_bounds_fetch(&[1], &[i64::MAX], 1, 1, 0, maximum)
            == Some((i64::MAX, i64::MAX, std::vec![(1, i64::MAX)]))
    );
    check_two_durable_followers(&[0, i64::MAX, i64::MAX], w);
    assert!(
        installed_wal_quorum_bounds_fetch(&[1, 2, 3], &[3, 8, 10], 1, 3, 0, FetchWatermarks {
            lso: 5,
            deliverable: 6,
            ..w
        }) == Some((8, 5, std::vec![(2, 8), (3, 10)]))
    );
}
