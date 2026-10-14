use super::*;

proptest::proptest! {
    #[test]
    fn batch_equality_matches_independent_tuple_oracle(
        left_base in proptest::prelude::any::<i64>(),
        left_last in proptest::prelude::any::<i64>(),
        left_bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        right_base in proptest::prelude::any::<i64>(),
        right_last in proptest::prelude::any::<i64>(),
        right_bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
    ) {
        let left = (left_base, left_last, left_bytes.as_slice());
        let right = (right_base, right_last, right_bytes.as_slice());
        assert2::assert!(wal_batch_equal(left, right) == (left == right));
        assert2::assert!(wal_batch_equal(left, left));
        if !left_bytes.is_empty() {
            let mut changed = left_bytes.clone();
            let last = changed.len() - 1;
            changed[last] ^= 1;
            assert2::assert!(!wal_batch_equal(left, (left_base, left_last, &changed)));
        }
    }

    #[test]
    fn full_wal_placement_is_distinct_and_exhausts_eligible_candidates(
        candidates in proptest::collection::vec((0u64..8, 0u64..8), 0..32),
        local in 0u64..8,
        requested in 0usize..12,
    ) {
        let selected = select_wal_voters(&candidates, local, requested);
        let nodes: std::collections::HashSet<_> = selected.iter().map(|(node, _)| *node).collect();
        let racks: std::collections::HashSet<_> = selected.iter().map(|(_, rack)| *rack).collect();
        assert2::assert!(nodes.len() == selected.len() && racks.len() == selected.len());
        assert2::assert!(selected.len() <= requested);
        assert2::assert!(selected.iter().all(|candidate| candidates.contains(candidate)));
        assert2::assert!(selected.is_empty() == (requested == 0 || !candidates.iter().any(|(node, _)| *node == local)));
        if let Some((first, _)) = selected.first() { assert2::assert!(*first == local); }
        if !selected.is_empty() && selected.len() < requested {
            assert2::assert!(candidates.iter().all(|(node, rack)| nodes.contains(node) || racks.contains(rack)));
        }
    }

    #[test]
    fn voter_installation_matches_set_oracle(
        voters in proptest::collection::vec(0u64..8, 0..16),
        local in 0u64..8,
        expected in 0usize..16,
    ) {
        let distinct: std::collections::HashSet<_> = voters.iter().copied().collect();
        let valid = expected > 0 && voters.len() == expected
            && voters.first() == Some(&local) && distinct.len() == voters.len();
        assert2::assert!(wal_voter_set_valid(&voters, local, expected) == valid);
    }
}

proptest::proptest! {
    #[test]
    fn covering_batch_ranges_match_an_independent_interval_oracle(
        rows in proptest::collection::vec((-2i64..60, -2i64..60), 0..12),
        start in -2i64..65, target in -2i64..65,
    ) {
        let bases: Vec<_> = rows.iter().map(|row| row.0).collect();
        let lasts: Vec<_> = rows.iter().map(|row| row.1).collect();
        let expected = if start < 0 || target < start { None }
            else if start == target { rows.is_empty().then_some(start) }
            else if let Some(&(first, last)) = rows.first() {
                let mut cursor = i128::from(first);
                let mut valid = first >= 0 && first <= start && start <= last;
                for &(base, last) in &rows {
                    valid &= i128::from(base) == cursor && base <= last && last < i64::MAX;
                    cursor = i128::from(last) + 1;
                }
                (valid && cursor == i128::from(target)).then_some(first)
            } else { None };
        assert2::assert!(wal_covering_batch_range(&bases, &lasts, start, target) == expected);
    }
}

#[test]
fn covering_batch_range_preserves_strict_ends_and_extreme_floors() {
    assert2::assert!(wal_covering_batch_range(&[0, 3], &[2, 5], 1, 6) == Some(0));
    assert2::assert!(!exact_wal_batch_range(&[0, 3], &[2, 5], 1, 6));
    assert2::assert!(wal_covering_batch_range(&[0], &[2], 1, 2).is_none());
    assert2::assert!(wal_covering_batch_range(&[0, 4], &[2, 5], 1, 6).is_none());
    assert2::assert!(wal_covering_batch_range(&[0], &[], 1, 3).is_none());
    assert2::assert!(wal_covering_batch_range(&[], &[], i64::MAX, i64::MAX) == Some(i64::MAX));
    assert2::assert!(
        wal_covering_batch_range(&[i64::MAX - 2], &[i64::MAX - 1], i64::MAX - 1, i64::MAX)
            == Some(i64::MAX - 2)
    );
}

#[test]
fn exact_wal_range_rejects_every_discontinuity_and_overflow() {
    assert2::assert!(exact_wal_batch_range(&[], &[], 4, 4));
    assert2::assert!(exact_wal_batch_range(&[4, 6], &[5, 8], 4, 9));
    assert2::assert!(!exact_wal_batch_range(&[4, 7], &[5, 8], 4, 9));
    assert2::assert!(!exact_wal_batch_range(&[4, 5], &[5, 8], 4, 9));
    assert2::assert!(!exact_wal_batch_range(&[6, 4], &[8, 5], 4, 9));
    assert2::assert!(!exact_wal_batch_range(&[4], &[3], 4, 4));
    assert2::assert!(!exact_wal_batch_range(
        &[i64::MAX],
        &[i64::MAX],
        i64::MAX,
        i64::MAX
    ));
    assert2::assert!(exact_wal_batch_range(
        &[i64::MAX - 1],
        &[i64::MAX - 1],
        i64::MAX - 1,
        i64::MAX
    ));
    assert2::assert!(!exact_wal_batch_range(&[4], &[], 4, 5));
}

#[yare::parameterized(
    unauthenticated = { None, 2, 1, 8, WalFetchAdmission::Denied },
    authenticated_as_other = { Some(3), 2, 1, 8, WalFetchAdmission::Denied },
    local_not_first = { Some(2), 2, 9, 8, WalFetchAdmission::Denied },
    claimed_not_voter = { Some(4), 4, 1, 8, WalFetchAdmission::Denied },
    unspecified_epoch = { Some(2), 2, 1, -1, WalFetchAdmission::Serve },
    current_epoch = { Some(2), 2, 1, 8, WalFetchAdmission::Serve },
    epoch_below_zero = { Some(2), 2, 1, 0, WalFetchAdmission::FencedLeaderEpoch },
    stale_epoch = { Some(2), 2, 1, 7, WalFetchAdmission::FencedLeaderEpoch },
    future_epoch = { Some(2), 2, 1, 9, WalFetchAdmission::UnknownLeaderEpoch },
)]
fn wal_fetch_admission_fails_closed_and_classifies_epochs(
    authenticated: Option<u64>,
    claimed: u64,
    local: u64,
    epoch: i32,
    expected: WalFetchAdmission,
) {
    let voters = [1, 2, 3];
    assert2::assert!(
        wal_fetch_admission(authenticated, claimed, local, &voters, epoch, 8) == expected
    );
}

#[test]
fn wal_voter_selection_is_local_first_and_rack_distinct() {
    let candidates = [(1, 10), (2, 20), (3, 10), (4, 30)];
    assert2::assert!(select_wal_voter_index(&candidates, &[], &[], 2, true) == Some(1));
    assert2::assert!(select_wal_voter_index(&candidates, &[2], &[20], 2, false) == Some(0));
    assert2::assert!(select_wal_voter_index(&candidates, &[1, 2], &[10, 20], 2, false) == Some(3));
    assert2::assert!(
        select_wal_voter_index(&candidates, &[1, 2, 4], &[10, 20, 30], 2, false) == None
    );
}
