use {
    super::{FetchWatermarks, committed_fetch_excludes_unstable, stable_abort_sources_cover_fetch},
    crate::broker::FetchVisibility,
    assert2::assert,
    proptest::prelude::*,
    std::{collections::BTreeSet, vec},
};

fn visibility_oracle(starts: &[i64], w: FetchWatermarks) -> Option<(i64, FetchVisibility)> {
    if starts.iter().any(|start| *start > w.log_end) {
        return None;
    }
    let lso = starts.iter().copied().min().unwrap_or(w.log_end);
    Some((lso, FetchVisibility {
        out_of_range: w.log_start > w.log_end,
        empty: w.log_start <= w.log_end && w.log_start >= w.hw.min(w.deliverable),
        limit_offset: lso.min(w.hw).min(w.deliverable),
        effective_lso: lso.min(w.hw),
        read_committed_aborts: true,
        response_hw: w.hw,
        response_lso: lso.min(w.hw),
    }))
}

proptest! {
    #[test]
    fn unstable_frontier_and_all_visibility_fields_match_independent_oracle(
        starts in proptest::collection::vec(any::<i64>(), 0..16),
        log_start in any::<i64>(), log_end in any::<i64>(), hw in any::<i64>(),
        inherited_lso in any::<i64>(), deliverable in any::<i64>(),
    ) {
        let w = FetchWatermarks { log_start, log_end, hw, lso: inherited_lso, deliverable };
        assert!(committed_fetch_excludes_unstable(&starts, w) == visibility_oracle(&starts, w));
    }

    #[test]
    fn derived_stability_and_abort_sources_match_complete_interval_union(
        starts in proptest::collection::vec(0_i64..=64, 0..12),
        remote in proptest::collection::btree_map(0_i64..=128, (0_i64..=16, 0_i64..=128), 0..12),
        local in proptest::collection::btree_map(0_i64..=128, (0_i64..=16, 0_i64..=128), 0..12),
        log_end in 0_i64..=64, hw in 0_i64..=64, inherited_lso in any::<i64>(),
        deliverable in 0_i64..=64, from in 0_i64..=64,
        cut in prop_oneof![0_i64..=64, Just(i64::MIN), Just(i64::MAX)],
        corrupt in any::<bool>(),
    ) {
        let (mut remote, remote_owner) = super::abort_union::index(remote);
        let (local, local_owner) = super::abort_union::index(local);
        if corrupt && !remote.is_empty() { remote[0].producer_id = -1; }
        let w = FetchWatermarks { log_start: 0, log_end, hw, lso: inherited_lso, deliverable };
        let actual = stable_abort_sources_cover_fetch(&starts, &remote, &local, (remote_owner, local_owner), w, from, cut);
        let stable = visibility_oracle(&starts, w);
        if stable.is_none() || remote.iter().any(|entry| entry.producer_id < 0) {
            assert!(actual == None);
        } else {
            let (lso, visibility) = stable.unwrap();
            let end = visibility.limit_offset.min(cut);
            let expected = super::abort_union::visible_rows(&remote, &local, from, end);
            let (actual_lso, actual_end, rows) = actual.unwrap();
            assert!(actual_lso == lso && actual_end == end && rows.len() == expected.len());
            assert!(rows.into_iter().collect::<BTreeSet<_>>() == expected);
        }
    }
}

#[test]
fn stability_bounds_cover_duplicate_starts_rejection_and_signed_extremes() {
    for starts in [&[][..], &[9, 3, 14], &[3, 3], &[20], &[21], &[-1], &[
        i64::MIN,
    ]] {
        for deliverable in [0, 2, 20] {
            let w = FetchWatermarks {
                log_start: 0,
                log_end: 20,
                hw: 15,
                lso: i64::MAX,
                deliverable,
            };
            assert!(committed_fetch_excludes_unstable(starts, w) == visibility_oracle(starts, w));
        }
    }
}

#[test]
fn abort_rows_follow_derived_stability_instead_of_stale_inherited_lso() {
    let (remote, owner) = super::abort_union::index([(10, (7, 0)), (12, (8, 6))].into());
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 20,
        hw: 20,
        lso: i64::MIN,
        deliverable: 20,
    };
    assert!(
        stable_abort_sources_cover_fetch(&[3], &remote, &remote, (owner, owner), w, 0, 20)
            == Some((3, 3, vec![(7, 0)]))
    );
    assert!(
        stable_abort_sources_cover_fetch(&[3], &remote, &remote, (owner, owner), w, 3, 20)
            == Some((3, 3, vec![]))
    );
    assert!(
        stable_abort_sources_cover_fetch(&[21], &remote, &remote, (owner, owner), w, 0, 20) == None
    );
    let mut corrupt_tail = remote.clone();
    corrupt_tail[1].producer_id = -1;
    assert!(
        stable_abort_sources_cover_fetch(&[3], &corrupt_tail, &[], (owner, owner), w, 0, 20)
            == None
    );
}
