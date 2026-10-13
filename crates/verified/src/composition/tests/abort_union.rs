use {
    super::*,
    crate::transaction::unique_aborted_transaction_rows,
    std::{collections::BTreeSet, vec},
};

pub(super) fn visible_rows(
    remote: &[RestoreAbortedTxn],
    local: &[RestoreAbortedTxn],
    from: i64,
    end: i64,
) -> BTreeSet<(i64, i64)> {
    remote
        .iter()
        .chain(local)
        .filter(|entry| from < end && entry.start_offset < end && entry.last_offset >= from)
        .map(|entry| (entry.producer_id, entry.start_offset))
        .collect()
}

pub(super) fn index(
    rows: std::collections::BTreeMap<i64, (i64, i64)>,
) -> (Vec<RestoreAbortedTxn>, RestoreSegmentExtent) {
    let owner = RestoreSegmentExtent {
        base_offset: rows.keys().next().copied().unwrap_or(0),
        last_offset: rows.keys().next_back().copied().unwrap_or(0),
    };
    (
        rows.into_iter()
            .map(|(last_offset, (producer_id, first))| RestoreAbortedTxn {
                producer_id,
                start_offset: first.min(last_offset),
                last_offset,
            })
            .collect(),
        owner,
    )
}

proptest! {
    #[test]
    fn complete_sources_match_an_independent_interval_and_set_oracle(
        remote in proptest::collection::btree_map(0_i64..=i64::MAX, (0_i64..=i64::MAX, 0_i64..=i64::MAX), 0..12),
        local in proptest::collection::btree_map(0_i64..=i64::MAX, (0_i64..=i64::MAX, 0_i64..=i64::MAX), 0..12),
        from in 0_i64..=i64::MAX, cut in any::<i64>(), hw in 0_i64..=i64::MAX,
        lso in 0_i64..=i64::MAX, deliverable in 0_i64..=i64::MAX, corrupt in any::<bool>(),
    ) {
        let (mut remote, remote_owner) = index(remote);
        let (local, local_owner) = index(local);
        if corrupt && !remote.is_empty() { remote[0].producer_id = -1; }
        let w = FetchWatermarks { log_start: 0, log_end: i64::MAX, hw, lso, deliverable };
        let result = restored_abort_sources_cover_committed_fetch(&remote, &local, (remote_owner, local_owner), w, from, cut);
        if remote.iter().any(|entry| entry.producer_id < 0) {
            prop_assert_eq!(result, None);
        } else {
            let end = hw.min(lso).min(deliverable).min(cut);
            let expected = visible_rows(&remote, &local, from, end);
            let actual = result.unwrap();
            prop_assert_eq!(actual.len(), expected.len());
            prop_assert_eq!(actual.into_iter().collect::<BTreeSet<_>>(), expected);
        }
    }
}

#[test]
fn later_marker_sources_are_complete_and_duplicate_wire_rows_are_removed() {
    let entry = RestoreAbortedTxn {
        producer_id: 7,
        start_offset: 0,
        last_offset: 10,
    };
    let owner = RestoreSegmentExtent {
        base_offset: 10,
        last_offset: 10,
    };
    let w = FetchWatermarks {
        log_start: 0,
        log_end: 20,
        hw: 20,
        lso: 20,
        deliverable: 20,
    };
    assert2::assert!(
        restored_abort_sources_cover_committed_fetch(&[entry], &[entry], (owner, owner), w, 0, 1)
            == Some(vec![(7, 0)])
    );
    assert2::assert!(
        restored_abort_sources_cover_committed_fetch(&[entry], &[], (owner, owner), w, 0, 1)
            == Some(vec![(7, 0)])
    );
    assert2::assert!(
        restored_abort_sources_cover_committed_fetch(&[], &[entry], (owner, owner), w, 0, 1)
            == Some(vec![(7, 0)])
    );
    for (from, cut) in [(0, i64::MIN), (0, 0), (10, 10), (11, i64::MAX)] {
        assert2::assert!(
            restored_abort_sources_cover_committed_fetch(
                &[entry],
                &[entry],
                (owner, owner),
                w,
                from,
                cut
            ) == Some(vec![])
        );
    }
    assert2::assert!(
        unique_aborted_transaction_rows(&[(7, 0), (7, 0), (7, 1), (8, 0)])
            == vec![(7, 0), (7, 1), (8, 0)]
    );
}
