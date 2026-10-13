use {
    super::{
        completed_batches_preserve_first_retry, completed_eviction_bounds_waiters,
        recovered_window_row,
    },
    assert2::assert,
    proptest::prelude::*,
};

#[test]
fn an_evicted_source_can_still_have_a_later_unready_sequence_alias() {
    let old = super::epoch_rows(5, 7, 0);
    let incoming = recovered_window_row(20, 2, 0);
    let (evicted, frontier, ready, retained) =
        completed_eviction_bounds_waiters(23, 3, 7, &old, incoming, 0);
    assert!(evicted && frontier == 3 && ready && retained.iter().all(|row| !row.2));
    let (_, _, decision, witness) = completed_batches_preserve_first_retry(
        23,
        3,
        Some(7),
        &old,
        incoming,
        (7, i32::MAX - 1, 2),
    );
    assert!(decision == super::ProducerDecision::Duplicate { retained: 0 });
    assert!(witness == Some((1, 4, 7, false)));
}

#[test]
fn a_newer_waiter_cannot_ack_before_an_evicted_batch() {
    let old = super::epoch_rows(5, 7, 0);
    let incoming = recovered_window_row(20, 2, 0);
    for hwm in [0, 2, 3, 6, 7, 22, 23] {
        let (evicted, frontier, ready, retained) =
            completed_eviction_bounds_waiters(23, hwm, 7, &old, incoming, 0);
        assert!(evicted && frontier == 3 && ready == (hwm >= 3));
        assert!(
            retained
                == (1..=5)
                    .map(|source| {
                        let target = 4 * i64::try_from(source).unwrap() + 3;
                        (source, target, hwm >= target)
                    })
                    .collect::<Vec<_>>()
        );
        assert!(
            retained
                .iter()
                .all(|(_, later, ack)| *later > frontier && (!*ack || ready))
        );
    }
}

#[test]
fn repeated_offsets_earlier_completions_and_lower_epochs_do_not_evict() {
    let old: Vec<_> = (1..=5)
        .map(|i| recovered_window_row(4 * i, 2, i32::MAX))
        .collect();
    for (base, epoch) in [(0, 7), (4, 7), (24, 6)] {
        let mut incoming = recovered_window_row(base, 2, 0);
        incoming.producer_epoch = epoch;
        for origin in 0..5 {
            let (evicted, frontier, ready, retained) =
                completed_eviction_bounds_waiters(27, 7, 7, &old, incoming, origin);
            assert!(
                !evicted && frontier == old[origin].last_offset + 1 && ready == (frontier <= 7)
            );
            assert!(retained.iter().map(|row| row.0).collect::<Vec<_>>() == vec![0, 1, 2, 3, 4]);
        }
    }
}

#[test]
fn maximum_frontiers_preserve_the_strict_ack_boundary() {
    let old: Vec<_> = (1..=5)
        .map(|i| recovered_window_row(i64::MAX - 7 + i, 0, 0))
        .collect();
    let incoming = recovered_window_row(i64::MAX - 1, 0, 0);
    for hwm in [i64::MAX - 6, i64::MAX - 5, i64::MAX - 1, i64::MAX] {
        let (evicted, frontier, ready, retained) =
            completed_eviction_bounds_waiters(i64::MAX, hwm, 7, &old, incoming, 0);
        assert!(evicted && frontier == i64::MAX - 5 && ready == (hwm >= frontier));
        assert!(retained.last().unwrap().1 == i64::MAX);
        assert!(
            retained
                .iter()
                .all(|(_, later, ack)| *later > frontier && (!*ack || ready))
        );
    }
}

proptest! {
    #[test]
    fn eviction_and_waiter_order_agree_with_a_distinct_offset_oracle(
        gaps in proptest::collection::vec((1_i64..8, 0_i32..4), 1..=5),
        new_base in 0_i64..64, new_delta in 0_i32..4, older in any::<bool>(),
        hwm in 0_i64..=70, origin_seed in any::<usize>(),
    ) {
        let mut end = 0;
        let old: Vec<_> = gaps.into_iter().map(|(gap, delta)| {
            let row = recovered_window_row(end + gap, delta, 0);
            end = row.last_offset + 1;
            row
        }).collect();
        let mut incoming = recovered_window_row(new_base, new_delta, 0);
        incoming.producer_epoch = if older { 6 } else { 7 };
        let origin = origin_seed % old.len();
        let mut offsets: std::collections::BTreeSet<_> = old.iter().map(|row| row.last_offset).collect();
        if !older { offsets.insert(incoming.last_offset); }
        let expected: Vec<_> = offsets.into_iter().rev().take(5).collect();
        let evicted = !expected.contains(&old[origin].last_offset);
        let (actual, frontier, ready, retained) =
            completed_eviction_bounds_waiters(70, hwm, 7, &old, incoming, origin);
        assert!(actual == evicted && frontier == old[origin].last_offset + 1 && ready == (frontier <= hwm));
        assert!(retained.iter().rev().map(|row| row.1 - 1).collect::<Vec<_>>() == expected);
        if evicted {
            assert!(retained.len() == 5 && retained.iter().all(|(_, later, ack)| *later > frontier && (!*ack || ready)));
        }
    }
}
