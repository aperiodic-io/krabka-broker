use super::*;

type Observation = (Option<u64>, i32, bool, Vec<WalCopyBatch>);
type QuorumLedger = (i64, i64, Vec<(u64, i64)>, Vec<i64>);

fn complete_source() -> [WalCopyBatch; 3] {
    [
        (0, 1, std::vec![1, 2]),
        (2, 0, std::vec![3, 4]),
        (3, 2, std::vec![5, 6]),
    ]
}

fn maximum_offset_source() -> ([WalCopyBatch; 1], FetchWatermarks) {
    ([(i64::MAX - 1, 0, std::vec![1])], FetchWatermarks {
        log_start: i64::MAX - 1,
        log_end: i64::MAX,
        hw: 0,
        lso: i64::MAX,
        deliverable: i64::MAX,
    })
}

fn oracle(
    voters: &[u64],
    rows: &[Observation],
    placement: (u64, usize, i32),
    source: &[WalCopyBatch],
    current: i64,
    w: FetchWatermarks,
) -> Option<QuorumLedger> {
    let unique: std::collections::HashSet<_> = voters.iter().collect();
    if voters.len() != rows.len()
        || voters.len() != placement.1
        || placement.1 == 0
        || voters.first() != Some(&placement.0)
        || unique.len() != voters.len()
    {
        return None;
    }
    let mut end = source.first().map_or(w.log_start, |b| b.0);
    let mut bytes = 0_u128;
    for (i, (base, delta, body)) in source.iter().enumerate() {
        let next = i128::from(*base) + i128::from(*delta) + 1;
        if *base < 0
            || *delta < 0
            || body.is_empty()
            || *base != end
            || next > i128::from(i64::MAX)
            || (i == 0 && !(*base <= w.log_start && i128::from(w.log_start) < next))
        {
            return None;
        }
        end = i64::try_from(next).unwrap();
        bytes += body.len() as u128;
    }
    if end != w.log_end || bytes > u128::from(u64::MAX) {
        return None;
    }
    let reports: Vec<_> = rows
        .iter()
        .zip(voters)
        .map(|(row, node)| {
            if !row.2
                || row.0 != Some(*node)
                || (row.1 >= 0 && row.1 != placement.2)
                || row.3.len() > source.len()
                || row.3 != source[..row.3.len()]
            {
                w.log_start
            } else {
                row.3
                    .last()
                    .map_or(w.log_start, |b| b.0 + i64::from(b.1) + 1)
            }
        })
        .collect();
    let mut ranked = reports.clone();
    ranked.sort_unstable();
    let hw = current
        .max(w.log_start)
        .max(ranked[ranked.len() - 1 - voters.len() / 2]);
    let limit = hw.min(w.lso).min(w.deliverable);
    let supporters = voters
        .iter()
        .copied()
        .zip(reports.iter().copied())
        .filter(|(_, end)| *end >= limit)
        .collect();
    Some((hw, limit, supporters, reports))
}

fn damaged_copy(source: &[WalCopyBatch], node: u64, kind: u16, count: usize) -> Observation {
    let mut copy = source[..count.min(source.len())].to_vec();
    if kind & 4 != 0 && !copy.is_empty() {
        copy[0].2[0] ^= 1;
    }
    if kind & 8 != 0 && !copy.is_empty() {
        copy[0].0 += 1;
    }
    (
        if kind & 1 == 0 { Some(node) } else { None },
        if kind & 16 == 0 { 7 } else { 6 },
        kind & 2 == 0,
        copy,
    )
}

fn maximum_offset_rows(source: &[WalCopyBatch]) -> [Observation; 3] {
    [
        damaged_copy(source, 1, 0, 1),
        damaged_copy(source, 2, 0, 1),
        damaged_copy(source, 3, 4, 1),
    ]
}

#[test]
fn only_completed_matching_copies_can_release_a_fresh_prefix() {
    let source = complete_source();
    for floor in [0, 1] {
        let w = FetchWatermarks {
            log_start: floor,
            log_end: 6,
            hw: 0,
            lso: 6,
            deliverable: 6,
        };
        for first in 0..32 {
            for second in 0..32 {
                for prefix in 0..=source.len() {
                    let rows = [
                        damaged_copy(&source, 1, 0, source.len()),
                        damaged_copy(&source, 2, first, prefix),
                        damaged_copy(&source, 3, second, source.len()),
                    ];
                    assert2::assert!(
                        durable_matching_copies_bound_fetch(
                            &[1, 2, 3],
                            &rows,
                            (1, 3, 7),
                            &source,
                            floor,
                            w
                        ) == oracle(&[1, 2, 3], &rows, (1, 3, 7), &source, floor, w)
                    );
                }
            }
        }
    }
    let w = FetchWatermarks {
        log_start: 1,
        log_end: 6,
        hw: 0,
        lso: 4,
        deliverable: 5,
    };
    let rows = [
        damaged_copy(&source, 1, 0, 3),
        damaged_copy(&source, 2, 0, 3),
        damaged_copy(&source, 3, 4, 3),
    ];
    assert2::assert!(
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &source, 1, w)
            == Some((6, 4, std::vec![(1, 6), (2, 6)], std::vec![6, 6, 1]))
    );
}

#[yare::parameterized(
    older_peer_epoch = { Some(2), -1, false, 6 },
    wrong_peer = { Some(3), 7, false, 1 },
    newer_peer_epoch = { Some(2), 8, false, 1 },
    extra_batch = { Some(2), 7, true, 1 },
)]
fn mismatched_peer_rows_cannot_vote(peer: Option<u64>, epoch: i32, extra: bool, expected_hw: i64) {
    let source = complete_source();
    let uncapped = FetchWatermarks {
        log_start: 1,
        log_end: 6,
        hw: 0,
        lso: 6,
        deliverable: 6,
    };
    let mut row = damaged_copy(&source, 2, 0, 3);
    row.0 = peer;
    row.1 = epoch;
    if extra {
        row.3.push(source[2].clone());
    }
    let rows = [
        damaged_copy(&source, 1, 0, 3),
        row,
        damaged_copy(&source, 3, 2, 3),
    ];
    let result =
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &source, 1, uncapped)
            .unwrap();
    assert2::assert!(result.0 == expected_hw);
    assert2::assert!(Some(result) == oracle(&[1, 2, 3], &rows, (1, 3, 7), &source, 1, uncapped));
}

#[test]
fn byte_quorum_distinguishes_floor_only_and_inherited_watermarks() {
    let (source, w) = maximum_offset_source();
    let rows = maximum_offset_rows(&source);
    assert2::assert!(
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &source, w.log_start, w)
            == oracle(&[1, 2, 3], &rows, (1, 3, 7), &source, w.log_start, w)
    );
    let rows = [
        (None, 7, false, std::vec![]),
        (None, 7, false, std::vec![]),
        (None, 7, false, std::vec![]),
    ];
    assert2::assert!(
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &source, w.log_end, w)
            == Some((w.log_end, w.log_end, std::vec![], std::vec![w.log_start; 3]))
    );
    let empty = FetchWatermarks {
        log_start: 5,
        log_end: 5,
        hw: 0,
        lso: 5,
        deliverable: 5,
    };
    assert2::assert!(
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &[], 0, empty)
            == Some((5, 5, std::vec![(1, 5), (2, 5), (3, 5)], std::vec![5; 3]))
    );
}

#[yare::parameterized(
    negative_delta = { std::vec![(0, -1, std::vec![1])] },
    empty_body = { std::vec![(0, 0, std::vec![])] },
    gapped_source = { std::vec![(0, 0, std::vec![1]), (2, 0, std::vec![2])] },
    end_overflow = { std::vec![(i64::MAX - 1, 1, std::vec![1])] },
)]
fn malformed_sources_are_rejected(bad: Vec<WalCopyBatch>) {
    let (source, w) = maximum_offset_source();
    let rows = maximum_offset_rows(&source);
    assert2::assert!(
        durable_matching_copies_bound_fetch(&[1, 2, 3], &rows, (1, 3, 7), &bad, w.log_start, w)
            .is_none()
    );
}

proptest! {
    #[test]
    fn authenticated_byte_votes_match_a_sorted_prefix_oracle(
        batches in proptest::collection::vec((0_i32..8, proptest::collection::vec(any::<u8>(), 1..8)), 0..8),
        raw_floor in 0_i64..8, raw_current in 0_i64..100, lso in any::<i64>(), deliverable in any::<i64>(),
        raw_voters in proptest::collection::vec(1_u64..9, 0..9),
        damage in proptest::collection::vec((0_u16..32, 0_usize..12), 0..9),
        expected in 0_usize..9, exact_size in any::<bool>(),
    ) {
        let mut base = 0;
        let source: Vec<_> = batches.into_iter().map(|(delta, bytes)| {
            let batch = (base, delta, bytes); base += i64::from(delta) + 1; batch
        }).collect();
        let floor = source.first().map_or(0, |b| raw_floor.min(i64::from(b.1)));
        let rows: Vec<_> = damage.iter().enumerate().map(|(i,(kind,count))|
            damaged_copy(&source, raw_voters.get(i).copied().unwrap_or(0), *kind, *count)).collect();
        let placement = (raw_voters.first().copied().unwrap_or(1), if exact_size { raw_voters.len() } else { expected }, 7);
        let w = FetchWatermarks { log_start: floor, log_end: base, hw: 0, lso, deliverable };
        let current = raw_current.min(base);
        prop_assert_eq!(durable_matching_copies_bound_fetch(&raw_voters, &rows, placement, &source, current, w),
            oracle(&raw_voters, &rows, placement, &source, current, w));
    }
}
