use {
    super::*,
    assert2::assert,
    std::collections::{BTreeMap, HashSet},
};

fn valid_rows(rows: &[(u32, u32)], max_relative: i64, log_bytes: u64) -> bool {
    rows.iter().all(|&(offset, position)| {
        i64::from(offset) <= max_relative && u64::from(position) < log_bytes
    }) && rows
        .windows(2)
        .all(|pair| pair[0].0 < pair[1].0 && pair[0].1 < pair[1].1)
}

fn check_seek(
    rows: &[(u32, u32)],
    batches: &[(u32, u32)],
    target: u32,
    max_relative: i64,
    log_bytes: u64,
) {
    let archive: BTreeMap<_, _> = rows.iter().copied().collect();
    let cursors = if valid_rows(rows, max_relative, log_bytes) {
        Ok((
            archive.range(..=target).next_back().map_or(0, |(_, &p)| p),
            archive.range(target..).next().map(|(_, &p)| p),
        ))
    } else {
        Err(())
    };
    assert!(validated_index_bounds_lookup(rows, target, max_relative, log_bytes) == cursors);
    let source: HashSet<_> = batches.iter().copied().collect();
    let valid = valid_rows(rows, max_relative, log_bytes)
        && valid_rows(batches, max_relative, log_bytes)
        && if batches.is_empty() {
            log_bytes == 0 && rows.is_empty()
        } else {
            batches[0].1 == 0 && rows.iter().all(|row| source.contains(row))
        };
    let expected = if valid {
        let (floor, ceiling) = cursors.expect("valid archive has cursors");
        Ok((
            floor,
            ceiling,
            batches.iter().position(|&(last, _)| last >= target),
        ))
    } else {
        Err(())
    };
    assert!(
        indexed_offset_scan_preserves_first_batch(rows, batches, target, max_relative, log_bytes)
            == expected
    );
}

proptest! {
    #[test]
    fn offset_seek_rejects_arbitrary_invalid_archives(
        rows in proptest::collection::vec(any::<(u32, u32)>(), 0..12),
        batches in proptest::collection::vec(any::<(u32, u32)>(), 0..12),
        target in any::<u32>(), max_relative in any::<i64>(), log_bytes in any::<u64>(),
    ) {
        check_seek(&rows, &batches, target, max_relative, log_bytes);
    }

    #[test]
    fn sparse_offset_seek_matches_full_batch_scan(
        offsets in proptest::collection::btree_set(0u32..1000, 0..20),
        selected in any::<u32>(), target in 0u32..1100,
    ) {
        let batches: Vec<_> = offsets.into_iter().enumerate().map(|(i, offset)| {
            (offset, u32::try_from(i).expect("at most twenty batches") * 20)
        }).collect();
        let rows: Vec<_> = batches.iter().enumerate().filter(|(i, _)| selected & (1 << i) != 0).map(|(_, &row)| row).collect();
        let bytes = u64::try_from(batches.len()).expect("at most twenty batches") * 20;
        check_seek(&rows, &batches, target, 1000, bytes);
    }
}

#[test]
fn offset_seek_requires_truthful_rows_and_covers_extremes() {
    let batches = [(10, 0), (20, 20), (u32::MAX, u32::MAX - 1)];
    for rows in [
        &[][..],
        &batches[..],
        &batches[1..],
        &[(0, 20)][..],
        &[(10, 20)][..],
        &[(20, 0)][..],
        &[(10, 0), (10, 20)][..],
        &[(10, 0), (20, 0)][..],
        &[(10, 0), (20, u32::MAX)][..],
    ] {
        for target in [0, 5, 10, 11, 20, 21, u32::MAX] {
            check_seek(
                rows,
                &batches,
                target,
                i64::from(u32::MAX),
                u64::from(u32::MAX),
            );
        }
    }
    // In-bounds, strictly ordered but false row would skip the first match.
    assert!(validated_index_bounds_lookup(&[(0, 20)], 5, 20, 40) == Ok((20, None)));
    assert!(
        indexed_offset_scan_preserves_first_batch(&[(0, 20)], &[(10, 0), (20, 20)], 5, 20, 40)
            == Err(())
    );
    for target in [0, u32::MAX] {
        check_seek(&[], &[], target, i64::MIN, 0);
        check_seek(&[], &[], target, 0, 1);
        check_seek(&[], &[(10, 1)], target, 10, 20);
        check_seek(&[], &[(20, 0), (10, 20)], target, 20, 40);
        check_seek(&[(10, 0)], &[(10, 0)], target, 9, 40);
        check_seek(&[(10, 0)], &[(10, 0)], target, 10, 0);
    }
}
