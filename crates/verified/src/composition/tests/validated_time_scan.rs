use {super::*, assert2::assert};

pub(super) fn prefix_oracle(entries: &[(i64, u32)], target: i64) -> (usize, u32) {
    let count = entries
        .iter()
        .enumerate()
        .position(|(i, &(time, offset))| time >= target || (i > 0 && entries[i - 1].1 >= offset))
        .unwrap_or(entries.len());
    (count, count.checked_sub(1).map_or(0, |i| entries[i].1))
}

pub(super) fn archive_oracle(entries: &[(i64, u32)], maximum: i64) -> bool {
    entries
        .iter()
        .all(|&(_, offset)| i64::from(offset) <= maximum)
        && entries
            .windows(2)
            .all(|rows| rows[0].0 <= rows[1].0 && rows[0].1 < rows[1].1)
}

fn check(entries: &[(i64, u32)], offsets: &[u32], times: &[i64], max: i64, min: u32, target: i64) {
    let (count, floor) = prefix_oracle(entries, target);
    let archive_valid = archive_oracle(entries, max);
    assert!(
        validated_remote_and_local_time_starts_agree(entries, max, target)
            == archive_valid.then_some((count, floor, floor)).ok_or(())
    );
    let bounds = offsets.len() == times.len()
        && entries.iter().all(|&(bound, coordinate)| {
            offsets
                .iter()
                .zip(times)
                .all(|(&offset, &time)| offset >= coordinate || time <= bound)
        });
    let ordered = offsets.windows(2).all(|pair| pair[0] < pair[1]);
    if bounds && ordered {
        let first = times.iter().position(|time| *time >= target);
        assert!(
            remote_timestamp_scan_preserves_first(entries, offsets, times, target)
                == (count, floor, first)
        );
    }
    let valid = archive_valid
        && bounds
        && ordered
        && offsets.iter().all(|&offset| i64::from(offset) <= max);
    let first = offsets
        .iter()
        .zip(times)
        .position(|(&offset, &time)| offset >= min && time >= target);
    let expected = valid.then_some((count, floor, floor, first)).ok_or(());
    assert!(
        validated_retained_time_scan_agrees(entries, offsets, times, max, min, target) == expected
    );
}

proptest! {
    #[test]
    fn checked_time_scan_rejects_exactly_invalid_decoded_inputs(
        rows in proptest::collection::vec((any::<i64>(), any::<u32>()), 0..8),
        offsets in proptest::collection::vec(any::<u32>(), 0..8),
        times in proptest::collection::vec(any::<i64>(), 0..8),
        max in any::<i64>(), min in any::<u32>(), target in any::<i64>(),
    ) {
        check(&rows, &offsets, &times, max, min, target);
    }

    #[test]
    fn validated_time_scan_matches_complete_retained_record_oracle(
        records in proptest::collection::btree_map(any::<u32>(), any::<i64>(), 0..24),
        min in any::<u32>(), target in any::<i64>(), step in 1usize..8,
    ) {
        let (offsets, times, rows) = indexed_timestamp_records(&records, step);
        check(&rows, &offsets, &times, i64::from(u32::MAX), min, target);
    }
}

#[test]
fn checked_time_scan_rejects_structurally_valid_false_bounds_and_padding() {
    let offsets = [0, 3, 6];
    let times = [100, 300, 200];
    let misleading = [(0, 3)];
    assert!(validated_remote_and_local_time_starts_agree(&misleading, 6, 100) == Ok((1, 3, 3)));
    assert!(
        validated_retained_time_scan_agrees(&misleading, &offsets, &times, 6, 0, 100) == Err(())
    );
    for rows in [
        &[][..],
        &[(100, 0), (300, 3), (300, 6)],
        &misleading,
        &[(100, 0), (300, 3), (300, 6), (0, 0)],
        &[(500, 0), (100, 3), (300, 6)],
        &[(i64::MIN, 0), (0, 0)],
    ] {
        for min in [0, 1, 3, 4, 6, 7, u32::MAX] {
            for target in [i64::MIN, 0, 100, 200, 300, 301, i64::MAX] {
                check(rows, &offsets, &times, 6, min, target);
            }
        }
    }
    // Pruning offset zero must not hide the retained match at offset three.
    assert!(
        validated_retained_time_scan_agrees(&[(100, 0), (300, 3)], &offsets, &times, 6, 1, 100)
            == Ok((0, 0, 0, Some(1)))
    );
}

#[test]
fn checked_time_scan_covers_empty_malformed_and_extreme_inputs() {
    for max in [-1, 0, i64::MAX] {
        check(&[], &[], &[], max, u32::MAX, i64::MIN);
        check(&[(i64::MIN, 0)], &[], &[], max, 0, i64::MAX);
        check(&[], &[0], &[], max, 0, 0);
        check(&[], &[], &[0], max, 0, 0);
        check(&[], &[0, 0], &[1, 2], max, 0, 0);
        check(&[], &[1, 0], &[1, 2], max, 0, 0);
    }
    let offsets = [0, 1, u32::MAX];
    let times = [i64::MAX, i64::MIN, i64::MAX];
    let rows = [(i64::MAX, 0), (i64::MAX, 1), (i64::MAX, u32::MAX)];
    for min in [0, 1, 2, u32::MAX] {
        for target in [i64::MIN, 0, i64::MAX] {
            check(&rows, &offsets, &times, i64::from(u32::MAX), min, target);
            check(&rows, &offsets, &times, 1, min, target);
        }
    }
}
