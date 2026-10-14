use {super::*, assert2::assert};

pub(super) struct TimePrefix {
    pub(super) count: usize,
    pub(super) floor: u32,
}

pub(super) fn prefix_oracle(entries: &[TimeEntry], target: i64) -> TimePrefix {
    let count = entries
        .iter()
        .enumerate()
        .position(|(i, entry)| {
            entry.key >= target || (i > 0 && entries[i - 1].coordinate >= entry.coordinate)
        })
        .unwrap_or(entries.len());
    TimePrefix {
        count,
        floor: count.checked_sub(1).map_or(0, |i| entries[i].coordinate),
    }
}

pub(super) fn archive_oracle(entries: &[TimeEntry], maximum: i64) -> bool {
    entries
        .iter()
        .all(|entry| i64::from(entry.coordinate) <= maximum)
        && entries
            .windows(2)
            .all(|rows| rows[0].key <= rows[1].key && rows[0].coordinate < rows[1].coordinate)
}

fn check(entries: &[TimeEntry], offsets: &[u32], times: &[i64], max: i64, min: u32, target: i64) {
    let TimePrefix { count, floor } = prefix_oracle(entries, target);
    let archive_valid = archive_oracle(entries, max);
    assert!(
        validated_remote_and_local_time_starts_agree(entries, max, target)
            == archive_valid.then_some((count, floor, floor)).ok_or(())
    );
    let bounds = offsets.len() == times.len()
        && entries.iter().all(|entry| {
            offsets
                .iter()
                .zip(times)
                .all(|(&offset, &time)| offset >= entry.coordinate || time <= entry.key)
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
        rows in proptest::collection::vec((any::<i64>(), any::<u32>()).prop_map(|(key, coordinate)| TimeEntry::new(key, coordinate)), 0..8),
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
        let IndexedTimestampRecords { offsets, timestamps: times, entries: rows } = indexed_timestamp_records(&records, step);
        check(&rows, &offsets, &times, i64::from(u32::MAX), min, target);
    }
}

#[test]
fn checked_time_scan_rejects_structurally_valid_false_bounds_and_padding() {
    let offsets = [0, 3, 6];
    let times = [100, 300, 200];
    let misleading = [IndexEntry::new(0, 3)];
    assert!(validated_remote_and_local_time_starts_agree(&misleading, 6, 100) == Ok((1, 3, 3)));
    assert!(
        validated_retained_time_scan_agrees(&misleading, &offsets, &times, 6, 0, 100) == Err(())
    );
    for rows in [
        &[][..],
        &[
            IndexEntry::new(100, 0),
            IndexEntry::new(300, 3),
            IndexEntry::new(300, 6),
        ],
        &misleading,
        &[
            IndexEntry::new(100, 0),
            IndexEntry::new(300, 3),
            IndexEntry::new(300, 6),
            IndexEntry::new(0, 0),
        ],
        &[
            IndexEntry::new(500, 0),
            IndexEntry::new(100, 3),
            IndexEntry::new(300, 6),
        ],
        &[IndexEntry::new(i64::MIN, 0), IndexEntry::new(0, 0)],
    ] {
        for min in [0, 1, 3, 4, 6, 7, u32::MAX] {
            for target in [i64::MIN, 0, 100, 200, 300, 301, i64::MAX] {
                check(rows, &offsets, &times, 6, min, target);
            }
        }
    }
    // Pruning offset zero must not hide the retained match at offset three.
    assert!(
        validated_retained_time_scan_agrees(
            &[IndexEntry::new(100, 0), IndexEntry::new(300, 3)],
            &offsets,
            &times,
            6,
            1,
            100
        ) == Ok((0, 0, 0, Some(1)))
    );
}

#[yare::parameterized(
    negative_maximum = { -1 },
    zero_maximum = { 0 },
    maximum_coordinate = { i64::MAX },
)]
fn checked_time_scan_covers_empty_malformed_and_extreme_inputs(max: i64) {
    check(&[], &[], &[], max, u32::MAX, i64::MIN);
    check(&[IndexEntry::new(i64::MIN, 0)], &[], &[], max, 0, i64::MAX);
    check(&[], &[0], &[], max, 0, 0);
    check(&[], &[], &[0], max, 0, 0);
    check(&[], &[0, 0], &[1, 2], max, 0, 0);
    check(&[], &[1, 0], &[1, 2], max, 0, 0);
    let offsets = [0, 1, u32::MAX];
    let times = [i64::MAX, i64::MIN, i64::MAX];
    let rows = [
        IndexEntry::new(i64::MAX, 0),
        IndexEntry::new(i64::MAX, 1),
        IndexEntry::new(i64::MAX, u32::MAX),
    ];
    for min in [0, 1, 2, u32::MAX] {
        for target in [i64::MIN, 0, i64::MAX] {
            check(&rows, &offsets, &times, i64::from(u32::MAX), min, target);
            check(&rows, &offsets, &times, 1, min, target);
        }
    }
}
