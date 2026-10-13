use {super::*, assert2::assert};

fn cursor_oracle(
    entries: &[(i64, u32)],
    base: i64,
    end: i64,
    lower: i64,
    upper: i64,
) -> Result<(i64, i64, i64), ()> {
    let valid = base >= 0
        && end
            .checked_sub(base)
            .is_some_and(|width| (0..=i64::from(u32::MAX)).contains(&width))
        && validated_time_scan::archive_oracle(entries, end.saturating_sub(base));
    if !valid {
        return Err(());
    }
    let inclusive = |target| {
        base + entries
            .iter()
            .rfind(|&&(time, _)| time <= target)
            .map_or(0, |&(_, offset)| i64::from(offset))
    };
    let strict = base
        + entries
            .iter()
            .rfind(|&&(time, _)| time < lower)
            .map_or(0, |&(_, offset)| i64::from(offset));
    Ok((inclusive(lower), inclusive(upper), strict))
}

pub(super) fn check_cursors(entries: &[(i64, u32)], base: i64, end: i64, lower: i64, upper: i64) {
    assert!(
        validated_time_cursors_are_monotone(entries, base, end, lower, upper)
            == cursor_oracle(entries, base, end, lower, upper)
    );
}

fn check_range(window: SparseTimestampWindow<'_>, bounds: (i64, i64, i64), targets: (i64, i64)) {
    let (offsets, times, _) = window;
    let (base, end, floor) = bounds;
    let (lower, upper) = targets;
    let entries: Vec<_> = prefix_maxima(window);
    check_cursors(&entries, base, end, lower, upper);
    let expected = cursor_oracle(&entries, base, end, lower, upper).and_then(|(lo, hi, scan)| {
        if offsets.iter().any(|&offset| base + i64::from(offset) > end) {
            return Err(());
        }
        let first = offsets.iter().zip(times).position(|(&offset, &time)| {
            base + i64::from(offset) >= floor && lower <= time && time <= upper
        });
        Ok((lo, hi, scan, first))
    });
    assert!(constructed_time_range_preserves_first(window, bounds, targets) == expected);
}

proptest! {
    #[test]
    fn validated_cursors_match_independent_last_row_oracles(
        rows in prop::collection::vec((any::<i64>(), any::<u32>()), 0..8),
        base in any::<i64>(), end in any::<i64>(), a in any::<i64>(), b in any::<i64>(),
        records in prop::collection::btree_map(any::<u32>(), any::<i64>(), 0..16),
    ) {
        check_cursors(&rows, base, end, a.min(b), a.max(b));
        let mut maximum = i64::MIN;
        let valid: Vec<_> = records.iter().map(|(&offset, &time)| {
            maximum = maximum.max(time); (maximum, offset)
        }).collect();
        check_cursors(&valid, 0, i64::from(u32::MAX), a.min(b), a.max(b));
    }

    #[test]
    fn constructed_interval_matches_full_retained_scan_despite_timestamp_regressions(
        records in prop::collection::btree_map(any::<u32>(), any::<i64>(), 0..24),
        base in 0i64..100, width in any::<u32>(), floor in any::<u32>(),
        a in any::<i64>(), b in any::<i64>(), step in 1usize..8,
    ) {
        let floor = base + i64::from(floor);
        let (offsets, times, rows) = timestamp_records(&records, step, 0);
        check_range((&offsets, &times, &rows), (base, base + i64::from(width), floor), (a.min(b), a.max(b)));
        check_range((&offsets, &times, &rows), (base, base + i64::from(u32::MAX), floor), (a.min(b), a.max(b)));
    }
}

#[test]
fn inclusive_cursors_cannot_crop_a_timestamp_range_scan() {
    let offsets = [0, 3, 6];
    let rows = [(0, 0), (1, 1), (2, 2)];
    let regressions = [100, 300, 200];
    let window = (&offsets[..], &regressions[..], &rows[..]);
    check_range(window, (0, 6, 0), (200, 250));
    assert!(
        constructed_time_range_preserves_first(window, (0, 6, 0), (200, 250))
            == Ok((0, 0, 0, Some(2)))
    );
    let repeated = [100, 100, 300];
    let window = (&offsets[..], &repeated[..], &rows[..]);
    assert!(
        constructed_time_range_preserves_first(window, (0, 6, 0), (100, 100))
            == Ok((3, 3, 0, Some(0)))
    );
    for floor in [0, 1, 3, 4, 6, 7] {
        check_range(window, (0, 6, floor), (100, 100));
        check_range((&offsets, &regressions, &[]), (0, 6, floor), (200, 250));
        check_range(
            (&offsets, &regressions, &[(0, 2)]),
            (0, 6, floor),
            (200, 250),
        );
    }
}

#[test]
fn time_range_rejects_invalid_extents_and_preserves_signed_extremes() {
    for (base, end) in [(0, -1), (2, 1), (0, i64::MAX), (i64::MAX, i64::MAX)] {
        check_range((&[], &[], &[]), (base, end, 0), (i64::MIN, i64::MAX));
    }
    for end in [0, 1, i64::from(u32::MAX)] {
        check_range(
            (&[0, 1, u32::MAX], &[i64::MAX, i64::MIN, i64::MAX], &[(
                0, 0,
            )]),
            (0, end, 1),
            (i64::MIN, i64::MIN),
        );
    }
    check_range(
        (&[0], &[i64::MIN], &[(0, 0)]),
        (i64::MAX, i64::MAX, i64::MAX),
        (i64::MIN, i64::MIN),
    );
    check_range(
        (&[0], &[i64::MAX], &[(0, 0)]),
        (i64::MAX, i64::MAX, i64::MAX),
        (i64::MAX, i64::MAX),
    );
    for lower in [i64::MIN, 0, i64::MAX] {
        check_cursors(
            &[(i64::MIN, 0), (i64::MAX, u32::MAX)],
            0,
            i64::from(u32::MAX),
            lower,
            i64::MAX,
        );
        check_cursors(&[(i64::MIN, 1)], -1, 1, lower, i64::MAX);
    }
}
