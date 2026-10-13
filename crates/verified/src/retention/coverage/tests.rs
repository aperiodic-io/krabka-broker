use {super::remote_covered_through, assert2::assert, proptest::prelude::*};

pub(crate) fn coverage_oracle(ranges: &[(i64, i64)], anchor: i64) -> Option<i64> {
    if ranges.iter().any(|(start, end)| start > end) {
        return None;
    }
    let end = ranges.iter().map(|(_, end)| *end).max()?;
    (anchor..=end)
        .take_while(|offset| {
            ranges
                .iter()
                .any(|(start, end)| start <= offset && offset <= end)
        })
        .last()
}

proptest! {
    #[test]
    fn actual_coverage_matches_independent_coordinate_enumeration(
        mut ranges in prop::collection::vec((-20i64..61, -20i64..61), 0..16), anchor in -20i64..61,
    ) {
        ranges.sort_unstable();
        assert!(remote_covered_through(&ranges, anchor) == coverage_oracle(&ranges, anchor));
        let valid: Vec<_> = ranges.iter().map(|&(start, end)| (start, start.max(end))).collect();
        assert!(remote_covered_through(&valid, anchor) == coverage_oracle(&valid, anchor));
    }
}

#[test]
fn coverage_ignores_obsolete_prefixes_but_rejects_real_gaps_and_malformed_tails() {
    assert!(remote_covered_through(&[(0, 9), (20, 29)], 20) == Some(29));
    assert!(remote_covered_through(&[(0, 9)], 20) == None);
    assert!(remote_covered_through(&[(0, 9), (20, 29)], 0) == Some(9));
    assert!(remote_covered_through(&[(0, 9), (5, 24), (25, 29)], 20) == Some(29));
    assert!(remote_covered_through(&[(0, 9), (20, 19)], 0) == None);
    assert!(remote_covered_through(&[], 0) == None);
}

#[test]
fn coverage_is_inclusive_and_never_overflows_at_signed_extremes() {
    assert!(
        remote_covered_through(&[(i64::MIN, i64::MIN), (i64::MIN + 1, 0)], i64::MIN) == Some(0)
    );
    assert!(
        remote_covered_through(
            &[(i64::MAX - 4, i64::MAX - 2), (i64::MAX - 1, i64::MAX)],
            i64::MAX - 3
        ) == Some(i64::MAX)
    );
    assert!(
        remote_covered_through(
            &[(i64::MAX - 4, i64::MAX - 3), (i64::MAX - 1, i64::MAX)],
            i64::MAX - 4
        ) == Some(i64::MAX - 3)
    );
    assert!(remote_covered_through(&[(i64::MAX, i64::MAX)], i64::MAX) == Some(i64::MAX));
}
