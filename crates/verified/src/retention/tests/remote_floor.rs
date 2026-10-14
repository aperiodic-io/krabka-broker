use {super::*, assert2::assert, proptest::prelude::*};

proptest! {
    #[test]
    fn floor_step_matches_wide_integer_oracle(
        current in any::<i64>(), contiguous in any::<bool>(),
        start in any::<i64>(), end in any::<i64>(),
    ) {
        let target = i128::from(end) + 1;
        let expected = if contiguous && current >= 0 && start >= 0 && start <= end
            && start <= current && target <= i128::from(i64::MAX) {
            (i64::try_from(i128::from(current).max(target)).unwrap(), true)
        } else { (current, false) };
        assert!(remote_retention_floor_step(current, contiguous, start, end) == expected);
    }
}

#[yare::parameterized(
    previous_floor = { 10, true, 0, 4, (10, true) },
    advances = { 10, true, 10, 14, (15, true) },
    gap = { 10, true, 11, 14, (10, false) },
    noncontiguous = { 10, false, 0, 14, (10, false) },
    max_successor = { 0, true, 0, i64::MAX - 1, (i64::MAX, true) },
    successor_overflow = { 0, true, 0, i64::MAX, (0, false) },
    current_max = { i64::MAX, true, 0, i64::MAX, (i64::MAX, false) },
    negative_start = { 0, true, -1, 2, (0, false) },
    inverted_range = { 10, true, 2, 1, (10, false) },
)]
fn floor_step_never_resumes_after_a_gap_or_overflow(
    current: i64,
    contiguous: bool,
    start: i64,
    end: i64,
    expected: (i64, bool),
) {
    assert2::assert!(remote_retention_floor_step(current, contiguous, start, end) == expected);
}
