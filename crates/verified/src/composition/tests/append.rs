use {
    super::{
        append_frontiers_agree, reservations_do_not_overlap,
        reserved_pair_preserves_recovery_and_ack_order,
    },
    crate::composition::append::AppendFrontiers,
    assert2::assert,
    proptest::prelude::*,
};

fn append_oracle(base: i64, delta: i32) -> Option<AppendFrontiers> {
    let last = i128::from(base) + i128::from(delta);
    let end = i64::try_from(last + 1).ok()?;
    if base < 0 || delta < 0 {
        return None;
    }
    let last = i64::try_from(last).ok()?;
    Some(AppendFrontiers {
        append: (last, end),
        reservation: (base, end),
        recovery: (last, end),
        acknowledgement: end,
        scan: end,
    })
}

fn reservation_oracle(base: i64, first: i64, second: i64) -> Option<(i64, i64, i64)> {
    if base < 0 || first <= 0 || second <= 0 {
        return None;
    }
    let split = i128::from(base) + i128::from(first);
    let end = split + i128::from(second);
    Some((base, i64::try_from(split).ok()?, i64::try_from(end).ok()?))
}

fn pair_oracle(
    base: i64,
    first_delta: i32,
    second_delta: i32,
) -> Option<(AppendFrontiers, AppendFrontiers)> {
    let first = append_oracle(base, first_delta)?;
    let second = append_oracle(first.acknowledgement, second_delta)?;
    Some((first, second))
}

proptest! {
    #[test]
    fn append_frontier_witnesses_match_wide_arithmetic(base in any::<i64>(), delta in any::<i32>()) {
        assert!(append_frontiers_agree(base, delta) == append_oracle(base, delta));
    }

    #[test]
    fn reservation_witnesses_match_wide_arithmetic(
        base in any::<i64>(), first in any::<i64>(), second in any::<i64>(),
    ) {
        assert!(reservations_do_not_overlap(base, first, second)
            == reservation_oracle(base, first, second));
    }

    #[test]
    fn reserved_pair_matches_independent_batch_geometry(
        base in any::<i64>(), first_delta in any::<i32>(), second_delta in any::<i32>(),
    ) {
        assert!(reserved_pair_preserves_recovery_and_ack_order(base, first_delta, second_delta)
            == pair_oracle(base, first_delta, second_delta));
    }

}

#[yare::parameterized(
    empty_append = { 0, 0 },
    ordinary_append = { 10, 2 },
    max_minus_one_empty = { i64::MAX - 1, 0 },
    max_empty = { i64::MAX, 0 },
    exclusive_end_overflow = { i64::MAX - 1, 1 },
    negative_base = { -1, 0 },
    negative_delta = { 0, -1 },
    maximum_delta = { 0, i32::MAX },
)]
fn append_witnesses_cover_zero_width_rejection_and_exclusive_end_overflow(base: i64, delta: i32) {
    assert2::assert!(append_frontiers_agree(base, delta) == append_oracle(base, delta));
}

#[yare::parameterized(
    ordinary = { 0, 2, 3 },
    last_valid_end = { i64::MAX - 2, 1, 1 },
    end_overflow = { i64::MAX - 1, 1, 1 },
    zero_first = { 0, 0, 1 },
    zero_second = { 0, 1, 0 },
    negative_second = { 0, 1, -1 },
    negative_base = { -1, 1, 1 },
    maximum_inputs = { 0, i64::MAX, i64::MAX },
    final_exclusive_overflow = { 0, i64::MAX - 1, 1 },
)]
fn reservations_reject_overlap_and_overflow(base: i64, first: i64, second: i64) {
    assert2::assert!(
        reservations_do_not_overlap(base, first, second) == reservation_oracle(base, first, second)
    );
}

#[yare::parameterized(
    empty_pair = { 0, 0, 0 },
    adjacent_pair = { 10, 2, 3 },
    last_empty_offset = { i64::MAX - 2, 0, 0 },
    near_max_empty_offset = { i64::MAX - 1, 0, 0 },
    maximum_deltas = { 0, i32::MAX, i32::MAX },
    negative_base = { -1, 0, 0 },
    negative_first_delta = { 0, -1, 0 },
    negative_second_delta = { 0, 0, -1 },
)]
fn reserved_pair_covers_adjacent_last_offsets_and_second_batch_overflow(
    base: i64,
    first_delta: i32,
    second_delta: i32,
) {
    assert2::assert!(
        reserved_pair_preserves_recovery_and_ack_order(base, first_delta, second_delta)
            == pair_oracle(base, first_delta, second_delta)
    );
}
