use super::*;

proptest! {
    #[test]
    fn partitioned_refill_matches_a_rational_elapsed_time_ledger(
        (initial, burst) in token_balance_cases(),
        first in prop_oneof![0_u64..1_000_000_000, any::<u64>()],
        raw_second in prop_oneof![0_u64..1_000_000_000, any::<u64>()],
        rate in prop_oneof![0_u64..1_000_000_000, any::<u64>()], request in any::<u64>(),
    ) {
        let second = raw_second.min(u64::MAX - first);
        let numerator = u128::from(first + second) * u128::from(rate) + u128::from(initial.2);
        let balance = i128::from(initial.0) - i128::from(initial.1)
            + i128::try_from(numerator / 1_000_000_000).unwrap();
        let budget = u64::try_from(balance.max(0).min(i128::from(burst))).unwrap();
        let grant = request.min(budget);
        let expected = (
            grant, budget - grant, u64::try_from((-balance).max(0)).unwrap(),
            if balance >= i128::from(burst) { 0 } else { (numerator % 1_000_000_000) as u64 },
        );
        prop_assert_eq!(refill_partition_preserves_consume_budget(
            initial, (first, second), rate, burst, request), expected);
    }
}

#[yare::parameterized(
    fractional_refill = { (0, 0, 0), (1, 1), 1_500_000_000, 10, 10, (3, 0, 0, 0) },
    fractional_debt = { (0, 7, 500_000_000), (1, 0), 1_500_000_000, 10, 5, (0, 0, 5, 0) },
    retained_fraction = { (0, 0, 0), (1, 1), 3, 10, 10, (0, 0, 0, 6) },
    zero_burst = { (0, 7, 0), (1, 1), 3, 0, 10, (0, 0, 7, 6) },
    saturation_discards_fraction = { (0, 0, 0), (1, 1), 1_500_000_000, 1, 10, (1, 0, 0, 0) },
    maximum_values = {
        (0, u64::MAX, 0),
        (u64::MAX, 0),
        u64::MAX,
        u64::MAX,
        u64::MAX,
        (u64::MAX, 0, 0, 0),
    },
)]
fn refill_partition_covers_fractional_debt_and_saturation_boundaries(
    initial: (u64, u64, u64),
    elapsed: (u64, u64),
    rate: u64,
    burst: u64,
    request: u64,
    expected: (u64, u64, u64, u64),
) {
    assert2::assert!(
        refill_partition_preserves_consume_budget(initial, elapsed, rate, burst, request)
            == expected
    );
}
