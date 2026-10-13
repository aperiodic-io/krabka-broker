use {super::*, std::vec};

fn rational_trace(
    initial: (u64, u64, u64),
    start: u64,
    steps: &[(u64, u64)],
    rate: u64,
    burst: u64,
    units: u64,
) -> (u128, (u64, u64, u64, u64), u128) {
    let mut balance = i128::from(initial.0) - i128::from(initial.1);
    let mut fraction = initial.2;
    let mut last_clock = start;
    let mut granted = 0_u128;
    let mut lost = 0_u128;
    for &(clock, request) in steps {
        let next = last_clock.max(clock);
        let earned = u128::from(next - last_clock) * u128::from(rate);
        let fractional = fraction + (earned % 1_000_000_000) as u64;
        balance += i128::try_from(earned / 1_000_000_000).unwrap()
            + i128::from(fractional / 1_000_000_000);
        fraction = fractional % 1_000_000_000;
        if balance >= i128::from(burst) {
            lost += u128::try_from(balance - i128::from(burst)).unwrap() * 1_000_000_000
                + u128::from(fraction);
            balance = i128::from(burst);
            fraction = 0;
        }
        let whole = u128::try_from(balance.max(0)).unwrap() / u128::from(units);
        let grant = whole.min(u128::from(request)) * u128::from(units);
        granted += grant / u128::from(units);
        balance -= i128::try_from(grant).unwrap();
        last_clock = next;
    }
    (
        granted,
        (
            u64::try_from(balance.max(0)).unwrap(),
            u64::try_from((-balance).max(0)).unwrap(),
            fraction,
            last_clock,
        ),
        lost,
    )
}

proptest! {
    #[test]
    fn consuming_traces_match_a_signed_rational_ledger(
        (initial, burst) in token_balance_cases(),
        start in prop_oneof![Just(0_u64), any::<u64>()],
        rate in prop_oneof![1_u64..2_000_000_000, 1_u64..=u64::MAX],
        units in prop_oneof![1_u64..1_000_001, 1_u64..=u64::MAX],
        steps in proptest::collection::vec((
            prop_oneof![0_u64..10_000_000_000, any::<u64>()],
            prop_oneof![0_u64..100, any::<u64>()]), 0..20),
    ) {
        prop_assert_eq!(metered_consumes_conserve_elapsed_credit(
            initial, start, &steps, rate, burst, units),
            rational_trace(initial, start, &steps, rate, burst, units));
    }
}

#[test]
fn trace_conservation_covers_spending_fractional_credit_and_cap_loss() {
    for (initial, start, steps, rate, burst, units, expected) in [
        (
            (0, 0, 0),
            0,
            vec![(1, 10), (1, 10), (2, 10)],
            1_500_000_000,
            10,
            1,
            (3, (0, 0, 0, 2), 0),
        ),
        (
            (0, 0, 0),
            0,
            vec![(1, 10), (2, 10)],
            1_500_000_000,
            1,
            1,
            (2, (0, 0, 0, 2), 1_000_000_000),
        ),
        (
            (0, 7, 0),
            0,
            vec![(3, 10), (3, 10), (2, 10), (6, 10)],
            1_500_000_000,
            10,
            2,
            (1, (0, 0, 0, 6), 0),
        ),
        (
            (5, 0, 500_000_000),
            7,
            vec![],
            u64::MAX,
            10,
            2,
            (0, (5, 0, 500_000_000, 7), 0),
        ),
        (
            (0, u64::MAX, 0),
            0,
            vec![(u64::MAX, u64::MAX), (0, u64::MAX)],
            u64::MAX,
            u64::MAX,
            u64::MAX,
            rational_trace(
                (0, u64::MAX, 0),
                0,
                &[(u64::MAX, u64::MAX), (0, u64::MAX)],
                u64::MAX,
                u64::MAX,
                u64::MAX,
            ),
        ),
    ] {
        assert2::assert!(
            metered_consumes_conserve_elapsed_credit(initial, start, &steps, rate, burst, units)
                == expected
        );
    }
}
