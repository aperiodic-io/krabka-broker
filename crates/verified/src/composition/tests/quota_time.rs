use super::*;

// Split seconds from subsecond credit so even a u128 wait needs no wide
// multiplication. Clipping above u64::MAX cannot affect the retained cap.
fn rational_cap(wait: u128, rate: u64) -> u64 {
    const SECOND: u128 = 1_000_000_000;
    u64::try_from(
        (wait / SECOND)
            .saturating_mul(u128::from(rate))
            .saturating_add(wait % SECOND * u128::from(rate) / SECOND)
            .min(u128::from(u64::MAX)),
    )
    .unwrap()
}

fn signed_deadline_ledger(
    initial: (u64, u64, u64),
    charge: (u64, u128),
    elapsed: (u64, u64),
    rate: u64,
    burst: u64,
    probe: u64,
    units: u64,
) -> (u64, u64, u64, u64, u64, u64) {
    let cap = rational_cap(charge.1, rate);
    let charged_balance = i128::from(initial.0) - i128::from(initial.1) - i128::from(charge.0);
    let retained_debt = (-charged_balance).max(0).min(i128::from(cap));
    let numerator = u128::from(elapsed.0 + elapsed.1) * u128::from(rate) + u128::from(initial.2);
    let balance =
        charged_balance.max(0) - retained_debt + i128::try_from(numerator / 1_000_000_000).unwrap();
    let available = u64::try_from(balance.max(0).min(i128::from(burst))).unwrap();
    let whole = probe.min(available / units);
    (
        cap,
        u64::try_from(retained_debt).unwrap(),
        whole,
        available - whole * units,
        u64::try_from((-balance).max(0)).unwrap(),
        if balance >= i128::from(burst) {
            0
        } else {
            u64::try_from(numerator % 1_000_000_000).unwrap()
        },
    )
}

#[yare::parameterized(
    fractional_credit = {
        1,
        500_000_000,
        (250_000_000, 250_000_000),
        (0, 0, 0, 0, 0, 500_000_000),
    },
    whole_token_repaid = {
        3,
        500_000_000,
        (250_000_000, 250_000_000),
        (1, 1, 0, 0, 0, 500_000_000),
    },
    zero_elapsed = { 3, 500_000_000, (0, 0), (1, 1, 0, 0, 1, 0) },
    two_tokens_earned = { 3, 500_000_000, (500_000_000, 500_000_000), (1, 1, 2, 0, 0, 0) },
)]
fn a_deadline_releases_debt_without_forgetting_earned_capacity(
    rate: u64,
    wait: u128,
    elapsed: (u64, u64),
    expected: (u64, u64, u64, u64, u64, u64),
) {
    assert2::assert!(
        capped_charge_is_repaid_by_elapsed_time(
            (0, 0, 0),
            (u64::MAX, wait),
            elapsed,
            rate,
            10,
            10,
            1,
        ) == expected
    );
}

#[test]
fn deadline_composition_covers_saturation_fraction_and_token_quantum() {
    // Credit covers the retained cap and every whole token in this probe;
    // the burst leaves a fractional token rather than denying service.
    assert2::assert!(
        capped_charge_is_repaid_by_elapsed_time(
            (0, 0, 0),
            (u64::MAX, 500_000_000),
            (2_000_000_000, 2_000_000_000),
            3,
            10,
            3,
            3,
        ) == (1, 1, 3, 1, 0, 0)
    );
    for burst in [0, 1, 10, u64::MAX] {
        for initial in [
            (0, 0, 0),
            (burst, 0, 999_999_999),
            (0, u64::MAX, 500_000_000),
        ] {
            for rate in [1, 3, 1_500_000_000, u64::MAX] {
                for wait in [
                    0,
                    500_000_000,
                    999_999_999,
                    1_000_000_000,
                    u128::from(u64::MAX),
                    u128::MAX,
                ] {
                    for elapsed in [
                        (0, 0),
                        (1, 1),
                        (250_000_000, 250_000_000),
                        (0, u64::MAX),
                        (u64::MAX / 2, u64::MAX / 2),
                    ] {
                        for request in [0, 1, u64::MAX] {
                            for units in [1, 1_000_000, u64::MAX] {
                                let charge = (request, wait);
                                let actual = capped_charge_is_repaid_by_elapsed_time(
                                    initial,
                                    charge,
                                    elapsed,
                                    rate,
                                    burst,
                                    u64::MAX,
                                    units,
                                );
                                assert2::assert!(
                                    actual
                                        == signed_deadline_ledger(
                                            initial,
                                            charge,
                                            elapsed,
                                            rate,
                                            burst,
                                            u64::MAX,
                                            units
                                        )
                                );
                                if wait <= u128::from(elapsed.0 + elapsed.1) {
                                    assert2::assert!(actual.4 == 0);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn capped_elapsed_quota_matches_an_independent_rational_ledger(
        raw_available in any::<u64>(), debt in any::<u64>(), owes in any::<bool>(),
        fraction in 0_u64..1_000_000_000, burst in any::<u64>(),
        request in any::<u64>(), wait in prop_oneof![0_u128..2_000_000_000, any::<u128>()],
        first in any::<u64>(), raw_second in any::<u64>(),
        rate in 1_u64..=u64::MAX, probe in any::<u64>(), units in 1_u64..=u64::MAX,
    ) {
        let initial = if owes { (0, debt, fraction) } else { (raw_available.min(burst), 0, fraction) };
        let elapsed = (first, raw_second.min(u64::MAX - first));
        prop_assert_eq!(capped_charge_is_repaid_by_elapsed_time(initial, (request, wait), elapsed, rate, burst, probe, units),
            signed_deadline_ledger(initial, (request, wait), elapsed, rate, burst, probe, units));
    }
}
