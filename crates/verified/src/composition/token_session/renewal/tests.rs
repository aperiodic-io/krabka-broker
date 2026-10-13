use {super::*, proptest::prelude::*};

fn saturated(value: i128) -> i64 { i64::try_from(value.min(i128::from(i64::MAX))).unwrap() }

// Wide closed-form deadlines and a symbolic extension/no-op schedule. No
// production deadline, mutation, credential or request policy is called.
fn expected(
    issued: i64,
    periods: (i64, i64, i64, i64, i64),
    delays: (i64, i64, i64),
    cap: i64,
    tail: bool,
    api: i16,
) -> RenewalTrace {
    let lifetime = if periods.0 > 0 {
        periods.0.min(periods.1)
    } else {
        periods.1
    };
    let maximum = saturated(i128::from(issued) + i128::from(lifetime));
    let original = saturated(i128::from(issued) + i128::from(periods.2)).min(maximum);
    let renewal_period = if periods.3 > 0 {
        periods.3.min(periods.4)
    } else {
        periods.4
    };
    let renewed = saturated(i128::from(original) + i128::from(renewal_period)).min(maximum);
    let extends = original < maximum;
    let renewal = if tail {
        TokenMutationDecision::Reject
    } else if extends {
        TokenMutationDecision::Append
    } else {
        TokenMutationDecision::Retry
    };
    let cleanup = if tail || extends {
        TokenMutationDecision::Reject
    } else {
        TokenMutationDecision::Append
    };
    let completed = i128::from(original) + i128::from(delays.1);
    let session = if tail || completed >= i128::from(renewed) {
        None
    } else {
        let at = if cap > 0 {
            saturated(completed + i128::from(cap)).min(renewed)
        } else {
            renewed
        };
        let lifetime = i64::try_from(i128::from(at) - completed).unwrap();
        let admitted =
            matches!(api, 17 | 36) || i128::from(original) + i128::from(delays.2) < i128::from(at);
        Some((at, lifetime, admitted))
    };
    RenewalTrace {
        original_expiry: original,
        maximum,
        renewed_expiry: renewed,
        boundary_authenticated: false,
        renewal,
        cleanup,
        session,
    }
}

#[test]
fn renewal_at_exact_expiry_preserves_service_against_old_cleanup() {
    let periods = (0, 2000, 1000, 500, 500);
    let trace = renewed_token_survives_captured_cleanup(0, periods, (0, 250, 499), 0, false, 0);
    assert2::assert!(trace.original_expiry == 1000 && trace.renewed_expiry == 1500);
    assert2::assert!(!trace.boundary_authenticated);
    assert2::assert!(trace.renewal == TokenMutationDecision::Append);
    assert2::assert!(trace.cleanup == TokenMutationDecision::Reject);
    assert2::assert!(trace.session == Some((1500, 250, true)));
    let expired = renewed_token_survives_captured_cleanup(0, periods, (0, 250, 500), 0, false, 0);
    assert2::assert!(expired.session == Some((1500, 250, false)));
    let late = renewed_token_survives_captured_cleanup(0, periods, (0, 500, 500), 0, false, 36);
    assert2::assert!(late.session == None);
}

#[test]
fn token_renewal_schedules_match_independent_deadline_oracle() {
    for issued in [0, 100, i64::MAX - 4, i64::MAX] {
        for periods in [
            (0, 2000, 1000, 500, 500),
            (0, 2000, 3000, 1, 500),
            (1, 2000, 1000, 0, 500),
            (-1, i64::MAX, 1, 1, i64::MAX),
            (i64::MAX, i64::MAX, i64::MAX, i64::MAX, i64::MAX),
        ] {
            let oracle = expected(issued, periods, (0, 0, 0), 0, false, 0);
            let width = oracle.renewed_expiry - oracle.original_expiry;
            for delays in [
                (0, 0, 0),
                (0, 0, 1),
                (0, 1, i64::MAX),
                (0, width, width),
                (0, width.saturating_sub(1).max(0), width),
            ] {
                for cap in [-1, 0, 1, 100, i64::MAX] {
                    for tail in [false, true] {
                        for api in [i16::MIN, 0, 17, 18, 36, 55, i16::MAX] {
                            let actual = renewed_token_survives_captured_cleanup(
                                issued, periods, delays, cap, tail, api,
                            );
                            assert2::assert!(
                                actual == expected(issued, periods, delays, cap, tail, api)
                            );
                        }
                    }
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn arbitrary_renewal_schedules_preserve_generation_and_session_bounds(
        issued in 0_i64..=i64::MAX, ceiling in 1_i64..=i64::MAX,
        initial in 1_i64..=i64::MAX, default in 1_i64..=i64::MAX,
        requested in any::<i64>(), renewal in any::<i64>(), cap in any::<i64>(),
        d0 in 0_i64..=i64::MAX, d1 in 0_i64..=i64::MAX, d2 in 0_i64..=i64::MAX,
        tail in any::<bool>(), api in any::<i16>(),
    ) {
        let mut delays = [d0, d1, d2]; delays.sort_unstable();
        let delays = (delays[0], delays[1], delays[2]);
        let periods = (requested, ceiling, initial, renewal, default);
        prop_assert_eq!(renewed_token_survives_captured_cleanup(issued, periods, delays, cap, tail, api),
            expected(issued, periods, delays, cap, tail, api));
    }
}
