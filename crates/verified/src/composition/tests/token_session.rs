use {
    super::*,
    crate::{
        authz::{sasl_session_expiry, session_expired_for_request},
        delegation_token::{TokenApi, TokenApiAdmission},
    },
};

fn session_oracle(now: i64, credential: Option<i64>, cap: Option<i64>) -> (Option<i64>, i64) {
    let ceiling = cap.filter(|cap| *cap > 0).map(|cap| {
        i64::try_from((i128::from(now) + i128::from(cap)).min(i128::from(i64::MAX))).unwrap()
    });
    let expiry = credential.into_iter().chain(ceiling).min();
    let lifetime = expiry.map_or(0, |at| {
        i64::try_from((i128::from(at) - i128::from(now)).clamp(0, i128::from(i64::MAX))).unwrap()
    });
    (expiry, lifetime)
}

fn check_trace(times: [i64; 4], periods: [i64; 4], api: i16, token_api: TokenApi) {
    let actual = created_token_session_bounds_requests(
        (times[0], times[1], times[2], times[3]),
        (periods[0], periods[1], periods[2], periods[3]),
        api,
        token_api,
    );
    let expected = if periods[1] <= 0 || periods[2] <= 0 {
        None
    } else {
        let lifetime = if periods[0] > 0 {
            periods[0].min(periods[1])
        } else {
            periods[1]
        };
        let maximum =
            i64::try_from((i128::from(times[0]) + i128::from(lifetime)).min(i128::from(i64::MAX)))
                .unwrap();
        let expiry =
            i64::try_from((i128::from(times[0]) + i128::from(periods[2])).min(i128::from(maximum)))
                .unwrap();
        if times[2] >= expiry {
            None
        } else {
            let (at, window) = session_oracle(times[2], Some(expiry), Some(periods[3]));
            let at = at.unwrap();
            Some((
                expiry,
                maximum,
                at,
                window,
                matches!(api, 17 | 36) || times[3] < at,
                TokenApiAdmission::Reject,
            ))
        }
    };
    assert2::assert!(
        actual == expected,
        "times={times:?} periods={periods:?} api={api}"
    );
}

#[test]
fn token_session_deadline_and_round_boundaries() {
    for times in [
        [100, 101, 105, 105],
        [100, 101, 105, 109],
        [100, 101, 105, 110],
        [100, 101, 110, 110],
        [100, 110, 110, 111],
        [100, 101, 105, 130],
        [i64::MAX - 4, i64::MAX - 3, i64::MAX - 1, i64::MAX],
        [0, 0, 0, 0],
        [i64::MAX; 4],
    ] {
        for periods in [
            [0, 30, 10, 0],
            [0, 30, 10, 2],
            [3, 30, 10, 20],
            [0, 30, 40, -1],
            [-1, i64::MAX, i64::MAX, i64::MAX],
            [0, 0, 10, 20],
            [0, 30, 0, 20],
        ] {
            for api in [-1, 0, 1, 17, 18, 36, 38, 39, 40, 41, i16::MAX] {
                for token_api in [
                    TokenApi::Create,
                    TokenApi::Renew,
                    TokenApi::Expire,
                    TokenApi::Describe,
                ] {
                    check_trace(times, periods, api, token_api);
                }
            }
        }
    }
    for now in [0, 1, i64::MAX - 1, i64::MAX] {
        for cap in [1, 30_000, i64::MAX] {
            for request in [now, i64::MAX] {
                for api in [0, 17, 18, 36, 41] {
                    let (at, window) = session_oracle(now, None, Some(cap));
                    let at = at.unwrap();
                    assert2::assert!(
                        credential_free_session_cap_bounds_requests(now, cap, request, api)
                            == (at, window, matches!(api, 17 | 36) || request < at)
                    );
                }
            }
        }
    }
    // Overflow must keep the cap even without a credential deadline.
    assert2::assert!(sasl_session_expiry(i64::MAX - 1, None, Some(30_000)) == (Some(i64::MAX), 1));
    for now in [i64::MIN, -1, 0, 1, i64::MAX - 1, i64::MAX] {
        for credential in [None, Some(i64::MIN), Some(-1), Some(0), Some(i64::MAX)] {
            for cap in [None, Some(-1), Some(0), Some(1), Some(i64::MAX)] {
                assert2::assert!(
                    sasl_session_expiry(now, credential, cap)
                        == session_oracle(now, credential, cap)
                );
                for api in [0, 17, 18, 36, 41] {
                    assert2::assert!(
                        session_expired_for_request(credential, api, now)
                            == (!matches!(api, 17 | 36) && credential.is_some_and(|at| at <= now))
                    );
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn session_kernel_matches_wide_integer_oracle(
        now in any::<i64>(), credential in proptest::option::of(any::<i64>()),
        cap in proptest::option::of(any::<i64>()), api in any::<i16>(),
    ) {
        prop_assert_eq!(sasl_session_expiry(now, credential, cap), session_oracle(now, credential, cap));
        prop_assert_eq!(session_expired_for_request(credential, api, now),
            !matches!(api, 17 | 36) && credential.is_some_and(|at| at <= now));
    }

    #[test]
    fn token_session_trace_matches_independent_deadline_oracle(
        raw in any::<[u64; 4]>(), periods in any::<[i64; 4]>(), api in any::<i16>(),
    ) {
        let mut times = raw.map(|value| i64::try_from(value & 0x7fff_ffff_ffff_ffff).unwrap());
        times.sort_unstable();
        check_trace(times, periods, api, TokenApi::Describe);
    }
}
