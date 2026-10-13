use {
    super::*,
    crate::{jwks::JwksCacheFacts, oauth::*},
};

fn cache_snapshot_ok(at: i64, c: JwksCacheFacts) -> bool {
    c.generation_before.is_multiple_of(2)
        && c.generation_before == c.generation_after
        && (!c.expiry_enabled
            || (c.expiry_ms >= 0
                && c.last_successful_fetch_ms > 0
                && at >= c.last_successful_fetch_ms
                && i128::from(at) - i128::from(c.last_successful_fetch_ms)
                    <= i128::from(c.expiry_ms)))
}

fn completion_ok(start: i64, finish: i64, cache: Option<JwksCacheFacts>) -> bool {
    start >= 0 && finish >= start && cache.is_none_or(|c| cache_snapshot_ok(finish, c))
}

fn oracle(
    f: OAuthSessionFacts,
    cache: Option<JwksCacheFacts>,
    completed: (i64, u64),
) -> OAuthSessionDecision {
    let initial_ok = cache.is_none_or(|c| cache_snapshot_ok(f.now_ms, c));
    let after = cache.map(|mut c| {
        c.generation_after = completed.1;
        c
    });
    let remaining = i128::from(f.token_expires_at_ms) - i128::from(completed.0);
    if !initial_ok
        || !completion_ok(f.now_ms, completed.0, after)
        || f.expiry == OAuthExpiryPresence::Missing
        || remaining <= 0
        || remaining > i128::from(i64::MAX)
        || (f.cap == OAuthSessionCap::Enabled && f.cap_ms <= 0)
        || (f.authentication == OAuthAuthenticationKind::Reauthentication
            && f.principal == OAuthPrincipalMatch::Differs)
    {
        return OAuthSessionDecision::Reject;
    }
    let lifetime = if f.cap == OAuthSessionCap::Enabled {
        remaining.min(i128::from(f.cap_ms))
    } else {
        remaining
    };
    OAuthSessionDecision::Admit {
        session_lifetime_ms: i64::try_from(lifetime).unwrap(),
        effective_expires_at_ms: i64::try_from(i128::from(completed.0) + lifetime).unwrap(),
    }
}

fn facts(start: i64, expiry: i64) -> OAuthSessionFacts {
    OAuthSessionFacts {
        expiry: OAuthExpiryPresence::Present,
        token_expires_at_ms: expiry,
        now_ms: start,
        cap: OAuthSessionCap::Disabled,
        cap_ms: 0,
        authentication: OAuthAuthenticationKind::Initial,
        principal: OAuthPrincipalMatch::Matches,
    }
}

fn configured_facts(
    start: i64,
    expiry: i64,
    enabled: bool,
    cap: i64,
    reauth: bool,
    matching: bool,
) -> OAuthSessionFacts {
    OAuthSessionFacts {
        cap: if enabled {
            OAuthSessionCap::Enabled
        } else {
            OAuthSessionCap::Disabled
        },
        cap_ms: cap,
        authentication: if reauth {
            OAuthAuthenticationKind::Reauthentication
        } else {
            OAuthAuthenticationKind::Initial
        },
        principal: if matching {
            OAuthPrincipalMatch::Matches
        } else {
            OAuthPrincipalMatch::Differs
        },
        ..facts(start, expiry)
    }
}

fn cache(start: i64) -> JwksCacheFacts {
    JwksCacheFacts {
        generation_before: 2,
        generation_after: 2,
        last_successful_fetch_ms: 80,
        now_ms: start,
        expiry_enabled: true,
        expiry_ms: 40,
    }
}

#[test]
fn validation_completion_checks_both_snapshot_times_and_exact_session() {
    for started in [-1, 0, 100, i64::MAX - 2] {
        for finished in [
            started.saturating_sub(1),
            started,
            started.saturating_add(1),
            started.saturating_add(20),
            i64::MAX,
        ] {
            for expiry in [started, started.saturating_add(20), i64::MAX] {
                for enabled in [false, true] {
                    for cap in [-1, 0, 1, i64::MAX] {
                        for reauth in [false, true] {
                            for matching in [false, true] {
                                let f = configured_facts(
                                    started, expiry, enabled, cap, reauth, matching,
                                );
                                for c in [
                                    None,
                                    Some(cache(started)),
                                    Some(JwksCacheFacts {
                                        expiry_enabled: false,
                                        ..cache(started)
                                    }),
                                ] {
                                    for generation in [2, 3, 4, u64::MAX] {
                                        assert2::assert!(
                                            validated_oauth_snapshot_bounds_session(
                                                f,
                                                c,
                                                (finished, generation)
                                            ) == oracle(f, c, (finished, generation))
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    // A future-dated start snapshot remains invalid even after its fetch time.
    let f = facts(50, 200);
    assert2::assert!(
        validated_oauth_snapshot_bounds_session(f, Some(cache(50)), (100, 2))
            == OAuthSessionDecision::Reject
    );
    // Cache TTL admits equality, whereas token expiry rejects equality.
    assert2::assert!(matches!(
        validated_oauth_snapshot_bounds_session(facts(100, 200), Some(cache(100)), (120, 2)),
        OAuthSessionDecision::Admit { .. }
    ));
    assert2::assert!(
        validated_oauth_snapshot_bounds_session(facts(100, 200), Some(cache(100)), (121, 2))
            == OAuthSessionDecision::Reject
    );
    assert2::assert!(
        validated_oauth_snapshot_bounds_session(facts(100, 120), None, (120, 2))
            == OAuthSessionDecision::Reject
    );
}

#[test]
fn completion_guard_uses_finish_time_even_if_the_supplied_clock_is_old() {
    let c = cache(100);
    for (start, finish) in [
        (100, 120),
        (100, 121),
        (100, 99),
        (-1, 120),
        (100, i64::MAX),
    ] {
        assert2::assert!(
            oauth_validation_admission(start, finish, Some(c))
                == completion_ok(start, finish, Some(c))
        );
    }
    for generation in [0, 2, 3, u64::MAX - 1, u64::MAX] {
        let c = JwksCacheFacts {
            generation_before: generation,
            generation_after: generation,
            expiry_enabled: false,
            ..c
        };
        assert2::assert!(
            oauth_validation_admission(100, 100, Some(c)) == completion_ok(100, 100, Some(c))
        );
    }
}

proptest! {
    #[test]
    fn completed_oauth_snapshot_matches_independent_wide_integer_oracle(
        times in any::<[i64; 2]>(), expiry in any::<i64>(), cap in any::<i64>(),
        enabled in any::<bool>(), reauth in any::<bool>(), matching in any::<bool>(), present in any::<bool>(),
        raw in proptest::option::of((any::<u64>(), any::<u64>(), any::<i64>(), any::<i64>(), any::<bool>())),
        generation in any::<u64>(),
    ) {
        let f = OAuthSessionFacts { expiry: if present { OAuthExpiryPresence::Present } else { OAuthExpiryPresence::Missing },
            ..configured_facts(times[0], expiry, enabled, cap, reauth, matching) };
        let c = raw.map(|(before, after, fetch, ttl, enabled)| JwksCacheFacts {
            generation_before: before, generation_after: after, last_successful_fetch_ms: fetch,
            now_ms: times[0], expiry_ms: ttl, expiry_enabled: enabled });
        prop_assert_eq!(validated_oauth_snapshot_bounds_session(f, c, (times[1], generation)), oracle(f, c, (times[1], generation)));
        prop_assert_eq!(oauth_validation_admission(times[0], times[1], c), completion_ok(times[0], times[1], c));
    }
}
