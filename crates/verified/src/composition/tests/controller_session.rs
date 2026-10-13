use {
    super::*,
    crate::{authz::controller_request_admission, jwks::JwksCacheFacts, oauth::*},
};

type Trace = (Option<i64>, Vec<bool>, (u64, u64));

fn acl(bits: u8) -> AclFacts {
    AclFacts {
        super_user: bits & 1 != 0,
        saw_deny: bits & 2 != 0,
        saw_allow: bits & 4 != 0,
        default_decision: if bits & 8 != 0 {
            AclDefault::Allow
        } else {
            AclDefault::Deny
        },
    }
}

fn fixture(expiry: i64) -> (OAuthSessionFacts, JwksCacheFacts) {
    (
        OAuthSessionFacts {
            now_ms: 1000,
            expiry: OAuthExpiryPresence::Present,
            token_expires_at_ms: expiry,
            cap: OAuthSessionCap::Disabled,
            cap_ms: 0,
            authentication: OAuthAuthenticationKind::Initial,
            principal: OAuthPrincipalMatch::Matches,
        },
        JwksCacheFacts {
            generation_before: 2,
            generation_after: 2,
            last_successful_fetch_ms: 1000,
            now_ms: 1000,
            expiry_enabled: true,
            expiry_ms: 100,
        },
    )
}

// Closed-form publication capacity and maximal prefix selection, with no
// production policy calls. The previously independent wide oracle is reused.
fn expected(
    facts: OAuthSessionFacts,
    cache: JwksCacheFacts,
    finish: i64,
    fetches: &[bool],
    pending: bool,
    requests: &[(i64, AclFacts)],
) -> Trace {
    let (stable, count) = jwks_publication::expected_trace(cache.generation_before, fetches);
    let generation = if pending && stable < u64::MAX - 1 {
        stable + 1
    } else {
        stable
    };
    let rejected = count > 0
        || generation != cache.generation_before
        || (cache.expiry_enabled
            && i128::from(finish) - i128::from(cache.last_successful_fetch_ms)
                > i128::from(cache.expiry_ms));
    let deadline = (!rejected).then_some(facts.token_expires_at_ms);
    let rows = if let Some(expiry) = deadline {
        requests
            .iter()
            .take_while(|(clock, _)| *clock < expiry)
            .map(|(_, a)| {
                a.super_user
                    || (!a.saw_deny && (a.saw_allow || a.default_decision == AclDefault::Allow))
            })
            .collect()
    } else {
        Vec::new()
    };
    (deadline, rows, (generation, count))
}

#[test]
fn controller_phase_expiry_is_exact_for_every_api_and_integer_boundary() {
    for expiry in [None, Some(-1), Some(0), Some(1000), Some(i64::MAX)] {
        for clock in [i64::MIN, -1, 0, 999, 1000, i64::MAX] {
            for api in [i16::MIN, 0, 17, 18, 36, 55, i16::MAX] {
                assert2::assert!(
                    controller_request_admission(expiry, api, clock)
                        == expiry.is_none_or(|at| clock < at)
                );
            }
        }
    }
}

#[test]
fn admitted_session_handles_exact_prefix_and_cannot_resurrect_after_expiry() {
    for expiry in [2000, i64::MAX] {
        let (facts, mut cache) = fixture(expiry);
        for initial in [0, 2, u64::MAX - 1] {
            cache.generation_before = initial;
            cache.generation_after = initial;
            for bits in 0_u8..16 {
                let fetches: Vec<bool> = (0..4).map(|i| bits & (1 << i) != 0).collect();
                for pending in [false, true] {
                    for (enabled, finish) in [(true, 1100), (true, 1101), (false, 1500)] {
                        cache.expiry_enabled = enabled;
                        for flags in 0..16 {
                            let histories = [
                                std::vec![],
                                std::vec![(expiry, acl(flags)), (finish, acl(flags))],
                                std::vec![
                                    (finish, acl(flags)),
                                    (expiry - 1, acl(flags ^ 3)),
                                    (expiry, acl(1)),
                                    (finish, acl(1)),
                                ],
                            ];
                            for requests in histories {
                                let actual = published_controller_session_bounds_quorum_requests(
                                    facts, cache, finish, &fetches, pending, &requests,
                                );
                                assert2::assert!(
                                    actual
                                        == expected(
                                            facts, cache, finish, &fetches, pending, &requests
                                        )
                                );
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
    fn arbitrary_request_clocks_and_acl_changes_obey_the_first_close(
        raw in any::<u64>(), fetches in proptest::collection::vec(any::<bool>(), 0..32),
        pending in any::<bool>(), history in proptest::collection::vec((any::<i64>(), any::<u8>()), 0..64),
    ) {
        let (facts, mut cache) = fixture(i64::MAX);
        cache.generation_before = raw & !1;
        cache.generation_after = cache.generation_before;
        let requests: Vec<_> = history.into_iter().map(|(clock, flags)| (clock, acl(flags))).collect();
        let actual = published_controller_session_bounds_quorum_requests(facts, cache, 1100, &fetches, pending, &requests);
        prop_assert_eq!(actual, expected(facts, cache, 1100, &fetches, pending, &requests));
    }
}
