use {
    super::*,
    crate::{jwks::*, oauth::*},
};

// A wide integer closed-form oracle, independent of the incremental publisher.
pub(super) fn expected_trace(initial: u64, fetches: &[bool]) -> (u64, u64) {
    let requested = fetches.iter().filter(|&&success| success).count() as u128;
    let capacity = (u128::from(u64::MAX) - 1 - u128::from(initial)) / 2;
    let installed = requested.min(capacity);
    (
        u64::try_from(u128::from(initial) + 2 * installed)
            .expect("generation is bounded by capacity"),
        u64::try_from(installed).expect("count is bounded by generation capacity"),
    )
}

#[yare::parameterized(
    initial = { 0 },
    odd_initial = { 1 },
    stable_even = { 2 },
    odd_stable = { 3 },
    near_max_odd = { u64::MAX - 3 },
    near_max_even = { u64::MAX - 2 },
    max_writable = { u64::MAX - 1 },
    maximum = { u64::MAX },
)]
fn writer_generations_cover_parity_and_exhaustion(generation: u64) {
    let valid = generation.is_multiple_of(2) && u128::from(generation) + 2 <= u128::from(u64::MAX);
    let result = jwks_publication_generations(generation);
    assert2::assert!(result.is_some() == valid);
    if let Some((writing, committed)) = result {
        assert2::assert!(u128::from(writing) == u128::from(generation) + 1);
        assert2::assert!(u128::from(committed) == u128::from(generation) + 2);
    }
}

#[test]
fn publication_history_and_cache_age_control_controller_admission() {
    let facts = OAuthSessionFacts {
        now_ms: 1000,
        expiry: OAuthExpiryPresence::Present,
        token_expires_at_ms: 2000,
        cap: OAuthSessionCap::Disabled,
        cap_ms: 0,
        authentication: OAuthAuthenticationKind::Initial,
        principal: OAuthPrincipalMatch::Matches,
    };
    for initial in [0, 2, u64::MAX - 5, u64::MAX - 3, u64::MAX - 1] {
        for bits in 0_u16..256 {
            let fetches: Vec<bool> = (0..8).map(|i| (bits & (1 << i)) != 0).collect();
            let (stable, count) = expected_trace(initial, &fetches);
            assert2::assert!(
                super::super::jwks_publication::publication_trace(initial, &fetches)
                    == (stable, count)
            );
            for pending in [false, true] {
                let generation = if pending && stable < u64::MAX - 1 {
                    stable + 1
                } else {
                    stable
                };
                for (expiry_enabled, ttl, finish) in [
                    (true, 100, 1000),
                    (true, 100, 1100),
                    (true, 100, 1101),
                    (false, 0, 1500),
                ] {
                    let cache = JwksCacheFacts {
                        generation_before: initial,
                        generation_after: initial,
                        last_successful_fetch_ms: 1000,
                        now_ms: 1000,
                        expiry_enabled,
                        expiry_ms: ttl,
                    };
                    let expected = if count > 0
                        || generation != initial
                        || (expiry_enabled && finish - 1000 > ttl)
                    {
                        OAuthSessionDecision::Reject
                    } else {
                        OAuthSessionDecision::Admit {
                            session_lifetime_ms: 2000 - finish,
                            effective_expires_at_ms: 2000,
                        }
                    };
                    assert2::assert!(
                        published_keys_bound_oauth_session(facts, cache, finish, &fetches, pending)
                            == (expected, (generation, count))
                    );
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn arbitrary_publication_traces_match_wide_integer_count(
        raw in any::<u64>(), fetches in proptest::collection::vec(any::<bool>(), 0..128),
    ) {
        let initial = raw & !1;
        let actual = super::super::jwks_publication::publication_trace(initial, &fetches);
        prop_assert_eq!(actual, expected_trace(initial, &fetches));
        prop_assert_eq!(actual.0 == initial, actual.1 == 0);
    }
}
