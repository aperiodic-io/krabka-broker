//! JWKS cache freshness and on-demand refresh decisions.

mod publication;
use creusot_std::prelude::ensures;
#[cfg(creusot)]
use creusot_std::prelude::{DeepModel, logic};
pub use publication::jwks_publication_generations;

/// Whether a validator may use one observed JWKS cache generation.
#[krabka_macros::model_type]
pub enum JwksCacheDecision {
    Reject,
    Admit,
}

/// Values read around one key validation.
#[krabka_macros::model_type]
pub struct JwksCacheFacts {
    pub generation_before: u64,
    pub generation_after: u64,
    pub last_successful_fetch_ms: i64,
    pub now_ms: i64,
    pub expiry_enabled: bool,
    pub expiry_ms: i64,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub fn jwks_cache_facts_admissible_at(facts: JwksCacheFacts, now_ms: i64) -> bool {
    pearlite! {
        facts.generation_before@ % 2 == 0
            && facts.generation_before@ == facts.generation_after@
            && (!facts.expiry_enabled || (
                facts.expiry_ms@ >= 0
                    && facts.last_successful_fetch_ms@ > 0
                    && now_ms@ >= facts.last_successful_fetch_ms@
                    && now_ms@ - facts.last_successful_fetch_ms@ <= facts.expiry_ms@
            ))
    }
}

/// Admit only a stable, fully published cache generation that is not stale.
#[ensures((result == JwksCacheDecision::Admit) == jwks_cache_facts_admissible_at(facts, facts.now_ms))]
#[must_use]
pub fn jwks_cache_admission(facts: JwksCacheFacts) -> JwksCacheDecision {
    if !facts.generation_before.is_multiple_of(2)
        || facts.generation_before != facts.generation_after
    {
        return JwksCacheDecision::Reject;
    }
    if !facts.expiry_enabled {
        return JwksCacheDecision::Admit;
    }
    if facts.expiry_ms < 0
        || facts.last_successful_fetch_ms <= 0
        || facts.now_ms < facts.last_successful_fetch_ms
    {
        return JwksCacheDecision::Reject;
    }
    if facts.now_ms - facts.last_successful_fetch_ms > facts.expiry_ms {
        JwksCacheDecision::Reject
    } else {
        JwksCacheDecision::Admit
    }
}

/// Whether an on-demand signal may start a fetch.
#[krabka_macros::model_type]
pub enum JwksOnDemandDecision {
    RateLimited,
    Refresh { next_refresh_ms: i64 },
}

/// Whether an on-demand refresh may start at `now_ms`: the clock and pause are
/// nonnegative, the clock has not stepped back behind the last refresh, and
/// either no refresh has happened yet (`last_refresh_ms <= 0`) or at least
/// `min_pause_ms` has elapsed since it.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn jwks_on_demand_refresh_allowed(
    now_ms: i64,
    last_refresh_ms: i64,
    min_pause_ms: i64,
) -> bool {
    pearlite! {
        now_ms@ >= 0
            && min_pause_ms@ >= 0
            && now_ms@ >= last_refresh_ms@
            && (last_refresh_ms@ <= 0 || now_ms@ - last_refresh_ms@ >= min_pause_ms@)
    }
}

/// Keep the limiter monotonic and fail closed across wall-clock rollback:
/// refresh exactly when `jwks_on_demand_refresh_allowed` holds, stamping
/// `now_ms` as the new last-refresh time.
#[ensures(match result {
    JwksOnDemandDecision::RateLimited => !jwks_on_demand_refresh_allowed(
        now_ms,
        last_refresh_ms,
        min_pause_ms,
    ),
    JwksOnDemandDecision::Refresh { next_refresh_ms } => next_refresh_ms == now_ms
        && jwks_on_demand_refresh_allowed(now_ms, last_refresh_ms, min_pause_ms),
})]
#[must_use]
pub fn jwks_on_demand_refresh_decision(
    now_ms: i64,
    last_refresh_ms: i64,
    min_pause_ms: i64,
) -> JwksOnDemandDecision {
    if now_ms < 0 || min_pause_ms < 0 || now_ms < last_refresh_ms {
        return JwksOnDemandDecision::RateLimited;
    }
    if last_refresh_ms > 0 && now_ms - last_refresh_ms < min_pause_ms {
        return JwksOnDemandDecision::RateLimited;
    }
    JwksOnDemandDecision::Refresh {
        next_refresh_ms: now_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JwksCacheDecision, JwksCacheFacts, JwksOnDemandDecision, jwks_cache_admission,
        jwks_on_demand_refresh_decision,
    };

    #[test]
    fn cache_requires_one_fresh_stable_generation() {
        let fresh = JwksCacheFacts {
            generation_before: 2,
            generation_after: 2,
            last_successful_fetch_ms: 100,
            now_ms: 200,
            expiry_enabled: true,
            expiry_ms: 100,
        };
        assert2::check!(jwks_cache_admission(fresh) == JwksCacheDecision::Admit);
        assert2::check!(
            jwks_cache_admission(JwksCacheFacts {
                expiry_enabled: false,
                ..fresh
            }) == JwksCacheDecision::Admit
        );
        assert2::check!(
            jwks_cache_admission(JwksCacheFacts {
                now_ms: 100,
                last_successful_fetch_ms: 100,
                expiry_ms: 0,
                ..fresh
            }) == JwksCacheDecision::Admit
        );
        for rejected in [
            JwksCacheFacts {
                generation_before: 1,
                ..fresh
            },
            JwksCacheFacts {
                generation_after: 4,
                ..fresh
            },
            JwksCacheFacts {
                last_successful_fetch_ms: 0,
                ..fresh
            },
            JwksCacheFacts {
                now_ms: 0,
                last_successful_fetch_ms: 0,
                ..fresh
            },
            JwksCacheFacts {
                now_ms: 99,
                ..fresh
            },
            JwksCacheFacts {
                now_ms: 201,
                ..fresh
            },
            JwksCacheFacts {
                expiry_ms: -1,
                ..fresh
            },
        ] {
            assert2::check!(jwks_cache_admission(rejected) == JwksCacheDecision::Reject);
        }
    }

    #[test]
    fn on_demand_limit_is_monotonic_and_overflow_safe() {
        let refresh = |next_refresh_ms| JwksOnDemandDecision::Refresh { next_refresh_ms };
        let limited = JwksOnDemandDecision::RateLimited;
        // (now, last refresh, min pause, expected)
        for (now_ms, last_refresh_ms, min_pause_ms, expected) in [
            // Never refreshed: the pause does not apply.
            (0, 0, 0, refresh(0)),
            (5, 0, 10, refresh(5)),
            (100, 0, 10, refresh(100)),
            (i64::MAX, 0, i64::MAX, refresh(i64::MAX)),
            // The pause boundary is inclusive.
            (110, 100, 10, refresh(110)),
            (109, 100, 10, limited),
            // Negative clock or pause and a backwards clock step fail closed.
            (-1, 0, 10, limited),
            (100, 0, -1, limited),
            (99, 100, 0, limited),
            // The elapsed-time subtraction cannot overflow.
            (i64::MAX, 1, i64::MAX, limited),
        ] {
            assert2::check!(
                jwks_on_demand_refresh_decision(now_ms, last_refresh_ms, min_pause_ms) == expected
            );
        }
    }
}
