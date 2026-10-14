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

    fn fresh_cache_facts() -> JwksCacheFacts {
        JwksCacheFacts {
            generation_before: 2,
            generation_after: 2,
            last_successful_fetch_ms: 100,
            now_ms: 200,
            expiry_enabled: true,
            expiry_ms: 100,
        }
    }

    #[yare::parameterized(
        fresh = { "a fresh stable generation", fresh_cache_facts(), JwksCacheDecision::Admit },
        expiry_disabled = {
            "a stable generation with expiry disabled",
            JwksCacheFacts { expiry_enabled: false, ..fresh_cache_facts() },
            JwksCacheDecision::Admit
        },
        exact_expiry_boundary = {
            "a fetch at the zero-expiry boundary",
            JwksCacheFacts { now_ms: 100, last_successful_fetch_ms: 100, expiry_ms: 0, ..fresh_cache_facts() },
            JwksCacheDecision::Admit
        },
        changed_before_generation = {
            "a generation changed before reading the cache",
            JwksCacheFacts { generation_before: 1, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        changed_after_generation = {
            "a generation changed while reading the cache",
            JwksCacheFacts { generation_after: 4, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        never_fetched = {
            "a cache with no successful fetch",
            JwksCacheFacts { last_successful_fetch_ms: 0, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        zero_fetch_and_clock = {
            "a cache whose fetch and current times are both zero",
            JwksCacheFacts { now_ms: 0, last_successful_fetch_ms: 0, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        clock_before_fetch = {
            "a clock before the successful fetch",
            JwksCacheFacts { now_ms: 99, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        expired = {
            "an expired cache",
            JwksCacheFacts { now_ms: 201, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
        negative_expiry = {
            "a negative expiry",
            JwksCacheFacts { expiry_ms: -1, ..fresh_cache_facts() },
            JwksCacheDecision::Reject
        },
    )]
    fn cache_requires_one_fresh_stable_generation(
        case: &str,
        facts: JwksCacheFacts,
        expected: JwksCacheDecision,
    ) {
        assert2::check!(jwks_cache_admission(facts) == expected, "{case}");
    }

    #[yare::parameterized(
        unrefreshed_zero = { 0, 0, 0, JwksOnDemandDecision::Refresh { next_refresh_ms: 0 } },
        unrefreshed_pause = { 5, 0, 10, JwksOnDemandDecision::Refresh { next_refresh_ms: 5 } },
        first_refresh = { 100, 0, 10, JwksOnDemandDecision::Refresh { next_refresh_ms: 100 } },
        unrefreshed_max = {
            i64::MAX,
            0,
            i64::MAX,
            JwksOnDemandDecision::Refresh { next_refresh_ms: i64::MAX },
        },
        pause_boundary = { 110, 100, 10, JwksOnDemandDecision::Refresh { next_refresh_ms: 110 } },
        before_pause_boundary = { 109, 100, 10, JwksOnDemandDecision::RateLimited },
        negative_clock = { -1, 0, 10, JwksOnDemandDecision::RateLimited },
        negative_pause = { 100, 0, -1, JwksOnDemandDecision::RateLimited },
        backwards_clock = { 99, 100, 0, JwksOnDemandDecision::RateLimited },
        elapsed_subtraction_overflow = { i64::MAX, 1, i64::MAX, JwksOnDemandDecision::RateLimited },
    )]
    fn on_demand_limit_is_monotonic_and_overflow_safe(
        now_ms: i64,
        last_refresh_ms: i64,
        min_pause_ms: i64,
        expected: JwksOnDemandDecision,
    ) {
        assert2::check!(
            jwks_on_demand_refresh_decision(now_ms, last_refresh_ms, min_pause_ms) == expected
        );
    }
}
