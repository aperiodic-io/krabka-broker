#[cfg(creusot)]
use crate::jwks::jwks_cache_facts_admissible_at;
use {
    crate::jwks::{JwksCacheDecision, JwksCacheFacts, jwks_cache_admission},
    creusot_std::prelude::*,
};

/// Bind validation completion to a forward clock observation and, when supplied,
/// the still-current and fresh JWKS generation. The supplied cache
/// timestamp is a start observation; freshness is always recomputed at finish.
#[ensures(result == (started_ms@ >= 0 && completed_ms@ >= started_ms@ && match cache {
    None => true,
    Some(facts) => jwks_cache_facts_admissible_at(facts, completed_ms),
}))]
#[must_use]
pub fn oauth_validation_admission(
    started_ms: i64,
    completed_ms: i64,
    cache: Option<JwksCacheFacts>,
) -> bool {
    if started_ms < 0 || completed_ms < started_ms {
        return false;
    }
    match cache {
        None => true,
        Some(mut facts) => {
            facts.now_ms = completed_ms;
            matches!(jwks_cache_admission(facts), JwksCacheDecision::Admit)
        }
    }
}
