use creusot_std::prelude::*;

#[cfg(creusot)]
use crate::oauth::{
    OAuthAuthenticationKind, OAuthPrincipalMatch, OAuthSessionCap, oauth_session_admissible_at,
};
use crate::{
    jwks::{JwksCacheDecision, JwksCacheFacts, jwks_cache_admission},
    oauth::{
        OAuthSessionDecision, OAuthSessionFacts, oauth_session_admission,
        oauth_validation_admission,
    },
};

// cargo-mutants: #[cfg(creusot)] specification, absent from runtime tests.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn completed_snapshot_admissible(
    cache: Option<JwksCacheFacts>,
    started_ms: i64,
    completed: (i64, u64),
) -> bool {
    pearlite! {
        started_ms@ >= 0 && completed.0@ >= started_ms@ && match cache {
            None => true,
            Some(c) => c.generation_before@ % 2 == 0
                && c.generation_before@ == c.generation_after@
                && c.generation_before@ == completed.1@
                && (!c.expiry_enabled || (c.expiry_ms@ >= 0
                    && c.last_successful_fetch_ms@ > 0
                    && started_ms@ >= c.last_successful_fetch_ms@
                    && completed.0@ - c.last_successful_fetch_ms@ <= c.expiry_ms@)),
        }
    }
}

/// Compose the start snapshot, validation-completion guard and OAuth session
/// binding. Freshness at the start cannot substitute for freshness at finish;
/// elapsed validation cannot extend a token, admit zero lifetime, or bypass
/// reauthentication principal binding. The exact Reject condition also proves
/// service for every fully admissible snapshot, excluding a reject-all policy.
/// Faithful atomic/clock/claim observations and successful cryptographic
/// validation remain host obligations. Generation equality assumes no ABA.
#[requires(match cache { None => true, Some(c) => c.now_ms@ == facts.now_ms@ })]
#[ensures((result == OAuthSessionDecision::Reject) == (
    !completed_snapshot_admissible(cache, facts.now_ms, completed)
        || !oauth_session_admissible_at(facts, completed.0@)
))]
#[ensures(match result {
    OAuthSessionDecision::Reject => true,
    OAuthSessionDecision::Admit { session_lifetime_ms, effective_expires_at_ms } =>
        completed_snapshot_admissible(cache, facts.now_ms, completed)
        && session_lifetime_ms@ > 0
        && effective_expires_at_ms@ == completed.0@ + session_lifetime_ms@
        && completed.0@ < effective_expires_at_ms@
        && effective_expires_at_ms@ <= facts.token_expires_at_ms@
        && session_lifetime_ms@ == if facts.cap == OAuthSessionCap::Enabled
            && facts.cap_ms@ < facts.token_expires_at_ms@ - completed.0@ {
                facts.cap_ms@
            } else { facts.token_expires_at_ms@ - completed.0@ }
        && (facts.authentication == OAuthAuthenticationKind::Reauthentication ==>
            facts.principal == OAuthPrincipalMatch::Matches),
})]
pub(super) fn validated_oauth_snapshot_bounds_session(
    mut facts: OAuthSessionFacts,
    cache: Option<JwksCacheFacts>,
    completed: (i64, u64), // completion clock, final generation
) -> OAuthSessionDecision {
    if let Some(c) = cache
        && matches!(jwks_cache_admission(c), JwksCacheDecision::Reject)
    {
        return OAuthSessionDecision::Reject;
    }
    let finished_cache = match cache {
        None => None,
        Some(mut c) => {
            c.generation_after = completed.1;
            Some(c)
        }
    };
    if !oauth_validation_admission(facts.now_ms, completed.0, finished_cache) {
        return OAuthSessionDecision::Reject;
    }
    facts.now_ms = completed.0;
    oauth_session_admission(facts)
}
