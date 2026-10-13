//! Publish actual cache updates, then bind the validation snapshot to OAuth.

#[cfg(creusot)]
use crate::oauth::{OAuthAuthenticationKind, OAuthExpiryPresence, OAuthSessionCap};
use {
    super::oauth_jwks_prelude::*, crate::jwks::jwks_publication_generations,
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] specification, absent from runtime tests.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= prefix && prefix <= fetches.len())]
#[ensures(0 <= result && result <= prefix)]
#[ensures((result == 0) == (forall<i: Int> 0 <= i && i < prefix ==> !fetches[i]))]
#[variant(prefix)]
fn successful_fetches(fetches: Seq<bool>, prefix: Int) -> Int {
    pearlite! {
        if prefix == 0 { 0 } else {
            successful_fetches(fetches, prefix - 1) + if fetches[prefix - 1] { 1 } else { 0 }
        }
    }
}

/// Failed fetches preserve the cache. A successful fetch with an available
/// exclusive writer publishes exactly once; exhaustion preserves old keys.
/// The count relates metadata to actual key replacements, rather than merely
/// comparing two arbitrary observations. CAS ownership and faithful ordered
/// atomic observations remain host obligations.
#[requires(initial@ % 2 == 0)]
#[ensures(result.0@ % 2 == 0 && result.0@ == initial@ + 2 * result.1@)]
#[ensures(result.1@ <= successful_fetches(fetches@, fetches@.len()))]
#[ensures(result.1@ == successful_fetches(fetches@, fetches@.len()) || result.0@ == u64::MAX@ - 1)]
#[ensures((result.1@ == 0) == (initial@ == u64::MAX@ - 1
    || forall<i: Int> 0 <= i && i < fetches@.len() ==> !fetches@[i]))]
pub(super) fn publication_trace(initial: u64, fetches: &[bool]) -> (u64, u64) {
    let mut generation = initial;
    let mut published = 0_u64;
    let mut i = 0_usize;
    #[invariant(i@ <= fetches@.len())]
    #[invariant(generation@ % 2 == 0 && generation@ == initial@ + 2 * published@)]
    #[invariant(published@ <= successful_fetches(fetches@, i@))]
    #[invariant(published@ == successful_fetches(fetches@, i@) || generation@ == u64::MAX@ - 1)]
    #[invariant((published@ == 0) == (initial@ == u64::MAX@ - 1
        || forall<j: Int> 0 <= j && j < i@ ==> !fetches@[j]))]
    #[variant(fetches@.len() - i@)]
    while i < fetches.len() {
        if fetches[i]
            && let Some((_, committed)) = jwks_publication_generations(generation)
        {
            generation = committed;
            published += 1;
        }
        i += 1;
    }
    (generation, published)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
pub(super) fn initial_session_input_valid(
    facts: OAuthSessionFacts,
    cache: JwksCacheFacts,
    completed: Int,
) -> bool {
    pearlite! {
        cache.now_ms@ == facts.now_ms@ && facts.now_ms@ >= 0 && completed >= facts.now_ms@
        && cache.generation_before@ % 2 == 0 && cache.generation_after == cache.generation_before
        && (!cache.expiry_enabled || (cache.expiry_ms@ >= 0
            && cache.last_successful_fetch_ms@ > 0 && cache.last_successful_fetch_ms@ <= facts.now_ms@))
        && facts.expiry == OAuthExpiryPresence::Present && facts.token_expires_at_ms@ > completed
        && facts.authentication == OAuthAuthenticationKind::Initial && facts.cap == OAuthSessionCap::Disabled
    }
}

/// For a valid initial controller credential and a stable starting generation,
/// authenticate exactly when no keys were replaced, no writer is in flight,
/// and hard cache expiry has not elapsed at completion. A final publication
/// attempt may leave its odd write phase unfinished if it can begin. No generation-ABA
/// precondition is needed: the publisher trace derives it, even at exhaustion.
/// Token expiry bounds the computed lifetime; this does not prove subsequent
/// controller frame expiry enforcement or cryptographic correctness.
#[requires(initial_session_input_valid(facts, cache, completed_ms@))]
#[ensures(result.1.0@ >= cache.generation_before@ && result.1.1@ <= successful_fetches(fetches@, fetches@.len()))]
#[ensures((result.0 == OAuthSessionDecision::Reject) == (publication_invalidates_credential(cache, completed_ms@, result.1.1@, begin_unfinished_writer)))]
#[ensures(match result.0 {
    OAuthSessionDecision::Reject => true,
    OAuthSessionDecision::Admit { session_lifetime_ms, effective_expires_at_ms } =>
        result.1.0 == cache.generation_before && result.1.1@ == 0
        && (cache.generation_before@ == u64::MAX@ - 1
            || forall<i: Int> 0 <= i && i < fetches@.len() ==> !fetches@[i])
        && effective_expires_at_ms == facts.token_expires_at_ms
        && session_lifetime_ms@ == facts.token_expires_at_ms@ - completed_ms@ && session_lifetime_ms@ > 0,
})]
pub(super) fn published_keys_bound_oauth_session(
    mut facts: OAuthSessionFacts,
    cache: JwksCacheFacts,
    completed_ms: i64,
    fetches: &[bool],
    begin_unfinished_writer: bool,
) -> (OAuthSessionDecision, (u64, u64)) {
    let (mut generation, published) = publication_trace(cache.generation_before, fetches);
    if begin_unfinished_writer && let Some((writing, _)) = jwks_publication_generations(generation)
    {
        generation = writing;
    }
    let trace = (generation, published);
    if matches!(jwks_cache_admission(cache), JwksCacheDecision::Reject) {
        return (OAuthSessionDecision::Reject, trace);
    }
    let finished = JwksCacheFacts {
        generation_after: generation,
        ..cache
    };
    if !oauth_validation_admission(facts.now_ms, completed_ms, Some(finished)) {
        return (OAuthSessionDecision::Reject, trace);
    }
    facts.now_ms = completed_ms;
    (oauth_session_admission(facts), trace)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Any key publication, unfinished writer or hard-expiry overrun invalidates this credential.
pub(super) fn publication_invalidates_credential(
    cache: JwksCacheFacts,
    completed: Int,
    published: Int,
    writer_unfinished: bool,
) -> bool {
    pearlite! { published > 0 || (writer_unfinished && cache.generation_before@ < u64::MAX@ - 1)
    || (cache.expiry_enabled && completed - cache.last_successful_fetch_ms@ > cache.expiry_ms@) }
}
