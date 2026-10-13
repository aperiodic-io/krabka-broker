#[cfg(creusot)]
use super::jwks_publication::initial_session_input_valid;
#[cfg(creusot)]
use crate::authz::AclDefault;
use {
    super::published_keys_bound_oauth_session,
    crate::{
        authz::{AclDecision, AclFacts, acl_decision, controller_request_admission},
        jwks::JwksCacheFacts,
        oauth::{OAuthSessionDecision, OAuthSessionFacts},
    },
    creusot_std::prelude::*,
};

type ControllerSessionTrace = (Option<i64>, Vec<bool>, (u64, u64));

/// Bind publication, cache freshness and validation completion to a history of
/// `DescribeQuorum` requests and their current Cluster Describe ACL facts.
/// Return the maximal handled prefix before the first expired clock observation:
/// ACL denial returns a refusal and continues; expiry closes even for superusers.
/// Once closed, later clock rollback cannot revive this connection.
/// Publication history covers validation, not later key rotation. Faithful claim,
/// clock and ACL projection, frame decoding and actual network closure remain
/// host obligations; this does not promise that an admitted operation finishes
/// before credential expiry or that idle connections close at the deadline.
#[requires(initial_session_input_valid(facts, cache, completed_ms@))]
#[ensures((result.0 == None) == (super::jwks_publication::publication_invalidates_credential(cache, completed_ms@, result.2.1@, begin_unfinished_writer)))]
#[ensures(match result.0 {
    None => result.1@.len() == 0,
    Some(expiry) => expiry == facts.token_expires_at_ms && expiry@ > completed_ms@
        && result.2.0 == cache.generation_before && result.2.1@ == 0,
})]
#[ensures(result.1@.len() <= requests@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.1@.len() ==> match result.0 {
    None => false,
    Some(expiry) => requests@[i].0@ < expiry@,
})]
#[ensures(match result.0 {
    None => true,
    Some(expiry) => result.1@.len() < requests@.len()
        ==> requests@[result.1@.len()].0@ >= expiry@,
})]
#[ensures(forall<i: Int> 0 <= i && i < result.1@.len() ==>
    result.1@[i] == (requests@[i].1.super_user || (!requests@[i].1.saw_deny
        && (requests@[i].1.saw_allow || requests@[i].1.default_decision == AclDefault::Allow))))]
pub(super) fn published_controller_session_bounds_quorum_requests(
    facts: OAuthSessionFacts,
    cache: JwksCacheFacts,
    completed_ms: i64,
    fetches: &[bool],
    begin_unfinished_writer: bool,
    requests: &[(i64, AclFacts)],
) -> ControllerSessionTrace {
    let (session, trace) = published_keys_bound_oauth_session(
        facts,
        cache,
        completed_ms,
        fetches,
        begin_unfinished_writer,
    );
    let mut responses = Vec::new();
    let expiry = match session {
        OAuthSessionDecision::Reject => return (None, responses, trace),
        OAuthSessionDecision::Admit {
            effective_expires_at_ms,
            ..
        } => effective_expires_at_ms,
    };
    let mut i = 0_usize;
    #[invariant(responses@.len() == i@ && i@ <= requests@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> requests@[j].0@ < expiry@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==>
        responses@[j] == (requests@[j].1.super_user || (!requests@[j].1.saw_deny
            && (requests@[j].1.saw_allow || requests@[j].1.default_decision == AclDefault::Allow))))]
    #[variant(requests@.len() - i@)]
    while i < requests.len() {
        if !controller_request_admission(Some(expiry), 55, requests[i].0) {
            break;
        }
        responses.push(matches!(
            acl_decision(requests[i].1),
            AclDecision::AllowSuperuser | AclDecision::AllowAcl | AclDecision::AllowNoAcl
        ));
        i += 1;
    }
    (Some(expiry), responses, trace)
}
