use {
    crate::{
        authz::{
            RequestAuthState, request_auth_admission, sasl_session_expiry,
            session_expired_for_request,
        },
        delegation_token::{
            ScramCredentialSource, TokenCreateDecision, TokenMutationDecision, TokenMutationFacts,
            TokenMutationKind, TokenMutationState, TokenRenewDecision, create_token_deadlines,
            renew_token_expiry, scram_credential_source, token_is_active, token_mutation_decision,
        },
    },
    creusot_std::prelude::*,
};

#[krabka_macros::model_type(proof(DeepModel))]
pub struct RenewalTrace {
    pub original_expiry: i64,
    pub maximum: i64,
    pub renewed_expiry: i64,
    pub boundary_authenticated: bool,
    pub renewal: TokenMutationDecision,
    pub cleanup: TokenMutationDecision,
    // Session deadline, advertised lifetime and request admission.
    pub session: Option<(i64, i64, bool)>,
}

/// Renew exactly at the expiry captured by a cleanup worker, then evaluate
/// that delayed cleanup against the resulting committed expiry. Fresh
/// SCRAM rounds before the new expiry survive the old cleanup. At the
/// immutable maximum, renewal is a no-op and cleanup can remove the token.
/// One identity and its immutable fields stay fixed; only expiry changes.
/// Commit ordering, faithful metadata projection and cryptographic success
/// remain host obligations. Delays model nondecreasing clock observations.
#[requires(issued_ms@ >= 0 && periods.1@ > 0 && periods.2@ > 0 && periods.4@ > 0)]
#[requires(0 <= delays.0@ && delays.0@ <= delays.1@ && delays.1@ <= delays.2@)]
#[ensures(!result.boundary_authenticated)]
#[ensures(issued_ms@ <= result.original_expiry@ && result.original_expiry@ <= result.maximum@)]
#[ensures(result.original_expiry@ <= result.renewed_expiry@ && result.renewed_expiry@ <= result.maximum@
    && (result.original_expiry@ < result.renewed_expiry@) == (result.original_expiry@ < result.maximum@))]
#[ensures((result.renewal == TokenMutationDecision::Reject) == uncommitted_tail)]
#[ensures((result.renewal == TokenMutationDecision::Append) == (!uncommitted_tail
    && result.original_expiry@ < result.maximum@))]
#[ensures((result.renewal == TokenMutationDecision::Retry) == (!uncommitted_tail
    && result.original_expiry == result.maximum))]
#[ensures((result.cleanup == TokenMutationDecision::Reject) == (uncommitted_tail
    || result.original_expiry@ < result.maximum@))]
#[ensures((result.cleanup == TokenMutationDecision::Append) == (!uncommitted_tail
    && result.original_expiry == result.maximum))]
#[ensures((result.session == None) == (uncommitted_tail
    || result.original_expiry@ + delays.1@ >= result.renewed_expiry@))]
#[ensures(match result.session {
    None => true,
    Some((at, lifetime, admitted)) => result.original_expiry@ + delays.1@ < at@
        && at@ <= result.renewed_expiry@ && lifetime@ == at@ - result.original_expiry@ - delays.1@
        && (cap_ms@ > 0 ==> at@ <= result.original_expiry@ + delays.1@ + cap_ms@)
        && (at == result.renewed_expiry || (cap_ms@ > 0
            && (at@ == result.original_expiry@ + delays.1@ + cap_ms@ || at@ == i64::MAX@)))
        && admitted == (api_key@ == 17 || api_key@ == 36
            || result.original_expiry@ + delays.2@ < at@),
})]
pub(super) fn renewed_token_survives_captured_cleanup(
    issued_ms: i64,
    periods: (i64, i64, i64, i64, i64), // requested lifetime, ceiling, initial, requested/default renewal
    delays: (i64, i64, i64),            // after original expiry: SCRAM rounds one/two and request
    cap_ms: i64,
    uncommitted_tail: bool,
    api_key: i16,
) -> RenewalTrace {
    let TokenCreateDecision::Create(created) =
        create_token_deadlines(issued_ms, periods.0, periods.1, periods.2)
    else {
        unreachable!();
    };
    let old = created.initial_expiry_ms;
    let maximum = created.max_timestamp_ms;
    let boundary_authenticated = token_is_active(old, old, maximum);
    let TokenRenewDecision::Renew(renewed) =
        renew_token_expiry(old, periods.3, periods.4, old, maximum)
    else {
        unreachable!();
    };
    let renewal = token_mutation_decision(TokenMutationFacts {
        kind: TokenMutationKind::Renew,
        state: TokenMutationState::Expected,
        now_ms: old,
        expected_expiry_ms: old,
        incoming_expiry_ms: renewed,
        max_timestamp_ms: maximum,
        uncommitted_tail,
    });
    let stored = if matches!(renewal, TokenMutationDecision::Append) {
        renewed
    } else {
        old
    };
    let cleanup = token_mutation_decision(TokenMutationFacts {
        kind: TokenMutationKind::Delete,
        state: if stored == old {
            TokenMutationState::Expected
        } else {
            TokenMutationState::Stale
        },
        now_ms: old,
        expected_expiry_ms: old,
        incoming_expiry_ms: old,
        max_timestamp_ms: maximum,
        uncommitted_tail,
    });
    let mut trace = RenewalTrace {
        original_expiry: old,
        maximum,
        renewed_expiry: renewed,
        boundary_authenticated,
        renewal,
        cleanup,
        session: None,
    };
    if matches!(cleanup, TokenMutationDecision::Append) {
        return trace;
    }
    let first = old.saturating_add(delays.0);
    if !matches!(
        scram_credential_source(false, true, true, token_is_active(first, stored, maximum)),
        ScramCredentialSource::DelegationToken
    ) {
        return trace;
    }
    let completed = old.saturating_add(delays.1);
    if !token_is_active(completed, stored, stored) {
        return trace;
    }
    let (Some(at), lifetime) = sasl_session_expiry(completed, Some(stored), Some(cap_ms)) else {
        unreachable!();
    };
    let request = old.saturating_add(delays.2);
    let admitted = request_auth_admission(RequestAuthState::Authenticated, api_key)
        && !session_expired_for_request(Some(at), api_key, request);
    trace.session = Some((at, lifetime, admitted));
    trace
}

#[cfg(test)]
mod tests;
