use {
    super::{
        ScramCredentialSource, TokenApi, TokenApiAdmission, TokenMutationDecision,
        TokenMutationFacts, TokenMutationKind, TokenMutationState,
    },
    creusot_std::prelude::*,
};

/// Classify the credential for SCRAM round 1.
///
/// `token_mechanism` is whether the delegation-token store may be read: the
/// broker passes the client-first `tokenauth` extension there, and leaves
/// `has_regular_credential` false when it is set, so exactly one store is
/// consulted, as in Kafka's `ScramSaslServer`.
#[ensures((result == ScramCredentialSource::Regular) == has_regular_credential)]
#[ensures((result == ScramCredentialSource::DelegationToken) ==
    (!has_regular_credential && token_mechanism && has_token && token_active))]
#[ensures((result == ScramCredentialSource::ExpiredDelegationToken) ==
    (!has_regular_credential && token_mechanism && has_token && !token_active))]
#[ensures((result == ScramCredentialSource::Unknown) ==
    (!has_regular_credential && (!token_mechanism || !has_token)))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies four independent credential lookup facts"
)]
#[must_use]
pub fn scram_credential_source(
    has_regular_credential: bool,
    token_mechanism: bool,
    has_token: bool,
    token_active: bool,
) -> ScramCredentialSource {
    if has_regular_credential {
        ScramCredentialSource::Regular
    } else if !token_mechanism || !has_token {
        ScramCredentialSource::Unknown
    } else if token_active {
        ScramCredentialSource::DelegationToken
    } else {
        ScramCredentialSource::ExpiredDelegationToken
    }
}

/// Whether one delegation token is visible to a Describe caller.
///
/// Matches `DelegationTokenManager.filterToken` in Kafka trunk: a caller sees
/// a token only when the (possibly absent) owner filter matches it, and even
/// then only as the token's owner, its requester (the principal that created
/// it, KIP-373), a listed renewer, or the holder of an ACL grant: `Describe`
/// on that exact token, or KIP-373's `DescribeTokens` on the owner's `User`
/// resource. `acl_allows` is either grant. The caller is never a delegation-token-
/// authenticated identity here — [`token_api_admission`] refuses every such
/// caller before the host ever builds this predicate, so this kernel does not
/// re-derive that isolation; folding it into the owner/requester/renewer/ACL
/// relation would let a filter-matching owner or ACL grant leak a
/// token-authed caller's sibling tokens back in.
#[ensures(result == (owner_filter_matches
    && (caller_is_owner || caller_is_requester || caller_is_renewer || acl_allows)))]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "the proof classifies independent token visibility relationships"
)]
#[must_use]
pub fn token_describe_visible(
    owner_filter_matches: bool,
    caller_is_owner: bool,
    caller_is_requester: bool,
    caller_is_renewer: bool,
    acl_allows: bool,
) -> bool {
    owner_filter_matches
        && (caller_is_owner || caller_is_requester || caller_is_renewer || acl_allows)
}

/// Admit delegation-token APIs only for a securely authenticated, non-token
/// identity.
///
/// Matches `KafkaApis.allowTokenRequests`: a delegation-token-authenticated
/// identity may not invoke ANY delegation-token API, including
/// `DescribeDelegationToken`. KIP-48 forbids a token-issued session from
/// bootstrapping further token access; letting it describe would leak the
/// HMACs of every other token the same owner holds. The host adapter is
/// responsible for treating an anonymous listener principal as
/// unauthenticated.
#[ensures((result == TokenApiAdmission::Allow) == (
    has_authenticated_identity && !authenticated_via_token
))]
#[ensures((result == TokenApiAdmission::Reject) == (
    !has_authenticated_identity || authenticated_via_token
))]
#[must_use]
pub fn token_api_admission(
    has_authenticated_identity: bool,
    authenticated_via_token: bool,
    _api: TokenApi,
) -> TokenApiAdmission {
    if has_authenticated_identity && !authenticated_via_token {
        TokenApiAdmission::Allow
    } else {
        TokenApiAdmission::Reject
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
fn token_generation_live(facts: TokenMutationFacts) -> bool {
    pearlite! { facts.now_ms@ >= 0
    && facts.expected_expiry_ms@ >= facts.now_ms@
    && facts.max_timestamp_ms@ >= facts.now_ms@
    && facts.expected_expiry_ms@ <= facts.max_timestamp_ms@ }
}

/// Fence a token mutation against its exact committed generation.
///
/// A retained log tail wins over retry classification. Exact already-applied
/// updates and already-missing deletes are idempotent. No update may revive a
/// token that has expired, which Kafka's `DelegationTokenControlManager`
/// defines as a deadline strictly before `now`, or cross the token's immutable
/// maximum timestamp. A renewal lands at or after `now` but, as in Kafka's
/// `renewDelegationToken`, may shorten the current expiry.
#[ensures((result == TokenMutationDecision::Append) == (
    !facts.uncommitted_tail
        && facts.state == TokenMutationState::Expected
        && match facts.kind {
            TokenMutationKind::Delete => true,
            TokenMutationKind::Renew =>
                token_generation_live(facts)
                    && facts.incoming_expiry_ms@ != facts.expected_expiry_ms@
                    && facts.incoming_expiry_ms@ >= facts.now_ms@
                    && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@,
            TokenMutationKind::Expire =>
                token_generation_live(facts)
                    && facts.incoming_expiry_ms@ >= 0
                    && facts.incoming_expiry_ms@ <= facts.max_timestamp_ms@,
        }))]
#[ensures((result == TokenMutationDecision::Retry) == (
    !facts.uncommitted_tail
        && (facts.state == TokenMutationState::Applied
            || (facts.kind == TokenMutationKind::Delete
                && facts.state == TokenMutationState::Missing)
            || (facts.kind == TokenMutationKind::Renew
                && facts.state == TokenMutationState::Expected
                && facts.incoming_expiry_ms@ == facts.expected_expiry_ms@
                && token_generation_live(facts)))))]
#[must_use]
pub fn token_mutation_decision(facts: TokenMutationFacts) -> TokenMutationDecision {
    if facts.uncommitted_tail {
        return TokenMutationDecision::Reject;
    }
    match facts.state {
        TokenMutationState::Applied => return TokenMutationDecision::Retry,
        TokenMutationState::Missing => {
            return match facts.kind {
                TokenMutationKind::Delete => TokenMutationDecision::Retry,
                TokenMutationKind::Renew | TokenMutationKind::Expire => {
                    TokenMutationDecision::Reject
                }
            };
        }
        TokenMutationState::Stale => return TokenMutationDecision::Reject,
        TokenMutationState::Expected => {}
    }

    match facts.kind {
        TokenMutationKind::Delete => TokenMutationDecision::Append,
        TokenMutationKind::Renew => {
            if facts.now_ms < 0
                || facts.expected_expiry_ms < facts.now_ms
                || facts.max_timestamp_ms < facts.now_ms
                || facts.expected_expiry_ms > facts.max_timestamp_ms
            {
                return TokenMutationDecision::Reject;
            }
            if facts.incoming_expiry_ms == facts.expected_expiry_ms {
                TokenMutationDecision::Retry
            } else if facts.incoming_expiry_ms >= facts.now_ms
                && facts.incoming_expiry_ms <= facts.max_timestamp_ms
            {
                TokenMutationDecision::Append
            } else {
                TokenMutationDecision::Reject
            }
        }
        TokenMutationKind::Expire => {
            if facts.now_ms >= 0
                && facts.expected_expiry_ms >= facts.now_ms
                && facts.max_timestamp_ms >= facts.now_ms
                && facts.expected_expiry_ms <= facts.max_timestamp_ms
                && facts.incoming_expiry_ms >= 0
                && facts.incoming_expiry_ms <= facts.max_timestamp_ms
            {
                TokenMutationDecision::Append
            } else {
                TokenMutationDecision::Reject
            }
        }
    }
}
