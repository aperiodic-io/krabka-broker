//! The `DescribeDelegationToken` response shapes: the wire projection of one
//! visible `krabka_metadata::DelegationToken`, and the error-only envelope the
//! handler returns before it has a caller to filter for.
//!
//! These are the response's contract with the JVM `AdminClient`, including the
//! KIP-373 token-requester fields, so they sit apart from the code that
//! decides which tokens are visible.

use {
    krabka_protocol::owned::describe_delegation_token_response::{
        DescribeDelegationTokenResponse, DescribedDelegationToken, DescribedDelegationTokenRenewer,
    },
    krabka_security::{SecretBytes, compute_token_hmac},
};

/// The wire form of one visible token. The stored token has no HMAC, as in
/// Kafka's metadata, so it is recomputed under `secret_key`, as Kafka's
/// `DelegationTokenManager` does when it builds a `DelegationToken`.
pub(super) fn describe_token(
    t: krabka_metadata::DelegationToken,
    secret_key: &SecretBytes,
) -> DescribedDelegationToken {
    DescribedDelegationToken {
        principal_type: t.owner.principal_type.clone(),
        principal_name: t.owner.name.clone(),
        // KIP-373 token requester: the principal that created the token,
        // which differs from the owner when it minted the token for another
        // owner (Kafka's `TokenInformation.tokenRequester`).
        token_requester_principal_type: t.requester.principal_type,
        token_requester_principal_name: t.requester.name,
        issue_timestamp: t.issue_timestamp_ms,
        expiry_timestamp: t.expiry_timestamp_ms,
        max_timestamp: t.max_timestamp_ms,
        hmac: bytes::Bytes::from(compute_token_hmac(secret_key.as_bytes(), &t.token_id)),
        token_id: t.token_id,
        renewers: t
            .renewers
            .into_iter()
            .map(|r| DescribedDelegationTokenRenewer {
                principal_type: r.principal_type,
                principal_name: r.name,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Kafka's `ownersListEmpty` response: `error_code = NONE` with no tokens,
/// for a present-but-empty `owners` filter.
pub(super) fn empty_response() -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse {
        error_code: 0,
        ..Default::default()
    }
}
