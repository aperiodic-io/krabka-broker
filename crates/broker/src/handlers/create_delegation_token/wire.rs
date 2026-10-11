//! The `CreateDelegationTokenResponse` shapes the handler emits: the two
//! refusals and the minted-token reply.
//!
//! Both are field-for-field contracts with the JVM admin client, so the
//! construction sits apart from the policy that decides which one to send.

use krabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use krabka_security::KafkaPrincipal;

use super::lifetime::TokenDeadlines;
use crate::codes::NONE;

/// A refusal decided on the broker before Kafka forwards the request.
///
/// Matches `CreateDelegationTokenResponse.prepareResponse(version, throttle,
/// error, owner, requester)`: the owner and requester are filled, the three
/// timestamps are `-1`, and the token id and HMAC are empty.
pub(super) fn broker_refusal(
    code: i16,
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        issue_timestamp_ms: -1,
        expiry_timestamp_ms: -1,
        max_timestamp_ms: -1,
        ..principals(code, owner, requester)
    }
}

/// A refusal decided by the controller.
///
/// Matches the error returns of
/// `DelegationTokenControlManager.createDelegationToken`: the owner and
/// requester are filled and every other field keeps its schema default.
pub(super) fn controller_refusal(
    code: i16,
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
) -> CreateDelegationTokenResponse {
    principals(code, owner, requester)
}

/// `code` plus the owner and requester principals, every other field at its
/// schema default.
fn principals(
    code: i16,
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        error_code: code,
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: requester.principal_type.clone(),
        token_requester_principal_name: requester.name.clone(),
        ..Default::default()
    }
}

/// Builds the success response for a token that the quorum has accepted.
///
/// `requester` is always populated into `token_requester_*`. For a token the
/// requester owns this equals the owner; otherwise it names who minted on
/// behalf of `owner`.
pub(super) fn minted_response(
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
    issue_timestamp_ms: i64,
    deadlines: &TokenDeadlines,
    token_id: String,
    hmac: Vec<u8>,
) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        issue_timestamp_ms,
        expiry_timestamp_ms: deadlines.initial_expiry_ms,
        max_timestamp_ms: deadlines.max_timestamp_ms,
        token_id,
        hmac: bytes::Bytes::from(hmac),
        ..principals(NONE, owner, requester)
    }
}
