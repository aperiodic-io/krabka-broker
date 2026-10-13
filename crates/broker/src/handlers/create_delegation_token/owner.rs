//! Owner resolution: which principal owns the token that a
//! `CreateDelegationToken` call mints, and whether the requester may mint for
//! that principal.

use {
    krabka_protocol::owned::create_delegation_token_request::CreateDelegationTokenRequest,
    krabka_security::KafkaPrincipal,
};

/// The owner of the token this request mints, or `None` when Kafka cannot
/// build one.
///
/// Matches `KafkaApis.handleCreateTokenRequest` and
/// `DelegationTokenControlManager.createDelegationToken` in Kafka 4.3.1: a
/// null or empty `owner_principal_name` means the requester, and any other
/// name is taken with whatever `owner_principal_type` the request carries.
/// A null `owner_principal_type` beside a name makes the `KafkaPrincipal`
/// constructor throw (`requireNonNull`), which is `None` here.
pub(super) fn resolve_owner(
    req: &CreateDelegationTokenRequest,
    requester: &KafkaPrincipal,
) -> Option<KafkaPrincipal> {
    match req.owner_principal_name.as_deref() {
        None | Some("") => Some(requester.clone()),
        Some(name) => Some(KafkaPrincipal {
            principal_type: req.owner_principal_type.clone()?,
            name: name.to_string(),
        }),
    }
}

/// Whether `requester` may mint a token owned by `owner`.
///
/// Matches `KafkaApis.handleCreateTokenRequest` in Kafka trunk: no ACL is
/// needed when the owner is the requester, and otherwise the requester needs
/// KIP-373's `CreateTokens` on the `User` resource named by the owner's
/// principal string (`owner.toString`, for example `User:alice`).
/// `authorize_create_tokens` answers that ACL question for a resource name;
/// the authorizer grants a super user every operation.
pub(super) fn may_create_for(
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
    authorize_create_tokens: impl FnOnce(&str) -> bool,
) -> bool {
    owner == requester || authorize_create_tokens(&owner.to_string())
}
